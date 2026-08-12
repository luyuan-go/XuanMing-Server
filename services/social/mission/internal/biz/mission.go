// Package biz — 任务域领域逻辑(2026-08-11,docs/design/mission.md)。
//
// 语义移植自 luyuan/mmorpg C++ MissionSystem/MissionsComp(对照表见设计文档 §7):
//   - 接取校验:配置存在 / 未接取 / 未完成 / (type,sub_type) 类型互斥 / 活跃数上限;
//   - 条件事实驱动进度:类别命中 + 槽位过滤 + 累加 clamp(condition 判定件在 pkg/configtable);
//   - 全条件满足即完成,完成扇出(同一事务):发奖或标记可领、自动接后续链、
//     COMPLETE_MISSION 条件再入(内部队列有界迭代,D 版 dispatcher 无环保护,移植加固);
//   - GM 批量完成与正常完成是两条刻意分离的路径(D 版 todo.md #225,不得合并)。
//
// 分层:引擎函数是纯函数(状态 + 配置进,突变出),IO 全在 MissionRepo(data 层)——
// 事务内 FOR UPDATE 载入状态 → 引擎回调 → 突变与推送出箱同事务持久化。
package biz

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
)

// maxFanoutRounds 完成扇出内部事件队列的迭代上限。
// 配置链环已在加载期拒批次(ValidateMissionCrossTables);本上限是运行期纵深兜底,
// 触顶即断链 + ERROR 日志,绝不无限循环(D 版 dispatcher 异步无此保护,移植加固)。
const maxFanoutRounds = 16

// pushPayloadSoftLimit 单条推送出箱 payload 软上限(列 VARBINARY(2048),留余量)。
const pushPayloadSoftLimit = 1800

// pushChunkMissions 单条 MissionUpdateEvent 携带的任务条数上限(分片粒度)。
const pushChunkMissions = 6

// CatalogSource 产出「一次领域操作期间恒定」的配置批次快照。
//
// 每个公开入口(ListMissions / Accept / Claim / ReportFacts / 补扫一轮)在最外层
// Snapshot() 一次,把得到的 Catalog 一路传进引擎与发放链。这样一次事务回调里几十次
// 配置读取全部来自同一批次 —— 热更 reload 的原子切换只影响**下一次**操作,不会在
// 半途换批次,造成同一条奖励的内容与装备/堆叠冻结位出身不同批次。
type CatalogSource interface {
	Snapshot() Catalog
}

// Catalog 是任务域需要的配置表最小视图(**单一批次**上的只读视图)。
// cmd/configtable.go 适配 configtable.Store;单测用 map 假件。
// 行指针只读,调用方不得修改。
type Catalog interface {
	MissionByID(id uint32) (*configpb.MissionRow, bool)
	ConditionByID(id uint32) (*configpb.ConditionRow, bool)
	RewardByID(id uint32) (*configpb.RewardRow, bool)
	// IsEquipment 判断道具是否装备(发奖路由:装备走 GrantInstances,其余走 GrantItems);
	// 行不存在返回 false(fail-closed,发放时由 inventory 白名单再兜)。
	IsEquipment(itemConfigID uint32) bool
}

// ── 领域状态与突变(repo ↔ 引擎的数据契约)──────────────────────────────────

// ActiveMission 一条活跃任务(player_mission_active 一行的领域形态)。
type ActiveMission struct {
	MissionConfigID uint32
	Progress        []uint32 // 每条件槽当前进度(与配置 condition_ids 等长同序)
	AcceptedAtMs    int64
}

// DoneMission 一条已完成任务(player_mission_done 一行的领域形态)。
type DoneMission struct {
	MissionConfigID uint32
	RewardState     uint32 // 与 missionv1.MissionRewardState 数值一致
	CompletedAtMs   int64
}

// 领奖状态(player_mission_done.reward_state 取值,与 proto enum 数值一致)。
const (
	RewardStateNone      uint32 = 0
	RewardStateClaimable uint32 = 1
	RewardStateClaimed   uint32 = 2
)

// PlayerState 单玩家任务全量状态(事务内 FOR UPDATE 载入;活跃 ≤ max_active_missions,
// 完成集被任务表行数有界,整体规模小)。
type PlayerState struct {
	PlayerID uint64
	Active   map[uint32]*ActiveMission
	Done     map[uint32]*DoneMission
}

// RewardLogEntry 一条待插入的发奖流水(repo 插入后回填 ID,供事务提交后立即尝试发放)。
type RewardLogEntry struct {
	ID              uint64 // repo 插入后回填
	MissionConfigID uint32
	Key             string // mission:<player_id>:<mission_config_id>
	RewardPB        []byte // MissionRewardStorageRecord
}

// Mutation 一次领域操作对存储的全部改动(repo 在同一事务内持久化)。
type Mutation struct {
	UpsertActive []*ActiveMission
	DeleteActive []uint32
	InsertDone   []*DoneMission
	ClaimDone    []uint32 // reward_state CLAIMABLE→CLAIMED(条件更新)
	RewardLogs   []*RewardLogEntry
	PushPayloads [][]byte // MissionUpdateEvent 分片(与状态写同事务入出箱)
	// FanoutTruncated 完成扇出触顶断链(理论不可达:链环加载期已拒;usecase 记 ERROR)。
	FanoutTruncated bool
}

// Fact 一条条件事实(ReportMissionFacts 入参的领域形态;COMPLETE_MISSION 再入也用它)。
type Fact struct {
	Category   uint32
	SlotValues []uint32
	Amount     uint32
}

// RewardLogRow 发奖流水一行(补扫工作集)。
type RewardLogRow struct {
	ID              uint64
	PlayerID        uint64
	MissionConfigID uint32
	Key             string
	RewardPB        []byte
}

// PushOutboxRow 推送出箱一行。
type PushOutboxRow struct {
	ID       uint64
	PlayerID uint64
	Payload  []byte
}

// MissionRepo 是任务域存储接口(MySQL 实现在 data 层)。
type MissionRepo interface {
	// MutatePlayer 事务内 FOR UPDATE 载入玩家状态 → fn 计算突变 → 同事务持久化。
	// fn 返回 error 时整个事务回滚(错误原样透出)。
	MutatePlayer(ctx context.Context, playerID uint64, fn func(st *PlayerState) (*Mutation, error)) error
	// ApplyFactsTx 同 MutatePlayer,外加事实收据幂等:同键已存在且指纹一致 →
	// already=true 且不调 fn;指纹不一致 → ErrMissionFactsConflict。
	ApplyFactsTx(ctx context.Context, playerID uint64, idemKey string, fingerprint []byte,
		fn func(st *PlayerState) (*Mutation, error)) (already bool, err error)
	// LoadPlayer 无锁读全量状态(ListMissions / resync 回源)。
	LoadPlayer(ctx context.Context, playerID uint64) (*PlayerState, error)

	// 发奖补扫工作集(status<>GRANTED 且 updated_at_ms 早于 olderThanMs,按 id 序)。
	ListUngrantedRewards(ctx context.Context, olderThanMs int64, limit int) ([]*RewardLogRow, error)
	// MarkReward 更新发奖流水状态(granted=false 记 FAILED,留给下轮补扫)。
	MarkReward(ctx context.Context, id uint64, granted bool, nowMs int64) error

	// 推送出箱(FIFO 按 id 序;投递成功即删)。
	FetchPushOutbox(ctx context.Context, limit int) ([]*PushOutboxRow, error)
	DeletePushOutbox(ctx context.Context, id uint64) error

	// 保留期清理(§9.24;mode=report_only 只报告)。
	SweepRewardLog(ctx context.Context, mode string, retentionDays, batch int) error
	SweepReceipts(ctx context.Context, mode string, retentionDays, batch int) error
}

// ── 发放下游接口(data 层实现;docs/design/mission.md §6)────────────────────

// RewardItem 一条道具发放(数量在 uint32 域,发给 inventory 时转 int64)。
type RewardItem struct {
	ItemConfigID uint32
	Count        uint32
}

// ItemGranter 道具/装备发放(inventory 系统 RPC)。
type ItemGranter interface {
	GrantItems(ctx context.Context, playerID uint64, idemKey string, items []RewardItem) error
	// GrantInstances 逐件发装备实例;capacityFull=true 表示背包满(调用方转邮件)。
	GrantInstances(ctx context.Context, playerID uint64, idemKey string, itemConfigIDs []uint32) (capacityFull bool, err error)
}

// ExpGranter 经验发放(player.AddExperience,reason="quest")。
type ExpGranter interface {
	AddExperience(ctx context.Context, playerID uint64, expDelta uint64, idemKey string) error
}

// OverflowMailSender 满包溢出转邮件(battle_result 模式:同键传 instance_grant_key)。
type OverflowMailSender interface {
	SendOverflowMail(ctx context.Context, playerID uint64, itemConfigIDs []uint32, grantKey string) error
}

// Pusher 推送投递(kafka pandora.mission.update,key=player_id)。
type Pusher interface {
	PushMissionUpdate(ctx context.Context, playerID uint64, payload []byte) error
}

// ── Usecase ────────────────────────────────────────────────────────────────

// MissionUsecase 任务域业务核心。
type MissionUsecase struct {
	repo     MissionRepo
	catalogs CatalogSource
	items    ItemGranter
	exp      ExpGranter
	mail     OverflowMailSender // 可 nil:满包溢出转邮件不可用,发放失败留补扫
	pusher   Pusher             // 可 nil:推送禁用(kafka 未配),客户端靠 ListMissions
	cfg      conf.MissionConf
	log      *klog.Helper
	nowMs    func() int64

	// pushLease 推送发布器的领导权来源;nil = 不选举。只在装配期由 SetPushWriterLease 写。
	pushLease PushWriterLease
	// pushLeaseHeld 上一轮领导权状态,只由发布器单 goroutine 读写(跃迁日志去重用)。
	pushLeaseHeld bool
}

// 关于 cellroute:任务域**刻意不接** region/cell 路由器(§14.1 不留"以后再接"的钩子,
// §15.3 拒绝预设性复杂化)。全仓先例是「接了就真读」(chat / inventory 都有实际读取点)
// 或「用不上就完全不接」(mail / guild / leaderboard);没有"存了不读"的中间态。
// 将来任务域真需要落点观测时,照 chat 的样子一次接到位。

// NewMissionUsecase 构造。
func NewMissionUsecase(repo MissionRepo, catalogs CatalogSource, items ItemGranter, exp ExpGranter,
	mail OverflowMailSender, pusher Pusher, cfg conf.MissionConf, logger klog.Logger) *MissionUsecase {
	return &MissionUsecase{
		repo: repo, catalogs: catalogs, items: items, exp: exp, mail: mail, pusher: pusher,
		cfg: cfg, log: klog.NewHelper(logger),
		nowMs: func() int64 { return time.Now().UnixMilli() },
	}
}

// StaticCatalogSource 把一个固定 Catalog 包成 CatalogSource(单测与不热更的场景)。
type StaticCatalogSource struct{ Catalog Catalog }

// Snapshot 见 CatalogSource。
func (s StaticCatalogSource) Snapshot() Catalog { return s.Catalog }

// SetNowFunc 仅测试用:注入确定性时钟。
func (uc *MissionUsecase) SetNowFunc(f func() int64) { uc.nowMs = f }

// ── 查询 ───────────────────────────────────────────────────────────────────

// ListMissions 权威快照(活跃 + 已完成;resync 回源接口)。
func (uc *MissionUsecase) ListMissions(ctx context.Context, playerID uint64) ([]*missionv1.ActiveMission, []*missionv1.CompletedMission, error) {
	cat := uc.catalogs.Snapshot()
	st, err := uc.repo.LoadPlayer(ctx, playerID)
	if err != nil {
		return nil, nil, err
	}
	active := make([]*missionv1.ActiveMission, 0, len(st.Active))
	for _, am := range sortedActive(st) {
		active = append(active, uc.toProtoActive(cat, am))
	}
	completed := make([]*missionv1.CompletedMission, 0, len(st.Done))
	for _, dm := range sortedDone(st) {
		completed = append(completed, toProtoCompleted(dm))
	}
	return active, completed, nil
}

// ── 接取 / 放弃 / GM ───────────────────────────────────────────────────────

// Accept 玩家接取任务。
func (uc *MissionUsecase) Accept(ctx context.Context, playerID uint64, missionID uint32) (*missionv1.ActiveMission, error) {
	cat := uc.catalogs.Snapshot()
	var accepted *ActiveMission
	err := uc.repo.MutatePlayer(ctx, playerID, func(st *PlayerState) (*Mutation, error) {
		am, aerr := uc.acceptInto(cat, st, missionID)
		if aerr != nil {
			return nil, aerr
		}
		accepted = am
		return &Mutation{UpsertActive: []*ActiveMission{am}}, nil
	})
	if err != nil {
		return nil, err
	}
	return uc.toProtoActive(cat, accepted), nil
}

// Abandon 玩家放弃任务。
// 语义:已完成不可弃(D 版一致);不在活跃列表返回 ErrMissionNotAccepted
// (刻意差异:D 版对不存在的任务静默成功,Go 版显式错误码暴露客户端状态漂移,§16 可观测)。
func (uc *MissionUsecase) Abandon(ctx context.Context, playerID uint64, missionID uint32) error {
	return uc.repo.MutatePlayer(ctx, playerID, func(st *PlayerState) (*Mutation, error) {
		if _, done := st.Done[missionID]; done {
			return nil, errcode.New(errcode.ErrMissionAlreadyCompleted,
				"abandon completed mission=%d player=%d", missionID, playerID)
		}
		if _, ok := st.Active[missionID]; !ok {
			return nil, errcode.New(errcode.ErrMissionNotAccepted,
				"abandon non-active mission=%d player=%d", missionID, playerID)
		}
		delete(st.Active, missionID)
		return &Mutation{DeleteActive: []uint32{missionID}}, nil
	})
}

// CompleteAll GM 批量完成:全部活跃任务置完成 + 清空活跃列表。
//
// 刻意不发奖、不标记可领、不自动接后续链、不触发 COMPLETE_MISSION 再入 ——
// 与正常完成扇出是两条不可合并的路径(D 版 CompleteAllMissions / todo.md #225)。
//
// **但仍产出推送**:不推等于让客户端留着一份与权威不一致的活跃列表,直到下次
// ListMissions 才对齐。推送不承担正确性,推它不会把两条路径合并,漏推却会制造一个
// "只有 GM 知道"的状态漂移(proto 注释此前写反了,2026-08-11 已更正为与本实现一致)。
func (uc *MissionUsecase) CompleteAll(ctx context.Context, playerID uint64) (uint32, error) {
	var count uint32
	err := uc.repo.MutatePlayer(ctx, playerID, func(st *PlayerState) (*Mutation, error) {
		mut := &Mutation{}
		now := uc.nowMs()
		evt := &missionv1.MissionUpdateEvent{TsMs: uint64(now)}
		for _, am := range sortedActive(st) {
			mid := am.MissionConfigID
			delete(st.Active, mid)
			dm := &DoneMission{MissionConfigID: mid, RewardState: RewardStateNone, CompletedAtMs: now}
			st.Done[mid] = dm
			mut.DeleteActive = append(mut.DeleteActive, mid)
			mut.InsertDone = append(mut.InsertDone, dm)
			evt.Completed = append(evt.Completed, toProtoCompleted(dm))
			count++
		}
		if count == 0 {
			return mut, nil
		}
		payloads, perr := marshalEventChunks(evt)
		if perr != nil {
			return nil, perr
		}
		mut.PushPayloads = payloads
		return mut, nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// ── 领奖 ───────────────────────────────────────────────────────────────────

// Claim 领取任务奖励:CLAIMABLE→CLAIMED CAS 与 reward_log(PENDING) 同事务 =
// 「已领」立即权威生效;内容发放 at-least-once(提交后立即尝试一次,失败留补扫)。
func (uc *MissionUsecase) Claim(ctx context.Context, playerID uint64, missionID uint32) error {
	cat := uc.catalogs.Snapshot()
	var logs []*RewardLogEntry
	err := uc.repo.MutatePlayer(ctx, playerID, func(st *PlayerState) (*Mutation, error) {
		dm, ok := st.Done[missionID]
		if !ok || dm.RewardState != RewardStateClaimable {
			return nil, errcode.New(errcode.ErrMissionNotClaimable,
				"claim mission=%d player=%d state=%v", missionID, playerID, dm)
		}
		row, ok := cat.MissionByID(missionID)
		if !ok {
			// 完成后任务行被策划删掉:配置错,fail-closed 不发未知内容。
			return nil, errcode.New(errcode.ErrMissionConfigNotFound,
				"claim mission=%d config missing", missionID)
		}
		entry, rerr := uc.buildRewardLog(cat, playerID, row)
		if rerr != nil {
			return nil, rerr
		}
		dm.RewardState = RewardStateClaimed
		mut := &Mutation{ClaimDone: []uint32{missionID}}
		if entry != nil {
			mut.RewardLogs = []*RewardLogEntry{entry}
		}
		logs = mut.RewardLogs
		return mut, nil
	})
	if err != nil {
		return err
	}
	// 提交后同步尝试发放一次(请求 ctx 有界;失败不回滚领取,补扫兜底)。
	uc.grantEntriesBestEffort(ctx, cat, playerID, logs)
	return nil
}

// ── 条件事实入账(唯一进度写入通道)──────────────────────────────────────────

// ReportFacts 事实入账:收据幂等 → 引擎推进 → 完成扇出,全部同一事务。
func (uc *MissionUsecase) ReportFacts(ctx context.Context, playerID uint64, facts []Fact, idemKey string) (bool, error) {
	if len(facts) == 0 || idemKey == "" || len(idemKey) > 128 {
		return false, errcode.New(errcode.ErrInvalidArg, "facts=%d key_len=%d", len(facts), len(idemKey))
	}
	if len(facts) > uc.cfg.MaxFactsPerReport {
		return false, errcode.New(errcode.ErrInvalidArg, "facts %d > max %d", len(facts), uc.cfg.MaxFactsPerReport)
	}
	cat := uc.catalogs.Snapshot()
	var logs []*RewardLogEntry
	already, err := uc.repo.ApplyFactsTx(ctx, playerID, idemKey, factsFingerprint(playerID, facts),
		func(st *PlayerState) (*Mutation, error) {
			mut, ferr := uc.applyFactsEngine(cat, st, facts)
			if ferr != nil {
				return nil, ferr
			}
			logs = mut.RewardLogs
			if mut.FanoutTruncated {
				uc.log.Errorw("msg", "mission_fanout_truncated", "player_id", playerID, "key", idemKey,
					"hint", "完成扇出触顶断链;链环应已在配置加载期拒绝,检查 ValidateMissionCrossTables")
			}
			return mut, nil
		})
	if err != nil || already {
		return already, err
	}
	// 自动发奖:提交后同步尝试一次,失败留补扫。
	uc.grantEntriesBestEffort(ctx, cat, playerID, logs)
	return false, nil
}

// ── 引擎(纯函数,只读 catalog)───────────────────────────────────────────────

// acceptInto 接取校验 + 建活跃行(D 版 CheckMissionAcceptance + AcceptMission)。
// 类型互斥从活跃行现算(D 版 typeFilter 派生态;sub_type=0 不参与互斥)。
func (uc *MissionUsecase) acceptInto(cat Catalog, st *PlayerState, missionID uint32) (*ActiveMission, error) {
	row, ok := cat.MissionByID(missionID)
	if !ok {
		return nil, errcode.New(errcode.ErrMissionConfigNotFound, "mission=%d", missionID)
	}
	if _, dup := st.Active[missionID]; dup {
		return nil, errcode.New(errcode.ErrMissionAlreadyAccepted, "mission=%d", missionID)
	}
	if _, done := st.Done[missionID]; done {
		return nil, errcode.New(errcode.ErrMissionAlreadyCompleted, "mission=%d", missionID)
	}
	if len(st.Active) >= uc.cfg.MaxActiveMissions {
		return nil, errcode.New(errcode.ErrMissionActiveLimit,
			"active=%d max=%d", len(st.Active), uc.cfg.MaxActiveMissions)
	}
	if row.GetMissionSubType() > 0 {
		for _, other := range st.Active {
			orow, ook := cat.MissionByID(other.MissionConfigID)
			if !ook {
				continue
			}
			if orow.GetMissionType() == row.GetMissionType() && orow.GetMissionSubType() == row.GetMissionSubType() {
				return nil, errcode.New(errcode.ErrMissionTypeConflict,
					"mission=%d conflicts with active=%d type=%d sub=%d",
					missionID, other.MissionConfigID, row.GetMissionType(), row.GetMissionSubType())
			}
		}
	}
	am := &ActiveMission{
		MissionConfigID: missionID,
		Progress:        make([]uint32, len(configtable.MissionConditionIDs(row))),
		AcceptedAtMs:    uc.nowMs(),
	}
	st.Active[missionID] = am
	return am, nil
}

// applyFactsEngine 进度推进 + 完成扇出(D 版 HandleConditionEvent + OnMissionCompletion)。
func (uc *MissionUsecase) applyFactsEngine(cat Catalog, st *PlayerState, facts []Fact) (*Mutation, error) {
	mut := &Mutation{}
	now := uc.nowMs()
	evt := &missionv1.MissionUpdateEvent{TsMs: uint64(now)}
	changed := make(map[uint32]bool)

	queue := make([]Fact, len(facts))
	copy(queue, facts)

	for rounds := 0; len(queue) > 0; rounds++ {
		if rounds >= maxFanoutRounds {
			mut.FanoutTruncated = true
			break
		}
		batch := queue
		queue = nil

		var completed []uint32
		for _, fact := range batch {
			// D 版护栏:空 condition_ids 的事实不推进任何进度(全空槽条件匹配任意同类
			// 事件,无槽位值的事实放行会误推);amount=0 无效。
			if fact.Amount == 0 || len(fact.SlotValues) == 0 || fact.Category == 0 {
				continue
			}
			for _, am := range sortedActive(st) {
				row, ok := cat.MissionByID(am.MissionConfigID)
				if !ok {
					continue // 配置热更删行:活跃孤行不再推进,不报错(展示层照旧)
				}
				if !uc.progressMission(cat, row, am, fact) {
					continue
				}
				changed[am.MissionConfigID] = true
				if uc.allConditionsFulfilled(cat, row, am) {
					completed = append(completed, am.MissionConfigID)
				}
			}
		}

		// 完成扇出(D 版 OnMissionCompletion 四步,同一事务内)。
		for _, mid := range completed {
			if _, still := st.Active[mid]; !still {
				continue // 同批重复完成判定(不同事实各推一次)去重
			}
			row, ok := cat.MissionByID(mid)
			if !ok {
				continue
			}
			delete(st.Active, mid)
			delete(changed, mid)
			mut.DeleteActive = append(mut.DeleteActive, mid)

			dm := &DoneMission{MissionConfigID: mid, RewardState: RewardStateNone, CompletedAtMs: now}
			if row.GetRewardId() > 0 {
				if row.GetAutoReward() > 0 {
					entry, rerr := uc.buildRewardLog(cat, st.PlayerID, row)
					if rerr != nil {
						return nil, rerr
					}
					if entry != nil {
						mut.RewardLogs = append(mut.RewardLogs, entry)
					}
				} else {
					dm.RewardState = RewardStateClaimable
				}
			}
			st.Done[mid] = dm
			mut.InsertDone = append(mut.InsertDone, dm)
			evt.Completed = append(evt.Completed, toProtoCompleted(dm))

			// 自动接后续链(校验不过跳过该条不阻断,D 版 enqueue AcceptMissionEvent 语义)。
			for _, nid := range configtable.MissionNextIDs(row) {
				next, aerr := uc.acceptInto(cat, st, nid)
				if aerr != nil {
					uc.log.Warnw("msg", "mission_chain_accept_skipped",
						"player_id", st.PlayerID, "from", mid, "next", nid, "err", aerr)
					continue
				}
				mut.UpsertActive = append(mut.UpsertActive, next)
				evt.AutoAccepted = append(evt.AutoAccepted, uc.toProtoActive(cat, next))
			}

			// COMPLETE_MISSION 条件再入(下一轮处理;此时链上新任务已激活,D 版 FIFO 等价)。
			queue = append(queue, Fact{
				Category:   configtable.ConditionCategoryCompleteMission,
				SlotValues: []uint32{mid},
				Amount:     1,
			})
		}
	}

	// 进度有变化且仍活跃的任务 → 突变 + 推送快照。
	for _, mid := range sortedKeys(changed) {
		am := st.Active[mid]
		if am == nil {
			continue
		}
		mut.UpsertActive = append(mut.UpsertActive, am)
		evt.Progressed = append(evt.Progressed, uc.toProtoActive(cat, am))
	}

	if len(evt.Progressed)+len(evt.Completed)+len(evt.AutoAccepted) > 0 {
		payloads, perr := marshalEventChunks(evt)
		if perr != nil {
			return nil, perr
		}
		mut.PushPayloads = payloads
	}
	return mut, nil
}

// alignProgressSlots 把活跃行的进度槽补齐到**当前配置**的条件槽数(只扩不缩)。
//
// 热更给任务加条件时,旧活跃行的 progress 比 condition_ids 短。原实现在推进与达标判定
// 两处都取 `min(len(condIDs), len(Progress))`,后果不是"新条件暂不生效"而是**白送完成**:
// 例如任务原本单条件 3/3 未满(progress=[2]),热更加了第二个条件后,下一条属于条件 1 的
// 事实把槽 0 推到 3 → 达标判定只看 min=1 个槽 → 直接判定全条件满足 → 完成并发奖,
// 新加的条件一次都没被检查过。补零扩容后:已有进度原样保留,新条件从 0 开始必须真做完。
//
// 反向(热更删条件)刻意不截断:多出的尾部槽本就被两处循环按 condition_ids 长度忽略,
// 留着还能在配置回滚后复原进度(截断则不可逆)。
func alignProgressSlots(row *configpb.MissionRow, am *ActiveMission) {
	need := len(configtable.MissionConditionIDs(row))
	if len(am.Progress) >= need {
		return
	}
	grown := make([]uint32, need)
	copy(grown, am.Progress)
	am.Progress = grown
}

// progressMission 单事实对单任务的槽位推进(D 版 UpdateMissionProgress)。
func (uc *MissionUsecase) progressMission(cat Catalog, row *configpb.MissionRow, am *ActiveMission, fact Fact) bool {
	alignProgressSlots(row, am)
	condIDs := configtable.MissionConditionIDs(row)
	slots := len(condIDs)
	if len(am.Progress) < slots {
		slots = len(am.Progress) // 防御:align 之后不该发生
	}
	progressed := false
	for i := 0; i < slots; i++ {
		cond, ok := cat.ConditionByID(condIDs[i])
		if !ok {
			continue
		}
		override := configtable.MissionSlotTarget(row, i)
		// 已达标槽不再累加(D 版 UpdateProgressIfConditionMatches 首查)。
		if configtable.ConditionIsFulfilled(cond, am.Progress[i], override) {
			continue
		}
		if cond.GetConditionCategory() != fact.Category {
			continue
		}
		if !configtable.ConditionMatchesEventSlots(cond, fact.SlotValues) {
			continue
		}
		next := saturatingAdd(am.Progress[i], fact.Amount)
		am.Progress[i] = configtable.ConditionClampIfFulfilled(cond, next, override)
		progressed = true
	}
	return progressed
}

// allConditionsFulfilled 全槽达标判定(D 版 AreAllConditionsFulfilled)。
//
// 按**配置的全部条件槽**判定,槽数不足一律 fail-closed 判未达标(原实现取 min 会让
// 热更新增的条件被整段跳过 → 白送完成,见 alignProgressSlots)。
func (uc *MissionUsecase) allConditionsFulfilled(cat Catalog, row *configpb.MissionRow, am *ActiveMission) bool {
	condIDs := configtable.MissionConditionIDs(row)
	for i, cid := range condIDs {
		if i >= len(am.Progress) {
			return false // 进度槽比配置短(未对齐):宁可不完成,不误发奖
		}
		cond, ok := cat.ConditionByID(cid)
		if !ok {
			return false // 条件行缺失 fail-closed:宁可不完成,不误发奖
		}
		if !configtable.ConditionIsFulfilled(cond, am.Progress[i], configtable.MissionSlotTarget(row, i)) {
			return false
		}
	}
	return true
}

// buildRewardLog 按奖励表拼发放内容快照(reward_id=0 或空内容返回 nil)。
func (uc *MissionUsecase) buildRewardLog(cat Catalog, playerID uint64, row *configpb.MissionRow) (*RewardLogEntry, error) {
	rewardID := row.GetRewardId()
	if rewardID == 0 {
		return nil, nil
	}
	rrow, ok := cat.RewardByID(rewardID)
	if !ok {
		// 加载期 fk 校验已挡;热更竞态兜底:fail-closed 拒绝而不是发空奖。
		return nil, errcode.New(errcode.ErrInternal, "reward=%d config missing (mission=%d)", rewardID, row.GetId())
	}
	record := &missionv1.MissionRewardStorageRecord{Exp: uint64(rrow.GetExp())}
	for _, entry := range configtable.RewardItems(rrow) {
		// 发放形态在**落快照这一刻**冻结:装备走 GrantInstances(`:inst` 键)、堆叠走
		// GrantItems(`:stack` 键),两个键在 inventory 台账里互不相识。若发放时才回读
		// 道具表,形态在两次投递之间被热更改掉(或滚动升级期新旧副本加载着不同配置批次),
		// 同一条奖励会先后用两个不同幂等键各发一次 —— 幂等键防不住,因为不是同一个键。
		equipment := cat.IsEquipment(entry.ItemConfigID)
		record.Items = append(record.Items, &missionv1.MissionRewardItem{
			ItemConfigId: entry.ItemConfigID, Count: entry.Count, Equipment: &equipment,
		})
	}
	pb, err := proto.Marshal(record)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal reward record mission=%d", row.GetId())
	}
	return &RewardLogEntry{
		MissionConfigID: row.GetId(),
		Key:             fmt.Sprintf("mission:%d:%d", playerID, row.GetId()),
		RewardPB:        pb,
	}, nil
}

// ── 辅助 ───────────────────────────────────────────────────────────────────

// toProtoActive 活跃任务 → 客户端可见结构(targets 服务端算好下发,客户端不重算)。
func (uc *MissionUsecase) toProtoActive(cat Catalog, am *ActiveMission) *missionv1.ActiveMission {
	out := &missionv1.ActiveMission{
		MissionConfigId: am.MissionConfigID,
		Progress:        append([]uint32(nil), am.Progress...),
		AcceptedAtMs:    uint64(am.AcceptedAtMs),
	}
	if row, ok := cat.MissionByID(am.MissionConfigID); ok {
		// 热更加条件后的旧行 progress 比 condition_ids 短:下发前补零,
		// 否则 progress 与 targets 长度不等,客户端逐槽渲染会错位/越界。
		for len(out.Progress) < len(configtable.MissionConditionIDs(row)) {
			out.Progress = append(out.Progress, 0)
		}
		condIDs := configtable.MissionConditionIDs(row)
		out.Targets = make([]uint32, 0, len(condIDs))
		for i, cid := range condIDs {
			target := configtable.MissionSlotTarget(row, i)
			if cond, cok := cat.ConditionByID(cid); cok {
				target = configtable.ConditionEffectiveTarget(cond, target)
			}
			out.Targets = append(out.Targets, target)
		}
	}
	return out
}

func toProtoCompleted(dm *DoneMission) *missionv1.CompletedMission {
	return &missionv1.CompletedMission{
		MissionConfigId: dm.MissionConfigID,
		RewardState:     missionv1.MissionRewardState(dm.RewardState),
		CompletedAtMs:   uint64(dm.CompletedAtMs),
	}
}

// marshalEventChunks 把推送事件按任务条数分片序列化,单片超软上限再对半拆
// (出箱列 VARBINARY(2048) 的写入侧上限,§9.24 深度闸)。
func marshalEventChunks(evt *missionv1.MissionUpdateEvent) ([][]byte, error) {
	type item struct {
		progressed   *missionv1.ActiveMission
		completed    *missionv1.CompletedMission
		autoAccepted *missionv1.ActiveMission
	}
	var items []item
	for _, p := range evt.GetProgressed() {
		items = append(items, item{progressed: p})
	}
	for _, c := range evt.GetCompleted() {
		items = append(items, item{completed: c})
	}
	for _, a := range evt.GetAutoAccepted() {
		items = append(items, item{autoAccepted: a})
	}
	if len(items) == 0 {
		return nil, nil
	}
	var out [][]byte
	var build func(part []item) error
	build = func(part []item) error {
		chunk := &missionv1.MissionUpdateEvent{TsMs: evt.GetTsMs()}
		for _, it := range part {
			switch {
			case it.progressed != nil:
				chunk.Progressed = append(chunk.Progressed, it.progressed)
			case it.completed != nil:
				chunk.Completed = append(chunk.Completed, it.completed)
			case it.autoAccepted != nil:
				chunk.AutoAccepted = append(chunk.AutoAccepted, it.autoAccepted)
			}
		}
		pb, err := proto.Marshal(chunk)
		if err != nil {
			return err
		}
		if len(pb) > pushPayloadSoftLimit && len(part) > 1 {
			mid := len(part) / 2
			if err := build(part[:mid]); err != nil {
				return err
			}
			return build(part[mid:])
		}
		out = append(out, pb)
		return nil
	}
	for i := 0; i < len(items); i += pushChunkMissions {
		end := i + pushChunkMissions
		if end > len(items) {
			end = len(items)
		}
		if err := build(items[i:end]); err != nil {
			return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal mission push event")
		}
	}
	return out, nil
}

// factsFingerprint 事实批的规范化指纹(收据表同键防串改账;与 proto 编码无关,
// 字段顺序固定,跨版本稳定)。
func factsFingerprint(playerID uint64, facts []Fact) []byte {
	h := sha256.New()
	fmt.Fprintf(h, "p=%d", playerID)
	for _, f := range facts {
		fmt.Fprintf(h, "|c=%d;a=%d;s=", f.Category, f.Amount)
		for _, v := range f.SlotValues {
			fmt.Fprintf(h, "%d,", v)
		}
	}
	return h.Sum(nil)
}

func saturatingAdd(a, b uint32) uint32 {
	if a > math.MaxUint32-b {
		return math.MaxUint32
	}
	return a + b
}

// sortedActive 活跃任务按配置 ID 稳定序(map 迭代序不稳定 → 推进/推送/测试全部确定性)。
func sortedActive(st *PlayerState) []*ActiveMission {
	out := make([]*ActiveMission, 0, len(st.Active))
	for _, am := range st.Active {
		out = append(out, am)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MissionConfigID < out[j].MissionConfigID })
	return out
}

func sortedDone(st *PlayerState) []*DoneMission {
	out := make([]*DoneMission, 0, len(st.Done))
	for _, dm := range st.Done {
		out = append(out, dm)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MissionConfigID < out[j].MissionConfigID })
	return out
}

func sortedKeys(m map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
