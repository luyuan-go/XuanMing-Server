// Package biz 是 battle_result 服务的业务逻辑层(W4 ③,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.13):
//   - Model-B 同步 ReportResult / legacy battle.result → 幂等落库(不变量 §2,unique match_id)
//   - MMR 在此算(Elo,DS 不可信,不变量 §6),落 battle_player_stats.mmr_delta
//   - 消费 pandora.ds.lifecycle 的 ABANDONED → 写 abandoned 补偿记录(不变量 §4)
//   - 落库同事务写 player.update 出箱 → 后台发布器可靠投递(不变量 §4)
//   - 提供 GetMatchResult / ListPlayerHistory 查询 RPC
//
// 关键不变量:
//   - 幂等键 = match_id(SaveResult 命中唯一键 → alreadyRecorded,不重复写)
//   - MMR 覆盖 DS 上报值(只信对局胜负 winner_team,不信 DS 给的 mmr_delta)
//
// W4 ⑨ 可靠补偿(事务出箱,HANDOFF §3 Step 2):
//
//	W4 ③ 落库后直接发 player.update 是 best-effort 弱依赖,Kafka 不可用时事件直接丢
//	→ 玩家段位永不更新。W4 ⑨ 改为:落 battles + stats 的同一事务里再写 player.update
//	出箱行(原子提交);后台 RunOutboxPublisher 轮询出箱逐条投递 Kafka,成功才删行。
//	配合 player 服务幂等消费(W4 ④ mmr_history uk),整条段位写链是 at-least-once
//	可靠闭环,可穿越 Kafka 临时不可用。
package biz

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// MMRReader 读玩家当前 MMR(算 Elo 期望胜率用)。
//
// W4 ③ player 服务未上线 → StaticMMRReader 全返 BaseMMR;player 上线后换 gRPC 实现。
type MMRReader interface {
	GetMMR(ctx context.Context, playerID uint64) (int, error)
}

// PlayerUpdatePusher 发 pandora.player.update 事件(kafka key=player_id,不变量 §9)。
//
// W4 ⑨ 起由后台 RunOutboxPublisher 调用:投递失败 → 返回 error → 出箱行保留下轮重试
// (不再是 best-effort 静默丢)。
type PlayerUpdatePusher interface {
	PushPlayerUpdate(ctx context.Context, playerID uint64, payload []byte) error
}

// MatchReleaser 通知 matchmaker 释放一场已结算/废弃对局的撮合状态(内部 RPC)。
//
// 修复:对局走完 READY → 进战斗 → 结算后,matchmaker 故意保留的 player→ticket 归属
// (SETNX claim)+ 票据 + match 镜像本只能等 30min TTL 自然过期;期间玩家回 Hub 再次
// StartMatch 会撞上残留 claim 报 ErrMatchAlreadyMatching(4002)。battle_result 落库后
// 主动调此接口让 matchmaker 彻底释放,玩家回 Hub 即可立刻再次匹配。
//
// 调用由 MySQL match_release_outbox worker 执行；失败/未知保留行重试，明确成功才 ACK。
type MatchReleaser interface {
	ReleaseMatch(ctx context.Context, matchID uint64, playerIDs []uint64) error
}

// TerminalReleaseRelay 把 MySQL 持久证明交给 ds_allocator 的两阶段控制面：
// ReleaseTerminal 只做永久 Redis terminal/receipt CAS + UID precondition Release；
// FinalizeTerminal 只在 MySQL 已 durable 标记 released 后给同 proof 墓碑恢复 TTL。
// 任一超时/未知结果都必须返回 error 让 outbox 保留重试。
type TerminalReleaseRelay interface {
	ReleaseTerminal(context.Context, data.TerminalReleaseRecord) error
	FinalizeTerminal(context.Context, data.TerminalReleaseRecord) error
}

// InstanceGranter 把战斗装备掉落幂等发放到 inventory(GrantInstances,W5 ④)。
//
// 由后台 RunDropPublisher 调用:发放失败 → 返回 error → drop 出箱行保留下轮重试
// (at-least-once,配合 GrantInstances 幂等键去重)。实现可为 nil:inventory_addr 未配
// → 不启动掉落发布器,掉落出箱积压不丢(等 inventory 地址配好重启后补发)。
type InstanceGranter interface {
	GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error
}

// MailSender 把背包满溢出的战斗装备掉落转个人邮件(mail.SendPersonalMail,幂等键防重发)。
//
// 由 RunDropPublisher 调用:GrantInstances 返回 ErrInventoryCapacityFull(背包满)时,
// 改调此接口把溢出装备转邮件,成功后删出箱行(不再无休止重试)。实现可为 nil:mail_addr 未配
// → 背包满掉落留在出箱轮询重试(退化为历史行为,at-least-once 不丢)。
type MailSender interface {
	SendOverflowMail(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error
}

// StaticMMRReader 是固定返回 base 的 MMRReader(player 服务未上线时兜底)。
type StaticMMRReader struct {
	base int
}

// NewStaticMMRReader 构造。
func NewStaticMMRReader(base int) *StaticMMRReader { return &StaticMMRReader{base: base} }

// GetMMR 恒返 base。
func (s *StaticMMRReader) GetMMR(_ context.Context, _ uint64) (int, error) { return s.base, nil }

// BattleResultUsecase 是 battle_result 业务逻辑核心。
type BattleResultUsecase struct {
	repo          data.BattleRepo
	mmr           MMRReader
	pusher        PlayerUpdatePusher
	releaser      MatchReleaser
	cfg           conf.BattleConf
	terminalRelay TerminalReleaseRelay

	// granter 把战斗装备掉落幂等发放到 inventory(W5 ④,nil-safe)。
	// nil = inventory_addr 未配 → 不启动 RunDropPublisher,掉落出箱积压不丢。
	// 用 setter 注入(SetInstanceGranter),避免构造签名被迫改(与 SetCellRouter 一致)。
	granter InstanceGranter

	// mailSender 把背包满溢出的战斗装备掉落转个人邮件(W5 ④+,nil-safe)。
	// nil = mail_addr 未配 → 背包满掉落留在出箱轮询重试(退化为历史行为,不丢)。
	mailSender MailSender

	// expGranter 把实时进度通道的击杀经验幂等入账到 player(实时成长,nil-safe)。
	// nil = player_addr 未配 → RunProgressPublisher 跳过经验行,出箱积压不丢。
	expGranter ExperienceGranter

	// monsterExp 怪物击杀经验查表(configtable role_level;数值权威见 §9.6)。
	// **非 nil-safe**:nil 时击杀事实按可重试错误拒收,绝不"跳过并推进水位"(会永久丢经验)。
	monsterExp MonsterExpTable

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分区,结算回流落点观测退化为不打日志(行为不变)。
	// 多 Region 部署(阶段 3)由 main 经 SetCellRouter 注入,ReportResult 落库后额外打一条
	// 跨 region 回流落点观测(overflow 对局 region_count>1 → 需多 region 回流)。nil-safe。
	router *cellroute.Router
}

// NewBattleResultUsecase 构造。pusher 可为 nil:player.update 已写事务出箱,
// pusher/producer 不可用时出箱积压不丢,等 producer 可用后由发布器补发(当前需重启/重配)。
// releaser 可为 nil:matchmaker 地址未配时 outbox 持久积压，配置恢复并重启后继续发送。
func NewBattleResultUsecase(repo data.BattleRepo, mmr MMRReader, pusher PlayerUpdatePusher, releaser MatchReleaser, cfg conf.BattleConf) *BattleResultUsecase {
	if mmr == nil {
		mmr = NewStaticMMRReader(cfg.BaseMMR)
	}
	return &BattleResultUsecase{repo: repo, mmr: mmr, pusher: pusher, releaser: releaser, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),ReportResult 不做结算回流落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction 一致)。Router 内部读路径无锁,并发安全。
func (u *BattleResultUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetInstanceGranter 注入 inventory 装备掉落发放器(W5 ④,nil-safe)。
//
// 用 setter 而非构造参数,保持 NewBattleResultUsecase 签名不变(与 SetCellRouter 一致)。
// nil / 不调用 = inventory_addr 未配 → RunDropPublisher 不启动,掉落出箱积压不丢。
func (u *BattleResultUsecase) SetInstanceGranter(g InstanceGranter) {
	u.granter = g
}

// SetMailSender 注入背包满溢出转邮件发送器(W5 ④+,nil-safe)。
//
// nil / 不调用 = mail_addr 未配 → 背包满掉落留在出箱轮询重试(退化为历史行为,不丢)。
// 用 setter 而非构造参数,保持 NewBattleResultUsecase 签名不变(与 SetInstanceGranter 一致)。
func (u *BattleResultUsecase) SetMailSender(m MailSender) {
	u.mailSender = m
}

// SetTerminalReleaseRelay 注入 Model-B 正常结算终态回收 relay。
// authority_mode=redis 的 main 在 schema probe 成功后必须注入；nil 时 worker 不启动，
// 但已存在 outbox 行绝不会被删除。
func (u *BattleResultUsecase) SetTerminalReleaseRelay(relay TerminalReleaseRelay) {
	u.terminalRelay = relay
}

// ── ReportResult:幂等落库 + MMR ─────────────────────────────────────────────

// ReportResult 落一场对局结算(消费 battle.result / 同步 RPC 共用)。
// 返回 alreadyRecorded:true 表示幂等命中,本次跳过(不算错误)。
// finalProgressSeq 是 DS 上报的实时进度对账水位(0 = 未走实时通道;legacy kafka 路径恒 0)。
func (u *BattleResultUsecase) ReportResult(ctx context.Context, result *battlev1.BattleResult, finalProgressSeq uint64) (bool, error) {
	return u.reportResult(ctx, result, nil, finalProgressSeq)
}

// ReportAuthorizedResult 是 Redis-authority 同步入口。terminalRelease 必须来自 service
// 已完成 Guard + active 校验的服务端快照；它与战绩同事务提交，不从 BattleResult 请求体补值。
// roster 精确校验通过后,reportResult 会先用 terminalRelease 携带的 canonical
// game_mode/map_id 覆盖请求体,再做 MMR 决策与落库(DS 伪报 game_mode/map_id 无效)。
func (u *BattleResultUsecase) ReportAuthorizedResult(
	ctx context.Context,
	result *battlev1.BattleResult,
	terminalRelease data.TerminalReleaseRecord,
	finalProgressSeq uint64,
) (bool, error) {
	if err := validateAuthorizedResultRoster(result, terminalRelease.PlayerIDs); err != nil {
		return false, err
	}
	return u.reportResult(ctx, result, &terminalRelease, finalProgressSeq)
}

// validateAuthorizedResultRoster binds every settlement side effect to the
// canonical BattleStorageRecord roster captured by the credential checker.
// Equality is set-based because stat order is not an authority signal, while
// duplicate stats, omissions and outsiders are all rejected before MMR/DB work.
func validateAuthorizedResultRoster(result *battlev1.BattleResult, authoritative []uint64) error {
	if result == nil || len(authoritative) == 0 || len(result.GetStats()) != len(authoritative) {
		return errcode.New(errcode.ErrUnauthorized, "battle result roster does not match authority")
	}
	want := make(map[uint64]struct{}, len(authoritative))
	for _, playerID := range authoritative {
		if playerID == 0 {
			return errcode.New(errcode.ErrUnauthorized, "battle authority roster is invalid")
		}
		if _, duplicate := want[playerID]; duplicate {
			return errcode.New(errcode.ErrUnauthorized, "battle authority roster is invalid")
		}
		want[playerID] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(result.GetStats()))
	for _, stat := range result.GetStats() {
		playerID := stat.GetPlayerId()
		if playerID == 0 {
			return errcode.New(errcode.ErrUnauthorized, "battle result roster contains an invalid player")
		}
		if _, duplicate := seen[playerID]; duplicate {
			return errcode.New(errcode.ErrUnauthorized, "battle result roster contains a duplicate player")
		}
		if _, member := want[playerID]; !member {
			return errcode.New(errcode.ErrUnauthorized, "battle result roster contains an unauthorized player")
		}
		seen[playerID] = struct{}{}
	}
	return nil
}

// canonicalGameModePVECoop 是 PVE walk-in 部署的 canonical game_mode
// (decision-dungeon-entry-modes.md §4:matchmaker-pve game_mode: "pve_coop")。
// 只允许与 TerminalReleaseRecord.GameMode(canonical BattleStorageRecord 来源)比较,
// 绝不与 DS 请求体 result.game_mode 比较(§9.6 DS 请求字段不可作 MMR 安全依据)。
const canonicalGameModePVECoop = "pve_coop"

func (u *BattleResultUsecase) reportResult(ctx context.Context, result *battlev1.BattleResult, terminalRelease *data.TerminalReleaseRecord, finalProgressSeq uint64) (bool, error) {
	if result == nil || result.GetMatchId() == 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if len(result.GetStats()) == 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "stats required for match %d", result.GetMatchId())
	}

	// 权威字段覆盖(§9.6 数值不信 DS):授权同步路径(terminalRelease 非空)下,
	// game_mode/map_id 一律以 canonical BattleStorageRecord(经 AuthorizeResult 随
	// terminalRelease 带入)为准,在任何 MMR/DB/outbox 副作用之前覆盖 DS 请求体。
	// canonical game_mode 为空(滚动升级前旧局)也照覆盖为空:宁可少存元数据,
	// 也不把不可信请求字段伪装成权威事实。legacy/内部路径(terminalRelease==nil)
	// 无权威可用,保持请求原值(该路径本就不受 canonical 保护,行为不变)。
	if terminalRelease != nil {
		result.GameMode = terminalRelease.GameMode
		result.MapId = terminalRelease.MapID
	}

	// 正常结算:outcome 缺省补 NORMAL
	if result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_UNSPECIFIED {
		result.Outcome = battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL
	}

	// MMR 仅对正常结算计算(不变量 §6,覆盖 DS 上报的 mmr_delta)。
	// ABANDONED 是补偿语义:权威路径是 ds.lifecycle → HandleAbandoned(delta 全 0,不掉段)。
	// 此处兜底:若 battle.result 误报 / 伪造 Outcome=ABANDONED,强制 delta 全 0,
	// 防止 DS 不可信地通过 abandoned 改玩家段位(不变量 §4/§6)。
	//
	// canonical pve_coop(仅授权路径可判定)永不算 Elo:PVE 副本没有对手结构,
	// 成功/失败/平局的 NORMAL 结算一律 mmr_delta=0,且不触碰 MMR reader。
	// 判定依据只能是 terminalRelease.GameMode(canonical);canonical 为空(旧局)
	// 保持保守旧行为照算 Elo,绝不用 DS 请求 game_mode 降级跳过 MMR。
	switch {
	case result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED:
		for _, s := range result.GetStats() {
			s.MmrDelta = 0
		}
	case terminalRelease != nil && terminalRelease.GameMode == canonicalGameModePVECoop:
		for _, s := range result.GetStats() {
			s.MmrDelta = 0
		}
	default:
		u.assignMMR(ctx, result)
	}

	// MMR 算完才组装出箱(携带最终 mmr_delta);与落库同事务原子提交(不变量 §4)。
	abandoned := result.GetOutcome() == battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED
	if abandoned && terminalRelease != nil {
		return false, errcode.New(errcode.ErrInvalidArg, "completed terminal release proof cannot settle abandoned match %d", result.GetMatchId())
	}
	if terminalRelease != nil {
		if err := prepareTerminalRelease(result, terminalRelease, u.cfg.TerminalReleaseGrace.Std()); err != nil {
			return false, err
		}
	}
	outbox, err := u.buildOutbox(result, abandoned)
	if err != nil {
		return false, err
	}

	// 战斗装备掉落出箱(W5 ④):正常结算才发放;ABANDONED(DS 崩溃补偿)不产出掉落。
	// DS 上报的 dropped_item_config_ids 按 drop 白名单过滤(DS 不可信),与落库同事务提交。
	var dropOutbox []data.DropOutboxRecord
	if !abandoned {
		dropOutbox = u.buildDropOutbox(ctx, result)
	}

	already, settleInfo, err := u.repo.SaveResult(ctx, result, outbox, dropOutbox, terminalRelease, finalProgressSeq)
	if err != nil {
		return false, err
	}
	if already {
		plog.With(ctx).Infow("msg", "battle_result_idempotent_hit", "match_id", result.GetMatchId())
		return true, nil
	}

	plog.With(ctx).Debugw("msg", "battle_result_recorded",
		"match_id", result.GetMatchId(), "winner_team", result.GetWinnerTeam(),
		"outcome", result.GetOutcome().String(), "players", len(result.GetStats()))

	// 实时进度通道对账 + 单一权威路径观测(realtime-progression.md §5):
	// 掉落发放权已归实时通道时,结算上报的 dropped_item_config_ids 只作审计不再发放。
	reconcileProgress(ctx, result.GetMatchId(), finalProgressSeq, settleInfo)
	if settleInfo.DropsSuppressed && len(dropOutbox) > 0 {
		plog.With(ctx).Infow("msg", "battle_drop_suppressed_by_progress",
			"match_id", result.GetMatchId(), "audit_rows", len(dropOutbox),
			"hint", "本场掉落已经实时通道逐事件发放,结算掉落字段仅审计")
	}

	// 多 region:观测本局结算回流落点分布(overflow 对局 region_count>1 → 需回流多 region)。
	// router 为 nil(单 Cell)→ 不打,行为不变;跨 region 桥 / 多 region topic 回流路径属 infra(§11.1)。
	u.logSettlementRouting(ctx, result)

	// 不在同步响应路径回收 DS。Model-B 的 terminal_release_outbox 会等待宽限窗，让 DS
	// 收到 OK 并通知客户端；响应丢失/令牌过期也会在宽限后由服务端可靠回收。
	return false, nil
}

func prepareTerminalRelease(result *battlev1.BattleResult, rec *data.TerminalReleaseRecord, grace time.Duration) error {
	if result == nil || rec == nil || rec.MatchID == 0 || rec.MatchID != result.GetMatchId() ||
		rec.AllocationID == "" || rec.DSPodName == "" || rec.DSPodName != result.GetDsPodName() ||
		rec.GameserverUID == "" || rec.InstanceEpoch == 0 || rec.AuthGen == 0 ||
		rec.AuthJTI == "" || rec.AuthExpMs <= 0 || rec.AuthKid == "" ||
		rec.AuthTokenSHA256 == "" || rec.AuthWriterEpoch != auth.DSAuthWriterEpochV2 ||
		rec.AuthorizedAtMs <= 0 || rec.AuthorizedAtMs >= rec.AuthExpMs || rec.ReleasedAtMs != 0 ||
		len(rec.PlayerIDs) == 0 {
		return errcode.New(errcode.ErrUnauthorized, "terminal release proof is incomplete or not bound to result")
	}
	if grace < 5*time.Second || grace > 2*time.Minute {
		return errcode.New(errcode.ErrInvalidState, "terminal release grace is outside [5s,2m]")
	}
	nowMs := time.Now().UnixMilli()
	if rec.AuthorizedAtMs > nowMs {
		return errcode.New(errcode.ErrUnauthorized, "terminal release authorization time is in the future")
	}
	rec.ReleaseAfterMs = nowMs + grace.Milliseconds()
	rec.ReleasedAtMs = 0 // 只允许 phase1 worker 经 MySQL CAS 推进，调用方不能伪造。
	rec.CreatedAtMs = 0  // MySQL writer owns created_at_ms; caller不能伪造。
	return nil
}

// ── HandleAbandoned:DS 崩溃补偿 ───────────────────────────────────────────────

// HandleAbandoned 处理 ds_allocator 发来的 ABANDONED 事件(不变量 §4)。
// 写一条 outcome=ABANDONED、mmr_delta 全 0 的补偿记录(幂等),并通知 player 段位回滚。
func (u *BattleResultUsecase) HandleAbandoned(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32, gameMode string, tsMs int64) error {
	if matchID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if tsMs <= 0 {
		tsMs = time.Now().UnixMilli()
	}

	stats := make([]*battlev1.PlayerStats, 0, len(playerIDs))
	for _, pid := range playerIDs {
		stats = append(stats, &battlev1.PlayerStats{PlayerId: pid, MmrDelta: 0})
	}
	result := &battlev1.BattleResult{
		MatchId:    matchID,
		EndedAtMs:  tsMs,
		WinnerTeam: winnerTeamDraw,
		Outcome:    battlev1.BattleOutcome_BATTLE_OUTCOME_ABANDONED,
		GameMode:   gameMode,
		MapId:      mapID,
		Stats:      stats,
	}

	// 出箱携 delta=0(不掉段)+ reason=abandon;与补偿记录同事务提交。
	outbox, err := u.buildOutbox(result, true)
	if err != nil {
		return err
	}

	// ABANDONED 同样收口实时进度水位(finalProgressSeq=0):打终局标记后,分区恢复的
	// 僵尸 DS 再上报进度一律拒;崩溃前已入账的经验 / 掉落按需求保留不回滚。
	already, _, err := u.repo.SaveResult(ctx, result, outbox, nil, nil, 0)
	if err != nil {
		return err
	}
	if already {
		// 已有正常结算或已补偿过 → 不重复(不变量 §2)
		plog.With(ctx).Infow("msg", "abandoned_idempotent_hit", "match_id", matchID)
		return nil
	}
	plog.With(ctx).Infow("msg", "battle_abandoned_recorded", "match_id", matchID, "players", len(playerIDs))

	// matchmaker 释放由同事务 match_release_outbox 异步可靠推进。这里不调 releaseDS：
	// ABANDONED 事件来自 ds_allocator sweep，它已经回收 pod 并保留诊断镜像。
	return nil
}

// ── 查询 RPC ──────────────────────────────────────────────────────────────────

// GetMatchResult 读一场对局结算。
func (u *BattleResultUsecase) GetMatchResult(ctx context.Context, matchID uint64) (*battlev1.BattleResult, bool, error) {
	if matchID == 0 {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	return u.repo.GetResult(ctx, matchID)
}

// ListPlayerHistory 倒序列出玩家战绩历史。
func (u *BattleResultUsecase) ListPlayerHistory(ctx context.Context, playerID uint64, limit int, beforeMs int64) ([]*battlev1.BattleResult, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.ListPlayerHistory(ctx, playerID, limit, beforeMs)
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// assignMMR 按两队当前 MMR 均值算 Elo delta,写回每个 stat.MmrDelta(不变量 §6)。
func (u *BattleResultUsecase) assignMMR(ctx context.Context, result *battlev1.BattleResult) {
	var sum0, n0, sum1, n1 int
	for _, s := range result.GetStats() {
		m, err := u.mmr.GetMMR(ctx, s.GetPlayerId())
		if err != nil {
			m = u.cfg.BaseMMR
			plog.With(ctx).Warnw("msg", "mmr_read_failed_fallback_base", "player_id", s.GetPlayerId(), "err", err)
		}
		if s.GetTeam() == winnerTeamA {
			sum0 += m
			n0++
		} else {
			sum1 += m
			n1++
		}
	}
	avgA := u.cfg.BaseMMR
	if n0 > 0 {
		avgA = sum0 / n0
	}
	avgB := u.cfg.BaseMMR
	if n1 > 0 {
		avgB = sum1 / n1
	}
	deltaA, deltaB := eloDeltas(avgA, avgB, u.cfg.EloKFactor, result.GetWinnerTeam())
	for _, s := range result.GetStats() {
		if s.GetTeam() == winnerTeamA {
			s.MmrDelta = int32(deltaA)
		} else {
			s.MmrDelta = int32(deltaB)
		}
	}
}

// buildOutbox 把一场结算的每个玩家组装成 player.update 出箱记录(待发布,与落库同事务)。
//
//	abandoned=true → reason 全 "abandon"(delta 已置 0,不掉段)
//	abandoned=false → 按胜负 win/lose/draw
func (u *BattleResultUsecase) buildOutbox(result *battlev1.BattleResult, abandoned bool) ([]data.OutboxRecord, error) {
	recs := make([]data.OutboxRecord, 0, len(result.GetStats()))
	for _, s := range result.GetStats() {
		reason := "abandon"
		if !abandoned {
			reason = reasonForTeam(s.GetTeam(), result.GetWinnerTeam())
		}
		evt := &playerv1.PlayerUpdateEvent{
			PlayerId: s.GetPlayerId(),
			MatchId:  result.GetMatchId(),
			MmrDelta: s.GetMmrDelta(),
			Reason:   reason,
			TsMs:     result.GetEndedAtMs(),
		}
		payload, err := proto.Marshal(evt)
		if err != nil {
			return nil, errcode.New(errcode.ErrInternal, "marshal player.update player=%d: %v", s.GetPlayerId(), err)
		}
		recs = append(recs, data.OutboxRecord{PlayerID: s.GetPlayerId(), Payload: payload})
	}
	return recs, nil
}

// buildDropOutbox 把一场结算里每个玩家的战斗装备掉落组装成 drop 出箱记录(与落库同事务,W5 ④)。
//
// DS 不可信:逐条按 cfg.IsDroppable(drop 白名单)过滤 DS 上报的 dropped_item_config_ids,
// 只有落在白名单内的 item_config_id 才入出箱发放;白名单为空 → 全过滤掉(不发放,安全默认)。
// 每玩家最多保留 cfg.MaxDropsPerPlayer() 条(超限截断记 Warn):防异常/恶意 DS 重复上报
// 海量白名单 ID 撑爆 battle_drop_outbox.item_config_ids VARCHAR(512) 导致整场结算回滚。
// 无任何白名单内掉落的玩家不产出出箱行。
// ctx 必须是**请求 ctx**(不是 context.Background):本函数的两条告警是「异常/恶意 DS 上报」
// 信号,必须能按 trace_id 关联回具体 ReportResult 调用链(不变量 §9.8 所有写都要带 trace_id)。
func (u *BattleResultUsecase) buildDropOutbox(ctx context.Context, result *battlev1.BattleResult) []data.DropOutboxRecord {
	maxDrops := u.cfg.MaxDropsPerPlayer()
	recs := make([]data.DropOutboxRecord, 0, len(result.GetStats()))
	for _, s := range result.GetStats() {
		reported := s.GetDroppedItemConfigIds()
		if len(reported) == 0 {
			continue
		}
		capHint := len(reported)
		if capHint > maxDrops {
			capHint = maxDrops
		}
		allowed := make([]uint32, 0, capHint)
		truncated := false
		for _, id := range reported {
			if id != 0 && u.cfg.IsDroppable(id) {
				if len(allowed) >= maxDrops {
					truncated = true
					break
				}
				allowed = append(allowed, id)
			}
		}
		if truncated {
			// 超过每玩家上限 → 截断丢弃并 Warn(大概率是异常/恶意 DS,不能让它打失败整场结算)。
			plog.With(ctx).Warnw("msg", "battle_drop_truncated",
				"match_id", result.GetMatchId(), "player_id", s.GetPlayerId(),
				"reported", len(reported), "kept", len(allowed), "max", maxDrops)
		}
		if len(allowed) == 0 {
			// DS 上报了掉落但全不在白名单 → 记一条 Warn(可能是配置漏项或 DS 越权尝试)。
			plog.With(ctx).Warnw("msg", "battle_drop_all_filtered",
				"match_id", result.GetMatchId(), "player_id", s.GetPlayerId(), "reported", len(reported))
			continue
		}
		recs = append(recs, data.DropOutboxRecord{PlayerID: s.GetPlayerId(), ItemConfigIDs: allowed})
	}
	return recs
}

// ── player.update 事务出箱发布器(W4 ⑨,不变量 §4)─────────────────────────────

// RunOutboxPublisher 启动后台 player.update 出箱发布循环,直到 ctx 取消。
//
// 每轮取一批待发布出箱行(FIFO 按 id),逐条投递 Kafka;投递成功才删行。投递失败 →
// 本批中断、保留出箱行,下一轮重试(同玩家 key 有序,不变量 §9)。配合 player 服务幂等
// 消费(W4 ④ mmr_history uk),整条段位写链是 at-least-once 可靠闭环,可穿越 Kafka 临时不可用。
func (u *BattleResultUsecase) RunOutboxPublisher(ctx context.Context) {
	interval := u.cfg.OutboxPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "outbox_publisher_started", "interval", interval.String(), "batch", u.outboxBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "outbox_publisher_stopped")
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,出箱行保留下轮重试。
			safego.Run(ctx, "battle_outbox_publisher", func() {
				if n, err := u.publishOutboxBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "outbox_publish_batch_failed", "published", n, "err", err)
				}
			})
		}
	}
}

// publishOutboxBatch 取一批出箱记录投递,返回本轮成功投递并删除的条数。
// 投递失败立即中断本轮(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递。
func (u *BattleResultUsecase) publishOutboxBatch(ctx context.Context) (int, error) {
	if u.pusher == nil {
		// kafka 未配置:出箱无法投递。出箱行已落库不丢,等 producer 可用后重启再发。
		return 0, nil
	}
	recs, err := u.repo.FetchOutbox(ctx, u.outboxBatchSize())
	if err != nil {
		return 0, err
	}
	published := 0
	for _, r := range recs {
		if perr := u.pusher.PushPlayerUpdate(ctx, r.PlayerID, r.Payload); perr != nil {
			return published, perr // 本轮中断,保留出箱行下轮重试
		}
		if derr := u.repo.DeleteOutbox(ctx, r.ID); derr != nil {
			return published, derr
		}
		published++
	}
	if published > 0 {
		plog.With(ctx).Debugw("msg", "outbox_published", "count", published)
	}
	return published, nil
}

// outboxBatchSize 返回每轮发布批大小(配置缺省 128)。
func (u *BattleResultUsecase) outboxBatchSize() int {
	if u.cfg.OutboxBatchSize > 0 {
		return u.cfg.OutboxBatchSize
	}
	return 128
}

// ── matchmaker release 事务出箱 ──────────────────────────────────────────────

func matchReleaseRetryDelay(attempt uint32) time.Duration {
	shift := attempt
	if shift > 6 {
		shift = 6
	}
	d := time.Second * time.Duration(1<<shift)
	if d > time.Minute {
		return time.Minute
	}
	return d
}

// RunMatchReleasePublisher 可靠释放 matchmaker 的 ticket/claim/match 状态。SaveResult
// 与 outbox 同事务；RPC 失败或响应未知只延期，明确成功后才删除行。
func (u *BattleResultUsecase) RunMatchReleasePublisher(ctx context.Context) {
	interval := u.cfg.OutboxPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "match_release_publisher_started", "interval", interval.String(), "batch", u.outboxBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "match_release_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_match_release_publisher", func() {
				if n, err := u.publishMatchReleaseBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "match_release_batch_failed", "released", n, "err", err)
				}
			})
		}
	}
}

func (u *BattleResultUsecase) publishMatchReleaseBatch(ctx context.Context) (int, error) {
	if u.releaser == nil {
		return 0, nil
	}
	nowMs := time.Now().UnixMilli()
	recs, err := u.repo.FetchMatchReleaseOutbox(ctx, u.outboxBatchSize(), nowMs)
	if err != nil {
		return 0, err
	}
	released := 0
	var joined error
	for _, rec := range recs {
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		rerr := u.releaser.ReleaseMatch(callCtx, rec.MatchID, rec.PlayerIDs)
		cancel()
		if rerr != nil {
			nextMs := time.Now().Add(matchReleaseRetryDelay(rec.AttemptCount)).UnixMilli()
			if derr := u.repo.DeferMatchReleaseOutbox(ctx, rec.ID, nextMs); derr != nil {
				joined = errors.Join(joined, rerr, derr)
			} else {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if derr := u.repo.DeleteMatchReleaseOutbox(ctx, rec.ID); derr != nil {
			joined = errors.Join(joined, derr)
			continue
		}
		released++
	}
	return released, joined
}

// ── Battle terminal-release 事务出箱(Model-B)────────────────────────────────

// RunTerminalReleasePublisher 启动正常结算资源回收 worker。它只能在 MySQL schema
// probe、relay 构造和 dsauth capability 获取全部成功后启动。单行失败保留重试，不阻塞
// 同批其它对局；UID precondition 与 ds_allocator Redis CAS 保证多副本/响应丢失幂等。
func (u *BattleResultUsecase) RunTerminalReleasePublisher(ctx context.Context) {
	if u.terminalRelay == nil {
		plog.With(ctx).Infow("msg", "terminal_release_publisher_disabled")
		return
	}
	interval := u.cfg.TerminalReleaseInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "terminal_release_publisher_started",
		"interval", interval.String(), "batch", u.terminalReleaseBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "terminal_release_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_terminal_release_publisher", func() {
				if n, err := u.publishTerminalReleaseBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "terminal_release_batch_failed", "finalized", n, "err", err)
				}
			})
		}
	}
}

func (u *BattleResultUsecase) publishTerminalReleaseBatch(ctx context.Context) (int, error) {
	if u.terminalRelay == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchTerminalReleaseOutbox(ctx, u.terminalReleaseBatchSize(), time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	finalized := 0
	for _, rec := range recs {
		if rec.ReleasedAtMs < 0 {
			return finalized, errcode.New(errcode.ErrInvalidState,
				"terminal release outbox id=%d has invalid released_at_ms", rec.ID)
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if rec.ReleasedAtMs == 0 {
			err := u.terminalRelay.ReleaseTerminal(callCtx, rec)
			cancel()
			if err != nil {
				// Redis/K8s unknown 绝不能推进 DB phase；永久墓碑/原始行保留重试。
				plog.With(ctx).Warnw("msg", "terminal_release_phase1_failed",
					"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
					"pod", rec.DSPodName, "err", err)
				continue
			}
			marked, err := u.repo.MarkTerminalReleaseReleased(ctx, rec.ID, time.Now().UnixMilli())
			if err != nil {
				// UID delete 已成功但 durable ACK 未知：phase1 绝不 expire Redis。
				// 下轮按 DB 真实状态重读；0 则重放 UID delete，>0 则进入 finalize。
				return finalized, err
			}
			if !marked {
				plog.With(ctx).Debugw("msg", "terminal_release_phase1_already_advanced",
					"match_id", rec.MatchID, "outbox_id", rec.ID)
			}
			continue
		}

		err := u.terminalRelay.FinalizeTerminal(callCtx, rec)
		cancel()
		if err != nil {
			// finalize 响应未知绝不能 DELETE released 行；重试只校验/Expire 同 proof，
			// 绝不再次删除 K8s。若 TTL 已自然清空全部墓碑，服务端按幂等成功返回。
			plog.With(ctx).Warnw("msg", "terminal_release_finalize_failed",
				"match_id", rec.MatchID, "allocation_id", rec.AllocationID,
				"pod", rec.DSPodName, "err", err)
			continue
		}
		if err := u.repo.DeleteTerminalReleaseOutbox(ctx, rec.ID); err != nil {
			// finalize 已成功但 DB delete 失败：released 行保留。下一轮只重放
			// finalize；即使墓碑 TTL 已过、三键都不存在，也会幂等重认成功。
			return finalized, err
		}
		finalized++
	}
	if finalized > 0 {
		plog.With(ctx).Debugw("msg", "terminal_release_outbox_finalized", "count", finalized)
	}
	return finalized, nil
}

func (u *BattleResultUsecase) terminalReleaseBatchSize() int {
	if u.cfg.TerminalReleaseBatchSize > 0 {
		return u.cfg.TerminalReleaseBatchSize
	}
	return 128
}

// ── 战斗装备掉落事务出箱发布器(W5 ④,at-least-once + GrantInstances 幂等)──────────

// RunDropPublisher 启动后台战斗装备掉落出箱发放循环,直到 ctx 取消。
//
// 每轮取一批 drop 出箱行,逐行调 inventory.GrantInstances(幂等键 battle_drop:{match_id}:{player_id}),
// 成功才删行。与 player.update 出箱不同:掉落无跨玩家保序需求 → 单行失败不中断本轮(continue),
// 只把失败行留到下轮重试(避免某玩家背包满时阻塞其他玩家)。配合 GrantInstances 幂等,
// 整条掉落写链是 at-least-once 可靠闭环,可穿越 inventory 临时不可用。
//
// granter==nil(inventory_addr 未配)→ 直接返回不启动;drop 出箱积压不丢,等地址配好重启补发。
func (u *BattleResultUsecase) RunDropPublisher(ctx context.Context) {
	if u.granter == nil {
		plog.With(ctx).Infow("msg", "drop_publisher_disabled", "hint", "inventory_addr 未配置 → 战斗装备掉落不发放(出箱积压不丢)")
		return
	}
	interval := u.cfg.DropPublishInterval.Std()
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "drop_publisher_started", "interval", interval.String(), "batch", u.dropBatchSize())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "drop_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_drop_publisher", func() {
				if n, err := u.publishDropBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "drop_publish_batch_failed", "granted", n, "err", err)
				}
			})
		}
	}
}

// publishDropBatch 取一批掉落出箱行发放,返回本轮成功发放并删除的条数。
// 单行发放失败仅记录并 continue(保留出箱行下轮重试),不阻塞其他玩家掉落。
func (u *BattleResultUsecase) publishDropBatch(ctx context.Context) (int, error) {
	if u.granter == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchDropOutbox(ctx, u.dropBatchSize())
	if err != nil {
		return 0, err
	}
	granted := 0
	for _, r := range recs {
		key := dropIdempotencyKey(r.MatchID, r.PlayerID)
		gerr := u.granter.GrantInstances(ctx, r.PlayerID, r.ItemConfigIDs, key)
		if gerr != nil {
			// 背包满且已配 mail:转个人邮件溢出(传相同源键 key,领取时 GrantInstances 同键去重
			// → 直发链与邮件链至多一次),成功后删出箱行,不再无休止重试。
			if u.mailSender != nil && errcode.As(gerr) == errcode.ErrInventoryCapacityFull {
				if merr := u.mailSender.SendOverflowMail(ctx, r.PlayerID, r.ItemConfigIDs, key); merr != nil {
					// 转邮件失败 → 保留出箱行下轮重试(不丢),不阻塞其他玩家。
					plog.With(ctx).Warnw("msg", "drop_overflow_mail_failed", "player_id", r.PlayerID, "items", len(r.ItemConfigIDs), "err", merr)
					continue
				}
				plog.With(ctx).Infow("msg", "drop_overflow_mailed", "player_id", r.PlayerID, "items", len(r.ItemConfigIDs))
				if derr := u.repo.DeleteDropOutbox(ctx, r.ID); derr != nil {
					return granted, derr
				}
				granted++
				continue
			}
			// 其他失败(inventory 临时不可用等)/ 未配 mail 的背包满 → 保留出箱行下轮重试,不阻塞其他玩家。
			plog.With(ctx).Warnw("msg", "drop_grant_failed", "player_id", r.PlayerID, "items", len(r.ItemConfigIDs), "err", gerr)
			continue
		}
		if derr := u.repo.DeleteDropOutbox(ctx, r.ID); derr != nil {
			return granted, derr
		}
		granted++
	}
	if granted > 0 {
		plog.With(ctx).Debugw("msg", "drop_outbox_granted", "count", granted)
	}
	return granted, nil
}

// dropBatchSize 返回每轮掉落发放批大小(配置缺省 128)。
func (u *BattleResultUsecase) dropBatchSize() int {
	if u.cfg.DropBatchSize > 0 {
		return u.cfg.DropBatchSize
	}
	return 128
}

// dropIdempotencyKey 组装战斗装备掉落幂等键:battle_drop:{match_id}:{player_id}。
// 同对局同玩家的掉落只入账一次(GrantInstances 幂等去重,资产不变量)。
func dropIdempotencyKey(matchID, playerID uint64) string {
	return "battle_drop:" + strconv.FormatUint(matchID, 10) + ":" + strconv.FormatUint(playerID, 10)
}
