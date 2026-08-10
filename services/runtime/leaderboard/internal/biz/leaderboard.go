// Package biz 是 leaderboard 服务的业务逻辑层(通用排行榜,2026-06-27)。
//
// 职责(docs/design/decision-revisit-leaderboard.md):
//   - SubmitScore:按 mode(SET_IF_HIGHER / SET / INCREMENT)写 Redis ZSET,首次按 Options 建榜
//     (TTL 临时榜 / max_size 截断 / 时间 tie-break);
//   - 读查询(GetRank / GetRange / GetAround)按榜 meta(ascending)选排序方向,只回客户端可见结构;
//   - SettleBoard:取 Top-N → 落 MySQL 快照 + 批次(uk 防重复结算)→ 按 RewardTable 幂等发奖
//     (调 inventory.GrantItems,uk grant_idem 防重复发奖)+ 发 kafka 事件 → 可选 reset。
//
// 写入(Submit / Settle / Remove / Delete)是系统接口,鉴权由 service 层 / 内网边界保证。
package biz

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	leaderboardv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/leaderboard/v1"

	"github.com/luyuancpp/pandora/services/runtime/leaderboard/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/leaderboard/internal/data"
)

// RewardGranter 抽象「按玩家幂等发奖」(结算时调,弱依赖)。
type RewardGranter interface {
	Grant(ctx context.Context, playerID uint64, idemKey string, items []data.RewardGrant) error
}

// NoopRewardGranter 是占位实现:发奖总成功(不真实入账)。用于无背包联调 / 单测。
type NoopRewardGranter struct{}

// Grant 永远成功(占位)。
func (NoopRewardGranter) Grant(_ context.Context, _ uint64, _ string, _ []data.RewardGrant) error {
	return nil
}

// SettleEventPusher 把结算事件转发 kafka(main.go 注入;弱依赖,nil 静默)。
type SettleEventPusher interface {
	PushSettle(ctx context.Context, settlementID uint64, b data.BoardKey, winners []*leaderboardv1.LeaderboardEntry) error
}

// snowflakeGen 是 snowflake.Node 的最小接口。
type snowflakeGen interface {
	Generate() uint64
}

// LeaderboardUsecase 是 leaderboard 服务业务逻辑核心。
type LeaderboardUsecase struct {
	repo    data.LeaderboardRepo
	board   data.BoardStore
	granter RewardGranter
	events  SettleEventPusher // 弱依赖,可为 nil
	sf      snowflakeGen
	cfg     conf.LeaderboardConf
}

// NewLeaderboardUsecase 构造。granter 为 nil 时退化为 Noop;events 允许 nil。
func NewLeaderboardUsecase(repo data.LeaderboardRepo, board data.BoardStore, granter RewardGranter, events SettleEventPusher, sf snowflakeGen, cfg conf.LeaderboardConf) *LeaderboardUsecase {
	if granter == nil {
		granter = NoopRewardGranter{}
	}
	if cfg.DefaultListLimit <= 0 {
		cfg.DefaultListLimit = 50
	}
	if cfg.MaxListLimit <= 0 {
		cfg.MaxListLimit = 200
	}
	if cfg.DefaultAroundRadius <= 0 {
		cfg.DefaultAroundRadius = 10
	}
	if cfg.DefaultSettleTopN <= 0 {
		cfg.DefaultSettleTopN = 100
	}
	if cfg.DefaultEstimateBucketWidth <= 0 {
		cfg.DefaultEstimateBucketWidth = data.DefaultEstimateBucketWidth
	}
	return &LeaderboardUsecase{repo: repo, board: board, granter: granter, events: events, sf: sf, cfg: cfg}
}

// validateBoard 校验 BoardKey 合法性。
func validateBoard(b data.BoardKey) error {
	if b.BoardType == 0 {
		return errcode.New(errcode.ErrLeaderboardInvalidBoard, "board_type required")
	}
	if b.Scope < data.ScopeGlobal || b.Scope > data.ScopeCustom {
		return errcode.New(errcode.ErrLeaderboardInvalidBoard, "invalid scope %d", int32(b.Scope))
	}
	return nil
}

// nowMs 返回当前毫秒。
func nowMs() int64 { return time.Now().UnixMilli() }

// SubmitScore 写入分数。
func (u *LeaderboardUsecase) SubmitScore(ctx context.Context, b data.BoardKey, entityID uint64, score int64, mode int32, opt data.Options) (newScore, rank int64, err error) {
	if err := validateBoard(b); err != nil {
		return 0, 0, err
	}
	if entityID == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "entity_id required")
	}
	if mode < data.ModeSetIfHigher || mode > data.ModeIncrement {
		mode = data.ModeSetIfHigher
	}
	if opt.EstimateBucketWidth <= 0 {
		opt.EstimateBucketWidth = u.cfg.DefaultEstimateBucketWidth
	}
	return u.board.Submit(ctx, b, entityID, score, mode, opt, nowMs())
}

// boardAscending 读榜排序方向(meta 缺失默认降序)。found=false 表示榜不存在。
func (u *LeaderboardUsecase) boardAscending(ctx context.Context, b data.BoardKey) (ascending, found bool, err error) {
	asc, _, exists, gerr := u.board.GetMeta(ctx, b)
	if gerr != nil {
		return false, false, gerr
	}
	return asc, exists, nil
}

// RankView 是 GetRank 的返回:精确名次或(未进精确榜时)直方图区间估算名次。
type RankView struct {
	Entry data.Entry
	Found bool
	// Estimated true=Entry.Rank 是榜外区间估算(约值),Entry.UpdatedAtMs 恒 0。
	Estimated bool
	// TotalSubmitters 直方图口径参与总人数(仅 Estimated=true 时填,供百分位展示)。
	TotalSubmitters int64
}

// GetRank 查某 entity 名次:精确榜命中返回精确名次;被 max_size 截断出榜的玩家回退
// 直方图区间估算(约值);从未上报 → Found=false。
func (u *LeaderboardUsecase) GetRank(ctx context.Context, b data.BoardKey, entityID uint64) (RankView, error) {
	if err := validateBoard(b); err != nil {
		return RankView{}, err
	}
	asc, _, err := u.boardAscending(ctx, b)
	if err != nil {
		return RankView{}, err
	}
	entry, found, err := u.board.Rank(ctx, b, entityID, asc)
	if err != nil {
		return RankView{}, err
	}
	if found {
		return RankView{Entry: entry, Found: true}, nil
	}
	estEntry, total, estFound, err := u.board.Estimate(ctx, b, entityID, asc)
	if err != nil {
		return RankView{}, err
	}
	if !estFound {
		return RankView{}, nil
	}
	return RankView{Entry: estEntry, Found: true, Estimated: true, TotalSubmitters: total}, nil
}

// GetRange 取榜区间;返回 entries + 榜总人数。
func (u *LeaderboardUsecase) GetRange(ctx context.Context, b data.BoardKey, offset int64, limit int) ([]data.Entry, int64, error) {
	if err := validateBoard(b); err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = u.cfg.DefaultListLimit
	}
	if limit > u.cfg.MaxListLimit {
		limit = u.cfg.MaxListLimit
	}
	if offset < 0 {
		offset = 0
	}
	asc, _, err := u.boardAscending(ctx, b)
	if err != nil {
		return nil, 0, err
	}
	entries, err := u.board.Range(ctx, b, offset, limit, asc)
	if err != nil {
		return nil, 0, err
	}
	total, err := u.board.Total(ctx, b)
	if err != nil {
		return nil, 0, err
	}
	return entries, total, nil
}

// GetAround 取某 entity 上下 radius 名。
func (u *LeaderboardUsecase) GetAround(ctx context.Context, b data.BoardKey, entityID uint64, radius int) ([]data.Entry, bool, error) {
	if err := validateBoard(b); err != nil {
		return nil, false, err
	}
	if radius <= 0 {
		radius = u.cfg.DefaultAroundRadius
	}
	if radius > u.cfg.MaxListLimit {
		radius = u.cfg.MaxListLimit
	}
	asc, _, err := u.boardAscending(ctx, b)
	if err != nil {
		return nil, false, err
	}
	return u.board.Around(ctx, b, entityID, radius, asc)
}

// RemoveEntry 移除某 entity。
func (u *LeaderboardUsecase) RemoveEntry(ctx context.Context, b data.BoardKey, entityID uint64) error {
	if err := validateBoard(b); err != nil {
		return err
	}
	if entityID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "entity_id required")
	}
	return u.board.Remove(ctx, b, entityID)
}

// DeleteBoard 删整个榜。
func (u *LeaderboardUsecase) DeleteBoard(ctx context.Context, b data.BoardKey) error {
	if err := validateBoard(b); err != nil {
		return err
	}
	return u.board.Delete(ctx, b)
}

// SettleResult 是 SettleBoard 的返回。
type SettleResult struct {
	SettlementID   uint64
	SettledCount   int64
	AlreadySettled bool
	Winners        []data.Entry
}

// SettleBoard 结算:取 Top-N → 落快照 + 批次(幂等)→ 发奖 + kafka → 可选 reset。
//
// 幂等:settle_idempotency_key(默认 = board 串)命中 → already=true,不重复发奖(回放已存批次的快照)。
// 发奖:仅对「按玩家发奖」的榜(GLOBAL / INSTANCE / CUSTOM,entity=player_id)调 granter;
//
//	GUILD 榜 entity=guild_id,不直接发玩家背包,只落快照 + 发 kafka 由工会服务消费分发。
func (u *LeaderboardUsecase) SettleBoard(ctx context.Context, b data.BoardKey, topN int, rewardTable *leaderboardv1.RewardTable, resetAfter bool, settleIdemKey string) (*SettleResult, error) {
	if err := validateBoard(b); err != nil {
		return nil, err
	}
	if topN <= 0 {
		topN = u.cfg.DefaultSettleTopN
	}
	if settleIdemKey == "" {
		settleIdemKey = "lb:" + b.String()
	}

	asc, exists, err := u.boardAscending(ctx, b)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errcode.New(errcode.ErrLeaderboardBoardNotFound, "board not found: %s", b.String())
	}

	winners, err := u.board.Range(ctx, b, 0, topN, asc)
	if err != nil {
		return nil, err
	}

	now := nowMs()
	rec := &data.SettlementRecord{
		SettlementID:  u.sf.Generate(),
		BoardType:     b.BoardType,
		Scope:         int32(b.Scope),
		ScopeID:       b.ScopeID,
		Period:        b.Period,
		TopN:          int32(topN),
		SettledCount:  int32(len(winners)),
		SettleIdemKey: settleIdemKey,
		ResetAfter:    resetAfter,
		CreatedAtMs:   now,
	}
	existing, already, err := u.repo.ClaimSettlement(ctx, rec)
	if err != nil {
		return nil, err
	}
	if already {
		// §9.2 结算幂等:重复 SettleBoard(重试 / 重复调度 / 多副本)命中,回放快照不重复发奖。
		// SettleBoard 是稀有管理操作,INFO 量级极低;可区分"首次结算"与"幂等重放"以排查疑似重复发奖。
		plog.With(ctx).Infow("msg", "lb_settle_idempotent_hit",
			"settlement_id", existing.SettlementID, "board", b.String())
		// 幂等命中:本次不重复发奖,从 MySQL 快照回放 winners(不能从 Redis 取——首次结算
		// reset_after=true 已清空榜;Redis 是计算层、可 evict / TTL,快照才是结算权威记录)。
		snapWinners, lerr := u.loadSnapshotWinners(ctx, existing.SettlementID)
		if lerr != nil {
			return nil, lerr
		}
		return &SettleResult{
			SettlementID:   existing.SettlementID,
			SettledCount:   int64(existing.SettledCount),
			AlreadySettled: true,
			Winners:        snapWinners,
		}, nil
	}

	// 落 Top-N 快照
	rows := make([]data.SnapshotRow, 0, len(winners))
	for _, w := range winners {
		rows = append(rows, data.SnapshotRow{Rank: w.Rank, EntityID: w.EntityID, Score: w.Score, CreatedAtMs: now})
	}
	if err := u.repo.SaveSnapshot(ctx, rec.SettlementID, rows); err != nil {
		return nil, err
	}

	// 发奖(仅按玩家发奖的榜)
	if rewardTable != nil && len(rewardTable.GetTiers()) > 0 && b.Scope != data.ScopeGuild {
		u.grantRewards(ctx, rec.SettlementID, winners, rewardTable)
	}

	// kafka 结算事件(弱依赖)
	if u.events != nil {
		pbWinners := make([]*leaderboardv1.LeaderboardEntry, 0, len(winners))
		for _, w := range winners {
			pbWinners = append(pbWinners, &leaderboardv1.LeaderboardEntry{
				EntityId: w.EntityID, Score: w.Score, Rank: w.Rank, UpdatedAtMs: w.UpdatedAtMs,
			})
		}
		if perr := u.events.PushSettle(ctx, rec.SettlementID, b, pbWinners); perr != nil {
			plog.With(ctx).Warnw("msg", "lb_settle_event_push_failed", "settlement_id", rec.SettlementID, "err", perr)
		}
	}

	// reset(周期榜进入下一周期)
	if resetAfter {
		if cerr := u.board.Clear(ctx, b); cerr != nil {
			plog.With(ctx).Warnw("msg", "lb_settle_reset_failed", "board", b.String(), "err", cerr)
		}
	}

	return &SettleResult{
		SettlementID: rec.SettlementID,
		SettledCount: int64(len(winners)),
		Winners:      winners,
	}, nil
}

// loadSnapshotWinners 从 MySQL 快照按 rank 升序回放 winners(幂等命中复用)。
// 快照不存 updated_at,UpdatedAtMs 留 0;结算快照是名次 + 分数的权威归档,展示时间非必需。
func (u *LeaderboardUsecase) loadSnapshotWinners(ctx context.Context, settlementID uint64) ([]data.Entry, error) {
	rows, err := u.repo.LoadSnapshot(ctx, settlementID)
	if err != nil {
		return nil, err
	}
	winners := make([]data.Entry, 0, len(rows))
	for _, r := range rows {
		winners = append(winners, data.Entry{EntityID: r.EntityID, Score: r.Score, Rank: r.Rank})
	}
	return winners, nil
}

// grantRewards 按 RewardTable 给 Top-N 逐名次幂等发奖(失败不中断整批,逐条记 log)。
func (u *LeaderboardUsecase) grantRewards(ctx context.Context, settlementID uint64, winners []data.Entry, table *leaderboardv1.RewardTable) {
	for _, w := range winners {
		items := rewardsForRank(table, w.Rank)
		if len(items) == 0 {
			continue
		}
		grantKey := fmt.Sprintf("lb:%d:%d", settlementID, w.EntityID)
		now := nowMs()
		payload, eerr := encodeRewardGrants(ctx, items)
		if eerr != nil {
			// 编码不出来就不能 Claim:留下一条解不开的补发入参比不发更糟(补扫会永远标 FAILED)。
			plog.With(ctx).Errorw("msg", "lb_reward_encode_failed",
				"settlement_id", settlementID, "entity", w.EntityID, "items", len(items), "err", eerr)
			continue
		}
		log := &data.RewardLogRecord{
			SettlementID:  settlementID,
			EntityID:      w.EntityID,
			Rank:          w.Rank,
			GrantIdemKey:  grantKey,
			Status:        data.RewardPending,
			RewardPayload: payload,
			CreatedAtMs:   now,
			UpdatedAtMs:   now,
		}
		already, err := u.repo.ClaimReward(ctx, log)
		if err != nil {
			plog.With(ctx).Errorw("msg", "lb_reward_claim_failed", "settlement_id", settlementID, "entity", w.EntityID, "err", err)
			continue
		}
		if already {
			continue // 本名次已发过(幂等)
		}
		if gerr := u.granter.Grant(ctx, w.EntityID, grantKey, items); gerr != nil {
			plog.With(ctx).Errorw("msg", "lb_reward_grant_failed", "settlement_id", settlementID, "entity", w.EntityID, "err", gerr)
			if merr := u.repo.MarkReward(ctx, grantKey, data.RewardFailed, nowMs()); merr != nil {
				plog.With(ctx).Warnw("msg", "lb_mark_reward_failed", "settlement_id", settlementID,
					"entity", w.EntityID, "target_status", "FAILED", "err", merr)
			}
			continue
		}
		if merr := u.repo.MarkReward(ctx, grantKey, data.RewardGranted, nowMs()); merr != nil {
			// 发奖成功但标 GRANTED 失败 → 该行滞留 PENDING,被补扫再次 Grant(幂等键兜住不双发,但状态漂移)。
			plog.With(ctx).Warnw("msg", "lb_mark_reward_failed", "settlement_id", settlementID,
				"entity", w.EntityID, "target_status", "GRANTED", "err", merr)
		}
	}
}

// RetryUngrantedRewards 补发未成的结算奖励(后台周期调,main.go 起 ticker)。
//
// 覆盖两类漏发:FAILED(inventory 拒绝 / 不可达)和 PENDING(ClaimReward 后、
// MarkReward 前进程崩残留)。olderThan 把「刚结算还在同步发」的批次挡在扫描外;
// Grant 以 grant_idempotency_key 幂等,多副本并发补扫 / 与同步路径重叠都不会双发。
// 返回本轮补发成功 / 仍失败的条数。
func (u *LeaderboardUsecase) RetryUngrantedRewards(ctx context.Context, olderThan time.Duration, limit int) (granted, failed int) {
	rows, err := u.repo.ListUngrantedRewards(ctx, nowMs()-olderThan.Milliseconds(), limit)
	if err != nil {
		plog.With(ctx).Errorw("msg", "lb_reward_retry_list_failed", "err", err)
		return 0, 0
	}
	for _, rec := range rows {
		items, uerr := decodeRewardGrants(rec.RewardPayload)
		if uerr != nil || len(items) == 0 {
			// 脏数据:标 FAILED 不再重扫死循环,靠 Errorw 告警人工介入。
			plog.With(ctx).Errorw("msg", "lb_reward_retry_bad_payload",
				"key", rec.GrantIdemKey, "payload_bytes", len(rec.RewardPayload), "err", uerr)
			_ = u.repo.MarkReward(ctx, rec.GrantIdemKey, data.RewardFailed, nowMs())
			failed++
			continue
		}
		if gerr := u.granter.Grant(ctx, rec.EntityID, rec.GrantIdemKey, items); gerr != nil {
			plog.With(ctx).Warnw("msg", "lb_reward_retry_grant_failed",
				"key", rec.GrantIdemKey, "entity", rec.EntityID, "err", gerr)
			_ = u.repo.MarkReward(ctx, rec.GrantIdemKey, data.RewardFailed, nowMs())
			failed++
			continue
		}
		if merr := u.repo.MarkReward(ctx, rec.GrantIdemKey, data.RewardGranted, nowMs()); merr != nil {
			// 已发成但没标上:下轮补扫会再 Grant(幂等 no-op)后重标,自愈。
			plog.With(ctx).Warnw("msg", "lb_reward_retry_mark_failed", "key", rec.GrantIdemKey, "err", merr)
		}
		granted++
	}
	if granted > 0 || failed > 0 {
		plog.With(ctx).Infow("msg", "lb_reward_retry_done", "granted", granted, "failed", failed)
	}
	return granted, failed
}

// rewardPayloadMaxBytes 是 reward_pb 写入侧字节上限。
// 列是 VARBINARY(2048),这里取 75% 预警线:单条 RewardItem 编码 ≤ 16 字节,
// 1536 字节已能装下上百件奖励——达到即 RewardTier.items 条数失控。
const rewardPayloadMaxBytes = 1536

// encodeRewardGrants 把发奖明细编成 pb 二进制(列 reward_pb),落库前过写入侧字节闸(§9.24)。
func encodeRewardGrants(ctx context.Context, items []data.RewardGrant) ([]byte, error) {
	rec := &leaderboardv1.RewardGrantStorageRecord{
		Items: make([]*leaderboardv1.RewardItem, 0, len(items)),
	}
	for _, it := range items {
		rec.Items = append(rec.Items, &leaderboardv1.RewardItem{ItemConfigId: it.ItemConfigID, Count: it.Count})
	}
	raw, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}
	if cerr := dbguard.CheckPayload(ctx, dbguard.PayloadLimit{
		DB: "pandora_leaderboard", Table: "leaderboard_reward_log", Column: "reward_pb",
		Max:  rewardPayloadMaxBytes,
		Hint: "发奖明细超设计上限:查 RewardTable 里该名次区间的 items 条数",
	}, raw); cerr != nil {
		return nil, cerr
	}
	return raw, nil
}

// decodeRewardGrants 从 pb 二进制解出发奖明细(补发重放入参)。
func decodeRewardGrants(raw []byte) ([]data.RewardGrant, error) {
	var rec leaderboardv1.RewardGrantStorageRecord
	if err := proto.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	out := make([]data.RewardGrant, 0, len(rec.GetItems()))
	for _, it := range rec.GetItems() {
		out = append(out, data.RewardGrant{ItemConfigID: it.GetItemConfigId(), Count: it.GetCount()})
	}
	return out, nil
}

// rewardsForRank 返回某名次命中的奖励(取第一个匹配区间)。
func rewardsForRank(table *leaderboardv1.RewardTable, rank int64) []data.RewardGrant {
	for _, tier := range table.GetTiers() {
		if rank >= tier.GetRankFrom() && rank <= tier.GetRankTo() {
			out := make([]data.RewardGrant, 0, len(tier.GetItems()))
			for _, it := range tier.GetItems() {
				if it.GetCount() <= 0 {
					continue
				}
				out = append(out, data.RewardGrant{ItemConfigID: it.GetItemConfigId(), Count: it.GetCount()})
			}
			return out
		}
	}
	return nil
}
