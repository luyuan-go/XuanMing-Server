// LeaderboardUsecase 业务层单测(内存 repo + miniredis 榜 + 计数 granter,2026-06-27)。
//
// 覆盖:上报 + 区间排序、结算落快照 + 按 RewardTable 发奖、结算幂等(不重复发奖)、
// GUILD 榜不直接发玩家奖、resetAfter 清榜。
package biz

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	leaderboardv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/leaderboard/v1"

	"github.com/luyuancpp/pandora/services/runtime/leaderboard/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/leaderboard/internal/data"
)

// ── fakeRepo:内存实现 LeaderboardRepo ────────────────────────────────────────

type fakeRepo struct {
	mu          sync.Mutex
	settlements map[string]*data.SettlementRecord // key = settle_idempotency_key
	snapshots   map[uint64][]data.SnapshotRow     // settlement_id → rows
	rewards     map[string]*data.RewardLogRecord  // grant_idempotency_key
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		settlements: map[string]*data.SettlementRecord{},
		snapshots:   map[uint64][]data.SnapshotRow{},
		rewards:     map[string]*data.RewardLogRecord{},
	}
}

func (r *fakeRepo) ClaimSettlement(_ context.Context, rec *data.SettlementRecord) (*data.SettlementRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ex, ok := r.settlements[rec.SettleIdemKey]; ok {
		return ex, true, nil
	}
	cp := *rec
	r.settlements[rec.SettleIdemKey] = &cp
	return &cp, false, nil
}

func (r *fakeRepo) SaveSnapshot(_ context.Context, settlementID uint64, rows []data.SnapshotRow) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[settlementID] = append([]data.SnapshotRow(nil), rows...)
	return nil
}

func (r *fakeRepo) LoadSnapshot(_ context.Context, settlementID uint64) ([]data.SnapshotRow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]data.SnapshotRow(nil), r.snapshots[settlementID]...), nil
}

func (r *fakeRepo) ClaimReward(_ context.Context, rec *data.RewardLogRecord) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rewards[rec.GrantIdemKey]; ok {
		return true, nil
	}
	cp := *rec
	r.rewards[rec.GrantIdemKey] = &cp
	return false, nil
}

func (r *fakeRepo) MarkReward(_ context.Context, grantIdemKey string, status int8, updatedAtMs int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.rewards[grantIdemKey]; ok {
		rec.Status = status
		rec.UpdatedAtMs = updatedAtMs
	}
	return nil
}

func (r *fakeRepo) ListUngrantedRewards(_ context.Context, olderThanMs int64, limit int) ([]data.RewardLogRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []data.RewardLogRecord
	for _, rec := range r.rewards {
		if rec.Status != data.RewardGranted && rec.UpdatedAtMs < olderThanMs {
			out = append(out, *rec)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r *fakeRepo) grantCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rewards)
}

// ── trackGranter:记录每次发奖 ───────────────────────────────────────────────

type trackGranter struct {
	mu     sync.Mutex
	grants map[uint64]int // player_id → 调用次数
}

func newTrackGranter() *trackGranter { return &trackGranter{grants: map[uint64]int{}} }

func (g *trackGranter) Grant(_ context.Context, playerID uint64, _ string, _ []data.RewardGrant) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.grants[playerID]++
	return nil
}

func (g *trackGranter) total() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, c := range g.grants {
		n += c
	}
	return n
}

// ── trackPusher:记录 kafka 结算事件 ─────────────────────────────────────────

type trackPusher struct {
	mu    sync.Mutex
	calls int
}

func (p *trackPusher) PushSettle(_ context.Context, _ uint64, _ data.BoardKey, _ []*leaderboardv1.LeaderboardEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return nil
}

func (p *trackPusher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// ── seqGen:自增 snowflake ────────────────────────────────────────────────────

type seqGen struct {
	mu sync.Mutex
	n  uint64
}

func (g *seqGen) Generate() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return g.n
}

// newTestUsecase 装配:内存 repo + miniredis 榜 + 计数 granter + 计数 pusher。
func newTestUsecase(t *testing.T) (*LeaderboardUsecase, *fakeRepo, *trackGranter, *trackPusher) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	repo := newFakeRepo()
	board := data.NewRedisBoardStore(rdb)
	granter := newTrackGranter()
	pusher := &trackPusher{}
	uc := NewLeaderboardUsecase(repo, board, granter, pusher, &seqGen{n: 1000}, conf.LeaderboardConf{})
	return uc, repo, granter, pusher
}

var globalBoard = data.BoardKey{BoardType: 1, Scope: data.ScopeGlobal, ScopeID: 0, Period: "2026W26"}

// rewardTable3 构造前 1 名 / 2-3 名两档奖励表。
func rewardTable3() *leaderboardv1.RewardTable {
	return &leaderboardv1.RewardTable{
		Tiers: []*leaderboardv1.RewardTier{
			{RankFrom: 1, RankTo: 1, Items: []*leaderboardv1.RewardItem{{ItemConfigId: 1001, Count: 100}}},
			{RankFrom: 2, RankTo: 3, Items: []*leaderboardv1.RewardItem{{ItemConfigId: 1002, Count: 50}}},
		},
	}
}

// ── 用例 ──────────────────────────────────────────────────────────────────────

// TestRetryUngrantedRewards_RegrantsFailed 补扫把 FAILED 记录重发成功并标 GRANTED;
// grace 内(updated_at_ms 新于阈值)的记录不动。
func TestRetryUngrantedRewards_RegrantsFailed(t *testing.T) {
	uc, repo, granter, _ := newTestUsecase(t)
	ctx := context.Background()

	old := nowMs() - 10*60_000 // 10 分钟前:超出 grace,应被补扫
	fresh := nowMs()           // 刚失败:在 grace 内,本轮不碰
	// 补发入参是 pb 二进制(reward_pb 列),用与生产同一条编码路径造种子。
	pay := func(itemID uint32, count int64) []byte {
		raw, err := encodeRewardGrants(context.Background(), []data.RewardGrant{{ItemConfigID: itemID, Count: count}})
		if err != nil {
			t.Fatalf("encode reward payload: %v", err)
		}
		return raw
	}
	seed := []*data.RewardLogRecord{
		{SettlementID: 1, EntityID: 11, Rank: 1, GrantIdemKey: "lb:1:11",
			Status: data.RewardFailed, RewardPayload: pay(1001, 100), UpdatedAtMs: old},
		{SettlementID: 1, EntityID: 12, Rank: 2, GrantIdemKey: "lb:1:12",
			Status: data.RewardPending, RewardPayload: pay(1002, 50), UpdatedAtMs: old},
		{SettlementID: 2, EntityID: 13, Rank: 1, GrantIdemKey: "lb:2:13",
			Status: data.RewardFailed, RewardPayload: pay(1001, 100), UpdatedAtMs: fresh},
	}
	for _, rec := range seed {
		if already, err := repo.ClaimReward(ctx, rec); err != nil || already {
			t.Fatalf("seed reward %s: already=%v err=%v", rec.GrantIdemKey, already, err)
		}
	}

	granted, failed := uc.RetryUngrantedRewards(ctx, 2*time.Minute, 100)
	if granted != 2 || failed != 0 {
		t.Fatalf("want granted=2 failed=0, got granted=%d failed=%d", granted, failed)
	}
	if granter.total() != 2 {
		t.Fatalf("want 2 grant calls, got %d", granter.total())
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if s := repo.rewards["lb:1:11"].Status; s != data.RewardGranted {
		t.Fatalf("lb:1:11 want GRANTED, got %d", s)
	}
	if s := repo.rewards["lb:1:12"].Status; s != data.RewardGranted {
		t.Fatalf("lb:1:12 want GRANTED, got %d", s)
	}
	if s := repo.rewards["lb:2:13"].Status; s != data.RewardFailed {
		t.Fatalf("lb:2:13 in grace window must stay FAILED, got %d", s)
	}
}

// TestGetRank_EstimatedFallback 精确榜命中回精确名次;被 max_size 截断的玩家回退直方图估算;
// 从未上报 Found=false。
func TestGetRank_EstimatedFallback(t *testing.T) {
	uc, _, _, _ := newTestUsecase(t)
	ctx := context.Background()
	b := data.BoardKey{BoardType: 9, Scope: data.ScopeGlobal, ScopeID: 0, Period: "-"}
	opt := data.Options{MaxSize: 2, EstimateBucketWidth: 10}

	for i := uint64(1); i <= 5; i++ {
		if _, _, err := uc.SubmitScore(ctx, b, i, int64(i*10), data.ModeSet, opt); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	// 榜内(id5=50,第 1 名):精确名次
	top, err := uc.GetRank(ctx, b, 5)
	if err != nil || !top.Found {
		t.Fatalf("rank id=5: found=%v err=%v", top.Found, err)
	}
	if top.Estimated || top.Entry.Rank != 1 {
		t.Fatalf("id=5: estimated=%v rank=%d, want precise rank 1", top.Estimated, top.Entry.Rank)
	}
	// 榜外(id2=20,真实第 4 名):估算名次 + 参与总人数
	off, err := uc.GetRank(ctx, b, 2)
	if err != nil || !off.Found {
		t.Fatalf("rank id=2: found=%v err=%v", off.Found, err)
	}
	if !off.Estimated {
		t.Fatalf("id=2 should be estimated (truncated out of top-2)")
	}
	if off.Entry.Rank != 4 || off.TotalSubmitters != 5 {
		t.Fatalf("id=2: rank=%d total=%d, want 4/5", off.Entry.Rank, off.TotalSubmitters)
	}
	if off.Entry.Score != 20 {
		t.Fatalf("id=2: score=%d, want 20", off.Entry.Score)
	}
	// 从未上报
	none, err := uc.GetRank(ctx, b, 999)
	if err != nil {
		t.Fatalf("rank id=999: %v", err)
	}
	if none.Found {
		t.Fatalf("id=999 should not be found")
	}
}

func TestSubmitAndRange(t *testing.T) {
	uc, _, _, _ := newTestUsecase(t)
	ctx := context.Background()

	_, _, _ = uc.SubmitScore(ctx, globalBoard, 1, 30, data.ModeSet, data.Options{})
	_, _, _ = uc.SubmitScore(ctx, globalBoard, 2, 90, data.ModeSet, data.Options{})
	_, _, _ = uc.SubmitScore(ctx, globalBoard, 3, 60, data.ModeSet, data.Options{})

	entries, total, err := uc.GetRange(ctx, globalBoard, 0, 10)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if total != 3 {
		t.Fatalf("total=%d, want 3", total)
	}
	if entries[0].EntityID != 2 || entries[1].EntityID != 3 || entries[2].EntityID != 1 {
		t.Fatalf("order = [%d,%d,%d], want [2,3,1]", entries[0].EntityID, entries[1].EntityID, entries[2].EntityID)
	}
}

func TestSettle_SnapshotAndRewards(t *testing.T) {
	uc, repo, granter, pusher := newTestUsecase(t)
	ctx := context.Background()

	for i := uint64(1); i <= 5; i++ {
		_, _, _ = uc.SubmitScore(ctx, globalBoard, i, int64(i*10), data.ModeSet, data.Options{})
	}
	// 降序后名次:id5(1) id4(2) id3(3) id2(4) id1(5)
	res, err := uc.SettleBoard(ctx, globalBoard, 3, rewardTable3(), false, "")
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if res.AlreadySettled {
		t.Fatalf("first settle should not be already")
	}
	if res.SettledCount != 3 {
		t.Fatalf("settled count=%d, want 3", res.SettledCount)
	}
	// 快照应有 3 行
	if rows := repo.snapshots[res.SettlementID]; len(rows) != 3 {
		t.Fatalf("snapshot rows=%d, want 3", len(rows))
	}
	// 前 3 名都该发奖(rank1 → 1001;rank2,3 → 1002),共 3 次 grant
	if got := granter.total(); got != 3 {
		t.Fatalf("grant total=%d, want 3", got)
	}
	if got := repo.grantCount(); got != 3 {
		t.Fatalf("reward log count=%d, want 3", got)
	}
	// kafka 结算事件一次
	if got := pusher.count(); got != 1 {
		t.Fatalf("pusher calls=%d, want 1", got)
	}
}

func TestSettle_Idempotent_NoDoubleGrant(t *testing.T) {
	uc, repo, granter, _ := newTestUsecase(t)
	ctx := context.Background()

	for i := uint64(1); i <= 3; i++ {
		_, _, _ = uc.SubmitScore(ctx, globalBoard, i, int64(i*10), data.ModeSet, data.Options{})
	}
	idem := "season-2026W26-final"
	r1, err := uc.SettleBoard(ctx, globalBoard, 3, rewardTable3(), false, idem)
	if err != nil {
		t.Fatalf("settle1: %v", err)
	}
	grantsAfter1 := granter.total()

	// 同一幂等键重复结算 → already=true,不重复发奖
	r2, err := uc.SettleBoard(ctx, globalBoard, 3, rewardTable3(), false, idem)
	if err != nil {
		t.Fatalf("settle2: %v", err)
	}
	if !r2.AlreadySettled {
		t.Fatalf("second settle AlreadySettled=false, want true")
	}
	if r2.SettlementID != r1.SettlementID {
		t.Fatalf("settlement id changed on replay: %d vs %d", r2.SettlementID, r1.SettlementID)
	}
	if got := granter.total(); got != grantsAfter1 {
		t.Fatalf("grant total grew on replay: %d → %d", grantsAfter1, got)
	}
	_ = repo
}

func TestSettle_GuildScope_NoDirectGrant(t *testing.T) {
	uc, _, granter, pusher := newTestUsecase(t)
	ctx := context.Background()
	guildBoard := data.BoardKey{BoardType: 7, Scope: data.ScopeGuild, ScopeID: 0, Period: "2026W26"}

	for i := uint64(1); i <= 3; i++ {
		_, _, _ = uc.SubmitScore(ctx, guildBoard, i, int64(i*10), data.ModeSet, data.Options{})
	}
	res, err := uc.SettleBoard(ctx, guildBoard, 3, rewardTable3(), false, "")
	if err != nil {
		t.Fatalf("settle guild: %v", err)
	}
	if res.SettledCount != 3 {
		t.Fatalf("settled count=%d, want 3", res.SettledCount)
	}
	// 工会榜不直接发玩家背包
	if got := granter.total(); got != 0 {
		t.Fatalf("guild board grant total=%d, want 0 (handled by guild service via kafka)", got)
	}
	// 但仍发 kafka 事件供工会服务消费
	if got := pusher.count(); got != 1 {
		t.Fatalf("pusher calls=%d, want 1", got)
	}
}

func TestSettle_ResetAfter_ClearsBoard(t *testing.T) {
	uc, _, _, _ := newTestUsecase(t)
	ctx := context.Background()

	for i := uint64(1); i <= 3; i++ {
		_, _, _ = uc.SubmitScore(ctx, globalBoard, i, int64(i*10), data.ModeSet, data.Options{})
	}
	if _, err := uc.SettleBoard(ctx, globalBoard, 3, nil, true, "reset-test"); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// reset 后榜应清空
	_, total, err := uc.GetRange(ctx, globalBoard, 0, 10)
	if err != nil {
		t.Fatalf("range after reset: %v", err)
	}
	if total != 0 {
		t.Fatalf("total after reset=%d, want 0", total)
	}
}

func TestSettle_BoardNotFound(t *testing.T) {
	uc, _, _, _ := newTestUsecase(t)
	ctx := context.Background()
	// 从未上报过的榜
	_, err := uc.SettleBoard(ctx, globalBoard, 3, nil, false, "")
	if err == nil {
		t.Fatalf("settle empty board should error (board not found)")
	}
}

// TestSettle_Idempotent_ReplaysSnapshotAfterReset 回归:首次结算 reset_after=true 清空 Redis 榜后,
// 同一 settle_idempotency_key 复调应 already=true,winners 从 MySQL 快照回放(非空且 = 首次 Top-N),
// 且不重复发奖。修复「幂等命中仍从已清空的 Redis 取 winners → 回放为空」的 bug。
func TestSettle_Idempotent_ReplaysSnapshotAfterReset(t *testing.T) {
	uc, _, granter, _ := newTestUsecase(t)
	ctx := context.Background()

	for i := uint64(1); i <= 5; i++ {
		_, _, _ = uc.SubmitScore(ctx, globalBoard, i, int64(i*10), data.ModeSet, data.Options{})
	}
	idem := "season-reset-replay"

	// 首次结算:reset_after=true 会清空 Redis 榜
	r1, err := uc.SettleBoard(ctx, globalBoard, 3, rewardTable3(), true, idem)
	if err != nil {
		t.Fatalf("settle1: %v", err)
	}
	if r1.AlreadySettled {
		t.Fatalf("first settle AlreadySettled=true, want false")
	}
	if len(r1.Winners) != 3 {
		t.Fatalf("first winners len=%d, want 3", len(r1.Winners))
	}
	grantsAfter1 := granter.total()

	// 确认 Redis 榜确实已清空(回放只能靠 MySQL 快照)
	if _, total, _ := uc.GetRange(ctx, globalBoard, 0, 10); total != 0 {
		t.Fatalf("board not cleared after reset: total=%d", total)
	}

	// 复调同一幂等键:应回放快照里的 winners,而非空
	r2, err := uc.SettleBoard(ctx, globalBoard, 3, rewardTable3(), true, idem)
	if err != nil {
		t.Fatalf("settle2: %v", err)
	}
	if !r2.AlreadySettled {
		t.Fatalf("replay AlreadySettled=false, want true")
	}
	if r2.SettlementID != r1.SettlementID {
		t.Fatalf("replay settlement id=%d, want %d", r2.SettlementID, r1.SettlementID)
	}
	if len(r2.Winners) != 3 {
		t.Fatalf("replay winners len=%d, want 3 (snapshot replay, board already cleared)", len(r2.Winners))
	}
	// 回放 winners 名次 / entity / 分数应与首次 Top-N 一致(id5/4/3,rank1/2/3,score50/40/30)
	wantID := []uint64{5, 4, 3}
	wantScore := []int64{50, 40, 30}
	for i, w := range r2.Winners {
		if w.Rank != int64(i+1) || w.EntityID != wantID[i] || w.Score != wantScore[i] {
			t.Fatalf("replay winner[%d]=rank%d/id%d/score%d, want rank%d/id%d/score%d",
				i, w.Rank, w.EntityID, w.Score, i+1, wantID[i], wantScore[i])
		}
	}
	// 不重复发奖
	if got := granter.total(); got != grantsAfter1 {
		t.Fatalf("grant total grew on replay: %d → %d", grantsAfter1, got)
	}
}
