// mission_test.go — 任务域领域语义单测(docs/design/mission.md §7 移植对照表)。
//
// 覆盖 README「验证矩阵」中 biz 可验的全部行:接取校验分支 / 槽位过滤与 clamp /
// 事实护栏 / 完成扇出(发奖分流 + 自动接链 + COMPLETE_MISSION 再入)/ 链上限 /
// 领奖 CAS / 发放路由 / 溢出饱和。存储层(FOR UPDATE 真实并发、收据 uk 幂等)
// 属集成验证,见 README「未在本地验证」。
package biz

import (
	"context"
	"errors"
	"fmt"
	"testing"

	klog "github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
)

// ── 假件 ────────────────────────────────────────────────────────────────────

type fakeCatalog struct {
	missions   map[uint32]*configpb.MissionRow
	conditions map[uint32]*configpb.ConditionRow
	rewards    map[uint32]*configpb.RewardRow
	equipment  map[uint32]bool
}

func (c *fakeCatalog) MissionByID(id uint32) (*configpb.MissionRow, bool) {
	row, ok := c.missions[id]
	return row, ok
}
func (c *fakeCatalog) ConditionByID(id uint32) (*configpb.ConditionRow, bool) {
	row, ok := c.conditions[id]
	return row, ok
}
func (c *fakeCatalog) RewardByID(id uint32) (*configpb.RewardRow, bool) {
	row, ok := c.rewards[id]
	return row, ok
}
func (c *fakeCatalog) IsEquipment(itemConfigID uint32) bool { return c.equipment[itemConfigID] }

// Snapshot 让假件同时充当 CatalogSource:单测里配置批次恒定,快照即自身。
func (c *fakeCatalog) Snapshot() Catalog { return c }

// fakeRepo 在内存里复刻事务语义:fn 出错即整体丢弃突变(不落任何状态)。
type fakeRepo struct {
	state       *PlayerState
	rewardLogs  []*RewardLogRow
	pushChunks  [][]byte
	nextLogID   uint64
	factKeys    map[string][]byte
	failMutate  error
	marked      map[uint64]bool // id → granted
	ungrantedIn []*RewardLogRow

	pushOutbox     []*PushOutboxRow
	deleteVanishes bool // true = DeletePushOutbox 命中 0 行(另一副本已抢先删)
}

// fakePusher 记录投递顺序(单写者用例判"热备副本一行不发")。
type fakePusher struct{ sent [][]byte }

func (p *fakePusher) PushMissionUpdate(_ context.Context, _ uint64, payload []byte) error {
	p.sent = append(p.sent, payload)
	return nil
}

func newFakeRepo(playerID uint64) *fakeRepo {
	return &fakeRepo{
		state: &PlayerState{
			PlayerID: playerID,
			Active:   map[uint32]*ActiveMission{},
			Done:     map[uint32]*DoneMission{},
		},
		factKeys: map[string][]byte{},
		marked:   map[uint64]bool{},
	}
}

func (r *fakeRepo) MutatePlayer(_ context.Context, _ uint64, fn func(*PlayerState) (*Mutation, error)) error {
	if r.failMutate != nil {
		return r.failMutate
	}
	snapshot := cloneState(r.state)
	mut, err := fn(r.state)
	if err != nil {
		r.state = snapshot // 事务回滚
		return err
	}
	r.persist(mut)
	return nil
}

func (r *fakeRepo) ApplyFactsTx(_ context.Context, _ uint64, key string, fingerprint []byte,
	fn func(*PlayerState) (*Mutation, error)) (bool, error) {
	if existing, ok := r.factKeys[key]; ok {
		if string(existing) != string(fingerprint) {
			return false, errcode.New(errcode.ErrMissionFactsConflict, "key reuse")
		}
		return true, nil
	}
	snapshot := cloneState(r.state)
	mut, err := fn(r.state)
	if err != nil {
		r.state = snapshot
		return false, err
	}
	r.factKeys[key] = fingerprint
	r.persist(mut)
	return false, nil
}

func (r *fakeRepo) persist(mut *Mutation) {
	if mut == nil {
		return
	}
	for _, entry := range mut.RewardLogs {
		r.nextLogID++
		entry.ID = r.nextLogID
		r.rewardLogs = append(r.rewardLogs, &RewardLogRow{
			ID: entry.ID, PlayerID: r.state.PlayerID,
			MissionConfigID: entry.MissionConfigID, Key: entry.Key, RewardPB: entry.RewardPB,
		})
	}
	r.pushChunks = append(r.pushChunks, mut.PushPayloads...)
}

func (r *fakeRepo) LoadPlayer(context.Context, uint64) (*PlayerState, error) {
	return cloneState(r.state), nil
}

func (r *fakeRepo) ListUngrantedRewards(context.Context, int64, int) ([]*RewardLogRow, error) {
	return r.ungrantedIn, nil
}
func (r *fakeRepo) MarkReward(_ context.Context, id uint64, granted bool, _ int64) error {
	r.marked[id] = granted
	return nil
}
func (r *fakeRepo) FetchPushOutbox(_ context.Context, limit int) ([]*PushOutboxRow, error) {
	if len(r.pushOutbox) <= limit {
		return r.pushOutbox, nil
	}
	return r.pushOutbox[:limit], nil
}

func (r *fakeRepo) DeletePushOutbox(_ context.Context, id uint64) error {
	if r.deleteVanishes {
		return errors.New("row already deleted by another replica")
	}
	out := r.pushOutbox[:0]
	for _, row := range r.pushOutbox {
		if row.ID != id {
			out = append(out, row)
		}
	}
	r.pushOutbox = out
	return nil
}
func (r *fakeRepo) SweepRewardLog(context.Context, string, int, int) error { return nil }
func (r *fakeRepo) SweepReceipts(context.Context, string, int, int) error  { return nil }

func cloneState(st *PlayerState) *PlayerState {
	out := &PlayerState{
		PlayerID: st.PlayerID,
		Active:   make(map[uint32]*ActiveMission, len(st.Active)),
		Done:     make(map[uint32]*DoneMission, len(st.Done)),
	}
	for id, am := range st.Active {
		out.Active[id] = &ActiveMission{
			MissionConfigID: am.MissionConfigID,
			Progress:        append([]uint32(nil), am.Progress...),
			AcceptedAtMs:    am.AcceptedAtMs,
		}
	}
	for id, dm := range st.Done {
		cp := *dm
		out.Done[id] = &cp
	}
	return out
}

type fakeItemGranter struct {
	stacks       []RewardItem
	instances    []uint32
	capacityFull bool
	failStacks   bool
}

func (g *fakeItemGranter) GrantItems(_ context.Context, _ uint64, _ string, items []RewardItem) error {
	if g.failStacks {
		return errors.New("inventory down")
	}
	g.stacks = append(g.stacks, items...)
	return nil
}

func (g *fakeItemGranter) GrantInstances(_ context.Context, _ uint64, _ string, ids []uint32) (bool, error) {
	g.instances = append(g.instances, ids...)
	return g.capacityFull, nil
}

type fakeExpGranter struct {
	total uint64
	keys  []string
}

func (g *fakeExpGranter) AddExperience(_ context.Context, _ uint64, delta uint64, key string) error {
	g.total += delta
	g.keys = append(g.keys, key)
	return nil
}

type fakeMail struct {
	sent []uint32
	key  string
}

func (m *fakeMail) SendOverflowMail(_ context.Context, _ uint64, ids []uint32, key string) error {
	m.sent = append(m.sent, ids...)
	m.key = key
	return nil
}

// ── 构造 ────────────────────────────────────────────────────────────────────

const testPlayer uint64 = 42

// cond 造一个条件行:类别 + 目标 + 可选槽位1过滤(comparison_op=0 即 >=)。
func cond(id, category, target uint32, slot1 string) *configpb.ConditionRow {
	return &configpb.ConditionRow{
		Id: id, Name: fmt.Sprintf("cond%d", id),
		ConditionCategory: category, TargetCount: target, Slot1: slot1,
	}
}

// mission 造一个任务行。
func mission(id, mtype, subType uint32, condIDs, targets, next string, rewardID, autoReward uint32) *configpb.MissionRow {
	return &configpb.MissionRow{
		Id: id, Name: fmt.Sprintf("m%d", id),
		MissionType: mtype, MissionSubType: subType,
		ConditionIds: condIDs, TargetCounts: targets, NextMissionIds: next,
		RewardId: rewardID, AutoReward: autoReward,
	}
}

func newTestUsecase(t *testing.T, cat *fakeCatalog, repo *fakeRepo,
	items ItemGranter, exp ExpGranter, mail OverflowMailSender) *MissionUsecase {
	t.Helper()
	cfg := conf.MissionConf{MaxActiveMissions: 3, MaxFactsPerReport: 64, PushPublishBatch: 128}
	uc := NewMissionUsecase(repo, cat, items, exp, mail, nil, cfg, klog.NewStdLogger(discard{}))
	uc.SetNowFunc(func() int64 { return 1_700_000_000_000 })
	return uc
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// marshalRecord 造发放快照 pb(生产侧由 buildRewardLog 生成)。
func marshalRecord(record *missionv1.MissionRewardStorageRecord) ([]byte, error) {
	return proto.Marshal(record)
}

// baseCatalog:任务 1(杀 101 怪 ×3,完成自动接任务 2,自动发奖 61)、
// 任务 2(完成任务 1 即达成,奖励 62 可领)、任务 3(与 1 同 type/sub → 互斥)。
func baseCatalog() *fakeCatalog {
	return &fakeCatalog{
		missions: map[uint32]*configpb.MissionRow{
			1: mission(1, 10, 1, "101", "", "2", 61, 1),
			2: mission(2, 10, 2, "102", "", "", 62, 0),
			3: mission(3, 10, 1, "101", "", "", 0, 0),
		},
		conditions: map[uint32]*configpb.ConditionRow{
			101: cond(101, 1, 3, "5001"), // 杀怪:槽位1 必须是 5001
			102: cond(102, 8, 1, "1"),    // 完成任务 1
		},
		rewards: map[uint32]*configpb.RewardRow{
			61: {Id: 61, Name: "r61", ItemIds: "9001", ItemCounts: "2", Exp: 100},
			62: {Id: 62, Name: "r62", ItemIds: "", ItemCounts: "", Exp: 50},
		},
		equipment: map[uint32]bool{},
	}
}

// ── 接取校验 ────────────────────────────────────────────────────────────────

func TestAcceptRejectsUnknownConfig(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	_, err := uc.Accept(context.Background(), testPlayer, 999)
	if got := errcode.As(err); got != errcode.ErrMissionConfigNotFound {
		t.Fatalf("code=%d, want ErrMissionConfigNotFound", got)
	}
}

func TestAcceptRejectsDuplicate(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("首次接取: %v", err)
	}
	_, err := uc.Accept(context.Background(), testPlayer, 1)
	if got := errcode.As(err); got != errcode.ErrMissionAlreadyAccepted {
		t.Fatalf("code=%d, want ErrMissionAlreadyAccepted", got)
	}
}

func TestAcceptRejectsCompleted(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	repo.state.Done[1] = &DoneMission{MissionConfigID: 1}
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	_, err := uc.Accept(context.Background(), testPlayer, 1)
	if got := errcode.As(err); got != errcode.ErrMissionAlreadyCompleted {
		t.Fatalf("code=%d, want ErrMissionAlreadyCompleted", got)
	}
}

// 同 (type, sub_type) 互斥;sub_type=0 不参与互斥(D 版 UnregisterMissionIndexes 语义)。
func TestAcceptTypeMutualExclusion(t *testing.T) {
	cat := baseCatalog()
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取任务 1: %v", err)
	}
	// 任务 3 与任务 1 同 type=10 sub=1 → 互斥。
	if _, err := uc.Accept(context.Background(), testPlayer, 3); errcode.As(err) != errcode.ErrMissionTypeConflict {
		t.Fatalf("code=%d, want ErrMissionTypeConflict", errcode.As(err))
	}
	// 任务 2 是 sub=2,不冲突。
	if _, err := uc.Accept(context.Background(), testPlayer, 2); err != nil {
		t.Fatalf("不同 sub_type 应可接取: %v", err)
	}
	// sub_type=0 的任务对任何同 type 都不互斥。
	cat.missions[4] = mission(4, 10, 0, "101", "", "", 0, 0)
	if _, err := uc.Accept(context.Background(), testPlayer, 4); err != nil {
		t.Fatalf("sub_type=0 不应参与互斥: %v", err)
	}
}

func TestAcceptRejectsOverActiveLimit(t *testing.T) {
	cat := baseCatalog()
	// 三个互不互斥的任务把上限(3)占满,第四个应被拒。
	for id := uint32(11); id <= 14; id++ {
		cat.missions[id] = mission(id, 20+id, 0, "101", "", "", 0, 0)
	}
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	for id := uint32(11); id <= 13; id++ {
		if _, err := uc.Accept(context.Background(), testPlayer, id); err != nil {
			t.Fatalf("接取 %d: %v", id, err)
		}
	}
	_, err := uc.Accept(context.Background(), testPlayer, 14)
	if got := errcode.As(err); got != errcode.ErrMissionActiveLimit {
		t.Fatalf("code=%d, want ErrMissionActiveLimit", got)
	}
}

// ── 放弃 ────────────────────────────────────────────────────────────────────

func TestAbandonSemantics(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)

	// 未接取 → ErrMissionNotAccepted(刻意差异:D 版静默成功)。
	if got := errcode.As(uc.Abandon(context.Background(), testPlayer, 1)); got != errcode.ErrMissionNotAccepted {
		t.Fatalf("未接取放弃 code=%d, want ErrMissionNotAccepted", got)
	}
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	if err := uc.Abandon(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("放弃: %v", err)
	}
	if _, still := repo.state.Active[1]; still {
		t.Fatal("放弃后仍在活跃列表")
	}
	// 已完成不可弃。
	repo.state.Done[2] = &DoneMission{MissionConfigID: 2}
	if got := errcode.As(uc.Abandon(context.Background(), testPlayer, 2)); got != errcode.ErrMissionAlreadyCompleted {
		t.Fatalf("放弃已完成 code=%d, want ErrMissionAlreadyCompleted", got)
	}
}

// ── 进度推进 ────────────────────────────────────────────────────────────────

// 槽位过滤:类别对但槽位值不匹配 → 不推进。
func TestProgressSlotFilterMismatch(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	// 类别 1 但槽位值 9999 ≠ 条件配置的 5001。
	facts := []Fact{{Category: 1, SlotValues: []uint32{9999}, Amount: 5}}
	if _, err := uc.ReportFacts(context.Background(), testPlayer, facts, "k1"); err != nil {
		t.Fatalf("report: %v", err)
	}
	if got := repo.state.Active[1].Progress[0]; got != 0 {
		t.Fatalf("槽位不匹配却推进了进度: %d", got)
	}
}

// 累加 + 达标 clamp + 已达标不再累加。
func TestProgressAccumulatesAndClamps(t *testing.T) {
	cat := baseCatalog()
	// 让任务 1 不自动完成(目标 3),先推 2 再推 5 验 clamp。
	cat.missions[1] = mission(1, 10, 1, "101", "", "", 0, 0)
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 2}}, "k1"); err != nil {
		t.Fatalf("report1: %v", err)
	}
	if got := repo.state.Active[1].Progress[0]; got != 2 {
		t.Fatalf("首次进度 %d, want 2", got)
	}
	// 再推 5 → 达标后 clamp 到 3(而非 7),且任务完成移出活跃。
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 5}}, "k2"); err != nil {
		t.Fatalf("report2: %v", err)
	}
	if _, still := repo.state.Active[1]; still {
		t.Fatal("全条件达标后仍在活跃列表")
	}
	if _, done := repo.state.Done[1]; !done {
		t.Fatal("达标后未进完成集")
	}
}

// 事实护栏:空槽位值 / amount=0 / 未知类别 一律零副作用(D 版护栏移植)。
func TestFactGuards(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	facts := []Fact{
		{Category: 1, SlotValues: nil, Amount: 5},            // 空槽位值
		{Category: 1, SlotValues: []uint32{5001}, Amount: 0}, // 零增量
		{Category: 0, SlotValues: []uint32{5001}, Amount: 5}, // 未知类别
		{Category: 7, SlotValues: []uint32{5001}, Amount: 5}, // 无任务关注的类别
	}
	if _, err := uc.ReportFacts(context.Background(), testPlayer, facts, "k1"); err != nil {
		t.Fatalf("report: %v", err)
	}
	if got := repo.state.Active[1].Progress[0]; got != 0 {
		t.Fatalf("护栏事实推进了进度: %d", got)
	}
}

// 幂等:同键同内容 already=true 且不重复入账。
func TestFactIdempotency(t *testing.T) {
	cat := baseCatalog()
	cat.missions[1] = mission(1, 10, 1, "101", "", "", 0, 0)
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	facts := []Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 2}}
	if already, err := uc.ReportFacts(context.Background(), testPlayer, facts, "same"); err != nil || already {
		t.Fatalf("首次 already=%v err=%v", already, err)
	}
	already, err := uc.ReportFacts(context.Background(), testPlayer, facts, "same")
	if err != nil || !already {
		t.Fatalf("重放 already=%v err=%v, want true/nil", already, err)
	}
	if got := repo.state.Active[1].Progress[0]; got != 2 {
		t.Fatalf("重放导致重复入账: %d", got)
	}
}

// ── 完成扇出 ────────────────────────────────────────────────────────────────

// 自动发奖 → reward_log;自动接后续链;COMPLETE_MISSION 再入让链上任务连锁完成。
func TestFanoutAutoRewardChainAndReentry(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	exp := &fakeExpGranter{}
	uc := newTestUsecase(t, baseCatalog(), repo, items, exp, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	// 一次推满任务 1 → 完成 → 发奖(auto)+ 接任务 2 → COMPLETE_MISSION 再入 → 任务 2 也完成。
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 3}}, "k1"); err != nil {
		t.Fatalf("report: %v", err)
	}
	if _, done := repo.state.Done[1]; !done {
		t.Fatal("任务 1 未完成")
	}
	if _, done := repo.state.Done[2]; !done {
		t.Fatal("COMPLETE_MISSION 再入未让链上任务 2 完成")
	}
	// 任务 1 是 auto_reward → 落 reward_log;任务 2 非 auto → 标记可领,不落流水。
	if len(repo.rewardLogs) != 1 || repo.rewardLogs[0].MissionConfigID != 1 {
		t.Fatalf("发奖流水 %+v, want 仅任务 1", repo.rewardLogs)
	}
	if got := repo.state.Done[2].RewardState; got != RewardStateClaimable {
		t.Fatalf("任务 2 领奖态 %d, want CLAIMABLE", got)
	}
	if got := repo.state.Done[1].RewardState; got != RewardStateNone {
		t.Fatalf("任务 1(自动发)领奖态 %d, want NONE", got)
	}
	// 自动发奖在事务提交后同步尝试:道具与经验都应已投递。
	if len(items.stacks) != 1 || items.stacks[0].ItemConfigID != 9001 || items.stacks[0].Count != 2 {
		t.Fatalf("道具发放 %+v", items.stacks)
	}
	if exp.total != 100 {
		t.Fatalf("经验发放 %d, want 100", exp.total)
	}
	if len(exp.keys) != 1 || exp.keys[0] != fmt.Sprintf("quest:%d:1", testPlayer) {
		t.Fatalf("经验幂等键 %v", exp.keys)
	}
}

// 链上任务接取失败(已完成)只跳过,不阻断本次完成扇出。
func TestFanoutChainAcceptSkipDoesNotBlock(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	repo.state.Done[2] = &DoneMission{MissionConfigID: 2} // 后续任务已完成过
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 3}}, "k1"); err != nil {
		t.Fatalf("链上接取失败不应让整批失败: %v", err)
	}
	if _, done := repo.state.Done[1]; !done {
		t.Fatal("任务 1 应正常完成")
	}
}

// 扇出内部队列有界:人造自环配置(加载期本会被拒)不得让引擎死循环。
func TestFanoutBoundedByRoundCap(t *testing.T) {
	cat := baseCatalog()
	// A 完成 → 接 B;B 的条件是「完成 A」→ 立即完成 → 接 A(已完成,跳过)。
	// 再叠一层互指链,验证轮次上限而非无限展开。
	cat.missions[20] = mission(20, 30, 0, "101", "", "21", 0, 0)
	cat.missions[21] = mission(21, 31, 0, "120", "", "22", 0, 0)
	cat.missions[22] = mission(22, 32, 0, "121", "", "", 0, 0)
	cat.conditions[120] = cond(120, 8, 1, "20")
	cat.conditions[121] = cond(121, 8, 1, "21")
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 20); err != nil {
		t.Fatalf("接取: %v", err)
	}
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 3}}, "k1"); err != nil {
		t.Fatalf("report: %v", err)
	}
	// 三级链应全部完成(远小于 16 轮上限),且函数正常返回(不死循环)。
	for _, id := range []uint32{20, 21, 22} {
		if _, done := repo.state.Done[id]; !done {
			t.Fatalf("链上任务 %d 未完成", id)
		}
	}
}

// GM 批量完成:只置完成清活跃,不发奖不接链不再入(与正常完成刻意分离)。
func TestCompleteAllHasNoSideEffects(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	exp := &fakeExpGranter{}
	uc := newTestUsecase(t, baseCatalog(), repo, items, exp, nil)
	if _, err := uc.Accept(context.Background(), testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	n, err := uc.CompleteAll(context.Background(), testPlayer)
	if err != nil || n != 1 {
		t.Fatalf("CompleteAll n=%d err=%v", n, err)
	}
	if len(repo.state.Active) != 0 {
		t.Fatal("活跃列表未清空")
	}
	if _, done := repo.state.Done[1]; !done {
		t.Fatal("未置完成")
	}
	if len(repo.rewardLogs) != 0 || len(items.stacks) != 0 || exp.total != 0 {
		t.Fatalf("GM 路径产生了发奖副作用: logs=%d items=%+v exp=%d", len(repo.rewardLogs), items.stacks, exp.total)
	}
	if _, chained := repo.state.Active[2]; chained {
		t.Fatal("GM 路径不应自动接后续链")
	}
	if got := repo.state.Done[1].RewardState; got != RewardStateNone {
		t.Fatalf("GM 完成不应标记可领: %d", got)
	}
}

// ── 领奖 ────────────────────────────────────────────────────────────────────

func TestClaimCASAndRepeat(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	repo.state.Done[2] = &DoneMission{MissionConfigID: 2, RewardState: RewardStateClaimable}
	exp := &fakeExpGranter{}
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, exp, nil)

	if err := uc.Claim(context.Background(), testPlayer, 2); err != nil {
		t.Fatalf("首次领取: %v", err)
	}
	if got := repo.state.Done[2].RewardState; got != RewardStateClaimed {
		t.Fatalf("领取后状态 %d, want CLAIMED", got)
	}
	if exp.total != 50 {
		t.Fatalf("领取应发放经验 50, got %d", exp.total)
	}
	// 重复领取被拒。
	if got := errcode.As(uc.Claim(context.Background(), testPlayer, 2)); got != errcode.ErrMissionNotClaimable {
		t.Fatalf("重复领取 code=%d, want ErrMissionNotClaimable", got)
	}
	// 未完成任务领取被拒。
	if got := errcode.As(uc.Claim(context.Background(), testPlayer, 1)); got != errcode.ErrMissionNotClaimable {
		t.Fatalf("未完成领取 code=%d, want ErrMissionNotClaimable", got)
	}
}

// ── 发放路由 ────────────────────────────────────────────────────────────────

// 装备走 GrantInstances;满包转邮件传同一 inst 键(直发链与邮件链至多一次)。
func TestDeliverRoutesEquipmentAndOverflowMail(t *testing.T) {
	cat := baseCatalog()
	cat.equipment[9001] = true
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{capacityFull: true}
	mail := &fakeMail{}
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, mail)

	row := &RewardLogRow{ID: 1, PlayerID: testPlayer, MissionConfigID: 1,
		Key: fmt.Sprintf("mission:%d:1", testPlayer)}
	record := &missionv1.MissionRewardStorageRecord{
		Items: []*missionv1.MissionRewardItem{{ItemConfigId: 9001, Count: 2}},
	}
	pb, err := marshalRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	row.RewardPB = pb

	if derr := uc.deliver(context.Background(), cat, row); derr != nil {
		t.Fatalf("deliver: %v", derr)
	}
	if len(items.instances) != 2 {
		t.Fatalf("装备实例发放 %v, want 2 件", items.instances)
	}
	if len(items.stacks) != 0 {
		t.Fatalf("装备不该走 GrantItems: %+v", items.stacks)
	}
	wantKey := row.Key + ":inst"
	if mail.key != wantKey {
		t.Fatalf("溢出邮件 grant key %q, want %q(须与直发同键)", mail.key, wantKey)
	}
	if len(mail.sent) != 2 {
		t.Fatalf("溢出邮件件数 %v", mail.sent)
	}
}

// 装备数量是按件展开的切片长度 —— 坏快照必须 fail-closed 拒发,不能照着数量分配内存。
//
// 回归背景:reward_pb 是历史快照,可能来自早于 configtable.MaxRewardEquipmentInstances
// 的批次,也可能是道具在热更里由堆叠改成了装备。加载期上限拦不住这两种,而补扫会
// **每轮**重放同一行 —— 没有这道运行期闸,一条 count=1e8 的坏快照会周期性打爆内存。
func TestDeliverRejectsOversizedEquipmentCount(t *testing.T) {
	cat := baseCatalog()
	cat.equipment[9001] = true
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, nil)

	record := &missionv1.MissionRewardStorageRecord{
		Items: []*missionv1.MissionRewardItem{
			{ItemConfigId: 9001, Count: configtable.MaxRewardEquipmentInstances + 1},
		},
	}
	pb, err := marshalRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	row := &RewardLogRow{ID: 11, PlayerID: testPlayer, MissionConfigID: 1, Key: "k", RewardPB: pb}
	if derr := uc.deliver(context.Background(), cat, row); derr == nil {
		t.Fatal("超上限的装备数量必须拒发(否则按数量展开切片 → OOM)")
	}
	if len(items.instances) != 0 {
		t.Fatalf("拒发后不该有实例发放: %v", items.instances)
	}

	// 多条同为装备的奖励项,合计超上限同样要拒(单条各自合规不代表合计合规)。
	half := uint32(configtable.MaxRewardEquipmentInstances/2 + 1)
	cat.equipment[9002] = true
	record = &missionv1.MissionRewardStorageRecord{
		Items: []*missionv1.MissionRewardItem{
			{ItemConfigId: 9001, Count: half},
			{ItemConfigId: 9002, Count: half},
		},
	}
	if pb, err = marshalRecord(record); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	row = &RewardLogRow{ID: 12, PlayerID: testPlayer, MissionConfigID: 1, Key: "k2", RewardPB: pb}
	if derr := uc.deliver(context.Background(), cat, row); derr == nil {
		t.Fatal("合计超上限的装备数量必须拒发")
	}

	// 边界内照常发放(闸不能误伤正常奖励)。
	record = &missionv1.MissionRewardStorageRecord{
		Items: []*missionv1.MissionRewardItem{
			{ItemConfigId: 9001, Count: configtable.MaxRewardEquipmentInstances},
		},
	}
	if pb, err = marshalRecord(record); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	row = &RewardLogRow{ID: 13, PlayerID: testPlayer, MissionConfigID: 1, Key: "k3", RewardPB: pb}
	if derr := uc.deliver(context.Background(), cat, row); derr != nil {
		t.Fatalf("恰好等于上限应放行: %v", derr)
	}
	if len(items.instances) != int(configtable.MaxRewardEquipmentInstances) {
		t.Fatalf("发放件数 %d, want %d", len(items.instances), configtable.MaxRewardEquipmentInstances)
	}
}

// 任一类发放失败 → 整条不 GRANTED(留补扫全量重放,下游幂等键去重)。
func TestGrantOneMarksFailedOnPartialFailure(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{failStacks: true}
	cat := baseCatalog()
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, nil)

	record := &missionv1.MissionRewardStorageRecord{
		Items: []*missionv1.MissionRewardItem{{ItemConfigId: 9001, Count: 1}},
		Exp:   10,
	}
	pb, err := marshalRecord(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	row := &RewardLogRow{ID: 7, PlayerID: testPlayer, MissionConfigID: 1, Key: "k", RewardPB: pb}
	if gerr := uc.grantOne(context.Background(), cat, row); gerr == nil {
		t.Fatal("道具发放失败应返回错误")
	}
	if granted, ok := repo.marked[7]; !ok || granted {
		t.Fatalf("应标记 FAILED(granted=false), got ok=%v granted=%v", ok, granted)
	}
}

// ── 溢出边界 ────────────────────────────────────────────────────────────────

func TestSaturatingAdd(t *testing.T) {
	const max = ^uint32(0)
	if got := saturatingAdd(max-1, 5); got != max {
		t.Fatalf("饱和加法 %d, want %d", got, max)
	}
	if got := saturatingAdd(3, 4); got != 7 {
		t.Fatalf("普通加法 %d, want 7", got)
	}
}

// ── 复核回归(2026-08-11 对抗式复核确认的 P1,每条都是"改回旧写法即红")────────────

// [P1-2] 发放路由必须用**快照冻结位**,不能在发放时回读道具表。
//
// 失效链(修复前真实存在):首投时 X 是堆叠 → 走 GrantItems、幂等键 `<key>:stack`,
// inventory 已入账;随后 MarkReward 失败 / 经验段失败 / 进程被杀 → 行停在非 GRANTED;
// 期间配置把 X 由堆叠改成装备 → 补扫重放 → 回读道具表得 equipment=true → 改走
// GrantInstances、键变成 `<key>:inst` → inventory 查无此键 → **同一份奖励第二次发放**。
// 幂等键防不住,因为压根不是同一个键。
//
// 本例把「快照之后配置翻转」压缩成:落快照时 catalog 说堆叠 → 翻成装备 → 再 deliver。
// 断言仍走 stack 路由。改回 `equipment := uc.catalog.IsEquipment(...)` 本测试立刻红。
func TestDeliverUsesFrozenRouteAcrossCatalogFlip(t *testing.T) {
	cat := baseCatalog()
	// 落快照这一刻:9001 是可堆叠道具。
	cat.equipment[9001] = false
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, nil)

	entry, err := uc.buildRewardLog(cat, testPlayer, cat.missions[1])
	if err != nil {
		t.Fatalf("buildRewardLog: %v", err)
	}
	if entry == nil {
		t.Fatal("任务 1 配了 reward_id,应产出发奖流水")
	}

	// 配置热更 / 滚动升级期新批次:9001 被改成装备。
	cat.equipment[9001] = true

	row := &RewardLogRow{
		ID: 1, PlayerID: testPlayer, MissionConfigID: entry.MissionConfigID,
		Key: entry.Key, RewardPB: entry.RewardPB,
	}
	if derr := uc.deliver(context.Background(), cat, row); derr != nil {
		t.Fatalf("deliver: %v", derr)
	}

	if len(items.instances) != 0 {
		t.Fatalf("翻转后仍按当前道具表走了实例路由 %v —— 冻结位没生效,会换幂等键重发", items.instances)
	}
	if len(items.stacks) != 1 || items.stacks[0].ItemConfigID != 9001 {
		t.Fatalf("应沿用快照冻结的堆叠路由,实得 %+v", items.stacks)
	}
}

// [P1-2 反向] 快照冻结的是「装备」时,即便配置改回堆叠也仍走实例路由。
func TestDeliverFrozenRouteHoldsForEquipmentToStackFlip(t *testing.T) {
	cat := baseCatalog()
	cat.equipment[9001] = true // 落快照时是装备
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, nil)

	entry, err := uc.buildRewardLog(cat, testPlayer, cat.missions[1])
	if err != nil || entry == nil {
		t.Fatalf("buildRewardLog: %v entry=%v", err, entry)
	}
	cat.equipment[9001] = false // 热更改回堆叠

	row := &RewardLogRow{ID: 2, PlayerID: testPlayer, MissionConfigID: entry.MissionConfigID,
		Key: entry.Key, RewardPB: entry.RewardPB}
	if derr := uc.deliver(context.Background(), cat, row); derr != nil {
		t.Fatalf("deliver: %v", derr)
	}
	if len(items.stacks) != 0 {
		t.Fatalf("翻转后走了堆叠路由 %+v —— 冻结位没生效", items.stacks)
	}
	if len(items.instances) != 2 {
		t.Fatalf("应沿用快照冻结的实例路由(count=2 → 2 件),实得 %v", items.instances)
	}
}

// [P1-3] 给已上线任务**加一条条件**,存量玩家不得跳过新条件直接完成。
//
// 失效链(修复前真实存在):任务 M 上线时 condition_ids="101",玩家接取 → progress 长度 1。
// 热更改成 "101,102" 后,玩家打满 101:推进与达标判定两处都取 min(len(condIDs)=2,
// len(Progress)=1)=1 → 只检查槽 0 → 判全条件满足 → **完成 + 发奖**,条件 102 一次没查过。
//
// 修复=推进前 alignProgressSlots 补零扩容 + 判定改为逐 condIDs 且槽不足即 fail-closed。
// 把 allConditionsFulfilled 改回 min 语义,本测试立刻红。
func TestHotAddedConditionIsNotSkipped(t *testing.T) {
	cat := baseCatalog()
	// 任务 20:上线时单条件(101 = 杀 5001 怪 ×3),无后续链,自动发奖。
	cat.missions[20] = mission(20, 70, 0, "101", "", "", 61, 1)
	repo := newFakeRepo(testPlayer)
	items := &fakeItemGranter{}
	uc := newTestUsecase(t, cat, repo, items, &fakeExpGranter{}, nil)

	if _, err := uc.Accept(context.Background(), testPlayer, 20); err != nil {
		t.Fatalf("接取: %v", err)
	}
	if got := len(repo.state.Active[20].Progress); got != 1 {
		t.Fatalf("接取时进度槽数 %d, want 1(上线时单条件)", got)
	}

	// —— 热更:给任务 20 追加条件 102(完成任务 1)——
	cat.missions[20] = mission(20, 70, 0, "101,102", "", "", 61, 1)

	// 玩家把原条件 101 打满。
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{5001}, Amount: 3}}, "k1"); err != nil {
		t.Fatalf("report: %v", err)
	}

	if _, done := repo.state.Done[20]; done {
		t.Fatal("只打满旧条件就完成了任务 —— 新增条件被整段跳过(白送完成)")
	}
	am, still := repo.state.Active[20]
	if !still {
		t.Fatal("任务应仍在活跃列表")
	}
	if len(am.Progress) != 2 {
		t.Fatalf("进度槽未补齐到当前配置槽数:%d, want 2", len(am.Progress))
	}
	if am.Progress[0] != 3 {
		t.Fatalf("已有进度必须原样保留,槽0=%d want 3", am.Progress[0])
	}
	if am.Progress[1] != 0 {
		t.Fatalf("新条件必须从 0 开始,槽1=%d", am.Progress[1])
	}
	if len(repo.rewardLogs) != 0 {
		t.Fatalf("未真正完成却落了发奖流水: %+v", repo.rewardLogs)
	}

	// 补做新条件 → 这次才真完成。
	if _, err := uc.ReportFacts(context.Background(), testPlayer,
		[]Fact{{Category: 8, SlotValues: []uint32{1}, Amount: 1}}, "k2"); err != nil {
		t.Fatalf("report2: %v", err)
	}
	if _, done := repo.state.Done[20]; !done {
		t.Fatal("两个条件都做完后应完成")
	}
}

// ── 比较符 GT:钳位必须落在「最小达标值」(本次修复的回归判据)────────────────

// 旧实现把达标进度无脑钳回 target,于是 GT 条件被钳成不达标:
// target=5 的 GT 条件,进度推到 6 达标 → 钳回 5 → 再判 `5 > 5` 为假 → 任务不完成,
// 而进度已写死在 5;下一条事实推到 6 又被钳回 5 —— **永久活锁,任务 100% 完不成**。
// 钳位的不变量是「钳位不得改变达标与否」,GT 的最小达标值是 target+1。
//
// 把 ConditionClampIfFulfilled 改回 `return target`,本用例必红。
func TestGreaterThanConditionCompletesInsteadOfLivelocking(t *testing.T) {
	cat := &fakeCatalog{
		missions: map[uint32]*configpb.MissionRow{
			1: mission(1, 10, 0, "101", "", "", 0, 0),
		},
		conditions: map[uint32]*configpb.ConditionRow{
			101: {
				Id: 101, Name: "杀怪超过5只", ConditionCategory: 1,
				TargetCount: 5, ComparisonOp: configtable.ConditionCompareGT, Slot1: "9",
			},
		},
	}
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	ctx := context.Background()

	if _, err := uc.Accept(ctx, testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	// 逐只上报,直到超过目标。6 只就该完成(6 > 5)。
	for i := 0; i < 8; i++ {
		facts := []Fact{{Category: 1, SlotValues: []uint32{9}, Amount: 1}}
		if _, err := uc.ReportFacts(ctx, testPlayer, facts, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("第 %d 次上报: %v", i+1, err)
		}
		if _, done := repo.state.Done[1]; done {
			if am := repo.state.Active[1]; am != nil {
				t.Fatalf("完成后不应仍在活跃列表")
			}
			return // 完成即通过
		}
	}
	am := repo.state.Active[1]
	t.Fatalf("杀了 8 只(目标 >5)任务仍未完成,进度钉死在 %v —— 钳位把达标打回了未达标", am.Progress)
}

// GE 对照组:钳位行为逐字节不变(最小达标值 = target)。
func TestGreaterEqualConditionClampsToTargetUnchanged(t *testing.T) {
	cat := &fakeCatalog{
		missions: map[uint32]*configpb.MissionRow{
			1: mission(1, 10, 0, "101", "", "", 0, 0),
		},
		conditions: map[uint32]*configpb.ConditionRow{
			101: {Id: 101, Name: "杀5只", ConditionCategory: 1, TargetCount: 5, Slot1: "9"},
		},
	}
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, cat, repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	ctx := context.Background()

	if _, err := uc.Accept(ctx, testPlayer, 1); err != nil {
		t.Fatalf("接取: %v", err)
	}
	// 单次 amount=8 超冲:必须钳到 5 且判定完成。
	if _, err := uc.ReportFacts(ctx, testPlayer,
		[]Fact{{Category: 1, SlotValues: []uint32{9}, Amount: 8}}, "k"); err != nil {
		t.Fatalf("上报: %v", err)
	}
	if _, done := repo.state.Done[1]; !done {
		t.Fatal("GE 目标 5、上报 8,应完成")
	}
}

// ── 推送发布器单写者(本次修复的回归判据)────────────────────────────────────

// fakePushLease 可切换的领导权来源。
type fakePushLease struct{ held bool }

func (l *fakePushLease) Current() (uint64, bool) { return 1, l.held }

// 未当选的副本一行都不许发:出箱是全局未分区表,两个发布器并发投递会交错,
// 而 progressed 是逐任务全量快照(后到即覆盖),客户端进度条会回退。
func TestPushPublisherOnlyRunsOnLeader(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	repo.pushOutbox = []*PushOutboxRow{
		{ID: 1, PlayerID: testPlayer, Payload: []byte("a")},
		{ID: 2, PlayerID: testPlayer, Payload: []byte("b")},
	}
	pusher := &fakePusher{}
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	uc.pusher = pusher

	lease := &fakePushLease{held: false}
	uc.SetPushWriterLease(lease)

	if uc.pushIsLeader() {
		t.Fatal("未当选副本不得被判为 leader")
	}
	// 热备副本这一拍什么都不做。
	if len(pusher.sent) != 0 {
		t.Fatalf("热备副本不得投递,got %d", len(pusher.sent))
	}

	// 当选后才真正排空。
	lease.held = true
	if !uc.pushIsLeader() {
		t.Fatal("当选副本必须被判为 leader")
	}
	uc.publishPushBatch(context.Background())
	if len(pusher.sent) != 2 {
		t.Fatalf("当选副本应排空 2 行,got %d", len(pusher.sent))
	}
	if len(repo.pushOutbox) != 0 {
		t.Fatalf("投递成功的行应被删除,剩 %d", len(repo.pushOutbox))
	}
}

// 未注入租约时恒为 leader(单进程 dev / 单副本 Recreate 的历史形态,行为不变)。
func TestPushPublisherWithoutLeaseAlwaysPublishes(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	if !uc.pushIsLeader() {
		t.Fatal("未注入租约时必须恒为 leader,否则 dev 单进程一行都发不出去")
	}
}

// 删行命中 0 行 = 另一副本抢先删了 → 必须报错中断本轮,而不是当成投递成功静默继续。
// 这是"两个发布器在打架"唯一可观测的信号。
func TestPushPublisherStopsWhenRowVanished(t *testing.T) {
	repo := newFakeRepo(testPlayer)
	repo.pushOutbox = []*PushOutboxRow{
		{ID: 1, PlayerID: testPlayer, Payload: []byte("a")},
		{ID: 2, PlayerID: testPlayer, Payload: []byte("b")},
	}
	repo.deleteVanishes = true // 模拟另一副本已删
	pusher := &fakePusher{}
	uc := newTestUsecase(t, baseCatalog(), repo, &fakeItemGranter{}, &fakeExpGranter{}, nil)
	uc.pusher = pusher

	uc.publishPushBatch(context.Background())
	if len(pusher.sent) != 1 {
		t.Fatalf("撞上竞争应在第一行后立即中断本轮,got %d 行被投递", len(pusher.sent))
	}
}
