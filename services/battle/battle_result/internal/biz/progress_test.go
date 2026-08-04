// progress_test.go — 实时进度通道业务逻辑单测(实时成长,realtime-progression.md)。
//
// 覆盖:校验矩阵 / 换算聚合 / 水位重放 / 已结算拒收 / 出箱发布幂等键 /
// 结算防双发(掉落发放单一权威路径)。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// fakeMonsterExp 测试用怪物击杀经验表(生产实现是 configtable 的 role_level 表)。
// 键是怪物角色 ID;等级 0 按 1 级处理(与生产 KillExpOf 同口径),非 1 级需显式登记。
type fakeMonsterExp map[uint32]map[uint32]uint64 // roleID → level → exp

func (f fakeMonsterExp) KillExpOf(roleID, level uint32) (uint64, bool) {
	if level == 0 {
		level = 1
	}
	byLevel, ok := f[roleID]
	if !ok {
		return 0, false
	}
	exp, ok := byLevel[level]
	return exp, ok
}

// lv1Exp 把「角色 ID → 经验」简写成只含 1 级的经验表(绝大多数用例只关心 1 级)。
func lv1Exp(m map[uint32]uint64) fakeMonsterExp {
	out := make(fakeMonsterExp, len(m))
	for roleID, exp := range m {
		out[roleID] = map[uint32]uint64{1: exp}
	}
	return out
}

// withMonsterExp 给 usecase 注入经验表并回传,便于在构造处一行接线。
func withMonsterExp(uc *BattleResultUsecase, exp fakeMonsterExp) *BattleResultUsecase {
	uc.SetMonsterExpTable(exp)
	return uc
}

// progressUsecase 构造带怪物经验表 + 掉落白名单的测试 usecase(通道显式开启:
// progress_enabled 缺省 false,§14.2 默认不改变现有行为)。
func progressUsecase(repo data.BattleRepo) *BattleResultUsecase {
	cfg := conf.BattleConf{
		EloKFactor: 32, BaseMMR: 1500,
		ProgressEnabled: true,
		DropWhitelist:   []uint32{5001, 5002},
	}
	return withMonsterExp(
		NewBattleResultUsecase(repo, NewStaticMMRReader(cfg.BaseMMR), &fakePusher{}, nil, cfg),
		lv1Exp(map[uint32]uint64{101: 10, 102: 25}))
}

func killEvent(seq, playerID uint64, monsterID, count uint32) *battlev1.BattleProgressEvent {
	return &battlev1.BattleProgressEvent{
		Seq: seq, PlayerId: playerID,
		Fact: &battlev1.BattleProgressEvent_MonsterKill{
			MonsterKill: &battlev1.MonsterKillFact{MonsterConfigId: monsterID, Count: count},
		},
	}
}

func pickupEvent(seq, playerID uint64, itemID, count uint32) *battlev1.BattleProgressEvent {
	return &battlev1.BattleProgressEvent{
		Seq: seq, PlayerId: playerID,
		Fact: &battlev1.BattleProgressEvent_ItemPickup{
			ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: itemID, Count: count},
		},
	}
}

func TestReportProgress_ValidationMatrix(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7, 8}

	cases := []struct {
		name   string
		events []*battlev1.BattleProgressEvent
		want   errcode.Code
	}{
		{"empty events", nil, errcode.ErrInvalidArg},
		{"seq zero", []*battlev1.BattleProgressEvent{killEvent(0, 7, 101, 1)}, errcode.ErrInvalidArg},
		{"seq not ascending", []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1), killEvent(2, 7, 101, 1)}, errcode.ErrInvalidArg},
		{"seq over cap", []*battlev1.BattleProgressEvent{killEvent(1_000_000, 7, 101, 1)}, errcode.ErrInvalidArg},
		{"player missing", []*battlev1.BattleProgressEvent{killEvent(1, 0, 101, 1)}, errcode.ErrInvalidArg},
		{"player not in roster", []*battlev1.BattleProgressEvent{killEvent(1, 999, 101, 1)}, errcode.ErrUnauthorized},
		{"kill count zero", []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 0)}, errcode.ErrInvalidArg},
		{"kill count over cap", []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 101)}, errcode.ErrInvalidArg},
		{"pickup count over cap", []*battlev1.BattleProgressEvent{pickupEvent(1, 7, 5001, 11)}, errcode.ErrInvalidArg},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := uc.ReportProgress(ctx, 900, roster, tc.events); errcode.As(err) != tc.want {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	if _, err := uc.ReportProgress(ctx, 0, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("match_id=0 want ErrInvalidArg, got %v", err)
	}
	// 坏批被拒必须零副作用(水位不动、无出箱行)。
	if seq := repo.progressSeq[900]; seq != 0 {
		t.Fatalf("rejected batches must not advance watermark, got %d", seq)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("rejected batches must not enqueue outbox, got %d", len(repo.progressOutbox))
	}
}

func TestReportProgress_DisabledByDefault(t *testing.T) {
	// 零值配置 = 通道关闭(§14.2:默认不改变现有行为;混版发布纪律见 conf.ProgressEnabled)。
	cfg := conf.BattleConf{}
	uc := NewBattleResultUsecase(newFakeRepo(), NewStaticMMRReader(1500), &fakePusher{}, nil, cfg)
	_, err := uc.ReportProgress(context.Background(), 900, nil, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)})
	if errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("disabled channel want ErrInvalidState, got %v", err)
	}
}

func TestReportProgress_KillswitchKeepsInFlightMatches(t *testing.T) {
	// 每场模式以水位行固化:通道中途关闭,已开流对局必须继续收流(否则该场后续拾取
	// 被拒 + 结算掉落又因水位>0 被抑制 → 永久丢奖,审计 P1)。
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7}

	if _, err := uc.ReportProgress(ctx, 905, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// 关闭开关(同一 repo,新 usecase 模拟配置翻转 / Stable 关 Canary 开的混配副本)。
	off := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, conf.BattleConf{
		DropWhitelist: []uint32{5001},
	}), lv1Exp(map[uint32]uint64{101: 10}))
	if acked, err := off.ReportProgress(ctx, 905, roster, []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1)}); err != nil || acked != 2 {
		t.Fatalf("in-flight match must keep streaming after killswitch, acked=%d err=%v", acked, err)
	}
	// 新对局在关闭副本上不得开流。
	if _, err := off.ReportProgress(ctx, 906, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("new match on disabled replica want ErrInvalidState, got %v", err)
	}
}

func TestReportProgress_AggregatesAndAcks(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7, 8}

	events := []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 101, 2), // 7: 2×10 = 20 exp
		killEvent(2, 7, 102, 1), // 7: +25 → 45 exp
		killEvent(3, 8, 101, 1), // 8: 10 exp
		pickupEvent(4, 7, 5001, 2),
		pickupEvent(5, 7, 9999, 1), // 非白名单 → 跳过发放,水位照常推进
		killEvent(6, 8, 777, 1),    // 未配置怪物 → 跳过发放
	}
	acked, err := uc.ReportProgress(ctx, 900, roster, events)
	if err != nil || acked != 6 {
		t.Fatalf("acked=%d err=%v, want 6/nil", acked, err)
	}
	if repo.progressSeq[900] != 6 {
		t.Fatalf("watermark=%d want 6", repo.progressSeq[900])
	}
	// 聚合:7 的 exp 一行(45,seq=批末 6)+ 8 的 exp 一行(10,seq=6)
	// + 7 的 item 一行([5001,5001],seq=事实自身 4,每拾取事实一行,审计 P1 不截断)。
	var exp7, exp8 uint64
	var items7 []uint32
	for _, row := range repo.progressOutbox {
		switch {
		case row.Kind == data.ProgressGrantExp && row.PlayerID == 7:
			exp7 = row.ExpDelta
			if row.Seq != 6 {
				t.Fatalf("exp row seq=%d want batch-end 6", row.Seq)
			}
		case row.Kind == data.ProgressGrantExp && row.PlayerID == 8:
			exp8 = row.ExpDelta
			if row.Seq != 6 {
				t.Fatalf("exp row seq=%d want batch-end 6", row.Seq)
			}
		case row.Kind == data.ProgressGrantItem && row.PlayerID == 7:
			items7 = row.ItemConfigIDs
			if row.Seq != 4 {
				t.Fatalf("item row seq=%d want fact seq 4", row.Seq)
			}
		default:
			t.Fatalf("unexpected outbox row %+v", row)
		}
	}
	if exp7 != 45 || exp8 != 10 {
		t.Fatalf("exp7=%d exp8=%d, want 45/10", exp7, exp8)
	}
	if len(items7) != 2 || items7[0] != 5001 || items7[1] != 5001 {
		t.Fatalf("items7=%v, want [5001 5001]", items7)
	}
	if repo.progressExp[900] != 55 || repo.progressItems[900] != 2 {
		t.Fatalf("cumulative exp=%d items=%d, want 55/2", repo.progressExp[900], repo.progressItems[900])
	}

	// 原批重发(at-least-once)→ 纯重放 ACK,零副作用。
	rows := len(repo.progressOutbox)
	acked2, err := uc.ReportProgress(ctx, 900, roster, events)
	if err != nil || acked2 != 6 {
		t.Fatalf("replay acked=%d err=%v, want 6/nil", acked2, err)
	}
	if len(repo.progressOutbox) != rows {
		t.Fatalf("replay must not enqueue outbox: %d → %d", rows, len(repo.progressOutbox))
	}

	// 续批(部分旧 + 部分新)→ 只入账新事件。
	next := []*battlev1.BattleProgressEvent{
		killEvent(6, 8, 777, 1), // 旧(≤水位)
		killEvent(7, 8, 101, 3), // 新:8 +30 exp
	}
	acked3, err := uc.ReportProgress(ctx, 900, roster, next)
	if err != nil || acked3 != 7 {
		t.Fatalf("next acked=%d err=%v, want 7/nil", acked3, err)
	}
	if len(repo.progressOutbox) != rows+1 {
		t.Fatalf("next batch rows=%d want %d", len(repo.progressOutbox), rows+1)
	}
}

func TestReportProgress_UnknownFactStopsStream(t *testing.T) {
	// 未知事实类型(新 DS 新 fact 打到旧 Go)= 整场性质的能力不匹配:必须整批拒且
	// 返回 ErrInvalidState(DS 停流;停流后本场剩余实时奖励按契约明示永久丢失,
	// 不结算兜底——只该出现在违反 Go 先行发布纪律时)。不得"跳过发放但推进水位"
	// (静默丢失),也不得 ErrInvalidArg(丢批继续 = 后续每批照样丢,逐批永久丢失,审计 P1)。
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	events := []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 101, 1),
		{Seq: 2, PlayerId: 7}, // oneof 未设置 = 本副本不认识的事实类型
	}
	if _, err := uc.ReportProgress(context.Background(), 904, []uint64{7}, events); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("unknown fact want ErrInvalidState (stop stream), got %v", err)
	}
	if repo.progressSeq[904] != 0 || len(repo.progressOutbox) != 0 {
		t.Fatalf("rejected batch must be side-effect free: seq=%d rows=%d", repo.progressSeq[904], len(repo.progressOutbox))
	}
	// 停流标记必须持久化(审计 P1):违纪 DS 随后只发已知事实的批也一律拒,
	// 禁止重新开流(否则"整场停流"契约被绕过)。
	if !repo.progressStopped[904] {
		t.Fatal("unknown fact must persist the stopped marker")
	}
	if _, err := uc.ReportProgress(context.Background(), 904, []uint64{7},
		[]*battlev1.BattleProgressEvent{killEvent(3, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("known-fact batch after stop must stay rejected, got %v", err)
	}
	if repo.progressSeq[904] != 0 || len(repo.progressOutbox) != 0 {
		t.Fatalf("post-stop batch must be side-effect free: seq=%d rows=%d", repo.progressSeq[904], len(repo.progressOutbox))
	}
}

func TestReportProgress_PerMatchCumulativeCaps(t *testing.T) {
	// 单场累计上限:失陷 DS 跨大量 seq 刷产出必须在事务权威侧封顶(审计 P1)。
	repo := newFakeRepo()
	cfg := conf.BattleConf{
		ProgressEnabled: true,
		DropWhitelist:   []uint32{5001},
		// exp 上限 25:第一批 2 杀=20 过,第二批再 1 杀=10 累计 30 超限拒。
		MaxProgressExpPerMatch: 25,
		// items 上限 3:一次拾取 4 件直接超限拒。
		MaxProgressItemsPerMatch: 3,
	}
	uc := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, cfg), lv1Exp(map[uint32]uint64{101: 10}))
	ctx := context.Background()
	roster := []uint64{7}

	if acked, err := uc.ReportProgress(ctx, 907, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 2)}); err != nil || acked != 1 {
		t.Fatalf("first batch acked=%d err=%v", acked, err)
	}
	if _, err := uc.ReportProgress(ctx, 907, roster, []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("cumulative exp over cap want ErrInvalidArg, got %v", err)
	}
	if repo.progressSeq[907] != 1 || repo.progressExp[907] != 20 {
		t.Fatalf("rejected batch must not advance: seq=%d exp=%d", repo.progressSeq[907], repo.progressExp[907])
	}
	if _, err := uc.ReportProgress(ctx, 908, roster, []*battlev1.BattleProgressEvent{
		pickupEvent(1, 7, 5001, 2), pickupEvent(2, 7, 5001, 2),
	}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("cumulative items over cap want ErrInvalidArg, got %v", err)
	}
}

func TestReportProgress_PerPlayerCumulativeCaps(t *testing.T) {
	// 单场单玩家上限(realtime-progression.md 反作弊上限):只有按场累计时,
	// 失陷 DS 仍可把全场额度灌给一人,必须按玩家封顶(审计 P1)。
	repo := newFakeRepo()
	cfg := conf.BattleConf{
		ProgressEnabled: true,
		DropWhitelist:   []uint32{5001},
		// 场上限放宽,确保拦截全部来自单玩家上限。
		MaxProgressExpPerMatch:    1000,
		MaxProgressItemsPerMatch:  100,
		MaxProgressExpPerPlayer:   25,
		MaxProgressItemsPerPlayer: 3,
		MaxProgressKillsPerPlayer: 5,
	}
	uc := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, cfg), lv1Exp(map[uint32]uint64{101: 10}))
	ctx := context.Background()
	roster := []uint64{7, 8}

	// 第一批:玩家 7 两杀=20 exp,玩家 8 一杀=10,均未超单玩家上限。
	if acked, err := uc.ReportProgress(ctx, 910, roster, []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 101, 2), killEvent(2, 8, 101, 1),
	}); err != nil || acked != 2 {
		t.Fatalf("first batch acked=%d err=%v", acked, err)
	}
	// 第二批:玩家 7 再一杀累计 30 超单玩家 exp 上限 25 → 整批拒且零副作用。
	if _, err := uc.ReportProgress(ctx, 910, roster, []*battlev1.BattleProgressEvent{killEvent(3, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("per-player exp over cap want ErrInvalidArg, got %v", err)
	}
	if repo.progressSeq[910] != 2 {
		t.Fatalf("rejected batch must not advance seq, got %d", repo.progressSeq[910])
	}
	// 其他玩家额度独立:玩家 8 继续入账。
	if acked, err := uc.ReportProgress(ctx, 910, roster, []*battlev1.BattleProgressEvent{killEvent(4, 8, 101, 1)}); err != nil || acked != 4 {
		t.Fatalf("other player batch acked=%d err=%v", acked, err)
	}

	// 单玩家掉落上限:同一人 4 件超 3 → 拒。
	if _, err := uc.ReportProgress(ctx, 911, roster, []*battlev1.BattleProgressEvent{
		pickupEvent(1, 7, 5001, 2), pickupEvent(2, 7, 5001, 2),
	}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("per-player items over cap want ErrInvalidArg, got %v", err)
	}

	// 单玩家击杀上限:未配置经验的怪(999)同样计入击杀额度,6 杀超 5 → 拒
	// (失陷 DS 不能靠刷未知怪 ID 绕过反作弊额度)。
	if _, err := uc.ReportProgress(ctx, 912, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 999, 6)}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("per-player kills over cap want ErrInvalidArg, got %v", err)
	}
}

func TestReportProgress_StaleWatermarkRetryStaysRetryable(t *testing.T) {
	// 审计 P1 回归:重试请求读到旧水位(首请求已提交推进)时,绝不能在混合快照上
	// 永久拒(旧实现在事务外用 旧水位+新累计 判上限:同批 delta 被重复计入,
	// 20+20 > 25 → ErrInvalidArg → DS 丢批并释放拾取认领 → 可重复发放)。
	// 新实现上限判定在 ApplyProgress 事务内:过期期望水位在 CAS 处以 ErrUnavailable
	// 收敛,DS 重读新水位后按纯重放 ACK,零副作用。
	repo := newFakeRepo()
	cfg := conf.BattleConf{
		ProgressEnabled:         true,
		MaxProgressExpPerPlayer: 25, // 首批 2 杀=20:若同批 delta 被重复计入(40)即误超限
	}
	uc := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, cfg), lv1Exp(map[uint32]uint64{101: 10}))
	ctx := context.Background()
	roster := []uint64{7}
	batch := []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 2)}

	if acked, err := uc.ReportProgress(ctx, 920, roster, batch); err != nil || acked != 1 {
		t.Fatalf("first batch acked=%d err=%v", acked, err)
	}

	// 注入过期水位快照(seq=0 / 累计 0,行已存在):模拟重试请求的读落在首请求提交之前。
	repo.staleWatermark = &data.ProgressWatermark{Existed: true}
	if _, err := uc.ReportProgress(ctx, 920, roster, batch); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-watermark retry must stay retryable (ErrUnavailable), got %v", err)
	}
	// 零副作用:累计不变、出箱不重复。
	if repo.progressExp[920] != 20 || repo.progressPlayers[920][7].TotalExp != 20 {
		t.Fatalf("retry must not double-count: match=%d player=%d",
			repo.progressExp[920], repo.progressPlayers[920][7].TotalExp)
	}
	if len(repo.progressOutbox) != 1 {
		t.Fatalf("retry must not duplicate outbox rows, got %d", len(repo.progressOutbox))
	}

	// DS 收到 ErrUnavailable 后重读(拿到新水位)→ 纯重放 ACK 收敛。
	if acked, err := uc.ReportProgress(ctx, 920, roster, batch); err != nil || acked != 1 {
		t.Fatalf("converged replay acked=%d err=%v", acked, err)
	}
}

func TestReportProgress_ZeroExpMonsterNoOutboxRow(t *testing.T) {
	// 角色等级表里**有行但击杀经验为 0**(如玩家英雄行,或策划有意不给经验的怪):
	// 不得产生 0 额度 exp 出箱行(player 会拒收并永久重试),但击杀仍计入单玩家击杀额度。
	// 注意与"表里没有这一行"是两回事:后者走 skippedFact 告警路径(见聚合用例的怪物 777)。
	repo := newFakeRepo()
	cfg := conf.BattleConf{
		ProgressEnabled: true,
	}
	uc := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, cfg), lv1Exp(map[uint32]uint64{101: 0}))
	ctx := context.Background()

	if acked, err := uc.ReportProgress(ctx, 913, []uint64{7}, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 2)}); err != nil || acked != 1 {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("zero-exp kill must not enqueue outbox rows, got %d", len(repo.progressOutbox))
	}
	if repo.progressPlayers[913][7].TotalKills != 2 {
		t.Fatalf("kills must still accumulate, got %d", repo.progressPlayers[913][7].TotalKills)
	}
}

func TestReportProgress_RejectedAfterSettlement(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{1, 2}

	if _, err := uc.ReportProgress(ctx, 901, roster, []*battlev1.BattleProgressEvent{killEvent(1, 1, 101, 1)}); err != nil {
		t.Fatalf("progress before settle: %v", err)
	}
	// 结算(fakeRepo SaveResult 打终局标记)。
	result := &battlev1.BattleResult{
		MatchId: 901, WinnerTeam: 0,
		Stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}, {PlayerId: 2, Team: 1}},
	}
	if _, err := uc.ReportResult(ctx, result, 1); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// 迟到进度(僵尸 / 分区恢复 DS)一律拒:ErrInvalidState → DS 停流。
	if _, err := uc.ReportProgress(ctx, 901, roster, []*battlev1.BattleProgressEvent{killEvent(2, 1, 101, 1)}); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("post-settle progress want ErrInvalidState, got %v", err)
	}
}

func TestReportResult_SuppressesDropsWhenProgressStreamed(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{1, 2}

	// 本场走过实时通道(水位 >0)。
	if _, err := uc.ReportProgress(ctx, 902, roster, []*battlev1.BattleProgressEvent{pickupEvent(1, 1, 5001, 1)}); err != nil {
		t.Fatalf("progress: %v", err)
	}
	// 结算再报同一掉落(恶意 / 兼容字段)→ 必须被抑制,不得双发。
	result := &battlev1.BattleResult{
		MatchId: 902, WinnerTeam: 0,
		Stats: []*battlev1.PlayerStats{
			{PlayerId: 1, Team: 0, DroppedItemConfigIds: []uint32{5001}},
			{PlayerId: 2, Team: 1},
		},
	}
	if _, err := uc.ReportResult(ctx, result, 1); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if len(repo.dropOutbox) != 0 {
		t.Fatalf("drops must be suppressed when progress streamed, got %d rows", len(repo.dropOutbox))
	}
	// 对照:未走实时通道的场,结算掉落照常发放。
	legacy := &battlev1.BattleResult{
		MatchId: 903, WinnerTeam: 0,
		Stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0, DroppedItemConfigIds: []uint32{5001}}},
	}
	if _, err := uc.ReportResult(ctx, legacy, 0); err != nil {
		t.Fatalf("legacy settle: %v", err)
	}
	if len(repo.dropOutbox) != 1 {
		t.Fatalf("legacy drops must still flow, got %d rows", len(repo.dropOutbox))
	}
}

// fakeExpGranter 捕获 AddExperience 调用;failFirst 模拟 player 瞬时不可用。
type fakeExpGranter struct {
	calls     []grantCall
	failFirst bool
}

func (g *fakeExpGranter) AddExperience(_ context.Context, playerID uint64, expDelta uint64, _ string, key string) error {
	if g.failFirst {
		g.failFirst = false
		return simpleErr("player down")
	}
	g.calls = append(g.calls, grantCall{playerID: playerID, items: []uint32{uint32(expDelta)}, key: key})
	return nil
}

func TestPublishProgressBatch(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7}

	events := []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 101, 1),    // 10 exp
		pickupEvent(2, 7, 5002, 1), // 一件 5002
	}
	if _, err := uc.ReportProgress(ctx, 910, roster, events); err != nil {
		t.Fatalf("progress: %v", err)
	}

	expG := &fakeExpGranter{failFirst: true}
	itemG := &fakeGranter{}
	uc.SetExperienceGranter(expG)
	uc.SetInstanceGranter(itemG)

	// 第一轮:exp 行失败保留,item 行照发(单行失败不阻塞)。
	if n, err := uc.publishProgressBatch(ctx); err != nil || n != 1 {
		t.Fatalf("round1 n=%d err=%v, want 1/nil", n, err)
	}
	if len(repo.progressOutbox) != 1 {
		t.Fatalf("failed exp row must remain, got %d rows", len(repo.progressOutbox))
	}
	// 第二轮:exp 行补发成功。
	if n, err := uc.publishProgressBatch(ctx); err != nil || n != 1 {
		t.Fatalf("round2 n=%d err=%v, want 1/nil", n, err)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("all rows must be delivered, got %d", len(repo.progressOutbox))
	}
	if len(expG.calls) != 1 || expG.calls[0].playerID != 7 || expG.calls[0].key != "progress:910:2:7:exp" {
		t.Fatalf("exp grant calls wrong: %+v", expG.calls)
	}
	if len(itemG.calls) != 1 || itemG.calls[0].key != "progress:910:2:7:item" || len(itemG.calls[0].items) != 1 {
		t.Fatalf("item grant calls wrong: %+v", itemG.calls)
	}
}

func TestPublishProgressBatch_CapacityFullGoesToMail(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()

	if _, err := uc.ReportProgress(ctx, 911, []uint64{7}, []*battlev1.BattleProgressEvent{pickupEvent(1, 7, 5001, 1)}); err != nil {
		t.Fatalf("progress: %v", err)
	}
	itemG := &fakeGranter{capacityFull: true}
	mail := &fakeMailSender{}
	uc.SetInstanceGranter(itemG)
	uc.SetMailSender(mail)

	if n, err := uc.publishProgressBatch(ctx); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1/nil", n, err)
	}
	if len(mail.calls) != 1 || mail.calls[0].key != "progress:911:1:7:item" {
		t.Fatalf("overflow mail calls wrong: %+v", mail.calls)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("mailed row must be deleted, got %d", len(repo.progressOutbox))
	}
}

func TestPublishProgressBatch_NilGrantersKeepRows(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()

	if _, err := uc.ReportProgress(ctx, 912, []uint64{7}, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); err != nil {
		t.Fatalf("progress: %v", err)
	}
	// granter 全未注入:行原样积压不丢。
	if n, err := uc.publishProgressBatch(ctx); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v, want 0/nil", n, err)
	}
	if len(repo.progressOutbox) != 1 {
		t.Fatalf("rows must stay when granters missing, got %d", len(repo.progressOutbox))
	}
}

// 停流标记写失败必须保持可重试(审计 P1:吞失败返终态 InvalidState 会让 DS 永久停流
// 而库无标记,后续已知批被误收)。
func TestReportProgress_MarkStoppedFailureIsRetryable(t *testing.T) {
	repo := newFakeRepo()
	repo.markStoppedErr = errcode.New(errcode.ErrInternal, "db down")
	uc := progressUsecase(repo)
	events := []*battlev1.BattleProgressEvent{{Seq: 1, PlayerId: 7}} // 未知事实

	if _, err := uc.ReportProgress(context.Background(), 930, []uint64{7}, events); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("mark failure must be retryable ErrUnavailable, got %v", err)
	}
	if repo.progressStopped[930] {
		t.Fatal("marker must not be set when persist failed")
	}
	// 恢复后重试同批 → 落标记 + 停流终态。
	repo.markStoppedErr = nil
	if _, err := uc.ReportProgress(context.Background(), 930, []uint64{7}, events); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("retry after recovery want ErrInvalidState, got %v", err)
	}
	if !repo.progressStopped[930] {
		t.Fatal("marker must be persisted on retry")
	}
}

// 停流与正常批的 CAS 竞态(审计 P1):正常批读到停流前旧快照,事务侧 stopped 条件
// 必须拒推进(Unavailable 重读收敛),不得写出箱。
func TestReportProgress_StopRaceFencedByCAS(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7}

	if _, err := uc.ReportProgress(ctx, 931, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// 注入旧快照(停流发生前),同时另一路已停流。
	repo.staleWatermark = &data.ProgressWatermark{LastAppliedSeq: 1, Existed: true}
	_ = repo.MarkProgressStopped(ctx, 931)

	rows := len(repo.progressOutbox)
	if _, err := uc.ReportProgress(ctx, 931, roster, []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1)}); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-snapshot batch vs stop must fence at CAS (ErrUnavailable), got %v", err)
	}
	if len(repo.progressOutbox) != rows || repo.progressSeq[931] != 1 {
		t.Fatalf("fenced batch must be side-effect free: rows=%d seq=%d", len(repo.progressOutbox), repo.progressSeq[931])
	}
	// 重读后收敛为停流终态。
	if _, err := uc.ReportProgress(ctx, 931, roster, []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1)}); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("converged read want ErrInvalidState, got %v", err)
	}
}

// 通道关闭时固化 legacy 标记(审计 P1):对局中途重新开启配置,同一对局也不得晚开流。
func TestReportProgress_DisabledPersistsLegacyMarker(t *testing.T) {
	repo := newFakeRepo()
	off := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, conf.BattleConf{}),
		lv1Exp(map[uint32]uint64{101: 10}))
	ctx := context.Background()
	roster := []uint64{7}
	batch := []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}

	if _, err := off.ReportProgress(ctx, 932, roster, batch); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("disabled want ErrInvalidState, got %v", err)
	}
	if !repo.progressStopped[932] {
		t.Fatal("disabled rejection must persist legacy marker")
	}
	// 配置翻开后同一对局仍拒(本场结算模式已固化)。
	on := progressUsecase(repo)
	if _, err := on.ReportProgress(ctx, 932, roster, batch); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("re-enabled config must not late-open a legacy match, got %v", err)
	}
	if repo.progressSeq[932] != 0 {
		t.Fatalf("legacy match must never open stream, seq=%d", repo.progressSeq[932])
	}
}

// 关闭副本 vs 开启副本的开流竞态(审计 R4 #11):关闭副本读到"行不存在"的旧快照后,
// 固化 legacy 必须是"无行才认领"——认领输给已开流的行时,不得停掉开启副本刚开的流,
// 必须重读水位并按已开流继续入账。
func TestReportProgress_DisabledClaimLosesToOpenStream(t *testing.T) {
	repo := newFakeRepo()
	ctx := context.Background()
	roster := []uint64{7}

	// 开启副本先开流(seq=1 已入账)。
	on := progressUsecase(repo)
	if _, err := on.ReportProgress(ctx, 933, roster, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); err != nil {
		t.Fatalf("open stream on enabled replica: %v", err)
	}

	// 关闭副本处理后续批:注入"行不存在"旧快照(它读水位发生在开启副本建行之前)。
	off := withMonsterExp(NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, conf.BattleConf{}),
		lv1Exp(map[uint32]uint64{101: 10}))
	repo.staleWatermark = &data.ProgressWatermark{Existed: false}
	acked, err := off.ReportProgress(ctx, 933, roster, []*battlev1.BattleProgressEvent{killEvent(2, 7, 101, 1)})
	if err != nil || acked != 2 {
		t.Fatalf("claim-losing disabled replica must join the open stream, acked=%d err=%v", acked, err)
	}
	if repo.progressStopped[933] {
		t.Fatal("disabled replica must NOT stop a stream the enabled replica opened (upsert bug)")
	}
	if repo.progressSeq[933] != 2 {
		t.Fatalf("batch must be applied to the open stream, seq=%d", repo.progressSeq[933])
	}
}

// 认领竞态输给"已停流/已结算"的行:重读后按对应终态拒绝,不得入账。
func TestReportProgress_DisabledClaimLosesToStoppedOrSettled(t *testing.T) {
	ctx := context.Background()
	roster := []uint64{7}
	batch := []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}

	for name, prep := range map[string]func(*fakeRepo){
		"stopped": func(r *fakeRepo) { _ = r.MarkProgressStopped(ctx, 934) },
		"settled": func(r *fakeRepo) { r.progressSettled[934] = true },
	} {
		repo := newFakeRepo()
		prep(repo)
		off := withMonsterExp(
			NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil, conf.BattleConf{}),
			lv1Exp(map[uint32]uint64{101: 10}))
		repo.staleWatermark = &data.ProgressWatermark{Existed: false}
		if _, err := off.ReportProgress(ctx, 934, roster, batch); errcode.As(err) != errcode.ErrInvalidState {
			t.Fatalf("%s: claim-losing batch must converge to terminal rejection, got %v", name, err)
		}
		if repo.progressSeq[934] != 0 {
			t.Fatalf("%s: must not apply, seq=%d", name, repo.progressSeq[934])
		}
	}
}

// sharedKillEvent 带经验归属权重的击杀事实(0 = 不设置该字段,模拟旧 DS)。
func sharedKillEvent(seq, playerID uint64, monsterID, count, sharePermille uint32) *battlev1.BattleProgressEvent {
	e := killEvent(seq, playerID, monsterID, count)
	e.GetMonsterKill().SharePermille = sharePermille
	return e
}

func TestReportProgress_SharePermilleSplitsMonsterExp(t *testing.T) {
	// 「伤害占比均分」档:同一只 25 经验的怪按 500/300/200 切给三个玩家。
	// 击杀数各计一只(未被权重稀释)——反作弊额度按击杀次数算,不按经验份额算。
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()
	roster := []uint64{7, 8, 9}

	events := []*battlev1.BattleProgressEvent{
		sharedKillEvent(1, 7, 102, 1, 500), // 25×500‰ = 12(向下取整)
		sharedKillEvent(2, 8, 102, 1, 300), // 25×300‰ = 7
		sharedKillEvent(3, 9, 102, 1, 200), // 25×200‰ = 5
	}
	if acked, err := uc.ReportProgress(ctx, 940, roster, events); err != nil || acked != 3 {
		t.Fatalf("acked=%d err=%v, want 3/nil", acked, err)
	}

	got := map[uint64]uint64{}
	for _, row := range repo.progressOutbox {
		if row.Kind == data.ProgressGrantExp {
			got[row.PlayerID] = row.ExpDelta
		}
	}
	for playerID, want := range map[uint64]uint64{7: 12, 8: 7, 9: 5} {
		if got[playerID] != want {
			t.Fatalf("player %d exp=%d want %d (全部份额: %v)", playerID, got[playerID], want, got)
		}
	}
	// 切分只能少给不能多给:三份之和必须 ≤ 一整份,否则均分档反而比独享还赚。
	if total := got[7] + got[8] + got[9]; total > 25 {
		t.Fatalf("切分后合计 %d 超过单只整份 25", total)
	}
	if repo.progressExp[940] != 24 {
		t.Fatalf("单场累计 exp=%d want 24(12+7+5)", repo.progressExp[940])
	}
	for _, playerID := range roster {
		if kills := repo.progressPlayers[940][playerID].TotalKills; kills != 1 {
			t.Fatalf("player %d kills=%d want 1(击杀额度按次数算,不被经验权重稀释)", playerID, kills)
		}
	}
}

func TestReportProgress_ShareUnsetMeansFullGrant(t *testing.T) {
	// 不设 share_permille = 整份(旧 DS 上报、以及「最后一击」「全队共享」两档的常态)。
	// 这条是滚动混版的兼容底线:新 Go 收到旧 DS 的批,经验必须与升级前逐字节一致。
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()

	if _, err := uc.ReportProgress(ctx, 941, []uint64{7}, []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 102, 2), // 旧 DS:字段缺省
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if repo.progressExp[941] != 50 {
		t.Fatalf("缺省权重应按整份入账,exp=%d want 50", repo.progressExp[941])
	}

	// 显式 1000 与缺省必须等价,否则新 DS 换档时数值会跳。
	repo2 := newFakeRepo()
	uc2 := progressUsecase(repo2)
	if _, err := uc2.ReportProgress(ctx, 942, []uint64{7}, []*battlev1.BattleProgressEvent{
		sharedKillEvent(1, 7, 102, 2, 1000),
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if repo2.progressExp[942] != 50 {
		t.Fatalf("显式满权重应与缺省等价,exp=%d want 50", repo2.progressExp[942])
	}
}

func TestReportProgress_ShareOverFullRejected(t *testing.T) {
	// >1000 = 单条事实拿到超过一整份,只可能来自坏 DS / 坏改动:按坏批拒收,零副作用。
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	ctx := context.Background()

	if _, err := uc.ReportProgress(ctx, 943, []uint64{7}, []*battlev1.BattleProgressEvent{
		sharedKillEvent(1, 7, 102, 1, 1001),
	}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("want ErrInvalidArg, got %v", err)
	}
	if repo.progressSeq[943] != 0 {
		t.Fatalf("坏批不得推水位,seq=%d", repo.progressSeq[943])
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("坏批不得产出箱行,got %d", len(repo.progressOutbox))
	}
}

func TestApplyExpShare_FloorsWithoutOverflow(t *testing.T) {
	for _, c := range []struct {
		total, share, want uint64
	}{
		{total: 25, share: 1000, want: 25},
		{total: 25, share: 500, want: 12},  // 12.5 向下取整
		{total: 25, share: 1, want: 0},     // 份额小到归零:不产出箱行,不是丢账
		{total: 0, share: 500, want: 0},
		// 大数不走乘法溢出:total*share 会超 uint64,拆商余后仍精确。
		{total: 1 << 62, share: 500, want: (1 << 62) / 2},
	} {
		if got := applyExpShare(c.total, c.share); got != c.want {
			t.Fatalf("applyExpShare(%d,%d)=%d want %d", c.total, c.share, got, c.want)
		}
	}
}

func TestReportProgress_MonsterLevelSelectsExpRow(t *testing.T) {
	// 经验按 (怪物角色 ID, 等级) 查表:同一只怪不同等级形态数值不同(11 级血量是 1 级的 4 倍)。
	// 等级 0 必须按 1 级处理 —— 旧刷怪点表没有「怪物等级」列,导表后取到 0,
	// 此时行为必须与"等级硬编码 1"的历史逐字节一致,否则整片查不到 → 静默零经验。
	repo := newFakeRepo()
	uc := withMonsterExp(
		NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil,
			conf.BattleConf{ProgressEnabled: true}),
		fakeMonsterExp{101: {1: 40, 11: 160}})
	ctx := context.Background()
	roster := []uint64{7}

	killAtLevel := func(seq uint64, level uint32) *battlev1.BattleProgressEvent {
		return &battlev1.BattleProgressEvent{
			Seq: seq, PlayerId: 7,
			Fact: &battlev1.BattleProgressEvent_MonsterKill{
				MonsterKill: &battlev1.MonsterKillFact{
					MonsterConfigId: 101, Count: 1, MonsterLevel: level,
				},
			},
		}
	}
	// seq1: 等级 0(旧 DS / 旧表)→ 按 1 级 = 40
	// seq2: 等级 1 显式    → 40
	// seq3: 等级 11        → 160
	// 合计 240
	if acked, err := uc.ReportProgress(ctx, 940, roster, []*battlev1.BattleProgressEvent{
		killAtLevel(1, 0), killAtLevel(2, 1), killAtLevel(3, 11),
	}); err != nil || acked != 3 {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	var exp uint64
	for _, row := range repo.progressOutbox {
		if row.Kind == data.ProgressGrantExp && row.PlayerID == 7 {
			exp = row.ExpDelta
		}
	}
	if exp != 240 {
		t.Fatalf("exp=%d want 240(0级→40 + 1级→40 + 11级→160)", exp)
	}

	// 表里没有的等级(如 5 级)= 配置漏项:跳过该事实但水位照常推进,不卡整条流。
	before := len(repo.progressOutbox)
	if acked, err := uc.ReportProgress(ctx, 940, roster, []*battlev1.BattleProgressEvent{killAtLevel(4, 5)}); err != nil || acked != 4 {
		t.Fatalf("未配置等级应跳过而非报错: acked=%d err=%v", acked, err)
	}
	if len(repo.progressOutbox) != before {
		t.Fatalf("未配置等级不应产生出箱行,before=%d after=%d", before, len(repo.progressOutbox))
	}
}

func TestReportProgress_MissingExpTableIsRetryableNotSkipped(t *testing.T) {
	// 经验表未注入(配置表没加载 / ConfigMap 没挂)时,击杀事实必须按**可重试**错误整批退回,
	// 绝不能走"跳过并推进水位"——水位一推进,DS 重发会被 seq<=lastSeq 跳过,
	// 这批击杀的经验就永久没有补救路径了(只留一条 Warn,玩家表现为"杀怪没经验")。
	repo := newFakeRepo()
	uc := NewBattleResultUsecase(repo, NewStaticMMRReader(1500), &fakePusher{}, nil,
		conf.BattleConf{ProgressEnabled: true}) // 刻意不注入 MonsterExpTable
	ctx := context.Background()

	_, err := uc.ReportProgress(ctx, 941, []uint64{7}, []*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)})
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("want ErrUnavailable(DS 原批重试), got %v", err)
	}
	// 零副作用:水位不动、无出箱行。
	if seq := repo.progressSeq[941]; seq != 0 {
		t.Fatalf("watermark must not advance, got %d", seq)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("must not enqueue outbox, got %d", len(repo.progressOutbox))
	}
}
