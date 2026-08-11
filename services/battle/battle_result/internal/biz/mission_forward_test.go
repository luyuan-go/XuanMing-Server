// mission_forward_test.go — 任务事实转发单测(docs/design/mission.md §5.1)。
//
// 覆盖:转发开关(reporter nil = 一行不产)/ 三类事实的类别与槽位映射 /
// 白名单与漏配纪律的差异(漏配经验的怪照样转发,非白名单拾取不转发)/
// 丢弃不转发 / 出箱投递成功即删、失败退避不丢。
package biz

import (
	"context"
	"errors"
	"testing"

	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// fakeMissionReporter 记录转发调用;failNext>0 时前 N 次返回错误(验退避不丢)。
type fakeMissionReporter struct {
	calls    []fakeMissionCall
	failNext int
}

type fakeMissionCall struct {
	playerID  uint64
	category  uint32
	slotValue uint32
	amount    uint32
	key       string
}

func (f *fakeMissionReporter) ReportMissionFact(_ context.Context, playerID uint64,
	category, slotValue, amount uint32, key string) error {
	if f.failNext > 0 {
		f.failNext--
		return errors.New("mission unavailable")
	}
	f.calls = append(f.calls, fakeMissionCall{playerID, category, slotValue, amount, key})
	return nil
}

func consumeEvent(seq, playerID uint64, itemID, count uint32) *battlev1.BattleProgressEvent {
	return &battlev1.BattleProgressEvent{
		Seq: seq, PlayerId: playerID,
		Fact: &battlev1.BattleProgressEvent_ItemConsume{
			ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: itemID, Count: count},
		},
	}
}

func discardEvent(seq, playerID uint64, itemID, count uint32) *battlev1.BattleProgressEvent {
	return &battlev1.BattleProgressEvent{
		Seq: seq, PlayerId: playerID,
		Fact: &battlev1.BattleProgressEvent_ItemDiscard{
			ItemDiscard: &battlev1.ItemDiscardFact{ItemConfigId: itemID, Count: count},
		},
	}
}

// 转发关闭(mission_addr 未配)时一行不产:产了投不出去只会让出箱无界堆积。
func TestMissionForward_DisabledProducesNoRows(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	// 刻意不调 SetMissionReporter。
	if _, err := uc.ReportProgress(context.Background(), 900, nil,
		[]*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 3)}); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	if len(repo.missionOutbox) != 0 {
		t.Fatalf("转发关闭仍产出任务出箱行 %d 条", len(repo.missionOutbox))
	}
}

// 击杀 / 拾取 / 局内使用三类事实的类别与槽位映射。
func TestMissionForward_FactMapping(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	uc.SetMissionReporter(&fakeMissionReporter{})

	// 5001 在白名单内(progressUsecase 的 DropWhitelist)。
	events := []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 101, 3),
		pickupEvent(2, 7, 5001, 2),
	}
	if _, err := uc.ReportProgress(context.Background(), 901, nil, events); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	if len(repo.missionOutbox) != 2 {
		t.Fatalf("任务出箱行数 %d, want 2: %+v", len(repo.missionOutbox), repo.missionOutbox)
	}
	kill := repo.missionOutbox[0]
	if kill.Category != missionCategoryKillMonster || kill.SlotValue != 101 || kill.Amount != 3 {
		t.Fatalf("击杀事实映射错: %+v", kill)
	}
	if kill.Seq != 1 || kill.PlayerID != 7 || kill.MatchID != 901 {
		t.Fatalf("击杀事实定位字段错: %+v", kill)
	}
	pick := repo.missionOutbox[1]
	if pick.Category != missionCategoryPickupItem || pick.SlotValue != 5001 || pick.Amount != 2 {
		t.Fatalf("拾取事实映射错: %+v", pick)
	}
}

// 经验表漏配的怪照样转发(「这只怪被杀了」与「配没配经验」是两件事);
// 非白名单拾取不转发(否则等于绕过白名单另开一条计数通道)。
func TestMissionForward_SkipDisciplineDiffersByFactType(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	uc.SetMissionReporter(&fakeMissionReporter{})

	events := []*battlev1.BattleProgressEvent{
		killEvent(1, 7, 999, 1),    // 999 不在经验表(漏配)
		pickupEvent(2, 7, 8888, 1), // 8888 不在白名单
	}
	if _, err := uc.ReportProgress(context.Background(), 902, nil, events); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	if len(repo.missionOutbox) != 1 {
		t.Fatalf("任务出箱行数 %d, want 1(只该有漏配怪那条): %+v", len(repo.missionOutbox), repo.missionOutbox)
	}
	if got := repo.missionOutbox[0]; got.Category != missionCategoryKillMonster || got.SlotValue != 999 {
		t.Fatalf("应转发漏配经验的击杀事实,实得: %+v", got)
	}
}

// 丢弃不转发:扔掉不是用掉,否则「使用 N 个 X」型任务能靠捡了再扔刷完。
func TestMissionForward_DiscardNotForwarded(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	uc.SetMissionReporter(&fakeMissionReporter{})
	uc.SetBattleItemCatalog(mapBattleItemCatalog{
		5001: {Droppable: true, BattleUsable: true, MaxStack: 99},
	})
	// consume/discard 是同步 action 路径,要走 inventory granter 才能落终态;
	// 本例关心的是任务事实产出,granter 只为让链路跑通。
	uc.SetInstanceGranter(&fakeGranter{})

	// 先拾取建立同场额度(consume/discard 需要已接受的 pickup 余额)。
	if _, err := uc.ReportProgress(context.Background(), 903, nil,
		[]*battlev1.BattleProgressEvent{pickupEvent(1, 7, 5001, 4)}); err != nil {
		t.Fatalf("pickup: %v", err)
	}
	if _, err := uc.ReportProgress(context.Background(), 903, nil,
		[]*battlev1.BattleProgressEvent{consumeEvent(2, 7, 5001, 1)}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := uc.ReportProgress(context.Background(), 903, nil,
		[]*battlev1.BattleProgressEvent{discardEvent(3, 7, 5001, 1)}); err != nil {
		t.Fatalf("discard: %v", err)
	}

	var categories []uint32
	for _, row := range repo.missionOutbox {
		categories = append(categories, row.Category)
	}
	// 期望:拾取(9) + 使用(4);丢弃不产行。
	if len(categories) != 2 ||
		categories[0] != missionCategoryPickupItem || categories[1] != missionCategoryUseItem {
		t.Fatalf("类别序列 %v, want [pickup use](丢弃不转发)", categories)
	}
}

// 「使用道具」事实必须挂 pending 闸:局内消费可能以业务失败终态收场(道具不足),
// 此时 inventory 一件没扣。事实不挂闸就照发 = 让"上报根本没发生的消耗"刷完
// 「使用 N 个 X」型任务(§9.6 派生数值不信 DS)。拾取 / 击杀没有 action 结果行可等,
// 不得误挂闸(挂了会永久卡住该玩家的任务事实队列)。
func TestMissionForward_UseItemFactPendsUntilConsumeResolves(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	uc.SetMissionReporter(&fakeMissionReporter{})
	uc.SetBattleItemCatalog(mapBattleItemCatalog{
		5001: {Droppable: true, BattleUsable: true, MaxStack: 99},
	})
	uc.SetInstanceGranter(&fakeGranter{})

	if _, err := uc.ReportProgress(context.Background(), 905, nil,
		[]*battlev1.BattleProgressEvent{pickupEvent(1, 7, 5001, 4)}); err != nil {
		t.Fatalf("pickup: %v", err)
	}
	if _, err := uc.ReportProgress(context.Background(), 905, nil,
		[]*battlev1.BattleProgressEvent{consumeEvent(2, 7, 5001, 1)}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 击杀单独用一条链路验证(它与同步 action 混在一场里会先驱动经验发放,本例不配 player_addr)。
	killRepo := newFakeRepo()
	killUC := progressUsecase(killRepo)
	killUC.SetMissionReporter(&fakeMissionReporter{})
	if _, err := killUC.ReportProgress(context.Background(), 906, nil,
		[]*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 1)}); err != nil {
		t.Fatalf("kill: %v", err)
	}

	rows := append(append([]data.MissionFactRecord(nil), repo.missionOutbox...), killRepo.missionOutbox...)
	if len(rows) != 3 {
		t.Fatalf("任务出箱行数 %d, want 3(拾取 + 使用 + 击杀)", len(rows))
	}
	for _, row := range rows {
		want := row.Category == missionCategoryUseItem
		if row.PendingAction != want {
			t.Fatalf("category=%d pending_action=%v, want %v(只有使用类等扣除落定)",
				row.Category, row.PendingAction, want)
		}
	}
}

// 投递成功即删行;失败退避但不丢(at-least-once,mission 侧收据幂等吸收)。
func TestMissionForward_DeliverySucceedsAndDefersOnFailure(t *testing.T) {
	repo := newFakeRepo()
	uc := progressUsecase(repo)
	reporter := &fakeMissionReporter{failNext: 1}
	uc.SetMissionReporter(reporter)

	if _, err := uc.ReportProgress(context.Background(), 904, nil,
		[]*battlev1.BattleProgressEvent{killEvent(1, 7, 101, 2)}); err != nil {
		t.Fatalf("report progress: %v", err)
	}

	// 第一轮:reporter 失败 → 行退避保留,不删。
	if n, err := uc.forwardMissionBatch(context.Background()); err != nil || n != 0 {
		t.Fatalf("首轮转发 n=%d err=%v, want 0/nil", n, err)
	}
	if len(repo.missionOutbox) != 1 {
		t.Fatalf("失败后行被删了,at-least-once 被破坏")
	}
	if len(repo.missionDeferredIDs) != 1 {
		t.Fatalf("失败后未退避: %v", repo.missionDeferredIDs)
	}

	// 第二轮:恢复 → 投递成功 → 删行。
	if n, err := uc.forwardMissionBatch(context.Background()); err != nil || n != 1 {
		t.Fatalf("次轮转发 n=%d err=%v, want 1/nil", n, err)
	}
	if len(repo.missionOutbox) != 0 {
		t.Fatalf("投递成功后未删行: %+v", repo.missionOutbox)
	}
	if len(reporter.calls) != 1 {
		t.Fatalf("转发调用次数 %d, want 1", len(reporter.calls))
	}
	call := reporter.calls[0]
	if call.key != "progress:904:1:7:mission" {
		t.Fatalf("幂等键 %q, want progress:904:1:7:mission", call.key)
	}
	if call.category != missionCategoryKillMonster || call.slotValue != 101 || call.amount != 2 {
		t.Fatalf("转发载荷错: %+v", call)
	}
}
