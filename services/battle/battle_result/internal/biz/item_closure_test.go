package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

type mapBattleItemCatalog map[uint32]BattleItemDefinition

func (c mapBattleItemCatalog) Lookup(id uint32) (BattleItemDefinition, bool) {
	d, ok := c[id]
	return d, ok
}

func itemClosureUsecase(repo *fakeRepo, granter *fakeGranter) *BattleResultUsecase {
	cfg := conf.BattleConf{
		EloKFactor: 32, BaseMMR: 1500, ProgressEnabled: true,
		ProgressPublishInterval: config.Duration(10),
	}
	uc := NewBattleResultUsecase(repo, nil, nil, nil, cfg)
	uc.SetInstanceGranter(granter)
	uc.SetBattleItemCatalog(mapBattleItemCatalog{
		10001: {BattleUsable: true, Droppable: true, MaxStack: 99},
		10002: {Droppable: true, MaxStack: 99},
		10003: {Equipment: true, Droppable: true, MaxStack: 1},
		10004: {MaxStack: 99},
	})
	return uc
}

func TestMixedDropRetryKeepsFrozenRoutesAndKeysAcrossCatalogReload(t *testing.T) {
	repo := newFakeRepo()
	repo.dropOutbox = append(repo.dropOutbox, data.DropOutboxRecord{
		ID: 1, MatchID: 78, PlayerID: 9,
		ItemConfigIDs:         []uint32{10001, 10003},
		StackItemConfigIDs:    []uint32{10001},
		InstanceItemConfigIDs: []uint32{10003},
	})
	g := &fakeGranter{failPlayer: 9}
	uc := itemClosureUsecase(repo, g)
	catalog := mapBattleItemCatalog{
		10001: {Droppable: true, MaxStack: 99},
		10003: {Equipment: true, Droppable: true, MaxStack: 1},
	}
	uc.SetBattleItemCatalog(catalog)
	if n, err := uc.publishDropBatch(context.Background()); err != nil || n != 0 || len(repo.dropOutbox) != 1 {
		t.Fatalf("partial first publish n=%d rows=%d err=%v", n, len(repo.dropOutbox), err)
	}
	if len(g.stackCalls) != 1 || g.stackCalls[0].key != "battle_drop:78:9:stack" {
		t.Fatalf("first stack route=%+v", g.stackCalls)
	}

	// 热更把两种类型完全对调；历史 outbox 仍只能走首次冻结的 route/key。
	catalog[10001] = BattleItemDefinition{Equipment: true, Droppable: true, MaxStack: 1}
	catalog[10003] = BattleItemDefinition{Droppable: true, MaxStack: 99}
	g.failPlayer = 0
	if n, err := uc.publishDropBatch(context.Background()); err != nil || n != 1 || len(repo.dropOutbox) != 0 {
		t.Fatalf("retry publish n=%d rows=%d err=%v", n, len(repo.dropOutbox), err)
	}
	if len(g.stackCalls) != 2 || g.stackCalls[1].key != g.stackCalls[0].key ||
		len(g.stackCalls[1].items) != 1 || g.stackCalls[1].items[0].ItemConfigID != 10001 {
		t.Fatalf("stack retry changed route/key: %+v", g.stackCalls)
	}
	if len(g.calls) != 1 || g.calls[0].key != "battle_drop:78:9:instance" ||
		len(g.calls[0].items) != 1 || g.calls[0].items[0] != 10003 {
		t.Fatalf("instance retry changed route/key: %+v", g.calls)
	}
}

func TestTerminalMixedDropRoutesStackAndInstances(t *testing.T) {
	repo := newFakeRepo()
	repo.dropOutbox = append(repo.dropOutbox, data.DropOutboxRecord{
		ID: 1, MatchID: 77, PlayerID: 9,
		ItemConfigIDs:         []uint32{10001, 10003, 10001},
		StackItemConfigIDs:    []uint32{10001, 10001},
		InstanceItemConfigIDs: []uint32{10003},
	})
	g := &fakeGranter{}
	uc := itemClosureUsecase(repo, g)

	n, err := uc.publishDropBatch(context.Background())
	if err != nil || n != 1 || len(repo.dropOutbox) != 0 {
		t.Fatalf("publish n=%d rows=%d err=%v", n, len(repo.dropOutbox), err)
	}
	if len(g.stackCalls) != 1 || len(g.stackCalls[0].items) != 1 ||
		g.stackCalls[0].items[0] != (data.StackGrant{ItemConfigID: 10001, Count: 2}) ||
		g.stackCalls[0].key != "battle_drop:77:9:stack" {
		t.Fatalf("stack route mismatch: %+v", g.stackCalls)
	}
	if len(g.calls) != 1 || len(g.calls[0].items) != 1 || g.calls[0].items[0] != 10003 ||
		g.calls[0].key != "battle_drop:77:9:instance" {
		t.Fatalf("instance route mismatch: %+v", g.calls)
	}
}

func TestProgressPickupGrantPrecedesConsumeAndFailureRetainsOrder(t *testing.T) {
	repo := newFakeRepo()
	g := &fakeGranter{failStack: true}
	uc := itemClosureUsecase(repo, g)
	pickup := []*battlev1.BattleProgressEvent{{Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
		ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 2},
	}}}
	consume := []*battlev1.BattleProgressEvent{{Seq: 2, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
		ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10001, Count: 1},
	}}}
	acked, err := uc.ReportProgress(context.Background(), 88, []uint64{9}, pickup)
	if err != nil || acked != 1 {
		t.Fatalf("ReportProgress ack=%d err=%v", acked, err)
	}
	acked, err = uc.ReportProgress(context.Background(), 88, []uint64{9}, consume)
	if err == nil || acked != 0 || !isRetryableProgressActionError(errcode.As(err)) {
		t.Fatalf("transient action must remain unacked ack=%d err=%v", acked, err)
	}
	if len(repo.progressOutbox) != 2 || repo.progressOutbox[0].Kind != data.ProgressGrantStack ||
		repo.progressOutbox[1].Kind != data.ProgressConsumeStack {
		t.Fatalf("progress rows=%+v", repo.progressOutbox)
	}
	if len(g.consumeCalls) != 0 {
		t.Fatal("前序 Grant 失败时 Consume 不得越过")
	}
	g.failStack = false
	acked, err = uc.ReportProgress(context.Background(), 88, []uint64{9}, consume)
	if err != nil || acked != 2 {
		t.Fatalf("action retry ack=%d err=%v", acked, err)
	}
	if len(g.stackCalls) != 1 || g.stackCalls[0].key != "progress:88:1:9:stack" {
		t.Fatalf("stack calls=%+v", g.stackCalls)
	}
	if len(g.consumeCalls) != 1 || g.consumeCalls[0].itemID != 10001 ||
		g.consumeCalls[0].count != 1 || g.consumeCalls[0].key != "progress:88:2:9:consume" {
		t.Fatalf("consume calls=%+v", g.consumeCalls)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("synchronous action left rows=%+v", repo.progressOutbox)
	}
}

func TestProgressRejectsNonConsumableWithoutAdvancingWatermark(t *testing.T) {
	repo := newFakeRepo()
	uc := itemClosureUsecase(repo, &fakeGranter{})
	_, err := uc.ReportProgress(context.Background(), 89, []uint64{9}, []*battlev1.BattleProgressEvent{{
		Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
			ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10002, Count: 1},
		},
	}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-consumable got %v", err)
	}
	if repo.progressSeq[89] != 0 || len(repo.progressOutbox) != 0 {
		t.Fatalf("rejected consume must be side-effect free: seq=%d rows=%d",
			repo.progressSeq[89], len(repo.progressOutbox))
	}
}

func TestProgressActionBatchIsolationAndSameMatchBalance(t *testing.T) {
	repo := newFakeRepo()
	uc := itemClosureUsecase(repo, &fakeGranter{})
	mixed := []*battlev1.BattleProgressEvent{
		{Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
			ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 1},
		}},
		{Seq: 2, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
			ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10001, Count: 1},
		}},
	}
	if acked, err := uc.ReportProgress(context.Background(), 92, []uint64{9}, mixed); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("mixed action batch ack=%d err=%v", acked, err)
	}
	if repo.progressSeq[92] != 0 || len(repo.progressOutbox) != 0 {
		t.Fatalf("mixed rejection had side effects seq=%d rows=%+v", repo.progressSeq[92], repo.progressOutbox)
	}

	consume := func(seq uint64, item uint32, count uint32) []*battlev1.BattleProgressEvent {
		return []*battlev1.BattleProgressEvent{{Seq: seq, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
			ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: item, Count: count},
		}}}
	}
	if acked, err := uc.ReportProgress(context.Background(), 92, []uint64{9}, consume(1, 10001, 1)); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("pre-match inventory spend must be rejected ack=%d err=%v", acked, err)
	}
	pickup := []*battlev1.BattleProgressEvent{{Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
		ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 2},
	}}}
	if acked, err := uc.ReportProgress(context.Background(), 92, []uint64{9}, pickup); acked != 1 || err != nil {
		t.Fatalf("pickup ack=%d err=%v", acked, err)
	}
	// 同场、同玩家但跨 item 不能借额度。
	discardCrossItem := []*battlev1.BattleProgressEvent{{Seq: 2, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemDiscard{
		ItemDiscard: &battlev1.ItemDiscardFact{ItemConfigId: 10002, Count: 1},
	}}}
	if acked, err := uc.ReportProgress(context.Background(), 92, []uint64{9}, discardCrossItem); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("cross-item spend ack=%d err=%v", acked, err)
	}
	if repo.progressSeq[92] != 1 || len(repo.progressOutbox) != 1 {
		t.Fatalf("cross-item rejection advanced state seq=%d rows=%+v", repo.progressSeq[92], repo.progressOutbox)
	}
	if acked, err := uc.ReportProgress(context.Background(), 92, []uint64{9}, consume(2, 10001, 3)); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("over-balance spend ack=%d err=%v", acked, err)
	}
}

func TestProgressTerminalFailureReplaysAndReleasesReservation(t *testing.T) {
	repo := newFakeRepo()
	g := &fakeGranter{consumeErr: errcode.New(errcode.ErrInventoryInsufficient, "inventory changed")}
	uc := itemClosureUsecase(repo, g)
	pickup := []*battlev1.BattleProgressEvent{{Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
		ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 2},
	}}}
	if acked, err := uc.ReportProgress(context.Background(), 93, []uint64{9}, pickup); acked != 1 || err != nil {
		t.Fatalf("pickup ack=%d err=%v", acked, err)
	}
	consume := func(seq uint64, count uint32) []*battlev1.BattleProgressEvent {
		return []*battlev1.BattleProgressEvent{{Seq: seq, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
			ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10001, Count: count},
		}}}
	}
	acked, err := uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(2, 2))
	if acked != 2 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("terminal failure ack=%d code=%d err=%v", acked, errcode.As(err), err)
	}
	actionKey := [4]uint64{93, 2, 9, uint64(data.ProgressConsumeStack)}
	if got := repo.progressActions[actionKey]; got.Status != data.ProgressActionFailed || got.ResultCode != errcode.ErrInventoryInsufficient {
		t.Fatalf("persisted failure=%+v", got)
	}
	if balance := repo.progressBalances[[3]uint64{93, 9, 10001}]; balance != [2]uint32{2, 0} {
		t.Fatalf("failed action reservation not released: picked/spent=%v", balance)
	}
	if len(repo.progressOutbox) != 0 {
		t.Fatalf("terminal failure must drain action row, got=%+v", repo.progressOutbox)
	}
	// 响应丢失重放从 action 表返回同一终态，不再次调用 inventory。
	acked, err = uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(2, 2))
	if acked != 2 || errcode.As(err) != errcode.ErrInvalidArg || g.consumeTries != 1 {
		t.Fatalf("failure replay ack=%d tries=%d err=%v", acked, g.consumeTries, err)
	}

	// 失败已释放额度且不饿死后序；新 seq 可以重新预留并成功。成功不释放额度。
	g.consumeErr = nil
	if acked, err = uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(3, 2)); acked != 3 || err != nil {
		t.Fatalf("retry as new action ack=%d err=%v", acked, err)
	}
	if balance := repo.progressBalances[[3]uint64{93, 9, 10001}]; balance != [2]uint32{2, 2} {
		t.Fatalf("successful action reservation changed incorrectly: %v", balance)
	}
	successTries := g.consumeTries
	if acked, err = uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(3, 2)); acked != 3 || err != nil || g.consumeTries != successTries {
		t.Fatalf("success response-loss replay ack=%d tries=%d want=%d err=%v",
			acked, g.consumeTries, successTries, err)
	}
	if acked, err = uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(4, 1)); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("successful spend must not be reusable ack=%d err=%v", acked, err)
	}
	repo.progressSettled[93] = true
	if acked, err = uc.ReportProgress(context.Background(), 93, []uint64{9}, consume(3, 2)); acked != 3 || err != nil {
		t.Fatalf("accepted action must replay after match settlement ack=%d err=%v", acked, err)
	}
}

func TestProgressActionUsesMaxStackNotPickupFactLimit(t *testing.T) {
	repo := newFakeRepo()
	g := &fakeGranter{consumeErr: errcode.New(errcode.ErrUnavailable, "inventory retry")}
	uc := itemClosureUsecase(repo, g)
	pickups := []*battlev1.BattleProgressEvent{
		{Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
			ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 10},
		}},
		{Seq: 2, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
			ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10001, Count: 1},
		}},
	}
	if acked, err := uc.ReportProgress(context.Background(), 94, []uint64{9}, pickups); acked != 2 || err != nil {
		t.Fatalf("pickups ack=%d err=%v", acked, err)
	}
	consume := []*battlev1.BattleProgressEvent{{Seq: 3, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
		ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10001, Count: 11},
	}}}
	if acked, err := uc.ReportProgress(context.Background(), 94, []uint64{9}, consume); acked != 0 || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("transient whole-stack action ack=%d err=%v", acked, err)
	}
	if len(repo.progressOutbox) != 1 || repo.progressOutbox[0].ItemCount != 11 ||
		len(repo.progressOutbox[0].ItemConfigIDs) != 1 || repo.progressOutbox[0].ItemConfigIDs[0] != 10001 {
		t.Fatalf("action must use compact item+count outbox: %+v", repo.progressOutbox)
	}
	g.consumeErr = nil
	if acked, err := uc.ReportProgress(context.Background(), 94, []uint64{9}, consume); acked != 3 || err != nil {
		t.Fatalf("whole-stack retry ack=%d err=%v", acked, err)
	}
	if len(g.consumeCalls) != 1 || g.consumeCalls[0].count != 11 {
		t.Fatalf("consume calls=%+v", g.consumeCalls)
	}
	tooLarge := []*battlev1.BattleProgressEvent{{Seq: 4, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemConsume{
		ItemConsume: &battlev1.ItemConsumeFact{ItemConfigId: 10001, Count: 100},
	}}}
	if acked, err := uc.ReportProgress(context.Background(), 94, []uint64{9}, tooLarge); acked != 0 || errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("count above max_stack ack=%d err=%v", acked, err)
	}
}

func TestProgressStackDiscardPersistsAndEquipmentDiscardFailsClosed(t *testing.T) {
	repo := newFakeRepo()
	g := &fakeGranter{}
	uc := itemClosureUsecase(repo, g)
	if acked, err := uc.ReportProgress(context.Background(), 90, []uint64{9}, []*battlev1.BattleProgressEvent{{
		Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemPickup{
			ItemPickup: &battlev1.ItemPickupFact{ItemConfigId: 10002, Count: 2},
		},
	}}); err != nil || acked != 1 {
		t.Fatalf("pickup ack=%d err=%v", acked, err)
	}
	acked, err := uc.ReportProgress(context.Background(), 90, []uint64{9}, []*battlev1.BattleProgressEvent{{
		Seq: 2, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemDiscard{
			ItemDiscard: &battlev1.ItemDiscardFact{ItemConfigId: 10002, Count: 2},
		},
	}})
	if err != nil || acked != 2 || len(repo.progressOutbox) != 0 {
		t.Fatalf("stack discard ack=%d rows=%+v err=%v", acked, repo.progressOutbox, err)
	}
	if len(g.discardCalls) != 1 || g.discardCalls[0].itemID != 10002 ||
		g.discardCalls[0].count != 2 || g.discardCalls[0].key != "progress:90:2:9:discard" {
		t.Fatalf("discard calls=%+v", g.discardCalls)
	}

	_, err = uc.ReportProgress(context.Background(), 91, []uint64{9}, []*battlev1.BattleProgressEvent{{
		Seq: 1, PlayerId: 9, Fact: &battlev1.BattleProgressEvent_ItemDiscard{
			ItemDiscard: &battlev1.ItemDiscardFact{ItemConfigId: 10003, Count: 1},
		},
	}})
	if errcode.As(err) != errcode.ErrInvalidArg || repo.progressSeq[91] != 0 {
		t.Fatalf("equipment discard must fail closed seq=%d err=%v", repo.progressSeq[91], err)
	}
}
