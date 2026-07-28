// auction 撮合引擎单测(2026-06-19):验证不超卖 / 价格-时间优先 / 幂等 / 部分成交。
//
// 用真 Redis ZSET 订单簿(miniredis)验证 score+member 编码的撮合顺序;
// 用内存 fakeRepo 当撮合权威库;用 trackLedger 计数结算次数(验证不超卖 / 不重复结算)。
package biz

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"

	"github.com/luyuancpp/pandora/services/economy/auction/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/data"
)

// ── 测试替身 ────────────────────────────────────────────────────────────────

// seqGen 是递增雪花替身;order_id 递增 → 订单簿同价按 member 字典序 = 时间优先可被验证。
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

// trackLedger 记录每笔结算(计数 = 成交笔数 + 量,验证不超卖 / 不重复结算)。
type trackLedger struct {
	mu                    sync.Mutex
	matches               []*data.MatchRecord
	freezes               []uint64
	releases              []uint64
	failFor               map[uint64]bool
	frozen                map[uint64]bool
	settled               map[uint64]bool
	released              map[uint64]bool
	freezeAttempts        map[uint64]int
	settleFailures        int
	settleFailuresByMatch map[uint64]int
	releaseFailures       map[uint64]int
	ensures               []uint64
	ensureFailures        map[uint64]error
}

type trackEvents struct {
	mu            sync.Mutex
	matchAttempts int
	matchFailures int
	matches       []uint64
}

func (p *trackEvents) PushMatch(_ context.Context, e *auctionv1.AuctionMatchEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.matchAttempts++
	if p.matchFailures > 0 {
		p.matchFailures--
		return errors.New("test match event failed")
	}
	p.matches = append(p.matches, e.GetMatchId())
	return nil
}

func (*trackEvents) PushAudit(context.Context, *auctionv1.AuctionOrder) error { return nil }

type blockingAuditEvents struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*blockingAuditEvents) PushMatch(context.Context, *auctionv1.AuctionMatchEvent) error {
	return nil
}

func (p *blockingAuditEvents) PushAudit(context.Context, *auctionv1.AuctionOrder) error {
	p.once.Do(func() { close(p.entered) })
	<-p.release
	return nil
}

func (l *trackLedger) Freeze(_ context.Context, _, orderID uint64, _ data.Side, _ uint32, _, _ int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failFor[orderID] {
		return errcode.New(errcode.ErrAuctionInsufficient, "test freeze insufficient order=%d", orderID)
	}
	if l.freezeAttempts == nil {
		l.freezeAttempts = make(map[uint64]int)
	}
	l.freezeAttempts[orderID]++
	if l.frozen == nil {
		l.frozen = make(map[uint64]bool)
	}
	if l.frozen[orderID] {
		return nil
	}
	l.frozen[orderID] = true
	l.freezes = append(l.freezes, orderID)
	return nil
}

func (l *trackLedger) Ensure(_ context.Context, _, orderID uint64, _ data.Side, _ uint32, _, _ int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureFailures[orderID]; err != nil {
		return err
	}
	if l.failFor[orderID] {
		return errcode.New(errcode.ErrAuctionInsufficient, "test ensure insufficient order=%d", orderID)
	}
	if l.frozen == nil {
		l.frozen = make(map[uint64]bool)
	}
	if !l.frozen[orderID] {
		l.frozen[orderID] = true
	}
	l.ensures = append(l.ensures, orderID)
	return nil
}

func (l *trackLedger) Settle(_ context.Context, m *data.MatchRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.settleFailures > 0 {
		l.settleFailures--
		return errors.New("test settle failed")
	}
	if l.settleFailuresByMatch != nil && l.settleFailuresByMatch[m.MatchID] > 0 {
		l.settleFailuresByMatch[m.MatchID]--
		return errors.New("test settle failed for match")
	}
	if l.settled == nil {
		l.settled = make(map[uint64]bool)
	}
	if l.settled[m.MatchID] {
		return nil
	}
	l.settled[m.MatchID] = true
	copyMatch := *m
	l.matches = append(l.matches, &copyMatch)
	return nil
}

func (l *trackLedger) Release(_ context.Context, _, orderID uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.releaseFailures != nil && l.releaseFailures[orderID] > 0 {
		l.releaseFailures[orderID]--
		return errors.New("test release failed")
	}
	if l.released == nil {
		l.released = make(map[uint64]bool)
	}
	if l.released[orderID] {
		return nil
	}
	l.released[orderID] = true
	l.releases = append(l.releases, orderID)
	return nil
}

func (l *trackLedger) totalQty() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var sum int64
	for _, m := range l.matches {
		sum += m.Quantity
	}
	return sum
}

func (l *trackLedger) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.releases)
}

// fakeRepo 是内存版 AuctionRepo。
type fakeRepo struct {
	mu      sync.Mutex
	orders  map[uint64]*data.OrderRecord
	byIdem  map[string]uint64
	matches map[uint64]*data.MatchRecord
}

type getOrderFailRepo struct {
	*fakeRepo
	failOrderID uint64
}

func (r *getOrderFailRepo) GetOrder(ctx context.Context, marketID uint32, orderID uint64) (*data.OrderRecord, bool, error) {
	if orderID == r.failOrderID {
		return nil, false, errors.New("test authoritative read failed")
	}
	return r.fakeRepo.GetOrder(ctx, marketID, orderID)
}

// canonicalClaimRepo 模拟 owner registry 已存在、market 行刚被 ClaimOrder 补回的路径。
// 该路径必须让业务层沿用 registry 中的 order_id 作为所有资产幂等键。
type canonicalClaimRepo struct {
	*fakeRepo
	canonical *data.OrderRecord
}

func (r *canonicalClaimRepo) ClaimOrder(_ context.Context, _ *data.OrderRecord) (*data.OrderRecord, bool, error) {
	return copyOrder(r.canonical), true, nil
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		orders:  make(map[uint64]*data.OrderRecord),
		byIdem:  make(map[string]uint64),
		matches: make(map[uint64]*data.MatchRecord),
	}
}

func copyOrder(o *data.OrderRecord) *data.OrderRecord {
	c := *o
	return &c
}

func idemKey(owner uint64, key string) string { return fmt.Sprintf("%d:%s", owner, key) }

func (r *fakeRepo) ClaimOrder(_ context.Context, rec *data.OrderRecord) (*data.OrderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := idemKey(rec.OwnerID, rec.IdempotencyKey)
	if id, ok := r.byIdem[k]; ok {
		return copyOrder(r.orders[id]), true, nil
	}
	r.byIdem[k] = rec.OrderID
	r.orders[rec.OrderID] = copyOrder(rec)
	return rec, false, nil
}

func (r *fakeRepo) GetOrder(_ context.Context, _ uint32, orderID uint64) (*data.OrderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[orderID]
	if !ok {
		return nil, false, nil
	}
	return copyOrder(o), true, nil
}

func (r *fakeRepo) ActivateOrder(_ context.Context, _ uint32, orderID uint64, updatedAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || o.Status != data.StatusPending || !o.EscrowVerified {
		return false, nil
	}
	o.Status = data.StatusOpen
	o.UpdatedAtMs = updatedAtMs
	return true, nil
}

func (r *fakeRepo) ConfirmOrderEscrow(_ context.Context, _ uint32, orderID uint64, updatedAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || o.Remaining() <= 0 ||
		(o.Status != data.StatusPending && !fakeActive(o.Status)) {
		return false, nil
	}
	if o.EscrowVerified {
		return true, nil
	}
	o.EscrowVerified = true
	if fakeActive(o.Status) {
		o.MatchPending = true
	}
	o.ReconcileNextAttemptAtMs = 0
	o.UpdatedAtMs = updatedAtMs
	return true, nil
}

func (r *fakeRepo) RejectPendingOrder(_ context.Context, _ uint32, orderID uint64, updatedAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || o.Status != data.StatusPending {
		return false, nil
	}
	o.Status = data.StatusCanceled
	o.ReleasePending = true
	o.MatchPending = false
	o.EscrowVerified = false
	o.ReconcileNextAttemptAtMs = 0
	o.ReleaseNextAttemptAtMs = 0
	o.UpdatedAtMs = updatedAtMs
	return true, nil
}

func (r *fakeRepo) ListPendingOrders(_ context.Context, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	now := time.Now().UnixMilli()
	for _, o := range r.orders {
		if o.Status == data.StatusPending && o.ReconcileNextAttemptAtMs <= now {
			out = append(out, copyOrder(o))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r *fakeRepo) ListMatchPendingOrders(_ context.Context, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	now := time.Now().UnixMilli()
	for _, o := range r.orders {
		if o.MatchPending && o.EscrowVerified && fakeActive(o.Status) && o.ReconcileNextAttemptAtMs <= now {
			out = append(out, copyOrder(o))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r *fakeRepo) ListUnverifiedActiveOrders(_ context.Context, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	now := time.Now().UnixMilli()
	for _, o := range r.orders {
		if fakeActive(o.Status) && !o.EscrowVerified && o.Remaining() > 0 && o.ReconcileNextAttemptAtMs <= now {
			out = append(out, copyOrder(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderID < out[j].OrderID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) DeferOrderReconcile(
	_ context.Context, _ uint32, orderID uint64, nextAttemptAtMs int64,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || nextAttemptAtMs <= 0 ||
		(o.Status != data.StatusPending && !fakeActive(o.Status)) {
		return false, nil
	}
	o.ReconcileNextAttemptAtMs = nextAttemptAtMs
	return true, nil
}

func (r *fakeRepo) ClearMatchPending(_ context.Context, _ uint32, orderID uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || !o.MatchPending || !fakeActive(o.Status) {
		return false, nil
	}
	o.MatchPending = false
	return true, nil
}

func (r *fakeRepo) FindBestActiveOrder(
	_ context.Context,
	marketID, itemConfigID uint32,
	side data.Side,
	excludeOwnerID uint64,
) (*data.OrderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best *data.OrderRecord
	for _, o := range r.orders {
		if o.MarketID != marketID || o.ItemConfigID != itemConfigID || o.Side != side ||
			o.OwnerID == excludeOwnerID || (o.Status != data.StatusOpen && o.Status != data.StatusPartial) ||
			!o.EscrowVerified || o.Remaining() <= 0 {
			continue
		}
		if best == nil || betterOrder(o, best, side) {
			best = o
		}
	}
	if best == nil {
		return nil, false, nil
	}
	return copyOrder(best), true, nil
}

func (r *fakeRepo) ReserveMatch(
	_ context.Context, marketID uint32, incomingOrderID, restingOrderID, matchID uint64, matchedAtMs int64,
) (*data.MatchRecord, *data.OrderRecord, *data.OrderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	incoming, resting := r.orders[incomingOrderID], r.orders[restingOrderID]
	if incoming == nil || resting == nil || incoming.MarketID != marketID || resting.MarketID != marketID ||
		incoming.ItemConfigID != resting.ItemConfigID || incoming.Side == resting.Side ||
		incoming.OwnerID == resting.OwnerID || !(incoming.Status == data.StatusPending || fakeActive(incoming.Status)) || !fakeActive(resting.Status) ||
		!incoming.EscrowVerified || !resting.EscrowVerified || incoming.Remaining() <= 0 || resting.Remaining() <= 0 ||
		!crosses(incoming.Side, incoming.Price, resting.Price) {
		return nil, copyOrderOrNil(incoming), copyOrderOrNil(resting), false, nil
	}
	qty := incoming.Remaining()
	if resting.Remaining() < qty {
		qty = resting.Remaining()
	}
	m := &data.MatchRecord{
		MatchID: matchID, MarketID: marketID, ItemConfigID: incoming.ItemConfigID,
		Quantity: qty, Price: resting.Price, MatchedAtMs: matchedAtMs,
		SettlementStatus: data.SettlementPending,
	}
	if incoming.Side == data.SideSell {
		m.SellOrderID, m.SellerID = incoming.OrderID, incoming.OwnerID
		m.BuyOrderID, m.BuyerID = resting.OrderID, resting.OwnerID
	} else {
		m.BuyOrderID, m.BuyerID = incoming.OrderID, incoming.OwnerID
		m.SellOrderID, m.SellerID = resting.OrderID, resting.OwnerID
	}
	incoming.FilledQuantity += qty
	resting.FilledQuantity += qty
	for _, o := range []*data.OrderRecord{incoming, resting} {
		o.UpdatedAtMs = matchedAtMs
		if o.Remaining() == 0 {
			o.Status = data.StatusFilled
			o.ReleasePending = true
		} else {
			o.Status = data.StatusPartial
		}
	}
	incoming.MatchPending = incoming.Remaining() > 0
	if resting.Remaining() == 0 {
		resting.MatchPending = false
	}
	copyMatch := *m
	r.matches[matchID] = &copyMatch
	return m, copyOrder(incoming), copyOrder(resting), true, nil
}

func copyOrderOrNil(o *data.OrderRecord) *data.OrderRecord {
	if o == nil {
		return nil
	}
	return copyOrder(o)
}

func fakeActive(status int32) bool { return status == data.StatusOpen || status == data.StatusPartial }

func (r *fakeRepo) CompleteMatch(_ context.Context, _ uint32, matchID uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.matches[matchID]
	if m == nil || m.SettlementStatus != data.SettlementPending {
		return false, nil
	}
	m.SettlementStatus = data.SettlementCompleted
	m.SettlementNextAttemptAtMs = 0
	m.EventPending = true
	m.EventNextAttemptAtMs = 0
	return true, nil
}

func (r *fakeRepo) DeferMatchSettlement(_ context.Context, _ uint32, matchID uint64, nextAttemptAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.matches[matchID]
	if m == nil || m.SettlementStatus != data.SettlementPending {
		return false, nil
	}
	m.SettlementNextAttemptAtMs = nextAttemptAtMs
	return true, nil
}

func (r *fakeRepo) ListPendingMatches(_ context.Context, limit int) ([]*data.MatchRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.MatchRecord
	now := time.Now().UnixMilli()
	for _, m := range r.matches {
		if m.SettlementStatus == data.SettlementPending && m.SettlementNextAttemptAtMs <= now {
			c := *m
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SettlementNextAttemptAtMs != out[j].SettlementNextAttemptAtMs {
			return out[i].SettlementNextAttemptAtMs < out[j].SettlementNextAttemptAtMs
		}
		if out[i].MatchedAtMs != out[j].MatchedAtMs {
			return out[i].MatchedAtMs < out[j].MatchedAtMs
		}
		return out[i].MatchID < out[j].MatchID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) ListPendingMatchEvents(_ context.Context, limit int) ([]*data.MatchRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.MatchRecord
	now := time.Now().UnixMilli()
	for _, m := range r.matches {
		if m.SettlementStatus == data.SettlementCompleted && m.EventPending && m.EventNextAttemptAtMs <= now {
			if sell := r.orders[m.SellOrderID]; sell != nil && sell.ReleasePending && isTerminal(sell.Status) {
				continue
			}
			if buy := r.orders[m.BuyOrderID]; buy != nil && buy.ReleasePending && isTerminal(buy.Status) {
				continue
			}
			c := *m
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventNextAttemptAtMs != out[j].EventNextAttemptAtMs {
			return out[i].EventNextAttemptAtMs < out[j].EventNextAttemptAtMs
		}
		return out[i].MatchID < out[j].MatchID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) DeferMatchEvent(
	_ context.Context, _ uint32, matchID uint64, nextAttemptAtMs int64,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.matches[matchID]
	if m == nil || !m.EventPending || m.SettlementStatus != data.SettlementCompleted || nextAttemptAtMs <= 0 {
		return false, nil
	}
	m.EventNextAttemptAtMs = nextAttemptAtMs
	return true, nil
}

func (r *fakeRepo) ClearMatchEventPending(_ context.Context, _ uint32, matchID uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.matches[matchID]
	if m == nil || !m.EventPending || m.SettlementStatus != data.SettlementCompleted {
		return false, nil
	}
	m.EventPending = false
	m.EventNextAttemptAtMs = 0
	return true, nil
}

func (r *fakeRepo) MarkOrderTerminal(_ context.Context, _ uint32, orderID uint64, status int32, updatedAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || !fakeActive(o.Status) {
		return false, nil
	}
	o.Status = status
	o.ReleasePending = true
	o.MatchPending = false
	o.EscrowVerified = false
	o.ReconcileNextAttemptAtMs = 0
	o.ReleaseNextAttemptAtMs = 0
	o.UpdatedAtMs = updatedAtMs
	return true, nil
}

func (r *fakeRepo) releasable(o *data.OrderRecord) bool {
	if o == nil || !o.ReleasePending ||
		(o.Status != data.StatusFilled && o.Status != data.StatusCanceled && o.Status != data.StatusExpired) {
		return false
	}
	for _, m := range r.matches {
		if m.SettlementStatus == data.SettlementPending && (m.SellOrderID == o.OrderID || m.BuyOrderID == o.OrderID) {
			return false
		}
	}
	return true
}

func (r *fakeRepo) GetReleasableOrder(_ context.Context, _ uint32, orderID uint64) (*data.OrderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if !r.releasable(o) {
		return nil, false, nil
	}
	return copyOrder(o), true, nil
}

func (r *fakeRepo) ListReleasableOrders(_ context.Context, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	now := time.Now().UnixMilli()
	for _, o := range r.orders {
		if r.releasable(o) && o.ReleaseNextAttemptAtMs <= now {
			out = append(out, copyOrder(o))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ReleaseNextAttemptAtMs != out[j].ReleaseNextAttemptAtMs {
			return out[i].ReleaseNextAttemptAtMs < out[j].ReleaseNextAttemptAtMs
		}
		return out[i].OrderID < out[j].OrderID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) DeferOrderRelease(_ context.Context, _ uint32, orderID uint64, nextAttemptAtMs int64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || !o.ReleasePending {
		return false, nil
	}
	o.ReleaseNextAttemptAtMs = nextAttemptAtMs
	return true, nil
}

func (r *fakeRepo) ClearReleasePending(_ context.Context, _ uint32, orderID uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if !r.releasable(o) {
		return false, nil
	}
	o.ReleasePending = false
	o.ReleaseNextAttemptAtMs = 0
	return true, nil
}

func (r *fakeRepo) ListTerminalOrdersForRepair(_ context.Context, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	for _, o := range r.orders {
		if isTerminal(o.Status) && (o.EscrowVerified || o.MatchPending) {
			out = append(out, copyOrder(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderID < out[j].OrderID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) RepairTerminalMarkers(_ context.Context, _ uint32, orderID uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.orders[orderID]
	if o == nil || !isTerminal(o.Status) || (!o.EscrowVerified && !o.MatchPending) {
		return false, nil
	}
	o.ReleasePending = true
	o.MatchPending = false
	o.EscrowVerified = false
	o.ReconcileNextAttemptAtMs = 0
	o.ReleaseNextAttemptAtMs = 0
	return true, nil
}

func betterOrder(candidate, current *data.OrderRecord, side data.Side) bool {
	if candidate.Price != current.Price {
		if side == data.SideSell {
			return candidate.Price < current.Price
		}
		return candidate.Price > current.Price
	}
	return candidate.OrderID < current.OrderID
}

func (r *fakeRepo) ListMarketOrders(_ context.Context, marketID uint32, side data.Side, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	for _, o := range r.orders {
		if o.MarketID == marketID && o.Side == side && (o.Status == data.StatusOpen || o.Status == data.StatusPartial) {
			out = append(out, copyOrder(o))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r *fakeRepo) ListOwnerOrders(
	_ context.Context, ownerID uint64, activeOnly bool, cursorOrderID uint64, limit int,
) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	for _, o := range r.orders {
		if o.OwnerID != ownerID || o.Status == data.StatusPending || (cursorOrderID > 0 && o.OrderID >= cursorOrderID) {
			continue
		}
		if activeOnly && o.Status != data.StatusOpen && o.Status != data.StatusPartial {
			continue
		}
		out = append(out, copyOrder(o))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderID > out[j].OrderID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) ListOwnerActiveAndPending(
	_ context.Context, ownerID uint64, limit int,
) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	for _, o := range r.orders {
		if o.OwnerID == ownerID && (o.Status == data.StatusPending || fakeActive(o.Status)) {
			out = append(out, copyOrder(o))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderID < out[j].OrderID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *fakeRepo) ListExpirableOrders(_ context.Context, createdBeforeMs int64, limit int) ([]*data.OrderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*data.OrderRecord
	for _, o := range r.orders {
		if o.CreatedAtMs >= createdBeforeMs {
			continue
		}
		if o.Status != data.StatusOpen && o.Status != data.StatusPartial {
			continue
		}
		out = append(out, copyOrder(o))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// newTestUsecase 装配:内存 repo + miniredis 订单簿 + 计数 ledger。
func newTestUsecase(t *testing.T) (*AuctionUsecase, *fakeRepo, *trackLedger) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	repo := newFakeRepo()
	book := data.NewRedisBookStore(rdb)
	slots := data.NewRedisOwnerSlotLimiter(rdb)
	ledger := &trackLedger{}
	// order_id / match_id 两个独立空间各一个发号器。起点相同 = 忠实模拟生产:
	// 两者共用 nodeID,会发出逐位相同的 ID,业务不得跨空间比较。
	uc := NewAuctionUsecase(repo, book, slots, ledger, nil, &seqGen{n: 1000}, &seqGen{n: 1000}, conf.AuctionConf{})
	t.Cleanup(uc.Close)
	return uc, repo, ledger
}

// ── 用例 ──────────────────────────────────────────────────────────────────────

func TestPlaceOrder_RestsWhenNoCounterparty(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	o, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s1")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if o.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("status = %v, want OPEN", o.GetStatus())
	}
	if o.GetFilledQuantity() != 0 {
		t.Fatalf("filled = %d, want 0", o.GetFilledQuantity())
	}
	if got := ledger.totalQty(); got != 0 {
		t.Fatalf("settled qty = %d, want 0", got)
	}
}

func TestPassiveWarmupRejectsMutationsAndBackgroundRepair(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	uc.cfg.PassiveWarmup = true
	ctx := context.Background()

	if _, err := uc.PlaceOrder(ctx, 1, 100, 200, 1, 100, "passive-write"); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("passive PlaceOrder error=%v, want unavailable", err)
	}
	if err := uc.CancelOrder(ctx, 1, 100, 1); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("passive CancelOrder error=%v, want unavailable", err)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("passive reconcile error=%v, want unavailable", err)
	}
	if _, err := uc.ReconcilePendingMatchEvents(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("passive event reconcile error=%v, want unavailable", err)
	}
	if _, err := uc.ExpireDueOrders(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("passive expiry error=%v, want unavailable", err)
	}
	if len(repo.orders) != 0 || len(ledger.freezes) != 0 || len(ledger.ensures) != 0 {
		t.Fatalf("passive mode mutated state: orders=%d freezes=%v ensures=%v",
			len(repo.orders), ledger.freezes, ledger.ensures)
	}
}

func TestSubmitRejectsUnsafeIdempotencyKeys(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	for _, key := range []string{"", strings.Repeat("a", 65), "line\nbreak", "含中文"} {
		if _, err := uc.PlaceOrder(context.Background(), 1, 100, 200, 1, 100, key); errcode.As(err) != errcode.ErrInvalidArg {
			t.Fatalf("key=%q error=%v, want invalid arg", key, err)
		}
	}
	if len(repo.orders) != 0 || len(ledger.freezes) != 0 {
		t.Fatalf("invalid keys must not mutate state: orders=%d freezes=%v", len(repo.orders), ledger.freezes)
	}
}

func TestMatch_FullFill(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s1")
	bid, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "b1")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status = %v, want FILLED", bid.GetStatus())
	}
	if bid.GetFilledQuantity() != 10 {
		t.Fatalf("bid filled = %d, want 10", bid.GetFilledQuantity())
	}
	if got := ledger.totalQty(); got != 10 {
		t.Fatalf("settled qty = %d, want 10", got)
	}
	_ = sell
}

func TestMatch_NoOversell_Concurrent(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s1")

	// 两买家并发各吃 10:per-market 单写者串行 → 总成交恰好 10,绝不超卖。
	var wg sync.WaitGroup
	for i, buyer := range []uint64{2, 3} {
		wg.Add(1)
		go func(buyer uint64, idx int) {
			defer wg.Done()
			_, _ = uc.Bid(ctx, buyer, 100, 200, 10, 100, fmt.Sprintf("b%d", idx))
		}(buyer, i)
	}
	wg.Wait()

	if got := ledger.totalQty(); got != 10 {
		t.Fatalf("oversell: settled qty = %d, want 10", got)
	}
	got, _, _ := repo.GetOrder(ctx, 100, sell.GetOrderId())
	if got.FilledQuantity != 10 {
		t.Fatalf("seller filled = %d, want 10", got.FilledQuantity)
	}
}

// TestMatch_CrossItemNeverSettles 验证同 market 同价的异物品对手单绝不成交(P0-2)：
// 候选由 MySQL 精确 item_config_id 查询，不能按 incoming 物品结算异物品卖家的资产。
func TestMatch_CrossItemNeverSettles(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	// 同 market 100、同价 100,但不同物品:sellA 卖 item 200,sellB 卖 item 201。
	sellA, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "sA") // 更早挂，但不是 item 201 候选。
	sellB, _ := uc.PlaceOrder(ctx, 4, 100, 201, 10, 100, "sB")

	// 买家出价买 item 201:只能与 sellB 成交,sellA(异物品)必须被隔离且不成交。
	bid, err := uc.Bid(ctx, 2, 100, 201, 10, 100, "b1")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status = %v, want FILLED (matched item 201)", bid.GetStatus())
	}
	if got := ledger.totalQty(); got != 10 {
		t.Fatalf("settled qty = %d, want 10", got)
	}
	// 唯一成交必须是 item 201(绝不把 sellA 的 item 200 当 201 转移)。
	for _, m := range ledger.matches {
		if m.ItemConfigID != 201 {
			t.Fatalf("settled wrong item %d, want 201", m.ItemConfigID)
		}
		if m.SellerID != sellB.GetOwnerId() {
			t.Fatalf("settled wrong seller %d, want %d (item 201 owner)", m.SellerID, sellB.GetOwnerId())
		}
	}
	// sellA(异物品)仍是 MySQL 权威活跃订单。
	gotA := mustGetOrder(t, uc, sellA.GetOrderId())
	if gotA.GetFilledQuantity() != 0 || gotA.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("sellA must stay OPEN unfilled, got status=%v filled=%d", gotA.GetStatus(), gotA.GetFilledQuantity())
	}
	// sellA 仍可被同物品买家吃到。
	bidA, err := uc.Bid(ctx, 3, 100, 200, 10, 100, "b2")
	if err != nil {
		t.Fatalf("bid item 200: %v", err)
	}
	if bidA.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("item-200 bid should fill sellA, got status=%v", bidA.GetStatus())
	}
	if got := ledger.totalQty(); got != 20 {
		t.Fatalf("total settled qty = %d, want 20", got)
	}
}

// TestMatch_CrossItemOnlyRests 验证只有异物品对手盘时,新单绝不成交而是挂簿(两单都保留)。
func TestMatch_CrossItemOnlyRests(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	sellA, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "sA") // 卖 item 200
	// 买家出价买 item 201:无同物品对手 → 不成交,自身挂簿。
	bid, err := uc.Bid(ctx, 2, 100, 201, 10, 100, "b1")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("bid status = %v, want OPEN (no same-item counterparty)", bid.GetStatus())
	}
	if got := ledger.totalQty(); got != 0 {
		t.Fatalf("settled qty = %d, want 0", got)
	}
	gotA := mustGetOrder(t, uc, sellA.GetOrderId())
	if gotA.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("sellA must stay OPEN, got %v", gotA.GetStatus())
	}
}

func TestPlaceOrder_Idempotent(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()

	first, _ := uc.Bid(ctx, 2, 100, 200, 5, 100, "dup")
	second, err := uc.Bid(ctx, 2, 100, 200, 5, 100, "dup")
	if err != nil {
		t.Fatalf("second bid: %v", err)
	}
	if first.GetOrderId() != second.GetOrderId() {
		t.Fatalf("idempotent replay returned different order: %d vs %d", first.GetOrderId(), second.GetOrderId())
	}
	owned, _ := repo.ListOwnerOrders(ctx, 2, false, 0, 100)
	if len(owned) != 1 {
		t.Fatalf("orders = %d, want 1 (no duplicate insert)", len(owned))
	}
	if got := ledger.totalQty(); got != 0 {
		t.Fatalf("settled qty = %d, want 0", got)
	}
}

func TestSubmit_RegistryHealUsesCanonicalOrderID(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	base := newFakeRepo()
	now := time.Now().UnixMilli()
	canonical := &data.OrderRecord{
		OrderID: 4242, MarketID: 100, OwnerID: 2, Side: data.SideBuy,
		ItemConfigID: 200, Quantity: 5, Price: 100, Status: data.StatusPending,
		IdempotencyKey: "Canonical-Key", CreatedAtMs: now, UpdatedAtMs: now,
	}
	base.orders[canonical.OrderID] = copyOrder(canonical)
	repo := &canonicalClaimRepo{fakeRepo: base, canonical: canonical}
	ledger := &trackLedger{}
	uc := NewAuctionUsecase(
		repo,
		data.NewRedisBookStore(rdb),
		data.NewRedisOwnerSlotLimiter(rdb),
		ledger,
		nil,
		&seqGen{n: 1000}, // order_id
		&seqGen{n: 1000}, // match_id(独立空间,序列可与 order_id 重合)
		conf.AuctionConf{},
	)
	t.Cleanup(uc.Close)

	o, err := uc.Bid(context.Background(), 2, 100, 200, 5, 100, "canonical-key")
	if err != nil {
		t.Fatalf("registry-only heal bid: %v", err)
	}
	if o.GetOrderId() != canonical.OrderID {
		t.Fatalf("returned order_id=%d, want canonical=%d", o.GetOrderId(), canonical.OrderID)
	}
	if ledger.freezeAttempts[canonical.OrderID] != 1 || ledger.freezeAttempts[1001] != 0 {
		t.Fatalf("Freeze must use canonical id: canonical_attempts=%d generated_attempts=%d",
			ledger.freezeAttempts[canonical.OrderID], ledger.freezeAttempts[1001])
	}
	got, found, getErr := base.GetOrder(context.Background(), canonical.MarketID, canonical.OrderID)
	if getErr != nil || !found || got.Status != data.StatusOpen || !got.EscrowVerified {
		t.Fatalf("canonical order not activated: found=%v order=%+v err=%v", found, got, getErr)
	}
}

func TestMatch_PriceTimePriority(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	ctx := context.Background()

	expensive, _ := uc.PlaceOrder(ctx, 5, 100, 200, 5, 110, "s_expensive")
	earlier, _ := uc.PlaceOrder(ctx, 1, 100, 200, 5, 100, "s_early")
	later, _ := uc.PlaceOrder(ctx, 4, 100, 200, 5, 100, "s_late")

	// 买家愿付 110 但只吃 5：先按价格选 100，再在同价中选更早的 earlier。
	bid, err := uc.Bid(ctx, 2, 100, 200, 5, 110, "b1")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetFilledQuantity() != 5 {
		t.Fatalf("bid filled = %d, want 5", bid.GetFilledQuantity())
	}
	// earlier 应被吃满。
	uc2 := uc
	got := mustGetOrder(t, uc2, earlier.GetOrderId())
	if got.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("earlier status = %v, want FILLED (time priority)", got.GetStatus())
	}
	for name, orderID := range map[string]uint64{"expensive": expensive.GetOrderId(), "later": later.GetOrderId()} {
		if other := mustGetOrder(t, uc, orderID); other.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
			t.Fatalf("%s status=%v, want OPEN", name, other.GetStatus())
		}
	}
}

func TestMatch_PartialFill(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s1")
	bid, err := uc.Bid(ctx, 2, 100, 200, 4, 100, "b1")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status = %v, want FILLED", bid.GetStatus())
	}
	got, _, _ := repo.GetOrder(ctx, 100, sell.GetOrderId())
	if got.Remaining() != 6 {
		t.Fatalf("seller remaining = %d, want 6", got.Remaining())
	}
	if got.Status != data.StatusPartial {
		t.Fatalf("seller status = %d, want PARTIAL", got.Status)
	}
	if q := ledger.totalQty(); q != 4 {
		t.Fatalf("settled qty = %d, want 4", q)
	}
}

func TestCancelOrder(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s1")
	if err := uc.CancelOrder(ctx, 1, 100, sell.GetOrderId()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 撤单后买家来吃应吃不到(簿已移除)。
	bid, _ := uc.Bid(ctx, 2, 100, 200, 10, 100, "b1")
	if bid.GetFilledQuantity() != 0 {
		t.Fatalf("bid filled = %d after cancel, want 0", bid.GetFilledQuantity())
	}
}

// TestMatch_SkipsSelfOrder 验证自撮合跳过：同一玩家的对手单不与自己成交，
// 既不结算、也不改变自己的权威挂单。
func TestMatch_SkipsSelfOrder(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()

	// 玩家 1 先挂一个卖单。
	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_self")
	// 同一玩家 1 再下一个能交叉的买单 → 不应自成交。
	bid, err := uc.Bid(ctx, 1, 100, 200, 10, 100, "b_self")
	if err != nil {
		t.Fatalf("self bid: %v", err)
	}
	if bid.GetFilledQuantity() != 0 {
		t.Fatalf("self bid filled = %d, want 0 (self-match skipped)", bid.GetFilledQuantity())
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("self bid status = %v, want OPEN", bid.GetStatus())
	}
	if got := ledger.totalQty(); got != 0 {
		t.Fatalf("settled qty = %d, want 0 (no self settlement)", got)
	}
	// 自己原来的卖单仍未成交。
	got, _, _ := repo.GetOrder(ctx, 100, sell.GetOrderId())
	if got.FilledQuantity != 0 || got.Status != data.StatusOpen {
		t.Fatalf("self sell order changed: filled=%d status=%d, want 0/OPEN", got.FilledQuantity, got.Status)
	}

	// 此时换别的买家(玩家 2)来吃，应能正常成交该权威挂单。
	other, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "b_other")
	if err != nil {
		t.Fatalf("other bid: %v", err)
	}
	if other.GetFilledQuantity() != 10 {
		t.Fatalf("other bid filled = %d, want 10 (self sell still on book)", other.GetFilledQuantity())
	}
	if q := ledger.totalQty(); q != 10 {
		t.Fatalf("settled qty = %d, want 10", q)
	}
}

// TestFreezeFail_OrderRejected 验证挂单冻结失败(资产不足)时:订单作废、不进簿、不撮合。
func TestFreezeFail_OrderRejected(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()

	// 预约:下一个生成的 order_id = 1001(seqGen 从 1000 起,Generate 先自增)。
	ledger.failFor = map[uint64]bool{1001: true}

	o, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_freeze_fail")
	if errcode.As(err) != errcode.ErrAuctionInsufficient {
		t.Fatalf("place with freeze fail should be ErrAuctionInsufficient, got %v (order=%v)", err, o)
	}
	// 订单已落库为 CANCELED,不在簿上。
	got, ok, _ := repo.GetOrder(ctx, 100, 1001)
	if !ok || got.Status != data.StatusCanceled {
		t.Fatalf("order after freeze fail should be CANCELED, ok=%v status=%d", ok, got.Status)
	}
	// 无任何结算。
	if q := ledger.totalQty(); q != 0 {
		t.Fatalf("settled qty = %d, want 0", q)
	}
	// 后续买家来吃应吃不到(没进簿)。
	bid, _ := uc.Bid(ctx, 2, 100, 200, 10, 100, "b_after_fail")
	if bid.GetFilledQuantity() != 0 {
		t.Fatalf("bid filled = %d after rejected order, want 0", bid.GetFilledQuantity())
	}
}

// TestCancelOrder_ReleasesEscrow 验证撤单会退还 escrow。
func TestCancelOrder_ReleasesEscrow(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_rel")
	if err := uc.CancelOrder(ctx, 1, 100, sell.GetOrderId()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if n := ledger.releaseCount(); n != 1 {
		t.Fatalf("release count after cancel = %d, want 1", n)
	}
}

// TestMatch_FullFillReleasesBothEscrows 验证完全成交时买卖双方都退还 escrow 残余。
func TestMatch_FullFillReleasesBothEscrows(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()

	// 卖单先挂(被动),买单后到(主动)完全吃掉 → 双方都 FILLED,各退一次 escrow。
	uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_fill")
	if _, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "b_fill"); err != nil {
		t.Fatalf("bid: %v", err)
	}
	if n := ledger.releaseCount(); n != 2 {
		t.Fatalf("release count after full fill = %d, want 2 (both sides)", n)
	}
}

// TestExpireDueOrders_ExpiresAndReleases 验证过期清扫:超 TTL 的挂单被置 EXPIRED、退还 escrow,
// 之后买家来吃吃不到(已移出簿)。
func TestExpireDueOrders_ExpiresAndReleases(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	uc.cfg.OrderTTLSeconds = 1 // 1 秒过期
	ctx := context.Background()

	sell, _ := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_exp")
	// 手动把创建时间提前到很久以前,模拟超 TTL。
	repo.mu.Lock()
	repo.orders[sell.GetOrderId()].CreatedAtMs = nowMs() - 10_000
	repo.mu.Unlock()

	n, err := uc.ExpireDueOrders(ctx)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 1 {
		t.Fatalf("expired count = %d, want 1", n)
	}
	got, _, _ := repo.GetOrder(ctx, 100, sell.GetOrderId())
	if got.Status != data.StatusExpired {
		t.Fatalf("order status = %d, want EXPIRED(5)", got.Status)
	}
	if rc := ledger.releaseCount(); rc != 1 {
		t.Fatalf("release count after expire = %d, want 1", rc)
	}
	// 过期后买家来吃吃不到(已移出簿)。
	bid, _ := uc.Bid(ctx, 2, 100, 200, 10, 100, "b_after_exp")
	if bid.GetFilledQuantity() != 0 {
		t.Fatalf("bid filled = %d after expire, want 0", bid.GetFilledQuantity())
	}
}

// TestExpireDueOrders_DisabledByDefault 验证 OrderTTLSeconds<=0 时清扫是 no-op。
func TestExpireDueOrders_DisabledByDefault(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	ctx := context.Background()
	uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "s_noexp")
	n, err := uc.ExpireDueOrders(ctx)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 0 {
		t.Fatalf("expired count = %d, want 0 (TTL disabled)", n)
	}
}

func mustGetOrder(t *testing.T, uc *AuctionUsecase, orderID uint64) *auctionv1.AuctionOrder {
	t.Helper()
	o, ok, err := uc.repo.GetOrder(context.Background(), 100, orderID)
	if err != nil || !ok {
		t.Fatalf("get order %d: ok=%v err=%v", orderID, ok, err)
	}
	return toProtoOrder(o)
}

// ── P0/P1 修复:MySQL 权威选单 + Redis 缓存故障降级 ────────────────────────────

type failingBook struct {
	addErr    error
	removeErr error
}

func (b *failingBook) Add(context.Context, uint32, data.Side, uint64, int64) error {
	return b.addErr
}

func (b *failingBook) Remove(context.Context, uint32, data.Side, uint64) error {
	return b.removeErr
}

func newFailingBookUsecase(t *testing.T) (*AuctionUsecase, *fakeRepo, *trackLedger) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := newFakeRepo()
	ledger := &trackLedger{}
	book := &failingBook{
		addErr:    errors.New("test redis add failed"),
		removeErr: errors.New("test redis remove failed"),
	}
	uc := NewAuctionUsecase(repo, book, data.NewRedisOwnerSlotLimiter(rdb), ledger, nil, &seqGen{n: 1000}, &seqGen{n: 1000}, conf.AuctionConf{})
	t.Cleanup(uc.Close)
	return uc, repo, ledger
}

func TestSubmit_BookAddFailureDoesNotHideAuthoritativeOrder(t *testing.T) {
	uc, _, ledger := newFailingBookUsecase(t)
	ctx := context.Background()

	sell, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "cache-down-sell")
	if err != nil {
		t.Fatalf("Redis Add 失败不得让已持久化挂单报失败: %v", err)
	}
	if sell.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("sell status=%v, want OPEN", sell.GetStatus())
	}

	// Redis 全程失败，买家仍必须从 MySQL 权威库找到并成交该挂单。
	bid, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "cache-down-bid")
	if err != nil {
		t.Fatalf("Redis 不可用时权威撮合失败: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status=%v, want FILLED", bid.GetStatus())
	}
	if got := ledger.totalQty(); got != 10 {
		t.Fatalf("settled qty=%d, want 10", got)
	}
}

func TestCancelOrder_BookRemoveFailureDoesNotBlockAuthority(t *testing.T) {
	uc, _, _ := newFailingBookUsecase(t)
	ctx := context.Background()
	o, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "cancel-cache-down")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if err := uc.CancelOrder(ctx, 1, 100, o.GetOrderId()); err != nil {
		t.Fatalf("Redis Remove 失败不得阻断权威撤单: %v", err)
	}
	got := mustGetOrder(t, uc, o.GetOrderId())
	if got.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_CANCELED {
		t.Fatalf("status=%v, want CANCELED", got.GetStatus())
	}
}

func TestMatch_MissingRedisEntryStillMatchesAuthority(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()
	sell, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "missing-cache")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if err := uc.book.Remove(ctx, 100, data.SideSell, sell.GetOrderId()); err != nil {
		t.Fatalf("remove cache entry: %v", err)
	}
	bid, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "match-without-cache")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED || ledger.totalQty() != 10 {
		t.Fatalf("缺失 Redis 条目时仍应成交: status=%v qty=%d", bid.GetStatus(), ledger.totalQty())
	}
}

func TestMatch_LargeCrossItemPrefixCannotStarveExactItem(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()
	for i := 0; i < 128; i++ {
		if _, err := uc.PlaceOrder(ctx, uint64(10+i), 100, uint32(1000+i), 1, 100,
			fmt.Sprintf("wrong-item-%d", i)); err != nil {
			t.Fatalf("place wrong item %d: %v", i, err)
		}
	}
	exact, err := uc.PlaceOrder(ctx, 500, 100, 200, 10, 100, "exact-item")
	if err != nil {
		t.Fatalf("place exact: %v", err)
	}
	bid, err := uc.Bid(ctx, 600, 100, 200, 10, 100, "exact-bid")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status=%v, want FILLED", bid.GetStatus())
	}
	if got := mustGetOrder(t, uc, exact.GetOrderId()); got.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("exact order status=%v, want FILLED", got.GetStatus())
	}
	if ledger.totalQty() != 10 {
		t.Fatalf("settled qty=%d, want 10", ledger.totalQty())
	}
}

func TestMatch_LargeSelfPrefixCannotStarveExternalOrder(t *testing.T) {
	uc, _, ledger := newTestUsecase(t)
	ctx := context.Background()
	const incomingOwner uint64 = 700
	for i := 0; i < 128; i++ {
		if _, err := uc.PlaceOrder(ctx, incomingOwner, 100, 200, 1, 100,
			fmt.Sprintf("self-prefix-%d", i)); err != nil {
			t.Fatalf("place self order %d: %v", i, err)
		}
	}
	external, err := uc.PlaceOrder(ctx, 701, 100, 200, 10, 100, "external-after-self-prefix")
	if err != nil {
		t.Fatalf("place external: %v", err)
	}
	bid, err := uc.Bid(ctx, incomingOwner, 100, 200, 10, 100, "self-owner-bid")
	if err != nil {
		t.Fatalf("bid: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("bid status=%v, want FILLED", bid.GetStatus())
	}
	if got := mustGetOrder(t, uc, external.GetOrderId()); got.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED {
		t.Fatalf("external order status=%v, want FILLED", got.GetStatus())
	}
	if ledger.totalQty() != 10 {
		t.Fatalf("settled qty=%d, want 10", ledger.totalQty())
	}
}

func TestPendingOrder_IsNeverMatchCandidate(t *testing.T) {
	repo := newFakeRepo()
	pending := &data.OrderRecord{
		OrderID: 1, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "pending",
	}
	if _, _, err := repo.ClaimOrder(context.Background(), pending); err != nil {
		t.Fatalf("claim pending: %v", err)
	}
	if got, found, err := repo.FindBestActiveOrder(context.Background(), 100, 200, data.SideSell, 2); err != nil || found {
		t.Fatalf("PENDING 不得被选中: found=%v order=%v err=%v", found, got, err)
	}
}

func TestReserveMatch_ConcurrentIsAtomic(t *testing.T) {
	repo := newFakeRepo()
	ctx := context.Background()
	seed := func(o *data.OrderRecord) {
		t.Helper()
		if _, _, err := repo.ClaimOrder(ctx, o); err != nil {
			t.Fatalf("claim %d: %v", o.OrderID, err)
		}
		if ok, err := repo.ConfirmOrderEscrow(ctx, o.MarketID, o.OrderID, 1); err != nil || !ok {
			t.Fatalf("confirm escrow %d: ok=%v err=%v", o.OrderID, ok, err)
		}
		if ok, err := repo.ActivateOrder(ctx, o.MarketID, o.OrderID, 1); err != nil || !ok {
			t.Fatalf("activate %d: ok=%v err=%v", o.OrderID, ok, err)
		}
	}
	seed(&data.OrderRecord{OrderID: 1, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200, Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "s"})
	seed(&data.OrderRecord{OrderID: 2, MarketID: 100, OwnerID: 2, Side: data.SideBuy, ItemConfigID: 200, Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "b1"})
	seed(&data.OrderRecord{OrderID: 3, MarketID: 100, OwnerID: 3, Side: data.SideBuy, ItemConfigID: 200, Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "b2"})

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for i, incomingID := range []uint64{2, 3} {
		wg.Add(1)
		go func(matchID, orderID uint64) {
			defer wg.Done()
			_, _, _, reserved, err := repo.ReserveMatch(ctx, 100, orderID, 1, matchID, 2)
			if err != nil {
				t.Errorf("reserve: %v", err)
			}
			results <- reserved
		}(uint64(10+i), incomingID)
	}
	wg.Wait()
	close(results)
	var reservedCount int
	for ok := range results {
		if ok {
			reservedCount++
		}
	}
	if reservedCount != 1 {
		t.Fatalf("reserved=%d, want exactly 1", reservedCount)
	}
	seller, _, _ := repo.GetOrder(ctx, 100, 1)
	if seller.FilledQuantity != 10 || seller.Status != data.StatusFilled || len(repo.matches) != 1 {
		t.Fatalf("atomic reserve failed: seller=%+v matches=%d", seller, len(repo.matches))
	}
}

func TestSettleFailure_PersistsAndReconciles(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	if _, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "durable-sell"); err != nil {
		t.Fatalf("place: %v", err)
	}
	ledger.mu.Lock()
	ledger.settleFailures = 1
	ledger.mu.Unlock()
	bid, err := uc.Bid(ctx, 2, 100, 200, 10, 100, "durable-bid")
	if err != nil {
		t.Fatalf("已持久化 PENDING 成交不应伪装成未受理: %v", err)
	}
	if bid.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_FILLED || ledger.totalQty() != 0 {
		t.Fatalf("reserve/settle window wrong: status=%v settled=%d", bid.GetStatus(), ledger.totalQty())
	}
	repo.mu.Lock()
	var pendingCount int
	for _, m := range repo.matches {
		if m.SettlementStatus == data.SettlementPending {
			pendingCount++
			if m.SettlementNextAttemptAtMs <= time.Now().UnixMilli() {
				repo.mu.Unlock()
				t.Fatal("failed settlement must be deferred out of current ready batch")
			}
			m.SettlementNextAttemptAtMs = 0 // 模拟退避到期，不在单测里真实等待 30 秒。
		}
	}
	repo.mu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("persisted pending matches=%d, want 1", pendingCount)
	}
	if _, found, _ := repo.GetReleasableOrder(ctx, 100, bid.GetOrderId()); found {
		t.Fatal("成交仍 PENDING 时不得释放终态订单 escrow")
	}
	settled, released, err := uc.ReconcilePendingSideEffects(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if settled != 1 || released != 2 || ledger.totalQty() != 10 {
		t.Fatalf("settled=%d released=%d qty=%d, want 1/2/10", settled, released, ledger.totalQty())
	}
	pending, _ := repo.ListPendingMatches(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("pending matches after reconcile=%d", len(pending))
	}
}

func TestReleaseFailure_PersistsAndReconciles(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	o, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "release-retry")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	ledger.mu.Lock()
	ledger.releaseFailures = map[uint64]int{o.GetOrderId(): 1}
	ledger.mu.Unlock()
	if err := uc.CancelOrder(ctx, 1, 100, o.GetOrderId()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	stored, _, _ := repo.GetOrder(ctx, 100, o.GetOrderId())
	if !stored.ReleasePending || ledger.releaseCount() != 0 {
		t.Fatalf("release failure must persist marker: pending=%v count=%d", stored.ReleasePending, ledger.releaseCount())
	}
	if stored.ReleaseNextAttemptAtMs <= time.Now().UnixMilli() {
		t.Fatal("failed release must be deferred out of current ready batch")
	}
	repo.mu.Lock()
	repo.orders[o.GetOrderId()].ReleaseNextAttemptAtMs = 0 // 模拟退避到期。
	repo.mu.Unlock()
	_, released, err := uc.ReconcilePendingSideEffects(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stored, _, _ = repo.GetOrder(ctx, 100, o.GetOrderId())
	if released != 1 || stored.ReleasePending || ledger.releaseCount() != 1 {
		t.Fatalf("release retry incomplete: released=%d pending=%v count=%d", released, stored.ReleasePending, ledger.releaseCount())
	}
}

func TestIdempotentReplay_RecoversPendingFreezeAndActivates(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	pending := &data.OrderRecord{
		OrderID: 9001, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "resume-pending",
	}
	if _, _, err := repo.ClaimOrder(ctx, pending); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// 模拟 Freeze 已成功但进程在 Activate 前退出；重试必须安全重复 Freeze。
	if err := ledger.Freeze(ctx, 1, 9001, data.SideSell, 200, 10, 100); err != nil {
		t.Fatalf("pre-freeze: %v", err)
	}
	o, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "resume-pending")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if o.GetOrderId() != 9001 || o.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("resumed order=%d status=%v", o.GetOrderId(), o.GetStatus())
	}
	ledger.mu.Lock()
	attempts, unique := ledger.freezeAttempts[9001], len(ledger.freezes)
	ledger.mu.Unlock()
	if attempts != 2 || unique != 1 {
		t.Fatalf("Freeze idempotency not exercised: attempts=%d unique=%d", attempts, unique)
	}
}

func TestPendingCrashWithoutClientRetry_BackgroundRecovers(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	seller, err := uc.PlaceOrder(ctx, 1, 100, 200, 10, 100, "resting-before-crash")
	if err != nil {
		t.Fatalf("place resting: %v", err)
	}
	pending := &data.OrderRecord{
		OrderID: 9101, MarketID: 100, OwnerID: 2, Side: data.SideBuy, ItemConfigID: 200,
		Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "crash-no-retry",
	}
	if _, _, err := repo.ClaimOrder(ctx, pending); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("background recover: %v", err)
	}
	stored, _, _ := repo.GetOrder(ctx, 100, 9101)
	resting, _, _ := repo.GetOrder(ctx, 100, seller.GetOrderId())
	if stored.Status != data.StatusFilled || resting.Status != data.StatusFilled || ledger.totalQty() != 10 {
		t.Fatalf("PENDING 必须先撮合再激活: incoming=%d resting=%d qty=%d",
			stored.Status, resting.Status, ledger.totalQty())
	}
}

func TestPendingCrashAfterEscrowConfirm_BackgroundRecoveryIsIdempotent(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	pending := &data.OrderRecord{
		OrderID: 9151, MarketID: 100, OwnerID: 2, Side: data.SideBuy, ItemConfigID: 200,
		Quantity: 3, Price: 100, Status: data.StatusPending, IdempotencyKey: "crash-after-confirm",
		CreatedAtMs: nowMs(), UpdatedAtMs: nowMs(),
	}
	if _, _, err := repo.ClaimOrder(ctx, pending); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := ledger.Freeze(ctx, pending.OwnerID, pending.OrderID, pending.Side,
		pending.ItemConfigID, pending.Quantity, pending.Price); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if ok, err := repo.ConfirmOrderEscrow(ctx, pending.MarketID, pending.OrderID, nowMs()); err != nil || !ok {
		t.Fatalf("confirm before crash: ok=%v err=%v", ok, err)
	}
	// 模拟 marker 提交后、撮合/激活前退出。后台会重复 Freeze+Confirm，两者都必须幂等。
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("background recover confirmed pending: %v", err)
	}
	got, found, err := repo.GetOrder(ctx, pending.MarketID, pending.OrderID)
	if err != nil || !found || got.Status != data.StatusOpen || !got.EscrowVerified {
		t.Fatalf("confirmed pending not activated: found=%v order=%+v err=%v", found, got, err)
	}
	ledger.mu.Lock()
	attempts, unique := ledger.freezeAttempts[pending.OrderID], len(ledger.freezes)
	ledger.mu.Unlock()
	if attempts != 2 || unique != 1 {
		t.Fatalf("idempotent recovery attempts=%d unique freezes=%d", attempts, unique)
	}
}

func TestMatchPendingCrash_AfterFirstReserveContinuesInBackground(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	sell1, err := uc.PlaceOrder(ctx, 1, 100, 200, 5, 100, "continuation-s1")
	if err != nil {
		t.Fatalf("place sell1: %v", err)
	}
	sell2, err := uc.PlaceOrder(ctx, 2, 100, 200, 5, 100, "continuation-s2")
	if err != nil {
		t.Fatalf("place sell2: %v", err)
	}
	incoming := &data.OrderRecord{
		OrderID: 9301, MarketID: 100, OwnerID: 3, Side: data.SideBuy, ItemConfigID: 200,
		Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "continuation-bid",
	}
	if _, _, err := repo.ClaimOrder(ctx, incoming); err != nil {
		t.Fatalf("claim incoming: %v", err)
	}
	if err := ledger.Freeze(ctx, 3, 9301, data.SideBuy, 200, 10, 100); err != nil {
		t.Fatalf("freeze incoming: %v", err)
	}
	if ok, err := repo.ConfirmOrderEscrow(ctx, 100, 9301, nowMs()); err != nil || !ok {
		t.Fatalf("confirm incoming escrow: ok=%v err=%v", ok, err)
	}
	_, updated, _, reserved, err := repo.ReserveMatch(ctx, 100, 9301, sell1.GetOrderId(), 9401, nowMs())
	if err != nil || !reserved {
		t.Fatalf("first reserve: reserved=%v err=%v", reserved, err)
	}
	if updated.Status != data.StatusPartial || !updated.MatchPending || updated.Remaining() != 5 {
		t.Fatalf("first reserve must persist continuation: %+v", updated)
	}

	// 模拟事务提交后进程立即退出：没有调用后续 match/Settle，由后台从 match_pending 续跑。
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("reconcile continuation: %v", err)
	}
	got, _, _ := repo.GetOrder(ctx, 100, 9301)
	gotSell2, _, _ := repo.GetOrder(ctx, 100, sell2.GetOrderId())
	if got.Status != data.StatusFilled || got.MatchPending || gotSell2.Status != data.StatusFilled || ledger.totalQty() != 10 {
		t.Fatalf("continuation incomplete: incoming=%+v sell2=%+v qty=%d", got, gotSell2, ledger.totalQty())
	}
}

func TestPendingCrash_FreezeErrorRejectsAndReleases(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	pending := &data.OrderRecord{
		OrderID: 9201, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 10, Price: 100, Status: data.StatusPending, IdempotencyKey: "crash-freeze-error",
	}
	if _, _, err := repo.ClaimOrder(ctx, pending); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ledger.failFor = map[uint64]bool{9201: true}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("reconcile resolved freeze error should converge: %v", err)
	}
	stored, _, _ := repo.GetOrder(ctx, 100, 9201)
	if stored.Status != data.StatusCanceled || stored.ReleasePending || ledger.releaseCount() != 1 {
		t.Fatalf("freeze error recovery: status=%d pending=%v releases=%d",
			stored.Status, stored.ReleasePending, ledger.releaseCount())
	}
}

func TestLegacyUnverifiedActiveOrder_EnsuresEscrowBeforeMatching(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	buy, err := uc.Bid(ctx, 2, 100, 200, 1, 100, "legacy-resting-buy")
	if err != nil {
		t.Fatalf("place resting buy: %v", err)
	}
	legacy := &data.OrderRecord{
		OrderID: 500, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, Price: 100, Status: data.StatusOpen, IdempotencyKey: "legacy-unverified",
		CreatedAtMs: nowMs() - 1000, UpdatedAtMs: nowMs() - 1000,
	}
	if _, already, err := repo.ClaimOrder(ctx, legacy); err != nil || already {
		t.Fatalf("seed legacy order: already=%v err=%v", already, err)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("reconcile legacy order: %v", err)
	}
	gotLegacy, _, _ := repo.GetOrder(ctx, 100, legacy.OrderID)
	gotBuy, _, _ := repo.GetOrder(ctx, 100, buy.GetOrderId())
	ledger.mu.Lock()
	ensures := append([]uint64(nil), ledger.ensures...)
	ledger.mu.Unlock()
	if len(ensures) != 1 || ensures[0] != legacy.OrderID {
		t.Fatalf("Ensure calls=%v, want [%d]", ensures, legacy.OrderID)
	}
	if gotLegacy.Status != data.StatusFilled || gotBuy.Status != data.StatusFilled || ledger.totalQty() != 1 {
		t.Fatalf("legacy match not converged: legacy=%+v buy=%+v qty=%d", gotLegacy, gotBuy, ledger.totalQty())
	}
}

func TestLegacyUnverifiedActiveOrder_DeterministicEnsureFailureCancels(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	legacy := &data.OrderRecord{
		OrderID: 501, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, Price: 100, Status: data.StatusOpen, IdempotencyKey: "legacy-insufficient",
		CreatedAtMs: nowMs() - 1000, UpdatedAtMs: nowMs() - 1000,
	}
	if _, _, err := repo.ClaimOrder(ctx, legacy); err != nil {
		t.Fatalf("seed legacy order: %v", err)
	}
	ledger.ensureFailures = map[uint64]error{
		legacy.OrderID: errcode.New(errcode.ErrInventoryInsufficient, "test legacy escrow missing"),
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("deterministic failure should converge: %v", err)
	}
	got, _, _ := repo.GetOrder(ctx, 100, legacy.OrderID)
	if got.Status != data.StatusCanceled || got.ReleasePending || ledger.releaseCount() != 1 {
		t.Fatalf("legacy failure not canceled/released: order=%+v releases=%d", got, ledger.releaseCount())
	}
}

func TestMatchEventOutbox_RetriesAfterPublishFailure(t *testing.T) {
	uc, repo, _ := newTestUsecase(t)
	events := &trackEvents{matchFailures: 1}
	uc.events = events
	ctx := context.Background()
	if _, err := uc.PlaceOrder(ctx, 1, 100, 200, 1, 100, "event-sell"); err != nil {
		t.Fatalf("place sell: %v", err)
	}
	if _, err := uc.Bid(ctx, 2, 100, 200, 1, 100, "event-buy"); err != nil {
		t.Fatalf("place buy: %v", err)
	}
	repo.mu.Lock()
	var match *data.MatchRecord
	for _, candidate := range repo.matches {
		match = candidate
		candidate.EventNextAttemptAtMs = 0 // 模拟退避到期。
	}
	repo.mu.Unlock()
	if match == nil || !match.EventPending {
		t.Fatalf("completed match must keep durable event marker: %+v", match)
	}
	events.mu.Lock()
	immediateAttempts := events.matchAttempts
	events.mu.Unlock()
	if immediateAttempts != 0 {
		t.Fatalf("market critical path must not synchronously publish Kafka: attempts=%d", immediateAttempts)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("asset reconciler: %v", err)
	}
	events.mu.Lock()
	assetWorkerAttempts := events.matchAttempts
	events.mu.Unlock()
	if assetWorkerAttempts != 0 {
		t.Fatalf("asset reconciler must not call Kafka: attempts=%d", assetWorkerAttempts)
	}
	if _, err := uc.ReconcilePendingMatchEvents(ctx); err == nil {
		t.Fatal("first background publish is configured to fail")
	}
	repo.mu.Lock()
	match.EventNextAttemptAtMs = 0 // 模拟退避再次到期。
	repo.mu.Unlock()
	if _, err := uc.ReconcilePendingMatchEvents(ctx); err != nil {
		t.Fatalf("second background event retry: %v", err)
	}
	repo.mu.Lock()
	pending := match.EventPending
	repo.mu.Unlock()
	events.mu.Lock()
	attempts, delivered := events.matchAttempts, append([]uint64(nil), events.matches...)
	events.mu.Unlock()
	if pending || attempts != 2 || len(delivered) != 1 || delivered[0] != match.MatchID {
		t.Fatalf("outbox not converged: pending=%v attempts=%d delivered=%v match=%d",
			pending, attempts, delivered, match.MatchID)
	}
}

func TestMatchEventOutbox_WaitsForTerminalEscrowRelease(t *testing.T) {
	uc, repo, _ := newTestUsecase(t)
	events := &trackEvents{}
	uc.events = events
	now := nowMs()
	repo.mu.Lock()
	repo.orders[6101] = &data.OrderRecord{
		OrderID: 6101, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, FilledQuantity: 1, Price: 100, Status: data.StatusFilled, ReleasePending: true,
	}
	repo.orders[6102] = &data.OrderRecord{
		OrderID: 6102, MarketID: 100, OwnerID: 2, Side: data.SideBuy, ItemConfigID: 200,
		Quantity: 2, FilledQuantity: 1, Price: 100, Status: data.StatusPartial, ReleasePending: true,
	}
	repo.matches[6201] = &data.MatchRecord{
		MatchID: 6201, MarketID: 100, SellOrderID: 6101, BuyOrderID: 6102,
		SellerID: 1, BuyerID: 2, ItemConfigID: 200, Quantity: 1, Price: 100,
		MatchedAtMs: now, SettlementStatus: data.SettlementCompleted, EventPending: true,
	}
	repo.mu.Unlock()

	published, err := uc.ReconcilePendingMatchEvents(context.Background())
	if err != nil || published != 0 {
		t.Fatalf("release pending must hide event: published=%d err=%v", published, err)
	}
	events.mu.Lock()
	attempts := events.matchAttempts
	events.mu.Unlock()
	if attempts != 0 {
		t.Fatalf("event published before terminal escrow release: attempts=%d", attempts)
	}

	repo.mu.Lock()
	repo.orders[6101].ReleasePending = false // PARTIAL 旧默认 marker=1 仍不得阻塞。
	repo.mu.Unlock()
	published, err = uc.ReconcilePendingMatchEvents(context.Background())
	if err != nil || published != 1 {
		t.Fatalf("released terminal escrow should expose event: published=%d err=%v", published, err)
	}
}

func TestAuditPush_NeverBlocksMarketOrRepairCaller(t *testing.T) {
	events := &blockingAuditEvents{entered: make(chan struct{}), release: make(chan struct{})}
	uc := NewAuctionUsecase(nil, nil, nil, nil, events, &seqGen{}, &seqGen{}, conf.AuctionConf{AuditQueueCapacity: 1})
	defer uc.Close()
	defer close(events.release) // 后注册，先释放被阻塞的 worker，再等待 Close。

	order := &auctionv1.AuctionOrder{OrderId: 9001}
	started := time.Now()
	uc.pushAudit(context.Background(), order)
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("first audit enqueue blocked caller: %v", elapsed)
	}
	select {
	case <-events.entered:
	case <-time.After(time.Second):
		t.Fatal("audit worker did not start")
	}

	// worker 已卡在底层 PushAudit；第二条占满队列，第三条必须立即告警丢弃，均不能反压调用者。
	started = time.Now()
	uc.pushAudit(context.Background(), &auctionv1.AuctionOrder{OrderId: 9002})
	uc.pushAudit(context.Background(), &auctionv1.AuctionOrder{OrderId: 9003})
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("full audit queue blocked caller: %v", elapsed)
	}
}

func TestTerminalMarkerRepair_ReleasesEscrowAndSlot(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	ctx := context.Background()
	o := &data.OrderRecord{
		OrderID: 502, MarketID: 100, OwnerID: 1, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, FilledQuantity: 1, Price: 100, Status: data.StatusFilled,
		EscrowVerified: true, MatchPending: true, IdempotencyKey: "terminal-marker-drift",
	}
	if _, _, err := repo.ClaimOrder(ctx, o); err != nil {
		t.Fatalf("seed terminal drift: %v", err)
	}
	if ok, err := uc.slots.Reserve(ctx, o.OwnerID,
		data.OwnerOrderSlot{MarketID: o.MarketID, OrderID: o.OrderID}, 200); err != nil || !ok {
		t.Fatalf("seed owner slot: ok=%v err=%v", ok, err)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("repair terminal marker: %v", err)
	}
	got, _, _ := repo.GetOrder(ctx, o.MarketID, o.OrderID)
	slots, err := uc.slots.List(ctx, o.OwnerID, 10)
	if err != nil || len(slots) != 0 || got.ReleasePending || got.MatchPending || got.EscrowVerified || ledger.releaseCount() != 1 {
		t.Fatalf("terminal repair not converged: order=%+v slots=%v releases=%d err=%v",
			got, slots, ledger.releaseCount(), err)
	}
}

func TestOwnerOrderLimit_PrewarmCountsLegacyActiveOrders(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	uc.cfg.MaxActiveOrdersPerPlayer = 1
	ctx := context.Background()
	legacy := &data.OrderRecord{
		OrderID: 500, MarketID: 100, OwnerID: 77, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, Price: 10, Status: data.StatusOpen, IdempotencyKey: "legacy-before-quota",
		CreatedAtMs: nowMs() - 1000, UpdatedAtMs: nowMs() - 1000,
	}
	if _, _, err := repo.ClaimOrder(ctx, legacy); err != nil {
		t.Fatalf("seed legacy order: %v", err)
	}
	_, err := uc.PlaceOrder(ctx, 77, 101, 200, 1, 10, "must-count-legacy")
	if errcode.As(err) != errcode.ErrAuctionOrderLimit {
		t.Fatalf("legacy active order must consume quota: err=%v code=%d", err, errcode.As(err))
	}
	newOrder, found, getErr := repo.GetOrder(ctx, 101, 1001)
	if getErr != nil || !found || newOrder.Status != data.StatusCanceled || ledger.freezeAttempts[1001] != 0 {
		t.Fatalf("excess order must cancel before freeze: found=%v order=%+v attempts=%d err=%v",
			found, newOrder, ledger.freezeAttempts[1001], getErr)
	}
	slots, listErr := uc.slots.List(ctx, legacy.OwnerID, 10)
	if listErr != nil || len(slots) != 1 || slots[0].OrderID != legacy.OrderID {
		t.Fatalf("prewarm slots=%+v err=%v, want legacy only", slots, listErr)
	}
}

func TestOwnerOrderLimit_RejectsExcessPendingAtomically(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	uc.cfg.MaxActiveOrdersPerPlayer = 2
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := uc.PlaceOrder(ctx, 77, uint32(100+i), 200, 1, 10, fmt.Sprintf("quota-%d", i)); err != nil {
			t.Fatalf("place within limit %d: %v", i, err)
		}
	}
	_, err := uc.PlaceOrder(ctx, 77, 102, 200, 1, 10, "quota-excess")
	if errcode.As(err) != errcode.ErrAuctionOrderLimit {
		t.Fatalf("excess error=%v code=%d, want %d", err, errcode.As(err), errcode.ErrAuctionOrderLimit)
	}
	excess, found, getErr := repo.GetOrder(ctx, 102, 1003)
	if getErr != nil || !found || excess.Status != data.StatusCanceled {
		t.Fatalf("excess pending not canceled: found=%v order=%+v err=%v", found, excess, getErr)
	}
	slots, listErr := uc.slots.List(ctx, 77, 10)
	if listErr != nil || len(slots) != 2 {
		t.Fatalf("owner slots len=%d err=%v, want 2", len(slots), listErr)
	}
	if ledger.freezeAttempts[1003] != 0 {
		t.Fatalf("excess order must not freeze, attempts=%d", ledger.freezeAttempts[1003])
	}
}

func TestOwnerOrderLimit_PrunesMySQLConfirmedTerminalSlot(t *testing.T) {
	uc, repo, _ := newTestUsecase(t)
	uc.cfg.MaxActiveOrdersPerPlayer = 1
	ctx := context.Background()
	first, err := uc.PlaceOrder(ctx, 88, 100, 200, 1, 10, "stale-slot")
	if err != nil {
		t.Fatalf("first place: %v", err)
	}
	// 模拟 MySQL 终态事务提交后、Redis SREM 前进程退出。
	changed, err := repo.MarkOrderTerminal(ctx, 100, first.GetOrderId(), data.StatusCanceled, nowMs())
	if err != nil || !changed {
		t.Fatalf("mark terminal: changed=%v err=%v", changed, err)
	}
	second, err := uc.PlaceOrder(ctx, 88, 101, 200, 1, 10, "after-prune")
	if err != nil {
		t.Fatalf("terminal stale slot should be pruned: %v", err)
	}
	if second.GetStatus() != auctionv1.AuctionOrderStatus_AUCTION_ORDER_STATUS_OPEN {
		t.Fatalf("second status=%v, want OPEN", second.GetStatus())
	}
}

func TestOwnerOrderLimit_PruneReadFailureIsFailClosed(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	base := newFakeRepo()
	base.orders[500] = &data.OrderRecord{
		OrderID: 500, MarketID: 100, OwnerID: 89, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, Price: 10, Status: data.StatusCanceled,
	}
	repo := &getOrderFailRepo{fakeRepo: base, failOrderID: 500}
	slots := data.NewRedisOwnerSlotLimiter(rdb)
	ctx := context.Background()
	if ok, err := slots.Reserve(ctx, 89, data.OwnerOrderSlot{MarketID: 100, OrderID: 500}, 1); err != nil || !ok {
		t.Fatalf("seed stale slot: ok=%v err=%v", ok, err)
	}
	uc := NewAuctionUsecase(repo, data.NewRedisBookStore(rdb), slots, &trackLedger{}, nil,
		&seqGen{n: 600}, &seqGen{n: 600}, conf.AuctionConf{MaxActiveOrdersPerPlayer: 1})
	t.Cleanup(uc.Close)
	if _, err := uc.PlaceOrder(ctx, 89, 101, 200, 1, 10, "fail-closed"); errcode.As(err) != errcode.ErrAuctionOrderLimit {
		t.Fatalf("uncertain authority must stay full: err=%v code=%d", err, errcode.As(err))
	}
	listed, err := slots.List(ctx, 89, 10)
	if err != nil || len(listed) != 1 || listed[0].OrderID != 500 {
		t.Fatalf("uncertain slot must not be pruned: slots=%+v err=%v", listed, err)
	}
}

func TestRecoverPendingOrder_ReservesSlotBeforeFreeze(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	uc.cfg.MaxActiveOrdersPerPlayer = 1
	ctx := context.Background()
	if _, err := uc.PlaceOrder(ctx, 99, 100, 200, 1, 10, "occupy-slot"); err != nil {
		t.Fatalf("occupy slot: %v", err)
	}
	pending := &data.OrderRecord{
		OrderID: 9000, MarketID: 101, OwnerID: 99, Side: data.SideSell, ItemConfigID: 200,
		Quantity: 1, Price: 10, Status: data.StatusPending, IdempotencyKey: "pending-over-limit",
		CreatedAtMs: nowMs(), UpdatedAtMs: nowMs(),
	}
	if _, already, err := repo.ClaimOrder(ctx, pending); err != nil || already {
		t.Fatalf("claim pending: already=%v err=%v", already, err)
	}
	if _, _, err := uc.ReconcilePendingSideEffects(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, found, err := repo.GetOrder(ctx, 101, 9000)
	if err != nil || !found || got.Status != data.StatusCanceled {
		t.Fatalf("pending over limit status: found=%v order=%+v err=%v", found, got, err)
	}
	if ledger.freezeAttempts[9000] != 0 {
		t.Fatalf("background must reserve before freeze, attempts=%d", ledger.freezeAttempts[9000])
	}
}

func TestFilledOrders_ReleaseOwnerSlotsImmediately(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	ctx := context.Background()
	if _, err := uc.PlaceOrder(ctx, 1, 100, 200, 1, 10, "slot-sell"); err != nil {
		t.Fatalf("sell: %v", err)
	}
	if _, err := uc.Bid(ctx, 2, 100, 200, 1, 10, "slot-buy"); err != nil {
		t.Fatalf("buy: %v", err)
	}
	for _, ownerID := range []uint64{1, 2} {
		slots, err := uc.slots.List(ctx, ownerID, 10)
		if err != nil || len(slots) != 0 {
			t.Fatalf("owner %d terminal slots len=%d err=%v", ownerID, len(slots), err)
		}
	}
}

func TestListMyOrders_CursorDefaultAndMaxLimit(t *testing.T) {
	uc, repo, _ := newTestUsecase(t)
	ctx := context.Background()
	repo.mu.Lock()
	for id := uint64(1); id <= 120; id++ {
		repo.orders[id] = &data.OrderRecord{
			OrderID: id, MarketID: uint32(id%3 + 1), OwnerID: 123, Side: data.SideSell,
			ItemConfigID: 200, Quantity: 1, Price: 10, Status: data.StatusCanceled,
		}
	}
	repo.mu.Unlock()

	page, next, more, err := uc.ListMyOrders(ctx, 123, false, 0, 0)
	if err != nil || len(page) != 50 || !more || next != 71 {
		t.Fatalf("default page len=%d next=%d more=%v err=%v", len(page), next, more, err)
	}
	if page[0].GetOrderId() != 120 || page[49].GetOrderId() != 71 {
		t.Fatalf("default page order range=%d..%d", page[0].GetOrderId(), page[49].GetOrderId())
	}
	maxPage, maxNext, maxMore, err := uc.ListMyOrders(ctx, 123, false, 0, 1000)
	if err != nil || len(maxPage) != 100 || !maxMore || maxNext != 21 {
		t.Fatalf("max-clamped first page len=%d next=%d more=%v err=%v", len(maxPage), maxNext, maxMore, err)
	}
	page2, next2, more2, err := uc.ListMyOrders(ctx, 123, false, next, 1000)
	if err != nil || len(page2) != 70 || more2 || next2 != 0 {
		t.Fatalf("max-clamped page len=%d next=%d more=%v err=%v", len(page2), next2, more2, err)
	}
}
