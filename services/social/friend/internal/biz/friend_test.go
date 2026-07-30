// friend_test.go — FriendUsecase 业务逻辑单测(2026-06-15)。
//
// 用内存版 fakeRepo / fakePusher / fakeOnline 复刻 MySQL + kafka + locator 语义,无需真依赖。
// 覆盖:AddFriend / AcceptFriend / ListFriends / Block 正常路径 + 自加 / 拉黑 / 已是好友 / 非本人接受等错误路径。
package biz

import (
	"context"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	friendv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/friend/v1"
	"github.com/luyuancpp/pandora/services/social/friend/internal/conf"
	"github.com/luyuancpp/pandora/services/social/friend/internal/data"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

// fakeRepo 是 data.FriendRepo 的内存实现。
type fakeRepo struct {
	friends  map[uint64]map[uint64]int64 // player → friend → since_ms
	requests map[uint64]*data.FriendRequestRow
	blocks   map[uint64]map[uint64]bool // player → blocked → true

	recommendMutual []data.RecommendRow
	recommendRandom []data.RecommendRow
	mutualCalls     int
	randomCalls     int

	// forceAcceptNotCompleted 模拟「预检后到事务取锁前请求被并发处理」:
	// AcceptRequest 返回 (false, nil),用于 P1 并发假成功回归测试。
	forceAcceptNotCompleted bool
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		friends:  map[uint64]map[uint64]int64{},
		requests: map[uint64]*data.FriendRequestRow{},
		blocks:   map[uint64]map[uint64]bool{},
	}
}

func (f *fakeRepo) AreFriends(_ context.Context, a, b uint64) (bool, error) {
	_, ok := f.friends[a][b]
	return ok, nil
}

func (f *fakeRepo) IsBlocked(_ context.Context, a, b uint64) (bool, error) {
	return f.blocks[a][b] || f.blocks[b][a], nil
}

func (f *fakeRepo) CountFriends(_ context.Context, playerID uint64) (int, error) {
	return len(f.friends[playerID]), nil
}

func (f *fakeRepo) findByPair(requester, target uint64) *data.FriendRequestRow {
	for _, r := range f.requests {
		if r.RequesterID == requester && r.TargetID == target {
			return r
		}
	}
	return nil
}

func (f *fakeRepo) CreateRequest(_ context.Context, newRequestID, requesterID, targetID uint64, maxIncoming int) (uint64, bool, error) {
	if existing := f.findByPair(requesterID, targetID); existing != nil {
		if existing.Status == requestStatusPending {
			return existing.RequestID, true, nil
		}
		if maxIncoming > 0 && f.countIncomingPending(targetID) >= maxIncoming {
			return 0, false, errcode.New(errcode.ErrFriendRequestLimit, "incoming limit")
		}
		// 对齐 MySQL 实现(R4 复审 P1-4):非 pending 重新申请复用行但换新 request_id,
		// 客户端 (request_id, reason) 判重不再吞掉合法的再次申请推送。
		delete(f.requests, existing.RequestID)
		existing.RequestID = newRequestID
		existing.Status = requestStatusPending
		f.requests[newRequestID] = existing
		return newRequestID, false, nil
	}
	if maxIncoming > 0 && f.countIncomingPending(targetID) >= maxIncoming {
		return 0, false, errcode.New(errcode.ErrFriendRequestLimit, "incoming limit")
	}
	f.requests[newRequestID] = &data.FriendRequestRow{
		RequestID:   newRequestID,
		RequesterID: requesterID,
		TargetID:    targetID,
		Status:      requestStatusPending,
	}
	return newRequestID, false, nil
}

func (f *fakeRepo) countIncomingPending(targetID uint64) int {
	n := 0
	for _, r := range f.requests {
		if r.TargetID == targetID && r.Status == requestStatusPending {
			n++
		}
	}
	return n
}

func (f *fakeRepo) GetRequest(_ context.Context, requestID uint64) (*data.FriendRequestRow, bool, error) {
	r, ok := f.requests[requestID]
	if !ok {
		return nil, false, nil
	}
	// 返回副本,避免测试改到内部状态
	cp := *r
	return &cp, true, nil
}

func (f *fakeRepo) addFriendEdge(a, b uint64) {
	if f.friends[a] == nil {
		f.friends[a] = map[uint64]int64{}
	}
	f.friends[a][b] = 1000
}

func (f *fakeRepo) AcceptRequest(_ context.Context, requestID, accepterID uint64, maxFriends int) (bool, error) {
	r, ok := f.requests[requestID]
	if !ok {
		return false, errcode.New(errcode.ErrFriendNotFound, "not found")
	}
	// 模拟并发:预检后到取锁前被 Block / 另一次 accept 处理(P1 回归用)
	if f.forceAcceptNotCompleted {
		return false, nil
	}
	if r.TargetID != accepterID {
		return false, errcode.New(errcode.ErrFriendNotFound, "not for accepter")
	}
	if r.Status != requestStatusPending {
		return false, nil // 已被并发处理,本次未真正完成
	}
	// 事务内权威 block 校验
	if f.blocks[accepterID][r.RequesterID] || f.blocks[r.RequesterID][accepterID] {
		return false, errcode.New(errcode.ErrFriendBlocked, "blocked")
	}
	if maxFriends > 0 {
		if len(f.friends[r.RequesterID]) >= maxFriends || len(f.friends[r.TargetID]) >= maxFriends {
			return false, errcode.New(errcode.ErrFriendLimit, "friend limit reached")
		}
	}
	r.Status = 2 // accepted
	f.addFriendEdge(r.RequesterID, r.TargetID)
	f.addFriendEdge(r.TargetID, r.RequesterID)
	return true, nil
}

func (f *fakeRepo) ListFriends(_ context.Context, playerID uint64) ([]data.FriendRow, error) {
	var out []data.FriendRow
	for fid, since := range f.friends[playerID] {
		out = append(out, data.FriendRow{FriendID: fid, SinceMs: since})
	}
	return out, nil
}

func (f *fakeRepo) Block(_ context.Context, playerID, targetID uint64, maxBlocks int) error {
	if f.blocks[playerID] == nil {
		f.blocks[playerID] = map[uint64]bool{}
	}
	if maxBlocks > 0 && !f.blocks[playerID][targetID] && len(f.blocks[playerID]) >= maxBlocks {
		return errcode.New(errcode.ErrFriendBlockLimit, "block limit")
	}
	f.blocks[playerID][targetID] = true
	delete(f.friends[playerID], targetID)
	delete(f.friends[targetID], playerID)
	if r := f.findByPair(playerID, targetID); r != nil && r.Status == requestStatusPending {
		r.Status = 3
	}
	if r := f.findByPair(targetID, playerID); r != nil && r.Status == requestStatusPending {
		r.Status = 3
	}
	return nil
}

func (f *fakeRepo) RejectRequest(_ context.Context, requestID, rejecterID uint64) (bool, error) {
	r, ok := f.requests[requestID]
	if !ok {
		return false, errcode.New(errcode.ErrFriendNotFound, "not found")
	}
	if r.TargetID != rejecterID {
		return false, errcode.New(errcode.ErrFriendNotFound, "not for rejecter")
	}
	if r.Status != requestStatusPending {
		return false, nil // 已被并发处理
	}
	r.Status = 3 // rejected
	return true, nil
}

func (f *fakeRepo) ListIncomingRequests(_ context.Context, playerID uint64) ([]data.IncomingRequestRow, error) {
	var out []data.IncomingRequestRow
	for _, r := range f.requests {
		if r.TargetID == playerID && r.Status == requestStatusPending {
			out = append(out, data.IncomingRequestRow{
				RequestID:   r.RequestID,
				RequesterID: r.RequesterID,
				CreatedMs:   1000,
			})
		}
	}
	return out, nil
}

func (f *fakeRepo) RemoveFriend(_ context.Context, playerID, targetID uint64) error {
	delete(f.friends[playerID], targetID)
	delete(f.friends[targetID], playerID)
	return nil
}

func (f *fakeRepo) Unblock(_ context.Context, playerID, targetID uint64) error {
	delete(f.blocks[playerID], targetID)
	return nil
}

func (f *fakeRepo) ListBlocks(_ context.Context, playerID uint64) ([]data.BlockRow, error) {
	var out []data.BlockRow
	for bid, on := range f.blocks[playerID] {
		if on {
			out = append(out, data.BlockRow{BlockedID: bid, SinceMs: 2000})
		}
	}
	return out, nil
}

func (f *fakeRepo) RecommendByMutual(_ context.Context, _ uint64, exclude []uint64, limit int) ([]data.RecommendRow, error) {
	f.mutualCalls++
	return pickRecommendRows(f.recommendMutual, exclude, limit), nil
}

func (f *fakeRepo) RecommendRandom(_ context.Context, _ uint64, exclude []uint64, limit int) ([]data.RecommendRow, error) {
	f.randomCalls++
	return pickRecommendRows(f.recommendRandom, exclude, limit), nil
}

// 保留期清理(§9.24):biz 单测不模拟时间,默认 no-op。
func (f *fakeRepo) SweepTerminalRequestsBefore(_ context.Context, mode dbguard.Mode, _, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) DeletePairGuardsBefore(context.Context, int, int) (int64, error) {
	return 0, nil
}

func pickRecommendRows(rows []data.RecommendRow, exclude []uint64, limit int) []data.RecommendRow {
	blocked := map[uint64]bool{}
	for _, id := range exclude {
		blocked[id] = true
	}
	out := make([]data.RecommendRow, 0, limit)
	for _, r := range rows {
		if blocked[r.CandidateID] {
			continue
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// fakePusher 记录推送事件。
type fakePusher struct {
	events []*friendv1.FriendEvent
}

func (p *fakePusher) PushFriendEvent(_ context.Context, _ uint64, evt *friendv1.FriendEvent) error {
	p.events = append(p.events, evt)
	return nil
}

// fakeOnline 返回预置在线状态。
type fakeOnline struct {
	status map[uint64]data.OnlineStatus
}

func (o *fakeOnline) BatchOnline(_ context.Context, ids []uint64) map[uint64]data.OnlineStatus {
	out := map[uint64]data.OnlineStatus{}
	for _, id := range ids {
		if st, ok := o.status[id]; ok {
			out[id] = st
		}
	}
	return out
}

// newUC 构造带 fakes 的 usecase。
func newUC(repo data.FriendRepo, pusher FriendEventPusher, online data.OnlineStatusReader) *FriendUsecase {
	return NewFriendUsecase(repo, pusher, online, conf.FriendConf{MaxFriends: 200})
}

// ── 测试 ──────────────────────────────────────────────────────────────────────

func TestAddFriend_OK_PushesRequestReceived(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)

	reqID, err := uc.AddFriend(context.Background(), 100, 200, 999)
	if err != nil {
		t.Fatalf("AddFriend err: %v", err)
	}
	if reqID != 999 {
		t.Fatalf("want request_id 999, got %d", reqID)
	}
	if len(pusher.events) != 1 {
		t.Fatalf("want 1 push, got %d", len(pusher.events))
	}
	e := pusher.events[0]
	if e.GetToPlayerId() != 200 || e.GetByPlayerId() != 100 {
		t.Fatalf("push routing wrong: by=%d to=%d", e.GetByPlayerId(), e.GetToPlayerId())
	}
	if e.GetReason() != friendv1.FriendEventReason_FRIEND_EVENT_REASON_REQUEST_RECEIVED {
		t.Fatalf("want REQUEST_RECEIVED, got %v", e.GetReason())
	}
}

func TestAddFriend_Self(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	_, err := uc.AddFriend(context.Background(), 100, 100, 1)
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("want ErrInvalidArg, got %v", err)
	}
}

func TestAddFriend_Blocked(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Block(context.Background(), 200, 100, 0) // 200 拉黑了 100
	uc := newUC(repo, &fakePusher{}, nil)
	_, err := uc.AddFriend(context.Background(), 100, 200, 1)
	if errcode.As(err) != errcode.ErrFriendBlocked {
		t.Fatalf("want ErrFriendBlocked, got %v", err)
	}
}

func TestAddFriend_AlreadyFriends(t *testing.T) {
	repo := newFakeRepo()
	repo.addFriendEdge(100, 200)
	repo.addFriendEdge(200, 100)
	uc := newUC(repo, &fakePusher{}, nil)
	_, err := uc.AddFriend(context.Background(), 100, 200, 1)
	if errcode.As(err) != errcode.ErrFriendAlreadyAdded {
		t.Fatalf("want ErrFriendAlreadyAdded, got %v", err)
	}
}

func TestAddFriend_ReusePending(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	id1, err := uc.AddFriend(context.Background(), 100, 200, 999)
	if err != nil {
		t.Fatalf("first AddFriend err: %v", err)
	}
	// 再次发起 → 复用已有 pending request_id,不新建
	id2, err := uc.AddFriend(context.Background(), 100, 200, 1234)
	if err != nil {
		t.Fatalf("second AddFriend err: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("want reuse request_id %d, got %d", id1, id2)
	}
}

func TestAddFriend_LimitReached_EarlyFail(t *testing.T) {
	repo := newFakeRepo()
	// requester 100 已有 2 个好友,上限设 2 → 再发请求提前失败
	repo.addFriendEdge(100, 11)
	repo.addFriendEdge(100, 12)
	uc := NewFriendUsecase(repo, &fakePusher{}, nil, conf.FriendConf{MaxFriends: 2})
	_, err := uc.AddFriend(context.Background(), 100, 200, 1)
	if errcode.As(err) != errcode.ErrFriendLimit {
		t.Fatalf("want ErrFriendLimit, got %v", err)
	}
}

func TestAcceptFriend_LimitReached_AtomicCheck(t *testing.T) {
	repo := newFakeRepo()
	uc := NewFriendUsecase(repo, &fakePusher{}, nil, conf.FriendConf{MaxFriends: 1})
	// requester 100 发请求时还没好友(提前校验过),挂起后 target 200 先攒满 1 个好友
	reqID, err := uc.AddFriend(context.Background(), 100, 200, 999)
	if err != nil {
		t.Fatalf("AddFriend err: %v", err)
	}
	repo.addFriendEdge(200, 77) // 200 已达上限 1
	// 接受时事务内对双方原子校验 → target 已满,拒绝
	if err := uc.AcceptFriend(context.Background(), 200, reqID); errcode.As(err) != errcode.ErrFriendLimit {
		t.Fatalf("want ErrFriendLimit at accept, got %v", err)
	}
	// 好友边不应建立
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); ok {
		t.Fatal("friendship must not be created when limit hit")
	}
}

func TestAcceptFriend_OK_PushesAccepted(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)

	reqID, _ := uc.AddFriend(context.Background(), 100, 200, 999)
	pusher.events = nil // 清掉 REQUEST_RECEIVED

	if err := uc.AcceptFriend(context.Background(), 200, reqID); err != nil {
		t.Fatalf("AcceptFriend err: %v", err)
	}
	// 双向好友边已建立
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); !ok {
		t.Fatal("want 100-200 friends")
	}
	if ok, _ := repo.AreFriends(context.Background(), 200, 100); !ok {
		t.Fatal("want 200-100 friends")
	}
	// 接受通知发给发起方 100
	if len(pusher.events) != 1 {
		t.Fatalf("want 1 push, got %d", len(pusher.events))
	}
	e := pusher.events[0]
	if e.GetToPlayerId() != 100 || e.GetByPlayerId() != 200 {
		t.Fatalf("accept push routing wrong: by=%d to=%d", e.GetByPlayerId(), e.GetToPlayerId())
	}
	if e.GetReason() != friendv1.FriendEventReason_FRIEND_EVENT_REASON_REQUEST_ACCEPTED {
		t.Fatalf("want REQUEST_ACCEPTED, got %v", e.GetReason())
	}
}

func TestAcceptFriend_NotTarget(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	reqID, _ := uc.AddFriend(context.Background(), 100, 200, 999)
	// 第三者 300 试图接受 → 找不到可接受请求
	err := uc.AcceptFriend(context.Background(), 300, reqID)
	if errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("want ErrFriendNotFound, got %v", err)
	}
}

func TestAcceptFriend_NoRequest(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	err := uc.AcceptFriend(context.Background(), 200, 555)
	if errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("want ErrFriendNotFound, got %v", err)
	}
}

// TestAcceptFriend_ConcurrentlyProcessed_NoFakeSuccess 回归 P1:
// 预检看到 pending,但 AcceptRequest 事务内发现请求已被并发处理(Block 改 rejected /
// 另一次 accept)→ 返回 accepted=false。biz 必须不推送 accepted、且不假成功(返回错误)。
func TestAcceptFriend_ConcurrentlyProcessed_NoFakeSuccess(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)

	reqID, _ := uc.AddFriend(context.Background(), 100, 200, 999)
	pusher.events = nil                 // 清掉 REQUEST_RECEIVED
	repo.forceAcceptNotCompleted = true // 模拟两步之间被并发处理

	err := uc.AcceptFriend(context.Background(), 200, reqID)
	if errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("want ErrFriendNotFound (no fake success), got %v", err)
	}
	if len(pusher.events) != 0 {
		t.Fatalf("must NOT push accepted on concurrent processing, got %d", len(pusher.events))
	}
	// 好友边不应建立
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); ok {
		t.Fatal("friendship must not exist when accept did not complete")
	}
}

func TestListFriends_FillsOnlineStatus(t *testing.T) {
	repo := newFakeRepo()
	repo.addFriendEdge(100, 200)
	repo.addFriendEdge(100, 300)
	online := &fakeOnline{status: map[uint64]data.OnlineStatus{
		200: {Online: true, LastSeenMs: 5000},
		// 300 不在 map → 离线
	}}
	uc := newUC(repo, &fakePusher{}, online)

	infos, err := uc.ListFriends(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListFriends err: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("want 2 friends, got %d", len(infos))
	}
	byID := map[uint64]*friendv1.FriendInfo{}
	for _, fi := range infos {
		byID[fi.GetPlayerId()] = fi
	}
	if !byID[200].GetIsOnline() || byID[200].GetLastSeenMs() != 5000 {
		t.Fatalf("friend 200 online status wrong: %+v", byID[200])
	}
	if byID[300].GetIsOnline() {
		t.Fatalf("friend 300 should be offline: %+v", byID[300])
	}
}

func TestListFriends_NilOnlineReader(t *testing.T) {
	repo := newFakeRepo()
	repo.addFriendEdge(100, 200)
	uc := newUC(repo, &fakePusher{}, nil) // online reader 弱依赖缺失
	infos, err := uc.ListFriends(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListFriends err: %v", err)
	}
	if len(infos) != 1 || infos[0].GetIsOnline() {
		t.Fatalf("want 1 offline friend, got %+v", infos)
	}
}

func TestBlock_RemovesFriendshipAndCancelsRequest(t *testing.T) {
	repo := newFakeRepo()
	repo.addFriendEdge(100, 200)
	repo.addFriendEdge(200, 100)
	uc := newUC(repo, &fakePusher{}, nil)

	if err := uc.Block(context.Background(), 100, 200); err != nil {
		t.Fatalf("Block err: %v", err)
	}
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); ok {
		t.Fatal("friendship 100-200 should be removed")
	}
	if ok, _ := repo.AreFriends(context.Background(), 200, 100); ok {
		t.Fatal("friendship 200-100 should be removed")
	}
	if blocked, _ := repo.IsBlocked(context.Background(), 100, 200); !blocked {
		t.Fatal("100 should have blocked 200")
	}
	// 拉黑后不能再加好友
	_, err := uc.AddFriend(context.Background(), 100, 200, 1)
	if errcode.As(err) != errcode.ErrFriendBlocked {
		t.Fatalf("want ErrFriendBlocked after block, got %v", err)
	}
}

func TestBlock_Self(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	err := uc.Block(context.Background(), 100, 100)
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("want ErrInvalidArg, got %v", err)
	}
}

// ── RejectFriend ──────────────────────────────────────────────────────────────

func TestRejectFriend_OK_NoPush(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)
	reqID, _ := uc.AddFriend(context.Background(), 100, 200, 999)
	pusher.events = nil // 清掉 REQUEST_RECEIVED

	if err := uc.RejectFriend(context.Background(), 200, reqID); err != nil {
		t.Fatalf("RejectFriend err: %v", err)
	}
	// 不建好友边、不推送(避免"被拒绝"尴尬)
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); ok {
		t.Fatal("reject must not create friendship")
	}
	if len(pusher.events) != 0 {
		t.Fatalf("reject must not push, got %d", len(pusher.events))
	}
	// 请求已不再 pending
	if reqs, _ := uc.ListFriendRequests(context.Background(), 200); len(reqs) != 0 {
		t.Fatalf("rejected request should not be pending, got %d", len(reqs))
	}
}

// 申请→拒绝→再次申请必须携带**新 request_id**(R4 复审 P1-4):推送 at-least-once,
// 客户端按 (request_id, reason) 判重,复用旧 ID 会让合法的再次申请推送被当重投丢弃。
func TestRejectThenReapply_PushesNewRequestID(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)

	firstID, err := uc.AddFriend(context.Background(), 100, 200, 111)
	if err != nil {
		t.Fatalf("first AddFriend err: %v", err)
	}
	if err := uc.RejectFriend(context.Background(), 200, firstID); err != nil {
		t.Fatalf("RejectFriend err: %v", err)
	}
	pusher.events = nil

	secondID, err := uc.AddFriend(context.Background(), 100, 200, 222)
	if err != nil {
		t.Fatalf("re-apply AddFriend err: %v", err)
	}
	if secondID == firstID {
		t.Fatalf("re-apply must rotate request_id, still %d", secondID)
	}
	if len(pusher.events) != 1 || pusher.events[0].GetRequestId() != secondID {
		t.Fatalf("re-apply push must carry the new request_id %d, got %+v", secondID, pusher.events)
	}
	// 旧 ID 已失效:迟到的 Accept 不得命中新申请。
	if err := uc.AcceptFriend(context.Background(), 200, firstID); errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("stale request_id accept must be ErrFriendNotFound, got %v", err)
	}
	// 新 ID 正常可接受。
	if err := uc.AcceptFriend(context.Background(), 200, secondID); err != nil {
		t.Fatalf("accept new request err: %v", err)
	}
}

func TestRejectFriend_NotTarget(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	reqID, _ := uc.AddFriend(context.Background(), 100, 200, 999)
	// 第三者 300 试图拒绝 → 找不到
	if err := uc.RejectFriend(context.Background(), 300, reqID); errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("want ErrFriendNotFound, got %v", err)
	}
}

func TestRejectFriend_NoRequest(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	if err := uc.RejectFriend(context.Background(), 200, 555); errcode.As(err) != errcode.ErrFriendNotFound {
		t.Fatalf("want ErrFriendNotFound, got %v", err)
	}
}

// ── ListFriendRequests ────────────────────────────────────────────────────────

func TestListFriendRequests_OnlyPendingIncoming(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	// 两个人给 200 发请求
	r1, _ := uc.AddFriend(context.Background(), 100, 200, 111)
	_, _ = uc.AddFriend(context.Background(), 300, 200, 222)
	// 200 也给别人发了请求(出站,不应出现在自己的待处理里)
	_, _ = uc.AddFriend(context.Background(), 200, 400, 333)

	reqs, err := uc.ListFriendRequests(context.Background(), 200)
	if err != nil {
		t.Fatalf("ListFriendRequests err: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("want 2 incoming pending, got %d", len(reqs))
	}
	// 接受其中一个后,只剩 1 条 pending
	if err := uc.AcceptFriend(context.Background(), 200, r1); err != nil {
		t.Fatalf("AcceptFriend err: %v", err)
	}
	reqs, _ = uc.ListFriendRequests(context.Background(), 200)
	if len(reqs) != 1 {
		t.Fatalf("want 1 pending after accept, got %d", len(reqs))
	}
	if reqs[0].GetFromPlayerId() != 300 {
		t.Fatalf("want remaining from 300, got %d", reqs[0].GetFromPlayerId())
	}
}

// ── RemoveFriend ──────────────────────────────────────────────────────────────

func TestRemoveFriend_OK(t *testing.T) {
	repo := newFakeRepo()
	repo.addFriendEdge(100, 200)
	repo.addFriendEdge(200, 100)
	uc := newUC(repo, &fakePusher{}, nil)

	if err := uc.RemoveFriend(context.Background(), 100, 200); err != nil {
		t.Fatalf("RemoveFriend err: %v", err)
	}
	if ok, _ := repo.AreFriends(context.Background(), 100, 200); ok {
		t.Fatal("100-200 edge should be removed")
	}
	if ok, _ := repo.AreFriends(context.Background(), 200, 100); ok {
		t.Fatal("200-100 edge should be removed")
	}
	// 删好友不写黑名单,可重新加
	if _, err := uc.AddFriend(context.Background(), 100, 200, 1); err != nil {
		t.Fatalf("should be able to re-add after remove: %v", err)
	}
}

func TestRemoveFriend_Idempotent(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	// 不是好友也不报错(幂等)
	if err := uc.RemoveFriend(context.Background(), 100, 200); err != nil {
		t.Fatalf("remove non-friend should be idempotent, got %v", err)
	}
}

func TestRemoveFriend_Self(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	if err := uc.RemoveFriend(context.Background(), 100, 100); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("want ErrInvalidArg, got %v", err)
	}
}

// ── Unblock / ListBlocks ──────────────────────────────────────────────────────

func TestUnblock_OK_AllowsReAdd(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	if err := uc.Block(context.Background(), 100, 200); err != nil {
		t.Fatalf("Block err: %v", err)
	}
	if err := uc.Unblock(context.Background(), 100, 200); err != nil {
		t.Fatalf("Unblock err: %v", err)
	}
	if blocked, _ := repo.IsBlocked(context.Background(), 100, 200); blocked {
		t.Fatal("100 should no longer block 200")
	}
	// 解黑后可重新加好友
	if _, err := uc.AddFriend(context.Background(), 100, 200, 1); err != nil {
		t.Fatalf("should be able to add after unblock: %v", err)
	}
}

func TestUnblock_Idempotent(t *testing.T) {
	uc := newUC(newFakeRepo(), &fakePusher{}, nil)
	if err := uc.Unblock(context.Background(), 100, 200); err != nil {
		t.Fatalf("unblock non-blocked should be idempotent, got %v", err)
	}
}

func TestListBlocks_OK(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo, &fakePusher{}, nil)
	_ = uc.Block(context.Background(), 100, 200)
	_ = uc.Block(context.Background(), 100, 300)

	blocks, err := uc.ListBlocks(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListBlocks err: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("want 2 blocks, got %d", len(blocks))
	}
	ids := map[uint64]bool{}
	for _, b := range blocks {
		ids[b.GetPlayerId()] = true
	}
	if !ids[200] || !ids[300] {
		t.Fatalf("want blocks 200,300; got %+v", ids)
	}
	// 解黑 200 后只剩 1
	_ = uc.Unblock(context.Background(), 100, 200)
	blocks, _ = uc.ListBlocks(context.Background(), 100)
	if len(blocks) != 1 || blocks[0].GetPlayerId() != 300 {
		t.Fatalf("want only 300 left, got %+v", blocks)
	}
}

// ── RecommendFriends ─────────────────────────────────────────────────────────

func TestRecommendFriends_DefaultStrategyChain_MutualThenRandomNoDup(t *testing.T) {
	repo := newFakeRepo()
	repo.recommendMutual = []data.RecommendRow{
		{CandidateID: 201, Mutual: 2},
		{CandidateID: 202, Mutual: 1},
	}
	repo.recommendRandom = []data.RecommendRow{
		{CandidateID: 202}, // 已被 mutual 选中,应被后续 exclude 掉
		{CandidateID: 203},
		{CandidateID: 204}, // 客户端 exclude,不应出现
	}
	online := &fakeOnline{status: map[uint64]data.OnlineStatus{
		203: {Online: true, LastSeenMs: 7000},
	}}
	uc := NewFriendUsecase(repo, nil, online, conf.FriendConf{RecommendLimit: 3})

	recs, err := uc.RecommendFriends(context.Background(), 100, 0, []uint64{204})
	if err != nil {
		t.Fatalf("RecommendFriends err: %v", err)
	}
	if repo.mutualCalls != 1 || repo.randomCalls != 1 {
		t.Fatalf("want mutual+random once, got mutual=%d random=%d", repo.mutualCalls, repo.randomCalls)
	}
	if len(recs) != 3 {
		t.Fatalf("want 3 recs, got %d: %+v", len(recs), recs)
	}
	wantIDs := []uint64{201, 202, 203}
	for i, want := range wantIDs {
		if recs[i].GetPlayerId() != want {
			t.Fatalf("rec[%d] player_id=%d, want %d", i, recs[i].GetPlayerId(), want)
		}
	}
	if recs[0].GetMutualFriendCount() != 2 || recs[1].GetMutualFriendCount() != 1 {
		t.Fatalf("mutual counts wrong: %+v", recs)
	}
	if !recs[2].GetIsOnline() || recs[2].GetLastSeenMs() != 7000 {
		t.Fatalf("online status for random fallback wrong: %+v", recs[2])
	}
}

func TestRecommendFriends_ConfigAllowsRandomOnly(t *testing.T) {
	repo := newFakeRepo()
	repo.recommendMutual = []data.RecommendRow{{CandidateID: 201, Mutual: 2}}
	repo.recommendRandom = []data.RecommendRow{{CandidateID: 301}, {CandidateID: 302}}
	uc := NewFriendUsecase(repo, nil, nil, conf.FriendConf{
		RecommendLimit:      2,
		RecommendStrategies: []string{"random"},
	})

	recs, err := uc.RecommendFriends(context.Background(), 100, 0, nil)
	if err != nil {
		t.Fatalf("RecommendFriends err: %v", err)
	}
	if repo.mutualCalls != 0 || repo.randomCalls != 1 {
		t.Fatalf("want random only, got mutual=%d random=%d", repo.mutualCalls, repo.randomCalls)
	}
	if len(recs) != 2 || recs[0].GetPlayerId() != 301 || recs[1].GetPlayerId() != 302 {
		t.Fatalf("random-only recs wrong: %+v", recs)
	}
}

func TestBuildStrategies_AllUnknownFallsBackToDefault(t *testing.T) {
	repo := newFakeRepo()
	strategies := buildStrategies(repo, []string{"same_region", "similar_power"})
	if len(strategies) != 2 {
		t.Fatalf("want fallback 2 strategies, got %d", len(strategies))
	}
	if strategies[0].Name() != "mutual" || strategies[1].Name() != "random" {
		t.Fatalf("fallback order wrong: %s, %s", strategies[0].Name(), strategies[1].Name())
	}
}
