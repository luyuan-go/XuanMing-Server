// guild_test.go — GuildUsecase 业务逻辑单测(2026-06-27)。
//
// 用内存版 fakeGuildRepo 复刻 MySQL 语义(单归属 + 职位 + 申请),无需真 DB。
// 覆盖:建会 / 申请 / 审批 / 退会(会长禁退)/ 踢人权限(leader / officer / member)/
// 解散 / 转让 / 任命官员 / 查询 / 推送 fan-out。
package biz

import (
	"context"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"sort"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
	"github.com/luyuancpp/pandora/services/social/guild/internal/conf"
	"github.com/luyuancpp/pandora/services/social/guild/internal/data"
)

// ── 内存 fakeGuildRepo ──────────────────────────────────────────────────────────

type fakeGuildRepo struct {
	guilds   map[uint64]*data.GuildRow
	members  map[uint64]*data.GuildMemberRow // key = player_id(单归属)
	requests map[uint64]*data.GuildJoinRequestRow
	names    map[string]struct{}

	disbandErr error // 非 nil 时 DisbandGuild 事务失败回滚(建模快照 / 删除事务失败:什么都不删)
}

func newFakeGuildRepo() *fakeGuildRepo {
	return &fakeGuildRepo{
		guilds:   map[uint64]*data.GuildRow{},
		members:  map[uint64]*data.GuildMemberRow{},
		requests: map[uint64]*data.GuildJoinRequestRow{},
		names:    map[string]struct{}{},
	}
}

func (f *fakeGuildRepo) CreateGuild(_ context.Context, newGuildID, leaderID uint64, name string, _ int) error {
	if _, ok := f.members[leaderID]; ok {
		return errcode.New(errcode.ErrGuildAlreadyInGuild, "already in guild")
	}
	if _, dup := f.names[name]; dup {
		return errcode.New(errcode.ErrGuildNameTaken, "name taken")
	}
	f.guilds[newGuildID] = &data.GuildRow{GuildID: newGuildID, Name: name, LeaderID: leaderID, MemberCount: 1, MaxMembers: 100}
	f.members[leaderID] = &data.GuildMemberRow{PlayerID: leaderID, GuildID: newGuildID, Role: data.GuildRoleLeader}
	f.names[name] = struct{}{}
	return nil
}

func (f *fakeGuildRepo) GetGuild(_ context.Context, guildID uint64) (*data.GuildRow, bool, error) {
	g, ok := f.guilds[guildID]
	return g, ok, nil
}

func (f *fakeGuildRepo) GetMyGuild(_ context.Context, playerID uint64) (*data.GuildRow, bool, error) {
	m, ok := f.members[playerID]
	if !ok {
		return nil, false, nil
	}
	return f.guilds[m.GuildID], true, nil
}

func (f *fakeGuildRepo) GetMember(_ context.Context, playerID uint64) (*data.GuildMemberRow, bool, error) {
	m, ok := f.members[playerID]
	return m, ok, nil
}

func (f *fakeGuildRepo) ListMembers(_ context.Context, guildID, cursor uint64, limit int) ([]data.GuildMemberRow, error) {
	var out []data.GuildMemberRow
	for _, m := range f.members {
		if m.GuildID == guildID && (cursor == 0 || m.PlayerID > cursor) {
			out = append(out, *m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PlayerID < out[j].PlayerID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeGuildRepo) CreateJoinRequest(_ context.Context, newRequestID, guildID, playerID uint64, maxPending int) (uint64, bool, error) {
	pending := 0
	for _, rq := range f.requests {
		if rq.GuildID == guildID && rq.PlayerID == playerID && rq.Status == 1 {
			return rq.RequestID, true, nil
		}
		if rq.GuildID == guildID && rq.Status == 1 {
			pending++
		}
	}
	if maxPending > 0 && pending >= maxPending {
		return 0, false, errcode.New(errcode.ErrGuildRequestLimit, "pending limit")
	}
	f.requests[newRequestID] = &data.GuildJoinRequestRow{RequestID: newRequestID, GuildID: guildID, PlayerID: playerID, Status: 1}
	return newRequestID, false, nil
}

func (f *fakeGuildRepo) GetRequest(_ context.Context, requestID uint64) (*data.GuildJoinRequestRow, bool, error) {
	rq, ok := f.requests[requestID]
	return rq, ok, nil
}

func (f *fakeGuildRepo) ApproveJoin(_ context.Context, requestID, approverID uint64, maxMembers int) (bool, error) {
	rq, ok := f.requests[requestID]
	if !ok || rq.Status != 1 {
		return false, nil
	}
	ap, ok := f.members[approverID]
	if !ok || ap.GuildID != rq.GuildID || (ap.Role != data.GuildRoleLeader && ap.Role != data.GuildRoleOfficer) {
		return false, errcode.New(errcode.ErrGuildNoPermission, "no perm")
	}
	if _, in := f.members[rq.PlayerID]; in {
		return false, errcode.New(errcode.ErrGuildAlreadyInGuild, "applicant already in guild")
	}
	g := f.guilds[rq.GuildID]
	if int(g.MemberCount) >= maxMembers {
		return false, errcode.New(errcode.ErrGuildFull, "full")
	}
	f.members[rq.PlayerID] = &data.GuildMemberRow{PlayerID: rq.PlayerID, GuildID: rq.GuildID, Role: data.GuildRoleMember}
	g.MemberCount++
	rq.Status = 2
	return true, nil
}

func (f *fakeGuildRepo) RejectJoin(_ context.Context, requestID, approverID uint64) (bool, error) {
	rq, ok := f.requests[requestID]
	if !ok || rq.Status != 1 {
		return false, nil
	}
	ap, ok := f.members[approverID]
	if !ok || ap.GuildID != rq.GuildID || (ap.Role != data.GuildRoleLeader && ap.Role != data.GuildRoleOfficer) {
		return false, errcode.New(errcode.ErrGuildNoPermission, "no perm")
	}
	rq.Status = 3
	return true, nil
}

func (f *fakeGuildRepo) RemoveMember(_ context.Context, guildID, playerID uint64) error {
	if m, ok := f.members[playerID]; ok && m.GuildID == guildID {
		if m.Role == data.GuildRoleLeader {
			return errcode.New(errcode.ErrGuildNotLeader, "leader must transfer or disband")
		}
		delete(f.members, playerID)
		if g := f.guilds[guildID]; g != nil {
			g.MemberCount--
		}
	}
	return nil
}

func (f *fakeGuildRepo) KickMember(_ context.Context, guildID, operatorID, targetID uint64) error {
	g, ok := f.guilds[guildID]
	if !ok {
		return errcode.New(errcode.ErrGuildNotFound, "not found")
	}
	op, ok := f.members[operatorID]
	if !ok || op.GuildID != guildID {
		return errcode.New(errcode.ErrGuildNoPermission, "operator not in guild")
	}
	target, ok := f.members[targetID]
	if !ok || target.GuildID != guildID {
		return errcode.New(errcode.ErrGuildNotMember, "target not in guild")
	}
	if target.Role == data.GuildRoleLeader {
		return errcode.New(errcode.ErrGuildNoPermission, "cannot kick the leader")
	}
	switch op.Role {
	case data.GuildRoleLeader:
	case data.GuildRoleOfficer:
		if target.Role != data.GuildRoleMember {
			return errcode.New(errcode.ErrGuildNoPermission, "officer can only kick members")
		}
	default:
		return errcode.New(errcode.ErrGuildNoPermission, "member cannot kick")
	}
	delete(f.members, targetID)
	g.MemberCount--
	return nil
}

func (f *fakeGuildRepo) DisbandGuild(_ context.Context, guildID, operatorID uint64) ([]uint64, error) {
	g, ok := f.guilds[guildID]
	if !ok {
		return nil, errcode.New(errcode.ErrGuildNotFound, "not found")
	}
	if g.LeaderID != operatorID {
		return nil, errcode.New(errcode.ErrGuildNotLeader, "not current leader")
	}
	if f.disbandErr != nil {
		// 事务失败 → 回滚:什么都不删,不返回成员集合。
		return nil, f.disbandErr
	}
	var deleted []uint64
	for pid, m := range f.members {
		if m.GuildID == guildID {
			deleted = append(deleted, pid)
			delete(f.members, pid)
		}
	}
	for rid, rq := range f.requests {
		if rq.GuildID == guildID {
			delete(f.requests, rid)
		}
	}
	delete(f.names, g.Name)
	delete(f.guilds, guildID)
	return deleted, nil
}

func (f *fakeGuildRepo) SetRole(_ context.Context, guildID, operatorID, targetID uint64, role int32) error {
	g, ok := f.guilds[guildID]
	if !ok {
		return errcode.New(errcode.ErrGuildNotFound, "not found")
	}
	if g.LeaderID != operatorID {
		return errcode.New(errcode.ErrGuildNotLeader, "not current leader")
	}
	if targetID == g.LeaderID {
		return errcode.New(errcode.ErrGuildNoPermission, "cannot change role of current leader")
	}
	m, ok := f.members[targetID]
	if !ok || m.GuildID != guildID {
		return errcode.New(errcode.ErrGuildNotMember, "target not in guild")
	}
	m.Role = role
	return nil
}

func (f *fakeGuildRepo) TransferLeader(_ context.Context, guildID, oldLeaderID, newLeaderID uint64) error {
	if m, ok := f.members[oldLeaderID]; ok {
		m.Role = data.GuildRoleMember
	}
	if m, ok := f.members[newLeaderID]; ok {
		m.Role = data.GuildRoleLeader
	}
	if g := f.guilds[guildID]; g != nil {
		g.LeaderID = newLeaderID
	}
	return nil
}

func (f *fakeGuildRepo) ListPendingRequests(_ context.Context, guildID, cursor uint64, limit int) ([]data.GuildJoinRequestRow, error) {
	var out []data.GuildJoinRequestRow
	for _, rq := range f.requests {
		if rq.GuildID == guildID && rq.Status == 1 && (cursor == 0 || rq.RequestID > cursor) {
			out = append(out, *rq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// 保留期清理(§9.24):biz 单测不模拟时间,默认 no-op。
func (f *fakeGuildRepo) SweepTerminalJoinRequestsBefore(_ context.Context, mode dbguard.Mode, _, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

// ── fakeGuildPusher ─────────────────────────────────────────────────────────────

type guildPushRecord struct {
	to  uint64
	evt *guildv1.GuildEvent
}

type fakeGuildPusher struct {
	pushes []guildPushRecord
}

func (f *fakeGuildPusher) PushGuildEvent(_ context.Context, toPlayerID uint64, evt *guildv1.GuildEvent) error {
	f.pushes = append(f.pushes, guildPushRecord{to: toPlayerID, evt: evt})
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newGuildUC(repo data.GuildRepo, pusher GuildEventPusher) *GuildUsecase {
	return NewGuildUsecase(repo, nil, pusher, conf.GuildConf{MaxGuildMembers: 100, MaxGroupMembers: 50, MaxNameLen: 24})
}

func wantGuildCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	if errcode.As(err) != code {
		t.Fatalf("want code %d, got err=%v (code=%d)", code, err, errcode.As(err))
	}
}

// ── 测试 ──────────────────────────────────────────────────────────────────────

func TestCreateGuild_OK(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	id, err := uc.CreateGuild(context.Background(), 1, "Knights", 1001)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 1001 {
		t.Fatalf("want 1001, got %d", id)
	}
	if m, _, _ := repo.GetMember(context.Background(), 1); m == nil || m.Role != data.GuildRoleLeader {
		t.Fatalf("creator must be leader")
	}
}

func TestCreateGuild_EmptyName(t *testing.T) {
	uc := newGuildUC(newFakeGuildRepo(), nil)
	_, err := uc.CreateGuild(context.Background(), 1, "   ", 1001)
	wantGuildCode(t, err, errcode.ErrInvalidArg)
}

func TestCreateGuild_AlreadyInGuild(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "A", 1001)
	_, err := uc.CreateGuild(context.Background(), 1, "B", 1002)
	wantGuildCode(t, err, errcode.ErrGuildAlreadyInGuild)
}

func TestCreateGuild_NameTaken(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "Dup", 1001)
	_, err := uc.CreateGuild(context.Background(), 2, "Dup", 1002)
	wantGuildCode(t, err, errcode.ErrGuildNameTaken)
}

func TestApplyAndApprove_OK(t *testing.T) {
	repo := newFakeGuildRepo()
	pusher := &fakeGuildPusher{}
	uc := newGuildUC(repo, pusher)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	rid, err := uc.ApplyJoin(context.Background(), 2, 1001, 2001)
	if err != nil {
		t.Fatalf("apply err: %v", err)
	}
	// 申请通知发给会长 1(原则 2:不发申请人)。
	if len(pusher.pushes) != 1 || pusher.pushes[0].to != 1 {
		t.Fatalf("want 1 push to leader, got %+v", pusher.pushes)
	}

	if err := uc.ApproveJoin(context.Background(), 1, rid); err != nil {
		t.Fatalf("approve err: %v", err)
	}
	if m, _, _ := repo.GetMember(context.Background(), 2); m == nil || m.GuildID != 1001 {
		t.Fatalf("applicant should be member now")
	}
}

func TestApplyJoin_AlreadyInGuild(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	_, err := uc.ApplyJoin(context.Background(), 1, 1001, 2001)
	wantGuildCode(t, err, errcode.ErrGuildAlreadyInGuild)
}

func TestApplyJoin_GuildNotFound(t *testing.T) {
	uc := newGuildUC(newFakeGuildRepo(), nil)
	_, err := uc.ApplyJoin(context.Background(), 2, 9999, 2001)
	wantGuildCode(t, err, errcode.ErrGuildNotFound)
}

func TestLeaveGuild_LeaderForbidden(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	err := uc.LeaveGuild(context.Background(), 1)
	wantGuildCode(t, err, errcode.ErrGuildNotLeader)
}

func TestLeaveGuild_MemberOK(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	rid, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2001)
	_ = uc.ApproveJoin(context.Background(), 1, rid)
	if err := uc.LeaveGuild(context.Background(), 2); err != nil {
		t.Fatalf("member leave err: %v", err)
	}
	if m, _, _ := repo.GetMember(context.Background(), 2); m != nil {
		t.Fatalf("member should be gone")
	}
}

func TestKickMember_OfficerCannotKickOfficer(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001) // 1 = leader
	// 2 / 3 入会
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	r3, _ := uc.ApplyJoin(context.Background(), 3, 1001, 2003)
	_ = uc.ApproveJoin(context.Background(), 1, r3)
	// 2 / 3 都升 officer
	_ = uc.SetOfficer(context.Background(), 1, 2, true)
	_ = uc.SetOfficer(context.Background(), 1, 3, true)
	// officer 2 踢 officer 3 → 无权
	err := uc.KickMember(context.Background(), 2, 3)
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
}

func TestKickMember_LeaderKicksOfficer(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	_ = uc.SetOfficer(context.Background(), 1, 2, true)
	if err := uc.KickMember(context.Background(), 1, 2); err != nil {
		t.Fatalf("leader kick officer err: %v", err)
	}
	if m, _, _ := repo.GetMember(context.Background(), 2); m != nil {
		t.Fatalf("officer should be kicked")
	}
}

func TestKickMember_CannotKickLeader(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	_ = uc.SetOfficer(context.Background(), 1, 2, true)
	err := uc.KickMember(context.Background(), 2, 1) // officer 踢 leader
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
}

func TestDisbandGuild_NotifiesAll(t *testing.T) {
	repo := newFakeGuildRepo()
	pusher := &fakeGuildPusher{}
	uc := newGuildUC(repo, pusher)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	pusher.pushes = nil // 清掉申请通知

	if err := uc.DisbandGuild(context.Background(), 1); err != nil {
		t.Fatalf("disband err: %v", err)
	}
	if len(pusher.pushes) != 2 {
		t.Fatalf("want 2 disband notifies (all members), got %d", len(pusher.pushes))
	}
	if _, ok := repo.guilds[1001]; ok {
		t.Fatalf("guild should be deleted")
	}
}

func TestDisbandGuild_NotLeader(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	err := uc.DisbandGuild(context.Background(), 2)
	wantGuildCode(t, err, errcode.ErrGuildNotLeader)
}

func TestTransferLeader_OK(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	if err := uc.TransferLeader(context.Background(), 1, 2); err != nil {
		t.Fatalf("transfer err: %v", err)
	}
	if m, _, _ := repo.GetMember(context.Background(), 2); m == nil || m.Role != data.GuildRoleLeader {
		t.Fatalf("2 should be leader")
	}
	if m, _, _ := repo.GetMember(context.Background(), 1); m == nil || m.Role != data.GuildRoleMember {
		t.Fatalf("1 should be demoted to member")
	}
}

func TestListJoinRequests_MemberNoPerm(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	_, _, err := uc.ListJoinRequests(context.Background(), 2, 0, 0) // 普通成员
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
}

func TestGetMyGuild_NotInGuild(t *testing.T) {
	uc := newGuildUC(newFakeGuildRepo(), nil)
	g, err := uc.GetMyGuild(context.Background(), 42)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if g != nil {
		t.Fatalf("want nil guild for non-member")
	}
}

// ── TOCTOU 权威复核(三审 P1-9):repo 层持父行锁再复核操作者,拒绝转让后的旧会长写 ──

// 转让后旧会长(已降 member)再解散公会,repo 复核 leader_id != 旧会长 → 拒绝。
func TestDisbandGuild_StaleLeaderRejected(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	_ = uc.TransferLeader(context.Background(), 1, 2) // 会长 1 → 2
	// 旧会长 1 直接调 repo 解散(模拟其请求晚到,已非会长):必须被拒。
	_, err := repo.DisbandGuild(context.Background(), 1001, 1)
	wantGuildCode(t, err, errcode.ErrGuildNotLeader)
	if _, ok, _ := repo.GetGuild(context.Background(), 1001); !ok {
		t.Fatalf("guild must survive stale-leader disband")
	}
}

// 转让后旧会长再 SetRole 把新会长降级,repo 复核 leader_id != 旧会长 → 拒绝;
// 且即便是现任会长也不能通过 SetRole 改现任会长职位(保 leader_id 与角色一致)。
func TestSetRole_StaleLeaderAndLeaderTargetRejected(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	_ = uc.TransferLeader(context.Background(), 1, 2) // 会长 1 → 2

	// 旧会长 1 试图把新会长 2 降成 member:拒。
	err := repo.SetRole(context.Background(), 1001, 1, 2, data.GuildRoleMember)
	wantGuildCode(t, err, errcode.ErrGuildNotLeader)
	if m, _, _ := repo.GetMember(context.Background(), 2); m == nil || m.Role != data.GuildRoleLeader {
		t.Fatalf("new leader 2 must stay leader")
	}
	// 现任会长 2 也不能通过 SetRole 改自己(现任会长)的职位。
	err = repo.SetRole(context.Background(), 1001, 2, 2, data.GuildRoleMember)
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
}

// 转让后旧会长(已降 member)再踢人,repo 复核操作者职位 → member 不能踢人。
func TestKickMember_StaleLeaderRejected(t *testing.T) {
	repo := newFakeGuildRepo()
	uc := newGuildUC(repo, nil)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	_ = uc.ApproveJoin(context.Background(), 1, r2)
	r3, _ := uc.ApplyJoin(context.Background(), 3, 1001, 2003)
	_ = uc.ApproveJoin(context.Background(), 1, r3)
	_ = uc.TransferLeader(context.Background(), 1, 2) // 会长 1 → 2,1 变 member

	// 旧会长 1(现为 member)踢成员 3:repo 复核操作者职位 → 拒。
	err := repo.KickMember(context.Background(), 1001, 1, 3)
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
	if m, _, _ := repo.GetMember(context.Background(), 3); m == nil {
		t.Fatalf("member 3 must survive stale-leader kick")
	}
	// 不能踢现任会长。
	err = repo.KickMember(context.Background(), 1001, 2, 2)
	wantGuildCode(t, err, errcode.ErrGuildNoPermission)
}

// ── 读缓存编排(cache-aside)测试 ────────────────────────────────────────────────

// fakeGuildCache 是内存版 data.GuildCache,记录各操作调用,可注入读故障。
type fakeGuildCache struct {
	info    map[uint64]*data.GuildRow // guild_id → 资料快照
	member  map[uint64]uint64         // player_id → guild_id 反查
	getErr  error                     // 非 nil 时 Get* 返回该错误(模拟 Redis 读故障)
	setInfo int
	setMem  int
	delInfo []uint64
	delMem  []uint64
}

func newFakeGuildCache() *fakeGuildCache {
	return &fakeGuildCache{info: map[uint64]*data.GuildRow{}, member: map[uint64]uint64{}}
}

func (c *fakeGuildCache) GetGuild(_ context.Context, guildID uint64) (*data.GuildRow, bool, error) {
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	g, ok := c.info[guildID]
	return g, ok, nil
}

func (c *fakeGuildCache) SetGuild(_ context.Context, g *data.GuildRow, _ time.Duration) error {
	c.setInfo++
	cp := *g
	c.info[g.GuildID] = &cp
	return nil
}

func (c *fakeGuildCache) DelGuild(_ context.Context, guildID uint64) error {
	c.delInfo = append(c.delInfo, guildID)
	delete(c.info, guildID)
	return nil
}

func (c *fakeGuildCache) GetMemberGuildID(_ context.Context, playerID uint64) (uint64, bool, error) {
	if c.getErr != nil {
		return 0, false, c.getErr
	}
	g, ok := c.member[playerID]
	return g, ok, nil
}

func (c *fakeGuildCache) SetMemberGuildID(_ context.Context, playerID, guildID uint64, _ time.Duration) error {
	if guildID == 0 {
		return nil
	}
	c.setMem++
	c.member[playerID] = guildID
	return nil
}

func (c *fakeGuildCache) DelMember(_ context.Context, playerID uint64) error {
	c.delMem = append(c.delMem, playerID)
	delete(c.member, playerID)
	return nil
}

// resetDeletes 清空失效记录,便于逐阶段核对「本阶段新增」的失效调用(防跨阶段累计伪阳性)。
func (c *fakeGuildCache) resetDeletes() {
	c.delInfo = nil
	c.delMem = nil
}

func contains(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func newGuildUCWithCache(repo data.GuildRepo, cache data.GuildCache) *GuildUsecase {
	return NewGuildUsecase(repo, cache, nil, conf.GuildConf{MaxGuildMembers: 100, MaxGroupMembers: 50, MaxNameLen: 24})
}

// 阻断点回归:CreateGuild 成功后必须失效创建者 member 反查缓存,
// 否则旧公会的陈旧反查会让 GetMyGuild 在 TTL 内继续返回旧公会。
func TestCreateGuild_InvalidatesStaleMemberCache(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	// 预置陈旧反查:玩家 1 曾属旧公会 999(删除失败 / 并发迟到回填残留)。
	cache.member[1] = 999
	uc := newGuildUCWithCache(repo, cache)

	if _, err := uc.CreateGuild(context.Background(), 1, "Knights", 1001); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !contains(cache.delMem, 1) {
		t.Fatalf("CreateGuild must invalidate creator member cache; delMem=%v", cache.delMem)
	}
	// GetMyGuild 应回落 MySQL 拿到新公会 1001,不再是陈旧 999。
	g, err := uc.GetMyGuild(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if g == nil || g.GetGuildId() != 1001 {
		t.Fatalf("want new guild 1001, got %v", g)
	}
}

// GetGuild:miss 回落 MySQL 并回填;二次读命中缓存(不再打 repo)。
func TestGetGuild_CacheAsideFillAndHit(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	// 首读:缓存 miss → 回填。
	if _, err := uc.GetGuild(context.Background(), 1001); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cache.setInfo == 0 {
		t.Fatalf("first GetGuild must fill info cache")
	}
	// 篡改 repo 里的名字,若二次读命中缓存则应返回旧名(证明走了缓存)。
	repo.guilds[1001].Name = "CHANGED"
	g, err := uc.GetGuild(context.Background(), 1001)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if g.GetName() != "G" {
		t.Fatalf("second GetGuild should hit cache (old name G), got %q", g.GetName())
	}
}

// GetMyGuild:member + info 缓存命中直接返回,不打 repo。
func TestGetMyGuild_CacheHit(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	// 首次 GetMyGuild 回填 member + info。
	if _, err := uc.GetMyGuild(context.Background(), 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if cache.setMem == 0 || cache.setInfo == 0 {
		t.Fatalf("first GetMyGuild must fill member+info cache")
	}
	// 篡改 repo,二次读若走缓存应返回旧名。
	repo.guilds[1001].Name = "CHANGED"
	g, err := uc.GetMyGuild(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if g == nil || g.GetName() != "G" {
		t.Fatalf("GetMyGuild should hit cache (old name G), got %v", g)
	}
}

// 写路径失效:审批入会 / 退会 / 踢人 / 解散后删对应缓存 key。
// 每阶段前清空失效记录,只核对「本阶段新增」的失效调用,避免跨阶段累计导致伪阳性
// (否则前一阶段留下的 delInfo=1001 / delMem=2 会让后续阶段即使漏删也误判通过)。
func TestWritePaths_InvalidateCache(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	// 审批入会:失效公会资料 + 申请人 member 反查。
	cache.resetDeletes()
	r2, _ := uc.ApplyJoin(context.Background(), 2, 1001, 2002)
	if err := uc.ApproveJoin(context.Background(), 1, r2); err != nil {
		t.Fatalf("approve err: %v", err)
	}
	if !contains(cache.delInfo, 1001) || !contains(cache.delMem, 2) {
		t.Fatalf("ApproveJoin must invalidate guild+member; delInfo=%v delMem=%v", cache.delInfo, cache.delMem)
	}

	// 退会:失效公会资料 + 退会者 member 反查。清空后核对本阶段新增。
	cache.resetDeletes()
	if err := uc.LeaveGuild(context.Background(), 2); err != nil {
		t.Fatalf("leave err: %v", err)
	}
	if !contains(cache.delInfo, 1001) || !contains(cache.delMem, 2) {
		t.Fatalf("LeaveGuild must invalidate guild+member 2; delInfo=%v delMem=%v", cache.delInfo, cache.delMem)
	}

	// 踢人:先入会(不计入断言),清空后核对踢人本身失效公会资料 + 被踢者 member 反查。
	r3, _ := uc.ApplyJoin(context.Background(), 3, 1001, 2003)
	_ = uc.ApproveJoin(context.Background(), 1, r3)
	cache.resetDeletes()
	if err := uc.KickMember(context.Background(), 1, 3); err != nil {
		t.Fatalf("kick err: %v", err)
	}
	if !contains(cache.delInfo, 1001) || !contains(cache.delMem, 3) {
		t.Fatalf("KickMember must invalidate guild+member 3; delInfo=%v delMem=%v", cache.delInfo, cache.delMem)
	}

	// 解散:先补一名普通成员(玩家 4),确保公会里不止会长——否则「只删会长、漏删普通成员」的
	// 退化无法被发现。清空后核对本阶段失效了公会资料 + 会长(1)与普通成员(4)两个 member key。
	r4, _ := uc.ApplyJoin(context.Background(), 4, 1001, 2004)
	_ = uc.ApproveJoin(context.Background(), 1, r4)
	cache.resetDeletes()
	if err := uc.DisbandGuild(context.Background(), 1); err != nil {
		t.Fatalf("disband err: %v", err)
	}
	if !contains(cache.delInfo, 1001) || !contains(cache.delMem, 1) || !contains(cache.delMem, 4) {
		t.Fatalf("DisbandGuild must invalidate guild + leader(1) + member(4); delInfo=%v delMem=%v", cache.delInfo, cache.delMem)
	}
}

// 解散事务失败(含持锁读成员快照失败)必须回滚:什么都不删,不能带残缺成员集合永久删公会。
func TestDisbandGuild_AbortsWhenTxFails(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	repo.disbandErr = errcode.New(errcode.ErrInternal, "tx down")
	cache.resetDeletes() // 清掉 CreateGuild 的失效记录,只核对解散阶段。
	err := uc.DisbandGuild(context.Background(), 1)
	if err == nil {
		t.Fatal("DisbandGuild must abort when tx fails, got nil err")
	}
	// 公会与成员必须仍在(未被删除)。
	if _, ok, _ := repo.GetGuild(context.Background(), 1001); !ok {
		t.Fatalf("guild 1001 must survive aborted disband")
	}
	if _, ok, _ := repo.GetMember(context.Background(), 1); !ok {
		t.Fatalf("leader 1 must survive aborted disband")
	}
	// 未删除 → 不得触发任何缓存失效。
	if len(cache.delInfo) != 0 || len(cache.delMem) != 0 {
		t.Fatalf("aborted disband must not invalidate cache; delInfo=%v delMem=%v", cache.delInfo, cache.delMem)
	}
}

// Redis 读故障:Get* 返回错误时降级直连 MySQL,仍返回正确结果。
func TestGetGuild_CacheReadErrorFallsBackToMySQL(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	cache.getErr = errcode.New(errcode.ErrInternal, "redis down")
	uc := newGuildUCWithCache(repo, cache)
	_, _ = uc.CreateGuild(context.Background(), 1, "G", 1001)

	g, err := uc.GetGuild(context.Background(), 1001)
	if err != nil {
		t.Fatalf("cache read error must degrade to MySQL, got err=%v", err)
	}
	if g == nil || g.GetName() != "G" {
		t.Fatalf("want guild G from MySQL fallback, got %v", g)
	}

	mg, err := uc.GetMyGuild(context.Background(), 1)
	if err != nil {
		t.Fatalf("cache read error must degrade to MySQL, got err=%v", err)
	}
	if mg == nil || mg.GetGuildId() != 1001 {
		t.Fatalf("want guild 1001 from MySQL fallback, got %v", mg)
	}
}

// GetMyGuild 权威判定不在公会时,自愈清掉残留 member 反查缓存。
func TestGetMyGuild_SelfHealsStaleMemberCache(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	// 玩家 5 实际不在任何公会,但缓存残留反查指向 777,且 777 的 info 也缺失。
	cache.member[5] = 777
	uc := newGuildUCWithCache(repo, cache)

	g, err := uc.GetMyGuild(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if g != nil {
		t.Fatalf("player 5 not in any guild, want nil, got %v", g)
	}
	if !contains(cache.delMem, 5) {
		t.Fatalf("GetMyGuild must self-heal stale member cache for player 5; delMem=%v", cache.delMem)
	}
}
