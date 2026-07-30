// chat_test.go — ChatUsecase 业务逻辑单测(2026-06-16)。
//
// 用内存版 fakeRepo / fakePusher / fakeTeam 复刻 MySQL + kafka + team 语义,无需真依赖。
// 覆盖:私聊 / 队伍 / 世界正常路径 + 频道非法 / 空内容 / 超长 / 私聊缺 target /
// 自聊 / 非队伍成员 / 敏感词屏蔽 / PullHistory 频道与游标。
package biz

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"
	"github.com/luyuancpp/pandora/services/social/chat/internal/conf"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

type fakeRepo struct {
	saved []*chatv1.ChatMessage
}

func (f *fakeRepo) SavePrivate(_ context.Context, msg *chatv1.ChatMessage) error {
	f.saved = append(f.saved, msg)
	return nil
}

// 保留期清理(§9.24):biz 单测不模拟时间,默认 no-op。
func (f *fakeRepo) SweepMessagesBefore(_ context.Context, mode dbguard.Mode, _ uint64, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) ListPrivate(_ context.Context, playerID, peerID uint64, limit int, beforeMs int64) ([]*chatv1.ChatMessage, error) {
	var out []*chatv1.ChatMessage
	for i := len(f.saved) - 1; i >= 0; i-- {
		m := f.saved[i]
		pair := (m.GetSenderId() == playerID && m.GetTargetId() == peerID) ||
			(m.GetSenderId() == peerID && m.GetTargetId() == playerID)
		if !pair {
			continue
		}
		if beforeMs > 0 && m.GetSendTimeMs() >= beforeMs {
			continue
		}
		out = append(out, m)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

type pushRecord struct {
	kind       string // private / team / world
	toPlayerID uint64
	evt        *chatv1.ChatPushEvent
}

type fakePusher struct {
	pushes []pushRecord
}

func (f *fakePusher) PushPrivate(_ context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	f.pushes = append(f.pushes, pushRecord{"private", toPlayerID, evt})
	return nil
}
func (f *fakePusher) PushTeam(_ context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	f.pushes = append(f.pushes, pushRecord{"team", toPlayerID, evt})
	return nil
}
func (f *fakePusher) PushWorld(_ context.Context, evt *chatv1.ChatPushEvent) error {
	f.pushes = append(f.pushes, pushRecord{"world", 0, evt})
	return nil
}
func (f *fakePusher) PushGuild(_ context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	f.pushes = append(f.pushes, pushRecord{"guild", toPlayerID, evt})
	return nil
}
func (f *fakePusher) PushGroup(_ context.Context, toPlayerID uint64, evt *chatv1.ChatPushEvent) error {
	f.pushes = append(f.pushes, pushRecord{"group", toPlayerID, evt})
	return nil
}

type fakeGuild struct {
	members map[uint64][]uint64
}

func (f *fakeGuild) GetGuildMembers(_ context.Context, guildID uint64) ([]uint64, bool, error) {
	m, ok := f.members[guildID]
	return m, ok, nil
}

type fakeGroup struct {
	members map[uint64][]uint64
}

func (f *fakeGroup) GetGroupMembers(_ context.Context, groupID uint64) ([]uint64, bool, error) {
	m, ok := f.members[groupID]
	return m, ok, nil
}

type fakeTeam struct {
	members map[uint64][]uint64
	err     error
}

func (f *fakeTeam) GetTeamMembers(_ context.Context, teamID uint64) ([]uint64, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	m, ok := f.members[teamID]
	return m, ok, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newUC(repo *fakeRepo, pusher *fakePusher, team *fakeTeam) *ChatUsecase {
	var p ChatPusher
	if pusher != nil {
		p = pusher
	}
	var t TeamReader
	if team != nil {
		t = team
	}
	return NewChatUsecase(repo, p, t, nil, nil, conf.ChatConf{MaxContentLen: 10, HistoryLimit: 50})
}

func wantCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	if errcode.As(err) != code {
		t.Fatalf("want code %d, got err=%v (code=%d)", code, err, errcode.As(err))
	}
}

// ── 测试 ───────────────────────────────────────────────────────────────────────

func TestSendPrivate_OK(t *testing.T) {
	repo := &fakeRepo{}
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want message_id 100, got %d", id)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("want 1 persisted msg, got %d", len(repo.saved))
	}
	if len(pusher.pushes) != 1 || pusher.pushes[0].kind != "private" || pusher.pushes[0].toPlayerID != 2 {
		t.Fatalf("want 1 private push to 2, got %+v", pusher.pushes)
	}
}

func TestSendPrivate_MissingTarget(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 0, "hi", 100)
	wantCode(t, err, errcode.ErrInvalidArg)
}

func TestSendPrivate_Self(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 1, "hi", 100)
	wantCode(t, err, errcode.ErrInvalidArg)
}

func TestSend_InvalidChannel(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_SYSTEM, 2, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)

	_, err = uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_UNSPECIFIED, 2, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSend_EmptyContent(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "   ", 100)
	wantCode(t, err, errcode.ErrInvalidArg)
}

func TestSend_TooLong(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil) // MaxContentLen=10
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "01234567890", 100)
	wantCode(t, err, errcode.ErrChatMessageTooLong)
}

func TestSendWorld_OK(t *testing.T) {
	pusher := &fakePusher{}
	uc := newUC(&fakeRepo{}, pusher, nil)
	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "gg", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
	if len(pusher.pushes) != 1 || pusher.pushes[0].kind != "world" || pusher.pushes[0].evt.GetToPlayerId() != 0 {
		t.Fatalf("want 1 world broadcast, got %+v", pusher.pushes)
	}
}

func TestSendTeam_OK(t *testing.T) {
	pusher := &fakePusher{}
	team := &fakeTeam{members: map[uint64][]uint64{77: {1, 2, 3}}}
	uc := newUC(&fakeRepo{}, pusher, team)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "rush", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
	// 原则 2:排除发送者 1,发给 2 / 3。
	if len(pusher.pushes) != 2 {
		t.Fatalf("want 2 team pushes, got %d (%+v)", len(pusher.pushes), pusher.pushes)
	}
	for _, p := range pusher.pushes {
		if p.toPlayerID == 1 {
			t.Fatalf("must not push back to sender")
		}
	}
}

func TestSendTeam_NotMember(t *testing.T) {
	team := &fakeTeam{members: map[uint64][]uint64{77: {2, 3}}}
	uc := newUC(&fakeRepo{}, &fakePusher{}, team)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSendTeam_TeamNotFound(t *testing.T) {
	team := &fakeTeam{members: map[uint64][]uint64{}}
	uc := newUC(&fakeRepo{}, &fakePusher{}, team)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSendTeam_DegradedNoDeps(t *testing.T) {
	// team / pusher 均 nil:不报错,返回 message_id(客户端本地回显)。
	uc := newUC(&fakeRepo{}, nil, nil)
	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "hi", 100)
	if err != nil {
		t.Fatalf("degraded team chat should not error, got %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
}

func TestSendTeam_ResolveError(t *testing.T) {
	// team 已接入但运行期解析失败:必须诚实报错(不假成功静默丢消息)。
	team := &fakeTeam{err: errors.New("team rpc down")}
	uc := newUC(&fakeRepo{}, &fakePusher{}, team)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "hi", 100)
	wantCode(t, err, errcode.ErrUnavailable)
}

// ── 公会 / 群频道 fan-out(2026-06-27 群聊上线)────────────────────────────────

// newGuildUC 构造带 guild / group reader 的 ChatUsecase(队伍 reader 留空)。
func newGuildUC(pusher *fakePusher, guild *fakeGuild, group *fakeGroup) *ChatUsecase {
	var p ChatPusher
	if pusher != nil {
		p = pusher
	}
	var g GuildReader
	if guild != nil {
		g = guild
	}
	var gp GroupReader
	if group != nil {
		gp = group
	}
	return NewChatUsecase(&fakeRepo{}, p, nil, g, gp, conf.ChatConf{MaxContentLen: 10, HistoryLimit: 50})
}

func TestSendGuild_OK(t *testing.T) {
	pusher := &fakePusher{}
	guild := &fakeGuild{members: map[uint64][]uint64{55: {1, 2, 3}}}
	uc := newGuildUC(pusher, guild, nil)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GUILD, 55, "hi", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
	// 原则 2:排除发送者 1,发给 2 / 3。
	if len(pusher.pushes) != 2 {
		t.Fatalf("want 2 guild pushes, got %d (%+v)", len(pusher.pushes), pusher.pushes)
	}
	for _, p := range pusher.pushes {
		if p.kind != "guild" {
			t.Fatalf("want guild kind, got %s", p.kind)
		}
		if p.toPlayerID == 1 {
			t.Fatalf("must not push back to sender")
		}
	}
}

func TestSendGuild_NotMember(t *testing.T) {
	guild := &fakeGuild{members: map[uint64][]uint64{55: {2, 3}}}
	uc := newGuildUC(&fakePusher{}, guild, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GUILD, 55, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSendGuild_GuildNotFound(t *testing.T) {
	guild := &fakeGuild{members: map[uint64][]uint64{}}
	uc := newGuildUC(&fakePusher{}, guild, nil)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GUILD, 55, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSendGuild_DegradedNoDeps(t *testing.T) {
	// guild / pusher 均 nil:不报错,返回 message_id(客户端本地回显)。
	uc := newGuildUC(nil, nil, nil)
	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GUILD, 55, "hi", 100)
	if err != nil {
		t.Fatalf("degraded guild chat should not error, got %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
}

func TestSendGroup_OK(t *testing.T) {
	pusher := &fakePusher{}
	group := &fakeGroup{members: map[uint64][]uint64{88: {1, 2}}}
	uc := newGuildUC(pusher, nil, group)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GROUP, 88, "yo", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
	// 原则 2:排除发送者 1,只发给 2。
	if len(pusher.pushes) != 1 {
		t.Fatalf("want 1 group push, got %d (%+v)", len(pusher.pushes), pusher.pushes)
	}
	if pusher.pushes[0].kind != "group" || pusher.pushes[0].toPlayerID != 2 {
		t.Fatalf("want group push to 2, got %+v", pusher.pushes[0])
	}
}

func TestSendGroup_NotMember(t *testing.T) {
	group := &fakeGroup{members: map[uint64][]uint64{88: {2, 3}}}
	uc := newGuildUC(&fakePusher{}, nil, group)
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GROUP, 88, "hi", 100)
	wantCode(t, err, errcode.ErrChatChannelInvalid)
}

func TestSendGroup_DegradedNoDeps(t *testing.T) {
	uc := newGuildUC(nil, nil, nil)
	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_GROUP, 88, "hi", 100)
	if err != nil {
		t.Fatalf("degraded group chat should not error, got %v", err)
	}
	if id != 100 {
		t.Fatalf("want 100, got %d", id)
	}
}

func TestMaskSensitive(t *testing.T) {
	repo := &fakeRepo{}
	uc := NewChatUsecase(repo, nil, nil, nil, nil, conf.ChatConf{MaxContentLen: 256, HistoryLimit: 50, SensitiveWords: []string{"bad"}})
	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "a bad word", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := repo.saved[0].GetContent(); strings.Contains(got, "bad") {
		t.Fatalf("sensitive word not masked: %q", got)
	}
}

func TestPullHistory_NonPrivateEmpty(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	msgs, err := uc.PullHistory(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, 10, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("world history should be empty, got %d", len(msgs))
	}
}

func TestPullHistory_Private(t *testing.T) {
	repo := &fakeRepo{}
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)
	// 1↔2 三条
	_, _ = uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "m1", 101)
	_, _ = uc.SendMessage(context.Background(), 2, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 1, "m2", 102)
	_, _ = uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "m3", 103)

	msgs, err := uc.PullHistory(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, 10, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 history, got %d", len(msgs))
	}
}

func TestPullHistory_MissingPeer(t *testing.T) {
	uc := newUC(&fakeRepo{}, &fakePusher{}, nil)
	_, err := uc.PullHistory(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 0, 10, 0)
	wantCode(t, err, errcode.ErrInvalidArg)
}
