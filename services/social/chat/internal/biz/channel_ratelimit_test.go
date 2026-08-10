// channel_ratelimit_test.go —— 非世界频道 per-player 冷却回归(anti-abuse §6 第 6 项)。
//
// 覆盖:冷却期内拒绝(ErrRateLimited,零落库零推送)/ 允许放行 / 判定失败 fail-open /
// nil limiter 不限流 / 频道独立占窗 / 世界频道不受非世界冷却影响。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"

	"github.com/luyuancpp/pandora/services/social/chat/internal/conf"
)

// fakeChannelLimiter 可编程非世界频道限流器:记录每个频道的调用并按预设返回。
type fakeChannelLimiter struct {
	allowed  bool
	err      error
	calls    int
	channels []string
	lastPID  uint64
	lastCool time.Duration
}

func (f *fakeChannelLimiter) AllowChannel(_ context.Context, channel string, playerID uint64, cooldown time.Duration) (bool, error) {
	f.calls++
	f.channels = append(f.channels, channel)
	f.lastPID = playerID
	f.lastCool = cooldown
	return f.allowed, f.err
}

// newChannelUC 构造带非世界冷却配置的用例(repo/pusher 为 fake,world limiter 不注入)。
func newChannelUC(pusher *fakePusher, limiter ChannelRateLimiter) (*ChatUsecase, *fakeRepo) {
	repo := &fakeRepo{}
	uc := NewChatUsecase(repo, pusher, nil, nil, nil, conf.ChatConf{
		MaxContentLen:    10,
		HistoryLimit:     50,
		WorldCooldown:    config.Duration(3 * time.Second),
		NonWorldCooldown: config.Duration(500 * time.Millisecond),
	})
	if limiter != nil {
		uc.SetChannelRateLimiter(limiter)
	}
	return uc, repo
}

func TestSendPrivate_ChannelCooldownRejectedZeroSideEffect(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeChannelLimiter{allowed: false}
	uc, repo := newChannelUC(pusher, lim)

	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100)
	wantCode(t, err, errcode.ErrRateLimited)
	if lim.calls != 1 || lim.channels[0] != "private" || lim.lastPID != 1 || lim.lastCool != 500*time.Millisecond {
		t.Fatalf("limiter call mismatch: %+v", lim)
	}
	// 零副作用:未落库、未推送(验收底线 1)。
	if len(repo.saved) != 0 {
		t.Fatalf("cooldown-rejected private must not persist, got %d rows", len(repo.saved))
	}
	if len(pusher.pushes) != 0 {
		t.Fatalf("cooldown-rejected private must not push, got %+v", pusher.pushes)
	}
}

func TestSendPrivate_ChannelCooldownAllowed(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeChannelLimiter{allowed: true}
	uc, repo := newChannelUC(pusher, lim)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100)
	if err != nil || id != 100 {
		t.Fatalf("allowed private = (%d, %v), want (100, nil)", id, err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("want 1 persisted private message, got %d", len(repo.saved))
	}
}

func TestSendPrivate_ChannelLimiterErrorFailOpen(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeChannelLimiter{allowed: false, err: errors.New("redis down")}
	uc, repo := newChannelUC(pusher, lim)

	if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100); err != nil {
		t.Fatalf("limiter error must fail-open, got %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("fail-open private must persist, got %d", len(repo.saved))
	}
}

func TestSendPrivate_NilChannelLimiterUnthrottled(t *testing.T) {
	pusher := &fakePusher{}
	uc, _ := newChannelUC(pusher, nil)

	for i := 0; i < 3; i++ {
		if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", uint64(100+i)); err != nil {
			t.Fatalf("nil channel limiter must not throttle, got %v", err)
		}
	}
}

// TestChannelKeysAreIndependent:四路频道各自传独立的频道名给 limiter(占各自的窗)。
func TestChannelKeysAreIndependent(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeChannelLimiter{allowed: true}
	uc, _ := newChannelUC(pusher, lim)
	ctx := context.Background()

	_, _ = uc.SendMessage(ctx, 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100)
	_, _ = uc.SendMessage(ctx, 1, chatv1.ChatChannel_CHAT_CHANNEL_TEAM, 77, "hi", 101)
	_, _ = uc.SendMessage(ctx, 1, chatv1.ChatChannel_CHAT_CHANNEL_GUILD, 88, "hi", 102)
	_, _ = uc.SendMessage(ctx, 1, chatv1.ChatChannel_CHAT_CHANNEL_GROUP, 99, "hi", 103)
	want := []string{"private", "team", "guild", "group"}
	if len(lim.channels) != 4 {
		t.Fatalf("want 4 channel checks, got %v", lim.channels)
	}
	for i, ch := range want {
		if lim.channels[i] != ch {
			t.Fatalf("channel #%d = %q, want %q", i, lim.channels[i], ch)
		}
	}
}

// TestSendWorld_NotAffectedByChannelCooldown:世界频道不过非世界 limiter(它有自己的冷却)。
func TestSendWorld_NotAffectedByChannelCooldown(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeChannelLimiter{allowed: false} // 非世界全拒
	uc, _ := newChannelUC(pusher, lim)

	if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "hi", 100); err != nil {
		t.Fatalf("world must not consult channel limiter, got %v", err)
	}
	if lim.calls != 0 {
		t.Fatalf("channel limiter must not be called for world, calls=%d", lim.calls)
	}
}
