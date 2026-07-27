// world_ratelimit_test.go —— 世界频道 per-player 冷却回归(压测审核【必修-5】,2026-07-26)。
//
// 覆盖:冷却期内拒绝(ErrRateLimited,零推送)/ 允许时正常广播 / 限流判定失败 fail-open /
// 未接线(nil limiter)不限流 / 其它频道不受世界冷却影响。
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

// fakeWorldLimiter 可编程限流器:记录调用并按预设返回。
type fakeWorldLimiter struct {
	allowed  bool
	err      error
	calls    int
	lastPID  uint64
	lastCool time.Duration
}

func (f *fakeWorldLimiter) AllowWorld(_ context.Context, playerID uint64, cooldown time.Duration) (bool, error) {
	f.calls++
	f.lastPID = playerID
	f.lastCool = cooldown
	return f.allowed, f.err
}

// newWorldUC 构造带世界冷却配置的用例。
func newWorldUC(pusher *fakePusher, limiter WorldRateLimiter) *ChatUsecase {
	uc := NewChatUsecase(&fakeRepo{}, pusher, nil, nil, nil, conf.ChatConf{
		MaxContentLen: 10,
		HistoryLimit:  50,
		WorldCooldown: config.Duration(3 * time.Second),
	})
	if limiter != nil {
		uc.SetWorldRateLimiter(limiter)
	}
	return uc
}

func TestSendWorld_CooldownRejected(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeWorldLimiter{allowed: false}
	uc := newWorldUC(pusher, lim)

	_, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "hi", 100)
	wantCode(t, err, errcode.ErrRateLimited)
	if lim.calls != 1 || lim.lastPID != 1 || lim.lastCool != 3*time.Second {
		t.Fatalf("limiter call mismatch: %+v", lim)
	}
	if len(pusher.pushes) != 0 {
		t.Fatalf("cooldown-rejected message must not be pushed, got %+v", pusher.pushes)
	}
}

func TestSendWorld_Allowed(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeWorldLimiter{allowed: true}
	uc := newWorldUC(pusher, lim)

	id, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "hi", 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if id != 100 {
		t.Fatalf("want message_id 100, got %d", id)
	}
	if len(pusher.pushes) != 1 || pusher.pushes[0].kind != "world" {
		t.Fatalf("want 1 world push, got %+v", pusher.pushes)
	}
}

func TestSendWorld_LimiterErrorFailOpen(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeWorldLimiter{allowed: false, err: errors.New("redis down")}
	uc := newWorldUC(pusher, lim)

	// 限流判定失败 → fail-open 放行(牺牲限流保聊天可用),消息仍广播。
	if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "hi", 100); err != nil {
		t.Fatalf("limiter error must fail-open, got err: %v", err)
	}
	if len(pusher.pushes) != 1 {
		t.Fatalf("want 1 world push after fail-open, got %+v", pusher.pushes)
	}
}

func TestSendWorld_NilLimiterUnthrottled(t *testing.T) {
	pusher := &fakePusher{}
	uc := newWorldUC(pusher, nil)

	for i := 0; i < 3; i++ {
		if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_WORLD, 0, "hi", uint64(100+i)); err != nil {
			t.Fatalf("nil limiter must not throttle, got err: %v", err)
		}
	}
	if len(pusher.pushes) != 3 {
		t.Fatalf("want 3 world pushes, got %d", len(pusher.pushes))
	}
}

func TestSendPrivate_NotAffectedByWorldCooldown(t *testing.T) {
	pusher := &fakePusher{}
	lim := &fakeWorldLimiter{allowed: false} // 世界频道全拒
	uc := newWorldUC(pusher, lim)

	if _, err := uc.SendMessage(context.Background(), 1, chatv1.ChatChannel_CHAT_CHANNEL_PRIVATE, 2, "hi", 100); err != nil {
		t.Fatalf("private must not consult world limiter, got err: %v", err)
	}
	if lim.calls != 0 {
		t.Fatalf("world limiter must not be called for private, calls=%d", lim.calls)
	}
}
