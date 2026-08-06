package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// TestAcknowledgeLocalDeparture_Idempotent:local 面没有 connected ownership 记录可删
// (authRepo=nil),离场 ACK 只需回执"该 exact identity 的 connected owner 不存在",
// 即 Departed=true。这是让 DS 的 PendingHubDepartures 队列出队的唯一条件
// (ShouldCompleteQueuedHubDeparture 认 code=0)。
func TestAcknowledgeLocalDeparture_Idempotent(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 1)
	res, err := uc.AcknowledgeLocalDeparture(context.Background(), 42,
		"assign-local-1", "pandora-hub-global-1")
	if err != nil {
		t.Fatalf("AcknowledgeLocalDeparture err: %v", err)
	}
	if !res.Departed {
		t.Fatalf("local departure 必须幂等回 Departed=true,got %+v", res)
	}
}

// TestAcknowledgeLocalDeparture_ModelBAuthorityFailClosed 是与
// AcknowledgeLocalAdmission 同款的隔离护栏:装了 Redis 授权面(authRepo != nil)还走
// legacy 离场 = 绕过 exact admission 仲裁,必须 fail-closed 拒,不能因为"回 OK 能让 DS
// 停止重试"就放行 —— 那等于把线上离场判定降级成无条件成功。
func TestAcknowledgeLocalDeparture_ModelBAuthorityFailClosed(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 1)
	uc.authRepo = &data.RedisHubAuthRepo{} // 非 nil 即 Model B 姿态;本测试不触其方法

	res, err := uc.AcknowledgeLocalDeparture(context.Background(), 42,
		"assign-local-1", "pandora-hub-global-1")
	if err == nil {
		t.Fatalf("Model B 下 legacy 离场必须拒,got %+v", res)
	}
	if got := errcode.As(err); got != errcode.ErrUnauthorized {
		t.Fatalf("code=%v, want ErrUnauthorized(%v)", got, errcode.ErrUnauthorized)
	}
}

// TestAcknowledgeLocalDeparture_IncompleteIdentity 身份不全一律 INVALID_ARG,
// 不给"空 assignment 也算离场成功"。
func TestAcknowledgeLocalDeparture_IncompleteIdentity(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 1)
	cases := []struct {
		name         string
		playerID     uint64
		assignmentID string
		pod          string
	}{
		{"player_id=0", 0, "assign-local-1", "pandora-hub-global-1"},
		{"assignment_id empty", 42, "", "pandora-hub-global-1"},
		{"pod empty", 42, "assign-local-1", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := uc.AcknowledgeLocalDeparture(context.Background(), c.playerID, c.assignmentID, c.pod)
			if err == nil {
				t.Fatalf("身份不全必须拒")
			}
			if got := errcode.As(err); got != errcode.ErrInvalidArg {
				t.Fatalf("code=%v, want ErrInvalidArg(%v)", got, errcode.ErrInvalidArg)
			}
		})
	}
}
