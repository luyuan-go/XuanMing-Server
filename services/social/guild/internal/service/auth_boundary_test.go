package service

import (
	"context"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
)

// TestGuildProtectedRPCsRequireJWT 覆盖所有依赖玩家身份的公会入口。nil usecase / nil 两个 snowflake
// 能证明无 JWT 请求在触达业务、数据库或生成 ID 前已被拒绝。
func TestGuildProtectedRPCsRequireJWT(t *testing.T) {
	svc := NewGuildService(nil, nil, nil)
	ctx := context.Background()

	tests := []struct {
		name string
		call func() (commonv1.ErrCode, error)
	}{
		{name: "创建", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.CreateGuild(ctx, &guildv1.CreateGuildRequest{Name: "G"})
			return resp.GetCode(), err
		}},
		{name: "申请", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.ApplyJoin(ctx, &guildv1.ApplyJoinRequest{GuildId: 1})
			return resp.GetCode(), err
		}},
		{name: "批准", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.ApproveJoin(ctx, &guildv1.ApproveJoinRequest{RequestId: 1})
			return resp.GetCode(), err
		}},
		{name: "拒绝", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.RejectJoin(ctx, &guildv1.RejectJoinRequest{RequestId: 1})
			return resp.GetCode(), err
		}},
		{name: "退会", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.LeaveGuild(ctx, &guildv1.LeaveGuildRequest{})
			return resp.GetCode(), err
		}},
		{name: "踢人", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.KickMember(ctx, &guildv1.KickMemberRequest{TargetId: 2})
			return resp.GetCode(), err
		}},
		{name: "解散", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.DisbandGuild(ctx, &guildv1.DisbandGuildRequest{})
			return resp.GetCode(), err
		}},
		{name: "转让", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.TransferLeader(ctx, &guildv1.TransferLeaderRequest{TargetId: 2})
			return resp.GetCode(), err
		}},
		{name: "任命", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.SetOfficer(ctx, &guildv1.SetOfficerRequest{TargetId: 2})
			return resp.GetCode(), err
		}},
		{name: "我的公会", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.GetMyGuild(ctx, &guildv1.GetMyGuildRequest{})
			return resp.GetCode(), err
		}},
		{name: "申请列表", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.ListJoinRequests(ctx, &guildv1.ListJoinRequestsRequest{})
			return resp.GetCode(), err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, err := tt.call()
			if err != nil {
				t.Fatalf("unexpected transport error: %v", err)
			}
			if code != commonv1.ErrCode_ERR_UNAUTHORIZED {
				t.Fatalf("无 JWT 必须拒绝: got=%s", code)
			}
		})
	}
}

// TestGuildServiceRejectsZeroIdentifiers 零目标/公会 ID 必须在 service 边界拒绝，
// 不能进入权限状态机或数据库锁路径。
func TestGuildServiceRejectsZeroIdentifiers(t *testing.T) {
	svc := NewGuildService(nil, nil, nil)
	authCtx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(7))

	tests := []struct {
		name string
		call func() (commonv1.ErrCode, error)
	}{
		{name: "踢人目标", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.KickMember(authCtx, &guildv1.KickMemberRequest{})
			return resp.GetCode(), err
		}},
		{name: "转让目标", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.TransferLeader(authCtx, &guildv1.TransferLeaderRequest{})
			return resp.GetCode(), err
		}},
		{name: "任命目标", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.SetOfficer(authCtx, &guildv1.SetOfficerRequest{})
			return resp.GetCode(), err
		}},
		{name: "查询公会", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.GetGuild(context.Background(), &guildv1.GetGuildRequest{})
			return resp.GetCode(), err
		}},
		{name: "成员列表", call: func() (commonv1.ErrCode, error) {
			resp, err := svc.ListMembers(context.Background(), &guildv1.ListMembersRequest{})
			return resp.GetCode(), err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, err := tt.call()
			if err != nil {
				t.Fatalf("unexpected transport error: %v", err)
			}
			if code != commonv1.ErrCode_ERR_INVALID_ARG {
				t.Fatalf("零标识必须拒绝: got=%s", code)
			}
		})
	}
}
