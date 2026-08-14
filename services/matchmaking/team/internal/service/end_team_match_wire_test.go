// end_team_match_wire_test.go — EndTeamMatch 的**接线**验证(INC-20260813-001)。
//
// biz 侧的行为用例在 internal/biz/end_team_match_test.go。本文件只回答两件单测答不了的事:
//
//  1. **这个 RPC 真的被注册到 gRPC server 上了吗** —— proto 加了方法、service 写了 handler、
//     两者都编译通过,但 `RegisterTeamServiceServer` 若因为任何原因没带上它,
//     matchmaker 在线上会拿到 `Unimplemented`,而**所有单测照样绿**。
//     这一类「编译得过但没接上」正是本事故那种缺口的形状(§14 接线完整性铁律)。
//  2. **systemOnly 守卫真的挡在业务之前吗** —— Envoy 按 `/pandora.team.v1.TeamService/`
//     整前缀路由,带玩家 JWT 的客户端同样能打到这个方法;它能把**任意**队伍打回 FORMING,
//     不挡就是一个「让任何队伍开不了局」的骚扰口子。
package service

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// TestEndTeamMatchRejectsClientCaller:带玩家 JWT 调必须拒。
//
// nil usecase 是本测试的关键:它证明拒绝发生在触达业务与 Redis **之前** ——
// 若门没生效,这里会 nil 解引用 panic 而不是安静地返回错误码。
// (与 TestGetPlayerTeamRejectsClientCaller 同一手法。)
func TestEndTeamMatchRejectsClientCaller(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	authCtx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(7))

	resp, err := svc.EndTeamMatch(authCtx, &teamv1.EndTeamMatchRequest{TeamId: 9001})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("带玩家 JWT 调内部 RPC 必须拒: got=%s", resp.GetCode())
	}
}

// TestEndTeamMatchRegisteredOnGrpcServer:真的走一遍 gRPC。
//
// 用 bufconn 起一台真 server(与 internal/server/grpc.go 同一个
// `RegisterTeamServiceServer`),再用**生成的 client** 打进去。
// 方法没被注册时这里会拿到 `Unimplemented`,而不是一个业务错误码。
//
// 刻意仍用 nil usecase + systemOnly 拒绝路径:本测试要证的是「线接上了」,
// 不是业务行为(那在 biz 包里已有 9 条)。让请求停在守卫处,就不必在 service 包里
// 搭一整套 Redis 夹具 —— 只要拿到的是 `ERR_PERMISSION_DENY` 而不是 `Unimplemented`,
// 就说明 proto → server 注册 → handler 这条线是通的。
func TestEndTeamMatchRegisteredOnGrpcServer(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	teamv1.RegisterTeamServiceServer(srv, NewTeamService(nil, nil, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(c)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// 服务端 handler 从 ctx 取 caller;bufconn 直连没有 JWT middleware,
	// 因此 callerID==0 → systemOnly 放行 → 落到 nil usecase 前的入参校验。
	// team_id=0 是最短的合法拒绝路径,足以证明请求确实到达了 handler。
	resp, err := teamv1.NewTeamServiceClient(conn).EndTeamMatch(ctx, &teamv1.EndTeamMatchRequest{})
	if err != nil {
		t.Fatalf("EndTeamMatch 未被注册到 gRPC server(matchmaker 线上会拿到 Unimplemented): %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("请求到达了 handler 但走错分支: got=%s want=ERR_INVALID_ARG", resp.GetCode())
	}
}

// TestBeginTeamMatchRejectsClientCaller:组票 fence 同样是内部东西向接口。
//
// 这道门此前**完全不存在**(2026-08-13 补)。它能给任意队伍上一把 roster 租约,
// 客户端反复调用即可让别人的队伍始终「被别人的组票占住」,队长自己反而开不了局。
// nil usecase 证明拒绝发生在触达业务之前。
func TestBeginTeamMatchRejectsClientCaller(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	authCtx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(7))

	resp, err := svc.BeginTeamMatch(authCtx, &teamv1.BeginTeamMatchRequest{
		TeamId: 9001, CaptainId: 7, OperationId: "op-1", LeaseMs: 5000,
	})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("带玩家 JWT 调组票 fence 必须拒: got=%s", resp.GetCode())
	}
	if resp.GetTeam() != nil || resp.GetLeaseExpiresAtMs() != 0 {
		t.Fatal("拒绝分支不得泄露队伍快照或上锁")
	}
}
