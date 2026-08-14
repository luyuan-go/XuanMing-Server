// end_team_match_client_test.go — `GrpcTeamReader.EndTeamMatch` 的**过线**验证
// (INC-20260813-001)。
//
// biz 侧用 mock TeamReader 测「ReleaseMatch 会不会调它」,team 侧用 bufconn 测
// 「RPC 有没有被注册」。中间还剩一段没人测过:**这个客户端到底往线上发了什么、
// 又怎么解释回来的 code**。这一段错了(发错字段、把非 OK 压成成功)的后果是静默的 ——
// ReleaseMatch 会认为复位成功并删掉 canonical match,而队伍其实还停在 READY,
// 且此后再也没有重投机会。
package data

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/luyuancpp/pandora/pkg/errcode"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// recordingTeamServer 记录收到的请求并按需回指定 code。
type recordingTeamServer struct {
	teamv1.UnimplementedTeamServiceServer
	got  *teamv1.EndTeamMatchRequest
	code commonv1.ErrCode
}

func (s *recordingTeamServer) EndTeamMatch(_ context.Context, req *teamv1.EndTeamMatchRequest) (*teamv1.EndTeamMatchResponse, error) {
	s.got = req
	return &teamv1.EndTeamMatchResponse{Code: s.code}, nil
}

func newWiredTeamReader(t *testing.T, srvImpl teamv1.TeamServiceServer) *GrpcTeamReader {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	teamv1.RegisterTeamServiceServer(srv, srvImpl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(c)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &GrpcTeamReader{conn: conn, cli: teamv1.NewTeamServiceClient(conn)}
}

// 请求体必须原样带上 team_id 与本局 roster —— 名单发丢了,team 侧就会退化成「复位全队」,
// 把对局期间才加入的人也一起清掉。
func TestGrpcTeamReader_EndTeamMatch请求体(t *testing.T) {
	srv := &recordingTeamServer{code: commonv1.ErrCode_OK}
	r := newWiredTeamReader(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.EndTeamMatch(ctx, 8001, []uint64{7901, 7902}, 0); err != nil {
		t.Fatalf("EndTeamMatch: %v", err)
	}
	if srv.got.GetTeamId() != 8001 {
		t.Fatalf("team_id 发错: %d", srv.got.GetTeamId())
	}
	if got := srv.got.GetPlayerIds(); len(got) != 2 || got[0] != 7901 || got[1] != 7902 {
		t.Fatalf("player_ids 发错: %v", got)
	}
}

// 非 OK 的业务 code **必须**如实上抛。压成 nil 的后果是静默的:
// ReleaseMatch 会当作复位成功、删掉 canonical match,队伍却还停在 READY,
// 而且此后再没有重投机会 —— 比一开始就没这条链更糟(至少那样还看得见)。
func TestGrpcTeamReader_EndTeamMatch非OK不得吞(t *testing.T) {
	srv := &recordingTeamServer{code: commonv1.ErrCode_ERR_TEAM_CONCURRENT}
	r := newWiredTeamReader(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.EndTeamMatch(ctx, 8001, []uint64{7901}, 0)
	if err == nil {
		t.Fatal("非 OK code 必须上抛,否则 outbox 不会重投,队伍永远停在 READY")
	}
	if errcode.As(err) != errcode.ErrTeamConcurrent {
		t.Fatalf("业务码必须原样透传(供上游区分「可重试」与「真失败」): got=%d", errcode.As(err))
	}
}

// ★ 滚动升级共存窗口(§9.21):team 还没滚到带本 RPC 的版本 ⇒ 服务端回 Unimplemented。
//
// 这一类**重试永远不会成功**,必须被识别成 `ErrNotImplemented` 而不是普通失败 ——
// 否则 outbox 会一直空转、canonical match 持续积压,并且实际上给发布引入了一条
// 「team 必须先于 matchmaker 上线」的顺序约束(而顺序搞错没有任何机械手段能拦住)。
func TestGrpcTeamReader_EndTeamMatch对端未实现要可判别(t *testing.T) {
	// UnimplementedTeamServiceServer 不实现 EndTeamMatch,正是旧版 team 的形状。
	r := newWiredTeamReader(t, &teamv1.UnimplementedTeamServiceServer{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.EndTeamMatch(ctx, 8001, []uint64{7901}, 0)
	if err == nil {
		t.Fatal("对端未实现必须上抛(交给 biz 判定如何降级),不能静默当成功")
	}
	if errcode.As(err) != errcode.ErrNotImplemented {
		t.Fatalf("必须可判别为「对端还没这个能力」而不是普通失败,否则 outbox 会无限空转: got=%d",
			errcode.As(err))
	}
}

// 反向:普通传输错误(team 挂了 / 超时)**不得**被误判成「对端未实现」——
// 那会让一次真故障被静默跳过,队伍再也没人复位。
func TestGrpcTeamReader_EndTeamMatch普通传输错误不得误判(t *testing.T) {
	r := newWiredTeamReader(t, &teamv1.UnimplementedTeamServiceServer{})
	// 用已取消的 ctx 造一个非 Unimplemented 的传输错误。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.EndTeamMatch(ctx, 8001, []uint64{7901}, 0)
	if err == nil {
		t.Fatal("传输错误必须上抛")
	}
	if errcode.As(err) == errcode.ErrNotImplemented {
		t.Fatal("真故障被误判成「对端未实现」会被静默跳过,队伍再也没人复位")
	}
}
