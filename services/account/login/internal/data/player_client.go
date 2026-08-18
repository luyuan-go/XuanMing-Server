// player_client.go — login → player gRPC 客户端封装(账号 / 角色分离,2026-08-18)。
//
// 目的:把「角色名 = 账号名」落到**全服显示名权威** pandora_player.players.nickname。
// 只在 LoginResponse 里下发一个名字是不够的 —— 那只有本人看得见,别人在头顶铭牌 /
// 队伍面板 / 聊天 / 好友 / 公会 / 排行榜里看到的仍是 player 服务的默认前缀名。
//
// 弱依赖(§9.21):player 不可达、或还没滚上带 EnsureProfile 的版本时必须降级,
// 绝不阻断登录。降级后果仅仅是角色名回落成 Player_<player_id>,下次登录再试。
// 这条降级不是可选项 —— 新 RPC 靠「先发布 player 再发布 login」的顺序兜底等于给
// 破规发通行证(§9.21:人执行的顺序没有机械手段能拦,后果还是静默的)。
package data

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// GrpcProfileSeeder 实现 biz.ProfileSeeder,内嵌 grpc client。
type GrpcProfileSeeder struct {
	conn   *grpc.ClientConn
	client playerv1.PlayerServiceClient
}

// NewGrpcProfileSeeder 用现成的 *grpc.ClientConn 包出 seeder。
// 调用方负责 conn 生命周期管理(main.go defer conn.Close())。
func NewGrpcProfileSeeder(conn *grpc.ClientConn) *GrpcProfileSeeder {
	return &GrpcProfileSeeder{conn: conn, client: playerv1.NewPlayerServiceClient(conn)}
}

// SeededProfile 是 EnsureProfile 的最小 client 视角产出。
// 字段语义与 biz.RoleProfile 一致(biz 不 import data 的结构,两边各自最小化)。
type SeededProfile struct {
	Created  bool
	Nickname string
	Level    uint32
}

// EnsureProfile 调 player 建档并播种角色名(INSERT IGNORE 语义,已存在则原样返回)。
//
// 超时由调用方(biz)控制:它挂在登录热路径上,子预算与降级策略属于业务决定,
// 这里不再自作主张收紧一次。
func (s *GrpcProfileSeeder) EnsureProfile(
	ctx context.Context, playerID uint64, nickname string,
) (SeededProfile, error) {
	resp, err := s.client.EnsureProfile(ctx, &playerv1.EnsureProfileRequest{
		PlayerId: playerID,
		Nickname: nickname,
	})
	if err != nil {
		// §9.21 弱依赖降级的关键一步:把 gRPC Unimplemented 翻成 ErrNotImplemented。
		// 对端**这个版本**还没有这个 method(滚动升级期的预期状态),重试永远不会成功,
		// 与「暂时不可用、重试会好」必须能被调用方区分开 —— 否则要么当故障刷告警,
		// 要么当可重试白白重试到超时。
		if status.Code(err) == codes.Unimplemented {
			return SeededProfile{}, errcode.NewCause(errcode.ErrNotImplemented, err,
				"player EnsureProfile not implemented on peer version")
		}
		return SeededProfile{}, errcode.New(errcode.ErrUnavailable,
			"player EnsureProfile rpc: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		// 业务码原样透出(尤其 ERR_PLAYER_NICKNAME_TAKEN):调用方按码分流日志与降级。
		return SeededProfile{}, errcode.New(errcode.Code(resp.GetCode()),
			"player EnsureProfile code=%d player_id=%d", resp.GetCode(), playerID)
	}
	return SeededProfile{
		Created:  resp.GetCreated(),
		Nickname: resp.GetEffectiveNickname(),
		Level:    resp.GetLevel(),
	}, nil
}
