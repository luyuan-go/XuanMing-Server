// match_canceler.go 实现 biz.MatchCanceler:通过 gRPC 调 matchmaker CancelMatch,
// 在成员离队 / 被踢时联动撤销其所在的匹配票据。
//
// 设计(修复"排队中离队不取消票据"的跨服务不一致,原 biz TODO):
//   - team 只知道队伍成员,不知道 matchmaker 的 ticket;按 player_id 撤销即可——matchmaker
//     由 player→ticket 归属(SETNX claim)定位该成员所在整张票据:
//     仍在排队 → CAS 删票 + 释放全队 claim;已进确认期 → 等价该玩家拒绝确认(match 失败退票)。
//   - 内部服务间调用:matchmaker gRPC server 用 AuthOptional,本调用不带玩家 JWT
//     (callerID==0),身份走 CancelMatchRequest.player_id 内部路径,合法。
//   - 弱依赖语义:matchmaker 地址未配 / 调用失败时由 biz.cancelMatchmaking 仅 Warn,
//     不阻断离队;残留票据由确认期超时 / TTL 兜底回收。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

// GrpcMatchClient 用 matchmaker 服务 gRPC client 实现 biz.MatchCanceler 与
// biz.MatchCommitmentReader —— 两个能力都只跟 matchmaker 说话,共用同一条连接。
type GrpcMatchClient struct {
	conn *grpc.ClientConn
	cli  matchv1.MatchServiceClient
}

// NewGrpcMatchClient 直连 matchmaker 服务 endpoint(host:port,内网 insecure)。
func NewGrpcMatchClient(matchmakerAddr string) *GrpcMatchClient {
	conn := grpcclient.MustDialInsecure(matchmakerAddr)
	return &GrpcMatchClient{
		conn: conn,
		cli:  matchv1.NewMatchServiceClient(conn),
	}
}

// Close 关闭底层连接。
func (g *GrpcMatchClient) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// CancelMatch 撤销 playerID 当前所在的匹配票据(整张票据,含全体队友)。
// 玩家未在排队时 matchmaker 返回 ErrMatchNotFound(4004),由调用方按常态忽略。
func (g *GrpcMatchClient) CancelMatch(ctx context.Context, playerID uint64) error {
	resp, err := g.cli.CancelMatch(ctx, &matchv1.CancelMatchRequest{PlayerId: playerID})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "matchmaker.CancelMatch code=%d player=%d", resp.GetCode(), playerID)
	}
	return nil
}

// IsPlayerCommittedToMatch 返回 playerID 当前是否已被一场对局占住。
//
// 权威来源是 matchmaker 的 start 索引 + player→ticket claim(ClaimPlayer 用无 TTL 的
// SETNX 写,只有显式取消 / 失败 / battle_result 调 ReleaseMatch 才删),因此
// STARTING / 排队 / 确认期 / 拉 DS / **整场战斗** 全窗口都会判定为已占住。
//
// 三态严格区分,不准把 UNKNOWN 压成 false(§9.22):
//   - ACTIVE → (true, nil)
//   - NONE   → (false, nil)
//   - UNSPECIFIED / 非 OK code / RPC error → (false, err),由调用方 fail-closed。
func (g *GrpcMatchClient) IsPlayerCommittedToMatch(ctx context.Context, playerID uint64) (bool, error) {
	resp, err := g.cli.ResolvePlayerMatchContext(ctx, &matchv1.ResolvePlayerMatchContextRequest{PlayerId: playerID})
	if err != nil {
		return false, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return false, errcode.New(errcode.Code(resp.GetCode()),
			"matchmaker.ResolvePlayerMatchContext code=%d player=%d", resp.GetCode(), playerID)
	}
	switch resp.GetState() {
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
		return true, nil
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE:
		return false, nil
	default:
		// UNKNOWN:读取错误 / 索引漂移 / 坏记录。不确定就是不确定,交给调用方 fail-closed。
		return false, errcode.New(errcode.ErrUnavailable,
			"matchmaker.ResolvePlayerMatchContext unknown state for player %d", playerID)
	}
}
