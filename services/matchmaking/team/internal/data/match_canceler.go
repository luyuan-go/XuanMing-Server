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
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

// team→matchmaker 跨服务边界的拒绝 reason 枚举(§11.3 R2)。
//
// 这条边界此前零日志:玩家「离队了但票据还在排队」「明明在打却被判成空闲」
// 两类问题在 team 与 matchmaker 两侧日志里都断线,只能靠猜。
const (
	matchRejectCancelRPCFailed    = "matchmaker_cancel_rpc_failed"
	matchRejectCancelCodeNotOK    = "matchmaker_cancel_code_not_ok"
	matchRejectCommitSignFailed   = "resume_auth_sign_failed"
	matchRejectCommitRPCFailed    = "matchmaker_resolve_rpc_failed"
	matchRejectCommitCodeNotOK    = "matchmaker_resolve_code_not_ok"
	matchRejectCommitStateUnknown = "matchmaker_resolve_state_unknown"
)

// GrpcMatchClient 用 matchmaker 服务 gRPC client 实现 biz.MatchCanceler 与
// biz.MatchCommitmentReader —— 两个能力都只跟 matchmaker 说话,共用同一条连接。
type GrpcMatchClient struct {
	conn *grpc.ClientConn
	cli  matchv1.MatchServiceClient
	// signer 是 team→matchmaker 的东西向服务鉴权签名器(caller="team")。
	// **只用于 ResolvePlayerMatchContext**:matchmaker 对该方法强制验签,不签必被
	// ERR_PERMISSION_DENY(7) 拒。CancelMatch 走的是另一条 AuthOptional 内部路径
	// (callerID==0 + 请求内 player_id),不需要也不应该签。
	// nil = 未配密钥,IsPlayerCommittedToMatch 会被拒 → 调用方按 UNKNOWN fail-closed。
	signer *internalrpcauth.Signer
}

// NewGrpcMatchClient 直连 matchmaker 服务 endpoint(host:port,内网 insecure)。
// signer 可为 nil(见字段注释)。
func NewGrpcMatchClient(matchmakerAddr string, signer *internalrpcauth.Signer) *GrpcMatchClient {
	conn := grpcclient.MustDialInsecure(matchmakerAddr)
	return &GrpcMatchClient{
		conn:   conn,
		cli:    matchv1.NewMatchServiceClient(conn),
		signer: signer,
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
		// 弱依赖:调用方只 Warn 不阻断离队,残留票据靠确认期超时 / TTL 兜底。
		// 但「离队后仍被匹配拉进对局」的第一嫌疑就是这里,必须留证。
		plog.With(ctx).Warnw("msg", "match_cancel_failed", "player_id", playerID,
			"reason", matchRejectCancelRPCFailed, "err", err,
			"hint", "票据未撤销,等确认期超时 / TTL 回收")
		return err
	}
	if code := resp.GetCode(); code != commonv1.ErrCode_OK {
		// 4001 = 玩家本就不在排队,是离队路径的常态(每次离队都会调一次),走 Debug 不刷屏。
		if errcode.Code(code) == errcode.ErrMatchNotFound {
			plog.With(ctx).Debugw("msg", "match_cancel_noop", "player_id", playerID,
				"reason", "player_not_in_queue")
		} else {
			plog.With(ctx).Warnw("msg", "match_cancel_failed", "player_id", playerID,
				"reason", matchRejectCancelCodeNotOK, "code", int32(code))
		}
		return errcode.New(errcode.Code(resp.GetCode()), "matchmaker.CancelMatch code=%d player=%d", resp.GetCode(), playerID)
	}
	// §11.3 R1:撤票是不可逆状态推进,且是「排队中离队」链路上 team 侧唯一的成功证据。
	plog.With(ctx).Infow("msg", "match_canceled", "player_id", playerID)
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
	// 每次调用签一份新鲜的 request-bound 凭证(方法 + player_id + 时间戳 + 一次性 nonce)。
	// 未配 signer 时照常发出:matchmaker 会拒(code=7),按上面的三态约定落到 err 分支
	// fail-closed,而不是被静默当成"没在对局中"。
	if g.signer != nil {
		signed, serr := g.signer.SignContext(ctx,
			matchv1.MatchService_ResolvePlayerMatchContext_FullMethodName, playerID)
		if serr != nil {
			plog.With(ctx).Warnw("msg", "match_commitment_query_failed", "player_id", playerID,
				"reason", matchRejectCommitSignFailed, "err", serr,
				"hint", "调用方按 UNKNOWN fail-closed,玩家会被判为「可能在对局中」")
			return false, errcode.NewCause(errcode.ErrInternal, serr,
				"sign matchmaker resume-auth credential for player %d", playerID)
		}
		ctx = signed
	} else {
		// 未配密钥 = 本部署恒查不到对局占用,matchmaker 必回 code=7。
		// 这是部署配置缺口,不是偶发故障,单独一个 reason 便于一眼区分。
		plog.With(ctx).Warnw("msg", "match_commitment_query_unsigned", "player_id", playerID,
			"reason", "resume_auth_signer_not_configured",
			"hint", "matchmaker 将回 ERR_PERMISSION_DENY(7),调用方 fail-closed")
	}
	resp, err := g.cli.ResolvePlayerMatchContext(ctx, &matchv1.ResolvePlayerMatchContextRequest{PlayerId: playerID})
	if err != nil {
		plog.With(ctx).Warnw("msg", "match_commitment_query_failed", "player_id", playerID,
			"reason", matchRejectCommitRPCFailed, "err", err)
		return false, err
	}
	if code := resp.GetCode(); code != commonv1.ErrCode_OK {
		plog.With(ctx).Warnw("msg", "match_commitment_query_failed", "player_id", playerID,
			"reason", matchRejectCommitCodeNotOK, "code", int32(code),
			"signed", g.signer != nil)
		return false, errcode.New(errcode.Code(resp.GetCode()),
			"matchmaker.ResolvePlayerMatchContext code=%d player=%d", resp.GetCode(), playerID)
	}
	switch resp.GetState() {
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
		// §11.3 R4:这是查询路径(离队 / 恢复判定都会调),成功侧走 Debug 不刷屏。
		plog.With(ctx).Debugw("msg", "match_commitment_queried", "player_id", playerID,
			"committed", true)
		return true, nil
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE:
		plog.With(ctx).Debugw("msg", "match_commitment_queried", "player_id", playerID,
			"committed", false)
		return false, nil
	default:
		// UNKNOWN:读取错误 / 索引漂移 / 坏记录。不确定就是不确定,交给调用方 fail-closed。
		// 这条必须可见 —— 它意味着玩家被判成「可能在对局中」而被挡住,且原因在 matchmaker 侧。
		plog.With(ctx).Warnw("msg", "match_commitment_query_failed", "player_id", playerID,
			"reason", matchRejectCommitStateUnknown, "state", int32(resp.GetState()),
			"hint", "matchmaker 侧索引漂移 / 坏记录,调用方 fail-closed")
		return false, errcode.New(errcode.ErrUnavailable,
			"matchmaker.ResolvePlayerMatchContext unknown state for player %d", playerID)
	}
}
