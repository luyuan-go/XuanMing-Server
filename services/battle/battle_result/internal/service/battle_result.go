// Package service 是 battle_result 服务的 gRPC service 层(W4 ③,2026-06-06)。
//
// 职责:
//   - 实现 battlev1.BattleResultServiceServer 接口
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 说明:Model-B 的 ReportResult 是正常唯一结算入口，必须经 DS callback Guard + Redis
// active 校验；无凭据 Kafka battle.result 只允许 legacy/off。调用方不从 ctx 取 player_id。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// BattleResultService 实现 battlev1.BattleResultServiceServer。
type BattleResultService struct {
	battlev1.UnimplementedBattleResultServiceServer
	uc *biz.BattleResultUsecase

	// dsGuard DS 回调令牌守卫(审核 P1 #1);nil = 未启用(mode=off)。
	dsGuard                 *middleware.DSCallbackGuard
	battleCredentialChecker BattleCredentialStateChecker
}

// NewBattleResultService 构造。
func NewBattleResultService(uc *biz.BattleResultUsecase) *BattleResultService {
	return &BattleResultService{uc: uc}
}

// SetDSCallbackGuard 注入 DS 回调令牌守卫(main 按 ds_auth 配置构建;nil 表示 off)。
func (s *BattleResultService) SetDSCallbackGuard(g *middleware.DSCallbackGuard) { s.dsGuard = g }

// SetBattleCredentialStateChecker 启用 Redis active credential 终态门。
func (s *BattleResultService) SetBattleCredentialStateChecker(checker BattleCredentialStateChecker) {
	s.battleCredentialChecker = checker
}

// logDSAuthReject 记录 DS 回调链的鉴权 / fencing 拒绝(僵尸 / 伪造 / 失租 / 换 pod 的 DS 拿旧票
// 或错 pod 上报结算 / 进度)。这些拒绝码是业务范围(ErrUnauthorized / 状态机 / 终态门),
// 经 in-band Code + nil transport error 返回,access-log 中间件按 rpc_ok(DEBUG)记录,线上
// info 级静默——但它们正是 §9.6 / §9.22 要能查到的安全信号,故在拒绝点显式打 WARN 留证。
func logDSAuthReject(ctx context.Context, rpc, stage string, matchID uint64, reportedPod, credentialPod string, err error) {
	kv := []any{"msg", "ds_auth_rejected", "rpc", rpc, "stage", stage, "match_id", matchID}
	if reportedPod != "" {
		kv = append(kv, "reported_pod", reportedPod)
	}
	if credentialPod != "" {
		kv = append(kv, "credential_pod", credentialPod)
	}
	if err != nil {
		kv = append(kv, "code", int32(toProtoCode(err)), "err", err.Error())
	}
	plog.With(ctx).Warnw(kv...)
}

// ReportResult 同步上报一场对局结算(幂等)。
func (s *BattleResultService) ReportResult(ctx context.Context, req *battlev1.ReportResultRequest) (*battlev1.ReportResultResponse, error) {
	if req.GetResult() == nil || req.GetResult().GetMatchId() == 0 {
		plog.With(ctx).Warnw("msg", "ds_report_result_rejected",
			"reason", "missing_match_id",
			"hint", "DS 上报结算缺 result / match_id,请求在鉴权前就被拒")
		return &battlev1.ReportResultResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// §11.3 R3:DS 回调面没有玩家 JWT,plog.With(ctx) 不会自动带 player_id / match_id。
	// 在 match_id 解析成功处写进 ctx,本请求后续**所有**日志(含 data 层、退避失败等
	// 没手写 match_id 的那些)自动带上这个 join key。
	ctx = plog.WithMatchID(ctx, req.GetResult().GetMatchId())
	// 链路第一站留痕(§11.3 R1):没有这条时,「打完没结算」分不清是 DS 根本没上报,
	// 还是上报了被后面某道门拒掉(鉴权码全是业务码,access log 只记 rpc_ok/DEBUG)。
	// 每场对局一条(DS 重试会各留一条,与后面的 idempotent_hit 成对)。
	plog.With(ctx).Infow("msg", "battle_result_received",
		"match_id", req.GetResult().GetMatchId(), "ds_pod_name", req.GetResult().GetDsPodName(),
		"players", len(req.GetResult().GetStats()), "winner_team", req.GetResult().GetWinnerTeam(),
		"outcome", req.GetResult().GetOutcome().String(),
		"final_progress_seq", req.GetFinalProgressSeq())
	// DS 回调范围绑定:battle 令牌 match_id 必须等于上报的 match_id
	// (防拿 A 局令牌伪造 B 局结算;不变量 §9.2 结算幂等 + §9.6 DS 不可信)。
	// RequireToken:纯 DS 回调,enforce 下无令牌直连一律拒(堵绕过 Envoy 的东西向旁路,审核 P1)。
	_, credential, err := s.dsGuard.CheckBattleCredential(ctx, middleware.DSScope{Type: auth.DSTypeBattle, MatchID: req.GetResult().GetMatchId(), RequireToken: true})
	if err != nil {
		logDSAuthReject(ctx, "ReportResult", "check_credential", req.GetResult().GetMatchId(), req.GetResult().GetDsPodName(), "", err)
		return &battlev1.ReportResultResponse{Code: toProtoCode(err)}, nil
	}
	var terminalRelease *data.TerminalReleaseRecord
	if s.battleCredentialChecker != nil {
		if credential == nil || req.GetResult().GetDsPodName() == "" || req.GetResult().GetDsPodName() != credential.Pod {
			credPod := ""
			if credential != nil {
				credPod = credential.Pod
			}
			logDSAuthReject(ctx, "ReportResult", "pod_mismatch", req.GetResult().GetMatchId(), req.GetResult().GetDsPodName(), credPod, nil)
			return &battlev1.ReportResultResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
		}
		proof, err := s.battleCredentialChecker.AuthorizeResult(ctx, req.GetResult().GetMatchId(), credential)
		if err != nil {
			logDSAuthReject(ctx, "ReportResult", "authorize_result", req.GetResult().GetMatchId(), req.GetResult().GetDsPodName(), credential.Pod, err)
			return &battlev1.ReportResultResponse{Code: toProtoCode(err)}, nil
		}
		terminalRelease = &proof
	}
	var already bool
	if terminalRelease != nil {
		already, err = s.uc.ReportAuthorizedResult(ctx, req.GetResult(), *terminalRelease, req.GetFinalProgressSeq())
	} else {
		already, err = s.uc.ReportResult(ctx, req.GetResult(), req.GetFinalProgressSeq())
	}
	if err != nil {
		return &battlev1.ReportResultResponse{Code: toProtoCode(err)}, nil
	}
	if s.battleCredentialChecker != nil && !already {
		// immediate receipt 只是低延迟优化。MySQL 已把同一鉴权证明与战绩原子写入
		// terminal_release_outbox；即使这里因响应丢失、Redis 抖动或 token 临界过期失败，
		// 也必须回 OK，后台 relay 会用持久证明完成 terminal CAS + UID 回收。
		if err := s.battleCredentialChecker.MarkResultRecorded(
			ctx, req.GetResult().GetMatchId(), credential); err != nil {
			plog.With(ctx).Warnw("msg", "battle_result_receipt_deferred_to_outbox",
				"match_id", req.GetResult().GetMatchId(), "err", err)
		}
	}
	return &battlev1.ReportResultResponse{Code: commonv1.ErrCode_OK, AlreadyRecorded: already}, nil
}

// ReportProgress 战斗中实时进度事实上报(实时成长,realtime-progression.md §3/§4.1)。
//
// 鉴权复用 ReportResult 的 DS 回调链:Guard battle 令牌绑 match_id;authority_mode=redis 时
// 另过 Redis active 校验并取权威 roster(玩家越权直接拒)。对局结算后 credential 进入终态
// + 水位表打终局标记,双重保证迟到进度一律拒(僵尸 DS fencing)。
func (s *BattleResultService) ReportProgress(ctx context.Context, req *battlev1.ReportProgressRequest) (*battlev1.ReportProgressResponse, error) {
	if req.GetMatchId() == 0 || len(req.GetEvents()) == 0 {
		plog.With(ctx).Warnw("msg", "ds_report_progress_rejected",
			"reason", "missing_match_id_or_events",
			"match_id", req.GetMatchId(), "events", len(req.GetEvents()))
		return &battlev1.ReportProgressResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// 同 ReportResult:在解析成功处把 match_id 写进 ctx 作为本请求的 join key(§11.3 R3)。
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
	_, credential, err := s.dsGuard.CheckBattleCredential(ctx, middleware.DSScope{Type: auth.DSTypeBattle, MatchID: req.GetMatchId(), RequireToken: true})
	if err != nil {
		logDSAuthReject(ctx, "ReportProgress", "check_credential", req.GetMatchId(), "", "", err)
		return &battlev1.ReportProgressResponse{Code: toProtoCode(err)}, nil
	}
	// roster:Redis active 校验副产物(canonical BattleStorageRecord),biz 用它拒绝
	// 非本场玩家的进度事实。checker 未启用(dev / mode off)→ roster=nil,biz 跳过成员校验。
	var roster []uint64
	if s.battleCredentialChecker != nil {
		if credential == nil {
			logDSAuthReject(ctx, "ReportProgress", "credential_nil", req.GetMatchId(), "", "", nil)
			return &battlev1.ReportProgressResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
		}
		proof, aerr := s.battleCredentialChecker.AuthorizeResult(ctx, req.GetMatchId(), credential)
		if aerr != nil {
			logDSAuthReject(ctx, "ReportProgress", "authorize_result", req.GetMatchId(), "", credential.Pod, aerr)
			return &battlev1.ReportProgressResponse{Code: toProtoCode(aerr)}, nil
		}
		roster = proof.PlayerIDs
	}
	acked, err := s.uc.ReportProgress(ctx, req.GetMatchId(), roster, req.GetEvents())
	return progressResponse(acked, err), nil
}

func progressResponse(acked uint64, err error) *battlev1.ReportProgressResponse {
	if err != nil {
		// isolated consume/discard 的 durable terminal failure 已经是一个被服务端
		// 明确处理的 seq：带回 AckedSeq 让 UE 释放该 action claim，但保留本地物品。
		// 瞬时失败返回 acked=0，UE 保持同 seq/同 payload 重试。
		return &battlev1.ReportProgressResponse{Code: toProtoCode(err), AckedSeq: acked}
	}
	return &battlev1.ReportProgressResponse{Code: commonv1.ErrCode_OK, AckedSeq: acked}
}

// GetMatchResult 查询一场对局结算。
func (s *BattleResultService) GetMatchResult(ctx context.Context, req *battlev1.GetMatchResultRequest) (*battlev1.GetMatchResultResponse, error) {
	if req.GetMatchId() == 0 {
		return &battlev1.GetMatchResultResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	res, found, err := s.uc.GetMatchResult(ctx, req.GetMatchId())
	if err != nil {
		return &battlev1.GetMatchResultResponse{Code: toProtoCode(err)}, nil
	}
	if !found {
		return &battlev1.GetMatchResultResponse{Code: commonv1.ErrCode_ERR_NOT_FOUND}, nil
	}
	return &battlev1.GetMatchResultResponse{Code: commonv1.ErrCode_OK, Result: res}, nil
}

// ListPlayerHistory 倒序列出玩家战绩历史。
func (s *BattleResultService) ListPlayerHistory(ctx context.Context, req *battlev1.ListPlayerHistoryRequest) (*battlev1.ListPlayerHistoryResponse, error) {
	if req.GetPlayerId() == 0 {
		return &battlev1.ListPlayerHistoryResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	results, err := s.uc.ListPlayerHistory(ctx, req.GetPlayerId(), int(req.GetLimit()), req.GetBeforeMs())
	if err != nil {
		return &battlev1.ListPlayerHistoryResponse{Code: toProtoCode(err)}, nil
	}
	return &battlev1.ListPlayerHistoryResponse{Code: commonv1.ErrCode_OK, Results: results}, nil
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
