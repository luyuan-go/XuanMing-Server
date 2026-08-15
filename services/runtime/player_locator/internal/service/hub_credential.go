// hub_credential.go 实现 player_locator 的 Hub DS active credential 终态门。
//
// JWT 验签只证明“令牌由受信签发方签过”；本文件再读取 Redis 唯一授权权威，证明这份
// (GameServer UID, protocol epoch, gen, jti) 凭据此刻仍是 active。任一失败都在位置/TTL/
// presence 副作用之前返回 fail-closed。
package service

import (
	"context"
	"crypto/subtle"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

// HubCredentialStateChecker 校验一份已验签 Hub 凭据是否仍为 Redis active。
type HubCredentialStateChecker interface {
	CheckActive(ctx context.Context, pod string, cred *middleware.VerifiedCredential) error
}

type redisHubCredentialStateChecker struct {
	reader                data.HubAuthReader
	now                   func() time.Time
	maxActiveHeartbeatAge time.Duration
}

// NewHubCredentialStateChecker 构造 Redis active credential 终态门。
func NewHubCredentialStateChecker(reader data.HubAuthReader, maxAge ...time.Duration) HubCredentialStateChecker {
	age := 30 * time.Second
	if len(maxAge) > 0 && maxAge[0] > 0 {
		age = maxAge[0]
	}
	return &redisHubCredentialStateChecker{reader: reader, now: time.Now, maxActiveHeartbeatAge: age}
}

// Hub 凭据终态门的拒绝 reason 枚举(§11.3 R2:一个 if 收敛 N 个条件必须拆成 N 个 reason)。
const (
	credReasonIncompleteClaims  = "credential_claims_incomplete"
	credReasonAuthorityDown     = "authority_unavailable"
	credReasonAuthorityReadFail = "authority_read_failed"
	credReasonNotActive         = "authority_record_absent"
	credReasonPhaseNotActive    = "authority_phase_not_active"
	credReasonActiveMissing     = "authority_active_missing"
	credReasonExpired           = "credential_expired"
	credReasonHeartbeatStale    = "active_heartbeat_stale"
	credReasonAuthorityMismatch = "credential_authority_mismatch"
)

// authorityMismatchReason 把最后那个「任一项不等都拒」的合取条件拆成单一枚举 reason,
// 判定顺序与 if 内完全一致(只读比较,不改变任何控制流)。
func authorityMismatchReason(pod string, rec *hubv1.HubShardAuthStorageRecord,
	active *hubv1.HubDSCredential, cred *middleware.VerifiedCredential) string {
	switch {
	case rec.GetPodName() != pod:
		return "record_pod_mismatch"
	case rec.GetInstanceUid() == "" || rec.GetInstanceUid() != cred.InstanceUID:
		return "record_instance_uid_mismatch"
	case rec.GetProtocolEpoch() == 0 || rec.GetProtocolEpoch() != cred.ProtocolEpoch:
		return "record_protocol_epoch_mismatch"
	case rec.GetRequiredWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "record_required_writer_epoch_mismatch"
	case rec.GetPending() != nil && rec.GetPending().GetWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "pending_writer_epoch_mismatch"
	case active.GetInstanceUid() == "" || active.GetInstanceUid() != cred.InstanceUID:
		return "active_instance_uid_mismatch"
	case active.GetProtocolEpoch() == 0 || active.GetProtocolEpoch() != cred.ProtocolEpoch:
		return "active_protocol_epoch_mismatch"
	case active.GetGen() == 0 || active.GetGen() != cred.Gen:
		return "active_gen_mismatch"
	case active.GetJti() == "" || active.GetJti() != cred.JTI:
		return "active_jti_mismatch"
	case active.GetExpMs() != uint64(cred.ExpMs):
		return "active_exp_mismatch"
	case active.GetKid() == "" || active.GetKid() != cred.Kid || active.GetTokenSha256() == "":
		return "active_kid_or_token_digest_missing"
	case active.GetWriterEpoch() != auth.DSAuthWriterEpochV2 || active.GetWriterEpoch() != cred.WriterEpoch:
		return "active_writer_epoch_mismatch"
	case rec.GetHighWaterGen() < active.GetGen():
		return "high_water_gen_regressed"
	default:
		return "token_digest_mismatch"
	}
}

// logCredRejected 记录一次 Hub 凭据终态门拒绝。
//
// 为什么必须在这里打:CheckActive 返回的是 ErrUnauthorized / ErrUnavailable,
// 调用方转成 in-band Code 后 handler 返回 nil error → access log 只记 DEBUG。
// 于是「Hub DS 明明在跑,写位置却全被拒」在线上零日志,现象只有玩家在大厅里查不到。
// 频次:稳态恒 0;凭据轮转窗口内短暂出现,属预期。
func logCredRejected(ctx context.Context, reason, pod string, cred *middleware.VerifiedCredential, extra ...any) {
	kvs := []any{"msg", "hub_credential_rejected", "reason", reason, "hub_pod", pod}
	if cred != nil {
		kvs = append(kvs,
			"req_instance_uid", cred.InstanceUID, "req_protocol_epoch", cred.ProtocolEpoch,
			"req_gen", cred.Gen, "req_jti", cred.JTI, "req_writer_epoch", cred.WriterEpoch,
			"req_kid", cred.Kid, "req_exp_ms", cred.ExpMs, "req_pod", cred.Pod)
	}
	plog.With(ctx).Warnw(append(kvs, extra...)...)
}

func (c *redisHubCredentialStateChecker) CheckActive(ctx context.Context, pod string, cred *middleware.VerifiedCredential) error {
	// Model B 下 legacy/不完整凭据绝不回退放行。JWT exp 虽已由 verifier 校验，这里仍将
	// claim exp 与 Redis active.exp_ms 精确绑定，避免 annotation/外部数字参与授权。
	if pod == "" || cred == nil || cred.Pod != pod || cred.InstanceUID == "" ||
		cred.ProtocolEpoch == 0 || cred.Gen == 0 || cred.JTI == "" || cred.ExpMs <= 0 ||
		cred.TokenSHA256 == "" || cred.Kid == "" || cred.WriterEpoch != auth.DSAuthWriterEpochV2 {
		logCredRejected(ctx, credReasonIncompleteClaims, pod, cred,
			"required_writer_epoch", auth.DSAuthWriterEpochV2)
		return errcode.New(errcode.ErrUnauthorized, "hub credential is incomplete or scope mismatched")
	}
	if c == nil || c.reader == nil || c.now == nil {
		logCredRejected(ctx, credReasonAuthorityDown, pod, cred)
		return errcode.New(errcode.ErrUnavailable, "hub credential authority is unavailable")
	}

	rec, found, err := c.reader.GetHubAuth(ctx, pod)
	if err != nil {
		logCredRejected(ctx, credReasonAuthorityReadFail, pod, cred, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err, "hub credential authority read failed")
	}
	if !found || rec == nil {
		logCredRejected(ctx, credReasonNotActive, pod, cred, "found", found)
		return errcode.New(errcode.ErrUnauthorized, "hub credential is not active")
	}

	// ROTATING 表示 active+pending 并存；旧 active 在 pending 被激活前仍是权威，必须继续
	// 可用以保证零停机。其余 phase 都没有可用于普通写 RPC 的 active 权限。
	if rec.GetPhase() != hubv1.HubAuthPhase_HUB_AUTH_PHASE_ACTIVE &&
		rec.GetPhase() != hubv1.HubAuthPhase_HUB_AUTH_PHASE_ROTATING {
		logCredRejected(ctx, credReasonPhaseNotActive, pod, cred, "cur_phase", int32(rec.GetPhase()))
		return errcode.New(errcode.ErrUnauthorized, "hub credential phase is not active")
	}
	active := rec.GetActive()
	if active == nil {
		logCredRejected(ctx, credReasonActiveMissing, pod, cred, "cur_phase", int32(rec.GetPhase()))
		return errcode.New(errcode.ErrUnauthorized, "hub credential active record is missing")
	}

	nowMs := c.now().UnixMilli()
	if nowMs <= 0 || cred.ExpMs <= nowMs || active.GetExpMs() == 0 || uint64(nowMs) >= active.GetExpMs() {
		logCredRejected(ctx, credReasonExpired, pod, cred,
			"now_ms", nowMs, "cur_active_exp_ms", active.GetExpMs())
		return errcode.New(errcode.ErrUnauthorized, "hub credential has expired")
	}
	lastHeartbeatMs := rec.GetLastActiveHeartbeatMs()
	maxHeartbeatAge := c.maxActiveHeartbeatAge
	if maxHeartbeatAge <= 0 {
		maxHeartbeatAge = 30 * time.Second
	}
	if lastHeartbeatMs <= 0 || lastHeartbeatMs > nowMs ||
		nowMs-lastHeartbeatMs > maxHeartbeatAge.Milliseconds() {
		logCredRejected(ctx, credReasonHeartbeatStale, pod, cred,
			"now_ms", nowMs, "last_active_heartbeat_ms", lastHeartbeatMs,
			"max_heartbeat_age_ms", maxHeartbeatAge.Milliseconds())
		return errcode.New(errcode.ErrUnauthorized, "hub credential active heartbeat is not fresh")
	}

	// record 顶层实例身份、active 内嵌身份和 JWT claims 三者必须完全一致。high-water
	// 小于 active.gen 表示权威记录自身不完整/回退，同样 fail-closed。
	if rec.GetPodName() != pod ||
		rec.GetInstanceUid() == "" || rec.GetInstanceUid() != cred.InstanceUID ||
		rec.GetProtocolEpoch() == 0 || rec.GetProtocolEpoch() != cred.ProtocolEpoch ||
		rec.GetRequiredWriterEpoch() != auth.DSAuthWriterEpochV2 ||
		(rec.GetPending() != nil && rec.GetPending().GetWriterEpoch() != auth.DSAuthWriterEpochV2) ||
		active.GetInstanceUid() == "" || active.GetInstanceUid() != cred.InstanceUID ||
		active.GetProtocolEpoch() == 0 || active.GetProtocolEpoch() != cred.ProtocolEpoch ||
		active.GetGen() == 0 || active.GetGen() != cred.Gen ||
		active.GetJti() == "" || active.GetJti() != cred.JTI ||
		active.GetExpMs() != uint64(cred.ExpMs) ||
		active.GetKid() == "" || active.GetKid() != cred.Kid || active.GetTokenSha256() == "" ||
		active.GetWriterEpoch() != auth.DSAuthWriterEpochV2 || active.GetWriterEpoch() != cred.WriterEpoch ||
		rec.GetHighWaterGen() < active.GetGen() ||
		subtle.ConstantTimeCompare([]byte(active.GetTokenSha256()), []byte(cred.TokenSHA256)) != 1 {
		// 期望值(Redis active 权威)与实际值(JWT claims)并排打出:只报「不匹配」
		// 查不出是哪一代凭据在写、权威此刻认的是哪一代 —— 而那正是排查
		// 「旧 Hub 实例的迟到写为什么被 fencing 掉」需要的两个数。
		// token_sha256 只打是否相等,不打摘要本身。
		logCredRejected(ctx, credReasonAuthorityMismatch, pod, cred,
			"detail_reason", authorityMismatchReason(pod, rec, active, cred),
			"cur_pod", rec.GetPodName(), "cur_instance_uid", rec.GetInstanceUid(),
			"cur_protocol_epoch", rec.GetProtocolEpoch(),
			"cur_required_writer_epoch", rec.GetRequiredWriterEpoch(),
			"cur_high_water_gen", rec.GetHighWaterGen(),
			"cur_active_instance_uid", active.GetInstanceUid(),
			"cur_active_protocol_epoch", active.GetProtocolEpoch(),
			"cur_active_gen", active.GetGen(), "cur_active_jti", active.GetJti(),
			"cur_active_kid", active.GetKid(), "cur_active_exp_ms", active.GetExpMs(),
			"cur_active_writer_epoch", active.GetWriterEpoch(),
			"token_digest_match", subtle.ConstantTimeCompare(
				[]byte(active.GetTokenSha256()), []byte(cred.TokenSHA256)) == 1)
		return errcode.New(errcode.ErrUnauthorized, "hub credential does not match active authority")
	}
	return nil
}
