// battle_credential.go 实现 ReportResult 的 Redis active credential 终态门。
package service

import (
	"context"
	"crypto/subtle"
	"sort"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// BattleCredentialStateChecker 证明已验签 JWT 此刻仍等于 Redis active。
type BattleCredentialStateChecker interface {
	CheckActive(context.Context, uint64, *middleware.VerifiedCredential) error
	// AuthorizeResult 返回只能由服务端 active 快照构造的持久 terminal-release 证明。
	// authorized_at_ms 取 checker 本机校验时刻，绝不接受 DS 请求字段。
	AuthorizeResult(context.Context, uint64, *middleware.VerifiedCredential) (data.TerminalReleaseRecord, error)
	MarkResultRecorded(context.Context, uint64, *middleware.VerifiedCredential) error
}

type redisBattleCredentialStateChecker struct {
	reader                data.BattleAuthReader
	recorder              data.BattleResultRecorder
	now                   func() time.Time
	maxActiveHeartbeatAge time.Duration
}

func NewBattleCredentialStateChecker(reader data.BattleAuthReader, maxAge time.Duration) BattleCredentialStateChecker {
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	recorder, _ := reader.(data.BattleResultRecorder)
	return &redisBattleCredentialStateChecker{reader: reader, recorder: recorder, now: time.Now, maxActiveHeartbeatAge: maxAge}
}

func (c *redisBattleCredentialStateChecker) CheckActive(ctx context.Context, matchID uint64, cred *middleware.VerifiedCredential) error {
	_, err := c.AuthorizeResult(ctx, matchID, cred)
	return err
}

// logCredentialReject 在 active credential 门的每个拒绝点留一条带**枚举 reason** 的 WARN
// (§11.3 R2)。历史实现把约 25 个子条件塌成一句 "battle credential does not match active
// authority":DS 换 pod、instance_epoch 轮换、battle.state 卡在 allocating、keyset 换钥后
// kid 不匹配 —— 这些故障现象完全相同(都是 ds_auth_rejected + code=Unauthorized),排障只能
// 翻 Redis 快照人肉比对。这些码全是业务码(ErrUnauthorized/ErrUnavailable),不属
// errcode.IsServerFault,access log 走 rpc_ok(DEBUG),不在拒绝点打就是线上完全不可见。
func logCredentialReject(ctx context.Context, matchID uint64, reason string, cred *middleware.VerifiedCredential, kv ...any) {
	fields := []any{"msg", "battle_credential_mismatch", "match_id", matchID, "reason", reason}
	if cred != nil {
		fields = append(fields, "ds_pod", cred.Pod, "cred_gen", cred.Gen,
			"cred_instance_epoch", cred.ProtocolEpoch, "cred_jti", cred.JTI, "cred_kid", cred.Kid)
	}
	fields = append(fields, kv...)
	plog.With(ctx).Warnw(fields...)
}

// credentialScopeReason 枚举「令牌本身不完整 / scope 不匹配」的具体项。
// 返回 "" 表示全部通过;判定集合与拆分前逐条一致。
func credentialScopeReason(matchID uint64, cred *middleware.VerifiedCredential) string {
	switch {
	case matchID == 0:
		return "missing_match_id"
	case cred == nil:
		return "missing_credential"
	case cred.DSType != auth.DSTypeBattle:
		return "ds_type_not_battle"
	case cred.MatchID != matchID:
		return "token_match_id_mismatch"
	case cred.Pod == "":
		return "token_missing_pod"
	case cred.InstanceUID == "":
		return "token_missing_instance_uid"
	case cred.ProtocolEpoch == 0:
		return "token_missing_instance_epoch"
	case cred.Gen == 0:
		return "token_missing_gen"
	case cred.JTI == "":
		return "token_missing_jti"
	case cred.ExpMs <= 0:
		return "token_missing_exp"
	case cred.TokenSHA256 == "":
		return "token_missing_token_sha"
	case cred.Kid == "":
		return "token_missing_kid"
	case cred.WriterEpoch != auth.DSAuthWriterEpochV2:
		return "token_writer_epoch_unsupported"
	default:
		return ""
	}
}

// credentialFreshnessReason 枚举「过期 / 心跳陈旧」的具体项(历史上是一条日志都没有的
// 7 个条件的或链,过期与心跳陈旧完全分不开)。
func credentialFreshnessReason(nowMs int64, maxAge time.Duration,
	cred *middleware.VerifiedCredential, rec *dsv1.BattleDSAuthStorageRecord, active *dsv1.BattleDSCredential) string {
	switch {
	case nowMs <= 0:
		return "server_clock_invalid"
	case cred.ExpMs <= nowMs:
		return "token_expired"
	case active.GetExpMs() == 0:
		return "active_exp_unset"
	case uint64(nowMs) >= active.GetExpMs():
		return "active_expired"
	case rec.GetLastActiveHeartbeatMs() <= 0:
		return "heartbeat_missing"
	case rec.GetLastActiveHeartbeatMs() > nowMs:
		return "heartbeat_in_future"
	case nowMs-rec.GetLastActiveHeartbeatMs() > maxAge.Milliseconds():
		return "heartbeat_stale"
	default:
		return ""
	}
}

// credentialAuthorityReason 枚举「已验签令牌 ≠ Redis active 权威」的具体项。
// 条件顺序与拆分前的或链逐条一致,聚合结果(是否为 "")与原表达式等价。
func credentialAuthorityReason(matchID uint64, cred *middleware.VerifiedCredential,
	rec *dsv1.BattleDSAuthStorageRecord, battle *dsv1.BattleStorageRecord, active *dsv1.BattleDSCredential) string {
	switch {
	case rec.GetMatchId() != matchID:
		return "record_match_id_mismatch"
	case rec.GetDsPodName() != cred.Pod:
		return "record_pod_mismatch"
	case rec.GetAllocationId() == "":
		return "record_missing_allocation_id"
	case battle.GetMatchId() != matchID:
		return "battle_match_id_mismatch"
	case battle.GetAllocationId() != rec.GetAllocationId():
		return "battle_allocation_id_mismatch"
	case battle.GetDsPodName() != cred.Pod:
		return "battle_pod_mismatch"
	case battle.GetState() != "ready" && battle.GetState() != "running":
		return "battle_state_not_playable"
	case battle.GetGameserverUid() != cred.InstanceUID:
		return "battle_instance_uid_mismatch"
	case battle.GetInstanceEpoch() != cred.ProtocolEpoch:
		return "battle_instance_epoch_mismatch"
	case battle.GetLastVerifiedGen() != cred.Gen:
		return "battle_gen_mismatch"
	case battle.GetLastVerifiedJti() != cred.JTI:
		return "battle_jti_mismatch"
	case battle.GetLastVerifiedWriterEpoch() != cred.WriterEpoch:
		return "battle_writer_epoch_mismatch"
	case rec.GetInstanceUid() == "":
		return "record_missing_instance_uid"
	case rec.GetInstanceUid() != cred.InstanceUID:
		return "record_instance_uid_mismatch"
	case rec.GetInstanceEpoch() == 0:
		return "record_missing_instance_epoch"
	case rec.GetInstanceEpoch() != cred.ProtocolEpoch:
		return "record_instance_epoch_mismatch"
	case active.GetInstanceUid() != cred.InstanceUID:
		return "active_instance_uid_mismatch"
	case active.GetInstanceEpoch() != cred.ProtocolEpoch:
		return "active_instance_epoch_mismatch"
	case active.GetGen() == 0:
		return "active_missing_gen"
	case active.GetGen() != cred.Gen:
		return "active_gen_mismatch"
	case active.GetJti() == "":
		return "active_missing_jti"
	case active.GetJti() != cred.JTI:
		return "active_jti_mismatch"
	case active.GetExpMs() != uint64(cred.ExpMs):
		return "active_exp_mismatch"
	case active.GetKid() == "":
		return "active_missing_kid"
	case active.GetKid() != cred.Kid:
		return "active_kid_mismatch"
	case active.GetTokenSha256() == "":
		return "active_missing_token_sha"
	case active.GetWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "active_writer_epoch_unsupported"
	case active.GetWriterEpoch() != cred.WriterEpoch:
		return "active_writer_epoch_mismatch"
	case rec.GetRequiredWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "record_required_writer_epoch_unsupported"
	case rec.GetPending() != nil && rec.GetPending().GetWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "pending_writer_epoch_unsupported"
	case rec.GetHighWaterGen() < active.GetGen():
		return "high_water_gen_regressed"
	case subtle.ConstantTimeCompare([]byte(active.GetTokenSha256()), []byte(cred.TokenSHA256)) != 1:
		return "token_sha_mismatch"
	default:
		return ""
	}
}

func (c *redisBattleCredentialStateChecker) AuthorizeResult(
	ctx context.Context,
	matchID uint64,
	cred *middleware.VerifiedCredential,
) (data.TerminalReleaseRecord, error) {
	if reason := credentialScopeReason(matchID, cred); reason != "" {
		logCredentialReject(ctx, matchID, reason, cred)
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential is incomplete or scope mismatched")
	}
	if c == nil || c.reader == nil || c.now == nil {
		logCredentialReject(ctx, matchID, "authority_not_wired", cred,
			"hint", "checker 未接线(reader/now 为空),本进程无法证明令牌仍等于 active")
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnavailable, "battle credential authority is unavailable")
	}
	rec, battle, found, err := c.reader.GetBattleAuthority(ctx, matchID)
	if err != nil {
		logCredentialReject(ctx, matchID, "authority_read_failed", cred, "err", err)
		return data.TerminalReleaseRecord{}, errcode.NewCause(errcode.ErrUnavailable, err, "battle credential authority read failed")
	}
	if !found || rec == nil || battle == nil {
		logCredentialReject(ctx, matchID, "authority_not_found", cred,
			"found", found, "has_auth_record", rec != nil, "has_battle_record", battle != nil,
			"hint", "Redis 里没有本局 active 凭据(已回收 / 从未写入 / 换了 authority 实例)")
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential is not active")
	}
	if rec.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
		rec.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING {
		logCredentialReject(ctx, matchID, "phase_not_active", cred, "phase", rec.GetPhase().String())
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential phase is not active")
	}
	active := rec.GetActive()
	if active == nil {
		logCredentialReject(ctx, matchID, "active_credential_missing", cred, "phase", rec.GetPhase().String())
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle active credential is missing")
	}
	nowMs := c.now().UnixMilli()
	maxAge := c.maxActiveHeartbeatAge
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	if reason := credentialFreshnessReason(nowMs, maxAge, cred, rec, active); reason != "" {
		logCredentialReject(ctx, matchID, reason, cred,
			"now_ms", nowMs, "token_exp_ms", cred.ExpMs, "active_exp_ms", active.GetExpMs(),
			"last_heartbeat_ms", rec.GetLastActiveHeartbeatMs(), "max_heartbeat_age_ms", maxAge.Milliseconds())
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential expired or heartbeat stale")
	}
	if reason := credentialAuthorityReason(matchID, cred, rec, battle, active); reason != "" {
		logCredentialReject(ctx, matchID, reason, cred,
			"record_pod", rec.GetDsPodName(), "battle_pod", battle.GetDsPodName(),
			"battle_state", battle.GetState(), "allocation_id", rec.GetAllocationId(),
			"active_gen", active.GetGen(), "high_water_gen", rec.GetHighWaterGen(),
			"active_instance_epoch", active.GetInstanceEpoch(), "active_kid", active.GetKid(),
			"hint", "已验签令牌与 Redis active 权威不一致(换 pod / epoch 轮换 / 换钥 / 僵尸 DS)")
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential does not match active authority")
	}
	playerIDs, err := canonicalBattleRoster(battle.GetPlayerIds())
	if err != nil {
		logCredentialReject(ctx, matchID, "canonical_roster_invalid", cred,
			"roster_size", len(battle.GetPlayerIds()), "err", err,
			"hint", "canonical BattleStorageRecord 名单为空 / 含 0 / 有重复,本局无法结算")
		return data.TerminalReleaseRecord{}, err
	}
	return data.TerminalReleaseRecord{
		MatchID: matchID, AllocationID: rec.GetAllocationId(), DSPodName: cred.Pod,
		GameserverUID: cred.InstanceUID, InstanceEpoch: cred.ProtocolEpoch,
		AuthGen: cred.Gen, AuthJTI: cred.JTI, AuthExpMs: cred.ExpMs, AuthKid: cred.Kid,
		AuthTokenSHA256: cred.TokenSHA256, AuthWriterEpoch: cred.WriterEpoch,
		AuthorizedAtMs: nowMs, PlayerIDs: playerIDs,
		// canonical game_mode/map_id/rating_mode 与 roster 同源:取自已通过上方精确比对的
		// BattleStorageRecord 快照,不做二次 Redis 查询,也绝不用 DS 请求体补值。
		// 滚动升级前的旧记录 game_mode 可能为空、rating_mode 可能是 UNSPECIFIED,
		// biz 层按"canonical 未知"保守处理(见 settlementRunsElo)。
		GameMode: battle.GetGameMode(), MapID: battle.GetMapId(),
		RatingMode: battle.GetRatingMode(), RatingPool: battle.GetRatingPool(),
	}, nil
}

func canonicalBattleRoster(raw []uint64) ([]uint64, error) {
	if len(raw) == 0 {
		return nil, errcode.New(errcode.ErrUnauthorized, "battle authority roster is missing")
	}
	out := append([]uint64(nil), raw...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	for i, playerID := range out {
		if playerID == 0 || (i > 0 && out[i-1] == playerID) {
			return nil, errcode.New(errcode.ErrUnauthorized, "battle authority roster is invalid")
		}
	}
	return out, nil
}

func (c *redisBattleCredentialStateChecker) MarkResultRecorded(
	ctx context.Context,
	matchID uint64,
	cred *middleware.VerifiedCredential,
) error {
	if c == nil || c.recorder == nil {
		return errcode.New(errcode.ErrUnavailable, "battle result receipt writer is unavailable")
	}
	if cred == nil || matchID == 0 || cred.MatchID != matchID ||
		cred.WriterEpoch != auth.DSAuthWriterEpochV2 {
		return errcode.New(errcode.ErrUnauthorized, "battle result credential writer epoch is not supported")
	}
	return c.recorder.RecordBattleResult(ctx, data.BattleResultCredential{
		MatchID: matchID, PodName: cred.Pod, InstanceUID: cred.InstanceUID,
		InstanceEpoch: cred.ProtocolEpoch, Gen: cred.Gen, JTI: cred.JTI,
		ExpMs: cred.ExpMs, Kid: cred.Kid, TokenSHA256: cred.TokenSHA256,
		WriterEpoch: cred.WriterEpoch,
	}, c.maxActiveHeartbeatAge)
}
