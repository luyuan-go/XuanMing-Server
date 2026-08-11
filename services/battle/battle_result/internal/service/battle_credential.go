// battle_credential.go 实现 ReportResult 的 Redis active credential 终态门。
package service

import (
	"context"
	"crypto/subtle"
	"sort"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
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

func (c *redisBattleCredentialStateChecker) AuthorizeResult(
	ctx context.Context,
	matchID uint64,
	cred *middleware.VerifiedCredential,
) (data.TerminalReleaseRecord, error) {
	if matchID == 0 || cred == nil || cred.DSType != auth.DSTypeBattle || cred.MatchID != matchID ||
		cred.Pod == "" || cred.InstanceUID == "" || cred.ProtocolEpoch == 0 || cred.Gen == 0 ||
		cred.JTI == "" || cred.ExpMs <= 0 || cred.TokenSHA256 == "" || cred.Kid == "" ||
		cred.WriterEpoch != auth.DSAuthWriterEpochV2 {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential is incomplete or scope mismatched")
	}
	if c == nil || c.reader == nil || c.now == nil {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnavailable, "battle credential authority is unavailable")
	}
	rec, battle, found, err := c.reader.GetBattleAuthority(ctx, matchID)
	if err != nil {
		return data.TerminalReleaseRecord{}, errcode.NewCause(errcode.ErrUnavailable, err, "battle credential authority read failed")
	}
	if !found || rec == nil || battle == nil {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential is not active")
	}
	if rec.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
		rec.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential phase is not active")
	}
	active := rec.GetActive()
	if active == nil {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle active credential is missing")
	}
	nowMs := c.now().UnixMilli()
	maxAge := c.maxActiveHeartbeatAge
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	if nowMs <= 0 || cred.ExpMs <= nowMs || active.GetExpMs() == 0 || uint64(nowMs) >= active.GetExpMs() ||
		rec.GetLastActiveHeartbeatMs() <= 0 || rec.GetLastActiveHeartbeatMs() > nowMs ||
		nowMs-rec.GetLastActiveHeartbeatMs() > maxAge.Milliseconds() {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential expired or heartbeat stale")
	}
	if rec.GetMatchId() != matchID || rec.GetDsPodName() != cred.Pod || rec.GetAllocationId() == "" ||
		battle.GetMatchId() != matchID || battle.GetAllocationId() != rec.GetAllocationId() ||
		battle.GetDsPodName() != cred.Pod || (battle.GetState() != "ready" && battle.GetState() != "running") ||
		battle.GetGameserverUid() != cred.InstanceUID || battle.GetInstanceEpoch() != cred.ProtocolEpoch ||
		battle.GetLastVerifiedGen() != cred.Gen || battle.GetLastVerifiedJti() != cred.JTI ||
		battle.GetLastVerifiedWriterEpoch() != cred.WriterEpoch ||
		rec.GetInstanceUid() == "" || rec.GetInstanceUid() != cred.InstanceUID ||
		rec.GetInstanceEpoch() == 0 || rec.GetInstanceEpoch() != cred.ProtocolEpoch ||
		active.GetInstanceUid() != cred.InstanceUID || active.GetInstanceEpoch() != cred.ProtocolEpoch ||
		active.GetGen() == 0 || active.GetGen() != cred.Gen || active.GetJti() == "" || active.GetJti() != cred.JTI ||
		active.GetExpMs() != uint64(cred.ExpMs) || active.GetKid() == "" || active.GetKid() != cred.Kid ||
		active.GetTokenSha256() == "" || active.GetWriterEpoch() != auth.DSAuthWriterEpochV2 ||
		active.GetWriterEpoch() != cred.WriterEpoch ||
		rec.GetRequiredWriterEpoch() != auth.DSAuthWriterEpochV2 ||
		(rec.GetPending() != nil && rec.GetPending().GetWriterEpoch() != auth.DSAuthWriterEpochV2) ||
		rec.GetHighWaterGen() < active.GetGen() ||
		subtle.ConstantTimeCompare([]byte(active.GetTokenSha256()), []byte(cred.TokenSHA256)) != 1 {
		return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "battle credential does not match active authority")
	}
	playerIDs, err := canonicalBattleRoster(battle.GetPlayerIds())
	if err != nil {
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
		RatingMode: battle.GetRatingMode(),
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
