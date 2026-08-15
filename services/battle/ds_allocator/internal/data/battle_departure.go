package data

// Battle→Hub 物理离场日志。
//
// 这里刻意不使用 TTL 与 presence 推断：
//   - pending order 只能被 credential-bound Battle DS 的完整 active-player
//     snapshot 提交；
//   - 或者外部 GameServer UID 条件回收明确成功后的 teardown proof 提交。
//
// journal / teardown / battle / auth 都使用 {match_id} hashtag，因此 Redis Cluster
// 下的 WATCH/MULTI 仍是同 slot 事务。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

const (
	battleDepartureCASRetries = 64
	maxBattleDepartureOrders  = 1024
	// BattleDepartureTerminalRetention bounds completed physical proofs without
	// turning TTL into an authorization decision. Pending/unknown journals stay
	// permanent; only an exact UID teardown proof and an all-terminal journal
	// receive this delayed seven-day retention window.
	BattleDepartureTerminalRetention = 7 * 24 * time.Hour
	// BattlePlayerCensusCapabilityVersionV1 要求 DS 快照覆盖所有
	// admission owner，而不是仅 PostLogin ActivePlayers。
	BattlePlayerCensusCapabilityVersionV1 uint32 = 1
)

// 离场日志的拒绝原因枚举。离场是 Battle→Hub 回流的唯一物理证明，任何一条拒绝都会
// 让玩家停在“旧 DS 还没放手”的等待里，因此每个 early return 都必须能按 reason 检索。
const (
	// EnsurePlayerDeparture（Hub 侧申请离场证明）。
	departureRejectExpectedIncomplete    = "departure_expected_incomplete"
	departureRejectTeardownTupleConflict = "teardown_proof_tuple_conflict"
	departureRejectIdempotencyConflict   = "departure_idempotency_tuple_conflict"
	departureRejectJournalFull           = "departure_journal_full"
	departureRejectSourceMissing         = "battle_source_missing_without_teardown_proof"
	departureRejectSourceTupleMismatch   = "battle_source_tuple_mismatch"
	departureRejectPlayerNotInRoster     = "player_not_in_authoritative_roster"
	departureRejectCASExhausted          = "departure_cas_retry_exhausted"
	// ReconcilePlayerDepartures(DS 心跳上报 census 时对账)。
	departureRejectHeartbeatSourceIncomplete   = "heartbeat_source_incomplete"
	departureRejectCensusZeroPlayer            = "census_contains_zero_player"
	departureRejectCensusDuplicatePlayer       = "census_contains_duplicate_player"
	departureRejectAckIDEmpty                  = "acknowledged_departure_id_empty"
	departureRejectAckIDDuplicate              = "acknowledged_departure_id_duplicate"
	departureRejectCensusCapabilityTooOld      = "census_capability_or_id_missing"
	departureRejectCensusPayloadWithoutPresent = "census_payload_without_present"
	departureRejectAckBeforeIssue              = "ack_before_order_issued"
	departureRejectAckWhileActive              = "ack_while_player_still_active"
	departureRejectAckUnknownID                = "ack_unknown_departure_id"
	departureRejectReconcileCASExhausted       = "departure_reconcile_cas_retry_exhausted"
	// RecordInstanceTeardown(整实例回收证明)。
	departureRejectTeardownIncomplete   = "teardown_source_incomplete"
	departureRejectTeardownCASExhausted = "teardown_proof_cas_retry_exhausted"
)

// departureRejected 统一带齐 R3 要求的实例 join key，避免每个 early return 手抄四个字段。
func departureRejected(
	ctx context.Context, msg string, matchID uint64,
	source BattleDepartureSource, reason string, kv ...any,
) {
	fields := []any{
		"msg", msg, "reason", reason, "match_id", matchID,
		"ds_pod", source.DSPodName, "ds_uid", source.GameServerUID,
		"ds_instance_epoch", source.InstanceEpoch, "allocation_id", source.AllocationID,
	}
	plog.With(ctx).Warnw(append(fields, kv...)...)
}

func battleDepartureJournalKey(matchID uint64) string {
	return fmt.Sprintf("pandora:ds:departures:{%d}", matchID)
}

func battleInstanceTeardownKey(matchID uint64, source BattleDepartureSource) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s",
		source.DSPodName, source.GameServerUID, source.InstanceEpoch, source.AllocationID)))
	return fmt.Sprintf("pandora:ds:teardown:{%d}:%s", matchID, hex.EncodeToString(sum[:16]))
}

// BattleDepartureSource 是 placement/source snapshot 与 Battle Redis authority 都必须精确
// 匹配的物理实例栅栏。
type BattleDepartureSource struct {
	DSPodName     string
	GameServerUID string
	InstanceEpoch uint32
	AllocationID  string
	// PodUID 只由 allocator 在分配时从 K8s 权威 GET 捕获，
	// heartbeat/Ensure 不信任调用方传入该值。
	PodUID string
}

func (s BattleDepartureSource) valid() bool {
	return s.DSPodName != "" && s.GameServerUID != "" && s.InstanceEpoch > 0 && s.AllocationID != ""
}

// BattlePlayerDepartureExpected 是 EnsurePlayerDeparture 的完整幂等输入。
type BattlePlayerDepartureExpected struct {
	MatchID  uint64
	PlayerID uint64
	// PlacementVersion/OperationID 是 Begin 后当前 PENDING->HUB 代际。
	PlacementVersion uint64
	OperationID      string
	// Source* 是 Begin 原子捕获的 STABLE BATTLE claims。
	SourcePlacementVersion uint64
	SourceOperationID      string
	Source                 BattleDepartureSource
}

// BattlePlayerDepartureResult 区分心跳离场与整个 UID teardown，便于观测但
// 两者都是 Hub ticket/admission 可接受的物理证明。
type BattlePlayerDepartureResult struct {
	Departed    bool
	Status      dsv1.BattlePlayerDepartureStatus
	DepartureID string
}

func stableDepartureID(in BattlePlayerDepartureExpected) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%d\x00%d\x00%s\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s",
		in.MatchID, in.PlayerID, in.PlacementVersion, in.OperationID,
		in.SourcePlacementVersion, in.SourceOperationID, in.Source.DSPodName,
		in.Source.GameServerUID, in.Source.InstanceEpoch, in.Source.AllocationID)))
	return hex.EncodeToString(sum[:16])
}

func departureSourceEqualsRecord(source BattleDepartureSource, record *dsv1.BattlePlayerDepartureStorageRecord) bool {
	return record != nil && record.GetDsPodName() == source.DSPodName &&
		record.GetGameserverUid() == source.GameServerUID &&
		record.GetInstanceEpoch() == source.InstanceEpoch &&
		record.GetAllocationId() == source.AllocationID
}

func departureSourceEqualsBattle(source BattleDepartureSource, battle *dsv1.BattleStorageRecord) bool {
	return battle != nil && battle.GetDsPodName() == source.DSPodName &&
		battle.GetGameserverUid() == source.GameServerUID &&
		battle.GetInstanceEpoch() == source.InstanceEpoch &&
		battle.GetAllocationId() == source.AllocationID
}

func departureSourceEqualsTeardown(source BattleDepartureSource, proof *dsv1.BattleInstanceTeardownStorageRecord) bool {
	return proof != nil && proof.GetDsPodName() == source.DSPodName &&
		proof.GetGameserverUid() == source.GameServerUID &&
		proof.GetInstanceEpoch() == source.InstanceEpoch &&
		proof.GetAllocationId() == source.AllocationID && proof.GetPodUid() != "" &&
		(source.PodUID == "" || proof.GetPodUid() == source.PodUID)
}

func departureSourceEqualsSource(a, b BattleDepartureSource) bool {
	return a.DSPodName == b.DSPodName && a.GameServerUID == b.GameServerUID &&
		a.InstanceEpoch == b.InstanceEpoch && a.AllocationID == b.AllocationID && a.PodUID == b.PodUID
}

func departureStatusTerminal(status dsv1.BattlePlayerDepartureStatus) bool {
	return status == dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED ||
		status == dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
}

func departureJournalTerminal(journal *dsv1.BattlePlayerDepartureJournalStorageRecord) bool {
	if journal == nil {
		return false
	}
	for _, order := range journal.GetDepartures() {
		if order == nil || !departureStatusTerminal(order.GetStatus()) {
			return false
		}
	}
	return true
}

// teardownProofRemainingRetention returns the non-renewable storage window
// anchored by the first durable teardown write. Legacy TTL=0 proofs are
// repaired once to the fixed window; already-bounded proofs keep only their
// remaining time, so retries cannot keep completed matches alive forever.
func teardownProofRemainingRetention(
	ctx context.Context, tx redis.Cmdable, key string, found bool,
) (remaining time.Duration, repairLegacy bool, err error) {
	if !found {
		return 0, false, nil
	}
	ttl, err := tx.PTTL(ctx, key).Result()
	if err != nil {
		return 0, false, err
	}
	if ttl > 0 {
		if ttl > BattleDepartureTerminalRetention {
			ttl = BattleDepartureTerminalRetention
		}
		return ttl, false, nil
	}
	// Redis PTTL uses -1 for a persistent key and -2 for an absent key. The
	// latter means the proof expired during this WATCH attempt and must retry.
	if ttl == -time.Nanosecond {
		return BattleDepartureTerminalRetention, true, nil
	}
	return 0, false, redis.TxFailedErr
}

func battleContainsPlayer(battle *dsv1.BattleStorageRecord, playerID uint64) bool {
	for _, candidate := range battle.GetPlayerIds() {
		if candidate == playerID {
			return true
		}
	}
	return false
}

func readDepartureJournal(ctx context.Context, tx redis.Cmdable, matchID uint64) (*dsv1.BattlePlayerDepartureJournalStorageRecord, error) {
	payload, err := tx.Get(ctx, battleDepartureJournalKey(matchID)).Bytes()
	if err == redis.Nil {
		return &dsv1.BattlePlayerDepartureJournalStorageRecord{MatchId: matchID}, nil
	}
	if err != nil {
		return nil, err
	}
	journal := &dsv1.BattlePlayerDepartureJournalStorageRecord{}
	if err := proto.Unmarshal(payload, journal); err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err,
			"decode battle departure journal %d", matchID)
	}
	if journal.GetMatchId() != matchID {
		return nil, errcode.New(errcode.ErrInvalidState,
			"battle departure journal match mismatch: got %d want %d", journal.GetMatchId(), matchID)
	}
	return journal, nil
}

func readTeardownProof(ctx context.Context, tx redis.Cmdable, matchID uint64, source BattleDepartureSource) (*dsv1.BattleInstanceTeardownStorageRecord, bool, error) {
	payload, err := tx.Get(ctx, battleInstanceTeardownKey(matchID, source)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	proof := &dsv1.BattleInstanceTeardownStorageRecord{}
	if err := proto.Unmarshal(payload, proof); err != nil {
		return nil, false, errcode.NewCause(errcode.ErrInternal, err,
			"decode battle teardown proof %d", matchID)
	}
	if proof.GetMatchId() != matchID {
		return nil, false, errcode.New(errcode.ErrInvalidState,
			"battle teardown proof match mismatch: got %d want %d", proof.GetMatchId(), matchID)
	}
	return proof, true, nil
}

func marshalDepartureMessage(message proto.Message) ([]byte, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal battle departure record")
	}
	return payload, nil
}

// EnsurePlayerDeparture 只创建/查询 exact pending order。battle key 缺失并不是
// departure proof；没有 durable teardown 时必须返回可重试 unavailable。
func (r *RedisBattleRepo) EnsurePlayerDeparture(
	ctx context.Context,
	expected BattlePlayerDepartureExpected,
) (BattlePlayerDepartureResult, error) {
	if expected.MatchID == 0 || expected.PlayerID == 0 || expected.PlacementVersion == 0 ||
		expected.OperationID == "" || expected.SourcePlacementVersion == 0 ||
		expected.SourceOperationID == "" || !expected.Source.valid() {
		return BattlePlayerDepartureResult{}, errcode.New(errcode.ErrInvalidArg,
			"complete battle departure operation and source tuple required")
	}
	departureID := stableDepartureID(expected)
	jKey := battleDepartureJournalKey(expected.MatchID)
	tKey := battleInstanceTeardownKey(expected.MatchID, expected.Source)
	bKey := battleKey(expected.MatchID)

	for attempt := 0; attempt < battleDepartureCASRetries; attempt++ {
		result := BattlePlayerDepartureResult{DepartureID: departureID}
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			journal, err := readDepartureJournal(ctx, tx, expected.MatchID)
			if err != nil {
				return err
			}
			proof, proofFound, err := readTeardownProof(ctx, tx, expected.MatchID, expected.Source)
			if err != nil {
				return err
			}
			if proofFound && !departureSourceEqualsTeardown(expected.Source, proof) {
				return errcode.New(errcode.ErrInvalidState,
					"battle %d teardown proof tuple conflict", expected.MatchID)
			}
			proofRetention, repairProofTTL, err := teardownProofRemainingRetention(
				ctx, tx, tKey, proofFound)
			if err != nil {
				return err
			}

			var existing *dsv1.BattlePlayerDepartureStorageRecord
			for _, order := range journal.GetDepartures() {
				if order.GetDepartureId() == departureID {
					existing = order
					break
				}
			}
			if existing != nil {
				if existing.GetMatchId() != expected.MatchID || existing.GetPlayerId() != expected.PlayerID ||
					existing.GetOperationId() != expected.OperationID ||
					existing.GetPlacementVersion() != expected.PlacementVersion ||
					existing.GetSourceOperationId() != expected.SourceOperationID ||
					existing.GetSourcePlacementVersion() != expected.SourcePlacementVersion ||
					!departureSourceEqualsRecord(expected.Source, existing) {
					return errcode.New(errcode.ErrInvalidState,
						"battle departure idempotency tuple conflict")
				}
				if departureStatusTerminal(existing.GetStatus()) {
					result.Departed, result.Status = true, existing.GetStatus()
					if proofFound && departureSourceEqualsTeardown(expected.Source, proof) &&
						departureJournalTerminal(journal) {
						_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
							pipe.Expire(ctx, jKey, proofRetention)
							if repairProofTTL {
								pipe.Expire(ctx, tKey, BattleDepartureTerminalRetention)
							}
							return nil
						})
						return err
					}
					return nil
				}
				if proofFound && departureSourceEqualsTeardown(expected.Source, proof) {
					existing.Status = dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
					existing.DepartedAtMs = proof.GetTornDownAtMs()
					payload, err := marshalDepartureMessage(journal)
					if err != nil {
						return err
					}
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						ttl := time.Duration(0)
						if departureJournalTerminal(journal) {
							ttl = proofRetention
						}
						pipe.Set(ctx, jKey, payload, ttl)
						if repairProofTTL {
							pipe.Expire(ctx, tKey, BattleDepartureTerminalRetention)
						}
						return nil
					})
					if err == nil {
						result.Departed, result.Status = true, existing.GetStatus()
					}
					return err
				}
				result.Status = dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_PENDING
				return nil
			}

			if len(journal.GetDepartures()) >= maxBattleDepartureOrders {
				return errcode.New(errcode.ErrRateLimited,
					"battle %d departure journal full", expected.MatchID)
			}
			nowMs := time.Now().UnixMilli()
			order := &dsv1.BattlePlayerDepartureStorageRecord{
				DepartureId: departureID, MatchId: expected.MatchID, PlayerId: expected.PlayerID,
				PlacementVersion:       expected.PlacementVersion,
				OperationId:            expected.OperationID,
				SourcePlacementVersion: expected.SourcePlacementVersion,
				SourceOperationId:      expected.SourceOperationID,
				DsPodName:              expected.Source.DSPodName,
				GameserverUid:          expected.Source.GameServerUID, InstanceEpoch: expected.Source.InstanceEpoch,
				AllocationId: expected.Source.AllocationID, RequestedAtMs: nowMs,
				Status: dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_PENDING,
			}
			if proofFound && departureSourceEqualsTeardown(expected.Source, proof) {
				order.Status = dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
				order.DepartedAtMs = proof.GetTornDownAtMs()
				result.Departed, result.Status = true, order.GetStatus()
			} else {
				battlePayload, getErr := tx.Get(ctx, bKey).Bytes()
				if getErr == redis.Nil {
					return errcode.New(errcode.ErrUnavailable,
						"battle %d source missing without exact teardown proof", expected.MatchID)
				}
				if getErr != nil {
					return getErr
				}
				battle, decodeErr := unmarshalBattle(expected.MatchID, battlePayload)
				if decodeErr != nil {
					return decodeErr
				}
				if !departureSourceEqualsBattle(expected.Source, battle) {
					return errcode.New(errcode.ErrInvalidState,
						"battle %d source tuple mismatch", expected.MatchID)
				}
				if !battleContainsPlayer(battle, expected.PlayerID) {
					return errcode.New(errcode.ErrInvalidState,
						"player %d not in battle %d authoritative roster", expected.PlayerID, expected.MatchID)
				}
				result.Status = order.GetStatus()
			}
			journal.Departures = append(journal.Departures, order)
			payload, err := marshalDepartureMessage(journal)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				ttl := time.Duration(0)
				if proofFound && departureSourceEqualsTeardown(expected.Source, proof) &&
					departureJournalTerminal(journal) {
					ttl = proofRetention
				}
				pipe.Set(ctx, jKey, payload, ttl)
				if repairProofTTL {
					pipe.Expire(ctx, tKey, BattleDepartureTerminalRetention)
				}
				return nil
			})
			return err
		}, jKey, tKey, bKey)
		if err == redis.TxFailedErr {
			continue
		}
		return result, err
	}
	return BattlePlayerDepartureResult{}, errcode.New(errcode.ErrDSAllocationFailed,
		"battle %d departure CAS retry exhausted", expected.MatchID)
}

// ReconcilePlayerDepartures 只在 Heartbeat 已通过 active/pending credential 验证后调用。
func (r *RedisBattleRepo) ReconcilePlayerDepartures(
	ctx context.Context,
	matchID uint64,
	source BattleDepartureSource,
	snapshotPresent bool,
	censusCapabilityVersion uint32,
	censusID string,
	activePlayerIDs []uint64,
	acknowledgedDepartureIDs []string,
) ([]*dsv1.BattleEvictionOrder, error) {
	if matchID == 0 || !source.valid() {
		return nil, errcode.New(errcode.ErrInvalidArg, "complete battle heartbeat source tuple required")
	}
	active := make(map[uint64]struct{}, len(activePlayerIDs))
	for _, playerID := range activePlayerIDs {
		if playerID == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "active player snapshot contains zero player")
		}
		if _, duplicate := active[playerID]; duplicate {
			return nil, errcode.New(errcode.ErrInvalidArg, "active player snapshot contains duplicate player %d", playerID)
		}
		active[playerID] = struct{}{}
	}
	acked := make(map[string]struct{}, len(acknowledgedDepartureIDs))
	for _, departureID := range acknowledgedDepartureIDs {
		if departureID == "" {
			return nil, errcode.New(errcode.ErrInvalidArg, "empty acknowledged departure id")
		}
		if _, duplicate := acked[departureID]; duplicate {
			return nil, errcode.New(errcode.ErrInvalidArg, "duplicate acknowledged departure id")
		}
		acked[departureID] = struct{}{}
	}
	if snapshotPresent && (censusCapabilityVersion < BattlePlayerCensusCapabilityVersionV1 || censusID == "") {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"complete battle player census requires capability_version>=1 and census_id")
	}
	if !snapshotPresent && (len(active) != 0 || len(acked) != 0 ||
		censusCapabilityVersion != 0 || censusID != "") {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"battle active player snapshot payload requires present=true")
	}

	jKey := battleDepartureJournalKey(matchID)
	for attempt := 0; attempt < battleDepartureCASRetries; attempt++ {
		var orders []*dsv1.BattleEvictionOrder
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			orders = nil
			journal, err := readDepartureJournal(ctx, tx, matchID)
			if err != nil {
				return err
			}
			changed := false
			knownAcks := make(map[string]struct{}, len(acked))
			nowMs := time.Now().UnixMilli()
			for _, order := range journal.GetDepartures() {
				if order.GetMatchId() != matchID || !departureSourceEqualsRecord(source, order) {
					continue
				}
				acknowledgedBeforeThisCensus := order.GetAcknowledgedAtMs() > 0
				if _, present := acked[order.GetDepartureId()]; present {
					knownAcks[order.GetDepartureId()] = struct{}{}
					if order.GetIssuedAtMs() == 0 {
						return errcode.New(errcode.ErrInvalidState,
							"departure %s acknowledged before an order was issued", order.GetDepartureId())
					}
					if _, stillActive := active[order.GetPlayerId()]; stillActive {
						return errcode.New(errcode.ErrInvalidState,
							"departure %s acknowledged while player %d remains active",
							order.GetDepartureId(), order.GetPlayerId())
					}
					if !acknowledgedBeforeThisCensus {
						order.AcknowledgedAtMs = nowMs
						order.AcknowledgedCensusId = censusID
						changed = true
					}
				}
				if order.GetStatus() != dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_PENDING {
					continue
				}
				if snapshotPresent && acknowledgedBeforeThisCensus &&
					order.GetAcknowledgedCensusId() != censusID {
					if _, stillActive := active[order.GetPlayerId()]; !stillActive {
						order.Status = dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED
						order.DepartedAtMs = nowMs
						changed = true
						continue
					}
				}
				if order.GetIssuedAtMs() == 0 {
					order.IssuedAtMs = nowMs
					changed = true
				}
				orders = append(orders, &dsv1.BattleEvictionOrder{
					DepartureId: order.GetDepartureId(), MatchId: order.GetMatchId(),
					PlayerId: order.GetPlayerId(), DsPodName: order.GetDsPodName(),
					GameserverUid: order.GetGameserverUid(), InstanceEpoch: order.GetInstanceEpoch(),
					AllocationId: order.GetAllocationId(), PlacementVersion: order.GetSourcePlacementVersion(),
					OperationId: order.GetSourceOperationId(),
				})
			}
			if snapshotPresent && len(knownAcks) != len(acked) {
				return errcode.New(errcode.ErrInvalidArg, "heartbeat acknowledged unknown departure id")
			}
			if !changed {
				return nil
			}
			payload, err := marshalDepartureMessage(journal)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, jKey, payload, 0)
				return nil
			})
			return err
		}, jKey)
		if err == redis.TxFailedErr {
			continue
		}
		return orders, err
	}
	return nil, errcode.New(errcode.ErrDSAllocationFailed,
		"battle %d departure reconcile CAS retry exhausted", matchID)
}

// RecordInstanceTeardown 必须在 ReleaseExpected 已明确成功之后调用。它先写
// durable proof 再允许上层 purge battle/auth；若 Redis 回包不确定，上层保留
// release fence 并重试 UID 条件回收，404 幂等成功后可重建证明。
func (r *RedisBattleRepo) RecordInstanceTeardown(
	ctx context.Context,
	matchID uint64,
	source BattleDepartureSource,
) error {
	if matchID == 0 || !source.valid() || source.PodUID == "" {
		return errcode.New(errcode.ErrInvalidArg, "complete battle teardown source tuple required")
	}
	jKey := battleDepartureJournalKey(matchID)
	tKey := battleInstanceTeardownKey(matchID, source)
	for attempt := 0; attempt < battleDepartureCASRetries; attempt++ {
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			journal, err := readDepartureJournal(ctx, tx, matchID)
			if err != nil {
				return err
			}
			existing, found, err := readTeardownProof(ctx, tx, matchID, source)
			if err != nil {
				return err
			}
			if found && !departureSourceEqualsTeardown(source, existing) {
				return errcode.New(errcode.ErrInvalidState,
					"battle %d teardown proof tuple conflict", matchID)
			}
			proofRetention, repairProofTTL, err := teardownProofRemainingRetention(ctx, tx, tKey, found)
			if err != nil {
				return err
			}
			if !found {
				proofRetention = BattleDepartureTerminalRetention
			}
			nowMs := time.Now().UnixMilli()
			proof := existing
			if proof == nil {
				proof = &dsv1.BattleInstanceTeardownStorageRecord{
					MatchId: matchID, DsPodName: source.DSPodName, GameserverUid: source.GameServerUID,
					InstanceEpoch: source.InstanceEpoch, AllocationId: source.AllocationID,
					TornDownAtMs: nowMs, PodUid: source.PodUID,
				}
			}
			journalChanged := false
			for _, order := range journal.GetDepartures() {
				if order.GetStatus() == dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_PENDING &&
					departureSourceEqualsRecord(source, order) {
					order.Status = dsv1.BattlePlayerDepartureStatus_BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
					order.DepartedAtMs = proof.GetTornDownAtMs()
					journalChanged = true
				}
			}
			proofPayload, err := marshalDepartureMessage(proof)
			if err != nil {
				return err
			}
			var journalPayload []byte
			if journalChanged {
				journalPayload, err = marshalDepartureMessage(journal)
				if err != nil {
					return err
				}
			}
			journalTerminal := departureJournalTerminal(journal)
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				// A teardown proof is authoritative because ReleaseExpected already
				// succeeded, not because it has a TTL. Rewriting an old permanent
				// proof here also repairs pre-retention deployments on idempotent retry.
				if found {
					pipe.Set(ctx, tKey, proofPayload, redis.KeepTTL)
					if repairProofTTL {
						pipe.Expire(ctx, tKey, BattleDepartureTerminalRetention)
					}
				} else {
					pipe.Set(ctx, tKey, proofPayload, BattleDepartureTerminalRetention)
				}
				if journalChanged {
					if journalTerminal {
						pipe.Set(ctx, jKey, journalPayload, proofRetention)
					} else {
						pipe.Set(ctx, jKey, journalPayload, 0)
					}
				} else if journalTerminal {
					// Idempotent replay bounds a legacy all-terminal journal whose
					// bytes were already committed with TTL=0.
					pipe.Expire(ctx, jKey, proofRetention)
				}
				return nil
			})
			return err
		}, jKey, tKey)
		if err == redis.TxFailedErr {
			continue
		}
		return err
	}
	return errcode.New(errcode.ErrDSAllocationFailed,
		"battle %d teardown proof CAS retry exhausted", matchID)
}
