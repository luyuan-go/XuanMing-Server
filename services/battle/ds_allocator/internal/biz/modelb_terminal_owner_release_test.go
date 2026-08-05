// modelb_terminal_owner_release_test.go — Model B 正常结算释放 owner 回归(2026-08-04)。
//
// 背景(INC-20260804-001 缺口⑦-B):全仓 owner 释放此前只接了「登出 / 判弃 / saga 中止」
// 三条**异常或主动离场**路径,**对局正常打完这条路一个都没接**。Model B 的正常结算唯一
// 收口 ReleaseBattleExpected 对 owner 权威零动作,玩家正常结束后 owner 仍是
// BATTLE/ADMITTED 指向一台已被 K8s 删除的 GameServer,login 的 query-first 会一直把玩家
// 指回去 —— 即 ownerReleaseAbandonedPlayersWeak 注释点名的「比不接 owner 更糟」。
package biz

import (
	"context"
	"testing"
	"time"

	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// modelBTerminalFixture 装配一台已 active 的 Model B 战斗实例,返回终态释放所需入参。
func modelBTerminalFixture(t *testing.T, matchID uint64, allocationID, podName, instanceUID string,
	playerIDs []uint64) (*AllocatorUsecase, data.BattleExpectedInstance, data.BattleResultAuthorizationProof) {
	t.Helper()
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, battleRepo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, _ := enableModelBForTest(t, uc, mr)

	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		AllocatedAtMs: time.Now().UnixMilli(), LastHeartbeatMs: time.Now().UnixMilli(),
	}
	if claimed, _, err := battleRepo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if fenced, err := battleRepo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
		t.Fatalf("fence=%v err=%v", fenced, err)
	}
	battle := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateWarming, AllocationId: allocationID,
		DsPodName: podName, DsAddr: "10.0.0.9:7777", GameserverUid: instanceUID,
		PodUid: "pod-uid-" + instanceUID, ReleaseTrack: "stable", PlayerIds: playerIDs,
		AllocatedAtMs: time.Now().UnixMilli(), LastHeartbeatMs: time.Now().UnixMilli(),
	}
	if finalized, err := battleRepo.FinalizeFencedBattleAllocation(ctx, battle, time.Hour); err != nil || !finalized {
		t.Fatalf("finalize=%v err=%v", finalized, err)
	}
	seed, err := authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID: matchID, AllocationID: allocationID, PodName: podName, InstanceUID: instanceUID,
		RequiredWriterEpoch: data.BattleDSWriterEpochV2, AuthTTL: time.Hour, BattleTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	credential := &dsv1.BattleDSCredential{
		Gen: seed.Gen, Jti: "jti-" + instanceUID, ExpMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		Kid: "kid-x", InstanceUid: instanceUID, InstanceEpoch: seed.InstanceEpoch,
		TokenSha256: "sha256-x", WriterEpoch: data.BattleDSWriterEpochV2,
	}
	if _, err := authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: credential, AuthTTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := authRepo.MarkDelivered(ctx, matchID, allocationID, credential, "rv-x", time.Hour); err != nil {
		t.Fatal(err)
	}
	identity := data.BattleCredentialIdentity{
		PodName: podName, InstanceUID: instanceUID, InstanceEpoch: seed.InstanceEpoch,
		Gen: seed.Gen, JTI: credential.Jti, ExpMs: credential.ExpMs, Kid: credential.Kid,
		TokenSHA256: credential.TokenSha256, WriterEpoch: credential.WriterEpoch,
	}
	if _, err := authRepo.ActivateHeartbeat(ctx, matchID, identity,
		data.BattleHeartbeatInput{PlayerCount: 1, State: stateRunning,
			AuthTTL: time.Hour, BattleTTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	expected := data.BattleExpectedInstance{
		AllocationID: allocationID, InstanceUID: instanceUID, InstanceEpoch: seed.InstanceEpoch,
	}
	proof := data.BattleResultAuthorizationProof{
		Credential: identity, AuthorizedAtMs: time.Now().UnixMilli(),
	}
	return uc, expected, proof
}

// Model B 正常结算(phase-1 回收确认后)必须释放本局玩家的 owner 归属。
func TestModelBTerminalRelease_ReleasesOwner(t *testing.T) {
	const (
		matchID      = uint64(94001)
		allocationID = "3717e1e9-e1b5-4841-81fc-5be66f55b3cc"
		podName      = "battle-94001"
		instanceUID  = "uid-94001"
	)
	uc, expected, proof := modelBTerminalFixture(t, matchID, allocationID, podName, instanceUID,
		[]uint64{5001, 5002})
	auth := &releaseRecordingOwnerAuthority{pod: podName, uid: instanceUID}
	uc.SetOwnerAuthority(auth)

	if err := uc.ReleaseBattleExpected(context.Background(), matchID, "completed", podName,
		expected, proof); err != nil {
		t.Fatalf("ReleaseBattleExpected err: %v", err)
	}
	if len(auth.released) != 2 {
		t.Fatalf("正常结算必须释放全部在册玩家的 owner 归属, got %v", auth.released)
	}
}

// exact 身份门:归属已指向别的实例时不得误删(玩家已被迁走的情形)。
func TestModelBTerminalRelease_SkipsOwnerPointingElsewhere(t *testing.T) {
	const (
		matchID      = uint64(94002)
		allocationID = "4717e1e9-e1b5-4841-81fc-5be66f55b3cc"
		podName      = "battle-94002"
		instanceUID  = "uid-94002"
	)
	uc, expected, proof := modelBTerminalFixture(t, matchID, allocationID, podName, instanceUID,
		[]uint64{5003})
	// owner 记录指向另一台实例:本次回收不得动它。
	auth := &releaseRecordingOwnerAuthority{pod: podName + "-other", uid: "uid-elsewhere"}
	uc.SetOwnerAuthority(auth)

	if err := uc.ReleaseBattleExpected(context.Background(), matchID, "completed", podName,
		expected, proof); err != nil {
		t.Fatalf("ReleaseBattleExpected err: %v", err)
	}
	if len(auth.released) != 0 {
		t.Fatalf("归属指向别处时不得释放, got %v", auth.released)
	}
}

// owner 权威未装配(owner_addr 未配)时,结算路径必须照常成功 —— 释放是弱依赖,
// 不能让 owner 缺席把已完成的 phase-1 回收变成失败(否则 outbox 会重放已删 GameServer 的回收)。
func TestModelBTerminalRelease_OwnerAbsentDoesNotFailRelease(t *testing.T) {
	const (
		matchID      = uint64(94003)
		allocationID = "5717e1e9-e1b5-4841-81fc-5be66f55b3cc"
		podName      = "battle-94003"
		instanceUID  = "uid-94003"
	)
	uc, expected, proof := modelBTerminalFixture(t, matchID, allocationID, podName, instanceUID,
		[]uint64{5004})
	uc.SetOwnerAuthority(nil) // owner 未启用

	if err := uc.ReleaseBattleExpected(context.Background(), matchID, "completed", podName,
		expected, proof); err != nil {
		t.Fatalf("owner 未装配时结算不得失败: %v", err)
	}
}
