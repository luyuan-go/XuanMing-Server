// local_credential_ack_test.go — legacy 战斗心跳的凭据 ACK 回显回归(2026-08-05)。
//
// 背景:UE 的 SendBattleHeartbeat 无条件用 IsBoundToRequest 把应答里的 CredentialAck 与
// DS 自持凭据逐字段比对(uid/instance_epoch/gen/jti/writer_epoch)。ds_allocator 的 legacy
// 面此前从不回显这五项,于是每一跳都被判 "heartbeat response credential ACK missing",
// UE 随即**清空整个 Command 与 BattleEvictionOrders**:stop/drain 送不到 DS、精确驱逐单
// 被整份丢弃、PendingBattleDepartureAcks 永不消费、本地准入租约永不打开
// (2026-08-05 实测 DS 日志每 5s 一条)。修复 = 与 hub 侧 applyLocalCredentialACK 同手法,
// 把经 env 下发给该进程的**同一份**凭据回显回去。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// localCredAllocator 在 Mock 分配器上叠加 localInstanceIdentitySource +
// localBattleCredentialSource,模拟 data.LocalGameServerAllocator 的形状。
type localCredAllocator struct {
	*MockGameServerAllocator
	uid   string
	epoch uint32
	cred  data.BattleCredentialIdentity
	// available=false 模拟 pod 已被回收 / 不在台账。
	available bool
}

func (a *localCredAllocator) LocalInstanceIdentity(podName string) (string, uint32, bool) {
	if podName == "" || a.uid == "" || a.epoch == 0 {
		return "", 0, false
	}
	return a.uid, a.epoch, true
}

func (a *localCredAllocator) LocalCredentialACK(podName string) (data.BattleCredentialIdentity, bool) {
	if podName == "" || !a.available {
		return data.BattleCredentialIdentity{}, false
	}
	return a.cred, true
}

func testACKCredential(uid string, epoch uint32) data.BattleCredentialIdentity {
	return data.BattleCredentialIdentity{
		InstanceUID: uid, InstanceEpoch: epoch,
		Gen: 1, JTI: "jti-" + uid, WriterEpoch: data.BattleDSWriterEpochV2,
	}
}

// localCredFixture 装配一个已 ready 的本地面对局(带 exact 实例身份与完整凭据)。
func localCredFixture(t *testing.T, matchID uint64, playerIDs []uint64, available bool) (
	*AllocatorUsecase, *localCredAllocator, string,
) {
	t.Helper()
	cfg := testCfg()
	const uid = "uid-local-cred-battle"
	alloc := &localCredAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     uid,
		epoch:                   1,
		cred:                    testACKCredential(uid, 1),
		available:               available,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	res := allocateReady(t, uc, repo, matchID, playerIDs, 7, "pve_coop")
	if res.GameserverUID == "" {
		t.Fatal("夹具前置:分配必须已回填实例身份")
	}
	return uc, alloc, res.DSPodName
}

func assertACKMatches(t *testing.T, res *HeartbeatResult, want data.BattleCredentialIdentity) {
	t.Helper()
	if res.AcceptedInstanceUID != want.InstanceUID || res.AcceptedInstanceEpoch != want.InstanceEpoch ||
		res.AcceptedTokenGen != want.Gen || res.AcceptedTokenJTI != want.JTI ||
		res.AcceptedWriterEpoch != want.WriterEpoch {
		t.Fatalf("ACK 五元组必须逐字段回显下发凭据, got uid=%q epoch=%d gen=%d jti=%q writer=%d",
			res.AcceptedInstanceUID, res.AcceptedInstanceEpoch, res.AcceptedTokenGen,
			res.AcceptedTokenJTI, res.AcceptedWriterEpoch)
	}
}

// 常规 running 心跳必须回显完整 ACK —— 否则 UE 每跳都判 ACK missing。
func TestLegacyBattleHeartbeat_EchoesLocalCredentialACK(t *testing.T) {
	const matchID uint64 = 94001
	uc, alloc, pod := localCredFixture(t, matchID, []uint64{5001}, true)

	res, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), true, []uint64{5001})
	if err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	assertACKMatches(t, res, alloc.cred)
}

// 终态 stop 应答**同样**必须带 ACK:这条恰恰最需要送达,而 UE 校验不过时会把
// Command 一并清空。killStrandedDS 是异步 Release,故 ACK 必须在进入本体前快照。
func TestLegacyTerminalHeartbeat_StopStillCarriesACK(t *testing.T) {
	const matchID uint64 = 94002
	uc, alloc, pod := localCredFixture(t, matchID, []uint64{5002}, true)
	ctx := context.Background()

	// 第一跳把对局推进到 ended,第二跳撞终态分支下发 stop。
	if _, err := uc.Heartbeat(ctx, matchID, pod, 1, "ended", time.Now().UnixMilli()); err != nil {
		t.Fatalf("ended 心跳 err: %v", err)
	}
	res, err := uc.Heartbeat(ctx, matchID, pod, 1, "ended", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("终态心跳 err: %v", err)
	}
	if res.Command != commandStop {
		t.Fatalf("终态心跳应下发 stop, got %q", res.Command)
	}
	assertACKMatches(t, res, alloc.cred)
}

// pod 不在台账(已回收 / 不是本机拉起的)时必须零回显,绝不糊半截 ACK ——
// 回半截只会把"缺字段"伪装成"不匹配",更难排查。
func TestLegacyBattleHeartbeat_NoCredentialStaysZero(t *testing.T) {
	const matchID uint64 = 94003
	uc, _, pod := localCredFixture(t, matchID, []uint64{5003}, false)

	res, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), true, []uint64{5003})
	if err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if res.AcceptedInstanceUID != "" || res.AcceptedTokenGen != 0 || res.AcceptedTokenJTI != "" ||
		res.AcceptedInstanceEpoch != 0 || res.AcceptedWriterEpoch != 0 {
		t.Fatalf("凭据不可得时必须零回显, got %+v", res)
	}
}

// 线上隔离:不实现 localBattleCredentialSource 的分配器(Mock,与 Agones 同样不实现)
// 应答逐字段不变,回显分支是机械死代码。
func TestLegacyBattleHeartbeat_NonLocalAllocatorEchoesNothing(t *testing.T) {
	const matchID uint64 = 94004
	uc, repo := newUsecase(t)
	res := allocateReady(t, uc, repo, matchID, []uint64{5004}, 7, "pve_coop")

	hb, err := uc.HeartbeatWithCensus(context.Background(), matchID, res.DSPodName, 1, "running",
		time.Now().UnixMilli(), true, []uint64{5004})
	if err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if hb.AcceptedInstanceUID != "" || hb.AcceptedTokenGen != 0 || hb.AcceptedTokenJTI != "" ||
		hb.AcceptedInstanceEpoch != 0 || hb.AcceptedWriterEpoch != 0 {
		t.Fatalf("非本地分配器必须零回显, got %+v", hb)
	}
}
