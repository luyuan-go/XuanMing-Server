// ready_generation_test.go — 跨代幂等与旧 MM 兜底(INC-20260813-001 ①②)。
//
// ① 的判据只有一条:**一条迟到的旧局释放,绝不能抹掉玩家的新意图**。
// ② 的判据是:旧 matchmaker 副本不调 EndTeamMatch 时,队伍不能永远停在 READY。
package biz

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ── ① 代际本身 ──────────────────────────────────────────────────────────────

// 任何改变「谁准备好了」的写都必须推进代际。漏掉任何一处,跨代幂等就形同虚设。
func TestReadyGeneration_ready意图变更必须推进(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	const teamID, captain, member = uint64(9901), uint64(7901), uint64(7902)
	setupTwoMemberTeam(t, uc, teamID, captain, member)

	prev := teamOf(t, uc, teamID).GetReadyGeneration()
	bump := func(name string, do func()) {
		t.Helper()
		do()
		got := teamOf(t, uc, teamID).GetReadyGeneration()
		if got <= prev {
			t.Fatalf("%s 必须推进 ready 代际: %d → %d", name, prev, got)
		}
		prev = got
	}

	bump("点准备", func() {
		if _, err := uc.SetReady(ctx, teamID, member, true, 0); err != nil {
			t.Fatal(err)
		}
	})
	bump("取消准备", func() {
		if _, err := uc.SetReady(ctx, teamID, member, false, 0); err != nil {
			t.Fatal(err)
		}
	})
	bump("离队(成员集合变了)", func() {
		if _, err := uc.LeaveTeam(ctx, teamID, member); err != nil {
			t.Fatal(err)
		}
	})
}

// 反向:与 ready 意图无关的写**不得**推进代际。
// 推进得太勤会让正常的 EndTeamMatch 频繁 CAS 失败 —— 该复位的反而不复位了。
func TestReadyGeneration_无关变更不得推进(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	const teamID, captain = uint64(9902), uint64(7911)
	setupTwoMemberTeam(t, uc, teamID, captain, 7912)

	before := teamOf(t, uc, teamID).GetReadyGeneration()
	if _, err := uc.SetTeamMap(ctx, teamID, captain, 9); err != nil {
		t.Fatal(err)
	}
	if got := teamOf(t, uc, teamID).GetReadyGeneration(); got != before {
		t.Fatalf("改地图与「谁准备好了」无关,不得推进代际: %d → %d", before, got)
	}
}

// ── ① 跨代幂等:本条是整个 ① 的存在理由 ────────────────────────────────────

// ★ 迟到的旧局释放绝不能抹掉玩家的新意图。
//
// 时序:对局结束 → EndTeamMatch 的 ACK 丢了 → 玩家重新点了准备(代际前进)
//
//	→ outbox 重投旧的 EndTeamMatch → **必须 no-op**。
func TestEndTeamMatch_迟到重投不得抹掉新意图(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	const teamID, captain, member = uint64(9903), uint64(7921), uint64(7922)
	setupTwoMemberTeam(t, uc, teamID, captain, member)

	// 开局那一刻的代际。
	if _, err := uc.SetReady(ctx, teamID, member, true, 0); err != nil {
		t.Fatal(err)
	}
	staleGen := teamOf(t, uc, teamID).GetReadyGeneration()

	// ACK 丢了,期间玩家重新动了准备状态 → 代际前进。
	if _, err := uc.SetReady(ctx, teamID, member, false, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.SetReady(ctx, teamID, member, true, 0); err != nil {
		t.Fatal(err)
	}
	freshGen := teamOf(t, uc, teamID).GetReadyGeneration()
	if freshGen == staleGen {
		t.Fatal("前提不成立:代际没有前进")
	}

	// outbox 用**旧**代际重投。
	if err := uc.EndTeamMatch(ctx, teamID, []uint64{member}, staleGen); err != nil {
		t.Fatalf("跨代重投必须幂等成功(不是报错): %v", err)
	}
	after := teamOf(t, uc, teamID)
	if !after.Members[memberIndex(after.Members, member)].GetReady() {
		t.Fatal("★ 迟到的旧局释放抹掉了玩家重新点上的准备 —— 这正是 ① 要防的")
	}
	if after.GetReadyGeneration() != freshGen {
		t.Fatalf("no-op 不得推进代际: %d → %d", freshGen, after.GetReadyGeneration())
	}
}

// 正常路径:代际对得上就照常复位。
func TestEndTeamMatch_代际匹配时正常复位(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	const teamID, captain, member = uint64(9904), uint64(7931), uint64(7932)
	setupTwoMemberTeam(t, uc, teamID, captain, member)
	if _, err := uc.SetReady(ctx, teamID, member, true, 0); err != nil {
		t.Fatal(err)
	}
	gen := teamOf(t, uc, teamID).GetReadyGeneration()

	if err := uc.EndTeamMatch(ctx, teamID, []uint64{member}, gen); err != nil {
		t.Fatal(err)
	}
	after := teamOf(t, uc, teamID)
	if after.Members[memberIndex(after.Members, member)].GetReady() {
		t.Fatal("代际匹配时必须复位")
	}
}

// 代际=0(滚动升级窗口的旧 matchmaker / 旧 team 记录)→ 退化为旧语义:照常复位一次。
// 不跨代安全,但严格优于「完全不复位」——那正是本事故的第一根因。
func TestEndTeamMatch_代际未知时退化复位(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	const teamID, captain, member = uint64(9905), uint64(7941), uint64(7942)
	setupTwoMemberTeam(t, uc, teamID, captain, member)
	if _, err := uc.SetReady(ctx, teamID, member, true, 0); err != nil {
		t.Fatal(err)
	}

	if err := uc.EndTeamMatch(ctx, teamID, []uint64{member}, 0); err != nil {
		t.Fatal(err)
	}
	after := teamOf(t, uc, teamID)
	if after.Members[memberIndex(after.Members, member)].GetReady() {
		t.Fatal("代际未知时应退化为旧语义(复位一次),而不是完全不动")
	}
}

// ── ② 旧 MM 共存兜底:已随「方案 A」删除 ──────────────────────────────────────
//
// 原先靠 pending_match_reset_gen 标记 + 读路径回查 matchmaker 权威兜底。方案 A 把 ready
// 改成在 BeginTeamMatch 里一次性消费后,「结束后还欠一次复位」这个状态本身就不存在了 ——
// 标记、兜底、以及它们的用例一并移除(proto 字段 14 已 reserved)。
// 保留本注释是为了说明这批用例是**被设计取代**而不是被删掉不测。
// ── 机械门禁:禁止绕过 updateTeam ────────────────────────────────────────────

// 「改了 ready 意图就要推进代际」有 7 处以上写点,漏一处的后果是**静默的** ——
// 代际停在旧值,CAS 照样通过,幂等保护形同虚设,而所有测试照样绿。
// 所以队伍写必须全部走 updateTeam;本测试机械拦住绕过。
func TestNoDirectUpdateWithLockInBiz(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") || name == "ready_generation.go" {
			continue // 包装器本身与测试夹具除外
		}
		b, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "repo.UpdateWithLock(") {
			offenders = append(offenders, name)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("队伍写必须走 u.updateTeam(它在锁内自动推进 ready 代际);"+
			"直接调 repo.UpdateWithLock 会静默丢掉代际推进,使跨代幂等失效。违规文件: %v", offenders)
	}
}

// waitRosterLockExpired 等到组票租约自净。租约有下限护栏(matchLockMinLease),
// 传 1ms 也会被钳到那个下限,所以这里必须按下限等。
func waitRosterLockExpired() { time.Sleep(matchLockMinLease + 200*time.Millisecond) }
