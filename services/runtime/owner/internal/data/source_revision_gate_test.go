// source_revision_gate_test.go — Hub 来源版本闸门的判定矩阵(INC-20260818-003)。
//
// 这里测的是纯函数 classifySourceRevision,不需要真库。它值得单独钉住,因为闸门的每一格
// 都对应一个具体的线上后果,而写反了**不会**让任何既有用例变红:
//   - 该拒的放行 → 事故反例复现(Redis=B 而 Owner=A2,玩家被送去一个已经不属于他的 Hub);
//   - 该放的拒掉 → 兼容窗内旧写者全部写失败 = 大厅分配停摆。
//
// 两个方向都必须有用例,只测一边等于没测。
package data

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/placement"
)

func TestClassifySourceRevisionMatrix(t *testing.T) {
	const legacy = placement.SourceRevisionLegacy

	cases := []struct {
		name         string
		highWater    uint64
		incoming     uint64
		sameTarget   bool
		rejectLegacy bool
		wantAllow    bool
		wantAdvance  bool
		wantReason   string
	}{
		{
			name:      "兼容窗:双方都没版本,放行且不建立水位",
			highWater: 0, incoming: legacy, sameTarget: false,
			wantAllow: true, wantAdvance: false, wantReason: "legacy_compat_window",
		},
		{
			// 本条是整个机制的安全核心:一旦该玩家见过带版本的写者,旧写者就不能再靠
			// 「我不带版本」把自己伪装成兼容窗里的正常请求。
			name:      "见过版本之后再来 legacy:永久拒",
			highWater: 500, incoming: legacy, sameTarget: true,
			wantAllow: false, wantReason: "legacy_after_versioned",
		},
		{
			name:      "全局门打开后,连兼容窗里的 legacy 也拒",
			highWater: 0, incoming: legacy, sameTarget: false, rejectLegacy: true,
			wantAllow: false, wantReason: "legacy_rejected_globally",
		},
		{
			// 事故反例里迟到的 R1/R2 落在这一格。
			name:      "来源更旧:拒",
			highWater: 500, incoming: 499, sameTarget: false,
			wantAllow: false, wantReason: "older_than_high_water",
		},
		{
			name:      "同版本同 target:幂等放行,水位不动",
			highWater: 500, incoming: 500, sameTarget: true,
			wantAllow: true, wantAdvance: false, wantReason: "same_revision_same_target",
		},
		{
			// 铸号正确时不可能发生。真出现说明两个写者共用了同一任期号(铸号被复制),
			// 那种情况下放行哪一个都是错的,只能拒。
			name:      "同版本却换了 target:拒",
			highWater: 500, incoming: 500, sameTarget: false,
			wantAllow: false, wantReason: "same_revision_different_target",
		},
		{
			name:      "正常前进:放行并推进水位",
			highWater: 500, incoming: 501, sameTarget: false,
			wantAllow: true, wantAdvance: true, wantReason: "advances_high_water",
		},
		{
			name:      "首次见到带版本的写者:放行并建立水位",
			highWater: 0, incoming: 1 << placement.SourceRevisionSeqBits, sameTarget: false,
			wantAllow: true, wantAdvance: true, wantReason: "advances_high_water",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifySourceRevision(c.highWater, c.incoming, c.sameTarget, c.rejectLegacy)
			if got.allow != c.wantAllow {
				t.Fatalf("allow=%t,期望 %t(reason=%s)", got.allow, c.wantAllow, got.reason)
			}
			if got.advance != c.wantAdvance {
				t.Fatalf("advance=%t,期望 %t(reason=%s)", got.advance, c.wantAdvance, got.reason)
			}
			if got.reason != c.wantReason {
				t.Fatalf("reason=%q,期望 %q", got.reason, c.wantReason)
			}
		})
	}
}

// TestClassifySourceRevisionNeverAdvancesOnLegacy 单独钉一条不变式:
// legacy(0)在任何情况下都不得推进水位。
//
// 它已经被上面的矩阵覆盖,但值得单列 —— 写入侧那个 `if sourceRevision > newHighWater`
// 的 max 语义正是靠它成立。哪天有人把 max 改成直接赋值,这条会立刻变红并指出原因:
// 直接赋值会让一次合法的 legacy 写把已经建立的水位打回 0,门就此对旧写者重新敞开。
func TestClassifySourceRevisionNeverAdvancesOnLegacy(t *testing.T) {
	for _, highWater := range []uint64{0, 1, 1 << 40} {
		got := classifySourceRevision(highWater, placement.SourceRevisionLegacy, true, false)
		if got.advance {
			t.Fatalf("legacy 推进了水位(high_water=%d reason=%s)", highWater, got.reason)
		}
	}
}
