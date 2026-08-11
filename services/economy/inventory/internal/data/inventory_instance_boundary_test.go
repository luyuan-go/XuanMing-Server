package data

import "testing"

// TestGrantInstancesFingerprintCanonicalAndMultiplicity 确保重复奖励重试可忽略顺序，
// 但不能把“同配置多发一件”误判成同一请求。
func TestGrantInstancesFingerprintCanonicalAndMultiplicity(t *testing.T) {
	base := GrantInstancesFingerprint([]uint32{5002, 5001, 5001})
	if got := GrantInstancesFingerprint([]uint32{5001, 5002, 5001}); got != base {
		t.Fatalf("同一奖励集合换序后指纹应一致: base=%s got=%s", base, got)
	}
	if got := GrantInstancesFingerprint([]uint32{5001, 5002}); got == base {
		t.Fatalf("减少一件奖励后指纹必须变化: fingerprint=%s", got)
	}
	if got := GrantInstancesFingerprint([]uint32{5001, 5001, 5001}); got == base {
		t.Fatalf("替换配置但保持件数时指纹必须变化: fingerprint=%s", got)
	}
}

// TestLowestFreeSlotCapacityBoundaries 覆盖最后一格、满格、缩容与扩容时的槽位裁决。
// 并发原子性由 MySQL 的 FOR UPDATE 路径负责，不能用该纯函数测试替代。
func TestLowestFreeSlotCapacityBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		occupied map[int32]struct{}
		capacity int32
		wantSlot int32
		wantOK   bool
	}{
		{name: "空背包首格", occupied: map[int32]struct{}{}, capacity: 3, wantSlot: 0, wantOK: true},
		{name: "最后一格", occupied: map[int32]struct{}{0: {}, 1: {}}, capacity: 3, wantSlot: 2, wantOK: true},
		{name: "满格", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 3, wantSlot: -1, wantOK: false},
		{name: "复用中间空槽", occupied: map[int32]struct{}{0: {}, 2: {}}, capacity: 3, wantSlot: 1, wantOK: true},
		{name: "缩容后无可用槽", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 2, wantSlot: -1, wantOK: false},
		{name: "扩容开放新槽", occupied: map[int32]struct{}{0: {}, 1: {}, 2: {}}, capacity: 4, wantSlot: 3, wantOK: true},
		{name: "零容量", occupied: map[int32]struct{}{}, capacity: 0, wantSlot: -1, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlot, gotOK := lowestFreeSlot(tt.occupied, tt.capacity)
			if gotSlot != tt.wantSlot || gotOK != tt.wantOK {
				t.Fatalf("槽位裁决不符: got=(%d,%v) want=(%d,%v)", gotSlot, gotOK, tt.wantSlot, tt.wantOK)
			}
		})
	}
}

// TestInstanceIDLedgerDetailRoundTrip 确保幂等回放保存的实例 ID 不丢失、不重排。
func TestInstanceIDLedgerDetailRoundTrip(t *testing.T) {
	want := []uint64{9003, 9001, 9002}
	got := decodeInstanceIDs(encodeInstanceIDs(want))
	if len(got) != len(want) {
		t.Fatalf("回放实例数量不符: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("回放实例顺序或内容不符: got=%v want=%v", got, want)
		}
	}
}

func TestItemClosureFingerprintsSeparateOperationsAndExactInstance(t *testing.T) {
	if UseFingerprint(10001, 1) == BattleConsumeFingerprint(10001, 1) ||
		DiscardFingerprint(10001, 1) == BattleDiscardFingerprint(10001, 1) {
		t.Fatal("大厅与战斗操作必须使用不同指纹域")
	}
	base := SellInstanceFingerprint(9001, 10003)
	if SellInstanceFingerprint(9001, 10027) == base ||
		SellInstanceFingerprint(9002, 10003) == base {
		t.Fatal("实例出售指纹必须绑定 instance/config")
	}
	if SellInstanceFingerprint(9001, 10003) != base {
		t.Fatal("服务端热更售价不得改变实例出售意图指纹")
	}
	if legacySaleLedgerMatches(legacySellFingerprint(10001, 2, 180),
		"sell item=10001 count=2 gold=180", saleLedgerIntent{op: "sell", itemConfigID: 10001, count: 2}) != true {
		t.Fatal("合法旧 stack ledger 应按 detail 中首次售价兼容")
	}
	if legacySaleLedgerMatches(legacySellFingerprint(10001, 2, 180),
		"sell item=10001 count=3 gold=180", saleLedgerIntent{op: "sell", itemConfigID: 10001, count: 2}) {
		t.Fatal("旧 ledger 的 detail/hash 与请求意图不一致不得放行")
	}
	if legacySaleLedgerMatches(legacySellInstanceFingerprint(9001, 10003, 180),
		"sell instance=9001 item=10003 gold=180 trailing", saleLedgerIntent{op: "sell_inst", instanceID: 9001, itemConfigID: 10003}) {
		t.Fatal("旧 ledger detail 必须精确解析，不得接受尾随内容")
	}
}
