package data

import (
	"strings"
	"testing"
)

func TestBuildSaveSnapshotSQLQuotesRank(t *testing.T) {
	query, args := buildSaveSnapshotSQL(100, []SnapshotRow{
		{Rank: 1, EntityID: 200, Score: 300, CreatedAtMs: 400},
		{Rank: 2, EntityID: 201, Score: 250, CreatedAtMs: 401},
	})

	if !strings.Contains(query, "`rank`") {
		t.Fatalf("query must quote rank column for MySQL 8: %s", query)
	}
	if strings.Contains(query, " rank,") {
		t.Fatalf("query contains unquoted rank column: %s", query)
	}
	if got, want := len(args), 10; got != want {
		t.Fatalf("args len=%d, want %d", got, want)
	}
}

// 失败标记绝不能覆盖已 GRANTED 的行(2026-08-11,INC-20260811-001 §6 同型扫描命中)。
//
// 多副本补扫是刻意允许的(正确性靠下游幂等键),但无条件 UPDATE 会让 A 副本发放成功写
// GRANTED 的同时,B 副本因下游瞬时不可用把同一行打回 FAILED —— 已发放的行重回补发工作集
// 每轮重放,审计信号被淹没,且下游幂等记录过保留期(90 天)后再重放就是**真重复发放**。
//
// 去掉 buildMarkRewardSQL 里的 `status <> ?` 分支,本用例必红。
func TestBuildMarkRewardSQLGuardsGrantedFromFailure(t *testing.T) {
	t.Run("失败标记必须带 GRANTED 守卫", func(t *testing.T) {
		query, args := buildMarkRewardSQL("k1", RewardFailed, 1700)
		if !strings.Contains(query, "AND status <> ?") {
			t.Fatalf("失败标记必须带 `status <> GRANTED` 条件,否则会把已发放行打回补发工作集: %s", query)
		}
		if got, want := len(args), 4; got != want {
			t.Fatalf("args len=%d, want %d(多出的那个是 GRANTED 守卫值)", got, want)
		}
		if args[3] != RewardGranted {
			t.Fatalf("守卫值必须是 RewardGranted,实为 %v", args[3])
		}
	})

	t.Run("成功标记无条件推进终态", func(t *testing.T) {
		query, args := buildMarkRewardSQL("k1", RewardGranted, 1700)
		if strings.Contains(query, "AND status <> ?") {
			t.Fatalf("成功标记不该带守卫(终态推进幂等,重复写 GRANTED 无害): %s", query)
		}
		if got, want := len(args), 3; got != want {
			t.Fatalf("args len=%d, want %d", got, want)
		}
	})
}
