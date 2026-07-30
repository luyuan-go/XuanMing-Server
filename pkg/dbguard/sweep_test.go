package dbguard

import "testing"

// TestParseMode 锁定最关键的安全语义:**只有显式写 "delete" 才允许删数据**,
// 空值/拼错一律不删(拼错一个字母就开始删生产数据是不可接受的失败模式)。
func TestParseMode(t *testing.T) {
	t.Run("默认与显式 report_only 都不删", func(t *testing.T) {
		for _, s := range []string{"", "report_only", "report", "report-only", "  REPORT_ONLY  "} {
			m, err := ParseMode(s)
			if err != nil {
				t.Fatalf("ParseMode(%q) 不该报错: %v", s, err)
			}
			if m != ModeReportOnly {
				t.Fatalf("ParseMode(%q) = %v, want ModeReportOnly", s, m)
			}
		}
	})

	t.Run("只有 delete 开启实删", func(t *testing.T) {
		m, err := ParseMode("delete")
		if err != nil || m != ModeDelete {
			t.Fatalf("ParseMode(\"delete\") = %v, %v; want ModeDelete, nil", m, err)
		}
		if m2, _ := ParseMode("DELETE"); m2 != ModeDelete {
			t.Fatalf("大小写不敏感: got %v", m2)
		}
	})

	t.Run("拼错绝不猜成 delete", func(t *testing.T) {
		// 这些都可能是运维想开删但拼错了;绝不能"猜对意图"去删数据,
		// 必须报错让服务 fail-fast,由人改对配置。
		for _, s := range []string{"del", "DELET", "true", "1", "on", "enabled", "yes", "purge", "clean"} {
			m, err := ParseMode(s)
			if err == nil {
				t.Fatalf("ParseMode(%q) 应报错(不认识的值), got mode=%v", s, m)
			}
			if m != ModeReportOnly {
				t.Fatalf("ParseMode(%q) 出错时也必须回落 ModeReportOnly, got %v", s, m)
			}
		}
	})

	t.Run("零值就是不删", func(t *testing.T) {
		var m Mode // 结构体零值 / 配置未填时的状态
		if m != ModeReportOnly {
			t.Fatalf("Mode 零值必须是 ModeReportOnly, got %v", m)
		}
		if m.String() != "report_only" {
			t.Fatalf("零值 String() = %q, want report_only", m.String())
		}
	})
}

// TestOutcomeCleaned 只有真删过才算 Cleaned(report-only 即使 Matched>0 也不算)。
func TestOutcomeCleaned(t *testing.T) {
	if (Outcome{Mode: ModeReportOnly, Matched: 10_000}).Cleaned() {
		t.Fatal("report_only 模式下 Matched>0 不得报告为已清理")
	}
	if !(Outcome{Mode: ModeDelete, Matched: 5, Deleted: 5}).Cleaned() {
		t.Fatal("delete 模式删了 5 行应为 Cleaned")
	}
}
