// retention_mode_test.go — 保留期清理模式的启动校验语义(§9.24)。
//
// 守的是一个真实盲点:ValidateRetentionMode 一度只有定义、没有任何 main 调用,
// 于是把 "delete" 拼错时 RetentionMode() 静默回落 report_only —— 运维以为开了清理,
// 实际一行没删,库继续增长且启动期毫无痕迹。main 现已 fail-fast,本测试守住语义不回退。
package conf

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/dbguard"
)

func TestRetentionModeValidation(t *testing.T) {
	var c ChatConf

	// 留空 = 默认只报告不删(2026-07-22 用户指令),且必须能通过启动校验。
	if err := c.ValidateRetentionMode(); err != nil {
		t.Fatalf("留空 retention_mode 必须通过启动校验: %v", err)
	}
	if mode := c.RetentionMode(); mode != dbguard.ModeReportOnly {
		t.Fatalf("留空 retention_mode 应为 report_only, got %v", mode)
	}

	// 显式 delete 生效。
	c.RetentionModeRaw = "delete"
	if err := c.ValidateRetentionMode(); err != nil {
		t.Fatalf("retention_mode=delete 必须通过启动校验: %v", err)
	}
	if mode := c.RetentionMode(); mode != dbguard.ModeDelete {
		t.Fatalf("retention_mode=delete 应为 delete, got %v", mode)
	}

	// 拼错必须拒启,且绝不能被猜成 delete。
	for _, raw := range []string{"delet", "Delete ", "true", "1", "purge", "off"} {
		c.RetentionModeRaw = raw
		err := c.ValidateRetentionMode()
		switch raw {
		case "Delete ": // 大小写与首尾空白是合法写法(ParseMode 归一化)
			if err != nil {
				t.Fatalf("retention_mode=%q 应被归一化接受: %v", raw, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("retention_mode=%q 必须启动 fail-fast", raw)
		}
		if mode := c.RetentionMode(); mode == dbguard.ModeDelete {
			t.Fatalf("retention_mode=%q 绝不能被猜成 delete", raw)
		}
	}
}
