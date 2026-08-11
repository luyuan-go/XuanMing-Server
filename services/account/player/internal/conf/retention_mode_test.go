// retention_mode_test.go — player 保留期清理模式的启动校验与两道闸语义(§9.24)。
//
// 守两件事:
//  1. 与其它 7 个服务同一口径:留空 = report_only 且过校验、"delete" 生效、拼错必须
//     fail-fast 且绝不能被猜成 delete(拼错就删生产数据是不可接受的失败模式);
//  2. player 特有的第二道闸:总闸 retention_mode 开了,但某一组的前置条件
//     (*_cleanup_enabled)没确认时,那一组必须降级成只报告 —— 上游重放窗口还没收敛到
//     留存期以内就删幂等收据 = 同一事件双发(重复加经验 / 段位分 / 点数 / 发卡)。
package conf

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/dbguard"
)

func TestRetentionModeValidation(t *testing.T) {
	var c PlayerConf

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
		if raw == "Delete " { // 大小写与首尾空白是合法写法(ParseMode 归一化)
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

// TestPerGroupRetentionGate 钉住两道闸的真值表:**两个都开才删**。
//
// 最要命的一格是 (delete, 前置未确认):它必须是 report_only。写反了就会在上游重放
// 还没有界时开始删幂等收据,而这类损坏(重复发放)删完才发现、且不可逆。
func TestPerGroupRetentionGate(t *testing.T) {
	cases := []struct {
		name         string
		mode         string
		expEnabled   bool
		histEnabled  bool
		wantExpMode  dbguard.Mode
		wantHistMode dbguard.Mode
	}{
		{"全默认", "", false, false, dbguard.ModeReportOnly, dbguard.ModeReportOnly},
		{"只开总闸", "delete", false, false, dbguard.ModeReportOnly, dbguard.ModeReportOnly},
		{"只开前置", "", true, true, dbguard.ModeReportOnly, dbguard.ModeReportOnly},
		{"两个都开", "delete", true, true, dbguard.ModeDelete, dbguard.ModeDelete},
		{"总闸开-仅exp前置", "delete", true, false, dbguard.ModeDelete, dbguard.ModeReportOnly},
		{"总闸开-仅history前置", "delete", false, true, dbguard.ModeReportOnly, dbguard.ModeDelete},
		{"显式report_only压过前置", "report_only", true, true, dbguard.ModeReportOnly, dbguard.ModeReportOnly},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := PlayerConf{
				RetentionModeRaw:         tc.mode,
				ExpHistoryCleanupEnabled: tc.expEnabled,
				HistoryCleanupEnabled:    tc.histEnabled,
			}
			if got := c.ExpHistoryRetentionMode(); got != tc.wantExpMode {
				t.Errorf("exp_history 模式=%v, 期望 %v", got, tc.wantExpMode)
			}
			if got := c.HistoryRetentionMode(); got != tc.wantHistMode {
				t.Errorf("history 组模式=%v, 期望 %v", got, tc.wantHistMode)
			}
		})
	}
}

// TestRetentionWindowsClamped 钉住留存期的钳位:两组的窗口是**报告与实删共用的同一个值**,
// 钳错了会让报告口径和真删口径一起漂。
func TestRetentionWindowsClamped(t *testing.T) {
	const day = 24 * 60 * 60

	histCases := map[int]int{0: 90, -1: 90, 1: 30, 29: 30, 30: 30, 90: 90, 365: 90}
	for days, wantDays := range histCases {
		c := PlayerConf{HistoryRetentionDays: days}
		if got := int(c.HistoryRetentionOrDefault().Seconds()) / day; got != wantDays {
			t.Errorf("history_retention_days=%d → %d 天, 期望 %d 天", days, got, wantDays)
		}
	}

	// exp_history 下限 7 天(必须覆盖 progress 出箱最长重试窗),上限 90 天。
	var zero PlayerConf
	if got := int(zero.ExpHistoryRetentionOrDefault().Seconds()) / day; got != 7 {
		t.Errorf("exp_history_retention 未配置 → %d 天, 期望 7 天", got)
	}
}
