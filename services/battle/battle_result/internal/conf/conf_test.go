package conf

import (
	"os"
	"path/filepath"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/kafkax"
)

func TestValidateRedisAuthorityIngressRejectsLegacyBattleResultTopic(t *testing.T) {
	cfg := Config{}
	cfg.Defaults()
	cfg.DSAuth.AuthorityMode = "redis"
	if err := cfg.ValidateRedisAuthorityIngress(); err == nil {
		t.Fatal("Redis authority must reject unauthenticated pandora.battle.result consumer")
	}

	cfg.Battle.ConsumeTopics = []string{kafkax.TopicDSLifecycle}
	cfg.Battle.DSAllocatorAddr = "ds-allocator:20020"
	if err := cfg.ValidateRedisAuthorityIngress(); err != nil {
		t.Fatalf("lifecycle-only config: %v", err)
	}

	cfg.Battle.DSAllocatorAddr = ""
	if err := cfg.ValidateRedisAuthorityIngress(); err == nil {
		t.Fatal("Redis authority accepted missing terminal release relay")
	}
}

func TestValidateRedisAuthorityIngressKeepsLegacyProfile(t *testing.T) {
	cfg := Config{}
	cfg.Defaults()
	if err := cfg.ValidateRedisAuthorityIngress(); err != nil {
		t.Fatalf("legacy/off profile remains compatible: %v", err)
	}
}

// TestHistoryRetentionDaysClamped 守住战报保留期口径:默认六个月,
// 配置只能在 [30,180] 内生效 —— 写 365 不得原样放行(库会突破 §9.24 登记的例外与
// budgets.go 的容量预算),写 1 也不得原样放行(本服是真删,一个手滑就删光玩家还在看的战报)。
func TestHistoryRetentionDaysClamped(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, HistoryRetentionMaxDays},   // 未配置 = 六个月
		{-1, HistoryRetentionMaxDays},  // 非法值同上
		{365, HistoryRetentionMaxDays}, // 超上限钳回
		{180, 180},                     // 上限本身
		{90, 90},                       // 区间内原样生效
		{1, HistoryRetentionMinDays},   // 低于下限钳回
	} {
		cfg := Config{}
		cfg.Battle.HistoryRetentionDays = tc.in
		cfg.Defaults()
		if got := cfg.Battle.HistoryRetentionDays; got != tc.want {
			t.Fatalf("history_retention_days=%d → %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestRetentionModeDefaultsToDelete 守住 2026-08-03 用户指令:战报库留空即真删。
// 本服默认与 dbguard 全局 report_only 相反,拿掉它 = 六个月口径静默失效、库无界增长。
func TestRetentionModeDefaultsToDelete(t *testing.T) {
	cfg := Config{}
	cfg.Defaults()
	if mode := cfg.Battle.RetentionMode(); mode != dbguard.ModeDelete {
		t.Fatalf("留空 retention_mode 应为 delete, got %v", mode)
	}
	if err := cfg.Battle.ValidateRetentionMode(); err != nil {
		t.Fatalf("留空 retention_mode 必须能通过启动校验: %v", err)
	}

	cfg.Battle.RetentionModeRaw = "report_only"
	if mode := cfg.Battle.RetentionMode(); mode != dbguard.ModeReportOnly {
		t.Fatalf("显式 report_only 必须停删, got %v", mode)
	}

	// 拼错必须拒启(不能静默回落成不删,那样库会一直涨且没人发现)。
	cfg.Battle.RetentionModeRaw = "delet"
	if err := cfg.Battle.ValidateRetentionMode(); err == nil {
		t.Fatal("拼错的 retention_mode 必须启动 fail-fast")
	}
}

func TestProductionExampleIsModelBOnly(t *testing.T) {
	examplePath := filepath.Join("..", "..", "etc", "battle_result-prod.yaml.example")
	raw, err := os.ReadFile(examplePath)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "battle_result-prod.yaml")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(path)))
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Load(); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := c.Scan(&cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Defaults()
	if cfg.DSAuth.Mode != "enforce" || !cfg.DSAuth.AuthorityModeRedis() {
		t.Fatalf("prod DS authority mode=%q authority=%q", cfg.DSAuth.Mode, cfg.DSAuth.AuthorityMode)
	}
	if len(cfg.Battle.ConsumeTopics) != 1 || cfg.Battle.ConsumeTopics[0] != kafkax.TopicDSLifecycle {
		t.Fatalf("prod consume topics=%v", cfg.Battle.ConsumeTopics)
	}
	if cfg.Node.RedisClient.Host == "" || cfg.Battle.DSAllocatorAddr == "" {
		t.Fatal("prod Model-B Redis/terminal relay dependency missing")
	}
	if err := cfg.DSAuth.ValidateRedisFence(); err != nil {
		t.Fatalf("prod fence: %v", err)
	}
	if err := cfg.ValidateRedisAuthorityIngress(); err != nil {
		t.Fatalf("prod ingress: %v", err)
	}
	// 战报六个月口径必须在生产模板里成立(模板是运维复制的起点,漂了等于没落地)。
	if cfg.Battle.HistoryRetentionDays != HistoryRetentionMaxDays {
		t.Fatalf("prod history_retention_days=%d, want %d(六个月)",
			cfg.Battle.HistoryRetentionDays, HistoryRetentionMaxDays)
	}
	if mode := cfg.Battle.RetentionMode(); mode != dbguard.ModeDelete {
		t.Fatalf("prod retention_mode=%v, want delete(超过六个月的战报不该留在 MySQL)", mode)
	}
}
