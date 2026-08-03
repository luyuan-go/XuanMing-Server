package conf

import (
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

func TestRequireHubAssignmentBindingValidation(t *testing.T) {
	t.Run("default-compatible", func(t *testing.T) {
		var cfg Config
		cfg.Defaults()
		if cfg.Login.RequireHubAssignmentBinding {
			t.Fatal("default must remain false for rolling compatibility")
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate default: %v", err)
		}
	})

	t.Run("requires-redis", func(t *testing.T) {
		var cfg Config
		cfg.Login.RequireHubAssignmentBinding = true
		cfg.Login.Hub.Addr = "hub-allocator:50021"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected missing Redis validation error")
		}
	})

	t.Run("requires-hub-allocator", func(t *testing.T) {
		var cfg Config
		cfg.Login.RequireHubAssignmentBinding = true
		cfg.Node.RedisClient.Host = "redis:6379"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected missing hub allocator validation error")
		}
	})

	t.Run("valid", func(t *testing.T) {
		var cfg Config
		cfg.Login.RequireHubAssignmentBinding = true
		cfg.Node.RedisClient.Addrs = []string{"redis-0:6379", "redis-1:6379"}
		cfg.Login.Hub.Addr = "hub-allocator:50021"
		cfg.Login.Locator.Addr = "player-locator:50006"
		cfg.Login.HubAssignmentFence.EtcdEndpoints = []string{"etcd:2379"}
		cfg.Login.HubAssignmentFence.KeysetRevision = "pandora-auth-r1"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})

	t.Run("requires-player-locator", func(t *testing.T) {
		var cfg Config
		cfg.Login.RequireHubAssignmentBinding = true
		cfg.Node.RedisClient.Host = "redis:6379"
		cfg.Login.Hub.Addr = "hub-allocator:50021"
		cfg.Login.HubAssignmentFence.EtcdEndpoints = []string{"etcd:2379"}
		cfg.Login.HubAssignmentFence.KeysetRevision = "pandora-auth-r1"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected missing player locator validation error")
		}
	})
}

func TestRedisDSAdmissionRequiresSingleConsistentFence(t *testing.T) {
	valid := func() Config {
		var cfg Config
		cfg.Defaults()
		cfg.Node.RedisClient.Host = "redis:6379"
		cfg.Login.Hub.Addr = "hub-allocator:50021"
		cfg.Login.Locator.Addr = "player-locator:50006"
		cfg.Login.RequireHubAssignmentBinding = true
		fence := config.DSAuthFenceConf{
			EtcdEndpoints: []string{"etcd:2379"}, EtcdPrefix: "/pandora/ds-auth/",
			EtcdLeaseTTLSec: 15, EtcdDialTimeout: config.Duration(5 * time.Second),
			KeysetRevision: "pandora-auth-r1",
		}
		cfg.Login.HubAssignmentFence = fence
		cfg.DSAuth.Mode = "enforce"
		cfg.DSAuth.AuthorityMode = "redis"
		cfg.DSAuth.Fence = fence
		return cfg
	}

	t.Run("valid-and-one-capability", func(t *testing.T) {
		cfg := valid()
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		fence, enabled := cfg.CapabilityFence()
		if !enabled || fence.KeysetRevision != "pandora-auth-r1" {
			t.Fatalf("fence=%+v enabled=%v", fence, enabled)
		}
	})

	t.Run("fence-mismatch", func(t *testing.T) {
		cfg := valid()
		cfg.Login.HubAssignmentFence.KeysetRevision = "other"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected inconsistent fence rejection")
		}
	})

	t.Run("binding-missing", func(t *testing.T) {
		cfg := valid()
		cfg.Login.RequireHubAssignmentBinding = false
		if err := cfg.Validate(); err == nil {
			t.Fatal("redis admission must require hub assignment binding")
		}
	})

	t.Run("redis-permissive", func(t *testing.T) {
		cfg := valid()
		cfg.DSAuth.Mode = "permissive"
		if err := cfg.Validate(); err == nil {
			t.Fatal("redis admission must require enforce")
		}
	})
}

// TestRetentionModeValidation 守住 §9.24 的启动 fail-fast:保留期清理模式一度只有
// ValidateRetentionMode 定义、没有任何调用方,拼错 "delete" 时 RetentionMode() 静默回落
// report_only —— 运维以为开了 account_devices 清理、实际一行没删。login 把这条校验挂在
// 已有的 Config.Validate() 上(main 本就调它),本测试守住它不被摘掉。
func TestRetentionModeValidation(t *testing.T) {
	var cfg Config
	cfg.Defaults()

	// 留空 = 默认只报告不删,必须通过启动校验。
	if err := cfg.Validate(); err != nil {
		t.Fatalf("留空 retention_mode 必须通过启动校验: %v", err)
	}
	if mode := cfg.Login.RetentionMode(); mode != dbguard.ModeReportOnly {
		t.Fatalf("留空 retention_mode 应为 report_only, got %v", mode)
	}

	cfg.Login.RetentionModeRaw = "delete"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("retention_mode=delete 必须通过启动校验: %v", err)
	}
	if mode := cfg.Login.RetentionMode(); mode != dbguard.ModeDelete {
		t.Fatalf("retention_mode=delete 应为 delete, got %v", mode)
	}

	// 拼错必须让整个 Config.Validate() 失败(= main 拒启),且绝不能被猜成 delete。
	for _, raw := range []string{"delet", "true", "1", "purge", "off"} {
		cfg.Login.RetentionModeRaw = raw
		if err := cfg.Validate(); err == nil {
			t.Fatalf("retention_mode=%q 必须启动 fail-fast", raw)
		}
		if mode := cfg.Login.RetentionMode(); mode == dbguard.ModeDelete {
			t.Fatalf("retention_mode=%q 绝不能被猜成 delete", raw)
		}
	}
}
