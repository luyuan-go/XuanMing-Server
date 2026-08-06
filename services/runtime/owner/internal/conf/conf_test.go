package conf

import (
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

func TestDefaultsProvideBoundedOwnerMaintenancePolicy(t *testing.T) {
	var cfg Config
	cfg.Defaults()

	if cfg.Server.Grpc.Addr != ":20017" || cfg.Server.Http.Addr != ":21017" {
		t.Fatalf("默认监听地址错误: grpc=%q http=%q", cfg.Server.Grpc.Addr, cfg.Server.Http.Addr)
	}
	if got := cfg.Owner.SweepInterval.Std(); got != 5*time.Minute {
		t.Fatalf("默认 sweep interval=%v", got)
	}
	if cfg.Owner.SweepBatch != 500 || cfg.Owner.LogRetentionDays != 90 {
		t.Fatalf("默认清理策略错误: batch=%d retention=%d", cfg.Owner.SweepBatch, cfg.Owner.LogRetentionDays)
	}
}

func TestDefaultsKeepExplicitPositiveOwnerPolicy(t *testing.T) {
	cfg := Config{
		Owner: OwnerConf{
			SweepInterval:    config.Duration(17 * time.Second),
			SweepBatch:       23,
			LogRetentionDays: 45,
		},
	}
	cfg.Server.Grpc.Addr = "127.0.0.1:15017"
	cfg.Server.Http.Addr = "127.0.0.1:15117"
	cfg.Defaults()

	if cfg.Server.Grpc.Addr != "127.0.0.1:15017" || cfg.Server.Http.Addr != "127.0.0.1:15117" {
		t.Fatalf("显式监听地址被覆盖: grpc=%q http=%q", cfg.Server.Grpc.Addr, cfg.Server.Http.Addr)
	}
	if cfg.Owner.SweepInterval.Std() != 17*time.Second || cfg.Owner.SweepBatch != 23 || cfg.Owner.LogRetentionDays != 45 {
		t.Fatalf("显式清理策略被覆盖: %+v", cfg.Owner)
	}
}

func TestDefaultsRepairZeroAndNegativeMaintenanceValues(t *testing.T) {
	cfg := Config{Owner: OwnerConf{
		SweepInterval:    config.Duration(-time.Second),
		SweepBatch:       -1,
		LogRetentionDays: -1,
	}}
	cfg.Defaults()

	if cfg.Owner.SweepInterval.Std() != 5*time.Minute || cfg.Owner.SweepBatch != 500 || cfg.Owner.LogRetentionDays != 90 {
		t.Fatalf("非法清理策略未回落安全默认值: %+v", cfg.Owner)
	}
}
