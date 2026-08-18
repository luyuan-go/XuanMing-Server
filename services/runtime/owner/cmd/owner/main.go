// Pandora owner 服务入口(每玩家 owner 权威,§9.22;owner-authority.md,2026-07-21)。
//
// 职责:owner_record 单调 epoch CAS、PENDING/ADMITTED 两阶段、admit_not_before 迁移屏障、
// DS 实例租约(pandora_owner 库,强依赖)。生产必须连 TiDB(线性一致 + 确认写不回滚);
// dev 允许单机 MySQL 联调(无复制,天然线性一致)。
//
// 启动顺序(对齐 inventory):
//  1. Logger
//  2. 加载 yaml → conf.Defaults
//  3. MySQL/TiDB client + schema gate(缺表 fail-fast 指向 15-owner-tables.sql / 02-owner-tidb.sql)
//  4. 装配 OwnerUsecase → OwnerService → gRPC/HTTP server
//  5. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/safego"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/biz"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/server"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/service"
)

const serviceName = "owner"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/owner-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger
	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

	// 2. 加载 yaml
	cfgPath, err := filepath.Abs(flagConf)
	if err != nil {
		helper.Errorw("msg", "abs_conf_path_failed", "err", err)
		os.Exit(1)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(cfgPath)))
	defer func() { _ = c.Close() }()

	if err := c.Load(); err != nil {
		helper.Errorw("msg", "config_load_failed", "err", err, "path", cfgPath)
		os.Exit(1)
	}

	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		helper.Errorw("msg", "config_scan_failed", "err", err)
		os.Exit(1)
	}
	cfg.Defaults()

	// 3. 权威存储(强依赖:owner CAS 不可降级;生产 TiDB / dev 单机 MySQL,DDL 同构)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_owner;生产必须 TiDB,§9.22)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "owner_store_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 严格模式断言(§9.24):owner 是玩家归属权威(§9.22),非严格 sql_mode 下超长写入被
	// 静默截断会让 owner 记录/租约字段损坏,后果比一般业务表更重,必须 fail-fast。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线,之后每小时一轮;超预算只告警不阻断。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_owner", data.Budgets(), nil))

	// schema gate:pandora_owner 是后建库,既有 volume 不会自动重放 init SQL;缺表 fail-fast。
	schemaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if serr := mysqlx.CheckTables(schemaCtx, db, "deploy/mysql-init/15-owner-tables.sql",
		"owner_record", "ds_instance_lease", "owner_transition_log"); serr != nil {
		cancel()
		helper.Errorw("msg", "owner_schema_check_failed", "err", serr)
		os.Exit(1)
	}
	cancel()

	// 后端强校验(§9.22):require_tidb=true(-Prod 产物注入)时权威库必须是 TiDB;
	// MySQL 异步复制切换会回滚已确认写,owner CAS 回滚即可能双 owner,fail-fast 拒启。
	if cfg.Owner.RequireTiDB {
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if verr := data.AssertTiDBBackend(verifyCtx, db); verr != nil {
			verifyCancel()
			helper.Errorw("msg", "owner_backend_not_tidb", "err", verr)
			os.Exit(1)
		}
		verifyCancel()
		helper.Infow("msg", "owner_backend_tidb_verified")
	}

	// expand DDL 校验(INC-20260818-003 分阶段发布第 1 步):本版 SELECT 已引用
	// hub_source_revision,缺列会让 owner 权威面每个 RPC 都报 1054 而启动日志毫无痕迹。
	// 与上面的建表校验同一纪律:漏一步 DDL 就让新镜像整个不可用时,拒启比带病运行好查。
	revCtx, revCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if rerr := data.AssertSourceRevisionColumn(revCtx, db); rerr != nil {
		revCancel()
		helper.Errorw("msg", "owner_source_revision_column_missing", "err", rerr)
		os.Exit(1)
	}
	revCancel()

	// 4. 装配链
	ownerRepo := data.NewMySQLOwnerRepo(db)
	// INC-20260818-003 分阶段发布最后一步的开关。默认关 = 兼容窗;打开前必须先证明旧
	// hub_allocator 已排空,否则仍在跑的旧副本会整体写失败(大厅分配停摆)。
	ownerRepo.SetRejectLegacySourceRevision(cfg.Owner.RejectLegacySourceRevision)
	if cfg.Owner.RejectLegacySourceRevision {
		helper.Warnw("msg", "owner_legacy_source_revision_gate_enabled",
			"hint", "已全局拒绝不带来源版本的 Begin;确认旧 hub_allocator 副本已全部排空")
	}
	uc := biz.NewOwnerUsecase(ownerRepo, cfg.Owner)
	svc := service.NewOwnerService(uc)

	// 审计流水保留期清理(§9.24;多副本各自跑,DELETE 幂等无需锁)。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go runTransitionLogSweep(sweepCtx, uc, helper, cfg.Owner.SweepInterval.Std(), cfg.Owner.SweepBatch)

	grpcSrv := server.NewGRPCServer(&cfg, svc)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"log_retention_days", cfg.Owner.LogRetentionDays,
	)

	// 5. Kratos App
	app := kratos.New(
		kratos.Name(serviceName),
		kratos.Logger(logger),
		kratos.Server(grpcSrv, httpSrv),
	)
	if err := app.Run(); err != nil {
		helper.Errorw("msg", "app_run_failed", "err", err)
		os.Exit(1)
	}
}

// runTransitionLogSweep 周期清理超保留期审计流水。
func runTransitionLogSweep(ctx context.Context, uc *biz.OwnerUsecase, helper *klog.Helper, interval time.Duration, batch int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "owner_transition_log_sweep", func() {
				sweepCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if n, err := uc.RunTransitionLogSweep(sweepCtx, batch); err != nil {
					helper.Errorw("msg", "owner_transition_log_sweep_failed", "err", err)
				} else if n > 0 {
					// §9.24 要求 sweep 有界清理可观测:删除行数此前被丢弃,只有失败可见,
					// 成功清理完全无痕——无法确认 owner_transition_log 真在排空、批量是否打满
					// (打满 = 积压,需调大 batch/interval)。
					helper.Infow("msg", "owner_transition_log_swept", "deleted_rows", n, "batch", batch)
				}
			})
		}
	}
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表);超预算只告警不阻断。
func runCapacityGuard(ctx context.Context, g *dbguard.Guard) {
	safego.Run(ctx, "db_capacity_guard_initial", func() { g.Check(ctx) })
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "db_capacity_guard", func() { g.Check(ctx) })
		}
	}
}

// maskDSN 脱敏 DSN 里的密码(对齐 inventory main.go)。
func maskDSN(dsn string) string {
	at := -1
	colon := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && colon == -1 {
			colon = i
		}
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if colon != -1 && at != -1 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}
