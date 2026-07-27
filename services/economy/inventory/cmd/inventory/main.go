// Pandora inventory 服务入口(背包 / 经济,W5 ③ 2026-06-18)。
//
// 职责:玩家货币 + 背包道具持久化(pandora_trade 库,强依赖);大厅态道具使用 / 出售
// 走事务 + 幂等键(不变量 §9.7);战斗内即时道具走 UE GAS,不经本服务(ds-arch §0.1)。
//
// 启动顺序(对齐 player):
//  1. Logger
//  2. 加载 yaml → conf.Defaults
//  3. MySQL client + 隐含 Ping(强依赖:背包落库不可降级)
//  4. 装配 InventoryUsecase → InventoryService → gRPC/HTTP server
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

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	pkgconfig "github.com/luyuancpp/pandora/pkg/config"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pkgmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/server"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/service"
)

const serviceName = "inventory"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/inventory-dev.yaml", "config file path")
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

	// 校验道具规则表(可出售必须单价 > 0;非法配置 fail-fast,避免上线后负价扣币)。
	if verr := cfg.Inventory.Validate(); verr != nil {
		helper.Errorw("msg", "inventory_item_rules_invalid", "err", verr)
		os.Exit(1)
	}

	// 校验背包域配置(段容量 / 堆叠上限;非法配置 fail-fast,bag-domain.md §5.2)。
	if verr := cfg.Bag.Validate(); verr != nil {
		helper.Errorw("msg", "bag_conf_invalid", "err", verr)
		os.Exit(1)
	}

	// 3. MySQL(强依赖:背包 / 货币落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_trade)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 4. 装配链
	repo := data.NewMySQLInventoryRepo(db)
	uc := biz.NewInventoryUsecase(repo, cfg.Inventory)

	// Snowflake(instance_id 生成,W5 ④):仅在启用实例背包(capacity>0)时装配。
	// node_id_source=static 静态,=etcd 走 etcd 自动抢占,失租自动退出。
	if cfg.Inventory.Capacity > 0 {
		// 启动期 schema 检查(2026-07-08):player_item_instance 是后补的表,既有 MySQL
		// volume / PVC 不会自动重放 init SQL;缺表时实例背包全链路必炸。fail-fast 并指向迁移 SQL。
		// 未启用(capacity<=0)不检也不读该表(biz.GetInventoryFull 同步跳过),旧库升级不受影响。
		schemaCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if serr := mysqlx.CheckTables(schemaCtx, db, "deploy/mysql-init/08-inventory-tables.sql", "player_item_instance"); serr != nil {
			cancel()
			helper.Errorw("msg", "mysql_schema_check_failed", "err", serr)
			os.Exit(1)
		}
		cancel()

		sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
		defer func() { _ = sfCloser.Close() }()
		uc.SetSnowflake(sf)
		helper.Infow("msg", "instance_bag_enabled", "capacity", cfg.Inventory.Capacity, "identify_rules", len(cfg.Inventory.IdentifyRules))
	}
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewInventoryService(uc)

	// 保留期清理:周期批量回收 inventory_ledger / auction_escrow(closed) 超期行,
	// 保证只增表增长有界(biz/sweep.go,CLAUDE.md §9 不变量 24)。
	// 多副本各自跑,DELETE 幂等无需锁(对齐 mail sweep)。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go runRetentionSweep(sweepCtx, uc, cfg.Inventory.SweepInterval.Std())
	helper.Infow(
		"msg", "retention_sweep_enabled",
		"interval", cfg.Inventory.SweepInterval.Std().String(),
		"batch", cfg.Inventory.SweepBatch,
		"ledger_retention_days", cfg.Inventory.LedgerRetentionDays,
		"escrow_retention_days", cfg.Inventory.EscrowRetentionDays,
	)

	// 背包域(pandora.bag.v1,bag-domain.md phase 1 由本进程承载):
	// bag.dsn 为空 = 未启用(不注册 BagService,现网行为不变,安全默认)。
	var bagSvc *service.BagService
	if cfg.Bag.DSN != "" {
		bagDB := mysqlx.MustNewClient(pkgconfig.MySQLConf{DSN: cfg.Bag.DSN})
		defer func() { _ = bagDB.Close() }()
		// 启动期 schema gate:pandora_bag 是后建库,既有 MySQL volume 不会自动重放 init SQL;
		// 缺表时背包域全链路必炸,fail-fast 并指向迁移 SQL。
		bagSchemaCtx, bagCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if serr := mysqlx.CheckTables(bagSchemaCtx, bagDB, "deploy/mysql-init/14-bag-tables.sql",
			"bag_meta", "bag_checkpoint", "bag_section", "bag_journal", "bag_generation", "bag_migration", "bag_capacity"); serr != nil {
			bagCancel()
			helper.Errorw("msg", "bag_schema_check_failed", "err", serr)
			os.Exit(1)
		}
		bagCancel()

		bagRepo := data.NewMySQLBagRepo(bagDB)
		bagUC := biz.NewBagUsecase(bagRepo, cfg.Bag)
		// 容量购买扣费(§5.3):经济域同进程直用 inventory repo(trade 库 ledger 幂等)。
		bagUC.SetCapacityCharger(repo)
		// 五要件② owner 授权(phase 2 写权威切换):背包写路径逐调校验当前 ADMITTED owner。
		// owner_addr 缺省且未显式开 allow_unverified_owner → 拒启(生产禁止无授权写)。
		if cfg.Bag.OwnerAddr != "" {
			ownerAuth := data.NewGrpcOwnerAuthorizer(cfg.Bag.OwnerAddr)
			defer func() { _ = ownerAuth.Close() }()
			bagUC.SetOwnerAuthorizer(ownerAuth)
			helper.Infow("msg", "bag_owner_authorizer_ready", "owner_addr", cfg.Bag.OwnerAddr)
		} else if !cfg.Bag.AllowUnverifiedOwner {
			helper.Errorw("msg", "bag_owner_addr_required",
				"hint", "bag.owner_addr required (CLAUDE.md §9.6 要件②), or set bag.allow_unverified_owner for dev only")
			os.Exit(1)
		} else {
			helper.Warnw("msg", "bag_owner_unverified",
				"hint", "bag writes accepted WITHOUT owner authorization (dev only, never in production)")
		}
		bagSvc = service.NewBagService(bagUC)
		// 五要件① DS 凭据身份:ds_auth.mode=enforce 时验签抽取 pod/uid 供 owner target 全等校验。
		dsGuard, gerr := pkgmw.NewDSCallbackGuardFromConf(cfg.DSAuth)
		if gerr != nil {
			helper.Errorw("msg", "ds_auth_guard_init_failed", "err", gerr)
			os.Exit(1)
		}
		if dsGuard != nil {
			bagSvc.SetDSGuard(dsGuard)
			helper.Infow("msg", "bag_ds_guard_ready", "mode", dsGuard.Mode().String())
		}
		go runBagJournalSweep(sweepCtx, bagUC, helper, cfg.Inventory.SweepInterval.Std(), cfg.Inventory.SweepBatch)
		// 存量迁移(D5,decision-revisit-bag-replay-semantics.md):默认关;contract 阶段
		// 旧写路径冻结后开启,一次性幂等作业(重跑 no-op,多副本并发安全)。
		if cfg.Bag.LegacyMigrationEnabled {
			migUC := biz.NewBagMigrationUsecase(repo, bagRepo, cfg.Bag, logger)
			go runLegacyBagMigration(sweepCtx, migUC, helper)
			helper.Warnw("msg", "bag_legacy_migration_enabled",
				"hint", "只准在旧写路径(GrantItems/UseItem/SellItem/escrow)冻结后运行(D5 时序纪律)")
		}
		helper.Infow(
			"msg", "bag_domain_enabled",
			"dsn", maskDSN(cfg.Bag.DSN),
			"max_journal_batch", cfg.Bag.MaxJournalBatch,
			"hourly_journal_quota", cfg.Bag.HourlyJournalQuota,
			"section_capacities", len(cfg.Bag.SectionCapacities),
			"journal_retention_days", cfg.Bag.JournalRetentionDays,
		)
	}

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, bagSvc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"item_rules", len(cfg.Inventory.ItemRules),
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

// runRetentionSweep 周期跑一轮保留期清理(对齐 mail runMailSweep 模式)。
func runRetentionSweep(ctx context.Context, uc *biz.InventoryUsecase, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "inventory_retention_sweep", func() { uc.SweepRetention(ctx) })
		}
	}
}

// runBagJournalSweep 周期清理超保留期背包流水(§9.24;多副本各自跑,DELETE 幂等无需锁)。
func runBagJournalSweep(ctx context.Context, uc *biz.BagUsecase, helper *klog.Helper, interval time.Duration, batch int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "bag_journal_sweep", func() {
				sweepCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if _, err := uc.RunJournalSweep(sweepCtx, batch); err != nil {
					helper.Errorw("msg", "bag_journal_sweep_failed", "err", err)
				}
			})
		}
	}
}

// runLegacyBagMigration 一次性存量迁移作业(D5;幂等可重跑,失败玩家逐个告警不阻断)。
func runLegacyBagMigration(ctx context.Context, uc *biz.BagMigrationUsecase, helper *klog.Helper) {
	sum, err := uc.RunOnce(ctx)
	if err != nil {
		helper.Errorw("msg", "bag_legacy_migration_aborted", "err", err,
			"scanned", sum.Scanned, "migrated", sum.Migrated, "skipped", sum.Skipped, "failed", sum.Failed)
		return
	}
	if sum.Failed > 0 {
		helper.Errorw("msg", "bag_legacy_migration_done_with_failures",
			"scanned", sum.Scanned, "migrated", sum.Migrated, "skipped", sum.Skipped, "failed", sum.Failed,
			"hint", "失败玩家已逐个告警(bound 实例等预期拦截),排障后重启作业重试")
		return
	}
	helper.Infow("msg", "bag_legacy_migration_done",
		"scanned", sum.Scanned, "migrated", sum.Migrated, "skipped", sum.Skipped, "failed", sum.Failed)
}

// maskDSN 脱敏 DSN 里的密码(对齐 player / trade main.go)。
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
