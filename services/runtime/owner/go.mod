module github.com/luyuancpp/pandora/services/runtime/owner

go 1.26.5

// owner 服务(每玩家 owner 权威,CLAUDE.md §9.22;docs/design/owner-authority.md,2026-07-21)。
//
// 职责:
//   owner_record 单调 owner_epoch CAS、PENDING/ADMITTED 两阶段、admit_not_before 迁移屏障、
//   DS 实例级租约(玩家 owner lease 由此派生)、迁移审计流水;
//   全部状态同库单事务(生产 TiDB:线性一致 + 法定多数 + 确认写不回滚;dev 单机 MySQL 联调)。
//
// 依赖来源:
//   - pkg/   (公共框架:mysqlx / grpcserver / middleware / metrics / placement / errcode / log)
//   - proto/ (owner/v1 + common/v1;pb 由 Codex proto_gen 重生)
//   - Kratos v2.9.2 / go-sql-driver(权威库)
//
// ⚠️ go.sum 由 Codex `go mod tidy` 生成(AGENTS.md §11.1);require 版本对齐 leaderboard。

require (
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/go-sql-driver/mysql v1.8.1
	github.com/google/uuid v1.6.0
	github.com/luyuancpp/pandora/pkg v0.0.0
	github.com/luyuancpp/pandora/proto v0.0.0-00010101000000-000000000000
)

// 本地 workspace 内的模块通过 replace 指向源码目录。
replace (
	github.com/luyuancpp/pandora/pkg => ../../../pkg
	github.com/luyuancpp/pandora/proto => ../../../proto
)
