// backend_check.go — owner 权威库后端强校验(§9.22)。
//
// owner CAS 依赖「线性一致 + 确认写不回滚」:MySQL 异步复制主从切换会回滚已确认写,
// 一次回滚就可能让两台 DS 同时拿到同一玩家的 owner(双 owner 脑裂),生产禁用。
// dev 单机 MySQL(无复制)天然满足,故校验由配置 require_tidb 驱动:
// -Prod 产物由 gen_cluster_config.ps1 机械注入 require_tidb: true,dev 模板保持 false。
//
// 2026-07-27:实现下沉到 pkg/mysqlx —— login 迁 TiDB 后成为第二个使用方,而各服务是独立
// go module(services/*/go.mod),owner 的 internal/data 无法被 login import,不下沉就只能
// 复制一份;两份各自实现的安全校验必然漂移(§15.5)。本文件保留同名薄封装,不改既有调用点
// 与 backend_check_test.go。
package data

import (
	"context"
	"database/sql"

	"github.com/luyuancpp/pandora/pkg/mysqlx"
)

// AssertTiDBBackend 校验连接的权威库确为 TiDB,不是则返回错误(调用方 fail-fast 退出)。
// TiDB 的 VERSION() 形如 "8.0.11-TiDB-v8.1.0",以 "-TiDB-" 特征串判定。
func AssertTiDBBackend(ctx context.Context, db *sql.DB) error {
	return mysqlx.AssertTiDBBackend(ctx, db)
}

// isTiDBVersion 判定 VERSION() 返回串是否 TiDB(保留供本包既有单测直接断言)。
func isTiDBVersion(version string) bool {
	return mysqlx.IsTiDBVersion(version)
}
