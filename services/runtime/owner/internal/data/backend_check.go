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
	"errors"
	"fmt"

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

// AssertSourceRevisionColumn 校验 owner_record 已有 hub_source_revision 列
// (INC-20260818-003 分阶段发布第 1 步 expand DDL)。
//
// 为什么必须 fail-fast 而不是等运行期报错:本版 owner 的 selectRecordCols 已经把该列写进
// SELECT,列不存在时**每一次** Query / BeginTransition / Admit 都会以 Error 1054 失败 ——
// 表现为整个 owner 权威面瘫痪,而 §9.23 的进场链以 owner 为第一权威,后果是全服进不去。
// 那种失败在启动日志里毫无痕迹,只能从各服务的下游报错反推,极难定位。
//
// 这是 §9.24「唯一允许因数据库检查而拒绝启动」那条纪律的同类场景:漏跑一步 DDL 就让
// 新镜像整个不可用,启动时一句明确的错误远比运行期海量 1054 好查。
//
// 用 information_schema 而不是 `SELECT hub_source_revision ... LIMIT 1`:后者在空表上
// 也返回零行成功,列不存在与表为空无法区分。
func AssertSourceRevisionColumn(ctx context.Context, db *sql.DB) error {
	const q = `SELECT COUNT(*) FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'owner_record'
   AND COLUMN_NAME = 'hub_source_revision'`
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return fmt.Errorf("探测 owner_record.hub_source_revision 失败: %w", err)
	}
	if n == 0 {
		return errors.New("owner_record 缺列 hub_source_revision(INC-20260818-003):" +
			"本版 owner 的 SELECT 已引用该列,缺列会让 owner 权威面每个 RPC 都报 Error 1054。" +
			"请先执行 expand DDL:" +
			"ALTER TABLE owner_record ADD COLUMN `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0" +
			"(TiDB 可加 IF NOT EXISTS;完整分阶段顺序见 " +
			"docs/incidents/2026-08-18-p1-hub-assignment-source-revision-rollout.md §5)")
	}
	return nil
}
