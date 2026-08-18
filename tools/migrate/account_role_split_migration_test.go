package main

// account_role_split_migration_test.go 钉住 pandora_account 000008 的账号 / 角色分离契约。
//
// 000008 把「1 账号 = 1 player_id = 1 角色」拆成 accounts.account_id(账号身份)+
// account_roles(角色归属台账)。它是 expand:accounts 的 PK 与 player_id 一律不动,
// 旧 login 二进制在滚动升级窗口内照常注册 / 登录。本文件把这条纪律里**改一个字就会
// 打穿滚动升级或打穿重跑**的几处机械化。

import (
	"strings"
	"testing"
)

const (
	accountV8UpPath   = "migrations/pandora_account/000008_account_roles.up.sql"
	accountV8DownPath = "migrations/pandora_account/000008_account_roles.down.sql"

	accountRolesTableMarker = "CREATE TABLE IF NOT EXISTS `account_roles`"
)

func TestPandoraAccountV8AccountRoleSplitContract(t *testing.T) {
	version, err := latestMigrationVersion("pandora_account")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	// 精确钉住 latest:本套迁移的「最新版契约」由本用例持有。加 v9 的人必须先来这里,
	// 把这条 pin 让给新用例(旧用例降级成 `version < 8` 的下限),顺手复核下面每一条
	// 是否仍然成立 —— 而不是让新迁移悄悄落地、契约测试却还停在上一代。
	if version != 8 {
		t.Fatalf("pandora_account latest version=%d,期望=8", version)
	}

	up := readEmbeddedMigration(t, accountV8UpPath)
	for _, fragment := range []string{
		// 账号身份列 + 唯一键。NULL-able 是滚动升级的命门,见下方 forbidden 断言。
		"ADD COLUMN `account_id` BIGINT UNSIGNED NULL",
		"ADD UNIQUE KEY `uk_account_id` (`account_id`)",
		// fail-closed:库里已有非空且与 player_id 不一致的 account_id,说明别的路径写过
		// 账号身份,猜哪个权威都是错的,必须让迁移当场炸。
		"__pandora_account_id_backfill_conflict__",
		"`account_id` IS NOT NULL AND `account_id` <> `player_id`",
		// 回填必须可重跑:只补 NULL 行,绝不覆盖新 login 已经铸好的 account_id。
		"UPDATE `accounts` SET `account_id` = `player_id` WHERE `account_id` IS NULL",
		// 角色归属台账。
		accountRolesTableMarker,
		"UNIQUE KEY `uk_account_slot` (`account_id`, `slot`)",
		"KEY `idx_account_status` (`account_id`, `status`)",
		// 同上:INSERT IGNORE 保证重跑不覆盖已存在的角色行。
		"INSERT IGNORE INTO `account_roles`",
		"WHERE a.`account_id` IS NOT NULL",
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000008 up 缺少账号 / 角色分离契约片段 %q", fragment)
		}
	}

	// expand 纪律:本版只加不减。accounts 的 PK / player_id 一动,尚未排空的旧 login
	// 当场读写不到自己的对象;contract 必须等旧二进制退场后另立版本。
	upper := strings.ToUpper(up)
	for _, forbidden := range []string{
		"ALTER TABLE `ACCOUNTS` DROP COLUMN", "ALTER TABLE `ACCOUNTS` DROP INDEX",
		"ALTER TABLE `ACCOUNTS` DROP PRIMARY KEY", "DROP TABLE `ACCOUNT_ROLES`",
		"RENAME COLUMN", "RENAME INDEX", "RENAME TABLE",
		// 写成 NOT NULL 会让**旧**二进制的注册 INSERT(它不认识这列,不会赋值)直接失败,
		// 等于滚动升级窗口内全服注册中断。expand 期这列必须允许 NULL。
		"`ACCOUNT_ID` BIGINT UNSIGNED NOT NULL",
	} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("000008 是 expand,禁止破坏旧二进制的兼容面,发现 %q", forbidden)
		}
	}

	// 执行顺序:加列 → 冲突闸 → 首次写。
	// 冲突闸直接引用 `accounts`.`account_id`(不像 000006 走 PREPARE),所以它必须排在
	// 加列之后,否则在没有该列的库上会炸在 Unknown column 而不是走到闸口;闸又必须排在
	// 首次写之前,否则冲突库已经被覆写过才发现冲突。
	addColumnAt := strings.Index(up, "PREPARE stmt_v8_add_account_id FROM")
	guardAt := strings.Index(up, "SET @account_id_backfill_conflicts")
	firstWriteAt := strings.Index(up, "UPDATE `accounts` SET `account_id` = `player_id`")
	if addColumnAt < 0 || guardAt < 0 || firstWriteAt < 0 || addColumnAt > guardAt || guardAt > firstWriteAt {
		t.Fatalf("000008 必须按 add column → backfill guard → first write 排序: add=%d guard=%d write=%d",
			addColumnAt, guardAt, firstWriteAt)
	}

	if statements := executableStatements(readEmbeddedMigration(t, accountV8DownPath)); len(statements) != 0 {
		t.Fatalf("000008 down 必须是有解释的 no-op:删掉 account_id / account_roles 会让已按新模型"+
			"登录的玩家失去角色归属,EnterRole 只能 fail-closed 拒绝一切角色。实际语句: %v", statements)
	}
}

// TestPandoraAccountFreshInitMatchesV8 钉住 fresh-init 与迁移产物的一致性。
//
// 两条路径必须落到同一套对象:全新集群跑 deploy/*-init,存量库跑 000008。任一边漏掉
// account_id / account_roles,login 启动期的 CheckTables / CheckColumnSpecs 就会 fail-fast
// 拒启(services/account/login/cmd/login/main.go)——即"新装一套集群直接起不来"。
//
// ⚠️ tidb-init 尤其容易漏:生产 account 库在 TiDB(§9.22),而日常联调只装 mysql-init,
// 本地全绿也发现不了 TiDB 那份的漂移。2026-08-18 本次就漏了整张 account_roles。
func TestPandoraAccountFreshInitMatchesV8(t *testing.T) {
	for path, content := range map[string]string{
		"deploy/mysql-init/02-account-tables.sql": readRepoFile(t, "../../deploy/mysql-init/02-account-tables.sql"),
		"deploy/tidb-init/03-account-tidb.sql":    readRepoFile(t, "../../deploy/tidb-init/03-account-tidb.sql"),
	} {
		for _, fragment := range []string{
			"`account_id`", "uk_account_id",
			accountRolesTableMarker, "uk_account_slot", "idx_account_status",
		} {
			if !strings.Contains(content, fragment) {
				t.Errorf("%s 与 000008 canonical schema 漂移,缺少 %q", path, fragment)
			}
		}

		// 列宽/对齐用的空格在两份 init 里不一致,先折叠再比形状。
		collapsed := collapseSpaces(content)
		if !strings.Contains(collapsed, "`account_id` BIGINT UNSIGNED NULL") {
			t.Errorf("%s 的 accounts.account_id 必须可空(旧二进制注册留 NULL,新 login 下次登录补铸)", path)
		}
		if strings.Contains(collapsed, "`account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号身份") {
			t.Errorf("%s 把 accounts.account_id 写成了 NOT NULL,旧二进制的注册 INSERT 会直接失败", path)
		}

		// role_name 刻意没有唯一键:显示名唯一性权威只有 players.nickname(uk_nickname)一处。
		// 两个库各加一个唯一键,必然出现"这边写进去了、那边被拒"的分叉且无法自动收敛。
		if body := createTableBody(content, accountRolesTableMarker); body == "" {
			t.Errorf("%s 找不到 account_roles 的建表体", path)
		} else if hasUniqueKeyOn(body, "role_name") {
			t.Errorf("%s 给 account_roles.role_name 加了唯一键:唯一性权威只能有一处"+
				"(pandora_player.players.uk_nickname),两处各判必然分叉", path)
		}
	}

	// 迁移那份同样不许给 role_name 加唯一键。
	if body := createTableBody(readEmbeddedMigration(t, accountV8UpPath), accountRolesTableMarker); body == "" {
		t.Error("000008 up 找不到 account_roles 的建表体")
	} else if hasUniqueKeyOn(body, "role_name") {
		t.Error("000008 给 account_roles.role_name 加了唯一键:唯一性权威只能有一处")
	}
}

// createTableBody 取出 marker 起、到 `) ENGINE=` 收尾的建表体(列与索引定义都在此之间)。
// 刻意不按分号切:建表体尾部的 COMMENT='...' 里含中文标点,按分号切容易切歪。
func createTableBody(text, marker string) string {
	start := strings.Index(text, marker)
	if start < 0 {
		return ""
	}
	rest := text[start:]
	end := strings.Index(rest, "\n) ENGINE=")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// hasUniqueKeyOn 判断建表体里有没有覆盖该列的唯一键(含 UNIQUE KEY / UNIQUE INDEX)。
func hasUniqueKeyOn(body, column string) bool {
	for _, line := range strings.Split(body, "\n") {
		upper := strings.ToUpper(line)
		if strings.Contains(upper, "UNIQUE") && strings.Contains(line, "`"+column+"`") {
			return true
		}
	}
	return false
}
