package main

// registry_test.go — 把"登记清单漂移"从只能靠真库发现,提前到 go test 就能发现。
//
// dbcheck 的第 1 步是拿真库的表去对内嵌 registry:没登记就 FAIL。问题是那一步**需要一个
// 真库**,而新表刚合进来时通常没人立刻拿生产库跑一遍 —— 于是"建表脚本里加了表、忘了登记"
// 这类漂移会一路活到发布门禁那天才爆(真实案例:battle_progress_action /
// battle_progress_item_balance 随 000009 合入,建表脚本、budgets、README 都有了,
// 唯独 registry 漏登记)。
//
// 本文件用仓库里的建表事实(deploy/mysql-init/*.sql 与 tools/migrate/migrations/**)
// 反向核对 registry,不连任何数据库。方向是单向的:**建表脚本里有的,registry 必须有**。
// 反过来不成立 —— registry 里有些表不由这两处建(data_service 的 player_data 是
// proto2mysql 运行期自动建表),那是合法的。

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	freshInitDir  = "../../../../deploy/mysql-init"
	migrationsDir = "../../migrations"
)

var (
	createTablePattern = regexp.MustCompile("(?i)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`([a-z0-9_]+)`")
	dropTablePattern   = regexp.MustCompile("(?i)DROP\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?`([a-z0-9_]+)`")
	renameTablePattern = regexp.MustCompile("(?i)RENAME\\s+TABLE\\s+`([a-z0-9_]+)`\\s+TO\\s+`([a-z0-9_]+)`")
	useDatabasePattern = regexp.MustCompile("(?i)^\\s*USE\\s+`?([a-z0-9_]+)`?\\s*;")
)

// sweptWithoutIndexAllowlist 是**故意**不声明清理索引的 swept 表。
// 每一条都必须说明为什么它的清理路径不需要额外索引,否则一律按缺索引处理。
var sweptWithoutIndexAllowlist = map[string]string{
	"pandora_social.chat_private_messages": "按雪花 message_id 做 PK 范围删,主键本身就是清理路径",
}

// TestFreshInitTablesAreRegistered:deploy/mysql-init 建的每张表都必须在 registry 里。
// 这是 dbcheck 第 1 步在"全新初始化的库"上会看到的表集合。
func TestFreshInitTablesAreRegistered(t *testing.T) {
	tables := collectFreshInitTables(t)
	if len(tables) < 40 {
		t.Fatalf("只从 %s 解析出 %d 张表,解析八成坏了", freshInitDir, len(tables))
	}
	assertRegistered(t, tables, "deploy/mysql-init")
}

// TestMigrationTablesAreRegistered:按 up 迁移顺序执行 CREATE / RENAME / DROP 后最终存在的
// 每张表都必须登记。不能把 immutable 历史迁移里曾创建、后来明确改名/删除的 legacy 表
// 当成最终表；也不能只扫 CREATE 而漏掉 RENAME 后的新名字。
func TestMigrationTablesAreRegistered(t *testing.T) {
	tables := collectMigrationTables(t)
	if len(tables) < 40 {
		t.Fatalf("只从 %s 解析出 %d 张表,解析八成坏了", migrationsDir, len(tables))
	}
	assertRegistered(t, tables, "tools/migrate/migrations")
}

// TestSweptTablesDeclareCleanupIndex:§9.24「清理列必须有可用索引」的机械化。
// swept 表不声明 RequiredIndexes,dbcheck 第 2 步就整段跳过它 —— 门禁看起来绿,
// 实际上那张表的清理有没有索引根本没人检查。要豁免必须写进 allowlist 并说明理由。
func TestSweptTablesDeclareCleanupIndex(t *testing.T) {
	for dbName, reg := range registry {
		for table, entry := range reg {
			if entry.Class != classSwept || len(entry.RequiredIndexes) > 0 {
				continue
			}
			key := dbName + "." + table
			if reason, ok := sweptWithoutIndexAllowlist[key]; ok {
				t.Logf("%s 豁免清理索引:%s", key, reason)
				continue
			}
			t.Errorf("%s 是 swept 表却没声明 RequiredIndexes:dbcheck 会跳过它的索引核对;"+
				"补索引 spec,或写进 sweptWithoutIndexAllowlist 并说明理由", key)
		}
	}
}

// TestPendingWhereOnlyOnSweptTables:PendingWhere 只在 swept 表上被 -pending 读到,
// 挂在 bounded/outbox/exempt 上是**静默失效**的死配置(写了以为有报告,其实没有)。
func TestPendingWhereOnlyOnSweptTables(t *testing.T) {
	for dbName, reg := range registry {
		for table, entry := range reg {
			if entry.PendingWhere != "" && entry.Class != classSwept {
				t.Errorf("%s.%s 是 %s 却配了 PendingWhere:-pending 只读 swept 表,这条永远不会执行",
					dbName, table, entry.Class)
			}
		}
	}
}

// TestSkillCardTablesRegistered 钉住 2026-08-10 技能卡三张表的登记形态(§9.24)。
//
// 单独写死一份而不是只靠上面的通用扫描:通用扫描只保证"登记了",这里保证"登记对了" ——
// 发放收据必须是 swept 且清理索引指向 created_at(dbcheck 拿它当发布门禁),
// 持卡 / 卡槽必须是 bounded(被玩家数 × 配置表行数 / 卡槽数有界,登记豁免不清理)。
func TestSkillCardTablesRegistered(t *testing.T) {
	player := registry["pandora_player"]

	for _, table := range []string{"player_skill_cards", "player_skill_slots"} {
		entry, ok := player[table]
		if !ok {
			t.Fatalf("pandora_player.%s 未登记", table)
		}
		if entry.Class != classBounded {
			t.Errorf("pandora_player.%s 类别=%s, 期望 bounded", table, entry.Class)
		}
	}

	grants, ok := player["skill_card_grants"]
	if !ok {
		t.Fatal("pandora_player.skill_card_grants 未登记")
	}
	if grants.Class != classSwept {
		t.Errorf("skill_card_grants 类别=%s, 期望 swept(只增的发放幂等收据)", grants.Class)
	}
	if grants.PendingWhere == "" {
		t.Error("skill_card_grants 缺 PendingWhere:-pending 报不出它的待清理量,report_only 下就没人知道积压多少")
	}
	wantIndex := indexSpec{Name: "idx_created", Columns: []string{"created_at"}}
	found := false
	for _, idx := range grants.RequiredIndexes {
		if idx.Name == wantIndex.Name && strings.Join(idx.Columns, ",") == strings.Join(wantIndex.Columns, ",") {
			found = true
		}
	}
	if !found {
		t.Errorf("skill_card_grants 缺清理索引断言 %v,实际 %v", wantIndex, grants.RequiredIndexes)
	}
}

// ── 解析辅助 ────────────────────────────────────────────────────────────────

// qualifiedTable 是 "db.table",带上它是在哪个文件里建的(报错时能直接定位)。
type qualifiedTable struct {
	db, table, source string
}

func assertRegistered(t *testing.T, tables []qualifiedTable, origin string) {
	t.Helper()
	var missing []string
	for _, qt := range tables {
		reg, ok := registry[qt.db]
		if !ok {
			missing = append(missing, qt.db+"(整个库未登记)← "+qt.source)
			continue
		}
		if _, ok := reg[qt.table]; !ok {
			missing = append(missing, qt.db+"."+qt.table+" ← "+qt.source)
		}
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	t.Errorf("%s 里建了表但 dbcheck registry 未登记(先在 CLAUDE.md §9.24 登记,再同步本清单):\n  %s",
		origin, strings.Join(missing, "\n  "))
}

func collectFreshInitTables(t *testing.T) []qualifiedTable {
	t.Helper()
	entries, err := os.ReadDir(freshInitDir)
	if err != nil {
		t.Fatalf("读取 %s: %v", freshInitDir, err)
	}
	var out []qualifiedTable
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		path := filepath.Join(freshInitDir, entry.Name())
		currentDB := ""
		for _, line := range strings.Split(readSQL(t, path), "\n") {
			if match := useDatabasePattern.FindStringSubmatch(line); match != nil {
				currentDB = strings.ToLower(match[1])
				continue
			}
			for _, table := range tableNames(line) {
				if currentDB == "" {
					t.Errorf("%s 在任何 `USE <db>;` 之前就建表 %s,无法判定归属库", entry.Name(), table)
					continue
				}
				out = append(out, qualifiedTable{db: currentDB, table: table, source: entry.Name()})
			}
		}
	}
	return out
}

func collectMigrationTables(t *testing.T) []qualifiedTable {
	t.Helper()
	sets, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("读取 %s: %v", migrationsDir, err)
	}
	var out []qualifiedTable
	for _, set := range sets {
		if !set.IsDir() {
			continue
		}
		// 迁移集目录名就是库名(分片库 pandora_auction_00 复用 pandora_auction 迁移集,
		// registry 也是按迁移集这一层登记的)。
		dbName := strings.ToLower(set.Name())
		files, derr := os.ReadDir(filepath.Join(migrationsDir, set.Name()))
		if derr != nil {
			t.Fatalf("读取 %s: %v", set.Name(), derr)
		}
		active := make(map[string]qualifiedTable)
		for _, file := range files {
			if !strings.HasSuffix(file.Name(), ".up.sql") {
				continue
			}
			path := filepath.Join(migrationsDir, set.Name(), file.Name())
			source := set.Name() + "/" + file.Name()
			for _, event := range migrationTableEvents(readSQL(t, path)) {
				switch event.kind {
				case "create":
					active[event.to] = qualifiedTable{db: dbName, table: event.to, source: source}
				case "rename":
					delete(active, event.from)
					active[event.to] = qualifiedTable{db: dbName, table: event.to, source: source}
				case "drop":
					delete(active, event.from)
				}
			}
		}
		var names []string
		for table := range active {
			names = append(names, table)
		}
		sort.Strings(names)
		for _, table := range names {
			out = append(out, active[table])
		}
	}
	return out
}

type migrationTableEvent struct {
	at       int
	kind     string
	from, to string
}

// migrationTableEvents 同时识别裸 DDL 与 PREPARE 字符串里的 DDL。这里只建模表名生命周期，
// 不判断条件表达式；迁移的目标形态必须在成功执行后唯一，因此 rename/drop 都代表旧名退出。
func migrationTableEvents(content string) []migrationTableEvent {
	var events []migrationTableEvent
	for _, match := range createTablePattern.FindAllStringSubmatchIndex(content, -1) {
		events = append(events, migrationTableEvent{at: match[0], kind: "create", to: strings.ToLower(content[match[2]:match[3]])})
	}
	for _, match := range renameTablePattern.FindAllStringSubmatchIndex(content, -1) {
		events = append(events, migrationTableEvent{
			at: match[0], kind: "rename",
			from: strings.ToLower(content[match[2]:match[3]]),
			to:   strings.ToLower(content[match[4]:match[5]]),
		})
	}
	for _, match := range dropTablePattern.FindAllStringSubmatchIndex(content, -1) {
		events = append(events, migrationTableEvent{at: match[0], kind: "drop", from: strings.ToLower(content[match[2]:match[3]])})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].at < events[j].at })
	return events
}

// tableNames 取一行里 CREATE TABLE 的表名。逐行处理是为了让 `USE` 的作用域好判定;
// CREATE TABLE 与表名在本仓所有建表脚本里都写在同一行(不满足时上面的行数断言会先报警)。
func tableNames(line string) []string {
	var out []string
	for _, match := range createTablePattern.FindAllStringSubmatch(line, -1) {
		out = append(out, strings.ToLower(match[1]))
	}
	return out
}

// readSQL 读文件并去掉 `-- ` 行注释(注释里成段解释表结构时会出现 CREATE TABLE 字样)。
func readSQL(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}
