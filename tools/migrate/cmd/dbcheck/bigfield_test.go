package main

// bigfield_test.go — 把"大字段登记漂移"从只能靠真库 -size-check 发现,提前到 go test。
//
// 与 registry_test.go 同源思路(那边管行数登记,这边管大字段登记),同样不连任何数据库:
// 用仓库里的建表事实(deploy/mysql-init/*.sql 与 tools/migrate/migrations/**)双向核对
// bigfield.go 的 bigFields 清单。
//
// 真实案例(2026-08-11):mission 三个大字段(progress / reward_pb / payload)在
// §9.24 表格、dbcheck 行数 registry、budgets.go 里都登记了,唯独 bigFields 漏了 ——
// 结果 `-size-check` 对任务域完全不体检,深度失控(单行越来越胖)无人看得见,而
// 建表脚本、单测、行数检查全绿。
//
// 双向:
//
//	① 正向:bigFields 里的 (库, 表, 列) 必须真的在建表脚本里存在 —— 挡住改名/笔误
//	   造成的"登记了但永远匹配不上、静默不体检"。
//	② 反向:建表脚本里凡"集合序列化"型大列(BLOB 家族 / JSON / VARBINARY(≥256))
//	   必须登记或进 allowlist —— 挡住新表加了 blob 却忘登记。
//	   VARBINARY(<256) 不在管辖范围:那个尺寸装的是定长标量(hash / token / 指纹),
//	   不会"越长越胖",登记它只会稀释信号。

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// bigFieldAllowlist 是**故意**不做大字段体检的大列(键 = 库.表.列)。
// 每条都必须说明为什么它不需要体检,否则一律按漏登记处理。
//
// ⚠️ 下面 6 条是 2026-08-11 引入本测试时的**存量欠账**,不是"确认无需体检":
// 它们都是 mail / player 域的 pb blob 列,与已登记的 player_mail.payload 同型,
// 缺的只是有人去推 MaxBytes 口径(按 §9.24"按设计期望定,不按列类型上限定")。
// 补登记时把对应条目从本表删掉即可,不需要改测试逻辑。
var bigFieldAllowlist = map[string]string{
	"pandora_social.sys_mail.payload":                 "存量欠账:与 player_mail.payload 同型(MailContentStorageRecord),待推 MaxBytes 口径后登记",
	"pandora_social.guild_mail.payload":               "存量欠账:同上",
	"pandora_social.player_mail_archive.payload":      "存量欠账:同上(归档表原样搬运 player_mail.payload)",
	"pandora_social.player_mail_claim.intent_payload": "存量欠账:MailClaimIntentStorageRecord,直连链为 NULL,待推口径后登记",
	"pandora_battle.player_update_outbox.payload":     "存量欠账:PlayerUpdateEvent(VARBINARY(512)),出箱投递成功即删,深度风险低于其它 blob",
	"pandora_player.player_push_outbox.payload":       "存量欠账:同上(事件 message pb,VARBINARY(512))",
}

var (
	// 注意 VARBINARY(N) 以 `)` 结尾:后面跟空格时 `\b` **不成立**(两侧都是非词字符),
	// 用显式的"后随空白/逗号/行尾"替代,否则全部 VARBINARY 列会被静默漏掉。
	bigFieldColumnPattern = regexp.MustCompile(
		"(?i)^\\s*`([a-z0-9_]+)`\\s+(VARBINARY\\s*\\(\\s*([0-9]+)\\s*\\)|TINYBLOB|BLOB|MEDIUMBLOB|LONGBLOB|JSON)(?:\\s|,|$)")
	bigFieldTableEndPattern = regexp.MustCompile(`^\s*\)\s*ENGINE`)
)

// ddlBigColumns 扫建表脚本,返回 "库.表.列" → true(只收本测试管辖的大列)。
// 同时返回 "库.表.列" 的全集(含小 VARBINARY),供正向核对用。
func ddlBigColumns(t *testing.T) (big map[string]bool, all map[string]bool) {
	t.Helper()
	big = map[string]bool{}
	all = map[string]bool{}

	// 归属库的判定方式两边不同(与 registry_test 一致):fresh-init 靠 `USE <db>;`,
	// 迁移靠所在目录名。
	type sqlFile struct {
		path string
		db   string // 空 = 由文件内 USE 决定
	}
	var files []sqlFile

	entries, err := os.ReadDir(freshInitDir)
	if err != nil {
		t.Fatalf("读取 %s: %v", freshInitDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, sqlFile{path: filepath.Join(freshInitDir, entry.Name())})
	}
	sets, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("读取 %s: %v", migrationsDir, err)
	}
	for _, set := range sets {
		if !set.IsDir() {
			continue
		}
		setFiles, derr := os.ReadDir(filepath.Join(migrationsDir, set.Name()))
		if derr != nil {
			t.Fatalf("读取 %s: %v", set.Name(), derr)
		}
		for _, file := range setFiles {
			if !strings.HasSuffix(file.Name(), ".up.sql") {
				continue
			}
			files = append(files, sqlFile{
				path: filepath.Join(migrationsDir, set.Name(), file.Name()),
				db:   strings.ToLower(set.Name()),
			})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })

	for _, f := range files {
		db := f.db
		var table string
		for _, line := range strings.Split(readSQL(t, f.path), "\n") {
			if m := useDatabasePattern.FindStringSubmatch(line); m != nil {
				db = strings.ToLower(m[1])
				continue
			}
			if names := tableNames(line); len(names) > 0 {
				table = names[len(names)-1]
				continue
			}
			if table == "" || db == "" {
				continue
			}
			if bigFieldTableEndPattern.MatchString(line) {
				table = ""
				continue
			}
			m := bigFieldColumnPattern.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			key := db + "." + table + "." + strings.ToLower(m[1])
			all[key] = true
			// VARBINARY(<256) 是定长标量位(hash / token / 指纹),不会"越长越胖",不在管辖范围。
			if m[3] == "" || atoiDefault(m[3]) >= 256 {
				big[key] = true
			}
		}
	}
	if len(big) == 0 {
		t.Fatal("未从建表脚本解析出任何大列(正则与 DDL 写法漂移)")
	}
	return big, all
}

func atoiDefault(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// TestBigFieldsRegisteredColumnsExistInDDL —— 正向:登记的列必须真的存在。
func TestBigFieldsRegisteredColumnsExistInDDL(t *testing.T) {
	_, all := ddlBigColumns(t)
	for _, b := range bigFields {
		key := b.DB + "." + b.Table + "." + b.Column
		if all[key] {
			continue
		}
		// player_data 之类运行期自动建表的列不在建表脚本里,允许缺席但要显式说明。
		t.Errorf("bigFields 登记了建表脚本里不存在的列 %s —— 改名/笔误会让它永远匹配不上、静默不体检", key)
	}
}

// TestDDLBigColumnsAreRegistered —— 反向:大列必须登记或 allowlist。
func TestDDLBigColumnsAreRegistered(t *testing.T) {
	big, _ := ddlBigColumns(t)
	registered := map[string]bool{}
	for _, b := range bigFields {
		registered[b.DB+"."+b.Table+"."+b.Column] = true
	}
	var missing []string
	for key := range big {
		if registered[key] {
			if _, allowed := bigFieldAllowlist[key]; allowed {
				t.Errorf("%s 既在 bigFields 又在 allowlist —— 两处只能留一处", key)
			}
			continue
		}
		if _, allowed := bigFieldAllowlist[key]; allowed {
			continue
		}
		missing = append(missing, key)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("以下大列未登记进 bigFields 也不在 allowlist(-size-check 对它们完全不体检,"+
			"深度失控无人看得见):\n  %s", strings.Join(missing, "\n  "))
	}
	// allowlist 里的键必须真实存在,否则是删表后残留的死配置。
	for key := range bigFieldAllowlist {
		if !big[key] {
			t.Errorf("bigFieldAllowlist 里的 %s 在建表脚本里不存在(死配置,请删除)", key)
		}
	}
}
