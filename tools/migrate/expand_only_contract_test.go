package main

// expand_only_contract_test.go — 「迁移默认只许 expand」的机械门禁(INC-20260812-001 行动项 A-2)。
//
// 存在的理由:2026-08-12 一次审查同时抓到两个**已发布**迁移做的是 contract 而不是 expand ——
// `pandora_account/000006` 用 RENAME+DROP 换掉角色编号三件套,`pandora_player/000007` 直接
// `DROP players.mmr`。迁移一执行,尚未排空的旧 Go 副本读写的对象**当场消失**,违反
// CLAUDE.md §9.16 / §9.21「删除能力必须走 expand → migrate → contract」。
//
// 这两条都是**人眼**在事后审查里发现的:此前的迁移契约测试只断言"某某片段存在"与
// fresh-init 一致性,没有任何一条断言"up.sql 不许出现 DROP / RENAME"。本文件把这道判断
// 机械化,让下一次写反的人在 `go test` 就红,而不是等迁移在生产上把旧副本打死。
//
// 判定规则:
//   - 只看 `*.up.sql`。down 迁移删掉自己刚建的对象是正常的,不在本门禁范围。
//   - 先剥掉 `-- ` 行注释(注释里成段解释这条规则本身属正常,见 stripLineComments)。
//   - 命中破坏性 DDL 的 up 迁移必须二选一:
//       ① 文件头显式标注 `-- CONTRACT:` 并写明**旧副本排空判据**(谁排空、怎么确认);
//       ② 在 grandfatheredContractMigrations 里登记(仅限门禁上线前已对 origin 暴露、
//          因而不可再修改的历史迁移),并写清它删了什么、兼容面是否已被后续 expand 补回。
//
// 反向门禁见 TestGrandfatheredContractListIsExact:allowlist 里的条目必须**确实还是**
// 破坏性迁移,否则这张表会退化成一张没人维护的永久豁免后门。

import (
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// destructiveDDL 是「会让旧副本的 SQL 目标对象消失」的语句形态。
// 只列真正拆兼容面的动作:ADD / MODIFY COMMENT / CREATE 一律不在内。
var destructiveDDL = []struct {
	name string
	re   *regexp.Regexp
}{
	{"DROP COLUMN", regexp.MustCompile(`(?i)\bDROP\s+COLUMN\b`)},
	{"DROP TABLE", regexp.MustCompile(`(?i)\bDROP\s+TABLE\b`)},
	{"DROP INDEX", regexp.MustCompile(`(?i)\bDROP\s+(INDEX|KEY)\b`)},
	{"RENAME COLUMN", regexp.MustCompile(`(?i)\bRENAME\s+COLUMN\b`)},
	{"RENAME INDEX", regexp.MustCompile(`(?i)\bRENAME\s+INDEX\b`)},
	{"RENAME TABLE", regexp.MustCompile(`(?i)\bRENAME\s+TABLE\b`)},
}

// contractMarker 是显式声明「本版就是 contract,我知道自己在删什么」的标记。
// drainCriterionMarker 强制同一份文件里必须写出旧副本排空判据 —— 只喊 CONTRACT
// 不写判据等于把 §9.21 的举证义务跳过去了。
const (
	contractMarker       = "-- CONTRACT:"
	drainCriterionMarker = "旧副本排空判据"
)

// grandfatheredContractMigrations 登记本门禁上线(2026-08-12)之前**已经对 origin 暴露**、
// 按 tools/migrate/README 已不可再修改的破坏性迁移。
//
// ⚠️ 这张表**只减不增**。新写的迁移一律走 expand;确实需要 contract 的走 contractMarker
// 显式标注路径,不许往这里加行。
var grandfatheredContractMigrations = map[string]string{
	"migrations/pandora_account/000005_rename_player_no.up.sql": "" +
		"RENAME COLUMN/INDEX/TABLE 把 register_no 三件套改名成 player_no。改名当时只论证了" +
		"「生产零注册路径、无存量数据」(数据无风险),没论证二进制共存无风险。兼容面已由" +
		"000007_player_no_expand_compat 重新建回并双写;INC-20260812-001。",
	"migrations/pandora_account/000006_reconcile_player_no.up.sql": "" +
		"同 000005 的收敛版:双对象库里 DROP INDEX uk_register_no / DROP COLUMN register_no / " +
		"DROP TABLE register_no_counter。兼容面已由 000007_player_no_expand_compat 补回;INC-20260812-001。",
	"migrations/pandora_player/000007_rating_pool_partition.up.sql": "" +
		"DROP players.mmr(且因列上挂着 idx_mmr,ALGORITHM=INSTANT 在 MySQL 8.4 必报 1845 → v7 dirty)。" +
		"兼容面已由 000008_rating_pool_expand_compat 补回并双写;INC-20260812-001 行动项 A-1 要求" +
		"未来的 contract 不得原样重放这条语句。",
	"migrations/pandora_leaderboard/000003_reward_proto_binary.up.sql": "" +
		"DROP COLUMN reward_json 与 ADD COLUMN reward_pb 在同一条 ALTER 里(json→pb 表示法切换)。" +
		"未经 expand 窗口;若重来应拆成加列→双写→排空→删列。",
	"migrations/pandora_trade/000004_attributes_proto_binary.up.sql": "" +
		"DROP COLUMN attributes(player_item_instance / mail_transfer_escrow 两张表),同为 " +
		"json→pb 表示法切换,同样未经 expand 窗口。",
}

// TestMigrationsAreExpandOnly 是主门禁。
func TestMigrationsAreExpandOnly(t *testing.T) {
	violations := make([]string, 0)

	forEachUpMigration(t, func(file, stripped, raw string) {
		hits := destructiveHits(stripped)
		if len(hits) == 0 {
			return
		}
		if _, ok := grandfatheredContractMigrations[file]; ok {
			return
		}
		if strings.Contains(raw, contractMarker) {
			if !strings.Contains(raw, drainCriterionMarker) {
				violations = append(violations, file+
					" 标了 "+contractMarker+" 但没写「"+drainCriterionMarker+
					"」:contract 必须写清谁排空、怎么确认旧副本归零,否则等于跳过 §9.21 的举证义务")
			}
			return
		}
		violations = append(violations, file+" 含破坏性 DDL "+strings.Join(hits, " / "))
	})

	if len(violations) == 0 {
		return
	}
	sort.Strings(violations)
	t.Fatalf("迁移默认只许 expand(CLAUDE.md §9.16/§9.21,INC-20260812-001)。\n"+
		"旧副本在滚动升级窗口内仍会读写这些对象,迁移一执行就当场打死它们。\n"+
		"正确做法:加新对象 → 新旧双写 → 确认旧副本排空 → 另立更高版本迁移收缩。\n"+
		"确实是 contract 的,在文件头写 %q 并说明「%s」。\n违规:\n  %s",
		contractMarker, drainCriterionMarker, strings.Join(violations, "\n  "))
}

// TestGrandfatheredContractListIsExact 防止 allowlist 腐化:登记过的文件必须**确实还是**
// 破坏性迁移,而且必须真实存在。历史迁移不可改,所以这两条恒该成立;一旦不成立,说明
// 有人改了不可改的文件,或者把条目留在表里当永久后门。
func TestGrandfatheredContractListIsExact(t *testing.T) {
	seen := make(map[string]bool, len(grandfatheredContractMigrations))

	forEachUpMigration(t, func(file, stripped, _ string) {
		if _, ok := grandfatheredContractMigrations[file]; !ok {
			return
		}
		seen[file] = true
		if len(destructiveHits(stripped)) == 0 {
			t.Errorf("%s 登记在 grandfathered 清单里,但已不含任何破坏性 DDL —— "+
				"要么这份不可改的历史迁移被人改过,要么该把它从清单里删掉", file)
		}
	})

	for file, reason := range grandfatheredContractMigrations {
		if !seen[file] {
			t.Errorf("grandfathered 清单里的 %s 在嵌入迁移里不存在(路径写错或文件被删):%s", file, reason)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("grandfathered 清单里的 %s 没写理由 —— 每条豁免都必须说明删了什么、兼容面补回了没有", file)
		}
	}
}

func destructiveHits(stripped string) []string {
	hits := make([]string, 0, len(destructiveDDL))
	for _, d := range destructiveDDL {
		if d.re.MatchString(stripped) {
			hits = append(hits, d.name)
		}
	}
	return hits
}

// forEachUpMigration 只遍历 up 迁移,同时把「剥注释后的正文」与「原始正文」都交给回调:
// 破坏性 DDL 要在剥注释后判(注释里讲解规则不算违规),而 CONTRACT 标记恰恰写在注释里。
func forEachUpMigration(t *testing.T, visit func(file, stripped, raw string)) {
	t.Helper()
	count := 0
	err := fs.WalkDir(migrationsFS, "migrations", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path.Base(p), ".up.sql") {
			return nil
		}
		raw, rerr := fs.ReadFile(migrationsFS, p)
		if rerr != nil {
			return rerr
		}
		count++
		visit(p, stripLineComments(string(raw)), string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("遍历嵌入 up 迁移: %v", err)
	}
	// 解析坏掉时(比如 embed 路径变了)必须炸,不能静默零遍历后打绿。
	if count < 20 {
		t.Fatalf("只遍历到 %d 份 up 迁移,遍历八成坏了", count)
	}
}
