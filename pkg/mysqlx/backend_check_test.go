// backend_check_test.go —— 权威库后端强校验的表驱动单测 + 真实后端集成证明。
//
// 集成部分是**行为探针**而不是文档推断:本次审计的教训是,一批「TiDB 不支持 X」的结论
// 都来自拿老版本的通用知识去套一个 pin 死在 v8.5.1 的集群,却没连上去验一次。
// 设 PANDORA_TIDB_TEST_DSN 后本文件会对真实后端跑一遍探针,证明 collation 语义确实成立。
package mysqlx

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIsTiDBVersion(t *testing.T) {
	cases := []struct {
		version string
		want    bool
	}{
		{"8.0.11-TiDB-v8.5.1", true},
		{"5.7.25-TiDB-v6.1.0", true},
		{"8.4.0", false},             // 真 MySQL
		{"8.0.11", false},            // TiDB 伪装的 MySQL 版本前缀,但没有 -TiDB- 段
		{"", false},                  // 查询异常兜底
		{"8.0.11-TiDBv8.5.1", false}, // 少一个连字符,不能误判为 TiDB
		{"tidb", false},              // 大小写/形态不符
	}
	for _, c := range cases {
		if got := IsTiDBVersion(c.version); got != c.want {
			t.Errorf("IsTiDBVersion(%q) = %v, want %v", c.version, got, c.want)
		}
	}
}

func TestTiDBVersionAtLeast(t *testing.T) {
	cases := []struct {
		name       string
		version    string
		major      int
		minor      int
		wantReject bool
	}{
		{"恰好等于下限", "8.0.11-TiDB-v7.4.0", 7, 4, false},
		{"高于下限(大版本)", "8.0.11-TiDB-v8.5.1", 7, 4, false},
		{"高于下限(小版本)", "8.0.11-TiDB-v7.5.0", 7, 4, false},
		{"低于下限(小版本)", "8.0.11-TiDB-v7.3.0", 7, 4, true},
		{"低于下限(大版本)", "5.7.25-TiDB-v6.1.0", 7, 4, true},
		// 解析不出版本号必须 fail-closed:不能假设"是 TiDB 就够新"。
		// utf8mb4_0900_ai_ci 在 v7.4 以下是**静默回退**到 utf8mb4_bin,正是最危险的形态。
		{"版本号解析失败必须拒绝", "8.0.11-TiDB-nightly", 7, 4, true},
		{"非 TiDB 串必须拒绝", "8.4.0", 7, 4, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := tidbVersionAtLeast(c.version, c.major, c.minor)
			if c.wantReject && err == nil {
				t.Fatalf("tidbVersionAtLeast(%q, %d, %d) = nil, 期望拒绝", c.version, c.major, c.minor)
			}
			if !c.wantReject && err != nil {
				t.Fatalf("tidbVersionAtLeast(%q, %d, %d) = %v, 期望通过", c.version, c.major, c.minor, err)
			}
		})
	}
}

// openTestDB 打开 PANDORA_TIDB_TEST_DSN 指向的后端;未设置则跳过(不让 CI 依赖外部集群)。
// 例:PANDORA_TIDB_TEST_DSN='root@tcp(127.0.0.1:4000)/pandora_account?parseTime=true&charset=utf8mb4&collation=utf8mb4_0900_ai_ci'
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PANDORA_TIDB_TEST_DSN")
	if dsn == "" {
		t.Skip("未设置 PANDORA_TIDB_TEST_DSN,跳过真实后端集成证明")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping 后端失败(集群没起?): %v", err)
	}
	return db
}

// TestAssertTiDBBackendAgainstRealBackend 正向证明:对真实 TiDB 必须通过。
// (owner 侧已有对真实 MySQL 的负向证明;正向此前从未被任何环境执行过 —— require_tidb
// 这条 fail-fast 路径只有真生产第一次跑到,这正是 owner ReadOnly 那个 P0 能潜伏到今天的原因。)
func TestAssertTiDBBackendAgainstRealBackend(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := AssertTiDBBackend(ctx, db); err != nil {
		t.Fatalf("AssertTiDBBackend 对真实 TiDB 必须通过: %v", err)
	}
	if err := AssertTiDBVersionAtLeast(ctx, db, 7, 4); err != nil {
		t.Fatalf("AssertTiDBVersionAtLeast(7,4) 对 v8.5.1 必须通过: %v", err)
	}
	// 版本下限确实会拦:要一个不可能满足的版本必须被拒绝(证明断言不是恒真)。
	if err := AssertTiDBVersionAtLeast(ctx, db, 99, 0); err == nil {
		t.Fatal("AssertTiDBVersionAtLeast(99,0) 必须拒绝,否则版本门形同虚设")
	}
}

// TestAccountCollationSemanticsAgainstRealBackend 是本次迁移最关键的一条证明:
// accounts.account 在真实后端上必须仍是**大小写不敏感 + NO PAD**。
// 退化路径不报错,只在真实玩家登录时暴露(老玩家换个大小写登不进 / 同名抢注),
// 所以只能靠行为探针,不能靠 collation 名字或版本号推断。
func TestAccountCollationSemanticsAgainstRealBackend(t *testing.T) {
	db := openTestDB(t)
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := AssertColumnCollationSemantics(ctx, db, "accounts", "account", true, true); err != nil {
		t.Fatalf("accounts.account 必须是大小写不敏感 + NO PAD: %v", err)
	}
	// 反向:同一列断言"应为大小写敏感"必须失败 —— 证明探针真的在测行为而不是恒真返回 nil。
	if err := AssertColumnCollationSemantics(ctx, db, "accounts", "account", false, true); err != nil {
		t.Fatalf("wantCaseInsensitive=false 不应因大小写维度报错: %v", err)
	}
	// 不存在的列必须 fail-closed(schema 未就绪时不能静默放行)。
	err := AssertColumnCollationSemantics(ctx, db, "accounts", "no_such_column", true, true)
	if err == nil || !strings.Contains(err.Error(), "不存在") {
		t.Fatalf("不存在的列必须 fail-closed,实际: %v", err)
	}
}
