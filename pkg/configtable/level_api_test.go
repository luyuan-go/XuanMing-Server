package configtable

import (
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// level_api_test.go — 生成的表访问 API(level_table.gen.go)与手写伴生钩子(level.go)测试。

func mustLevelTable(t *testing.T, rows ...*configpb.LevelRow) *LevelTable {
	t.Helper()
	tbl, err := newLevelTable(&configpb.LevelTableData{Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func battleRow(id uint32, name string) *configpb.LevelRow {
	return &configpb.LevelRow{Id: id, Name: name, AssetPath: "/Game/L/x.x",
		Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE}
}

func TestGeneratedAPI(t *testing.T) {
	tbl := mustLevelTable(t, battleRow(6, "MOBA战斗"), battleRow(7, "松林镇副本"), battleRow(9, "备用"))

	// ByIDs:缺失键跳过,结果按入参序
	got := tbl.ByIDs([]uint32{7, 999, 6})
	if len(got) != 2 || got[0].GetId() != 7 || got[1].GetId() != 6 {
		t.Fatalf("ByIDs=%v", got)
	}
	// Where / First
	if rows := tbl.Where(func(r *configpb.LevelRow) bool { return r.GetId() > 6 }); len(rows) != 2 {
		t.Fatalf("Where=%v", rows)
	}
	if row, ok := tbl.First(func(r *configpb.LevelRow) bool { return r.GetName() == "备用" }); !ok || row.GetId() != 9 {
		t.Fatalf("First=%v %v", row, ok)
	}
	if _, ok := tbl.First(func(r *configpb.LevelRow) bool { return false }); ok {
		t.Fatal("First 无命中应 false")
	}
	// RandOne:非空表必命中且行属于本表
	row, ok := tbl.RandOne()
	if !ok || !tbl.Exists(row.GetId()) {
		t.Fatalf("RandOne=%v %v", row, ok)
	}
	// 空表:RandOne false / 其余 API 零值安全
	empty := mustLevelTable(t)
	if _, ok := empty.RandOne(); ok {
		t.Fatal("空表 RandOne 应 false")
	}
	if empty.Count() != 0 || len(empty.ByIDs([]uint32{1})) != 0 {
		t.Fatal("空表 API 应零值安全")
	}
}

// TestLevelBitIndex 生成的稳定位序映射((excel_bit_index),level_bitindex.gen.go):
// 与 configtable/bitindex_state/level.json 同源,g_关卡 现网 ID 1-7 → 位 0-6。
func TestLevelBitIndex(t *testing.T) {
	if bit, ok := LevelBitIndex(1); !ok || bit != 0 {
		t.Fatalf("LevelBitIndex(1)=%d,%v", bit, ok)
	}
	if bit, ok := LevelBitIndex(7); !ok || bit != 6 {
		t.Fatalf("LevelBitIndex(7)=%d,%v", bit, ok)
	}
	if _, ok := LevelBitIndex(999); ok {
		t.Fatal("不存在的 ID 不应有位序")
	}
	if LevelBitCount < 7 {
		t.Fatalf("LevelBitCount=%d,应 ≥ 7", LevelBitCount)
	}
	// 位序互不重复(位图存储的硬前提)
	seen := map[uint32]uint32{}
	for id, bit := range levelBitIndexMap {
		if prev, dup := seen[bit]; dup {
			t.Fatalf("位 %d 被 id %d 与 %d 复用", bit, prev, id)
		}
		if bit >= LevelBitCount {
			t.Fatalf("id %d 位 %d 超出 LevelBitCount %d", id, bit, LevelBitCount)
		}
		seen[bit] = id
	}
}

// TestValidateHookWired 生成的 newLevelTable 必须调用手写 validateLevelRow(钩子接线守护)。
func TestValidateHookWired(t *testing.T) {
	bad := battleRow(6, "MOBA战斗")
	bad.AssetPath = ""
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{bad}}); err == nil {
		t.Fatal("asset_path 为空应被伴生钩子拦下")
	}
	bad2 := battleRow(7, "x")
	bad2.Category = configpb.LevelCategory_LEVEL_CATEGORY_UNSPECIFIED
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{bad2}}); err == nil {
		t.Fatal("category 未填应被伴生钩子拦下")
	}
}

// TestValidateTeamSizeUpperBound team_size 上限校验(防撮合 need=2*teamSize 预分配爆内存):
// 0(沿用全局)与 ≤MaxLevelTeamSize 放行,超上限整表拒绝(§9.15 加载失败保留旧表)。
func TestValidateTeamSizeUpperBound(t *testing.T) {
	zero := battleRow(6, "默认沿用全局") // TeamSize 默认 0
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{zero}}); err != nil {
		t.Fatalf("team_size=0 应放行(沿用全局兜底),得 %v", err)
	}
	atMax := battleRow(7, "上限内")
	atMax.TeamSize = MaxLevelTeamSize
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{atMax}}); err != nil {
		t.Fatalf("team_size=MaxLevelTeamSize 应放行,得 %v", err)
	}
	over := battleRow(8, "超上限")
	over.TeamSize = MaxLevelTeamSize + 1
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{over}}); err == nil {
		t.Fatalf("team_size>%d 应被拦下(防预分配爆内存)", MaxLevelTeamSize)
	}
}

// TestLevelPackagePath 关卡资源列 → UE 长包名:必须与 UE 侧
// APandoraDSLoaderGameMode::BuildTravelURL 同规则(点号后的对象名不能进地图路径)。
func TestLevelPackagePath(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"/Game/A/B.B", "/Game/A/B"},     // ObjectPath → 剥对象名
		{"/Game/A/B", "/Game/A/B"},       // 已是长包名,原样
		{"  /Game/A/B.B  ", "/Game/A/B"}, // 两端空白容错
		{"/Game/A.x/B", "/Game/A.x/B"},   // 点在斜杠之前:目录名带点,不得误剥
		{"/Game/StylizedCyberpunk/Levels/Sc.Sc", "/Game/StylizedCyberpunk/Levels/Sc"},
		{"", ""},
	} {
		if got := LevelPackagePath(tc.in); got != tc.want {
			t.Fatalf("LevelPackagePath(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestBattleLaunchURL 关卡表 → DS 启动 URL 的全部分支。
func TestBattleLaunchURL(t *testing.T) {
	withGameMode := battleRow(7, "松林镇")
	withGameMode.AssetPath = "/Game/Test/Level/SonglinTown.SonglinTown"
	withGameMode.GameModeClass = "/Script/Pandora.PandoraPveGameMode"

	noGameMode := battleRow(5, "测试场景") // GameMode类 列留空(真实表 id=5 就是这样)
	noGameMode.AssetPath = "/Game/Test/Level/_Test/Level_TopDown_Test02.Level_TopDown_Test02"

	login := &configpb.LevelRow{Id: 1, Name: "登录", AssetPath: "/Game/Level/Login/Lvl_Login.Lvl_Login",
		Category: configpb.LevelCategory_LEVEL_CATEGORY_LOGIN}

	tbl := mustLevelTable(t, withGameMode, noGameMode, login)

	got, err := tbl.BattleLaunchURL(7)
	if err != nil || got != "/Game/Test/Level/SonglinTown?game=/Script/Pandora.PandoraPveGameMode" {
		t.Fatalf("BattleLaunchURL(7)=(%q,%v)", got, err)
	}
	// game_mode_class 为空 → 不拼 ?game=,沿用关卡自带 GameMode(不塞猜的默认值)
	got, err = tbl.BattleLaunchURL(5)
	if err != nil || got != "/Game/Test/Level/_Test/Level_TopDown_Test02" {
		t.Fatalf("BattleLaunchURL(5)=(%q,%v)", got, err)
	}
	// 表里没有的 map_id → 报错(调用方据此让分配失败,绝不回退兜底图)
	if _, err := tbl.BattleLaunchURL(999); err == nil {
		t.Fatal("表里没有的 map_id 必须报错")
	}
	// 非战斗类关卡(登录/选角/主城)不能开局
	if _, err := tbl.BattleLaunchURL(1); err == nil {
		t.Fatal("非战斗类关卡必须拒绝")
	}
}

// TestValidateBattleLaunchURLs 批次级校验器:任一战斗关卡拼不出 URL 即整批拒绝;
// 非战斗类关卡不受本校验约束。
func TestValidateBattleLaunchURLs(t *testing.T) {
	ok := battleRow(6, "MOBA战斗")
	ok.AssetPath = "/Game/Test/Level/MobaLevel.MobaLevel"
	if err := mustLevelTable(t, ok).ValidateBattleLaunchURLs(); err != nil {
		t.Fatalf("合法批次不应报错: %v", err)
	}
	// asset_path 只有一个对象名(剥完为空)→ 该战斗关卡永远起不来,必须在加载边界拒绝。
	bad := battleRow(9, "坏行")
	bad.AssetPath = ".x"
	if err := mustLevelTable(t, ok, bad).ValidateBattleLaunchURLs(); err == nil {
		t.Fatal("战斗关卡拼不出 URL 必须整批拒绝")
	}
}
