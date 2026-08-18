package configtable

import (
	"strings"
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

// TestValidateMinTeamSize 直进人数下限的加载期校验:
//   - 0 = 无下限,合法(本列上线前的默认态,滚动混版下旧表照常加载);
//   - 填了下限就必须同时填上限(上限留 0 表示"沿用全局",全局值逐部署不同,加载期无从比对);
//   - 下限不得大于上限——那会让该图任何人数都进不去,是个静默拒服务,必须挡在加载边界。
func TestValidateMinTeamSize(t *testing.T) {
	unset := battleRow(20, "无下限")
	unset.TeamSize = 5
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{unset}}); err != nil {
		t.Fatalf("min_team_size=0 应放行(无下限),得 %v", err)
	}

	ok := battleRow(21, "5 人本最少 3 人")
	ok.TeamSize, ok.MinTeamSize = 5, 3
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{ok}}); err != nil {
		t.Fatalf("min=3 ≤ team_size=5 应放行,得 %v", err)
	}

	eq := battleRow(22, "下限=上限(必须满员才准直进)")
	eq.TeamSize, eq.MinTeamSize = 5, 5
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{eq}}); err != nil {
		t.Fatalf("min=team_size 应放行,得 %v", err)
	}

	noMax := battleRow(23, "填了下限却没填上限")
	noMax.MinTeamSize = 3 // TeamSize 留 0
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{noMax}}); err == nil {
		t.Fatal("min_team_size>0 而 team_size=0 应被拦下(下限相对上限才有意义)")
	}

	inverted := battleRow(24, "下限大于上限")
	inverted.TeamSize, inverted.MinTeamSize = 3, 5
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{inverted}}); err == nil {
		t.Fatal("min_team_size>team_size 应被拦下(否则该图任何人数都进不去)")
	}
}

// TestValidatePhaseDurations 对局三段式两段窗口时长的加载期上限校验:
//   - 两列留 0 = 该段不存在,一律放行——这是两列上线前的默认态,旧批次表必须照常加载(§9.21);
//   - 上限内(含恰好等于上限)放行;
//   - 超上限整表拒绝。误配这两个数不会崩任何东西,只会让整图玩家静默卡在一段
//     什么都做不了的时间里(准备期还不能打 / 结算期已打完只等退场),
//     是最难从现场反推回配置的一类故障,所以必须挡在加载边界而不是等玩家反馈。
func TestValidatePhaseDurations(t *testing.T) {
	unset := battleRow(40, "两段都没配")
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{unset}}); err != nil {
		t.Fatalf("prepare/settle 均为 0 应放行(该段不存在),得 %v", err)
	}

	ok := battleRow(41, "准备 20s 结算 10s")
	ok.PrepareDurationSeconds, ok.SettleDurationSeconds = 20, 10
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{ok}}); err != nil {
		t.Fatalf("上限内的准备/结算时长应放行,得 %v", err)
	}

	atMax := battleRow(42, "两段都恰好卡上限")
	atMax.PrepareDurationSeconds = MaxLevelPhaseDurationSeconds
	atMax.SettleDurationSeconds = MaxLevelPhaseDurationSeconds
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{atMax}}); err != nil {
		t.Fatalf("时长=MaxLevelPhaseDurationSeconds 应放行(闭区间),得 %v", err)
	}

	prepareOver := battleRow(43, "准备时长多打一个 0")
	prepareOver.PrepareDurationSeconds = MaxLevelPhaseDurationSeconds + 1
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{prepareOver}}); err == nil {
		t.Fatalf("prepare_duration_seconds>%d 应被拦下(玩家会被静默挡在开打之外)", MaxLevelPhaseDurationSeconds)
	}

	settleOver := battleRow(44, "结算时长多打一个 0")
	settleOver.SettleDurationSeconds = MaxLevelPhaseDurationSeconds + 1
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{settleOver}}); err == nil {
		t.Fatalf("settle_duration_seconds>%d 应被拦下(玩家会被静默压在结算界面)", MaxLevelPhaseDurationSeconds)
	}
}

// TestValidateRatingMode 计分模式的加载期校验:
//   - 未配置(0)一律放行——这是本列上线前的默认态,旧批次表必须照常加载(§9.21);
//   - NONE 与任何对局结构都相容(合作副本、对抗图都可以不计分);
//   - ELO 与 side_count=1 互斥:单方合作副本没有对手结构,Elo 算给谁都说不通,
//     属配置错配,必须挡在加载边界(整批不切换、保留旧表),而不是等打完一局
//     才在结算里给一群合作玩家互相扣分;
//   - side_count=0 是"沿用服务端默认 2 方",按 2 方对待,与 ELO 相容。
func TestValidateRatingMode(t *testing.T) {
	elo := configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO
	none := configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE

	unset := battleRow(30, "旧批次表没有这一列")
	unset.TeamSize, unset.SideCount = 3, 2
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{unset}}); err != nil {
		t.Fatalf("rating_mode 未配置应放行(旧表兼容),得 %v", err)
	}

	coopNone := battleRow(31, "合作副本不计分")
	coopNone.TeamSize, coopNone.SideCount, coopNone.RatingMode = 3, 1, none
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{coopNone}}); err != nil {
		t.Fatalf("单方合作副本 + 不计分应放行,得 %v", err)
	}

	pvpElo := battleRow(32, "双方对抗算段位")
	pvpElo.TeamSize, pvpElo.SideCount, pvpElo.RatingMode = 3, 2, elo
	pvpElo.RatingPool = "3v3_ranked"
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{pvpElo}}); err != nil {
		t.Fatalf("双方对抗 + ELO 应放行,得 %v", err)
	}

	defaultSides := battleRow(33, "方数留空沿用默认 2 方")
	defaultSides.TeamSize, defaultSides.SideCount, defaultSides.RatingMode = 3, 0, elo
	defaultSides.RatingPool = "5v5_ranked"
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{defaultSides}}); err != nil {
		t.Fatalf("side_count=0(默认 2 方)+ ELO 应放行,得 %v", err)
	}

	coopElo := battleRow(34, "合作副本却要算 Elo")
	coopElo.TeamSize, coopElo.SideCount, coopElo.RatingMode = 3, 1, elo
	coopElo.RatingPool = "3v3_ranked"
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{coopElo}}); err == nil {
		t.Fatal("side_count=1 + ELO 应被拦下(单方合作副本没有对手结构,无法算 Elo)")
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

// TestValidateRatingPool 段位池与计分模式必须配套(2026-08-11「3v3 与 5v5 不共用同一份段位」):
//   - ELO 必须填池:不填则结算无处落账,只能兜底进 default 池,表现为"这张图的分和别的
//     图混在一起算"且毫无报错 —— 正是本轮要消灭的静默错配;
//   - 非 ELO 必须留空:填了说明配的人以为它生效了,早报早改;
//   - 池名不设白名单(策划自由填,同值即同一份段位),但超长必须拒 —— 非严格 sql_mode 下
//     超长会被静默截断成**另一份**段位。
func TestValidateRatingPool(t *testing.T) {
	elo := configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO
	none := configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE

	eloNoPool := battleRow(40, "要算段位却没说算哪份")
	eloNoPool.TeamSize, eloNoPool.SideCount, eloNoPool.RatingMode = 5, 2, elo
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{eloNoPool}}); err == nil {
		t.Fatal("rating_mode=ELO 而 rating_pool 为空应被拦下(结算会兜底进 default 池,静默混算)")
	}

	noneWithPool := battleRow(41, "不计分却填了池")
	noneWithPool.TeamSize, noneWithPool.SideCount, noneWithPool.RatingMode = 3, 1, none
	noneWithPool.RatingPool = "3v3_ranked"
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{noneWithPool}}); err == nil {
		t.Fatal("不计分的图填了 rating_pool 应被拦下(配置误解,填了也不生效)")
	}

	unsetWithPool := battleRow(42, "计分模式没填却填了池")
	unsetWithPool.TeamSize, unsetWithPool.SideCount = 3, 2
	unsetWithPool.RatingPool = "5v5_ranked"
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{unsetWithPool}}); err == nil {
		t.Fatal("rating_mode 未填而 rating_pool 已填应被拦下(旧批次表兼容路径不该带池)")
	}

	// 分池是本次需求的核心:3v3 与 5v5 同为双方对抗,靠**不同池名**分开算分,
	// 两行都合法且互不影响(池名不同即两份段位)。
	for _, c := range []struct {
		id   uint32
		size uint32
		pool string
	}{{43, 3, "3v3_ranked"}, {44, 5, "5v5_ranked"}} {
		row := battleRow(c.id, "对抗图")
		row.TeamSize, row.SideCount, row.RatingMode, row.RatingPool = c.size, 2, elo, c.pool
		if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{row}}); err != nil {
			t.Fatalf("%dv%d + 池 %q 应放行,得 %v", c.size, c.size, c.pool, err)
		}
	}

	tooLong := battleRow(45, "池名超长")
	tooLong.TeamSize, tooLong.SideCount, tooLong.RatingMode = 5, 2, elo
	tooLong.RatingPool = strings.Repeat("p", 33) // 列宽 VARCHAR(32)
	if _, err := newLevelTable(&configpb.LevelTableData{Rows: []*configpb.LevelRow{tooLong}}); err == nil {
		t.Fatal("超长池名应被拦下(非严格 sql_mode 会静默截断成另一份段位)")
	}
}
