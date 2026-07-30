// configtable_gate_test.go — StartMatch 关卡表准入门(不变量 §9.15 接线)测试。
// 用真实 pkg/configtable.Store 从临时目录加载批次,覆盖启用 / 未启用 / 热更三种形态。
package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// writeLevelBatch 在临时目录写一批完整配置表产物(与 tools/configtable-gen 同一 JSON 口径)。
func writeLevelBatch(t *testing.T, dir string, version uint64, rows []*configpb.LevelRow) {
	t.Helper()
	levelRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
		Marshal(&configpb.LevelTableData{Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	playerLevelExpRows := []*configpb.PlayerLevelExpRow{
		{Id: 1, Level: 1, UpgradeExp: 100, CumulativeExp: 0},
		{Id: 2, Level: 2, UpgradeExp: 0, CumulativeExp: 100},
	}
	playerLevelExpRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
		Marshal(&configpb.PlayerLevelExpTableData{Rows: playerLevelExpRows})
	if err != nil {
		t.Fatal(err)
	}
	// Store.Load 要求 manifest 覆盖本进程注册的**全部**表(缺一整批拒绝),
	// 所以即使 matchmaker 只用关卡表,批次也必须带上道具 / 专精表。
	itemRows := []*configpb.ItemRow{
		{Id: 10001, Name: "测试消耗品", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE,
			MaxStackSize: 99, Usable: true, UseHealHp: 50},
	}
	itemRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
		Marshal(&configpb.ItemTableData{Rows: itemRows})
	if err != nil {
		t.Fatal(err)
	}
	talentRows := []*configpb.TalentRow{
		{Id: 1, Name: "强击", MaxLevel: 5, CostPerLevel: 1},
	}
	talentRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
		Marshal(&configpb.TalentTableData{Rows: talentRows})
	if err != nil {
		t.Fatal(err)
	}

	levelSum := sha256.Sum256(levelRaw)
	playerLevelExpSum := sha256.Sum256(playerLevelExpRaw)
	itemSum := sha256.Sum256(itemRaw)
	talentSum := sha256.Sum256(talentRaw)
	manifest := map[string]any{
		"version":   version,
		"generator": "test",
		"tables": []map[string]any{
			{
				"name": "level", "file": "level.json",
				"proto":    "pandora.config.v1.LevelTableData",
				"checksum": "sha256:" + hex.EncodeToString(levelSum[:]),
				"rows":     len(rows),
			},
			{
				"name": "player_level_exp", "file": "player_level_exp.json",
				"proto":    "pandora.config.v1.PlayerLevelExpTableData",
				"checksum": "sha256:" + hex.EncodeToString(playerLevelExpSum[:]),
				"rows":     len(playerLevelExpRows),
			},
			{
				"name": "item", "file": "item.json",
				"proto":    "pandora.config.v1.ItemTableData",
				"checksum": "sha256:" + hex.EncodeToString(itemSum[:]),
				"rows":     len(itemRows),
			},
			{
				"name": "talent", "file": "talent.json",
				"proto":    "pandora.config.v1.TalentTableData",
				"checksum": "sha256:" + hex.EncodeToString(talentSum[:]),
				"rows":     len(talentRows),
			},
		},
	}
	mraw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "level.json"), levelRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "player_level_exp.json"), playerLevelExpRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "item.json"), itemRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "talent.json"), talentRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, configtable.ManifestFileName), mraw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func levelRows(includeMap6 bool) []*configpb.LevelRow {
	rows := []*configpb.LevelRow{
		{Id: 1, Name: "登录", AssetPath: "/Game/L/Login.Login",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_LOGIN},
		{Id: 7, Name: "松林镇副本", AssetPath: "/Game/L/SonglinTown.SonglinTown",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE},
	}
	if includeMap6 {
		rows = append(rows, &configpb.LevelRow{Id: 6, Name: "MOBA战斗",
			AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category:  configpb.LevelCategory_LEVEL_CATEGORY_BATTLE})
	}
	return rows
}

func TestStartMatch_MapGate(t *testing.T) {
	f := newFixtureWith(t, 8000, func(c *conf.MatchConf) { c.MapId = 6 })

	dir := t.TempDir()
	writeLevelBatch(t, dir, 100, levelRows(true))
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)
	ctx := context.Background()

	// 战斗类关卡放行
	if _, err := f.uc.StartMatch(ctx, 8101, 8101, 1001, 6); err != nil {
		t.Fatalf("map 6 应放行: %v", err)
	}
	// map_id=0 → 兜底 cfg.MapId=6,放行
	if _, err := f.uc.StartMatch(ctx, 8102, 8102, 1002, 0); err != nil {
		t.Fatalf("map 0(默认 6)应放行: %v", err)
	}
	// 非战斗类关卡(登录)拒绝
	if _, err := f.uc.StartMatch(ctx, 8103, 8103, 1003, 1); errcode.As(err) != errcode.ErrMatchInvalidMap {
		t.Fatalf("map 1(登录)应拒绝 ErrMatchInvalidMap: %v", err)
	}
	// 表里不存在的 map 拒绝
	if _, err := f.uc.StartMatch(ctx, 8104, 8104, 1004, 999); errcode.As(err) != errcode.ErrMatchInvalidMap {
		t.Fatalf("map 999 应拒绝 ErrMatchInvalidMap: %v", err)
	}
}

// TestStartMatch_MapGateHotReload 热更后新批次立即生效:删掉 map 6 → 后续 StartMatch 被拒。
func TestStartMatch_MapGateHotReload(t *testing.T) {
	f := newFixtureWith(t, 8200, func(c *conf.MatchConf) { c.MapId = 7 })

	v1 := t.TempDir()
	writeLevelBatch(t, v1, 100, levelRows(true))
	store := configtable.NewStore()
	if _, err := store.Load(v1, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 8201, 8201, 2001, 6); err != nil {
		t.Fatalf("热更前 map 6 应放行: %v", err)
	}

	v2 := t.TempDir()
	writeLevelBatch(t, v2, 200, levelRows(false)) // 新批次删掉 map 6
	res, err := store.Load(v2, 0)
	if err != nil || !res.Reloaded {
		t.Fatalf("热更失败: res=%+v err=%v", res, err)
	}
	if _, err := f.uc.StartMatch(ctx, 8202, 8202, 2002, 6); errcode.As(err) != errcode.ErrMatchInvalidMap {
		t.Fatalf("热更后 map 6 应被拒: %v", err)
	}
	// 默认副本 7 仍在表内,map 0 继续放行
	if _, err := f.uc.StartMatch(ctx, 8203, 8203, 2003, 0); err != nil {
		t.Fatalf("热更后 map 0(默认 7)应放行: %v", err)
	}
}

// TestStartMatch_MapGameModeCrossCheck 玩法模式交叉校验(CLAUDE.md §17.1 服务端一侧):
// 关卡表 game_mode 与本实例 cfg.GameMode 不等即拒——挡住伪造/选错的 x-pandora-game-mode
// 路由头,避免「PVE 图进 PVP 池空排队到 ticket TTL」的静默故障。
// 同时锁住 §9.21 兼容口径:留空(旧批次表无本列)只跳过校验,绝不据此拒绝,
// 否则新二进制 + 旧表在滚动升级窗口内会拒掉所有匹配。
func TestStartMatch_MapGameModeCrossCheck(t *testing.T) {
	// 本实例承接 pvp 池。
	f := newFixtureWith(t, 8500, func(c *conf.MatchConf) {
		c.MapId = 6
		c.GameMode = "5v5_ranked"
	})

	dir := t.TempDir()
	writeLevelBatch(t, dir, 100, []*configpb.LevelRow{
		{Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked"},
		{Id: 7, Name: "松林镇副本", AssetPath: "/Game/L/SonglinTown.SonglinTown",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "pve_coop"},
		{Id: 8, Name: "旧批次无本列", AssetPath: "/Game/L/Legacy.Legacy",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE},
	})
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)
	ctx := context.Background()

	// 同模式放行。
	if _, err := f.uc.StartMatch(ctx, 8501, 8501, 5001, 6); err != nil {
		t.Fatalf("同 game_mode 的 map 6 应放行: %v", err)
	}
	// 跨模式拒绝:pve_coop 的图被送进 5v5_ranked 实例。
	if _, err := f.uc.StartMatch(ctx, 8502, 8502, 5002, 7); errcode.As(err) != errcode.ErrMatchInvalidMap {
		t.Fatalf("跨 game_mode 的 map 7 应拒绝 ErrMatchInvalidMap: %v", err)
	}
	// 留空 = 无法判定,不是错误证据:必须放行(§9.21 新二进制兼容旧批次表)。
	if _, err := f.uc.StartMatch(ctx, 8503, 8503, 5003, 8); err != nil {
		t.Fatalf("game_mode 留空(旧批次表)应放行而非拒绝: %v", err)
	}
}

// TestStartMatch_SoloWithoutTeam 单人入口(team_id=0)必须放行:「单人」与「单人组队」在
// 协议层是同一件事,不该强迫玩家先建一个 1 人队(CLAUDE.md §17)。
// 同时锁住:名单来自 JWT 身份(captainID),不查 team 服务;单排进 5v5 图同样合法——
// 撮合按人数凑齐后由 binPack 装箱,5 张单人票天然凑满一方。
func TestStartMatch_SoloWithoutTeam(t *testing.T) {
	f := newFixtureWith(t, 8600, func(c *conf.MatchConf) {
		c.MapId = 6
		c.GameMode = "5v5_ranked"
		c.TeamSize = 5
	})
	dir := t.TempDir()
	writeLevelBatch(t, dir, 100, []*configpb.LevelRow{
		{Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked",
			TeamSize: 5, SideCount: 2,
			EntryMode: configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE},
	})
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)

	// team_id=0 单排进 5v5:受理成功,票据成员就是调用者本人。
	ticketID, err := f.uc.StartMatch(context.Background(), 8601, 0, 6001, 6)
	if err != nil {
		t.Fatalf("单人入口(team_id=0)应放行: %v", err)
	}
	if ticketID == 0 {
		t.Fatal("单人入口应返回有效票据句柄")
	}
}

// TestEntryModeAndSideCountFromLevelTable 锁住两列的读取与回退口径:
//   - entry_mode 决定直进/撮合,未配置时沿用部署级 walk_in(§9.21 旧批次表兼容);
//   - side_count 决定几方,未配置/表缺失回退 2(与历史 need=2×team_size 等价)。
func TestEntryModeAndSideCountFromLevelTable(t *testing.T) {
	// 部署级 walk_in=false(撮合部署),用于验证"未配置时沿用部署开关"。
	f := newFixtureWith(t, 8700, func(c *conf.MatchConf) {
		c.MapId = 6
		c.GameMode = "5v5_ranked"
		c.WalkIn = false
	})
	// 表未启用:两者都回退。
	if f.uc.isWalkInMap(6) {
		t.Fatal("tables=nil 应沿用部署 walk_in=false")
	}
	if got := f.uc.sideCountForMap(6); got != 2 {
		t.Fatalf("tables=nil 应回退 2 方,得 %d", got)
	}

	dir := t.TempDir()
	writeLevelBatch(t, dir, 100, []*configpb.LevelRow{
		{Id: 6, Name: "撮合图", AssetPath: "/Game/L/A.A",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked",
			SideCount: 2, EntryMode: configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE},
		{Id: 7, Name: "直进副本", AssetPath: "/Game/L/B.B",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked",
			SideCount: 1, EntryMode: configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN},
		{Id: 8, Name: "混战", AssetPath: "/Game/L/C.C",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked",
			SideCount: 4, EntryMode: configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE},
		{Id: 9, Name: "旧批次无两列", AssetPath: "/Game/L/D.D",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, GameMode: "5v5_ranked"},
	})
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)

	cases := []struct {
		mapID     uint32
		wantWalk  bool
		wantSides int
		name      string
	}{
		{6, false, 2, "撮合图"},
		{7, true, 1, "直进副本"},
		{8, false, 4, "四方混战"},
		{9, false, 2, "旧批次两列留空 → 沿用部署 walk_in=false + 默认 2 方"},
		{999, false, 2, "表内不存在 → 全回退"},
	}
	for _, c := range cases {
		if got := f.uc.isWalkInMap(c.mapID); got != c.wantWalk {
			t.Fatalf("%s: isWalkInMap(%d)=%v, 期望 %v", c.name, c.mapID, got, c.wantWalk)
		}
		if got := f.uc.sideCountForMap(c.mapID); got != c.wantSides {
			t.Fatalf("%s: sideCountForMap(%d)=%d, 期望 %d", c.name, c.mapID, got, c.wantSides)
		}
	}

	// 同一张表里直进与撮合共存 —— 这正是部署级开关表达不了、必须下沉到表的场景。
	if !f.uc.isWalkInMap(7) || f.uc.isWalkInMap(6) {
		t.Fatal("同池内直进图与撮合图必须能共存")
	}
}

// TestStartMatch_MapGateDisabled 未启用配置表(tables=nil)保持历史行为:任意 map_id 放行。
func TestStartMatch_MapGateDisabled(t *testing.T) {
	f := newFixture(t, 8300)
	if _, err := f.uc.StartMatch(context.Background(), 8301, 8301, 3001, 424242); err != nil {
		t.Fatalf("未启用配置表时不应校验 map_id: %v", err)
	}
}

// TestTeamSizeForMap 按 map_id 读关卡表一方人数:表填正值按表,未填 / 未知 map / 未启用回退全局 cfg.TeamSize。
func TestTeamSizeForMap(t *testing.T) {
	f := newFixtureWith(t, 8400, func(c *conf.MatchConf) {
		c.TeamSize = 5
		c.MapId = 6 // map_id==0 的默认副本兜底
	})

	// 未启用配置表(tables=nil)→ 回退全局 5。
	if got := f.uc.teamSizeForMap(7); got != 5 {
		t.Fatalf("tables=nil 应回退全局 5,得 %d", got)
	}

	dir := t.TempDir()
	rows := []*configpb.LevelRow{
		{Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, TeamSize: 5},
		{Id: 7, Name: "松林镇副本", AssetPath: "/Game/L/SonglinTown.SonglinTown",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, TeamSize: 1, AllowExit: true},
		{Id: 8, Name: "未填人数副本", AssetPath: "/Game/L/X.X",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE}, // TeamSize 留 0
	}
	writeLevelBatch(t, dir, 100, rows)
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)

	cases := []struct {
		name  string
		mapID uint32
		want  int
	}{
		{"表填 1v1", 7, 1},
		{"表填 5v5", 6, 5},
		{"map_id=0 兜底默认副本 6", 0, 5},
		{"表内未填人数回退全局", 8, 5},
		{"表内不存在的 map 回退全局", 999, 5},
	}
	for _, c := range cases {
		if got := f.uc.teamSizeForMap(c.mapID); got != c.want {
			t.Fatalf("%s: teamSizeForMap(%d)=%d, 期望 %d", c.name, c.mapID, got, c.want)
		}
	}
}

// TestTeamSizeForMap_ClampsGlobalFallback 复审 P1:全局 YAML cfg.TeamSize 未在别处校验,
// 负值/巨值经回退分支会流进撮合 need=2*teamSize(负容量 panic / OOM)。teamSizeForMap 必须
// 把最终一方人数钳到 [1, MaxLevelTeamSize],无论来源是全局 fallback 还是关卡表。
func TestTeamSizeForMap_ClampsGlobalFallback(t *testing.T) {
	// 负值(int 型 YAML 可为负)→ 钳到下界 1(tables=nil 走 fallback 分支)。
	fNeg := newFixtureWith(t, 8410, func(c *conf.MatchConf) { c.TeamSize = -3 })
	if got := fNeg.uc.teamSizeForMap(7); got != 1 {
		t.Fatalf("负 team_size 应钳到 1,得 %d", got)
	}
	// 巨值 → 钳到上界 MaxLevelTeamSize。
	fBig := newFixtureWith(t, 8411, func(c *conf.MatchConf) { c.TeamSize = 1 << 20 })
	if got := fBig.uc.teamSizeForMap(7); got != configtable.MaxLevelTeamSize {
		t.Fatalf("巨 team_size 应钳到 %d,得 %d", configtable.MaxLevelTeamSize, got)
	}
}

// TestPartitionTicketsByMap_NormalizesDefaultMap 复审 P1:map_id=0(省略=默认副本)与显式默认
// map(cfg.MapId)语义相同,必须归一化进同一撮合池,否则被拆两池永不互相成局。
func TestPartitionTicketsByMap_NormalizesDefaultMap(t *testing.T) {
	f := newFixtureWith(t, 8420, func(c *conf.MatchConf) { c.MapId = 6 })
	mk := func(id uint64, mapID uint32) *matchv1.MatchTicketStorageRecord {
		return &matchv1.MatchTicketStorageRecord{
			TicketId: id, CaptainId: id, MapId: mapID,
			Members: []*matchv1.MatchMemberStorageRecord{{PlayerId: id * 100}},
		}
	}
	tickets := []*matchv1.MatchTicketStorageRecord{mk(1, 0), mk(2, 6), mk(3, 7)}
	groups := f.uc.partitionTicketsByMap(tickets)
	// map_id=0 与显式默认 6 归一同池 → 只应有 2 个池(默认副本 + 副本 7)。
	if len(groups) != 2 {
		t.Fatalf("map_id=0 与显式默认 6 应归一同池 → 期望 2 个池,得 %d", len(groups))
	}
	merged := false
	for _, g := range groups {
		if len(g) == 2 { // 含 ticket 1(map 0)与 ticket 2(map 6)
			merged = true
		}
	}
	if !merged {
		t.Fatal("map_id=0 与显式默认 map 未归一到同一池")
	}
}
