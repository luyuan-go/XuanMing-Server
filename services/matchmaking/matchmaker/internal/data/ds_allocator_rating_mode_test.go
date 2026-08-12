// 本局计分模式的定格契约(2026-08-11 关卡表「计分模式」列):
//
// 「算不算段位」以前是 battle_result 从 canonical game_mode 字符串用排除法推的
// (`!= "pve_coop"` 即算 Elo),新增一个撮合池就会静默按排位改玩家段位。现在改为
// matchmaker 在**发出 AllocateBattle 那一刻**按 map_id 读关卡表 rating_mode 定格进请求,
// 由 allocator 存进 canonical BattleStorageRecord,battle_result 结算只认那份定格值。
//
// 本文件钉死 matchmaker 这一侧的两件事:
//  1. 表里填了就必须原样定格进请求(按 effective map_id,含 map_id==0 的默认副本兜底);
//  2. 拿不到就必须是 UNSPECIFIED(0),**绝不猜 ELO** —— 猜错会给合作副本玩家扣段位,
//     而改段位不可逆(§9.21 共存窗口 + §9.6 方向安全)。
package data

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/configtable/configtabletest"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// writeRatingModeBatch 写一份只有关卡表有行的合法批次(其余注册表补空表,否则 Load 整批拒绝)。
func writeRatingModeBatch(t *testing.T, dir string, rows []*configpb.LevelRow) {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.
		Marshal(&configpb.LevelTableData{Rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	tables := []map[string]any{{
		"name": "level", "file": "level.json",
		"proto":    "pandora.config.v1.LevelTableData",
		"checksum": "sha256:" + hex.EncodeToString(sum[:]),
		"rows":     len(rows),
	}}
	tables = append(tables, configtabletest.FillMissingTables(t, dir, []string{"level"})...)
	mraw, err := json.Marshal(map[string]any{"version": uint64(100), "generator": "test", "tables": tables})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "level.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), mraw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func ratingModeStore(t *testing.T, rows []*configpb.LevelRow) *configtable.Store {
	t.Helper()
	dir := t.TempDir()
	writeRatingModeBatch(t, dir, rows)
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatalf("load level batch: %v", err)
	}
	return store
}

// ratingModeLevelRows:6=排位对抗图(ELO),7=合作副本(不计分),两者都是战斗类关卡。
func ratingModeLevelRows() []*configpb.LevelRow {
	return []*configpb.LevelRow{
		{
			Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE,
			TeamSize: 3, SideCount: 2, GameMode: "5v5_ranked",
			RatingMode: configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO,
			RatingPool: "5v5_ranked",
		},
		{
			Id: 7, Name: "松林镇副本", AssetPath: "/Game/L/SonglinTown.SonglinTown",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE,
			TeamSize: 3, SideCount: 1, GameMode: "pve_coop",
			RatingMode: configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE,
		},
	}
}

func TestAllocateBattleFreezesRatingModeFromLevelTable(t *testing.T) {
	store := ratingModeStore(t, ratingModeLevelRows())

	cases := []struct {
		name        string
		requestMap  uint32
		defaultMap  uint32
		wantRating  configpb.LevelRatingMode
		wantMapSent uint32
	}{
		{"排位图按表定格 ELO", 6, 6, configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO, 6},
		{"合作副本按表定格不计分", 7, 6, configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE, 7},
		// map_id==0 是"用本实例默认副本",定格必须按 effective map_id 解,
		// 否则默认副本的局会永远拿不到计分模式(与 team_size / side_count 同一兜底口径)。
		{"map_id=0 走默认副本的行", 0, 7, configpb.LevelRatingMode_LEVEL_RATING_MODE_NONE, 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := &allocatorCaptureClient{}
			allocator := &GrpcDSAllocator{cli: client, gameMode: "custom", mapID: c.defaultMap}
			allocator.SetConfigTables(store)
			if _, err := allocator.AllocateBattle(t.Context(), 9001, []uint64{1, 2}, c.requestMap); err != nil {
				t.Fatal(err)
			}
			if got := client.request.GetRatingMode(); got != c.wantRating {
				t.Fatalf("rating_mode = %v, want %v", got, c.wantRating)
			}
			if got := client.request.GetMapId(); got != c.wantMapSent {
				t.Fatalf("map_id = %d, want %d", got, c.wantMapSent)
			}
		})
	}
}

// TestAllocateBattleRatingModeFallsBackToUnspecified:拿不到表 / 表里没这张图 /
// 表里这一列没填,三种情况都必须是 UNSPECIFIED —— 让 battle_result 回落旧口径,
// 而不是让 matchmaker 猜一个计分规则出来。
func TestAllocateBattleRatingModeFallsBackToUnspecified(t *testing.T) {
	unconfigured := []*configpb.LevelRow{{
		Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
		Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE,
		TeamSize: 3, SideCount: 2, GameMode: "5v5_ranked",
		// 刻意不填 RatingMode:模拟旧批次表 / 策划漏填。
	}}
	cases := []struct {
		name  string
		store *configtable.Store
		mapID uint32
	}{
		{"未启用配置表", nil, 6},
		{"表里没有这张图", ratingModeStore(t, ratingModeLevelRows()), 999},
		{"表里这一列没填", ratingModeStore(t, unconfigured), 6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := &allocatorCaptureClient{}
			allocator := &GrpcDSAllocator{cli: client, gameMode: "custom", mapID: 6}
			if c.store != nil {
				allocator.SetConfigTables(c.store)
			}
			if _, err := allocator.AllocateBattle(t.Context(), 9002, []uint64{1, 2}, c.mapID); err != nil {
				t.Fatal(err)
			}
			if got := client.request.GetRatingMode(); got != configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED {
				t.Fatalf("rating_mode = %v, want UNSPECIFIED(回落旧口径,绝不猜 ELO)", got)
			}
		})
	}
}
