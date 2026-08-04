// configtable_admin_test.go — 配置表热更入口(ReloadConfigTable)语义测试:
// 幂等 no-op / 成功切换 / 失败保留旧表 / expect_version / 玩家身份拒绝。
package service

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
	"github.com/luyuancpp/pandora/pkg/configtable/configtabletest"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func writeLevelBatchDir(t *testing.T, dir string, version uint64) {
	t.Helper()
	levelRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(
		&configpb.LevelTableData{Rows: []*configpb.LevelRow{{
			Id: 6, Name: "MOBA战斗", AssetPath: "/Game/L/MobaLevel.MobaLevel",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_BATTLE,
		}}})
	if err != nil {
		t.Fatal(err)
	}
	playerLevelExpRows := []*configpb.PlayerLevelExpRow{
		{Id: 1, Level: 1, UpgradeExp: 100, CumulativeExp: 0},
		{Id: 2, Level: 2, UpgradeExp: 0, CumulativeExp: 100},
	}
	playerLevelExpRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(
		&configpb.PlayerLevelExpTableData{Rows: playerLevelExpRows})
	if err != nil {
		t.Fatal(err)
	}
	// Store.Load 要求 manifest 覆盖本进程注册的**全部**表(缺一整批拒绝),
	// 所以即使 matchmaker 只用关卡表,批次也必须带上道具 / 专精表。
	itemRows := []*configpb.ItemRow{
		{Id: 10001, Name: "测试消耗品", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE,
			MaxStackSize: 99, Usable: true, UseHealHp: 50},
	}
	itemRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(
		&configpb.ItemTableData{Rows: itemRows})
	if err != nil {
		t.Fatal(err)
	}
	talentRows := []*configpb.TalentRow{
		{Id: 1, Name: "强击", MaxLevel: 5, CostPerLevel: 1},
	}
	talentRaw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(
		&configpb.TalentTableData{Rows: talentRows})
	if err != nil {
		t.Fatal(err)
	}

	levelSum := sha256.Sum256(levelRaw)
	playerLevelExpSum := sha256.Sum256(playerLevelExpRaw)
	itemSum := sha256.Sum256(itemRaw)
	talentSum := sha256.Sum256(talentRaw)
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

	tables := []map[string]any{
		{
			"name": "level", "file": "level.json",
			"proto":    "pandora.config.v1.LevelTableData",
			"checksum": "sha256:" + hex.EncodeToString(levelSum[:]),
			"rows":     1,
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
	}
	// 其余已注册表按空表补齐(缺一张 Load 就整批拒绝);见 configtabletest 包注释。
	tables = append(tables, configtabletest.FillMissingTables(t, dir,
		[]string{"level", "player_level_exp", "item", "talent"})...)
	mraw, err := json.Marshal(map[string]any{"version": version, "tables": tables})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, configtable.ManifestFileName), mraw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReloadConfigTable(t *testing.T) {
	dir := t.TempDir()
	writeLevelBatchDir(t, dir, 100)
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	svc := NewConfigTableAdminService(store, dir)
	ctx := context.Background()

	// 同版本 → 幂等 no-op
	resp, err := svc.ReloadConfigTable(ctx, &configv1.ReloadConfigTableRequest{})
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || resp.GetReloaded() || resp.GetActiveVersion() != 100 {
		t.Fatalf("同版本应 no-op: %+v err=%v", resp, err)
	}

	// 新版本 → 切换
	writeLevelBatchDir(t, dir, 200)
	resp, err = svc.ReloadConfigTable(ctx, &configv1.ReloadConfigTableRequest{ExpectVersion: 200})
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || !resp.GetReloaded() || resp.GetActiveVersion() != 200 {
		t.Fatalf("新版本应切换: %+v err=%v", resp, err)
	}

	// expect_version 不符 → 拒绝且保留 200
	resp, err = svc.ReloadConfigTable(ctx, &configv1.ReloadConfigTableRequest{ExpectVersion: 999})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_INVALID_STATE || resp.GetActiveVersion() != 200 {
		t.Fatalf("expect 不符应拒绝并保留旧版本: %+v err=%v", resp, err)
	}

	// active 目录损坏 → 失败保留旧表(标准流水线核心不变量)
	writeLevelBatchDir(t, dir, 300)
	if err := os.WriteFile(filepath.Join(dir, "level.json"), []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	resp, err = svc.ReloadConfigTable(ctx, &configv1.ReloadConfigTableRequest{})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_INVALID_STATE || resp.GetActiveVersion() != 200 {
		t.Fatalf("损坏批次应失败并保留 200: %+v err=%v", resp, err)
	}
	if store.Tables().Version != 200 || store.Tables().Level.Count() != 1 {
		t.Fatalf("旧表应原样生效: %+v", store.Tables())
	}

	// 带玩家身份(经 Envoy 注入)调用 → 拒绝
	playerCtx := context.WithValue(ctx, plog.CtxKeyPlayerID, uint64(42))
	resp, err = svc.ReloadConfigTable(playerCtx, &configv1.ReloadConfigTableRequest{})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("玩家身份应被拒: %+v err=%v", resp, err)
	}
}
