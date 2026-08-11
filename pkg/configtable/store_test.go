package configtable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// marshalLevel 与生成器一致的序列化口径:proto 原名 + 枚举数字。
func marshalLevel(t *testing.T, data *configpb.LevelTableData) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(data)
	if err != nil {
		t.Fatalf("marshal level: %v", err)
	}
	return raw
}

func marshalPlayerLevelExp(t *testing.T, data *configpb.PlayerLevelExpTableData) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(data)
	if err != nil {
		t.Fatalf("marshal player level exp: %v", err)
	}
	return raw
}

func samplePlayerLevelExpData() *configpb.PlayerLevelExpTableData {
	return &configpb.PlayerLevelExpTableData{Rows: []*configpb.PlayerLevelExpRow{
		{Id: 1, Level: 1, UpgradeExp: 100, CumulativeExp: 0},
		{Id: 2, Level: 2, UpgradeExp: 0, CumulativeExp: 100},
	}}
}

// sampleItemData / sampleTalentData 是 store 用例的最小合法道具 / 专精表。
// Store.Load 要求 manifest 覆盖本进程注册的**全部**表(缺一整批拒绝),
// 因此每批产物都要带上这两张表,否则用例连加载都过不去。
func sampleItemData() *configpb.ItemTableData {
	return &configpb.ItemTableData{Rows: []*configpb.ItemRow{
		{Id: 10001, Name: "测试消耗品", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE,
			MaxStackSize: 99, Usable: true, UseHealHp: 50},
		{Id: 10003, Name: "测试装备", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT,
			MaxStackSize: 1, EquipSlot: 1},
	}}
}

func sampleTalentData() *configpb.TalentTableData {
	return &configpb.TalentTableData{Rows: []*configpb.TalentRow{
		{Id: 1, Name: "强击", MaxLevel: 5, CostPerLevel: 1},
		{Id: 4, Name: "聚能", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 1, RequireTalentLevel: 3},
	}}
}

func marshalItem(t *testing.T, data *configpb.ItemTableData) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(data)
	if err != nil {
		t.Fatalf("marshal item: %v", err)
	}
	return raw
}

func marshalTalent(t *testing.T, data *configpb.TalentTableData) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(data)
	if err != nil {
		t.Fatalf("marshal talent: %v", err)
	}
	return raw
}

func sampleLevelData() *configpb.LevelTableData {
	return &configpb.LevelTableData{Rows: []*configpb.LevelRow{
		{Id: 1, Name: "登录", AssetPath: "/Game/Level/Login/Lvl_Login.Lvl_Login",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_LOGIN, DisableUiShortcut: true},
		{Id: 6, Name: "MOBA战斗", AssetPath: "/Game/Test/Level/MobaLevel.MobaLevel",
			GameModeClass: "/Script/Pandora.PandoraBattleGameMode",
			Category:      configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, ShowInMatchList: true},
		{Id: 7, Name: "松林镇副本", AssetPath: "/Game/Test/Level/SonglinTown.SonglinTown",
			GameModeClass: "/Script/Pandora.PandoraPveGameMode",
			Category:      configpb.LevelCategory_LEVEL_CATEGORY_BATTLE, ShowInMatchList: true},
	}}
}

func checksumOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// writeBatch 在 dir 写出一批完整产物,mutate 可在写盘前篡改清单。
func writeBatch(t *testing.T, dir string, version uint64, levelRaw []byte, rows uint32, mutate func(*Manifest)) {
	t.Helper()
	writeBatchWithPlayerLevel(t, dir, version, levelRaw, rows,
		marshalPlayerLevelExp(t, samplePlayerLevelExpData()), 2, mutate)
}

func writeBatchWithPlayerLevel(t *testing.T, dir string, version uint64, levelRaw []byte, levelRows uint32,
	playerLevelRaw []byte, playerLevelRows uint32, mutate func(*Manifest),
) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "level.json"), levelRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "player_level_exp.json"), playerLevelRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	itemRaw := marshalItem(t, sampleItemData())
	if err := os.WriteFile(filepath.Join(dir, "item.json"), itemRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	talentRaw := marshalTalent(t, sampleTalentData())
	if err := os.WriteFile(filepath.Join(dir, "talent.json"), talentRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	m := &Manifest{
		Version:   version,
		Generator: "configtable-gen@test",
		SourceRev: "test",
		// level 必须留在下标 0:多个用例用 m.Tables[0] 篡改校验和 / proto 名 / 路径来验拒绝路径。
		Tables: []ManifestTable{
			{
				Name: "level", File: "level.json",
				Proto: "pandora.config.v1.LevelTableData", Checksum: checksumOf(levelRaw), Rows: levelRows,
			},
			{
				Name: "player_level_exp", File: "player_level_exp.json",
				Proto:    "pandora.config.v1.PlayerLevelExpTableData",
				Checksum: checksumOf(playerLevelRaw), Rows: playerLevelRows,
			},
			{
				Name: "item", File: "item.json",
				Proto: "pandora.config.v1.ItemTableData", Checksum: checksumOf(itemRaw),
				Rows: uint32(len(sampleItemData().GetRows())),
			},
			{
				Name: "talent", File: "talent.json",
				Proto: "pandora.config.v1.TalentTableData", Checksum: checksumOf(talentRaw),
				Rows: uint32(len(sampleTalentData().GetRows())),
			},
		},
	}
	// 上面四张是用例真正断言的表(level 必须留在下标 0)。除它们之外,本进程注册的**每一张**
	// 表都必须出现在清单里,否则 Load 走 "manifest 缺少本进程必需的表" 整批拒绝。
	// 这里按 specByName 自动补空表,而不是把表名手写进夹具——手写的话每加一张配置表
	// 就会连带压垮十几个与该表无关的用例(2026-08-04 一次性登记 19 张客户端表时实测)。
	fillRemainingTables(t, dir, m)
	if mutate != nil {
		mutate(m)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ManifestFileName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// fillRemainingTables 为清单里尚未出现的已注册表写出空表产物并补登记条目。
// 与 configtable/configtabletest.FillMissingTables 同一件事,但本文件是包内测试
// (package configtable),引用 configtabletest 会构成 import 环,故保留这份本地实现。
// 空表(rows 为空)对加载引擎是合法批次:跨表引用校验只校验实际存在的引用,
// 夹具里有行的四张表都不引用这些空表,故不会触发 fk 失败。
func fillRemainingTables(t *testing.T, dir string, m *Manifest) {
	t.Helper()
	present := make(map[string]bool, len(m.Tables))
	for _, mt := range m.Tables {
		present[mt.Name] = true
	}
	// specByName 是 map,遍历顺序随机;排序后再写,保证夹具产物可复现。
	names := make([]string, 0, len(specByName))
	for name := range specByName {
		if !present[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		spec := specByName[name]
		mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(spec.protoName))
		if err != nil {
			t.Fatalf("夹具补表 %q: 找不到 proto 类型 %q: %v", name, spec.protoName, err)
		}
		raw, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true}.Marshal(mt.New().Interface())
		if err != nil {
			t.Fatalf("夹具补表 %q: 序列化空表失败: %v", name, err)
		}
		file := name + ".json"
		if err := os.WriteFile(filepath.Join(dir, file), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		m.Tables = append(m.Tables, ManifestTable{
			Name: name, File: file, Proto: spec.protoName, Checksum: checksumOf(raw), Rows: 0,
		})
	}
}

func TestLoadPlayerLevelExpHotReloadAndBadBatchRetention(t *testing.T) {
	levelRaw := marshalLevel(t, sampleLevelData())
	playerRaw := func(rows ...*configpb.PlayerLevelExpRow) []byte {
		return marshalPlayerLevelExp(t, &configpb.PlayerLevelExpTableData{Rows: rows})
	}
	v1, v2, bad := t.TempDir(), t.TempDir(), t.TempDir()
	writeBatchWithPlayerLevel(t, v1, 100, levelRaw, 3, playerRaw(
		&configpb.PlayerLevelExpRow{Id: 1, Level: 1, UpgradeExp: 100},
		&configpb.PlayerLevelExpRow{Id: 2, Level: 2, CumulativeExp: 100},
	), 2, nil)
	writeBatchWithPlayerLevel(t, v2, 200, levelRaw, 3, playerRaw(
		&configpb.PlayerLevelExpRow{Id: 1, Level: 1, UpgradeExp: 250},
		&configpb.PlayerLevelExpRow{Id: 2, Level: 2, CumulativeExp: 250},
	), 2, nil)
	writeBatchWithPlayerLevel(t, bad, 300, levelRaw, 3, playerRaw(
		&configpb.PlayerLevelExpRow{Id: 1, Level: 1, UpgradeExp: 999},
		&configpb.PlayerLevelExpRow{Id: 2, Level: 2, CumulativeExp: 1},
	), 2, nil)

	s := NewStore()
	s.AddValidator(func(tb *Tables) error { return tb.PlayerLevelExp.ValidateCurve() })
	if _, err := s.Load(v1, 0); err != nil {
		t.Fatalf("首载 v1: %v", err)
	}
	if got := s.Tables().PlayerLevelExp.ExperienceCurve(); len(got) != 1 || got[0] != 100 {
		t.Fatalf("v1 曲线=%v, want [100]", got)
	}
	if _, err := s.Load(v2, 0); err != nil {
		t.Fatalf("热更 v2: %v", err)
	}
	if got := s.Tables().PlayerLevelExp.ExperienceCurve(); len(got) != 1 || got[0] != 250 {
		t.Fatalf("v2 曲线=%v, want [250]", got)
	}
	if _, err := s.Load(bad, 0); err == nil || !strings.Contains(err.Error(), "语义校验失败") {
		t.Fatalf("累计经验错误批次应被拒绝: %v", err)
	}
	if tb := s.Tables(); tb.Version != 200 || tb.PlayerLevelExp.ExperienceCurve()[0] != 250 {
		t.Fatalf("坏批次后应保留 v2: version=%d curve=%v", tb.Version, tb.PlayerLevelExp.ExperienceCurve())
	}
}

func writeGoodBatch(t *testing.T, dir string, version uint64) {
	t.Helper()
	writeBatch(t, dir, version, marshalLevel(t, sampleLevelData()), 3, nil)
}

// 批次级语义校验器:启动与热 reload 同一门禁,失败整批不切换保留旧批次(审计 P1)。
func TestLoadValidatorGatesReload(t *testing.T) {
	dirOK := t.TempDir()
	writeGoodBatch(t, dirOK, 100)
	s := NewStore()
	s.AddValidator(func(tb *Tables) error {
		if !tb.Level.IsBattleLevel(6) {
			return fmt.Errorf("默认 map 6 不是战斗关卡")
		}
		return nil
	})
	if _, err := s.Load(dirOK, 0); err != nil {
		t.Fatalf("首载应通过校验器: %v", err)
	}

	// 新批次删掉 id=6 → 校验器失败 → 整批不切换,旧批次(v100)继续生效。
	bad := &configpb.LevelTableData{Rows: []*configpb.LevelRow{
		{Id: 1, Name: "登录", AssetPath: "/Game/Level/Login/Lvl_Login.Lvl_Login",
			Category: configpb.LevelCategory_LEVEL_CATEGORY_LOGIN, DisableUiShortcut: true},
	}}
	dirBad := t.TempDir()
	writeBatch(t, dirBad, 101, marshalLevel(t, bad), 1, nil)
	if _, err := s.Load(dirBad, 0); err == nil || !strings.Contains(err.Error(), "语义校验失败") {
		t.Fatalf("坏批次应被校验器拒绝, got %v", err)
	}
	if tb := s.Tables(); tb == nil || tb.Version != 100 || !tb.Level.IsBattleLevel(6) {
		t.Fatalf("校验失败必须保留旧批次: %+v", tb)
	}
}

func TestLoadHappyPath(t *testing.T) {
	dir := t.TempDir()
	writeGoodBatch(t, dir, 100)
	s := NewStore()
	res, err := s.Load(dir, 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !res.Reloaded || res.Version != 100 {
		t.Fatalf("res=%+v", res)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("意外告警: %v", res.Warnings)
	}
	tb := s.Tables()
	if tb == nil || tb.Version != 100 || tb.Level.Count() != 3 {
		t.Fatalf("Tables=%+v", tb)
	}
	if row, ok := tb.Level.ByID(6); !ok || row.GetName() != "MOBA战斗" {
		t.Fatalf("ByID(6)=%v %v", row, ok)
	}
	if !tb.Level.IsBattleLevel(6) || !tb.Level.IsBattleLevel(7) {
		t.Fatal("6/7 应为战斗关卡")
	}
	if tb.Level.IsBattleLevel(1) {
		t.Fatal("1(登录)不应为战斗关卡")
	}
	if tb.Level.IsBattleLevel(999) {
		t.Fatal("不存在的 map_id 不应通过")
	}
}

func TestLoadExpectVersion(t *testing.T) {
	dir := t.TempDir()
	writeGoodBatch(t, dir, 100)
	s := NewStore()
	if _, err := s.Load(dir, 99); err == nil {
		t.Fatal("expectVersion 不符应拒绝")
	}
	if s.Tables() != nil {
		t.Fatal("失败后不应有生效批次")
	}
	if _, err := s.Load(dir, 100); err != nil {
		t.Fatalf("expectVersion 相符应成功: %v", err)
	}
}

func TestLoadIdempotentSameVersion(t *testing.T) {
	dir := t.TempDir()
	writeGoodBatch(t, dir, 100)
	s := NewStore()
	if _, err := s.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	first := s.Tables()
	res, err := s.Load(dir, 0)
	if err != nil || res.Reloaded {
		t.Fatalf("同版本应幂等 no-op: res=%+v err=%v", res, err)
	}
	if s.Tables() != first {
		t.Fatal("no-op 不应更换快照指针")
	}
}

func TestLoadRejectVersionRegress(t *testing.T) {
	newDir, oldDir := t.TempDir(), t.TempDir()
	writeGoodBatch(t, newDir, 200)
	writeGoodBatch(t, oldDir, 100)
	s := NewStore()
	if _, err := s.Load(newDir, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(oldDir, 0); err == nil || !strings.Contains(err.Error(), "回退") {
		t.Fatalf("低版本应被拒绝: %v", err)
	}
	if s.Tables().Version != 200 {
		t.Fatal("拒绝回退后应保留 200")
	}
}

// TestLoadFailKeepsOld 标准流水线核心不变量:新批次任一步失败,旧批次原样生效。
func TestLoadFailKeepsOld(t *testing.T) {
	good := t.TempDir()
	writeGoodBatch(t, good, 100)
	s := NewStore()
	if _, err := s.Load(good, 0); err != nil {
		t.Fatal(err)
	}

	cases := map[string]func(dir string){
		"checksum 不匹配": func(dir string) {
			writeBatch(t, dir, 200, marshalLevel(t, sampleLevelData()), 3, func(m *Manifest) {
				m.Tables[0].Checksum = "sha256:" + strings.Repeat("0", 64)
			})
		},
		"行数与清单不符": func(dir string) {
			writeBatch(t, dir, 200, marshalLevel(t, sampleLevelData()), 99, nil)
		},
		"JSON 损坏": func(dir string) {
			raw := []byte(`{"rows": [{`)
			writeBatch(t, dir, 200, raw, 3, nil)
		},
		"主键重复": func(dir string) {
			d := sampleLevelData()
			d.Rows[1].Id = 1
			writeBatch(t, dir, 200, marshalLevel(t, d), 3, nil)
		},
		"asset_path 为空": func(dir string) {
			d := sampleLevelData()
			d.Rows[0].AssetPath = ""
			writeBatch(t, dir, 200, marshalLevel(t, d), 3, nil)
		},
		"category 未填": func(dir string) {
			d := sampleLevelData()
			d.Rows[0].Category = configpb.LevelCategory_LEVEL_CATEGORY_UNSPECIFIED
			writeBatch(t, dir, 200, marshalLevel(t, d), 3, nil)
		},
		"proto 全名不符": func(dir string) {
			writeBatch(t, dir, 200, marshalLevel(t, sampleLevelData()), 3, func(m *Manifest) {
				m.Tables[0].Proto = "pandora.config.v1.WrongTable"
			})
		},
		"file 路径逃逸": func(dir string) {
			writeBatch(t, dir, 200, marshalLevel(t, sampleLevelData()), 3, func(m *Manifest) {
				m.Tables[0].File = "../level.json"
			})
		},
		"缺必需表": func(dir string) {
			writeBatch(t, dir, 200, marshalLevel(t, sampleLevelData()), 3, func(m *Manifest) {
				m.Tables[0].Name = "renamed"
				m.Tables[0].File = "renamed.json"
			})
			// 同步改文件名,让失败原因落在「缺 level 表」而非文件缺失
			if err := os.Rename(filepath.Join(dir, "level.json"), filepath.Join(dir, "renamed.json")); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			bad := t.TempDir()
			prepare(bad)
			if _, err := s.Load(bad, 0); err == nil {
				t.Fatal("应加载失败")
			}
			tb := s.Tables()
			if tb == nil || tb.Version != 100 || tb.Level.Count() != 3 {
				t.Fatalf("失败后旧批次应原样保留, got %+v", tb)
			}
		})
	}
}

// TestLoadUnknownTableSkipped 前向兼容:清单含未注册新表 → 跳过 + 告警,不失败。
func TestLoadUnknownTableSkipped(t *testing.T) {
	dir := t.TempDir()
	extra := []byte(`{"rows":[]}`)
	writeBatch(t, dir, 100, marshalLevel(t, sampleLevelData()), 3, func(m *Manifest) {
		m.Tables = append(m.Tables, ManifestTable{
			Name: "future_table", File: "future_table.json",
			Proto: "pandora.config.v1.FutureTableData", Checksum: checksumOf(extra), Rows: 0,
		})
	})
	if err := os.WriteFile(filepath.Join(dir, "future_table.json"), extra, 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	res, err := s.Load(dir, 0)
	if err != nil {
		t.Fatalf("未知表不应导致失败: %v", err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "future_table") {
		t.Fatalf("应有未知表告警: %v", res.Warnings)
	}
}

// TestLoadStrayFileWarned 目录里 manifest 未列出的 json = 脏数据告警,不拒载。
func TestLoadStrayFileWarned(t *testing.T) {
	dir := t.TempDir()
	writeGoodBatch(t, dir, 100)
	if err := os.WriteFile(filepath.Join(dir, "stray.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	res, err := s.Load(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "stray.json") {
		t.Fatalf("应有脏文件告警: %v", res.Warnings)
	}
}

// TestLoadTolerateUnknownField 运行时 DiscardUnknown:新增字段的 JSON 旧进程可读(滚动窗口)。
func TestLoadTolerateUnknownField(t *testing.T) {
	dir := t.TempDir()
	raw := marshalLevel(t, sampleLevelData())
	patched := strings.Replace(string(raw), `"rows":[{`, `"rows":[{"future_field":123,`, 1)
	if patched == string(raw) {
		t.Fatal("补丁未生效")
	}
	writeBatch(t, dir, 100, []byte(patched), 3, nil)
	s := NewStore()
	if _, err := s.Load(dir, 0); err != nil {
		t.Fatalf("未知字段应被容忍: %v", err)
	}
}

// TestConcurrentReadDuringReload -race 下并发读 + 热切换:读方永远看到完整一致的批次。
func TestConcurrentReadDuringReload(t *testing.T) {
	v1, v2 := t.TempDir(), t.TempDir()
	writeGoodBatch(t, v1, 100)
	writeGoodBatch(t, v2, 200)
	s := NewStore()
	if _, err := s.Load(v1, 0); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tb := s.Tables()
				if tb.Level.Count() != 3 {
					t.Error("读到不完整批次")
					return
				}
				if tb.PlayerLevelExp.Count() != 2 {
					t.Error("读到不完整玩家等级经验表")
					return
				}
				if v := tb.Version; v != 100 && v != 200 {
					t.Errorf("非法版本 %d", v)
					return
				}
			}
		}()
	}
	if _, err := s.Load(v2, 0); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if s.Tables().Version != 200 {
		t.Fatal("切换未生效")
	}
}
