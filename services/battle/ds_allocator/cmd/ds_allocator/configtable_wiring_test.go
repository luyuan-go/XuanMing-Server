package main

import (
	"os"
	"path/filepath"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// TestDevConfigResolvesMapsFromLevelTable 把「dev yaml ↔ 关卡表」这条接线钉死:
// 本机 mode=local 的关卡必须能从策划表现查出来,而不是从 yaml 里再抄一份。
//
// 覆盖三件事(2026-08-04 消除 local_ds.maps 影子配置):
//  1. dev yaml 里 config_table.dir 指得到真实 dist(相对路径按进程 CWD=服务目录解析);
//  2. 该批次能通过与 main 相同的批次级校验器(战斗关卡整表可开局);
//  3. 关卡表里 category=BATTLE 的每一张图都能拼出启动 URL —— 这取代了过去"yaml 的 maps
//     必须与 category=4 的 id 集合逐行对齐"那份人工核对(漏一行就是一张永远进不去的图)。
func TestDevConfigResolvesMapsFromLevelTable(t *testing.T) {
	const devYAML = "../../etc/ds_allocator-dev.yaml"
	if _, err := os.Stat(devYAML); err != nil {
		t.Skipf("dev 配置不存在,跳过: %v", err)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(devYAML)))
	defer func() { _ = c.Close() }()
	if err := c.Load(); err != nil {
		t.Fatalf("加载 dev 配置失败: %v", err)
	}
	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		t.Fatalf("解析 dev 配置失败: %v", err)
	}
	cfg.Defaults()

	if err := cfg.ValidateLocalMapSourceConfig(); err != nil {
		t.Fatalf("dev 配置没有可用的关卡来源: %v", err)
	}
	if cfg.ConfigTable.Dir == "" {
		t.Skip("dev 配置改走 loader_map(DS 侧查表),本用例只覆盖 allocator 查表这条路")
	}

	// yaml 里的相对路径以进程 CWD(= 服务目录 services/battle/ds_allocator)为基准,
	// 而测试 CWD 是本包目录,故回退两级还原。
	dist := filepath.Join("..", "..", cfg.ConfigTable.Dir)
	if _, err := os.Stat(filepath.Join(dist, configtable.ManifestFileName)); err != nil {
		t.Fatalf("config_table.dir=%q 指向的目录没有 manifest(dev 配置与仓库产物不一致): %v",
			cfg.ConfigTable.Dir, err)
	}

	store := configtable.NewStore()
	// 与 main 装配同一个批次级校验器:坏批次整批不切换。
	store.AddValidator(func(tb *configtable.Tables) error { return tb.Level.ValidateBattleLaunchURLs() })
	if _, err := store.Load(dist, 0); err != nil {
		t.Fatalf("按 dev 配置加载配置表失败: %v", err)
	}

	lv := store.Tables().Level
	battles := 0
	for _, row := range lv.All() {
		if !lv.IsBattleLevel(row.GetId()) {
			continue
		}
		battles++
		url, err := lv.BattleLaunchURL(row.GetId())
		if err != nil {
			t.Fatalf("map_id=%d(%s)拼不出启动 URL: %v", row.GetId(), row.GetName(), err)
		}
		t.Logf("map_id=%d %s → %s", row.GetId(), row.GetName(), url)
	}
	if battles == 0 {
		t.Fatal("关卡表里一张战斗关卡都没有,dev 配置必然开不了局")
	}
}
