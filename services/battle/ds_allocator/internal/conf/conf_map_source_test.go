package conf

import (
	"strings"
	"testing"
)

// TestValidateLocalMapSourceConfig 锁住「mode=local 的关卡必须有唯一权威源」。
//
// 2026-08-04 之前 local_ds.maps 是关卡表的手抄副本,抄漏一行 = 那张图永远进不去
// (起兜底图 → DS 关卡门判 Mismatch 自杀 → 分配卡到超时 → 玩家只看到"排队中")。
// 该表已删,现在两条同源路径二选一:allocator 现查关卡表(config_table.dir),
// 或 DS 侧 Loader GameMode 查同一张表(local_ds.loader_map)。都没有就拒绝启动。
func TestValidateLocalMapSourceConfig(t *testing.T) {
	newCfg := func(mode, dir, loaderMap string) *Config {
		c := &Config{}
		c.Mode = mode
		c.ConfigTable.Dir = dir
		c.LocalDS.LoaderMap = loaderMap
		return c
	}

	for _, tc := range []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"local + config_table", newCfg(ModeLocal, "../../../configtable/dist", ""), false},
		{"local + loader_map", newCfg(ModeLocal, "", "/Game/Entry/Level/Lvl_Server_Entry"), false},
		{"local 两者皆有", newCfg(ModeLocal, "../../../configtable/dist", "/Game/Entry/Level/Lvl_Server_Entry"), false},
		{"local 两者皆空 → 拒绝启动", newCfg(ModeLocal, "", ""), true},
		{"local 空白字符不算配置", newCfg(ModeLocal, "   ", "  "), true},
		// agones:关卡由 DS 侧 Loader 查表决定,allocator 只透传 map_id,不需要本地关卡源。
		{"agones 两者皆空放行", newCfg(ModeAgones, "", ""), false},
		{"mock 两者皆空放行", newCfg(ModeMock, "", ""), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.ValidateLocalMapSourceConfig()
			if tc.wantErr != (err != nil) {
				t.Fatalf("ValidateLocalMapSourceConfig() err=%v, wantErr=%v", err, tc.wantErr)
			}
			// 报错必须指出两条出路,否则运维只知道"起不来"不知道改哪。
			if err != nil && (!strings.Contains(err.Error(), "config_table.dir") ||
				!strings.Contains(err.Error(), "loader_map")) {
				t.Fatalf("错误信息未写明两条出路: %v", err)
			}
		})
	}
}
