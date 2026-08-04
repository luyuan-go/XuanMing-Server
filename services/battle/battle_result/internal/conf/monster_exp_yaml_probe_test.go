package conf

import (
	"os"
	"path/filepath"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
)

// TestMonsterExpLoadsFromRealYAML 用真实 etc yaml 断言怪物经验表能被 kratos 配置链读出来。
//
// 存在理由:MonsterExp 是本服唯一的「整型键 map」配置项,而它一旦解成空表,失败模式是
// **静默的** —— 击杀照常上报、照常推水位,只是每批多打一条 progress_facts_skipped,
// 与"策划真没配这只怪"在日志上完全同形,联调时极易被当成 DS 没上报去查错方向。
// 因此这里用真实配置文件跑一遍完整 kratos 解析链,把"表填了但没读进来"钉死在单测里。
func TestMonsterExpLoadsFromRealYAML(t *testing.T) {
	for _, rel := range []string{
		"../../../../../run/cluster/etc/battle-result.yaml",
		"../../etc/battle_result-dev.yaml",
	} {
		path := filepath.Clean(rel)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("配置文件不存在 %s: %v", path, err)
		}
		c := kconfig.New(kconfig.WithSource(file.NewSource(path)))
		if err := c.Load(); err != nil {
			t.Fatalf("加载 %s 失败: %v", path, err)
		}
		var cfg Config
		if err := c.Scan(&cfg); err != nil {
			_ = c.Close()
			t.Fatalf("解析 %s 失败: %v", path, err)
		}
		_ = c.Close()

		if len(cfg.Battle.MonsterExp) == 0 {
			t.Fatalf("%s: monster_exp 解析为空表(键须加引号,否则 kratos yaml→struct 会丢)", path)
		}
		// 松林近战腐蚀体是首版数值里最基础的一档,拿它当探针即可证明整型键确实落到了 map 上。
		if exp, ok := cfg.Battle.MonsterExpOf(2001); !ok || exp == 0 {
			t.Fatalf("%s: monster_exp 缺少怪物 2001(实得 exp=%d ok=%v)", path, exp, ok)
		}
	}
}
