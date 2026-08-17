// ds_auth_conf_test.go — DS 回调令牌守卫的默认档与跨契约约束。
package conf_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/services/social/guild/internal/conf"
)

// devTemplates 两份都要过:tidb 那份是独立文件,漏改同样只在切库时才暴露。
var devTemplates = []string{"etc/guild-dev.yaml", "etc/guild-dev-tidb.yaml"}

// dsAuthAuthorityLists 是「谁可以持有 DS 回调 HMAC 密钥」的**两处**权威清单。
const dsAuthAuthorityLists = "tools/scripts/gen_cluster_config.ps1 的 $DsSecretServiceNames " +
	"与 tools/scripts/lib/online_manifest_contract.ps1 的 $PandoraDsCallbackHmacServices"

// loadConfig 按 main.go 的同一条路径加载 etc 模板(kratos file source + Scan + Defaults)。
func loadConfig(t *testing.T, rel string) conf.Config {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatalf("abs %s: %v", rel, err)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(path)))
	defer c.Close()
	if err := c.Load(); err != nil {
		t.Fatalf("load %s: %v", rel, err)
	}
	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		t.Fatalf("scan %s: %v", rel, err)
	}
	cfg.Defaults()
	return cfg
}

// TestDevConfigCarriesDSAuthSection 守住一条**跨语言**契约,踩过一次的那种。
//
// 生成器对每个服务做双向断言:etc 模板里出现 ds_auth 节点 ⟺ 该服务在权威清单里。
// guild 于 2026-08-17 入列(公会归属反查暴露在无 jwt_authn 的 DS 面,dsGuard 此前是
// 纸面门),两处 PowerShell 清单已同批加 'guild' —— 双向断言反向成立:两份模板都
// **必须**带 ds_auth 节点,缺了就是 `[FATAL] guild 的 ds_auth 节点与权威服务清单不一致`,
// 而且只在真实配置生成时才炸,本地 go test 全绿、代码 review 也看不出来。
//
// 本用例把那个 FATAL 提前到 go test:删节点/删清单的人当场知道要同批动。
func TestDevConfigCarriesDSAuthSection(t *testing.T) {
	for _, rel := range devTemplates {
		t.Run(rel, func(t *testing.T) {
			path, err := filepath.Abs(filepath.Join("..", "..", rel))
			if err != nil {
				t.Fatalf("abs: %v", err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", rel, err)
			}
			for _, line := range strings.Split(string(raw), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "ds_auth:") {
					return
				}
			}
			t.Fatalf("%s 缺 ds_auth 节点,但 guild 已在权威清单里(2026-08-17 入列)—— "+
				"真实配置生成会 [FATAL] 拒绝。节点与 %s 必须同批增删", rel, dsAuthAuthorityLists)
		})
	}
}

// TestDSAuthDefaultsOffAndWiringIsAlive 分开钉两件事:
//
//	① dev 模板配的是 permissive(2026-08-17 入列):守卫必须存在 —— permissive 跑与
//	   enforce 相同的验签路径,失败只 warn 不拒,不改变现有行为(§14.2)。
//	② 接线不能是死的:给足密钥并置成灰度档时,必须能构造出一把真守卫。
func TestDSAuthDefaultsOffAndWiringIsAlive(t *testing.T) {
	for _, rel := range devTemplates {
		cfg := loadConfig(t, rel)
		guard, err := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
		if err != nil {
			t.Fatalf("[%s] 默认配置构造守卫失败: %v", rel, err)
		}
		if guard == nil {
			t.Fatalf("[%s] dev 模板已配 ds_auth(permissive),必须得到一把真守卫;nil 等于静默不校验", rel)
		}
	}

	for _, mode := range []string{"permissive", "enforce"} {
		enabled := config.DSAuthConf{
			Mode:   mode,
			Secret: "pandora-dev-jwt-secret-change-me-32!",
		}
		enabled.Defaults()
		g, err := middleware.NewDSCallbackGuardFromConf(enabled)
		if err != nil {
			t.Fatalf("给足密钥并置 mode=%q 后仍无法构造守卫(接线是死的): %v", mode, err)
		}
		if g == nil {
			t.Fatalf("mode=%q 必须得到一把真守卫,得到 nil 等于静默不校验", mode)
		}
	}
}
