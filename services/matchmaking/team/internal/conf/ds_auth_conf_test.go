package conf_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/middleware"
)

// dsAuthAuthorityLists 是「谁可以持有 DS 回调 HMAC 密钥」的**两处**权威清单。
// 两处必须同值,且与 etc 模板里是否出现 ds_auth 节点严格双向一致。
const dsAuthAuthorityLists = "tools/scripts/gen_cluster_config.ps1 的 $DsSecretServiceNames " +
	"与 tools/scripts/lib/online_manifest_contract.ps1 的 $PandoraDsCallbackHmacServices"

// TestDevConfigCarriesDSAuthSection 守住一条**跨语言**契约,踩过一次的那种。
//
// 生成器对每个服务做双向断言:etc 模板里出现 ds_auth 节点 ⟺ 该服务在权威清单里。
// team 于 2026-08-17 入列(GetPlayerTeam 暴露在无 jwt_authn 的 DS 面,dsGuard 此前是
// 纸面门),两处 PowerShell 清单已同批加 'team' —— 于是双向断言反向成立:模板**必须**
// 带 ds_auth 节点,缺了就是 `[FATAL] team 的 ds_auth 节点与权威服务清单不一致`,
// 而且只在真实配置生成时才炸,本地 go test 全绿、代码 review 也看不出来。
//
// 本用例把那个 FATAL 提前到 go test:删节点/删清单的人当场知道两处必须同批动。
func TestDevConfigCarriesDSAuthSection(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("..", "..", "etc/team-dev.yaml"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read team-dev.yaml: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "ds_auth:") {
			return
		}
	}
	t.Fatalf("team-dev.yaml 缺 ds_auth 节点,但 team 已在权威清单里(2026-08-17 入列)—— "+
		"真实配置生成会 [FATAL] 拒绝。节点与 %s 必须同批增删", dsAuthAuthorityLists)
}

// TestDSAuthDefaultsOffAndWiringIsAlive 分开钉两件事,别混成一件。
//
//	① dev 模板配的是 permissive(2026-08-17 入列):守卫必须**存在**且是灰度档 ——
//	   permissive 跑与 enforce 完全相同的验签路径,失败只 warn 不拒,不改变现有行为(§14.2)。
//	② 接线不能是死的:给了密钥并置灰度档,必须能构造出一把真守卫。
func TestDSAuthDefaultsOffAndWiringIsAlive(t *testing.T) {
	cfg := loadConfig(t, "etc/team-dev.yaml")

	guard, err := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if err != nil {
		t.Fatalf("默认配置构造守卫失败: %v", err)
	}
	if guard == nil {
		t.Fatal("dev 模板已配 ds_auth(permissive),必须得到一把真守卫;nil 等于静默不校验")
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
