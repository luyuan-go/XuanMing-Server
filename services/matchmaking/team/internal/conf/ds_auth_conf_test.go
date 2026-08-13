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

// TestDevConfigCarriesNoDSAuthSection 守住一条**跨语言**契约,踩过一次的那种。
//
// 生成器对每个服务做双向断言:etc 模板里出现 ds_auth 节点 ⟺ 该服务在权威清单里。
// team 不在清单里(清单只有 login / ds-allocator / hub-allocator / battle-result /
// player-locator),所以模板里**不得**出现 ds_auth 节点 —— 加了就是
// `[FATAL] team 的 ds_auth 节点与权威服务清单不一致`,而且只在真实配置生成时才炸,
// 本地 go test 全绿、代码 review 也看不出来。
//
// 本用例把那个 FATAL 提前到 go test:加节点的人当场就知道还要动另外两处 PowerShell 清单,
// 而不是等到发布窗口。inventory 是同款先例 —— 它也在代码里接了 DSCallbackGuard,
// 同样不带 ds_auth 节点、同样不在清单里。
//
// ⚠️ 真要给 team 启用 ds_auth,三处必须同批改:本模板加节点 + 上述两处清单加 'team'。
// 那等于把 DS 回调密钥分发到多一个服务,是安全面决策,不能作为某次加固的副作用顺手带上。
func TestDevConfigCarriesNoDSAuthSection(t *testing.T) {
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
			t.Fatalf("team-dev.yaml 出现了 ds_auth 节点,但 team 不在权威清单里 —— "+
				"真实配置生成会 [FATAL] 拒绝。要启用必须同批改 %s", dsAuthAuthorityLists)
		}
	}
}

// TestDSAuthDefaultsOffAndWiringIsAlive 分开钉两件事,别混成一件。
//
//	① 默认必须是关:模板不配 ds_auth ⇒ 守卫为 nil ⇒ GetPlayerTeam 的行为与接线前**完全一致**。
//	   接线本身不得让任何还没配 ds_auth 的环境静默收紧准入。
//	② 但接线不能是死的:一旦真给了密钥并置成灰度档,必须能构造出一把真守卫。
//	   密钥在这里由用例自己给(而不是从模板读),正是因为模板按①不该带它。
func TestDSAuthDefaultsOffAndWiringIsAlive(t *testing.T) {
	cfg := loadConfig(t, "etc/team-dev.yaml")

	guard, err := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if err != nil {
		t.Fatalf("默认配置构造守卫失败: %v", err)
	}
	if guard != nil {
		t.Fatal("未配 ds_auth 时必须得到 nil 守卫(等价 mode=off),否则接线改变了既有行为")
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
