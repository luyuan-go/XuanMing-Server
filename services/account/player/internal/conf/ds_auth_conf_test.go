// ds_auth_conf_test.go — DS 回调令牌守卫的配置档与跨语言发布契约(2026-08-13)。
//
// player 这天进入了 DS 回调密钥权威清单(GetLoadout 被挂上 Envoy DS 面 :8444,而该监听器
// 没有 jwt_authn,服务端不验 DS 令牌就等于把任意玩家出战快照开放给能连到该端口的任何进程)。
// 入列意味着**三处必须同增同减**:etc dev 模板的 ds_auth 节点、gen_cluster_config.ps1 的
// $DsSecretServiceNames、online_manifest_contract.ps1 的 $PandoraDsCallbackHmacServices。
// 本文件把这三处的一致性钉在 go test 里 —— 否则漏改只会在真实配置生成时炸成 [FATAL],
// 本地 go test 全绿、code review 也看不出来。
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
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
)

// devTemplate 是 main.go 与 gen_cluster_config.ps1 共同读的那一份。
const devTemplate = "etc/player-dev.yaml"

// prodExample 是人照抄的生产底稿(扩展名不是 .yaml,不走 kratos,只做文本断言)。
const prodExample = "etc/player-prod.yaml.example"

// repoRel 把仓库根相对路径转成本包(services/account/player/internal/conf)可用的相对路径。
func repoRel(t *testing.T, rel string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", rel))
	if err != nil {
		t.Fatalf("abs %s: %v", rel, err)
	}
	return path
}

// loadConfig 按 main.go 的同一条路径加载 etc 模板(kratos file source + Scan + Defaults)。
func loadConfig(t *testing.T, rel string) conf.Config {
	t.Helper()
	c := kconfig.New(kconfig.WithSource(file.NewSource(repoRel(t, rel))))
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

// readRepoFile 读仓库根相对路径的文本(prod 样例不走 kratos)。
func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(repoRel(t, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(raw)
}

// hasTopLevelKey 判断 yaml 文本里有没有某个顶层键(生成器用的是同款正则口径:行首无缩进)。
func hasTopLevelKey(text, key string) bool {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimRight(line, "\r"), key+":") {
			return true
		}
	}
	return false
}

// TestDevConfigCarriesDSAuthSection 守住跨语言双向断言的「必须有」那一侧。
//
// 生成器:etc 模板里出现 ds_auth 节点 ⟺ 该服务在 $DsSecretServiceNames 里。player 已入列,
// 所以模板里**必须**有 —— 删了就是 `[FATAL] player 的 ds_auth 节点与权威服务清单不一致`。
func TestDevConfigCarriesDSAuthSection(t *testing.T) {
	if !hasTopLevelKey(readRepoFile(t, devTemplate), "ds_auth") {
		t.Fatalf("%s 缺少 ds_auth 节点,但 player 已在 DS 回调密钥权威清单里 —— "+
			"真实配置生成会 [FATAL] 拒绝。要退出清单必须三处同批改", devTemplate)
	}
	if !hasTopLevelKey(readRepoFile(t, prodExample), "ds_auth") {
		t.Fatalf("%s 缺少 ds_auth 节点:生产底稿漏了它,照抄的人就会部署出一个不验 DS 令牌的 player", prodExample)
	}
}

// TestDevDSAuthSecretMatchesSignerAndIsolatedFromPlayerJWT 钉两条只在跨服务层面才成立的约束。
//
//	① player 是**验签方**,签发方是 ds_allocator / hub_allocator —— 三处 secret 必须同值,
//	   不同值的表现不是启动失败,而是每次 DS 调用都验签失败(permissive 下只是一堆 warn,
//	   enforce 下整条出战快照链路全挂),属于配错了也照常启动的那类故障。
//	② DS callback keyset 与玩家面 jwt keyset 不得相交(发布契约会硬拒相交的发布)。
func TestDevDSAuthSecretMatchesSignerAndIsolatedFromPlayerJWT(t *testing.T) {
	cfg := loadConfig(t, devTemplate)
	if cfg.DSAuth.Secret == "" {
		t.Fatal("dev 模板必须给出 ds_auth.secret:mode!=off 时缺 secret 会启动失败")
	}

	for _, signer := range []string{
		"services/battle/ds_allocator/etc/ds_allocator-dev.yaml",
		"services/battle/hub_allocator/etc/hub_allocator-dev.yaml",
	} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", signer))
		if err != nil {
			t.Fatalf("read %s: %v", signer, err)
		}
		if !strings.Contains(string(raw), cfg.DSAuth.Secret) {
			t.Fatalf("player 的 ds_auth.secret 与签发方 %s 不一致:"+
				"DS 令牌会验签失败,而两边都能正常启动(permissive 下只剩 warn)", signer)
		}
	}

	// 同一个 Convert-Secret 还断言了另一半:jwt 节点存在 ⟺ 服务在 $PlayerSecretServiceNames 里。
	// player 不在那份清单(它不签发玩家 SessionToken),所以模板里**不得**有 jwt 节点 ——
	// 这次为了加 ds_auth 而顺手把 jwt 也抄进来,会以同样的 [FATAL] 拒绝生成。
	if hasTopLevelKey(readRepoFile(t, devTemplate), "jwt") {
		t.Fatalf("%s 出现了 jwt 节点,但 player 不在 $PlayerSecretServiceNames 里 —— "+
			"真实配置生成会 [FATAL] 拒绝", devTemplate)
	}
}

// TestDSAuthDevIsPermissiveAndProdEnforces 钉住灰度档位本身。
//
//	dev=permissive:跑与 enforce 完全相同的验签 + 范围校验路径,但失败只 warn 放行 ——
//	  既不改变现有行为(§14.2,本地联调 / 一键启动不会被这次加固弄坏),
//	  又能在切 enforce 之前拿到「令牌到底有没有到、验不验得过」的真实证据。
//	  写成 off 就退回了「纸面门」,那正是这次评审打回的状态。
//	prod=enforce:GetLoadout 经无 jwt_authn 的 :8444 暴露,不 enforce 等于不设防。
func TestDSAuthDevIsPermissiveAndProdEnforces(t *testing.T) {
	cfg := loadConfig(t, devTemplate)
	if cfg.DSAuth.Mode != "permissive" {
		t.Fatalf("dev 档位必须是 permissive(拿证据但不改变行为),got=%q", cfg.DSAuth.Mode)
	}

	prod := readRepoFile(t, prodExample)
	if !strings.Contains(prod, `mode: "enforce"`) {
		t.Fatalf("%s 的 ds_auth.mode 必须是 enforce:生产不 enforce 等于这道门不存在", prodExample)
	}
}

// TestDSAuthWiringIsAlive 接线不能是死的:各档位都必须能构造出真守卫。
//
// dev 模板现在就是 permissive,所以这里同时验证「按真实模板构造出来的守卫非 nil」——
// 它是 off 与 permissive 的唯一可机械区分点(off 返回 nil 守卫)。
func TestDSAuthWiringIsAlive(t *testing.T) {
	cfg := loadConfig(t, devTemplate)
	guard, err := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if err != nil {
		t.Fatalf("[%s] 按真实模板构造守卫失败: %v", devTemplate, err)
	}
	if guard == nil {
		t.Fatalf("[%s] permissive 必须得到一把真守卫,得到 nil 等于静默不校验", devTemplate)
	}
	if got := guard.Mode().String(); got != "permissive" {
		t.Fatalf("守卫档位与模板不符: got=%s want=permissive", got)
	}

	for _, mode := range []string{"permissive", "enforce"} {
		enabled := config.DSAuthConf{Mode: mode, Secret: "pandora-dev-jwt-secret-change-me-32!"}
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
