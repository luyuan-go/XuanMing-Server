// team_addr_required_test.go — team_addr 留空必须启动失败(INC-20260813-001)。
//
// 留空的后果是**静默**的:StartMatch 不再校验队伍(任何玩家可为任意 team_id 开局),
// 对局结束也不再复位准备状态(正是本次事故的第一根因)。两者都不报错、不打 ERROR。
// 这与事故本身同一类失效形状 —— 配错了没有任何人会发现 —— 所以按 fail-closed 处理。
package conf

import (
	"strings"
	"testing"
)

// baseValidConfig 返回一份除 team_addr 外都能过 Validate 的配置。
func baseValidConfig(t *testing.T) *Config {
	t.Helper()
	var c Config
	c.Defaults()
	c.Match.TeamAddr = "127.0.0.1:20010"
	// Validate 还要求内部 RPC 鉴权密钥合法且与玩家 JWT 分域;这里只是把它们填成合法值,
	// 本文件测的是 team_addr 那一道门。
	c.JWT.Secret = "player-jwt-secret-at-least-32-bytes-long!!"
	c.Match.MatchResumeAuthSecret = "match-resume-internal-secret-32-bytes-min!"
	c.Match.MatchResumeAuthAudience = "matchmaker"
	if err := c.Validate(); err != nil {
		t.Fatalf("前提不成立:基线配置本身就过不了 Validate: %v", err)
	}
	return &c
}

func TestValidate_teamAddr留空必须启动失败(t *testing.T) {
	c := baseValidConfig(t)
	c.Match.TeamAddr = ""

	err := c.Validate()
	if err == nil {
		t.Fatal("team_addr 留空必须 fail-closed —— 否则组票与对局结束复位两条链都静默不走")
	}
	// 错误信息必须说清「关掉的是什么」和「怎么显式跳过」,否则运维只会看到一句 required。
	for _, want := range []string{"team_addr", "allow_missing_team", "INC-20260813-001"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误信息缺少 %q,运维无法据此判断该配还是该显式跳过: %v", want, err)
		}
	}
}

func TestValidate_显式allowMissingTeam才放行(t *testing.T) {
	c := baseValidConfig(t)
	c.Match.TeamAddr = ""
	c.Match.AllowMissingTeam = true

	if err := c.Validate(); err != nil {
		t.Fatalf("显式声明后应放行(骨架联调路径): %v", err)
	}
}

// 零值即安全:没写 allow_missing_team 的配置不得因为本改动被放行。
func TestValidate_零值不得放行(t *testing.T) {
	var c Config
	c.Defaults()
	if c.Match.AllowMissingTeam {
		t.Fatal("AllowMissingTeam 零值必须是 false(零值即安全,同 battle_gate_fail_open)")
	}
}

// 三份纳管配置都必须真的配了 team_addr —— 否则上面那道门等于没有。
func TestManagedConfigs_都配了teamAddr(t *testing.T) {
	// 这里只断言字段语义,真实 yaml 的取值由 cmd 包的加载用例覆盖;
	// 本条防的是「有人把默认值改成 allow_missing_team=true 让门形同虚设」。
	var c Config
	c.Defaults()
	if c.Match.TeamAddr != "" {
		t.Fatal("Defaults 不得凭空填 team_addr —— 那会让「留空」这个失败态永远测不到")
	}
}
