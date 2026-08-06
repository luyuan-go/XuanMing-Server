// conf_test.go — 校验 etc/ 下各部署配置能被 main.go 同款加载路径解析,且关键字段符合部署语义。
// 防止改配置文件时手滑(缩进 / 字段名拼错)直到服务启动才发现。
package conf_test

import (
	"path/filepath"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// loadConfig 复刻 main.go 的加载方式:kratos file source → Scan → Defaults。
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

// PVP 撮合实例:默认部署,走排队撮合(非 walk-in)。
func TestConfig_DevPVP(t *testing.T) {
	cfg := loadConfig(t, "etc/matchmaker-dev.yaml")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg.Match.WalkIn {
		t.Fatalf("PVP 实例必须走撮合(walk_in=false),否则每张票都单独开局")
	}
	if cfg.Match.GameMode == "" {
		t.Fatalf("game_mode 不能为空(撮合池命名空间)")
	}
}

// PVE 直进实例:与 PVP 同二进制不同部署;单人/整队直接开局,不撮合。
// 两实例必须错开 gRPC 端口与 snowflake node_id(match_id 全局唯一)。
func TestConfig_PVE(t *testing.T) {
	pve := loadConfig(t, "etc/matchmaker-pve.yaml")
	pvp := loadConfig(t, "etc/matchmaker-dev.yaml")
	if err := pve.Validate(); err != nil {
		t.Fatalf("PVE Validate: %v", err)
	}

	if !pve.Match.WalkIn {
		t.Fatalf("PVE 实例必须 walk_in=true(组好队/单人直进副本)")
	}
	if pve.Match.EnableSoloMatch {
		t.Fatalf("PVE yaml 应已迁移到 walk_in 新键,不该再写废弃的 enable_solo_match")
	}
	if pve.Match.GameMode == pvp.Match.GameMode {
		t.Fatalf("PVE 与 PVP game_mode 相同(%q),撮合池会串", pve.Match.GameMode)
	}
	if pve.Match.MatchResumeAuthAudience == pvp.Match.MatchResumeAuthAudience {
		t.Fatal("PVE 与 PVP Match resume audience 必须隔离，防签名跨部署重放")
	}
	if pve.Server.Grpc.Addr == pvp.Server.Grpc.Addr {
		t.Fatalf("PVE 与 PVP gRPC 端口相同(%q),同机部署会撞端口", pve.Server.Grpc.Addr)
	}
	if pve.Node.NodeId == pvp.Node.NodeId {
		t.Fatalf("PVE 与 PVP node_id 相同(%d),snowflake match_id 会撞", pve.Node.NodeId)
	}
	if pve.Match.TeamSize <= 0 {
		t.Fatalf("team_size 必须 > 0")
	}
}

// 旧键兼容回归(walk_in 正名,2026-07-25):滚动升级期新二进制可能先于 ConfigMap / yaml 上线,
// 旧部署里只有废弃的 enable_solo_match。若它被静默忽略,PVE 实例会从「直进副本」退化成
// 「排队等对手撮合」——而 PVE 没有单边成局逻辑,玩家永远等不到人。此测试锁死旧键仍能点亮 walk_in。
// contract 阶段删除 EnableSoloMatch 字段时,本测试应与之一并删除。
func TestLegacyEnableSoloMatchStillEnablesWalkIn(t *testing.T) {
	var cfg conf.Config
	cfg.Match.EnableSoloMatch = true
	cfg.Defaults()
	if !cfg.Match.WalkIn {
		t.Fatal("旧键 enable_solo_match=true 必须并入 walk_in,否则漏迁移的 PVE 部署会静默退化成撮合模式")
	}
}

// 反向:两个键都没写时不得凭空开启 walk-in(PVP 默认必须是撮合)。
func TestWalkInDefaultsOff(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	if cfg.Match.WalkIn {
		t.Fatal("未配置任何键时 walk_in 必须为 false(默认走撮合)")
	}
}

func TestMatchResumeAuthSecretMustBeIndependent(t *testing.T) {
	cfg := loadConfig(t, "etc/matchmaker-dev.yaml")
	cfg.Match.MatchResumeAuthSecret = cfg.JWT.Secret
	if err := cfg.Validate(); err == nil {
		t.Fatal("player JWT and match resume service identity must not share a key")
	}
}

// Team 与 Login 读的是同一个 ResolvePlayerMatchContext,但必须各持一把 key:
// 共用等于两个服务的信任域合并(任一方可冒充另一方),且换钥变成全有全无。
func TestTeamResumeAuthSecretMustBeIndependent(t *testing.T) {
	base := loadConfig(t, "etc/matchmaker-dev.yaml")
	if base.Match.TeamResumeAuthSecret == "" {
		t.Fatal("dev 模板缺 match.team_resume_auth_secret;缺它 team 入队闸门出厂即 fail-closed、招募列表恒空")
	}
	if base.Match.TeamResumeAuthSecret == base.Match.MatchResumeAuthSecret {
		t.Fatal("dev 模板把 team 与 login 的 resume key 配成了同一把")
	}
	for name, reused := range map[string]string{
		"player-jwt":   base.JWT.Secret,
		"login-resume": base.Match.MatchResumeAuthSecret,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.Match.TeamResumeAuthSecret = reused
			if err := cfg.Validate(); err == nil {
				t.Fatalf("team resume key reused %s trust domain", name)
			}
		})
	}
	cfg := base
	cfg.Match.TeamResumeAuthSecret = "short"
	if err := cfg.Validate(); err == nil {
		t.Fatal("配错(过短)的 team resume key 必须致命:它看着已启用,实际每次 team 调用都静默失败")
	}
}

// 留空是允许的:尚未分发该 key 的存量部署要能继续启动(team 保持被拒 = 原本的
// fail-closed 现状),它绝不能回落到 Login 那把。
func TestTeamResumeAuthSecretOptionalKeepsExistingDeploymentsBootable(t *testing.T) {
	cfg := loadConfig(t, "etc/matchmaker-dev.yaml")
	cfg.Match.TeamResumeAuthSecret = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("未配 team resume key 不应阻断启动: %v", err)
	}
}

func TestAllocationAbortAuthMustBeDedicatedForRealAllocator(t *testing.T) {
	base := loadConfig(t, "etc/matchmaker-dev.yaml")
	for name, reused := range map[string]string{
		"player-jwt":   base.JWT.Secret,
		"login-resume": base.Match.MatchResumeAuthSecret,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.Match.AllocationAbortAuthSecret = reused
			if err := cfg.Validate(); err == nil {
				t.Fatalf("allocation abort key reused %s trust domain", name)
			}
		})
	}
	cfg := base
	cfg.Match.AllocationAbortAuthSecret = "short"
	if err := cfg.Validate(); err == nil {
		t.Fatal("short allocation abort key accepted")
	}
	cfg = base
	cfg.Match.AllocationAbortAuthAudience = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty allocation abort audience accepted")
	}
}
