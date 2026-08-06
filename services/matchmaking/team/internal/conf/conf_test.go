// conf_test.go — 校验 etc/ 下 team 部署配置能被 main.go 同款加载路径解析,且鉴权字段齐备。
// 这里盯的是一类**无声故障**:team→matchmaker 的 ResolvePlayerMatchContext 强制验签,
// 少了 secret / audience 拼错,两端进程照常启动、日志一片正常,只表现为
// 「招募列表恒空 + 申请入队恒失败」。YAML 手滑必须在测试里就红。
package conf_test

import (
	"path/filepath"
	"strings"
	"testing"

	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
)

func TestValidateOfflineLeaveDisabled(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	if err := cfg.ValidateOfflineLeave(); err != nil {
		t.Fatalf("offline_leave 默认关闭时不应拒绝启动: %v", err)
	}
}

func TestValidateOfflineLeaveEnabledFailsWithoutSharedRosterFence(t *testing.T) {
	var cfg conf.Config
	cfg.Defaults()
	cfg.Team.OfflineLeave.Enabled = true
	// 两个读取依赖都配齐也不能弥合跨服务 TOCTOU；该用例防止以后误把“能查到状态”
	// 当成“摘人与 StartMatch 已经共享线性化点”。
	cfg.Team.LocatorAddr = "player-locator:20006"
	cfg.Team.MatchmakerAddr = "matchmaker:20011"

	err := cfg.ValidateOfflineLeave()
	if err == nil {
		t.Fatal("缺少与 StartMatch 共用的 roster fence 时，offline_leave.enabled=true 必须拒绝启动")
	}
	if !strings.Contains(err.Error(), "roster fence") {
		t.Fatalf("拒启原因必须明确指出缺少 roster fence，实际错误: %v", err)
	}
}

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

// 配了 matchmaker_addr 就等于启用了入队闸门与招募列表复核,这两条路径都必须签名。
func TestDevConfigCarriesMatchResumeAuthCredential(t *testing.T) {
	cfg := loadConfig(t, "etc/team-dev.yaml")
	if cfg.Team.MatchmakerAddr == "" {
		t.Skip("dev 模板未接 matchmaker,鉴权凭据无从校验")
	}
	if cfg.Team.MatchResumeAuthSecret == "" {
		t.Fatal("team-dev.yaml 缺 team.match_resume_auth_secret;matchmaker 会拒掉每一次调用,表现为招募列表恒空 + 入队恒失败")
	}
	if cfg.Team.MatchResumeAuthAudience == "" {
		t.Fatal("team-dev.yaml 缺 team.match_resume_auth_audience;audience 不匹配同样是静默被拒")
	}
	// main.go 用同一组参数构造 signer,构造失败即进程退出。让 YAML 里的弱 key 在这里就红。
	if _, err := internalrpcauth.NewSigner(cfg.Team.MatchResumeAuthSecret, "team",
		cfg.Team.MatchResumeAuthAudience); err != nil {
		t.Fatalf("dev 配置无法构造 team resume signer: %v", err)
	}
}
