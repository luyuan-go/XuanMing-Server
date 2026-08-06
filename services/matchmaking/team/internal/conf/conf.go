// Package conf 是 team 服务的私有配置结构。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 team 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Team TeamConf `yaml:"team" json:"team"`
}

// TeamConf 是 team 服务私有配置。
type TeamConf struct {
	// InviteTTL 邀请令牌 Redis key 的 TTL,客户端须在此时间内 AcceptInvite。
	InviteTTL config.Duration `yaml:"invite_ttl,omitempty" json:"invite_ttl,omitempty"`

	// DisbandedRetention 队伍解散后 Redis key 的保留时长,供客户端查询最终状态。
	DisbandedRetention config.Duration `yaml:"disbanded_retention,omitempty" json:"disbanded_retention,omitempty"`

	// ActiveTTL 活跃队伍(未解散)Redis key 的生命周期。
	// 队伍在此时间内无任何写操作则整体过期消失,防止僵尸队伍长期占用 Redis。
	ActiveTTL config.Duration `yaml:"active_ttl,omitempty" json:"active_ttl,omitempty"`

	// MaxMembers MOBA 5v5,一队最多允许多少成员。
	MaxMembers int `yaml:"max_members,omitempty" json:"max_members,omitempty"`

	// OptimisticRetry WATCH/MULTI/EXEC 乐观锁冲突时最大重试次数。
	// 耗尽后返回 ErrTeamConcurrent(3007)。
	OptimisticRetry int `yaml:"optimistic_retry,omitempty" json:"optimistic_retry,omitempty"`

	// MatchmakerAddr matchmaker 服务 gRPC 直连地址(host:port,内网 insecure)。
	// 成员离队/被踢时联动撤销其所在的匹配票据(弱依赖):留空 → 不联动,
	// 行为与历史一致(本机不起 matchmaker 的骨架联调路径)。
	MatchmakerAddr string `yaml:"matchmaker_addr,omitempty" json:"matchmaker_addr,omitempty"`

	// MatchResumeAuthSecret / MatchResumeAuthAudience 是 team→matchmaker 调
	// ResolvePlayerMatchContext 的东西向服务鉴权凭据(pkg/internalrpcauth),caller 固定
	// 签成 "team"。matchmaker 侧对该方法强制验签,**不签名一律 ERR_PERMISSION_DENY(7)**。
	//
	// 这把密钥必须与 login 那把 match_resume_auth_secret **不同**(matchmaker 侧按 caller
	// 分别持有独立密钥,共用会把两个服务的信任域合并),对应 matchmaker 的
	// match.team_resume_auth_secret。audience 填 matchmaker 的 match_resume_auth_audience
	// (audience 标识被调方服务池,不区分调用方,故与 login 相同)。
	//
	// 留空 → 不签名:入队闸门与招募列表复核都会拿不到判定,前者 fail-closed 拒绝入队、
	// 后者返回空列表。因此 matchmaker_addr 已配时留空会在启动打 WARN。
	MatchResumeAuthSecret   string `yaml:"match_resume_auth_secret,omitempty" json:"match_resume_auth_secret,omitempty"`
	MatchResumeAuthAudience string `yaml:"match_resume_auth_audience,omitempty" json:"match_resume_auth_audience,omitempty"`

	// InvitePushMode 邀请推送模式(金丝雀灰度用)。老客户端只认 TeamUpdateEvent(reason=INVITE_SENT),
	// 新客户端只认独立的 TeamInviteEvent(event_type=INVITE=1、已不再从 TeamUpdateEvent 读邀请)。
	// 两代客户端各认各的 payload,单一模式无法同时喂饱两代 → 灰度共存期必须"双发"。
	//   - "dual"(默认):两条都发。老客户端用 legacy 弹框、把独立事件当 TeamUpdateEvent 误解→
	//     InviteId/TeamId=0→护栏不过→仅多一次无害快照,不误弹;新客户端忽略 legacy(纯化后不读)、
	//     只在独立事件弹框。**各弹一次、不双弹**(客户端已纯化,故双发安全,金丝雀期首选)。
	//   - "dedicated":只发独立事件。全量铺完新客户端后切到此值,停发冗余 legacy。老客户端会漏弹。
	//   - "legacy":只发旧 TeamUpdateEvent。回退用;新客户端会漏弹邀请框。
	// 空串按 "dual" 处理(见 Defaults)。
	InvitePushMode string `yaml:"invite_push_mode,omitempty" json:"invite_push_mode,omitempty"`

	// MaxPendingInvites 同一被邀请人的未过期 pending 邀请数上限(不变量 §9-18:
	// 组队邀请是客户端可写入、可堆积、会被拉取展示的列表,必须有写入侧上限)。
	// 超限 Invite 返 ErrTeamInvitePendingLimit(3008);ListMyPendingInvites 单次返回
	// 也按此值截断(写入侧硬上限已兜住总量,无需分页)。默认 10。
	MaxPendingInvites int `yaml:"max_pending_invites,omitempty" json:"max_pending_invites,omitempty"`

	// ── 找队伍(ListOpenTeams / ApplyToTeam / HandleTeamApplication) ──────────

	// JoinPolicy 决定 ApplyToTeam 的语义,全服一份配置(不给每支队伍存,§9.22 不重复影子状态):
	//   - "approval"(默认):申请进入队伍待处理列表,队长同意才入队;
	//   - "open"          :队伍未满即当场入队,不需要队长操作。
	// 空串按 "approval" 处理(见 Defaults):默认保守——不经队长同意就把陌生人塞进队伍
	// 属于"多做了玩家没授权的事",反过来只是多一次点击。
	// ParseJoinPolicy 对无法识别的值**报错而非猜**(拼错一个字母就变成任何人都能进队,
	// 是不可接受的失败模式),启动 ValidateJoinPolicy fail-fast。
	JoinPolicy string `yaml:"join_policy,omitempty" json:"join_policy,omitempty"`

	// MaxOpenTeamsPerQuery 单次 ListOpenTeams 返回的队伍数上限(读取侧上限,不变量 §9-18)。
	// 请求里的 limit=0 取本值,>本值一律钳到本值。默认 10。
	MaxOpenTeamsPerQuery int `yaml:"max_open_teams_per_query,omitempty" json:"max_open_teams_per_query,omitempty"`

	// MaxApplicationsPerTeam 同一队伍未过期 pending 入队申请数上限(写入侧上限,不变量 §9-18:
	// 入队申请是客户端可写入、可堆积、会被队长拉取展示的列表)。
	// 超限 ApplyToTeam 返 ErrTeamApplyPendingLimit(3009);ListTeamApplications 单次返回
	// 也按此值截断(写入侧硬上限已兜住总量,无需分页)。默认 10。
	MaxApplicationsPerTeam int `yaml:"max_applications_per_team,omitempty" json:"max_applications_per_team,omitempty"`

	// ApplyTTL 入队申请令牌的存活时长。取值依据:队长要能在一次组队界面停留内看到并处理
	// (推送到达 + 人眼反应 + 点击),同时申请人不该被无限期挂着——到期后客户端按钮自动
	// 恢复可点,是"有界等待 + 可见出口"(验收底线第 1 条)。默认 120s(邀请是 60s,
	// 申请方向多一次队长注意力成本,故取两倍)。**待实测复核**:若线上出现大量
	// "队长还没看到就过期",优先调大本值而不是取消上限。
	ApplyTTL config.Duration `yaml:"apply_ttl,omitempty" json:"apply_ttl,omitempty"`
}

// 入队策略取值(JoinPolicy)。
const (
	// JoinPolicyApproval 申请 → 队长审批 → 入队。
	JoinPolicyApproval = "approval"
	// JoinPolicyOpen 未满即直接入队。
	JoinPolicyOpen = "open"
)

// ParseJoinPolicy 把配置字符串解析成合法策略值。
//
// 空串 = 默认 approval(保守)。**无法识别的值报错,绝不猜**:把 "aproval" 猜成 open
// 会让全服队伍对陌生人敞开,属于静默的权限放大,失败模式不可接受(同 dbguard.ParseMode
// 对 retention_mode 的处理口径)。
func ParseJoinPolicy(s string) (string, error) {
	switch s {
	case "":
		return JoinPolicyApproval, nil
	case JoinPolicyApproval, JoinPolicyOpen:
		return s, nil
	default:
		return "", fmt.Errorf("team: unknown join_policy %q (want %q or %q)", s, JoinPolicyApproval, JoinPolicyOpen)
	}
}

// ValidateJoinPolicy 供 main 启动时 fail-fast:配置写错不该等到第一个玩家点"申请"才暴露。
func (c *Config) ValidateJoinPolicy() error {
	_, err := ParseJoinPolicy(c.Team.JoinPolicy)
	return err
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发 panic。
func (c *Config) Defaults() {
	if c.Team.InviteTTL == 0 {
		c.Team.InviteTTL = config.Duration(60 * time.Second)
	}
	if c.Team.DisbandedRetention == 0 {
		c.Team.DisbandedRetention = config.Duration(5 * time.Minute)
	}
	if c.Team.ActiveTTL == 0 {
		c.Team.ActiveTTL = config.Duration(60 * time.Minute)
	}
	if c.Team.MaxMembers == 0 {
		c.Team.MaxMembers = 5
	}
	if c.Team.OptimisticRetry == 0 {
		c.Team.OptimisticRetry = 3
	}
	if c.Team.InvitePushMode == "" {
		// 金丝雀期默认双发:老/新客户端各认各的 payload,互不干扰,各弹一次。
		c.Team.InvitePushMode = "dual"
	}
	if c.Team.MaxPendingInvites == 0 {
		c.Team.MaxPendingInvites = 10
	}
	if c.Team.JoinPolicy == "" {
		// 保守默认:需要队长同意。改成 open 必须是显式配置动作。
		c.Team.JoinPolicy = JoinPolicyApproval
	}
	if c.Team.MaxOpenTeamsPerQuery == 0 {
		c.Team.MaxOpenTeamsPerQuery = 10
	}
	if c.Team.MaxApplicationsPerTeam == 0 {
		c.Team.MaxApplicationsPerTeam = 10
	}
	if c.Team.ApplyTTL == 0 {
		c.Team.ApplyTTL = config.Duration(120 * time.Second)
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20010"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21010"
	}
}
