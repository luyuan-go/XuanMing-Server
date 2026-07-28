// Package conf 是 matchmaker 服务的私有配置结构。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
)

// Config 是 matchmaker 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Match MatchConf `yaml:"match" json:"match"`

	// JWT 用于给战斗 DS 票据签名(matchmaker 全员确认 → 调 ds_allocator 拉 DS →
	// 给每个玩家签一张 battle DSTicket)。secret 必须与 login / Envoy jwt_authn 一致。
	// 留空(无 ds_allocator_addr)时不签票据,仍走 StubDSAllocator。
	JWT JWTConf `yaml:"jwt,omitempty" json:"jwt,omitempty"`

	// DSTicket 是玩家 DSTicket v2(RS256 非对称,方案 B)签发配置。private_key_file 非空
	// 即启用:battle 票据改由 auth.DSTicketSigner 签发并绑死到唯一 DS 实例
	// (ds_allocator 必须回填 gameserver_uid / instance_epoch / allocation_id,
	// 缺失时 fail-closed 拒签)。只要配置真实 ds_allocator_addr，本项就是必填，禁止
	// 回退 legacy HS256；只有不连分配器的 Stub 纯本地模式不会签正式票。
	DSTicket config.DSTicketConf `yaml:"ds_ticket,omitempty" json:"ds_ticket,omitempty"`
}

// JWTConf 是签发 battle DSTicket 的 JWT 参数(镜像 login.JWTConf)。
//
// Issuer / Audience / Secret 必须与 login 服务和 Envoy jwt_authn provider 完全一致。
type JWTConf struct {
	Issuer   string `yaml:"issuer,omitempty" json:"issuer,omitempty"`
	Audience string `yaml:"audience,omitempty" json:"audience,omitempty"`
	Secret   string `yaml:"secret,omitempty" json:"secret,omitempty"`
	// AdditionalSecrets 是**仅用于校验**的额外可接受密钥(不用于签发),支持玩家面
	// JWT 不停服密钥轮换(三段式,同 ds_auth.additional_secrets)。默认空。
	AdditionalSecrets []string        `yaml:"additional_secrets,omitempty" json:"additional_secrets,omitempty"`
	SessionTTL        config.Duration `yaml:"session_ttl,omitempty" json:"session_ttl,omitempty"`
	DSTicketTTL       config.Duration `yaml:"ds_ticket_ttl,omitempty" json:"ds_ticket_ttl,omitempty"`
}

// MatchConf 是 matchmaker 服务私有配置。
type MatchConf struct {
	// TeamAddr 是 team 服务 gRPC 直连地址(StartMatch 时拉取队伍快照校验 READY)。
	// 留空则 StartMatch 跳过 team 校验(本机不起 team 也能跑撮合骨架)。
	TeamAddr string `yaml:"team_addr,omitempty" json:"team_addr,omitempty"`

	// DSAllocatorAddr 是 ds_allocator 服务 gRPC 直连地址(全员确认后拉战斗 DS)。
	// 留空则用 StubDSAllocator(W4 ① 行为,返回固定 mock 地址 + mock 票据)。
	DSAllocatorAddr string `yaml:"ds_allocator_addr,omitempty" json:"ds_allocator_addr,omitempty"`
	// DSAllocateTimeout 是调 ds_allocator.AllocateBattle 的客户端超时(默认 60s)。
	// 该 RPC 在 ds_allocator 侧阻塞等 DS ready 心跳(agones allocate + ready_wait + 余量;
	// dev 大图 ready_wait=120s → 三处配套 150s,见 matchmaker-pve.yaml 注释),
	// 必须 ≥ ds_allocator 的 server.grpc.timeout;不可用 grpcclient.DefaultTimeout(15s),
	// 否则 k8s 真 Linux DS 冷加载大图时 matchmaker 先超时,客户端拿不到 ds_addr。
	DSAllocateTimeout config.Duration `yaml:"ds_allocate_timeout,omitempty" json:"ds_allocate_timeout,omitempty"`
	// LocatorAddr 是 player_locator 服务 gRPC 直连地址（撮合状态机上报玩家位置：
	// 成局→MATCHING、就绪→BATTLE，不变量 §1）。留空则不上报（本机不起 locator 也能跑撮合）。
	LocatorAddr string `yaml:"locator_addr,omitempty" json:"locator_addr,omitempty"`
	// MatchResumeAuthSecret authenticates the sole Login→Matchmaker canonical
	// resume read. It is never shared with player JWT keys.
	MatchResumeAuthSecret   string `yaml:"match_resume_auth_secret,omitempty" json:"match_resume_auth_secret,omitempty"`
	MatchResumeAuthAudience string `yaml:"match_resume_auth_audience,omitempty" json:"match_resume_auth_audience,omitempty"`
	// AllocationAbortAuth authenticates only Matchmaker→DS allocator's exact
	// pre-admission abort request. It must not reuse Login resume, player JWT,
	// or DS callback trust-domain keys.
	AllocationAbortAuthSecret   string `yaml:"allocation_abort_auth_secret,omitempty" json:"allocation_abort_auth_secret,omitempty"`
	AllocationAbortAuthAudience string `yaml:"allocation_abort_auth_audience,omitempty" json:"allocation_abort_auth_audience,omitempty"`

	// BattleGateFailOpen 控制 StartMatch 前置"战斗中禁止匹配"检查在 player_locator 查询失败时的行为。
	//   - false（默认，生产安全 / fail-closed）：locator 查询失败时拒绝入队（返回 ERR_UNAVAILABLE 让客户端重试），
	//     只有明确查到成员非 BATTLE 才放行；避免 locator 短暂抖动 + 旧 claim 过期时绕过保护，把战斗中玩家二次塞进队列。
	//   - true（仅 dev / 弱依赖联调）：locator 查询失败时仅 Warn 后放行，兜底仍由 ClaimPlayer 的 SETNX 保证一人一队列。
	// 注意：locator 完全未配置（LocatorAddr 为空 → 注入 nil）时此开关不生效，直接跳过检查。
	BattleGateFailOpen bool `yaml:"battle_gate_fail_open,omitempty" json:"battle_gate_fail_open,omitempty"`

	// LivenessGateEnabled 是否启用 locator 在线保活的两道离线判定门（默认 false，关闭）：
	//   - 成局最终门（onAllConfirmed）：全员确认后、拉 DS 前批量校验在线，掉线者所在票据判责删除；
	//   - 队列在线扫除（livenessSweepOnce）：周期清扫队列里掉线玩家的死票。
	// 两道门把「locator 无 HUB 位置记录」判为离线，而 HUB 位置续期依赖 Hub DS 心跳捎带的
	// player_ids 字段（hub/v1/allocator.proto HeartbeatRequest.player_ids）。UE Hub DS 生产端
	// 尚未上报该字段前开启，会把全部在线玩家在 locator TTL（30s）后误判离线、扫掉排队票据。
	// 必须等 Hub DS 侧联发后才可开启；关闭时行为与旧版一致（仅靠票据 TTL / 确认超时兜底）。
	LivenessGateEnabled bool `yaml:"liveness_gate_enabled,omitempty" json:"liveness_gate_enabled,omitempty"`
	// MapId 撮合成局后请求的战斗地图配置 ID(配置表 ID,uint32)。
	MapId uint32 `yaml:"map_id,omitempty" json:"map_id,omitempty"`

	// GameMode 战斗模式标识(如 "5v5_ranked"),透传给 ds_allocator。
	GameMode string `yaml:"game_mode,omitempty" json:"game_mode,omitempty"`

	// ConfirmTimeout 确认期时长,凑齐 10 人后等待全员确认的窗口(默认 15s)。
	ConfirmTimeout config.Duration `yaml:"confirm_timeout,omitempty" json:"confirm_timeout,omitempty"`

	// MatchInterval 后台撮合循环的扫描间隔(默认 2s)。
	MatchInterval config.Duration `yaml:"match_interval,omitempty" json:"match_interval,omitempty"`

	// AllocationWorkers DS 分配的有界并发 worker 数(默认 16;压测审核【必修-3】)。
	// 分配含最长 ~60s 的 RPC(agones allocate + DS ready_wait),此前在撮合 leader 单协程
	// 内联串行,吞吐被钳到「1 局/单次分配时延」。16 取审核建议区间(16~64)下限,
	// 待实测复核;<=0 退化为 1(串行,等价历史行为)。
	AllocationWorkers int `yaml:"allocation_workers,omitempty" json:"allocation_workers,omitempty"`

	// MaxQueueTickets StartMatch 准入的排队票据数上限(默认 5000;压测审核【必修-4】)。
	// 撮合循环每 tick 全量拉 queue + 逐票 GET + O(N log N) 排序,队列 ~1e4 时单 tick 耗时
	// 已逼近 match_interval(2s)→ tick 堆积正反馈雪崩。超限 StartMatch 返回 ERR_RATE_LIMITED
	// (可重试忙),把无界积压换成有界的显式排队失败(§9.18 精神)。5000 取雪崩阈值一半,
	// 待实测复核。<=0 = 不限(不推荐,仅联调)。
	MaxQueueTickets int `yaml:"max_queue_tickets,omitempty" json:"max_queue_tickets,omitempty"`

	// TicketTTL 排队票据 Redis key 的 TTL(默认 30min,防僵尸票据)。
	TicketTTL config.Duration `yaml:"ticket_ttl,omitempty" json:"ticket_ttl,omitempty"`

	// MatchTTL 已撮合 match Redis key 的 TTL(默认 30min)。
	MatchTTL config.Duration `yaml:"match_ttl,omitempty" json:"match_ttl,omitempty"`

	// TeamSize 一方人数(MOBA 5v5,一方 5 人)。
	TeamSize int `yaml:"team_size,omitempty" json:"team_size,omitempty"`

	// WalkIn 是「即时开局 / walk-in」分叉开关:开启后 matchOnce 对每张票走 formSoloMatch——
	// 单人或整队立即成局、跳过撮合与确认、直接拉起 Battle DS,不与陌生人凑对手。
	//   - PVP 实例(matchmaker-dev.yaml):false —— 走正常撮合(凑齐 2×TeamSize + 确认期)。
	//   - PVE 实例(matchmaker-pve.yaml,game_mode=pve_coop):true —— 「组好队 / 单人直进副本」的生产核心路径。
	// 2026-07-25 由 enable_solo_match 正名而来(decision-dungeon-entry-modes.md §5):旧名含 solo
	// 且历史注释写「测试专用」,与其「入口是否撮合」的真实语义不符,也掩盖了它是 PVE 生产开关的事实。
	WalkIn bool `yaml:"walk_in,omitempty" json:"walk_in,omitempty"`

	// EnableSoloMatch 是 WalkIn 的**废弃旧键名**,保留仅为滚动升级期兼容(§9.21 expand→migrate→contract
	// 的 migrate 阶段):新二进制可能先于 ConfigMap / yaml 上线,此时旧部署里只有 enable_solo_match。
	// Defaults() 把它**并入**(OR)WalkIn,不做覆盖——见那里的 fail-safe 方向说明。
	// 契约:新配置一律写 walk_in;待全部部署迁移完毕后删除本字段(contract 阶段)。
	EnableSoloMatch bool `yaml:"enable_solo_match,omitempty" json:"enable_solo_match,omitempty"`

	// AutoConfirmMatch 只作用于撮合(versus)路径:开启后凑齐成局即视为全员已确认、跳过确认期直接拉 DS。
	//   - 默认 false —— 保留真实确认期(玩家点接受),PVP 正式对局用;
	//   - dev / 压测设 true 省去人工点确认(robot/stress 默认与之对齐)。
	// 与 solo / walk-in 无关:formSoloMatch 本就直接建成 confirmAccepted,不看本开关
	//(PVE 实例保留 true 仅防误配,walk-in 下不生效)。
	AutoConfirmMatch bool `yaml:"auto_confirm_match,omitempty" json:"auto_confirm_match,omitempty"`

	// MmrBaseWindow 初始 MMR 撮合窗口半宽(默认 200);两张票 avg_mmr 差 ≤ 窗口才可同场。
	MmrBaseWindow int `yaml:"mmr_base_window,omitempty" json:"mmr_base_window,omitempty"`

	// MmrWidenPerSec 每等待 1 秒窗口放宽的 MMR(默认 20),等待越久越容易撮合。
	MmrWidenPerSec int `yaml:"mmr_widen_per_sec,omitempty" json:"mmr_widen_per_sec,omitempty"`

	// MmrMaxWindow MMR 窗口放宽上限(默认 2000),超过即不再放宽。
	MmrMaxWindow int `yaml:"mmr_max_window,omitempty" json:"mmr_max_window,omitempty"`

	// OptimisticRetry WATCH/MULTI/EXEC 乐观锁冲突时最大重试次数。
	// 耗尽后返回 ErrMatchConcurrent(4006)。
	OptimisticRetry int `yaml:"optimistic_retry,omitempty" json:"optimistic_retry,omitempty"`

	// Leader 控制后台撮合循环的单写者选举(见
	// docs/design/decision-revisit-matchmaker-single-writer.md)。
	Leader LeaderConf `yaml:"leader,omitempty" json:"leader,omitempty"`
}

// LeaderConf 控制后台撮合循环的单写者选举。
//
// 背景:撮合循环在共享队列上做全局优化,天然是单写者问题。多副本部署时,若每个副本都无条件
// 跑循环,会重复成局(同一玩家进两场 match,违反不变量 §1)。
//
//   - Enabled=false(默认):本副本直接跑 RunMatchLoop(单副本 / dev 行为不变)。
//   - Enabled=true:经 etcd 选举,仅当选副本跑撮合循环,其余副本只服务 RPC + 热备;
//     失主自动交棒,满足不停机滚动更新(不变量 §16)。
type LeaderConf struct {
	// Enabled 是否启用选举门禁(多副本部署必开)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// EtcdEndpoints etcd 地址(Enabled=true 时必填)。
	EtcdEndpoints []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	// Prefix 选举 key 前缀,留空用 etcdleader 默认(/pandora/leader/)。
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	// LeaseTTLSec session lease TTL(秒),留空用 etcdleader 默认(15);失主检测粒度 ≈ 此值。
	LeaseTTLSec int `yaml:"lease_ttl_sec,omitempty" json:"lease_ttl_sec,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发 panic。
func (c *Config) Defaults() {
	// 旧键兼容(walk_in 的前身 enable_solo_match,2026-07-25 正名)。用 OR 并入而非覆盖:
	// 漏迁移的部署若被静默判成 false,PVE 实例会从「单人/整队直进副本」退化为「排队等对手撮合」,
	// 而 PVE 侧根本没有单边成局逻辑(只产 A/B 对战结构),玩家会永远等不到人——比误开 walk-in
	// 严重得多,故 fail-safe 方向取「保住 walk-in」。旧字段值不清空,留给 main.go 打废弃告警。
	if c.Match.EnableSoloMatch {
		c.Match.WalkIn = true
	}
	if c.Match.DSAllocateTimeout == 0 {
		c.Match.DSAllocateTimeout = config.Duration(60 * time.Second)
	}
	if c.Match.ConfirmTimeout == 0 {
		c.Match.ConfirmTimeout = config.Duration(15 * time.Second)
	}
	if c.Match.MatchInterval == 0 {
		c.Match.MatchInterval = config.Duration(2 * time.Second)
	}
	if c.Match.MaxQueueTickets == 0 {
		c.Match.MaxQueueTickets = 5000
	}
	if c.Match.AllocationWorkers == 0 {
		c.Match.AllocationWorkers = 16
	}
	if c.Match.TicketTTL == 0 {
		c.Match.TicketTTL = config.Duration(30 * time.Minute)
	}
	if c.Match.MatchTTL == 0 {
		c.Match.MatchTTL = config.Duration(30 * time.Minute)
	}
	if c.Match.TeamSize == 0 {
		c.Match.TeamSize = 5
	}
	// 越界钳制(复审 P1:全局 YAML match.team_size 此前完全不校验)。撮合按 need=2*teamSize
	// 预分配票据切片:负值(int 型 YAML 可为负)会导致 make 负容量 panic,巨值会 OOM。
	// 钳到 [1, configtable.MaxLevelTeamSize],与关卡表入口同一上限常量,防阈值漂移。
	if c.Match.TeamSize < 1 {
		c.Match.TeamSize = 1
	} else if c.Match.TeamSize > configtable.MaxLevelTeamSize {
		c.Match.TeamSize = configtable.MaxLevelTeamSize
	}
	if c.Match.MmrBaseWindow == 0 {
		c.Match.MmrBaseWindow = 200
	}
	if c.Match.MmrWidenPerSec == 0 {
		c.Match.MmrWidenPerSec = 20
	}
	if c.Match.MmrMaxWindow == 0 {
		c.Match.MmrMaxWindow = 2000
	}
	if c.Match.OptimisticRetry == 0 {
		c.Match.OptimisticRetry = 3
	}
	if c.Match.MapId == 0 {
		c.Match.MapId = 1
	}
	if c.Match.GameMode == "" {
		c.Match.GameMode = "5v5_ranked"
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":50011"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":51011"
	}
}

// Validate rejects service-auth configurations that would silently reopen the
// internal resolver or collapse independent trust domains.
func (c *Config) Validate() error {
	if err := internalrpcauth.ValidateSecret(c.Match.MatchResumeAuthSecret); err != nil {
		return fmt.Errorf("match.match_resume_auth_secret invalid: %w", err)
	}
	if err := internalrpcauth.ValidateIdentity(c.Match.MatchResumeAuthAudience); err != nil {
		return fmt.Errorf("match.match_resume_auth_audience invalid: %w", err)
	}
	if c.Match.MatchResumeAuthSecret == c.JWT.Secret {
		return fmt.Errorf("match.match_resume_auth_secret must use an independent trust-domain key")
	}
	if c.Match.DSAllocatorAddr != "" {
		if err := internalrpcauth.ValidateSecret(c.Match.AllocationAbortAuthSecret); err != nil {
			return fmt.Errorf("match.allocation_abort_auth_secret invalid: %w", err)
		}
		if err := internalrpcauth.ValidateIdentity(c.Match.AllocationAbortAuthAudience); err != nil {
			return fmt.Errorf("match.allocation_abort_auth_audience invalid: %w", err)
		}
		for name, secret := range map[string]string{
			"player JWT":   c.JWT.Secret,
			"Login resume": c.Match.MatchResumeAuthSecret,
		} {
			if c.Match.AllocationAbortAuthSecret == secret {
				return fmt.Errorf("match.allocation_abort_auth_secret must not reuse %s key", name)
			}
		}
	}
	return nil
}
