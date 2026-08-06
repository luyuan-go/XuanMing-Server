// Package scenario 定义压测场景配置与加载。
//
// 配置用 JSON(非 yaml),保证 robot 机群零外部依赖可离线构建。
// 5 个开放问题的推荐默认值(设计文档 §10)全部落到这里、可被配置覆盖:
//  1. 机器成本 —— 不在代码内决策(人定),仅由 VUCount / RampSeconds 体现规模。
//  2. 阶段 1 DS —— DSMode 默认 "stub"(只压后端,不起真 DS)。
//  3. 复用 run_services.ps1 停服,不在 robot 内做。
//  4. 账号 —— AccountPrefix + 首次登录自动注册(login devAutoRegister)。
//  5. 注入单 (region,cell) —— Router.RegionID/CellID 默认 0(单 Cell)。
package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Targets 是各后端服务的直连 gRPC 地址(host:port)。
// 默认对齐 docs/design/infra.md §6.2 端口表,本机单 Cell 部署。
type Targets struct {
	Login        string `json:"login"`
	Player       string `json:"player"`
	DataService  string `json:"data_service"`
	Friend       string `json:"friend"`
	Chat         string `json:"chat"`
	Locator      string `json:"player_locator"`
	Team         string `json:"team"`
	Matchmaker   string `json:"matchmaker"`
	Auction      string `json:"auction"`
	BattleResult string `json:"battle_result"`
	Push         string `json:"push"`
	// EnvoyAddr 是 gRPC-Web 入口(https://host:8443),给对照样本 VU 走完整边缘链路。
	EnvoyAddr string `json:"envoy_addr"`
}

// BehaviorWeights 是大厅稳态各类操作的相对权重(设计文档 §6),可配置。
// 调度器按归一化后的概率加权随机挑动作,值不必和为 100。
type BehaviorWeights struct {
	LocatorSetLocation int `json:"locator_set_location"` // 心跳 / 位置上报(默认最高)
	PlayerGetProfile   int `json:"player_get_profile"`
	TeamGetMyTeam      int `json:"team_get_my_team"`
	FriendListFriends  int `json:"friend_list_friends"`
	ChatSendMessage    int `json:"chat_send_message"`
	AuctionListMarket  int `json:"auction_list_market"`
	MatchFlow          int `json:"match_flow"` // 组队→匹配→确认→battle_result 上报整链
}

// Router 是注入给 VU 的单 (region,cell) 落点,用于在单 Cell 压测时
// 观察 owner-cell 锚定埋点(team_composition_routing / profile_placement 等)。
type Router struct {
	RegionID uint32 `json:"region_id"`
	CellID   uint32 `json:"cell_id"`
}

// Config 是一次压测的完整场景描述。
type Config struct {
	Name    string  `json:"name"`
	Targets Targets `json:"targets"`

	// VUCount 目标虚拟玩家数;RampSeconds 线性爬坡时长;SteadySeconds 稳态保持时长。
	VUCount       int `json:"vu_count"`
	RampSeconds   int `json:"ramp_seconds"`
	SteadySeconds int `json:"steady_seconds"`

	// MachineID 标识本压测进程(多机时区分),写进 robot-stats.jsonl。
	MachineID string `json:"machine_id"`

	// DSMode: "stub"(默认,只压后端) / "real"(接真 DS,阶段 1 不用)。
	DSMode string `json:"ds_mode"`

	// AutoConfirmMatch 必须与 matchmaker 的 auto_confirm_match 保持一致。
	// true:服务端撮合后自动确认并直接拉 DS,VU 不再发 ConfirmMatch,只轮询到 READY/ALLOCATING
	// 即视为成局。原因:auto_confirm 下确认期 ~1s 内 match 仍持久化为 CONFIRM(onAllConfirmed 在
	// AllocateBattle 返回后才写 READY),VU 抢发 ConfirmMatch 会与「自动确认→拉DS→上报→释放」流水线
	// 竞态、撞上已推进/已删除的 match 而报错。需与服务端配置一致,否则手动/自动确认口径错位。
	AutoConfirmMatch bool `json:"auto_confirm_match"`

	// AccountPrefix 账号前缀,VU 账号 = <prefix><index>,首次登录自动注册。
	AccountPrefix string `json:"account_prefix"`

	// EnvoySampleRatio 走 Envoy 对照链路的 VU 比例(0~1),默认 0.01。
	EnvoySampleRatio float64 `json:"envoy_sample_ratio"`

	// ActionIntervalMs 每个 VU 两次大厅操作之间的基准间隔(泊松抖动围绕它)。
	ActionIntervalMs int `json:"action_interval_ms"`

	Behavior BehaviorWeights `json:"behavior"`
	Router   Router          `json:"router"`

	// StatsFile robot-stats.jsonl 输出路径。
	StatsFile string `json:"stats_file"`
}

// Default 返回阶段 1 单 Cell ~40 万 CCU 的推荐默认配置(5 个开放问题默认值已内置)。
func Default() Config {
	return Config{
		Name: "single-cell-40w",
		Targets: Targets{
			Login:        "127.0.0.1:20001",
			Player:       "127.0.0.1:20002",
			DataService:  "127.0.0.1:20003",
			Friend:       "127.0.0.1:20004",
			Chat:         "127.0.0.1:20005",
			Locator:      "127.0.0.1:20006",
			Team:         "127.0.0.1:20010",
			Matchmaker:   "127.0.0.1:20011",
			Auction:      "127.0.0.1:20016",
			BattleResult: "127.0.0.1:20022",
			Push:         "127.0.0.1:20014",
			EnvoyAddr:    "127.0.0.1:8443",
		},
		VUCount:          400000,
		RampSeconds:      600,
		SteadySeconds:    1800,
		MachineID:        "robot-0",
		DSMode:           "stub",
		AutoConfirmMatch: true,
		AccountPrefix:    "stressbot_",
		EnvoySampleRatio: 0.01,
		ActionIntervalMs: 5000,
		Behavior: BehaviorWeights{
			LocatorSetLocation: 40,
			PlayerGetProfile:   12,
			TeamGetMyTeam:      8,
			FriendListFriends:  8,
			ChatSendMessage:    7,
			AuctionListMarket:  15,
			MatchFlow:          10,
		},
		Router:    Router{RegionID: 0, CellID: 0},
		StatsFile: "robot/logs/robot-stats.jsonl",
	}
}

// Load 读取 JSON 配置,缺省字段用 Default 兜底。path 为空时直接返回默认配置。
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("读取压测配置 %q 失败: %w", path, err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("解析压测配置 %q 失败: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Validate 做基本健壮性检查,避免起一堆 VU 后才发现配置不合理。
func (c Config) Validate() error {
	if c.VUCount <= 0 {
		return fmt.Errorf("vu_count 必须 > 0,当前 %d", c.VUCount)
	}
	if c.RampSeconds < 0 || c.SteadySeconds < 0 {
		return fmt.Errorf("ramp_seconds / steady_seconds 不能为负")
	}
	if c.EnvoySampleRatio < 0 || c.EnvoySampleRatio > 1 {
		return fmt.Errorf("envoy_sample_ratio 必须在 [0,1],当前 %v", c.EnvoySampleRatio)
	}
	if c.ActionIntervalMs <= 0 {
		return fmt.Errorf("action_interval_ms 必须 > 0,当前 %d", c.ActionIntervalMs)
	}
	if c.Targets.Login == "" {
		return fmt.Errorf("targets.login 不能为空")
	}
	return nil
}

// RampInterval 返回每启动一个 VU 之间的平均间隔。
func (c Config) RampInterval() time.Duration {
	if c.RampSeconds <= 0 || c.VUCount <= 0 {
		return 0
	}
	return time.Duration(float64(c.RampSeconds) * float64(time.Second) / float64(c.VUCount))
}

// ActionInterval 返回大厅操作基准间隔。
func (c Config) ActionInterval() time.Duration {
	return time.Duration(c.ActionIntervalMs) * time.Millisecond
}
