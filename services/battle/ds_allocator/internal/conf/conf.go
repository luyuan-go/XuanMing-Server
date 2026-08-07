// Package conf 是 ds_allocator 服务的私有配置结构。
package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
)

// DS 启动后端模式(标准两模式开关 + 离线兜底)。
//
//	ModeLocal  本机 exec Windows DS 进程(LocalDSConf),Windows 单机自测
//	ModeAgones k8s Agones GameServerAllocation(AgonesConf),Linux 线上
//	ModeMock   确定性假地址(无真实 DS),离线联调兜底
const (
	ModeLocal  = "local"
	ModeAgones = "agones"
	ModeMock   = "mock"
)

// Config 是 ds_allocator 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	// Mode 选择 DS 启动后端,与 hub_allocator.mode 对齐的「标准两模式开关」:
	//   "local"  → 本机 exec Windows DS 进程(LocalDSConf,Windows 单机自测)
	//   "agones" → k8s Agones 分配(AgonesConf,Linux 线上)
	//   "mock"   → 确定性假地址(无真实 DS,离线联调)
	// 留空时按 legacy 的 agones.enabled / local_ds.enabled 推导(向后兼容旧配置)。
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`

	Allocator AllocatorConf `yaml:"allocator" json:"allocator"`
	Agones    AgonesConf    `yaml:"agones" json:"agones"`
	LocalDS   LocalDSConf   `yaml:"local_ds" json:"local_ds"`

	// LocatorAddr player_locator gRPC 地址，用于心跳续期短 TTL BATTLE presence/监控。
	// 留空不续期 presence(弱依赖)，但不改变无 TTL 的权威 placement，也绝不能据此回 Hub。
	LocatorAddr string `yaml:"locator_addr,omitempty" json:"locator_addr,omitempty"`

	// DSAuth DS 回调服务令牌(审核 P1 #1:DS→后端回调认证)。本服务两个角色都用它:
	//   - 签发:AllocateBattle 时给战斗 DS 签 battle 令牌(绑 match_id),经 GameServer
	//     annotation(agones)/ PANDORA_DS_TOKEN env(local)下发;secret 配了就签(无害)。
	//   - 校验:Heartbeat / GmService Poll·Ack 按 mode(off/permissive/enforce)验证令牌。
	// 详见 pkg/config.DSAuthConf、docs/design/decision-revisit-ds-callback-auth.md。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// RequiresReliableLifecyclePublication 返回 abandoned 是否必须走可靠的
// pandora.ds.lifecycle 发布链。Redis authority 是生产授权权威，缺失该链会让
// BattleResult 无法生成 match release / battle exit proof；Agones+enforce 的
// legacy 灰度同样属于生产路径，不能以“镜像稍后过期”冒充恢复完成。
func (c *Config) RequiresReliableLifecyclePublication() bool {
	return strings.EqualFold(strings.TrimSpace(c.DSAuth.AuthorityMode), "redis") ||
		(strings.EqualFold(strings.TrimSpace(c.Mode), ModeAgones) &&
			strings.EqualFold(strings.TrimSpace(c.DSAuth.Mode), "enforce"))
}

// ValidateLifecyclePublicationConfig 在任何 Redis/Kubernetes 副作用前锁住生产配置。
// broker 列表中的空白项不算已配置，避免启动后 producer 初始化才发现没有恢复出口。
func (c *Config) ValidateLifecyclePublicationConfig() error {
	if !c.RequiresReliableLifecyclePublication() {
		return nil
	}
	for _, broker := range c.Kafka.Brokers {
		if strings.TrimSpace(broker) != "" {
			return nil
		}
	}
	return fmt.Errorf("ds_allocator: production authority requires kafka.brokers for reliable %s publication", "pandora.ds.lifecycle")
}

// ValidateBattleDepartureConfig: production authority requires locator_addr so
// battle presence renewal (the only in-battle routing signal) keeps working.
func (c *Config) ValidateBattleDepartureConfig() error {
	if c.RequiresReliableLifecyclePublication() && strings.TrimSpace(c.LocatorAddr) == "" {
		return fmt.Errorf("ds_allocator: production authority requires locator_addr for battle presence renewal")
	}
	return nil
}

// ValidateAllocationAbortAuthConfig makes the destructive Matchmaker abort
// endpoint a startup dependency of Redis Model-B. Legacy/local modes do not
// expose a weaker fallback; the RPC itself still fails closed when unwired.
func (c *Config) ValidateAllocationAbortAuthConfig() error {
	if !c.DSAuth.AuthorityModeRedis() {
		return nil
	}
	if err := internalrpcauth.ValidateSecret(c.Allocator.AllocationAbortAuthSecret); err != nil {
		return fmt.Errorf("ds_allocator: allocator.allocation_abort_auth_secret invalid: %w", err)
	}
	if err := internalrpcauth.ValidateIdentity(c.Allocator.AllocationAbortAuthAudience); err != nil {
		return fmt.Errorf("ds_allocator: allocator.allocation_abort_auth_audience invalid: %w", err)
	}
	if c.Allocator.AllocationAbortAuthSecret == c.DSAuth.Secret {
		return fmt.Errorf("ds_allocator: allocation abort auth must use an independent trust-domain key")
	}
	return nil
}

// LocalDSConf 是「本机拉起 Windows Dedicated Server 进程」的调试后端配置。
//
// 这是与 Agones(Linux 生产)并列的第二种 DS 启动方式,专供本机联调:匹配成局后
// ds_allocator 直接 exec 打包好的 UE Windows DS 可执行文件,分配一个本机端口,返回
// 真实地址(host:port)给客户端 NetDriver 连入;Release / 心跳超时 abandoned 时 Kill 进程。
//
// 三种 DS 启动方式互斥,按 main.go 优先级选装配:
//   - agones.enabled=true   → AgonesGameServerAllocator(Linux 生产)
//   - local_ds.enabled=true → LocalGameServerAllocator(本机 Windows 调试,本结构)
//   - 都为 false            → MockGameServerAllocator(确定性假地址,无真实 DS)
//
// agones.enabled 与 local_ds.enabled 不可同时为 true(main.go 会 fatal)。
type LocalDSConf struct {
	// Enabled 打开本机拉起 Windows DS 进程(默认 false)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// Launcher 选择本机 DS 的「进程形态」,两种模式并存、互不影响,只改这一个字段即可切换:
	//   - LauncherPackaged("packaged",默认)= 现状:跑打包好的 PandoraServer.exe。
	//     内容已 cook,策划改 .uasset 必须重新出包才生效;性能接近线上,适合验收/压测。
	//   - LauncherEditor("editor")= 跑 UnrealEditor.exe + .uproject,仍带 -server
	//     (NetMode 依旧是 NM_DedicatedServer,登录/大厅/匹配/心跳/结算全链路一字不改),
	//     但直接读工程里未 cook 的 Content/,策划存盘后下一局即生效,免出包。
	//
	// 两种模式对后端完全透明:差异只在 exec 哪个 exe、命令行首个参数是否要带 .uproject。
	Launcher string `yaml:"launcher,omitempty" json:"launcher,omitempty"`

	// ExecutablePath 本机要 exec 的可执行文件绝对路径,含义随 Launcher 变化:
	//   - packaged:打包好的 UE Windows Dedicated Server
	//     (例如 C:\work\Pandora-Client-SVN\...\PandoraServer.exe)。
	//   - editor:  引擎的 UnrealEditor.exe
	//     (例如 E:\Program Files\UE_5.8\Engine\Binaries\Win64\UnrealEditor.exe)。
	// Enabled=true 时必填且必须存在。
	ExecutablePath string `yaml:"executable_path,omitempty" json:"executable_path,omitempty"`

	// ProjectPath 工程 .uproject 绝对路径,仅 Launcher=editor 时必填且必须存在(packaged 忽略)。
	// UE 的 LaunchSetGameName 只认命令行里第一个不以 '-' 开头的 token,所以它必须排在关卡 URL 之前;
	// buildArgs 已保证该顺序。
	ProjectPath string `yaml:"project_path,omitempty" json:"project_path,omitempty"`

	// LoaderMap 非空时,DS 统一启动到这张「加载 / 分发关卡」,而不是直接启到目标副本图。
	// 目标副本由 UE 侧 Loader GameMode 在 BeginPlay 读 PANDORA_MAP_ID(本 allocator 已经注入)→
	// 查 g_关卡.xlsx → ServerTravel 过去(见 Doc/服务器/副本选择_UE侧交接_Codex.md)。
	// 这是「策划填表即用」的生产权威路径:allocator 只传数字 map_id(env),不再把 umap 路径写进命令行,
	// 策划新增副本 = 改表 + 重打 DS 内容,服务端零改动。留空(默认)= 沿用 Maps/MapName 直接启到目标图的
	// dev 桥(仍要求每加副本改 yaml),向后兼容。启用前提:UE 侧已交付 Loader 关卡 + Loader GameMode。
	//
	// 例:"/Game/Test/Level/Lvl_DS_Loader?game=/Script/Pandora.PandoraDSLoaderGameMode"。
	LoaderMap string `yaml:"loader_map,omitempty" json:"loader_map,omitempty"`

	// AdvertiseHost 返回给客户端的可连接 host(默认 127.0.0.1,本机联调)。
	AdvertiseHost string `yaml:"advertise_host,omitempty" json:"advertise_host,omitempty"`

	// PortBase 分配给 DS 进程的端口基址(默认 7777)。
	PortBase int `yaml:"port_base,omitempty" json:"port_base,omitempty"`

	// PortRange 端口池大小(默认 100),实际端口在 [PortBase, PortBase+PortRange) 内取空闲。
	PortRange int `yaml:"port_range,omitempty" json:"port_range,omitempty"`

	// WorkingDir DS 进程工作目录(留空用 ds_allocator 当前目录)。
	WorkingDir string `yaml:"working_dir,omitempty" json:"working_dir,omitempty"`

	// LogDir DS 进程 stdout/stderr 落盘目录(默认 run/dev/logs/ds);每进程一个 <pod>.log。
	LogDir string `yaml:"log_dir,omitempty" json:"log_dir,omitempty"`

	// ExtraArgs 追加到 DS 命令行末尾的额外参数(例如后端 gRPC-Web 入口地址覆盖)。
	ExtraArgs []string `yaml:"extra_args,omitempty" json:"extra_args,omitempty"`

	// ExtraEnv 注入 DS 进程的额外环境变量(在 PANDORA_MATCH_ID 等内置变量之后追加)。
	ExtraEnv map[string]string `yaml:"extra_env,omitempty" json:"extra_env,omitempty"`
}

// 本机 DS 的两种进程形态(LocalDSConf.Launcher 的合法取值)。
const (
	// LauncherPackaged 跑打包好的 PandoraServer.exe(默认,现状行为)。
	LauncherPackaged = "packaged"
	// LauncherEditor 跑 UnrealEditor.exe + .uproject -server,读未 cook 的工程内容,免出包。
	LauncherEditor = "editor"
)

// ValidateLocalMapSourceConfig 锁死 mode=local 的「关卡从哪来」:必须有唯一权威源。
//
// 2026-08-04 之前这里是一张 local_ds.maps 手抄表(map_id → UE 关卡 URL),它把关卡表
// (g_关卡.xlsx → configtable/dist/level.json)的 asset_path / game_mode_class 两列抄了第二份,
// 违反 §9.22(不重复存储影子状态)。抄漏一行的后果不是"回退默认图"而是"这张图永远进不去":
// DS 起了默认图 → DS 侧关卡门判「已加载世界 ≠ 注入 map_id」→ fail-closed 自杀 → 分配卡到超时,
// 玩家侧只看到"一直排队中"(map_id=11 实测)。该表已整块删除,关卡一律现查关卡表。
//
// 因此 mode=local 必须二选一,都没有就拒绝启动(而不是启动后每局失败):
//   - config_table.dir:allocator 读关卡表按 map_id 拼启动 URL(默认路径);
//   - local_ds.loader_map:DS 统一启到加载 / 分发关卡,由 UE 侧 Loader GameMode 读同一张表决定目标图。
func (c *Config) ValidateLocalMapSourceConfig() error {
	if c.Mode != ModeLocal {
		return nil
	}
	if strings.TrimSpace(c.LocalDS.LoaderMap) != "" || strings.TrimSpace(c.ConfigTable.Dir) != "" {
		return nil
	}
	return fmt.Errorf("ds_allocator: mode=local 必须配 config_table.dir(关卡按 map_id 现查 g_关卡.xlsx)" +
		"或 local_ds.loader_map(DS 侧 Loader 查同一张表);两者皆空则无处得知 map_id 对应哪张图")
}

// AgonesConf 是真 Agones GameServerAllocation 后端配置(W4 ⑫)。
//
// Enabled=false(默认)→ 用 MockGameServerAllocator;Enabled=true → 用
// AgonesGameServerAllocator(经 k8s apiserver REST 调 allocation.agones.dev/v1
// GameServerAllocation,provider 无关:ACK / 自建 / minikube 上跑的 Agones 都一致)。
//
// 集群内运行时 token_path / ca_path / api_server / namespace 留空即用 in-cluster 默认;
// 集群外联调可显式指定 api_server + token_path(或经 kubectl proxy 不带 token)。
type AgonesConf struct {
	// Enabled 打开真 Agones 分配(默认 false → Mock)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// APIServer k8s apiserver 地址(默认 https://kubernetes.default.svc,in-cluster)。
	APIServer string `yaml:"api_server,omitempty" json:"api_server,omitempty"`

	// Namespace GameServerAllocation / GameServer 所在命名空间(默认 default)。
	Namespace string `yaml:"namespace,omitempty" json:"namespace,omitempty"`

	// FleetName 选择 GameServer 的 Fleet 名(selector agones.dev/fleet=<FleetName>)。
	// Enabled=true 时必填,否则构造失败。它是「通用池」/兜底池:未命中 MapFleets 的 map_id
	// 或专属池无空闲时都落到它(通常是 Loader 模式的 Fleet,分配后按 label travel)。
	FleetName string `yaml:"fleet_name,omitempty" json:"fleet_name,omitempty"`

	// CanaryFleetName 是 canary 通用池。CanaryPercent>0 时必填；stable 请求永不
	// 选择它，canary 请求无 Ready 时可按同一 GSA selector 顺序回退 FleetName。
	CanaryFleetName string `yaml:"canary_fleet_name,omitempty" json:"canary_fleet_name,omitempty"`

	// CanaryPercent/CanarySeed 以 match_id 做确定性 cohort，同一局永不拆轨。
	CanaryPercent uint32 `yaml:"canary_percent,omitempty" json:"canary_percent,omitempty"`
	CanarySeed    string `yaml:"canary_seed,omitempty" json:"canary_seed,omitempty"`

	// MapFleets 按 map_id 路由到专属预热 Fleet(可选,标准混合形态)。
	// 专属 Fleet 的 env 烤死目标 umap,Pod 预热时就已加载好目标图 → 分配即可玩,零 travel 延迟。
	// 分配时生成有序 selectors:[专属 Fleet, 通用 FleetName],Agones 按顺序尝试——
	// 专属池有空闲用专属,没有自动回落通用 Loader 池(同一次 allocation 调用,无额外 RTT)。
	// 未配置(默认)= 全部走通用池,行为不变。
	MapFleets []AgonesMapFleet `yaml:"map_fleets,omitempty" json:"map_fleets,omitempty"`

	// AdvertiseHost 覆盖返回给客户端连接的 host;留空则使用 Agones status.address。
	// 本机 minikube docker-driver 联调时常设为 127.0.0.1,配合 UDP relay。
	AdvertiseHost string `yaml:"advertise_host,omitempty" json:"advertise_host,omitempty"`

	// TokenPath ServiceAccount bearer token 文件路径
	// (默认 /var/run/secrets/kubernetes.io/serviceaccount/token;留 "-" 显式禁用 token)。
	TokenPath string `yaml:"token_path,omitempty" json:"token_path,omitempty"`

	// CAPath apiserver CA 证书路径
	// (默认 /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)。
	CAPath string `yaml:"ca_path,omitempty" json:"ca_path,omitempty"`

	// InsecureSkipTLSVerify 跳过 apiserver TLS 校验(仅 dev,生产禁用)。
	InsecureSkipTLSVerify bool `yaml:"insecure_skip_tls_verify,omitempty" json:"insecure_skip_tls_verify,omitempty"`

	// AllocateTimeout 单次 allocate / release REST 调用超时(默认 5s)。
	AllocateTimeout config.Duration `yaml:"allocate_timeout,omitempty" json:"allocate_timeout,omitempty"`

	// CapacityWatchInterval Fleet 容量巡检间隔(默认 30s;设负值禁用巡检)。
	// 巡检定期 GET 通用 Fleet + 各 map_fleets 专属 Fleet 的 status(replicas/ready/allocated),
	// 暴露 pandora_ds_allocator_fleet_* 指标,并在接近上限时打预警日志(见 CapacityWarnRatio)。
	CapacityWatchInterval config.Duration `yaml:"capacity_watch_interval,omitempty" json:"capacity_watch_interval,omitempty"`

	// CapacityWarnRatio 容量预警阈值,取值 (0,1](默认 0.8)。
	// allocated/replicas ≥ 此比例 → Warn 日志 ds_fleet_capacity_near_limit;
	// ready==0(完全打满/无可分配)→ Error 日志 ds_fleet_capacity_exhausted。
	CapacityWarnRatio float64 `yaml:"capacity_warn_ratio,omitempty" json:"capacity_warn_ratio,omitempty"`
}

// AgonesMapFleet 是 map_id → 专属预热 Fleet 的一条路由。
type AgonesMapFleet struct {
	// MapID 对齐 g_关卡.xlsx 的关卡 id。
	MapID uint32 `yaml:"map_id" json:"map_id"`
	// FleetName 该副本的专属 Fleet 名(其 env 烤死对应 umap + 战斗 GameMode)。
	FleetName string `yaml:"fleet_name" json:"fleet_name"`
	// CanaryFleetName 是同 map 的 canary 专属预热池；留空时 canary 直接走通用 canary 池。
	CanaryFleetName string `yaml:"canary_fleet_name,omitempty" json:"canary_fleet_name,omitempty"`
}

// DedicatedFleetFor 返回 mapID 的专属 Fleet 名;未配置返空串(= 只走通用池)。
func (c AgonesConf) DedicatedFleetFor(mapID uint32) string {
	if mapID == 0 {
		return ""
	}
	for _, mf := range c.MapFleets {
		if mf.MapID == mapID && mf.FleetName != "" {
			return mf.FleetName
		}
	}
	return ""
}

// DedicatedFleetForTrack 返回指定轨道的 map 专属 Fleet。
func (c AgonesConf) DedicatedFleetForTrack(mapID uint32, releaseTrack string) string {
	if mapID == 0 {
		return ""
	}
	for _, mf := range c.MapFleets {
		if mf.MapID != mapID {
			continue
		}
		if releaseTrack == "canary" {
			return mf.CanaryFleetName
		}
		return mf.FleetName
	}
	return ""
}

// AllocatorConf 是 ds_allocator 服务私有配置。
type AllocatorConf struct {
	// AllocationAbortAuth verifies only the payload-bound Matchmaker
	// pre-admission abort RPC. The caller and verifier share this dedicated key;
	// no player, placement, resume, or DS callback flow may receive it.
	AllocationAbortAuthSecret   string `yaml:"allocation_abort_auth_secret,omitempty" json:"allocation_abort_auth_secret,omitempty"`
	AllocationAbortAuthAudience string `yaml:"allocation_abort_auth_audience,omitempty" json:"allocation_abort_auth_audience,omitempty"`

	// HeartbeatTimeout DS 心跳超时阈值(默认 15s,不变量 §4)。
	// 超过此时长没收到 Heartbeat → 标记 abandoned + 释放(W4 ② 仅释放,补偿留 W4 ③)。
	HeartbeatTimeout config.Duration `yaml:"heartbeat_timeout,omitempty" json:"heartbeat_timeout,omitempty"`

	// ActivationStabilityBeats 首次激活(staged→ACTIVE)所需的最少**实收**业务心跳次数
	// (默认 3);ActivationStabilitySpan 是这些心跳的最小首尾跨度(默认 10s = 2 个完整
	// 5s 心跳周期)。推导(INC-20260727-001 第三 P0):DS 在 PostLoadMapWithWorld 回调内
	// 发出的首拍只能证明"回调这一刻游戏线程活着"——实测 Artic01 48s 冷加载后首拍即
	// 激活并放行 ds_addr,回调后游戏线程继续阻塞,17s 后被 ACTIVE 15s 阈值判弃,客户端
	// 连上一个不回包的 DS。跨 ≥2 个完整心跳周期的 ≥3 次实收心跳证明游戏线程持续 pump
	// TimerManager(每一拍都是一次真实穿越)。证据不足期间 battle 保持 warming(120s
	// 分配宽限兜底,pending 心跳不续命)、不返回 ds_addr、不发 ACK;ACTIVE 15s 崩溃
	// 补偿阈值不变。beats≤1 且 span≤0 时门关闭(仅供测试/回退)。
	ActivationStabilityBeats int             `yaml:"activation_stability_beats,omitempty" json:"activation_stability_beats,omitempty"`
	ActivationStabilitySpan  config.Duration `yaml:"activation_stability_span,omitempty" json:"activation_stability_span,omitempty"`

	// OwnerAddr owner 权威服务地址(owner-authority.md migrate ⑥)。
	// 空 = 不双写实例租约(未启用,现网行为不变,安全默认)。
	OwnerAddr string `yaml:"owner_addr,omitempty" json:"owner_addr,omitempty"`

	// OwnerLeaseRequired 实例租约双写失败是否令授权心跳失败。
	// 默认 false = migrate 弱依赖(失败只告警,旧 last_heartbeat_ms 再入门双门并行兜底);
	// contract 阶段全链验证后置 true 转强依赖(续租失败心跳必须失败 → DS 自我 fencing,
	// 权威侧租约滞后时 DS 必然停玩,屏障时序闭合)。
	OwnerLeaseRequired bool `yaml:"owner_lease_required,omitempty" json:"owner_lease_required,omitempty"`

	// SweepInterval 心跳超时扫描间隔(默认 5s)。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// WriterLeaseMode 心跳扫描循环的写者继任租约档位(留空 = enforce,与 hub_allocator 同一档位语义)。
	//
	// 为什么 ds_allocator 需要它(2026-07-29 事故闭环):本服务此前是 replicas=1 + Recreate,
	// 且整个进程被 dsauthfence capability 门控、失租即 os.Exit(1)。于是**任何**重启(崩溃、
	// 失租、例行换镜像)都会让 DSAllocatorService/Heartbeat 整体不可用;而 Battle DS 在
	// placement.DSFenceLeaseMaxSeconds=20s 内拿不到凭据绑定 ACK 就会自我 fencing 踢掉全部
	// 在场玩家。实测一次失租到重新 Ready 耗时 160s ≈ 8× 租约,§16.8「重启预算必须闭合」不成立,
	// 底线 7「升级不得打断对局」直接被破。
	//
	// 解法是把可用性交给副本数,而不是放宽 DS 的 fencing:
	//   · capability key 按 (service, PodUID) 唯一(pkg/dsauthfence 注释),异 Pod 副本各持异 key,
	//     故多副本天然共存,单副本失租退出不再等于整服无 allocator;
	//   · 唯一「同一未分区权威的单写者循环」是 RunHeartbeatSweep,由本档位挂 etcd 选举;
	//   · 其余写路径(AllocateBattle / Heartbeat / Release / abort / GM)全部按 match_id 分区并在
	//     Redis 事务内按精确凭据身份 CAS(data/battle_auth.go ActivateHeartbeat),属 §9.21 明说的
	//     「可并行 worker + 幂等 CAS」,不得为金丝雀强行全局串行化,故一律不 gate。
	//
	// 档位:
	//	enforce:竞选,只有当选副本跑 sweepOnce(稳态;RollingUpdate 必须用它);
	//	warmup :只竞选、只观测 token 单调与失主重选,不改扫描行为(首次引导升级的第一跳);
	//	off    :不启动租约(仅限单副本 Recreate 的历史部署)。
	// 取值非法时启动 fail-fast(安全开关不允许静默变形)。
	WriterLeaseMode string `yaml:"writer_lease_mode,omitempty" json:"writer_lease_mode,omitempty"`

	// BattleTTL 战斗 DS 镜像 Redis key 的 TTL(默认 2h,防僵尸镜像)。
	BattleTTL config.Duration `yaml:"battle_ttl,omitempty" json:"battle_ttl,omitempty"`

	// ReadyWaitTimeout AllocateBattle 等待战斗 DS 用 Heartbeat 上报 ready 的最长时间(默认 10s)。
	// Agones Allocated 只说明 pod 被分配,不代表 DS 进程已读到 pandora.dev/match-id;必须等
	// DS 用正确 match_id/pod 的心跳确认 ready/running,后端才把 ds_addr 回给 matchmaker(否则
	// 客户端太快连接时 DS 内部 match_id 仍为 0,PreLogin 会拒票)。超时则回收 pod + 删镜像 + 分配失败。
	ReadyWaitTimeout config.Duration `yaml:"ready_wait_timeout,omitempty" json:"ready_wait_timeout,omitempty"`

	// EmptyBattleTimeout 空场超时(默认 5m):对局活跃(ready/running)但 DS 上报 player_count==0
	// 持续超过此时长 → 后端兜底判 abandoned(全员掉线未归 / 客户端从未连入,DS 空转烧资源)。
	// 主路径是 DS 侧空场计时器自结算 + Shutdown(agones-dev.md §2.4),此阈值应大于 DS 侧计时器,
	// 且必须远大于战斗断线重连窗口(~30s,battle-reconnect.md),避免误杀「全员短暂掉线正在重连」的局。
	// 设为负值禁用(0 = 用默认 5m)。
	//
	// ⚠️ 本值**只管「有人连进来过」的局**(BattleStorageRecord.ever_had_players=true);
	// 「从未连入」的局走下面更短的 NoShowBattleTimeout。
	EmptyBattleTimeout config.Duration `yaml:"empty_battle_timeout,omitempty" json:"empty_battle_timeout,omitempty"`

	// NoShowBattleTimeout 从未连入超时(默认 150s):对局已 ready/running 但**从头到尾没有任何
	// 玩家连入过**(ever_had_players=false)持续超过此时长 → 判 abandoned。
	//
	// 为什么必须与 EmptyBattleTimeout 分开(docs/design/anti-abuse-scene-entry.md §3.2.1):
	// 两者安全下界完全不同。EmptyBattleTimeout 必须远大于断线重连窗(~30s),让掉线的人回得来;
	// 而 no-show 局里**一个玩家都没有**,没有"谁要回来"的问题,只需覆盖「DS 报 ready →
	// 客户端 travel + 连接 + Admission」。用同一个 5m 值的后果:每次分配都白押一台 14Gi Pod
	// 满 5 分钟,刷进出副本的外挂能用 maxReplicas 个小号把整个 Fleet 押死(正常玩家进不去,
	// 本身即违反 §9.20);正常玩家强退后也会被 locator BATTLE 锁满 5 分钟。
	//
	// 默认值推导:DSTicket v2 生产档 TTL 120s(pkg/auth/dsticket.go)+ 30s 时钟/网络余量。
	// 票据是进场权威的唯一搬运通道(§9.3),票据过期后该客户端不可能再凭它进来 ⇒ 150s 后
	// "没人连入"这件事已不可逆,回收是安全的,不是拍脑袋的经验值。
	// ⚠️ 仍需实测「DS 报 ready → 首次 player_count>0」的 P99 复核本值。
	//
	// 爆炸半径:no-show 只在**全员缺席**时成立;只要有一个玩家连入过就永久切到
	// EmptyBattleTimeout 长阈值。因此 5v5 里"9 个人在打、1 个没连上"绝不会走这条。
	//
	// 取值:0 = 用默认 150s;负值 = 显式禁用差异化(退化为单阈值,与改动前行为一致);
	// 正值按 [NoShowTimeoutFloor, EmptyBattleTimeout] 钳制(下限防手滑配出"玩家进不去",
	// 上限保证 no-show 不会比普通空场还晚回收)。
	NoShowBattleTimeout config.Duration `yaml:"no_show_battle_timeout,omitempty" json:"no_show_battle_timeout,omitempty"`

	// OrphanGsReclaimAfter 孤儿 Allocated GameServer(处于 Allocated 却无任何权威分配
	// 记录引用)连续观察超过该时长后,由 sweep 的对账清扫按 UID+resourceVersion
	// precondition 精确回收(biz/orphan_gameserver.go;2026-08-03)。
	// 默认 10m,下限 5m(biz 侧钳制);推导:阈值覆盖 DSTicket 硬上限 180s +
	// ready_wait 120s + 时钟余量,保证"无记录 ⇒ 不可能再有玩家进来"。
	// ⚠️ "不可能已在内"不由阈值保证,靠的是心跳停机契约(无记录心跳 → commandStop /
	// DS 失联自我 fencing,§9.22)——完整判定链见 biz/orphan_gameserver.go 文件头。
	// 0/负值 = 用默认;仅 Agones 分配器生效(local/mock 无 Allocated 概念)。
	OrphanGsReclaimAfter config.Duration `yaml:"orphan_gs_reclaim_after,omitempty" json:"orphan_gs_reclaim_after,omitempty"`

	// MockDSAddrHost W4 ② MockGameServerAllocator 返回的假 DS host(默认 127.0.0.1)。
	// W4 ③ 接 Agones 后此字段废弃,addr 由 GameServerAllocation status 返回。
	MockDSAddrHost string `yaml:"mock_ds_addr_host,omitempty" json:"mock_ds_addr_host,omitempty"`

	// MockDSPortBase W4 ② MockGameServerAllocator 端口基址(默认 30000)。
	// 每场 match 端口 = MockDSPortBase + (match_id % MockDSPortRange)。
	MockDSPortBase int `yaml:"mock_ds_port_base,omitempty" json:"mock_ds_port_base,omitempty"`

	// MockDSPortRange Mock 端口取模范围(默认 1000)。
	MockDSPortRange int `yaml:"mock_ds_port_range,omitempty" json:"mock_ds_port_range,omitempty"`
}

// 写者继任租约档位取值(AllocatorConf.WriterLeaseMode);语义与 hub_allocator 同名常量一致。
const (
	WriterLeaseEnforce = "enforce"
	WriterLeaseWarmup  = "warmup"
	WriterLeaseOff     = "off"
)

// ResolveWriterLeaseMode 归一化并校验 writer_lease_mode(空 → enforce)。
// 非法值返回 error,由 main fail-fast:安全档位配错必须炸,不能静默退化成 off。
func (c AllocatorConf) ResolveWriterLeaseMode() (string, error) {
	switch strings.ToLower(strings.TrimSpace(c.WriterLeaseMode)) {
	case "", WriterLeaseEnforce:
		return WriterLeaseEnforce, nil
	case WriterLeaseWarmup:
		return WriterLeaseWarmup, nil
	case WriterLeaseOff:
		return WriterLeaseOff, nil
	default:
		return "", fmt.Errorf("allocator.writer_lease_mode %q invalid (want enforce|warmup|off)", c.WriterLeaseMode)
	}
}

// 空场回收双阈值常量(AllocatorConf.NoShowBattleTimeout)。
const (
	// DefaultNoShowBattleTimeout「从未连入」局的默认回收阈值。
	// 推导:DSTicket v2 生产档 TTL 120s(pkg/auth/dsticket.go)+ 30s 时钟/网络余量。
	// 票据是进场权威的唯一搬运通道(§9.3),过期后客户端不可能再凭它连入。
	DefaultNoShowBattleTimeout = 150 * time.Second

	// NoShowTimeoutFloor no-show 阈值下限护栏。配得比这更短会开始误杀「正在加载地图 /
	// 正在 travel」的正常玩家,把防刷改动变成"玩家进不去场景"(§9.20 红线)。
	// 手滑配 1s 必须被钳住并留日志,而不是静默生效。
	NoShowTimeoutFloor = 60 * time.Second
)

// ResolveNoShowTimeout 返回「从未连入」(ever_had_players=false)局实际生效的空场回收阈值。
//
// 语义(见 NoShowBattleTimeout 字段注释):
//   - EmptyBattleTimeout <= 0(整体禁用/未配):跟随它,不做差异化;
//   - NoShowBattleTimeout < 0:显式禁用差异化 → 退回 EmptyBattleTimeout(改动前的单阈值行为);
//   - == 0:用 DefaultNoShowBattleTimeout;
//   - > 0:按 [NoShowTimeoutFloor, EmptyBattleTimeout] 钳制。
//
// 返回值直接就是可用阈值,调用方无需再判分支:no-show 用本函数,其余用 EmptyBattleTimeout。
func (c AllocatorConf) ResolveNoShowTimeout() time.Duration {
	empty := c.EmptyBattleTimeout.Std()
	if empty <= 0 {
		return empty
	}
	noShow := c.NoShowBattleTimeout.Std()
	switch {
	case noShow < 0:
		return empty
	case noShow == 0:
		noShow = DefaultNoShowBattleTimeout
	}
	if noShow < NoShowTimeoutFloor {
		noShow = NoShowTimeoutFloor
	}
	if noShow > empty {
		noShow = empty
	}
	return noShow
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	// Mode 归一化:显式 mode 优先;留空时按 legacy 的 enabled 开关推导(向后兼容)。
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		switch {
		case c.Agones.Enabled:
			c.Mode = ModeAgones
		case c.LocalDS.Enabled:
			c.Mode = ModeLocal
		default:
			c.Mode = ModeMock
		}
	}
	// Launcher 两模式开关:缺省/非法值一律归一到 packaged(现状行为),旧配置零改动。
	// PANDORA_DS_LAUNCHER / PANDORA_DS_UPROJECT 让一键脚本免改 yaml 就能切换(与
	// PANDORA_DS_EXE 同一套注入机制);非空时优先级高于 yaml 写死值。
	// 归一化必须早于下面的 Allocator 超时默认值 —— editor 形态启动更慢,超时要按它放宽。
	if envLauncher := strings.TrimSpace(os.Getenv("PANDORA_DS_LAUNCHER")); envLauncher != "" {
		c.LocalDS.Launcher = envLauncher
	}
	if c.LocalDS.Launcher = strings.ToLower(strings.TrimSpace(c.LocalDS.Launcher)); c.LocalDS.Launcher != LauncherEditor {
		c.LocalDS.Launcher = LauncherPackaged
	}
	c.LocalDS.ProjectPath = filepath.FromSlash(os.ExpandEnv(c.LocalDS.ProjectPath))
	if envProj := strings.TrimSpace(os.Getenv("PANDORA_DS_UPROJECT")); envProj != "" {
		if _, err := os.Stat(c.LocalDS.ProjectPath); c.LocalDS.ProjectPath == "" || err != nil {
			c.LocalDS.ProjectPath = filepath.FromSlash(envProj)
		}
	}
	// editor 形态(UnrealEditor.exe -server)启动显著慢于打包 DS:要加载一大批编辑器模块、
	// 读未 cook 的散装资产(比 pak 慢),首次进一张新图还可能现场构 DDC(网格/贴图的
	// render data),可达数分钟。沿用打包 DS 的 10s/15s 会让 AllocateBattle 在 DS 还没起来时
	// 就判 ready 超时回收,editor 模式永远开不了局。
	// 注:并**不包括编 shader** —— -server 下 IsRunningDedicatedServer()==true,引擎会跳过全局
	// 着色器(RenderCore/ShaderCore.cpp AllowGlobalShaderLoad)与材质着色器(Material.cpp PostLoad
	// 的 FApp::CanEverRender() 守卫);要编 shader 的是 listen server / PIE 那类会出画面的形态。
	// 因此仅在「用户没显式配置(==0)」且 launcher=editor 时放宽;显式配置永远优先。
	// 只影响本机 local 调试路径,Agones/线上默认值一字不动。
	editorLocal := c.Mode == ModeLocal && c.LocalDS.Launcher == LauncherEditor
	if c.Allocator.HeartbeatTimeout == 0 && editorLocal {
		c.Allocator.HeartbeatTimeout = config.Duration(120 * time.Second)
	}
	if c.Allocator.ReadyWaitTimeout == 0 && editorLocal {
		c.Allocator.ReadyWaitTimeout = config.Duration(300 * time.Second)
	}
	if c.Allocator.HeartbeatTimeout == 0 {
		c.Allocator.HeartbeatTimeout = config.Duration(15 * time.Second)
	}
	if c.Allocator.ActivationStabilityBeats == 0 {
		c.Allocator.ActivationStabilityBeats = 3
	}
	if c.Allocator.ActivationStabilitySpan == 0 {
		c.Allocator.ActivationStabilitySpan = config.Duration(10 * time.Second)
	}
	if c.Allocator.SweepInterval == 0 {
		c.Allocator.SweepInterval = config.Duration(5 * time.Second)
	}
	if c.Allocator.BattleTTL == 0 {
		c.Allocator.BattleTTL = config.Duration(2 * time.Hour)
	}
	if c.Allocator.ReadyWaitTimeout == 0 {
		c.Allocator.ReadyWaitTimeout = config.Duration(10 * time.Second)
	}
	if c.Allocator.EmptyBattleTimeout == 0 {
		c.Allocator.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	}
	if c.Allocator.MockDSAddrHost == "" {
		c.Allocator.MockDSAddrHost = "127.0.0.1"
	}
	if c.Allocator.MockDSPortBase == 0 {
		c.Allocator.MockDSPortBase = 30000
	}
	if c.Allocator.MockDSPortRange == 0 {
		c.Allocator.MockDSPortRange = 1000
	}
	if c.Agones.APIServer == "" {
		c.Agones.APIServer = "https://kubernetes.default.svc"
	}
	if c.Agones.Namespace == "" {
		c.Agones.Namespace = "default"
	}
	if c.Agones.TokenPath == "" {
		c.Agones.TokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if c.Agones.CAPath == "" {
		c.Agones.CAPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if c.Agones.AllocateTimeout == 0 {
		c.Agones.AllocateTimeout = config.Duration(5 * time.Second)
	}
	if c.Agones.CapacityWatchInterval == 0 {
		c.Agones.CapacityWatchInterval = config.Duration(30 * time.Second)
	}
	if c.Agones.CapacityWarnRatio <= 0 || c.Agones.CapacityWarnRatio > 1 {
		c.Agones.CapacityWarnRatio = 0.8
	}
	c.DSAuth.Defaults()
	// 路径字段支持环境变量展开 + 跨机器兜底,便于策划机移植(Client 目录可能不在配置写死的盘符):
	//  1. 先做 ${VAR}/$VAR 展开(绝对路径不含 $,dev 配置原样保留);
	//  2. filepath.FromSlash 归一化分隔符:策划在 yaml 里写正斜杠 / (无需 \\ 转义)也能在 Windows 正常工作;
	//  3. 展开后的路径在本机不存在时,回退到启动脚本按平级 Client 目录探测注入的
	//     PANDORA_DS_EXE / PANDORA_DS_DIR(play.ps1 自动填充);dev 上 F:\ 路径存在则不覆盖。
	c.LocalDS.ExecutablePath = filepath.FromSlash(os.ExpandEnv(c.LocalDS.ExecutablePath))
	c.LocalDS.WorkingDir = filepath.FromSlash(os.ExpandEnv(c.LocalDS.WorkingDir))
	if envExe := os.Getenv("PANDORA_DS_EXE"); envExe != "" {
		if _, err := os.Stat(c.LocalDS.ExecutablePath); c.LocalDS.ExecutablePath == "" || err != nil {
			c.LocalDS.ExecutablePath = filepath.FromSlash(envExe)
			if envDir := os.Getenv("PANDORA_DS_DIR"); envDir != "" {
				c.LocalDS.WorkingDir = filepath.FromSlash(envDir)
			}
		}
	}
	// AdvertiseHost 是「返回给客户端连接的 host」,属每台机器各异的运行期值:内网测试服要用
	// 局域网 IP(远程策划客户端才连得到战斗 DS),本机自测用 127.0.0.1。启动脚本(play.ps1 -Battle
	// -Intranet)自动探测本机内网 IPv4 并经 PANDORA_DS_ADVERTISE_HOST 注入,优先级高于 yaml 写死值,
	// 无需改仓库配置。留空时回退 yaml 值 / 127.0.0.1。
	if envHost := strings.TrimSpace(os.Getenv("PANDORA_DS_ADVERTISE_HOST")); envHost != "" {
		c.LocalDS.AdvertiseHost = envHost
	}
	if c.LocalDS.AdvertiseHost == "" {
		c.LocalDS.AdvertiseHost = "127.0.0.1"
	}
	if c.LocalDS.PortBase == 0 {
		c.LocalDS.PortBase = 7777
	}
	if c.LocalDS.PortRange == 0 {
		c.LocalDS.PortRange = 100
	}
	if c.LocalDS.LogDir == "" {
		c.LocalDS.LogDir = "run/dev/logs/ds"
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20020"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21020"
	}
}
