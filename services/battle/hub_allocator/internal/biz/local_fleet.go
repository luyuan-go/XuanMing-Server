// local_fleet.go — 本机 exec 常驻 Windows Hub DS 的 HubFleetProvider(mode=local)。
//
// 与 ds_allocator/data/local_allocator.go 对称,是 Windows 单机自测时大厅 DS 的来源:
// 首次 ListShards 时懒拉起「一个」常驻 Hub DS 进程(加载 hub 关卡 / PandoraHubGameMode),
// 把它作为该 region 唯一的 ShardCandidate 返回;进程随 hub_allocator 退出由 Close Kill。
//
// 与战斗 DS 的差异:Hub DS 是常驻分片(不按对局回收),所以这里只起一个进程并长期持有,
// 不做端口池 / 多实例 / reaper 回收。topology-only:不实现 HubFleetScaler,故本机模式下
// autoscale/consolidation 不会运行(与 Mock 同语义)。
package biz

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
)

// LocalHubFleetProvider 在本机 exec 一个常驻 UE Windows Hub Dedicated Server 进程。
type LocalHubFleetProvider struct {
	cfg           conf.LocalHubConf
	podName       string
	instanceUID   string
	protocolEpoch uint32
	addr          string
	mapURL        string

	// dsTokenIssuer 签发 DS 回调服务令牌(审核 P1 #1;main 在 ds_auth.secret 已配时、
	// 懒拉起前注入)。local 模式经 PANDORA_DS_TOKEN env 一次性下发(进程常驻无法改 env,
	// 不支持续期;dev 自测会话远短于 hub_token_ttl 默认 24h,够用)。签发失败只告警不阻断拉起。
	dsTokenIssuer func(pod, instanceUID string, protocolEpoch uint32) (token string, expiresAtMs int64, gen uint64, err error)
	// dsTokenRequired=true 时签发失败则不拉起 Hub DS。local-off-v1 虽不做服务端 Guard，
	// UE 仍强制完整凭据，故 main 也必须把它设为 true，防止启动“能连但所有回调都发不出”的半成品。
	dsTokenRequired bool
	// tokenGen 是本机 Hub DS 当前令牌的代际(拉起时经签发器 Redis INCR 领取,经 env 一次性下发)。
	// local 不续期 → gen 全程不变,心跳回显同一 gen 与分片记录精确相等(审核 P1-6)。
	// once.Do 建立 happens-before:ensureStarted 完成后 ListShards 方读,无需额外锁。
	tokenGen uint64

	once sync.Once // 懒拉起只执行一次
	mu   sync.Mutex
	cmd  *exec.Cmd
	logF *os.File
	// startErr 记录 ensureStarted 的启动失败(enforce 下 DS 令牌签发失败即在此);
	// ListShards 据此在 enforce 时拒绝返回候选(fail-closed)。受 mu 保护。
	startErr error
}

// SetDSTokenIssuer 注入 DS 回调令牌签发器(可选依赖;须在首次 ListShards 前调用)。
// required=true 时签发失败则懒拉起失败(fail-closed)。
func (l *LocalHubFleetProvider) SetDSTokenIssuer(f func(pod, instanceUID string, protocolEpoch uint32) (string, int64, uint64, error), required bool) {
	l.dsTokenIssuer = f
	l.dsTokenRequired = required
}

// NewLocalHubFleetProvider 构造本机 Hub DS 拉起器。
//
// 失败场景(返 error,main 据此 fatal):
//   - ExecutablePath 空
//   - ExecutablePath 指向的文件不存在
//   - launcher=editor 但 ProjectPath 空 / 指向的 .uproject 不存在
func NewLocalHubFleetProvider(cfg conf.LocalHubConf) (*LocalHubFleetProvider, error) {
	if cfg.ExecutablePath == "" {
		return nil, fmt.Errorf("local_hub: executable_path required when mode=local")
	}
	if _, err := os.Stat(cfg.ExecutablePath); err != nil {
		return nil, fmt.Errorf("local_hub: executable_path %q not found: %w", cfg.ExecutablePath, err)
	}
	// editor 形态必须能定位 .uproject:否则 UnrealEditor.exe 会把关卡路径当工程名解析失败,
	// 表现为 Hub DS 秒退、客户端登录后连不上大厅 —— 在启动时 fail-fast。
	if cfg.Launcher == conf.LauncherEditor {
		if cfg.ProjectPath == "" {
			return nil, fmt.Errorf("local_hub: project_path required when launcher=%s", conf.LauncherEditor)
		}
		if _, err := os.Stat(cfg.ProjectPath); err != nil {
			return nil, fmt.Errorf("local_hub: project_path %q not found: %w", cfg.ProjectPath, err)
		}
	}
	mapURL, err := hubMapURLWithMaxPlayers(cfg.MapName, cfg.Capacity)
	if err != nil {
		return nil, fmt.Errorf("local_hub: %w", err)
	}
	return &LocalHubFleetProvider{
		cfg: cfg,
		// 每次进程启动生成唯一实例名(对齐线上 Agones「GameServer 名每次唯一」语义):
		// 旧进程被杀后残留的 Redis 分片记录会因名字不再匹配而成为「不在 Fleet live 集」的孤儿,
		// 被 reconcileShardTopology 清理;新进程用新名建一条全新 ready 记录,不复用旧的 draining 状态。
		// 与 HeartbeatShard 存活复活是双保险:UUID 治「身份复用」,复活治「活 pod 被误判超时」。
		podName:       "pandora-hub-local-" + uuid.NewString()[:8],
		instanceUID:   uuid.NewString(),
		protocolEpoch: 1,
		addr:          fmt.Sprintf("%s:%d", cfg.AdvertiseHost, cfg.Port),
		mapURL:        mapURL,
	}, nil
}

// ListShards 返回本机唯一的 Hub 分片(并在首次调用时懒拉起常驻 Hub DS 进程)。
func (l *LocalHubFleetProvider) ListShards(_ context.Context, region string) ([]ShardCandidate, error) {
	l.ensureStarted()
	// required(dsTokenRequired):启动失败(含 DS 回调令牌签发失败)则不返回候选分片,
	// 否则 ensureShards 会据此在 Redis 种一条 ready 记录、把客户端路由到一个未拉起 /
	// 回调必被 enforce 守卫全拒的 Hub(fail-closed,对齐 agones 路径的「签发失败跳过候选」)。
	if l.dsTokenRequired {
		l.mu.Lock()
		serr := l.startErr
		l.mu.Unlock()
		if serr != nil {
			return nil, errcode.New(errcode.ErrHubNoAvailable,
				"local hub ds not started because required credential/start failed: %v", serr)
		}
	}
	if region == "" {
		region = l.cfg.Region
	}
	return []ShardCandidate{{
		PodName:      l.podName,
		Addr:         l.addr,
		Region:       region,
		ShardID:      1,
		Capacity:     l.cfg.Capacity,
		ReleaseTrack: releasetrack.Stable,
		TokenReady:   true,
		TokenGen:     l.tokenGen,
	}}, nil
}

// ensureStarted 懒拉起常驻 Hub DS 进程(仅一次)。
// off/permissive 下拉起失败只记日志:客户端仍会拿到分片地址,连接失败时由其自身重试/报错,
// 便于排查 DS 启动问题。enforce 下把错误记入 startErr,ListShards 据此 fail-closed 不返回候选。
func (l *LocalHubFleetProvider) ensureStarted() {
	l.once.Do(func() {
		if err := l.start(); err != nil {
			l.mu.Lock()
			l.startErr = err
			l.mu.Unlock()
			plog.With(context.Background()).Errorw("msg", "local_hub_ds_start_failed",
				"err", err, "executable", l.cfg.ExecutablePath, "addr", l.addr,
				"enforce", l.dsTokenRequired,
				"hint", "检查 local_hub.executable_path / map_name;enforce 下将 fail-closed 不返回候选")
			return
		}
		plog.With(context.Background()).Infow("msg", "local_hub_ds_started",
			"pod", l.podName, "addr", l.addr, "map", l.cfg.MapName)
	})
}

// start 真正 exec UE Windows Hub DS 并把 stdout/stderr 落盘。
func (l *LocalHubFleetProvider) start() error {
	cmd := exec.Command(l.cfg.ExecutablePath, l.buildArgs()...) //nolint:gosec // 路径来自受信本机配置
	if l.cfg.WorkingDir != "" {
		cmd.Dir = l.cfg.WorkingDir
	}
	env, eerr := l.buildEnv()
	if eerr != nil {
		return eerr
	}
	cmd.Env = env

	var logF *os.File
	if l.cfg.LogDir != "" {
		if err := os.MkdirAll(l.cfg.LogDir, 0o755); err == nil {
			if f, ferr := os.Create(filepath.Join(l.cfg.LogDir, l.podName+".log")); ferr == nil {
				logF = f
				cmd.Stdout = f
				cmd.Stderr = f
			}
		}
	}

	if err := cmd.Start(); err != nil {
		if logF != nil {
			_ = logF.Close()
		}
		return err
	}

	l.mu.Lock()
	l.cmd = cmd
	l.logF = logF
	l.mu.Unlock()
	return nil
}

// buildArgs 拼 UE DS 命令行:[.uproject] + 大厅关卡 + -server -log -port=<port> + 额外参数。
//
// launcher=editor 时在最前面插 .uproject:UE 的 LaunchSetGameName 只把命令行里**第一个**不以 '-'
// 开头的 token 当工程/关卡,所以 .uproject 必须排在关卡 URL 之前。-server 两种形态都带 ——
// NetMode 恒为 NM_DedicatedServer,大厅心跳/SetLocationHub/在线准入与打包 DS 完全一致。
func (l *LocalHubFleetProvider) buildArgs() []string {
	args := make([]string, 0, 5+len(l.cfg.ExtraArgs))
	if l.cfg.Launcher == conf.LauncherEditor && l.cfg.ProjectPath != "" {
		args = append(args, l.cfg.ProjectPath)
	}
	if l.mapURL != "" {
		args = append(args, l.mapURL)
	}
	args = append(args, "-server", "-log", fmt.Sprintf("-port=%d", l.cfg.Port))
	args = append(args, l.cfg.ExtraArgs...)
	return args
}

func hubMapURLWithMaxPlayers(mapName string, capacity int32) (string, error) {
	if capacity <= 0 {
		return "", fmt.Errorf("capacity must be positive")
	}
	parts := strings.Split(mapName, "?")
	found := false
	for _, option := range parts[1:] {
		keyValue := strings.SplitN(option, "=", 2)
		if !strings.EqualFold(strings.TrimSpace(keyValue[0]), "MaxPlayers") {
			continue
		}
		if found || len(keyValue) != 2 {
			return "", fmt.Errorf("map_name contains duplicate/invalid MaxPlayers option")
		}
		parsed, err := strconv.ParseInt(keyValue[1], 10, 32)
		canonical := strconv.FormatInt(int64(capacity), 10)
		if err != nil || parsed != int64(capacity) || keyValue[1] != canonical {
			return "", fmt.Errorf("map_name MaxPlayers must exactly equal capacity %d", capacity)
		}
		found = true
	}
	if found {
		return mapName, nil
	}
	return fmt.Sprintf("%s?MaxPlayers=%d", mapName, capacity), nil
}

// buildEnv 注入 Hub DS 身份变量(对齐 UE DS 侧读取的 env)。
func (l *LocalHubFleetProvider) buildEnv() ([]string, error) {
	env := os.Environ()
	env = append(env,
		"AGONES_GAMESERVER_NAME="+l.podName,
		"PANDORA_DS_TYPE=hub",
		"PANDORA_REGION="+l.cfg.Region,
		// local-off-v1 是 Windows 本机联调专用的机械隔离契约。UE 还会同时校验
		// 本地 pod 前缀与非 Agones 运行态，Linux/Agones 不会因误注入单一变量而降级。
		auth.DSLocalProfileEnv+"="+auth.DSLocalProfileOffV1,
	)
	// DS 回调服务令牌(审核 P1 #1):local 模式经 env 一次性下发(agones 模式走 annotation 可续期)。
	// required(dsTokenRequired):签发失败 fail-closed 不拉起；否则失败只告警照拉。
	if l.dsTokenIssuer != nil {
		if tok, _, gen, err := l.dsTokenIssuer(l.podName, l.instanceUID, l.protocolEpoch); err != nil {
			if l.dsTokenRequired {
				return nil, fmt.Errorf("required hub_ds_token sign failed for pod %s: %w", l.podName, err)
			}
			plog.With(context.Background()).Warnw("msg", "hub_ds_token_sign_failed", "pod", l.podName, "err", err)
		} else {
			l.tokenGen = gen // once.Do 内写,ListShards 在 ensureStarted 后读(happens-before)
			env = append(env, "PANDORA_DS_TOKEN="+tok)
		}
	}
	// extra_env 追加,但严禁覆盖内置身份/令牌变量(审核 P1:extra_env 覆盖 PANDORA_DS_TOKEN
	// 会用静态/伪造令牌替换真签发令牌,绕过范围绑定)。保留字命中即跳过并告警。
	for k, v := range l.cfg.ExtraEnv {
		if isReservedHubDSEnvKey(k) {
			plog.With(context.Background()).Warnw("msg", "extra_env_reserved_key_ignored", "key", k,
				"hint", "extra_env 不得覆盖 PANDORA_DS_TOKEN / AGONES_GAMESERVER_NAME / PANDORA_DS_TYPE 等内置变量")
			continue
		}
		env = append(env, k+"="+v)
	}
	return env, nil
}

// isReservedHubDSEnvKey 判断 env key 是否为 fleet 内置注入的身份/令牌变量,extra_env 不得覆盖。
// 大小写不敏感:Windows(local 模式 Hub DS 宿主)环境变量名大小写不敏感,小写别名(如
// `pandora_ds_token`)与内置大写名指向同一变量,精确比对会漏放行小写覆盖(审核 P1 补漏)。
func isReservedHubDSEnvKey(k string) bool {
	switch strings.ToUpper(strings.TrimSpace(k)) {
	case "PANDORA_DS_TOKEN", auth.DSLocalProfileEnv, "AGONES_GAMESERVER_NAME", "PANDORA_DS_TYPE", "PANDORA_REGION":
		return true
	default:
		return false
	}
}

// Close 终止常驻 Hub DS 进程(hub_allocator 退出时调用,避免遗留孤儿)。
func (l *LocalHubFleetProvider) Close() error {
	l.mu.Lock()
	cmd, logF := l.cmd, l.logF
	l.cmd, l.logF = nil, nil
	l.mu.Unlock()

	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	if logF != nil {
		_ = logF.Close()
	}
	return nil
}
