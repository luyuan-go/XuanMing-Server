// local_allocator.go — 本机拉起 Windows Dedicated Server 进程的调试用 GameServerAllocator。
//
// 这是与 AgonesGameServerAllocator(Linux 生产,见 agones_allocator.go)并列的第二种
// DS 启动方式,专供本机联调:匹配成局后 ds_allocator 直接 exec 打包好的 UE Windows DS,
// 分配一个本机端口,返回真实地址(host:port)给客户端 NetDriver;Release / 心跳超时
// abandoned 时 Kill 进程。三种方式共用 biz.GameServerAllocator 接口,biz 逻辑零改。
//
// 设计要点:
//   - 进程台账(podName → 进程 + 端口)在内存维护,带互斥锁;ds_allocator 退出时 Close 全杀。
//   - 每个 DS 进程一个 reaper goroutine Wait(),进程自行退出(崩溃)时清理台账释放端口
//     (镜像仍靠心跳超时 sweep 标 abandoned,与 Agones pod 崩溃同语义)。
//   - Allocate 幂等:同 podName(由 matchID 派生)已在台账则直接返回原地址,不重复拉进程。
//   - 启动函数 startProc 抽成字段,单测可注入假进程,避免真的 exec UE。
package data

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// dsProcess 抽象一个已拉起的 DS 进程,便于单测注入假实现。
type dsProcess interface {
	// Kill 终止进程(已退出则应为 no-op 不报致命错)。
	Kill() error
	// Wait 阻塞直到进程退出。
	Wait() error
}

// execProcess 是 dsProcess 的真实现,包一个 *exec.Cmd。
type execProcess struct {
	cmd  *exec.Cmd
	logF *os.File // 日志文件句柄,进程退出后关闭
}

func (e *execProcess) Kill() error {
	if e.cmd.Process == nil {
		return nil
	}
	// Windows:UE DS(PandoraServer.exe)可能派生子进程,只 kill 父进程会留下仍占监听端口的
	// 子进程(幽灵 DS),导致后续对局撞端口。taskkill /T 杀整棵进程树,/F 强制,确保端口真正释放。
	if runtime.GOOS == "windows" {
		kc := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(e.cmd.Process.Pid)) //nolint:gosec // pid 来自本进程派生的 DS
		if err := kc.Run(); err == nil {
			return nil
		}
		// taskkill 不可用/失败 → 回退直接 kill 父进程(至少不泄漏父进程)。
	}
	return e.cmd.Process.Kill()
}

func (e *execProcess) Wait() error {
	err := e.cmd.Wait()
	if e.logF != nil {
		_ = e.logF.Close()
	}
	return err
}

// launchedProc 是台账里的一条记录。
type launchedProc struct {
	proc dsProcess
	port int
	addr string
	// instanceUID / instanceEpoch 是本机 DS 的 exact 实例身份,与下发给该进程的
	// DS 回调令牌**同源**(Allocate 处一次生成,既签进令牌又留台账)。留存的意义在于
	// 幂等重分配必须返回同一身份:身份漂移会让票据绑到一个不存在的实例。
	instanceUID   string
	instanceEpoch uint32
}

// LocalGameServerAllocator 在本机 exec UE Windows Dedicated Server 进程。
type LocalGameServerAllocator struct {
	cfg conf.LocalDSConf

	mu        sync.Mutex
	procs     map[string]*launchedProc // podName → 进程记录
	usedPorts map[int]struct{}
	// pendingRosters:matchID → 待随 DS env 下发的权威准入元数据(见 SetPendingBattleRoster)。
	// 由 Allocate 在 buildEnv 时消费并删除;受 mu 保护。
	pendingRosters map[uint64]localBattleRoster

	// startProc 拉起一个 DS 进程;单测注入假实现绕过真 exec。
	// token 是本对局的 DS 回调令牌(Allocate 处一次性签发,避免二次签发失败只告警的空窗)。
	startProc func(podName string, port int, matchID uint64, mapID uint32, gameMode, token string) (dsProcess, error)

	// portProbe 探测端口在本机是否真的空闲(可绑定)。nil=不探测(单测默认放行)。
	// 用于挡住「台账已释放但进程未退出(幽灵 DS)」或外部程序占用的端口:否则 allocator 把
	// -port=X 传给 UE DS,X 被占时 UE 会静默 fallback 到 X+1,导致 allocator 记录/返回的端口(X)
	// 与 DS 实际监听端口(X+1)不一致,新对局客户端拿新 ticket 却连到 X 上的旧 DS,被 PreLogin 拒。
	portProbe func(port int) bool

	// dsTokenIssuer 签发 DS 回调服务令牌(审核 P1 #1;main 在 ds_auth.secret 已配时注入)。
	// 非 nil 时 defaultStart 把令牌注入 DS 进程 env PANDORA_DS_TOKEN,DS 回调时带 Bearer 头。
	// 签发失败只告警不阻断拉起(guard 默认 off/permissive,先保对局可开)。
	dsTokenIssuer func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (token string, err error)
	// dsTokenRequired 为 guard=enforce 时 true:签发失败则 fail-closed 不拉起 DS(否则该 DS
	// 回调会被 enforce 守卫全拒)。off/permissive 下 false,签发失败只告警照拉。
	dsTokenRequired bool
}

// SetDSTokenIssuer 注入 DS 回调令牌签发器(可选依赖,main 在 ds_auth.secret 已配时调用)。
// required=true(guard=enforce)时签发失败会让 Allocate 返回错误(fail-closed)。
func (l *LocalGameServerAllocator) SetDSTokenIssuer(f func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (string, error), required bool) {
	l.dsTokenIssuer = f
	l.dsTokenRequired = required
}

// NewLocalGameServerAllocator 构造本机 DS 拉起器。
//
// 失败场景(返 error,main 据此 fatal):
//   - ExecutablePath 空
//   - ExecutablePath 指向的文件不存在
//   - launcher=editor 但 ProjectPath 空 / 指向的 .uproject 不存在
//   - PortRange <= 0
func NewLocalGameServerAllocator(cfg conf.LocalDSConf) (*LocalGameServerAllocator, error) {
	if cfg.ExecutablePath == "" {
		return nil, fmt.Errorf("local_ds: executable_path required when enabled")
	}
	if _, err := os.Stat(cfg.ExecutablePath); err != nil {
		return nil, fmt.Errorf("local_ds: executable_path %q not found: %w", cfg.ExecutablePath, err)
	}
	// editor 形态必须能定位 .uproject:否则 UnrealEditor.exe 会把关卡路径当工程名解析失败,
	// 表现为 DS 秒退 + ready 等待超时,排查成本高 —— 在启动时 fail-fast。
	if cfg.Launcher == conf.LauncherEditor {
		if cfg.ProjectPath == "" {
			return nil, fmt.Errorf("local_ds: project_path required when launcher=%s", conf.LauncherEditor)
		}
		if _, err := os.Stat(cfg.ProjectPath); err != nil {
			return nil, fmt.Errorf("local_ds: project_path %q not found: %w", cfg.ProjectPath, err)
		}
	}
	if cfg.PortRange <= 0 {
		return nil, fmt.Errorf("local_ds: port_range must be > 0")
	}
	l := &LocalGameServerAllocator{
		cfg:       cfg,
		procs:     make(map[string]*launchedProc),
		usedPorts: make(map[int]struct{}),
	}
	l.startProc = l.defaultStart
	l.portProbe = defaultPortProbe
	return l, nil
}

// Allocate 拉起一个本机 DS 进程,返回 (podName, host:port)。
func (l *LocalGameServerAllocator) Allocate(_ context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	podName := fmt.Sprintf("pandora-battle-local-%d", matchID)

	l.mu.Lock()
	defer l.mu.Unlock()

	// 幂等:同对局已拉起 → 直接返回原地址。
	if p, ok := l.procs[podName]; ok {
		return podName, p.addr, releaseTrack, nil
	}

	port, ok := l.pickPortLocked()
	if !ok {
		return "", "", "", errcode.New(errcode.ErrDSNoAvailable,
			"local_ds: no free port in [%d,%d) for match %d",
			l.cfg.PortBase, l.cfg.PortBase+l.cfg.PortRange, matchID)
	}

	// DS 回调令牌一次性签发(审核 P1):在此签一次并透传给进程 env,避免“预签验证 + 启动再签”
	// 的二次签发失败只告警的空窗(enforce 下二次失败会拉起一个无令牌、回调必被拒的 DS)。
	//   enforce(dsTokenRequired):签发失败 fail-closed,不拉起。
	//   off/permissive:签发失败只告警,token 置空(DS 无令牌照常运行,守卫放行)。
	var dsToken string
	instanceUID := uuid.NewString()
	const instanceEpoch uint32 = 1
	if l.dsTokenIssuer != nil {
		tok, terr := l.dsTokenIssuer(matchID, podName, instanceUID, instanceEpoch)
		if terr != nil {
			if l.dsTokenRequired {
				return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
					"ds_callback_token sign failed under enforce for match %d: %v", matchID, terr)
			}
			plog.With(context.Background()).Warnw("msg", "ds_callback_token_sign_failed", "match_id", matchID, "err", terr)
		} else {
			dsToken = tok
		}
	}

	proc, err := l.startProc(podName, port, matchID, mapID, gameMode, dsToken)
	if err != nil {
		return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
			"local_ds: launch match %d on port %d: %v", matchID, port, err)
	}

	addr := fmt.Sprintf("%s:%d", l.cfg.AdvertiseHost, port)
	lp := &launchedProc{proc: proc, port: port, addr: addr,
		instanceUID: instanceUID, instanceEpoch: instanceEpoch}
	l.procs[podName] = lp
	l.usedPorts[port] = struct{}{}

	go l.reap(podName, lp)

	return podName, addr, releaseTrack, nil
}

// localBattleRoster 是一局待拉起 DS 的权威准入元数据(mode=local 版的 Agones annotation)。
type localBattleRoster struct {
	playerIDs    []uint64
	allocationID string
	releaseTrack string
}

// SetPendingBattleRoster 在拉起 DS **之前**登记本局的权威准入元数据。
//
// 为什么必须有:UE 的 APandoraBattleGameMode::ApplyAgonesAdmissionMetadata 只从 Agones
// GameServer annotation 装载 ExpectedPlayers;mode=local 没有 Agones,花名册恒空,
// APandoraPveGameMode::EvaluateSoloDungeonLeaveRequest 因 roster_count==0 返回
// AuthorityNotReady —— 玩家在副本里能正常战斗,但点「退出副本/失败结算」永远被拒
// (2026-08-04 实测,DS 日志:result=4 canonical_pve=1 roster_count=0)。
// 本地没有 annotation 通道,故改用 env 投递同一份事实(与 PANDORA_DS_TOKEN 同机制)。
//
// 必须在 Allocate 之前调用:DS 进程在 Allocate 内 exec,env 那一刻就已定型。
// 幂等:同 matchID 重复登记以最后一次为准;Allocate 消费后即删除,避免台账无界增长。
func (l *LocalGameServerAllocator) SetPendingBattleRoster(matchID uint64, playerIDs []uint64,
	allocationID, releaseTrack string) {
	if matchID == 0 {
		return
	}
	ids := append([]uint64(nil), playerIDs...)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pendingRosters == nil {
		l.pendingRosters = make(map[uint64]localBattleRoster)
	}
	l.pendingRosters[matchID] = localBattleRoster{
		playerIDs: ids, allocationID: allocationID, releaseTrack: releaseTrack,
	}
}

// canonicalRosterText 把玩家 ID 编成 UE 侧 ParseCanonicalBattleRoster 认的 canonical 文本:
// 十进制、逗号分隔、**严格升序去重**、非零、上限 128 人。
// 任一条不满足 UE 会整份判非法并保持空名单(fail-closed),所以这里宁可返回空串不投递,
// 也不投一份会被对面拒掉的半成品 —— 那只会把「没投」伪装成「投了但格式错」,更难查。
func canonicalRosterText(playerIDs []uint64) string {
	const maxBattleRosterPlayers = 128
	uniq := make([]uint64, 0, len(playerIDs))
	seen := make(map[uint64]struct{}, len(playerIDs))
	for _, id := range playerIDs {
		if id == 0 {
			return ""
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	if len(uniq) == 0 || len(uniq) > maxBattleRosterPlayers {
		return ""
	}
	slices.Sort(uniq)
	parts := make([]string, 0, len(uniq))
	for _, id := range uniq {
		parts = append(parts, strconv.FormatUint(id, 10))
	}
	return strings.Join(parts, ",")
}

// LocalInstanceIdentity 返回本机 DS 进程的 exact 实例身份(供 legacy 面回填战斗记录)。
//
// 为什么必须有:legacy(非 Model B)分配路径只回 (pod, addr, track),BattleStorageRecord
// 的 gameserver_uid / instance_epoch 恒为零值,matchmaker 据此判「ds_allocator 未回填完整
// DS 目标」拒签 v2 战斗票,对局直接判 FAILED —— 玩家永远进不去副本
// (2026-08-04 mode=local 实测)。这里给出的身份**就是**签进该 DS 回调令牌的那一组
// (Allocate 处同一次生成),所以与 DS 自报身份逐字段相等,不是另造一份。
//
// pod 不在台账(已回收 / 名字不符)时返回 false —— 调用方必须据此**不回填**,
// 绝不能拿别的进程或半截身份糊一个实例身份出来。
func (l *LocalGameServerAllocator) LocalInstanceIdentity(podName string) (string, uint32, bool) {
	if podName == "" {
		return "", 0, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lp, ok := l.procs[podName]
	if !ok || lp.instanceUID == "" || lp.instanceEpoch == 0 {
		return "", 0, false
	}
	return lp.instanceUID, lp.instanceEpoch, true
}

// Release 终止指定 DS 进程;台账无此记录视作已释放(幂等)。
func (l *LocalGameServerAllocator) Release(_ context.Context, podName string) error {
	if podName == "" {
		return nil
	}
	l.mu.Lock()
	lp, ok := l.procs[podName]
	if ok {
		delete(l.procs, podName)
		delete(l.usedPorts, lp.port)
	}
	l.mu.Unlock()

	if !ok {
		return nil
	}
	if err := lp.proc.Kill(); err != nil {
		return errcode.New(errcode.ErrDSAllocationFailed, "local_ds: kill %s: %v", podName, err)
	}
	return nil
}

// Close 终止全部在管 DS 进程(ds_allocator 退出时调用,避免遗留孤儿 DS)。
func (l *LocalGameServerAllocator) Close() error {
	l.mu.Lock()
	procs := l.procs
	l.procs = make(map[string]*launchedProc)
	l.usedPorts = make(map[int]struct{})
	l.mu.Unlock()

	for _, lp := range procs {
		_ = lp.proc.Kill()
	}
	return nil
}

// reap 等待进程退出后清理台账释放端口(仅当台账里仍是同一条记录,避免与 Release/重拉竞态)。
func (l *LocalGameServerAllocator) reap(podName string, lp *launchedProc) {
	_ = lp.proc.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.procs[podName]; ok && cur == lp {
		delete(l.procs, podName)
		delete(l.usedPorts, lp.port)
	}
}

// pickPortLocked 在端口池里取一个空闲端口(调用方须持锁)。
// 除排除本 allocator 已分配的端口(usedPorts)外,还用 portProbe 实际探测端口在本机可绑定,
// 跳过被幽灵 DS / 外部程序占用的端口,保证发给 UE DS 的 -port 就是它能真正绑上的端口。
func (l *LocalGameServerAllocator) pickPortLocked() (int, bool) {
	for p := l.cfg.PortBase; p < l.cfg.PortBase+l.cfg.PortRange; p++ {
		if _, used := l.usedPorts[p]; used {
			continue
		}
		if l.portProbe != nil && !l.portProbe(p) {
			continue // 端口被占(幽灵 DS / 外部程序)→ 跳过,避免 UE 静默换端口
		}
		return p, true
	}
	return 0, false
}

// defaultPortProbe 探测端口在所有网卡上 UDP+TCP 是否可绑定(UE DS NetDriver 用 UDP,保守起见
// TCP 也探,兼容 TCP/WebSocket 传输)。绑定成功立即释放再交给 UE 启动;探测与 UE 真正绑定之间
// 有极短 TOCTOU 窗口,但幽灵 DS 是持续占用能被稳定挡住,usedPorts 又已防 allocator 自身重复分配。
func defaultPortProbe(port int) bool {
	uc, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return false
	}
	_ = uc.Close()
	tl, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	_ = tl.Close()
	return true
}

// defaultStart 是 startProc 的真实现:exec UE Windows DS 并把 stdout/stderr 落盘。
func (l *LocalGameServerAllocator) defaultStart(podName string, port int, matchID uint64, mapID uint32, gameMode, token string) (dsProcess, error) {
	cmd := exec.Command(l.cfg.ExecutablePath, l.buildArgs(port, mapID)...) //nolint:gosec // 路径来自受信本机配置
	if l.cfg.WorkingDir != "" {
		cmd.Dir = l.cfg.WorkingDir
	}
	cmd.Env = l.buildEnv(podName, matchID, mapID, gameMode, token)

	var logF *os.File
	if l.cfg.LogDir != "" {
		if err := os.MkdirAll(l.cfg.LogDir, 0o755); err == nil {
			if f, ferr := os.Create(filepath.Join(l.cfg.LogDir, podName+".log")); ferr == nil {
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
		return nil, err
	}
	return &execProcess{cmd: cmd, logF: logF}, nil
}

// buildArgs 拼 UE DS 命令行:[.uproject] + 关卡 + -server -log -port=<port> + 额外参数。
// 首个加载关卡由 cfg.ResolveStartupMap(mapID) 决定:配了 LoaderMap 则统一启到加载/分发关卡(UE 侧
// 读 PANDORA_MAP_ID → 查表 → ServerTravel);否则按 map_id 从 Maps 直接选副本图(未命中回退 MapName)。
//
// launcher=editor 时在最前面插 .uproject:UE 的 LaunchSetGameName 只把命令行里**第一个**不以 '-'
// 开头的 token 当工程/关卡,所以 .uproject 必须排在关卡 URL 之前,否则引擎会把关卡路径当工程名解析失败。
// -server 两种形态都带 —— NetMode 恒为 NM_DedicatedServer,IsRunningDedicatedServer() 为 true,
// DS 子系统/心跳/在线准入全链路与打包 DS 完全一致,后端对此无感。
func (l *LocalGameServerAllocator) buildArgs(port int, mapID uint32) []string {
	args := make([]string, 0, 5+len(l.cfg.ExtraArgs))
	if l.cfg.Launcher == conf.LauncherEditor && l.cfg.ProjectPath != "" {
		args = append(args, l.cfg.ProjectPath)
	}
	if mapName := l.cfg.ResolveStartupMap(mapID); mapName != "" {
		args = append(args, mapName)
	}
	args = append(args, "-server", "-log", fmt.Sprintf("-port=%d", port))
	args = append(args, l.cfg.ExtraArgs...)
	return args
}

// buildEnv 在当前进程环境基础上注入 DS 身份变量(对齐 UE DS 侧 PandoraAgonesProvider 读取的 env)。
// token 是 Allocate 一次性签好的 DS 回调令牌(空=未启用/off 下签发失败,DS 无令牌运行)。
//
// **调用约定:调用方必须已持有 l.mu**(唯一链路 Allocate → startProc → defaultStart → buildEnv,
// Allocate 全程持锁)。本函数内部因此直接读写 pendingRosters,绝不可再取锁(不可重入 → 死锁)。
func (l *LocalGameServerAllocator) buildEnv(podName string, matchID uint64, mapID uint32, gameMode, token string) []string {
	env := os.Environ()
	env = append(env,
		"AGONES_GAMESERVER_NAME="+podName,
		"PANDORA_MATCH_ID="+strconv.FormatUint(matchID, 10),
		"PANDORA_MAP_ID="+strconv.FormatUint(uint64(mapID), 10),
		"PANDORA_GAME_MODE="+gameMode,
		// 仅 Windows 本机 allocator 注入；UE 还会校验 local pod 身份与非 Agones 运行态。
		auth.DSLocalProfileEnv+"="+auth.DSLocalProfileOffV1,
	)
	// DS 回调服务令牌(审核 P1 #1):local 模式经 env 下发(agones 模式走 annotation)。
	if token != "" {
		env = append(env, "PANDORA_DS_TOKEN="+token)
	}
	// 权威准入元数据(mode=local 版的 battle admission annotation 三件套)。
	// UE 侧要求 roster / allocation-id / release-track **同时**齐备才认(缺一即整份判非法),
	// 故这里也三者同投同不投,绝不投半份。roster 编码见 canonicalRosterText。
	// ⚠️ 这里**不能**再取 l.mu:唯一生产调用链是 Allocate → startProc → defaultStart → buildEnv,
	// 而 Allocate 全程持有 l.mu(defer Unlock),Go 互斥锁不可重入,再取即自锁死
	// —— 表现为 DS 进程永远不被拉起、AllocateBattle 无限挂起、对局最终按空 pod 判弃
	// (2026-08-04 实测,pod="" 的 battle_abandoned_heartbeat_timeout 就是它)。
	// 故本函数按「调用方已持 l.mu」的前提直接读写 pendingRosters。
	roster, hasRoster := l.pendingRosters[matchID]
	if hasRoster {
		delete(l.pendingRosters, matchID) // 消费即删,台账不随对局数无界增长
	}
	if hasRoster {
		if text := canonicalRosterText(roster.playerIDs); text != "" &&
			roster.allocationID != "" && roster.releaseTrack != "" {
			env = append(env,
				"PANDORA_BATTLE_ROSTER="+text,
				"PANDORA_ALLOCATION_ID="+roster.allocationID,
				"PANDORA_RELEASE_TRACK="+roster.releaseTrack,
			)
		} else {
			plog.With(context.Background()).Warnw("msg", "local_battle_roster_not_canonical",
				"match_id", matchID, "players", len(roster.playerIDs),
				"allocation_id", roster.allocationID, "release_track", roster.releaseTrack,
				"hint", "三件套不全或 roster 非 canonical,不投递;UE 侧将保持空名单、主动退出会被判 AuthorityNotReady")
		}
	}
	// extra_env 追加,但严禁覆盖内置身份/令牌变量(审核 P1:extra_env 覆盖 PANDORA_DS_TOKEN
	// 会用静态/伪造令牌替换真签发令牌,绕过范围绑定)。保留字命中即跳过并告警。
	for k, v := range l.cfg.ExtraEnv {
		if isReservedDSEnvKey(k) {
			plog.With(context.Background()).Warnw("msg", "extra_env_reserved_key_ignored", "key", k,
				"hint", "extra_env 不得覆盖 PANDORA_DS_TOKEN / PANDORA_MATCH_ID / AGONES_GAMESERVER_NAME 等内置变量")
			continue
		}
		env = append(env, k+"="+v)
	}
	return env
}

// isReservedDSEnvKey 判断 env key 是否为 allocator 内置注入的身份/令牌变量,extra_env 不得覆盖。
// 大小写不敏感:Windows(local 模式 DS 宿主)环境变量名大小写不敏感,`pandora_ds_token` 与
// `PANDORA_DS_TOKEN` 指向同一变量;若只按精确大写比对,小写别名仍能覆盖真令牌(审核 P1 补漏)。
func isReservedDSEnvKey(k string) bool {
	switch strings.ToUpper(strings.TrimSpace(k)) {
	case "PANDORA_DS_TOKEN", auth.DSLocalProfileEnv, "AGONES_GAMESERVER_NAME", "PANDORA_MATCH_ID",
		"PANDORA_MAP_ID", "PANDORA_GAME_MODE", "PANDORA_DS_TYPE", "PANDORA_REGION":
		return true
	default:
		return false
	}
}
