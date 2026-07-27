// Package writerlease 是「单活跃代际写者」继任租约(INC-20260722-004 R9 P0-7 收口;
// docs/design/session-generation-rollout.md §5)。
//
// 背景:hub_allocator 是 assignment/容量账本的单写者(dsauthfence V3 契约)。部署层
// Recreate 只能用「先杀旧再起新」保证单写者,滚动更新(RollingUpdate)会出现新旧两个
// 二进制并行的双写窗口。本包把单写者约束从部署策略下沉为运行时协议:
//
//   - 所有副本竞选同一个 etcd election(concurrency.Election,session lease TTL 内
//     自动失效);任一时刻至多一个副本持有领导权;
//   - 当选副本获得一个 fencing token = 本届 leader key 的 CreateRevision。etcd 选举
//     按 CreateRevision 最小者当选、删除后由次小者接任,因此**历届 leader 的 token
//     严格单调递增**(Chubby sequencer 语义);
//   - 业务写路径在入口检查 Current()(快速拒绝),存储层把 token 写进与业务事务同
//     slot 的 fence key 并做「只进不退」比较(迟到旧写者一旦落后于已推进的 fence
//     立即被拒,见 hub_allocator data 层 guardWriterFence);
//   - 失去领导权(session 过期/etcd 断连)**不退出进程**:Current() 立即转为不持有,
//     副本降级为热备并自动重新竞选。这正是 RollingUpdate 所需语义——新副本 Ready 后
//     旧副本收 SIGTERM 主动 Resign,新副本亚秒接任;崩溃场景由 lease TTL 兜底。
//
// 与 pkg/leader/etcdleader 的区别:etcdleader 是回调式(当选后跑一个循环),不暴露
// 持有权句柄与 fencing token;本包是句柄式,供 RPC 写路径逐请求查询。放在 dsauthfence
// module 子包:同属 DS 授权生产栅栏体系,且复用其 etcd client 依赖与 mTLS 安全构造,
// 业务服务(已依赖 dsauthfence)零新增 go.mod 条目。
package writerlease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/luyuancpp/pandora/pkg/dsauthfence"
)

const (
	// DefaultPrefix 是选举 key 前缀(与 dsauthfence required/capability、
	// pkg/leader/etcdleader 的 /pandora/leader/ 均隔离)。
	DefaultPrefix = "/pandora/writerlease/"
	// DefaultLeaseTTLSec 与 etcdleader/infra.md Leader Election 对齐:崩溃场景的
	// 最大接任延迟 ≈ 此值;正常滚动更新走主动 Resign,亚秒接任。
	DefaultLeaseTTLSec = 15
	// DefaultDialTimeout 是 etcd 连接默认超时。
	DefaultDialTimeout = 5 * time.Second
	// recampaignBackoff 是失主/出错后重新竞选前的退避,防 etcd 抖动忙等。
	recampaignBackoff = 2 * time.Second
	// resignTimeout 是让位/清理操作的独立超时(不继承已取消的父 ctx)。
	resignTimeout = 3 * time.Second
	// campaignErrEscalateAfter:连续竞选失败达此次数后日志从 Warn 升级 Error
	// (复审 P0-6:无限重试不能 fail-silent——长时间无写者必须可告警)。按默认
	// 2s 退避,15 次 ≈ 30s 无主,超过 lease TTL 兜底接任窗口即异常。
	// 激活钩子(OnElected)连续失败复用同一阈值:两者都表现为"长期无可写副本"。
	campaignErrEscalateAfter = 15
	// holdSafetyMarginSec 是"本地安全截止时间"相对 etcd lease TTL 的提前量
	// (R11 二轮复审:Current() 自带时间过期)。
	//
	// 必须**早于**服务端 lease 真正过期:etcd 侧续租是周期性的,本地不可能精确知道
	// 服务端何时判定过期;宁可自己先停手,也不要在服务端已经把任期交给别人之后还认为
	// 自己持有。3s 覆盖一次续租往返 + 时钟抖动;lease TTL 15s 时本地窗口 = 12s。
	holdSafetyMarginSec = 3
	// DefaultActivationTimeoutSec 是激活钩子(OnElected)的独立总期限
	// (R11 复审 P0-2 缺口 1)。
	//
	// 必须有界的理由:激活期间本副本**已经当选并持有 etcd leader key**,却还没对外
	// 宣告持有(Current() 仍返回不持有)。钩子若永久阻塞而不是返回错误:
	//   ① 本副本永远不可写;
	//   ② 它同时占着 leader key 不让位,其它副本的 Campaign 全部排在后面——
	//      **整个集群进入无写者状态**,而失败计数器一次都不会 +1(计数只在 err != nil
	//      分支),degraded 恒为 false,长期无主完全静默。
	// 加上期限后阻塞会转成 context.DeadlineExceeded 返回错误,走既有的
	// 「让位 → 退避 → 重新竞选」路径,计数器与 degraded 才能动起来。
	//
	// 取值:激活钩子是有界工作(hub_allocator 是把全部已知 pod 的 fence 水位推一遍),
	// 30s = 2× lease TTL,足够慢 etcd 完成,又保证无主状态在租约兜底窗口的同数量级内
	// 变成可观测。
	DefaultActivationTimeoutSec = 30
)

// HealthSnapshot 是竞选/激活健康度快照(R10 复审 P0-2:长期无主必须可观测)。
// held/token 是当前对外宣告的持有状态;两组计数分别定位"选不上"与"选上了但激活不过"。
type HealthSnapshot struct {
	// Held/Token 与 Current() 同源(激活钩子成功后才为 true)。
	Held  bool
	Token uint64
	// ConsecutiveCampaignErrs/LastCampaignErr:连续竞选失败次数与最近原因(当选清零)。
	ConsecutiveCampaignErrs uint64
	LastCampaignErr         string
	// ConsecutiveActivationErrs/LastActivationErr:当选后激活钩子、首次/持有期 TTL proof
	// 连续失败次数与最近原因。只有任期进入 held 后又完成一次稳定续证才清零。
	ConsecutiveActivationErrs uint64
	LastActivationErr         string
	// EscalateAfter 是日志升级/告警建议阈值(连续失败达此值 ≈ 超过 lease TTL 兜底窗口)。
	EscalateAfter uint64
}

// Degraded 报告"本副本长期既非写者、也无法成为写者"。运维告警/观测端点用它把
// 无限重试的热备语义与真正的持续无主区分开(不接 K8s readiness——见 Config.OnElected
// 与 docs/design/audit-residual-architecture-20260724.md A1:热备副本必须保持可服务)。
func (h HealthSnapshot) Degraded() bool {
	if h.Held {
		return false
	}
	return h.ConsecutiveCampaignErrs >= h.EscalateAfter || h.ConsecutiveActivationErrs >= h.EscalateAfter
}

// Config 是继任租约配置。
type Config struct {
	// Endpoints etcd 地址(必填;生产复用 ds_auth.fence.etcd_endpoints)。
	Endpoints []string
	// Election 选举名(必填),如 "hub_allocator/writer"。同一写者域必须全体副本一致。
	Election string
	// Identity 本副本标识,仅用于可观测/排障(正确性由 lease + fencing token 保证)。
	Identity string
	// Prefix 留空用 DefaultPrefix。
	Prefix string
	// LeaseTTLSec 留空用 DefaultLeaseTTLSec。
	LeaseTTLSec int
	// DialTimeout 留空用 DefaultDialTimeout。
	DialTimeout time.Duration
	// OnElected 是**接流前硬门**(R10 复审 P0-4):当选之后、对外宣告持有领导权之前
	// 必须成功跑完的激活动作。返回 nil 才 `Current() → held`,业务写路径与存储级
	// fencing 才会放行本副本。
	//
	// 存在意义:继任者的 fence 水位推扫(hub_allocator AdvanceWriterFences)此前挂在
	// 后台 sweep tick 上懒执行,于是"当选瞬间"到"推扫完成"之间本副本已经在接写,而
	// 前任在**尚未被推扫触碰的 {pod} slot** 上仍能写入——正是审核指出的"继任 sweep
	// 不是接流前硬门"。放进本钩子后,推扫成为获得写权的前置条件。
	//
	// 契约:必须幂等(同一 token 可能重试)、必须尊重 ctx(失主/进程退出即取消)、
	// 失败按普通竞选失败处理(让位 → 退避 → 重新竞选),不退出进程;连续失败可经
	// Health() 观测。token 与本届 Current() 将要宣告的 fencing token 相同。
	OnElected func(ctx context.Context, token uint64) error
	// ActivationTimeout 是 OnElected 的独立总期限,留空用 DefaultActivationTimeoutSec。
	// 见该常量注释:阻塞型激活是"当选却不可写、又不让位"的最坏形态,必须有界。
	ActivationTimeout time.Duration
}

// Term 是一届领导任期:token 单调、失主通知、主动让位。
type Term interface {
	// Token 是本届 fencing token(历届严格单调递增,恒 >0)。
	Token() uint64
	// Lost 在任期失效(lease 过期/连接丢失)时关闭。
	Lost() <-chan struct{}
	// RemainingTTL 向 etcd 服务端确认本届 lease 仍存在并返回剩余寿命。
	// 本地安全截止时间只能由这份服务端证据续期,不能把“Lost 尚未关闭”误当成
	// keepalive ACK；确认失败时调用方必须立即自 fencing 并让位。
	RemainingTTL(ctx context.Context) (time.Duration, error)
	// Resign 主动让位并释放本届资源(幂等)。
	Resign(ctx context.Context) error
}

// Backend 隔离 etcd 细节,允许用确定性 fake 覆盖竞选/失主/让位全部分支。
type Backend interface {
	// Campaign 阻塞直到当选(返回本届 Term)或 ctx 取消/出错。
	Campaign(ctx context.Context, identity string) (Term, error)
	Close() error
}

// Lease 是继任租约句柄。业务写路径逐请求调用 Current();失主时 Current() 立即转
// 不持有,进程保持存活并自动重新竞选(新任期 token 更大)。
type Lease struct {
	backend  Backend
	identity string

	// active 把 token、Lost 信号与本地安全截止放进同一原子快照，避免快速再选时
	// Current() 混读“旧 token + 新任期 Lost channel”的 ABA 交错。
	active atomic.Pointer[holdState]

	// leaseTTLSec:本实例的 etcd session lease TTL(秒),只用于推导确认节奏/超时；
	// 可写截止必须来自 Term.RemainingTTL 的服务端实证，不能从配置值自报。
	leaseTTLSec int64

	// consecutiveCampaignErrs:连续竞选失败次数(当选即清零;复审 P0-6 可观测)。
	consecutiveCampaignErrs atomic.Uint64
	// lastCampaignErr:最近一次竞选失败原因(atomic.Value[string];当选清空)。
	lastCampaignErr atomic.Value
	// consecutiveActivationErrs/lastActivationErr:当选后激活钩子(Config.OnElected)
	// 连续失败次数与最近原因(激活成功清零;R10 P0-4 接流前硬门可观测)。
	consecutiveActivationErrs atomic.Uint64
	lastActivationErr         atomic.Value
	// activationRunning 把不尊重 ctx 的激活钩子限制为最多一个后台执行体。Go 无法
	// 强杀 goroutine；超时后本副本会立即让位，后续竞选在旧钩子退出前不再叠加泄漏。
	activationRunning atomic.Bool

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// holdState 是一次已激活任期的不可混代快照。validUntilUnixNano 只由 etcd
// RemainingTTL 成功响应续期；一旦过期，本届任期永久自 fencing，不允许“恢复后续命”。
type holdState struct {
	token              uint64
	lost               <-chan struct{}
	validUntilUnixNano atomic.Int64
	// selfFenced 是本届任期的单调终态。一旦任一 goroutine 观察到本地安全截止
	// 已过，同一 token 永久不可再次 held；迟到的 TTL proof 只能促使主循环让位，
	// 不能把已经自 fencing 的旧任期续活。
	selfFenced atomic.Bool
}

// Health 返回竞选/激活健康度快照(复审 P0-6 + R10 P0-2:无限重试不得 fail-silent,
// 运维观测端点可轮询此接口把「长期无主」暴露为告警)。计数持续增长 = etcd 不可达 /
// 配置错误 / 激活动作(如继任者 fence 推扫)持续失败。
func (l *Lease) Health() HealthSnapshot {
	token, held := l.Current()
	snap := HealthSnapshot{
		Held:                      held,
		Token:                     token,
		ConsecutiveCampaignErrs:   l.consecutiveCampaignErrs.Load(),
		ConsecutiveActivationErrs: l.consecutiveActivationErrs.Load(),
		EscalateAfter:             campaignErrEscalateAfter,
	}
	if v, ok := l.lastCampaignErr.Load().(string); ok {
		snap.LastCampaignErr = v
	}
	if v, ok := l.lastActivationErr.Load().(string); ok {
		snap.LastActivationErr = v
	}
	return snap
}

// Current 返回 (fencing token, 是否持有领导权)。数据层把 token 写进同 slot fence key
// 做只进不退比较;biz 层用 held 快速拒绝非写者副本上的写请求。
func (l *Lease) Current() (uint64, bool) {
	state := l.active.Load()
	if state == nil || state.token == 0 {
		return 0, false
	}
	if state.selfFenced.Load() {
		return 0, false
	}
	// 直接检查任期 channel，而不是等待竞选 goroutine先消费 Lost 后再清原子值。
	select {
	case <-state.lost:
		return 0, false
	default:
	}
	// R11 二轮复审:**自带时间过期,不依赖竞选 goroutine 及时消费 term.Lost()**。
	//
	// 缺陷现场:整个进程被暂停(宿主换页/GC 长停顿/容器被 freeze)数十秒后恢复。
	// 此时 etcd 侧的 session lease(TTL 15s)早已过期、任期实际上已经没了,但
	// `current` 只在竞选 goroutine 观察到 term.Lost() 之后才清零。恢复瞬间调度顺序
	// 若让业务请求 goroutine 先跑,它就会读到一个**陈旧的 held=true** 并带着作废的
	// token 去写存储——而 assignment 侧的 fencing 墓碑只有有限 TTL,暂停足够久时
	// 墓碑已过期,拦不住这一笔。
	//
	// 修法是让"持有"本身带时间:任期开始时记下一个比 etcd lease 更保守的本地截止
	// 时间(单调时钟),过了就地视为不持有。这样无论调度顺序如何,长暂停后的第一笔
	// 写就已经拿不到写权,不需要任何一方"及时"做事。
	if deadline := state.validUntilUnixNano.Load(); deadline != 0 && nowMonotonicNanos() >= deadline {
		state.selfFenced.Store(true)
		return 0, false
	}
	return state.token, true
}

// Close 主动让位并停止竞选(进程下线路径)。幂等。
func (l *Lease) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		<-l.done
		_ = l.backend.Close()
	})
	return nil
}

// Start 连接 etcd 并启动竞选循环,立即返回(领导权异步获得,调用方以 Current() 判定)。
// 连接/配置错误 fail-fast 返回 error;竞选期出错内部退避重试,绝不退出进程。
func Start(ctx context.Context, cfg Config) (*Lease, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("writerlease: empty endpoints")
	}
	if cfg.Election == "" {
		return nil, errors.New("writerlease: empty election name")
	}
	normalize(&cfg)
	cli, err := dsauthfence.DialSecureEtcdClient(cfg.Endpoints, cfg.DialTimeout, cfg.Prefix)
	if err != nil {
		return nil, err
	}
	backend := &etcdBackend{cli: cli, electionKey: cfg.Prefix + cfg.Election, ttlSec: cfg.LeaseTTLSec}
	return StartWithBackend(ctx, backend, cfg), nil
}

// StartWithBackend 使用已构造 Backend 启动竞选循环(生产走 Start;测试注入 fake)。
func StartWithBackend(ctx context.Context, backend Backend, cfg Config) *Lease {
	normalize(&cfg)
	runCtx, cancel := context.WithCancel(ctx)
	l := &Lease{
		backend:  backend,
		identity: cfg.Identity,
		// 配置 TTL 只决定确认节奏；本地安全截止由服务端 RemainingTTL 证明建立。
		leaseTTLSec: int64(cfg.LeaseTTLSec),
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go l.runWithActivationTimeout(runCtx, cfg.Election, cfg.OnElected, cfg.ActivationTimeout)
	return l
}

func normalize(cfg *Config) {
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.LeaseTTLSec <= 0 {
		cfg.LeaseTTLSec = DefaultLeaseTTLSec
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.Identity == "" {
		cfg.Identity = "unknown"
	}
	if cfg.ActivationTimeout <= 0 {
		cfg.ActivationTimeout = time.Duration(DefaultActivationTimeoutSec) * time.Second
	}
}

func (l *Lease) run(ctx context.Context, election string, onElected func(context.Context, uint64) error) {
	l.runWithActivationTimeout(ctx, election, onElected, time.Duration(DefaultActivationTimeoutSec)*time.Second)
}

func (l *Lease) runWithActivationTimeout(ctx context.Context, election string,
	onElected func(context.Context, uint64) error, activationTimeout time.Duration) {
	defer close(l.done)
	for ctx.Err() == nil {
		term, err := l.backend.Campaign(ctx, l.identity)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// 复审 P0-6:连续失败计数 + 升级日志。重试本身不变(热备语义),但长期
			// 无主必须从 Warn 升级 Error 以触发日志告警;Health() 供探针/运维轮询。
			fails := l.consecutiveCampaignErrs.Add(1)
			l.lastCampaignErr.Store(err.Error())
			if fails >= campaignErrEscalateAfter {
				klog.Errorf("[writerlease] campaign failing persistently election=%s identity=%s consecutive=%d err=%v — no writer may be active, check etcd connectivity/config",
					election, l.identity, fails, err)
			} else {
				klog.Warnf("[writerlease] campaign failed election=%s identity=%s consecutive=%d err=%v", election, l.identity, fails, err)
			}
			if !sleepCtx(ctx, recampaignBackoff) {
				return
			}
			continue
		}
		l.consecutiveCampaignErrs.Store(0)
		l.lastCampaignErr.Store("")
		// 接流前硬门(R10 P0-4):当选 ≠ 可写。激活钩子(继任者 fence 水位推扫等)
		// 必须先跑成功,本副本才对外宣告持有领导权;失败就让位重选,期间 Current()
		// 保持不持有,写请求继续被拒(可重试),绝不出现"已接流但继任未完成"的窗口。
		// 激活钩子成功还不等于有写权。宣告持有前必须再向 etcd 服务端证明本届
		// lease 仍有足够剩余寿命，并用这份实际 TTL 建立初始安全截止；否则激活
		// 期间发生的 keepalive/分区异常会在 Lost 信号尚未到达时凭配置 TTL 误放写。
		state, aerr := l.prepareHold(ctx, term, onElected, activationTimeout)
		if aerr != nil {
			if ctx.Err() != nil {
				resignTerm(term)
				return
			}
			l.recordActivationFailure(election, term.Token(), aerr)
			resignTerm(term)
			if !sleepCtx(ctx, recampaignBackoff) {
				return
			}
			continue
		}
		// token/Lost/初始安全截止来自同一届任期、一次原子发布。Current() 因而
		// 不会在快速再选时混读两个任期，也不会在 TTL 证据建立前观察到 held。
		l.active.Store(state)
		klog.Infof("[writerlease] elected election=%s identity=%s token=%d", election, l.identity, term.Token())
		if shutdown := l.holdUntilTermEnds(ctx, election, term, state); shutdown {
			l.active.CompareAndSwap(state, nil)
			resignTerm(term)
			klog.Infof("[writerlease] resigned election=%s identity=%s (shutdown)", election, l.identity)
			return
		}
		// 失主:先撤销本地持有权(Current() 立即转不持有,快速挡住后续写入口),
		// 再 best-effort 清理旧任期;存储层 fence 兜住撤销瞬间仍在途的迟到写。
		l.active.CompareAndSwap(state, nil)
		klog.Warnf("[writerlease] term lost election=%s identity=%s token=%d — stepping down (process stays alive)",
			election, l.identity, term.Token())
		resignTerm(term)
		if !sleepCtx(ctx, recampaignBackoff) {
			return
		}
	}
}

func (l *Lease) recordActivationFailure(election string, token uint64, err error) {
	fails := l.consecutiveActivationErrs.Add(1)
	l.lastActivationErr.Store(err.Error())
	if fails >= campaignErrEscalateAfter {
		klog.Errorf("[writerlease] activation/lease proof failing persistently election=%s identity=%s token=%d consecutive=%d err=%v — no stable writer, check storage/etcd",
			election, l.identity, token, fails, err)
	} else {
		klog.Warnf("[writerlease] activation/lease proof failed election=%s identity=%s token=%d consecutive=%d err=%v — resigning to retry",
			election, l.identity, token, fails, err)
	}
}

func (l *Lease) markHoldStable() {
	l.consecutiveActivationErrs.Store(0)
	l.lastActivationErr.Store("")
}

// prepareHold 完成本届从“etcd 当选”到“可对外写”的全部前置门禁。业务激活钩子与
// 首次 TTL 证明任一失败都让位，active 始终保持 nil。
func (l *Lease) prepareHold(ctx context.Context, term Term,
	onElected func(context.Context, uint64) error, activationTimeout time.Duration) (*holdState, error) {
	if onElected != nil {
		if err := l.activate(ctx, term, onElected, activationTimeout); err != nil {
			return nil, err
		}
	}
	return l.beginHold(ctx, term)
}

// activate 在"已当选、未宣告"的窗口里跑激活钩子。钩子 ctx 同时受进程退出、本届失主与
// **独立总期限**驱动:任期一旦失效立即取消,避免用已作废的 token 继续对存储做推进;
// 期限到了则把"永久阻塞"转成可计数、可告警的错误(R11 复审 P0-2:阻塞时本副本占着
// leader key 又不可写,会把全集群拖成静默无主)。
func (l *Lease) activate(ctx context.Context, term Term, onElected func(context.Context, uint64) error,
	timeout time.Duration) error {
	if !l.activationRunning.CompareAndSwap(false, true) {
		return errors.New("writerlease: previous timed-out activation is still running")
	}
	actCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		defer l.activationRunning.Store(false)
		result <- onElected(actCtx, term.Token())
	}()
	stop := make(chan struct{})
	go func() {
		select {
		case <-term.Lost():
			cancel()
		case <-stop:
		}
	}()
	var err error
	select {
	case err = <-result:
	case <-actCtx.Done():
		err = actCtx.Err()
	}
	close(stop)
	if err != nil {
		return err
	}
	// 钩子"返回 nil 但其实是被期限打断"的情况(钩子吞掉了 ctx 错误)同样不许宣告持有。
	if actCtx.Err() != nil {
		return fmt.Errorf("writerlease: activation exceeded its %s budget: %w", timeout, actCtx.Err())
	}
	// 钩子成功但任期在此期间已失效 → 不得宣告持有(否则用作废 token 接流)。
	select {
	case <-term.Lost():
		return errors.New("writerlease: term lost during activation")
	default:
	}
	return nil
}

// nowMonotonicNanos 返回单调时钟纳秒。**必须用单调时钟**:系统时间可被 NTP 回拨,
// 用它算租约剩余会在回拨瞬间凭空延长写权。time.Since(处理器启动基准) 即单调。
func nowMonotonicNanos() int64 { return int64(time.Since(processStart)) }

// processStart 是进程内单调基准。time.Now() 返回值带单调读数,Since 只用单调部分。
var processStart = time.Now()

// beginHold 在宣告持有前向 etcd 服务端确认本届 lease，并按实际 RemainingTTL 建立
// 本地安全截止。不能用配置 TTL 代替服务端证据：激活阶段可能已消耗租期，keepalive
// 也可能已异常而 Lost 信号尚未关闭。
func (l *Lease) beginHold(ctx context.Context, term Term) (*holdState, error) {
	state := &holdState{token: term.Token(), lost: term.Lost()}
	confirmCtx, cancel := context.WithTimeout(ctx, l.ttlProofTimeout())
	// 以请求发出前的单调时刻为 proof 锚点，而不是响应返回后的“现在”。进程可能
	// 在 TimeToLive 返回到发布 active 之间被长暂停；若用恢复后的 now + remaining，
	// 会把暂停前的陈旧 TTL 凭空平移到未来，重新打开旧任期。
	proofStartedAt := nowMonotonicNanos()
	remaining, err := term.RemainingTTL(confirmCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("writerlease: initial etcd lease proof failed: %w", err)
	}
	select {
	case <-term.Lost():
		return nil, errors.New("writerlease: term lost before initial lease proof could be published")
	default:
	}
	if !l.applyTTLProof(state, remaining, proofStartedAt) {
		return nil, fmt.Errorf("writerlease: initial etcd lease TTL %s is not above safety margin %s",
			remaining, time.Duration(holdSafetyMarginSec)*time.Second)
	}
	return state, nil
}

// renewHold 只接受 etcd 服务端 RemainingTTL 的成功响应。remaining 必须大于
// 安全余量；否则没有足够证据继续写，调用方立即自 fencing。
func (l *Lease) renewHold(state *holdState, remaining time.Duration) bool {
	return l.applyTTLProof(state, remaining, nowMonotonicNanos())
}

// applyTTLProof 用一次服务端证明建立绝对单调截止点。proofStartedAt 取请求发出前，
// 因而 RPC 耗时或响应后进程暂停只会缩短写权，不会把陈旧 remaining 平移到未来。
func (l *Lease) applyTTLProof(state *holdState, remaining time.Duration, proofStartedAt int64) bool {
	if state.selfFenced.Load() {
		return false
	}
	window := remaining - time.Duration(holdSafetyMarginSec)*time.Second
	if window <= 0 {
		state.selfFenced.Store(true)
		return false
	}
	deadline := proofStartedAt + int64(window)
	if nowMonotonicNanos() >= deadline {
		state.selfFenced.Store(true)
		return false
	}
	state.validUntilUnixNano.Store(deadline)
	// 与 Current() 的“观察过期→置 selfFenced”并发时，deadline 的迟到写本身
	// 无害；二次检查保证调用方看到 false 并立即让位，同一 state 永不复活。
	if state.selfFenced.Load() || nowMonotonicNanos() >= deadline {
		state.selfFenced.Store(true)
		return false
	}
	return true
}

// holdUntilTermEnds 持有期主循环:等任期失效或进程退出,期间按固定节奏把本地截止时间
// 往后推。返回 true 表示进程退出(shutdown),false 表示失主。
//
// 为什么本地续租是安全的:每次推进都先向 etcd 服务端查询本 lease 的 RemainingTTL；
// “Lost 未关闭”只作快速失效信号，**不**作为 keepalive 成功证据。进程暂停时 ticker
// 不会触发、截止时间自然流逝；恢复后同一届已过截止便永久自 fencing，不允许续活。
func (l *Lease) holdUntilTermEnds(ctx context.Context, election string, term Term, state *holdState) bool {
	interval := l.holdRenewInterval()
	if interval <= 0 {
		// 未知 TTL(不设本地截止)时退化为原来的纯事件等待。
		select {
		case <-term.Lost():
			return false
		case <-ctx.Done():
			return true
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-term.Lost():
			// Current() 可能先于竞选 goroutine 观察到本地 deadline 过期并置
			// selfFenced；若随后 Lost 也关闭，select 可能先走本分支。仍需把这次
			// 不稳定任期计入 Health，不能因事件先后而漏报。
			if state.selfFenced.Load() {
				l.recordActivationFailure(election, state.token,
					errors.New("writerlease: term lost after local self-fencing"))
			}
			return false
		case <-ctx.Done():
			return true
		case <-ticker.C:
			// 截止一旦越过，本届永久失效。禁止先过期、后靠 ticker/网络恢复把同一
			// token “续活”，否则已被继任的旧写者会复活。
			if deadline := state.validUntilUnixNano.Load(); deadline != 0 && nowMonotonicNanos() >= deadline {
				state.selfFenced.Store(true)
				l.active.CompareAndSwap(state, nil)
				l.recordActivationFailure(election, state.token,
					errors.New("writerlease: local safety deadline expired"))
				return false
			}
			confirmTimeout := interval
			if confirmTimeout > 2*time.Second {
				confirmTimeout = 2 * time.Second
			}
			confirmCtx, cancel := context.WithTimeout(ctx, confirmTimeout)
			proofStartedAt := nowMonotonicNanos()
			remaining, err := term.RemainingTTL(confirmCtx)
			cancel()
			// 确认期间也可能越过旧截止；这种情况下即使响应成功也不复活本届。
			deadline := state.validUntilUnixNano.Load()
			if err != nil {
				state.selfFenced.Store(true)
			}
			if deadline != 0 && nowMonotonicNanos() >= deadline {
				state.selfFenced.Store(true)
			}
			proofApplied := err == nil && !state.selfFenced.Load() &&
				l.applyTTLProof(state, remaining, proofStartedAt)
			if !proofApplied {
				l.active.CompareAndSwap(state, nil)
				proofErr := err
				if proofErr == nil {
					proofErr = fmt.Errorf("writerlease: lease proof unusable (remaining=%s, deadline expired or self-fenced)", remaining)
				}
				l.recordActivationFailure(election, state.token, proofErr)
				return false
			}
			// 首次证明只允许发布，尚不足以说明 writer 稳定；至少再完成一轮持有期
			// TTL proof 后才清掉历史连续失败，避免“每届刚 held 就断”永远计数为 0。
			l.markHoldStable()
		}
	}
}

// holdRenewInterval 取本地窗口的 1/3(标准 keepalive 比例);TTL 未知时返回 0。
func (l *Lease) holdRenewInterval() time.Duration {
	if l.leaseTTLSec <= 0 {
		return 0
	}
	window := l.leaseTTLSec - holdSafetyMarginSec
	if window < 1 {
		window = 1
	}
	interval := time.Duration(window) * time.Second / 3
	if interval < 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	return interval
}

// ttlProofTimeout 给首次证明与持有期续证使用同一有界预算。配置 TTL 未知只会发生在
// 直接构造 Lease 的单元测试；生产 StartWithBackend 已 normalize，此处仍给安全上界。
func (l *Lease) ttlProofTimeout() time.Duration {
	timeout := l.holdRenewInterval()
	if timeout <= 0 || timeout > 2*time.Second {
		return 2 * time.Second
	}
	return timeout
}

func resignTerm(term Term) {
	rctx, cancel := context.WithTimeout(context.Background(), resignTimeout)
	defer cancel()
	_ = term.Resign(rctx)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// ── etcd Backend ─────────────────────────────────────────────────────────────

type etcdBackend struct {
	cli         *clientv3.Client
	electionKey string
	ttlSec      int
}

// Campaign 每届新建 session(lease)+ election,当选后 token = 本届 leader key 的
// CreateRevision(concurrency.Election.Rev())。选举按 CreateRevision 排队接任,
// 历届 token 严格单调递增。
func (b *etcdBackend) Campaign(ctx context.Context, identity string) (Term, error) {
	session, err := concurrency.NewSession(b.cli, concurrency.WithTTL(b.ttlSec), concurrency.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	election := concurrency.NewElection(session, b.electionKey)
	if err := election.Campaign(ctx, identity); err != nil {
		_ = session.Close()
		return nil, err
	}
	rev := election.Rev()
	if rev <= 0 {
		// 防御:当选后 leaderRev 理应恒 >0;异常时绝不能以 token=0(哨兵)冒充持有。
		_ = session.Close()
		return nil, errors.New("writerlease: elected but leader key revision is not positive")
	}
	return &etcdTerm{session: session, election: election, token: uint64(rev)}, nil
}

func (b *etcdBackend) Close() error { return b.cli.Close() }

type etcdTerm struct {
	session    *concurrency.Session
	election   *concurrency.Election
	token      uint64
	resignOnce sync.Once
}

func (t *etcdTerm) Token() uint64         { return t.token }
func (t *etcdTerm) Lost() <-chan struct{} { return t.session.Done() }
func (t *etcdTerm) RemainingTTL(ctx context.Context) (time.Duration, error) {
	resp, err := t.session.Client().TimeToLive(ctx, t.session.Lease())
	if err != nil {
		return 0, err
	}
	if resp == nil || resp.TTL <= 0 {
		return 0, errors.New("writerlease: etcd lease no longer has positive TTL")
	}
	return time.Duration(resp.TTL) * time.Second, nil
}
func (t *etcdTerm) Resign(ctx context.Context) error {
	var err error
	t.resignOnce.Do(func() {
		err = t.election.Resign(ctx)
		_ = t.session.Close()
	})
	return err
}
