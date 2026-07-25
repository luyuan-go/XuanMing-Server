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
	// ConsecutiveActivationErrs/LastActivationErr:当选后激活钩子连续失败次数与最近原因
	// (激活成功清零)。>0 表示本副本能当选但**尚未获得写权**,写请求仍被拒。
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

	// current:0 = 不持有;>0 = 当前任期 fencing token。etcd CreateRevision 恒 >0,
	// 0 可安全作哨兵。
	current atomic.Uint64

	// consecutiveCampaignErrs:连续竞选失败次数(当选即清零;复审 P0-6 可观测)。
	consecutiveCampaignErrs atomic.Uint64
	// lastCampaignErr:最近一次竞选失败原因(atomic.Value[string];当选清空)。
	lastCampaignErr atomic.Value
	// consecutiveActivationErrs/lastActivationErr:当选后激活钩子(Config.OnElected)
	// 连续失败次数与最近原因(激活成功清零;R10 P0-4 接流前硬门可观测)。
	consecutiveActivationErrs atomic.Uint64
	lastActivationErr         atomic.Value

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
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
	token := l.current.Load()
	return token, token != 0
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
	l := &Lease{backend: backend, identity: cfg.Identity, cancel: cancel, done: make(chan struct{})}
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
		if onElected != nil {
			if aerr := l.activate(ctx, term, onElected, activationTimeout); aerr != nil {
				if ctx.Err() != nil {
					return
				}
				fails := l.consecutiveActivationErrs.Add(1)
				l.lastActivationErr.Store(aerr.Error())
				if fails >= campaignErrEscalateAfter {
					klog.Errorf("[writerlease] activation failing persistently election=%s identity=%s token=%d consecutive=%d err=%v — elected but never writable, check storage/etcd",
						election, l.identity, term.Token(), fails, aerr)
				} else {
					klog.Warnf("[writerlease] activation failed election=%s identity=%s token=%d consecutive=%d err=%v — resigning to retry",
						election, l.identity, term.Token(), fails, aerr)
				}
				resignTerm(term)
				if !sleepCtx(ctx, recampaignBackoff) {
					return
				}
				continue
			}
			l.consecutiveActivationErrs.Store(0)
			l.lastActivationErr.Store("")
		}
		l.current.Store(term.Token())
		klog.Infof("[writerlease] elected election=%s identity=%s token=%d", election, l.identity, term.Token())
		select {
		case <-term.Lost():
			// 失主:先撤销本地持有权(Current() 立即转不持有,快速挡住后续写入口),
			// 再 best-effort 清理旧任期;存储层 fence 兜住撤销瞬间仍在途的迟到写。
			l.current.Store(0)
			klog.Warnf("[writerlease] term lost election=%s identity=%s token=%d — stepping down (process stays alive)",
				election, l.identity, term.Token())
			resignTerm(term)
		case <-ctx.Done():
			l.current.Store(0)
			resignTerm(term)
			klog.Infof("[writerlease] resigned election=%s identity=%s (shutdown)", election, l.identity)
			return
		}
		if !sleepCtx(ctx, recampaignBackoff) {
			return
		}
	}
}

// activate 在"已当选、未宣告"的窗口里跑激活钩子。钩子 ctx 同时受进程退出、本届失主与
// **独立总期限**驱动:任期一旦失效立即取消,避免用已作废的 token 继续对存储做推进;
// 期限到了则把"永久阻塞"转成可计数、可告警的错误(R11 复审 P0-2:阻塞时本副本占着
// leader key 又不可写,会把全集群拖成静默无主)。
func (l *Lease) activate(ctx context.Context, term Term, onElected func(context.Context, uint64) error,
	timeout time.Duration) error {
	actCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-term.Lost():
			cancel()
		case <-stop:
		}
	}()
	if err := onElected(actCtx, term.Token()); err != nil {
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
func (t *etcdTerm) Resign(ctx context.Context) error {
	var err error
	t.resignOnce.Do(func() {
		err = t.election.Resign(ctx)
		_ = t.session.Close()
	})
	return err
}
