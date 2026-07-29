// Package etcdnode 用 etcd Lease 自动分配 snowflake 的 nodeID。
//
// 背景(docs/design/infra.md §8.1):静态 node.node_id 在单副本 / dev 下够用,但进入
// k8s 多副本动态扩缩后,同一服务跑 N 个 pod,人工排号必然撞号 → 发重复 ID。本包用 etcd
// Lease 在 [0, MaxNodeID) 区间里**抢占一个独占 nodeID**,并以 KeepAlive 续租维持独占权。
//
// ⚠️ fencing 契约(必须遵守,否则同 nodeID 双活发重号):
//   - etcd Lease 是 nodeID 独占权的**事实来源**;KeepAlive 不是普通健康检查,而是独占权信号。
//   - 一旦 KeepAlive channel 关闭 / 续租失败 / lease 被 revoke,Holder.Lost() 会被关闭。
//   - 此外还有**自 fencing 提前量**:距上一次续租确认超过 TTL 的 2/3 仍无确认时,即便
//     channel 还没关也触发 Lost。原因:clientv3 的失租感知天然滞后于服务端过期点
//     (client 端 deadline = 收到响应时刻 + TTL,晚于服务端授予时刻 + TTL,且其
//     deadlineLoop 每 1s 才扫一轮)——只等 channel 关闭,旧 holder 会在 nodeID 已可被
//     新副本抢走之后仍继续发号 1~2s,与新 holder 同秒双活发重号。
//   - 调用方(main.go)**必须** select Lost(),收到信号后立即停止发号并主动退出进程,
//     不能只打日志继续 Generate——否则与领走同 nodeID 的新 holder 形成双活。
//
// 用法(进入 k8s 多副本阶段的服务 main.go):
//
//	holder, err := etcdnode.Acquire(ctx, etcdnode.Config{
//	    Endpoints: cfg.Snowflake.EtcdEndpoints,
//	    Service:   "matchmaker",
//	})
//	if err != nil { log.Fatal(err) }
//	defer holder.Close()
//	sf := holder.Node() // 直接当 *snowflake.Node 用
//
//	go func() {
//	    <-holder.Lost()
//	    log.Error("snowflake nodeID lease lost, exiting to avoid dual-active")
//	    os.Exit(1) // 停止发号并退出,交给 k8s 重新拉起重新抢号
//	}()
//
// 单副本 / dev 仍走 snowflake.NewNode(cfg.Node.NodeId),不引入本包。
package etcdnode

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/luyuancpp/pandora/pkg/snowflake"
)

const (
	// DefaultPrefix 是 nodeID 注册 key 的前缀。实际 key = <prefix><service>/<id>。
	DefaultPrefix = "/pandora/snowflake/node/"
	// DefaultLeaseTTLSec 是 lease 默认 TTL(秒)。15s 与 docs/design/infra.md §8.1 一致。
	DefaultLeaseTTLSec = 15
	// DefaultDialTimeout 是 etcd 连接默认超时。
	DefaultDialTimeout = 5 * time.Second

	// FirstDynamicNodeID 是 etcd 动态分配的最小 nodeID。**低于它的号段全部保留**,
	// etcd 永远不会分配出去:
	//   - 0        :UE DS 本地发号器(FMySnowflake)机器号恒为 0。服务端铸的 instance_id
	//                与 DS 本地 guid 汇进同一玩家背包键空间,服务端拿到 0 即撞键
	//                (bag 的 DuplicateGuid fail-closed 会卡住玩家领取)。
	//   - 1..7     :static 模式(node.node_id)号段。static→etcd 的滚更**共存窗口**里,
	//                仍在跑的静态旧副本不写 etcd、对 Acquire 完全不可见;若 etcd 从 1 起扫,
	//                新副本会领到旧副本正在用的号,双活发重号。把动态段抬到静态段之上,
	//                新旧永不同号,该跳无需 Recreate 也无需人工预置占位 key。
	// 新增静态 node_id 必须落在 [1, FirstDynamicNodeID) 内;不够用时抬高本常量并同步
	// docs/design/infra.md §8.1(抬高是纯扩容,不影响已分配的动态号)。
	FirstDynamicNodeID = 8

	// maxConsecutiveTxnFailures 是扫描期连续传输层失败的熔断阈值。
	// etcd 整体不可用时每个 Txn 都会吃满 dial 超时,131072 个候选顺序扫完需以天计,
	// 而 Must* 入口传的是 context.Background()(无 deadline),循环里的 ctx 检查永不触发。
	// 连续失败到该阈值即判定 etcd 不可用直接返回错误,由调用方 fail-fast 退出重来。
	maxConsecutiveTxnFailures = 5
)

// Config 是 nodeID 自动分配的配置。
type Config struct {
	// Endpoints etcd 地址(必填)。
	Endpoints []string
	// Service 服务名,用于 key 命名空间隔离:不同服务各自一套 [FirstDynamicNodeID, MaxNodeID)
	// 空间。留空回退 "default"。
	// ⚠️ 共铸同一种 ID 的服务(如 inventory / mail 共铸 instance_id)必须显式共用同一个
	// Service,否则各自的空间会分到相同 nodeID(见 docs/design/infra.md §8.2②)。
	Service string
	// Prefix key 前缀,留空用 DefaultPrefix。
	Prefix string
	// LeaseTTLSec lease TTL(秒),留空用 DefaultLeaseTTLSec。
	LeaseTTLSec int64
	// DialTimeout etcd 连接超时,留空用 DefaultDialTimeout。
	DialTimeout time.Duration
	// MaxNodeID 候选 nodeID 上界(exclusive)。留空用 snowflake.NodeMask+1(即 17bit 全空间)。
	// 实际抢占区间为 [1, MaxNodeID):nodeID 0 全局保留,**不参与分配**——
	// UE DS 的本地发号器(FMySnowflake)机器号恒为 0,服务端铸的 ID(如 instance_id)会与
	// DS 本地 guid 汇进同一玩家背包键空间,若服务端也拿到 nodeID 0,同秒同步即撞键
	// (bag 的 DuplicateGuid fail-closed 会卡住玩家领取)。static 模式同理:node.node_id
	// 配置约定从 1 起(dev 配置现值 1/2 均满足)。
	MaxNodeID uint64
}

// ErrNoFreeNode 表示 [FirstDynamicNodeID, MaxNodeID) 区间已被占满,没有空闲 nodeID 可抢。
var ErrNoFreeNode = fmt.Errorf("etcdnode: no free node id in range")

// Holder 持有一个抢占成功的 nodeID + 其 etcd lease。
type Holder struct {
	node    *snowflake.Node
	nodeID  uint64
	key     string
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	ttlSec  int64 // 服务端实际授予的 lease TTL(秒),自 fencing deadline 由它推导

	lost        chan struct{}
	lostOnce    sync.Once
	intentional atomic.Bool // Close() 主动关闭时置位,避免误报 Lost
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

// Acquire 连接 etcd,在 [0, MaxNodeID) 里抢占一个独占 nodeID,启动 KeepAlive 续租。
//
// 成功返回的 Holder:
//   - Node() 是绑定该 nodeID 的 *snowflake.Node;
//   - Lost() 在续租失败 / lease 丢失时关闭,调用方必须据此停发并退出;
//   - Close() 释放 lease 并停止续租(进程正常退出时调用)。
func Acquire(ctx context.Context, cfg Config) (*Holder, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("etcdnode: empty endpoints")
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	service := cfg.Service
	if service == "" {
		service = "default"
	}
	ttl := cfg.LeaseTTLSec
	if ttl <= 0 {
		ttl = DefaultLeaseTTLSec
	}
	// TTL 是正确性参数,不只是保活参数:Close 停止续租后 lease 自然过期形成的 nodeID
	// 复用隔离期 ≥ TTL*2/3,必须显著大于 snowflake 的 1s 秒粒度 + 现实时钟偏差。
	// 钳制下限,防止配置误填小值把隔离期打穿。
	const minLeaseTTLSec = 5
	if ttl < minLeaseTTLSec {
		klog.Warnf("[snowflake] etcdnode lease ttl %ds too small, clamped to %ds (reuse-quarantine floor)",
			ttl, minLeaseTTLSec)
		ttl = minLeaseTTLSec
	}
	dial := cfg.DialTimeout
	if dial <= 0 {
		dial = DefaultDialTimeout
	}
	maxNode := cfg.MaxNodeID
	if maxNode == 0 || maxNode > snowflake.NodeMask+1 {
		maxNode = snowflake.NodeMask + 1
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: dial,
	})
	if err != nil {
		return nil, fmt.Errorf("etcdnode: dial etcd: %w", err)
	}

	// 1. Grant lease。
	grantCtx, cancelGrant := context.WithTimeout(ctx, dial)
	lease, err := cli.Grant(grantCtx, ttl)
	cancelGrant()
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("etcdnode: grant lease: %w", err)
	}

	keyPrefix := strings.TrimSuffix(prefix, "/") + "/" + service + "/"
	value := holderIdentity()

	// 2. 顺序扫描候选 nodeID,事务抢占第一个空闲的。
	// 从 FirstDynamicNodeID 起:低位号段保留给 DS 本地发号器与 static 副本(见常量注释)。
	var (
		acquiredID  uint64
		acquiredKey string
		acquired    bool
		scanned     uint64
		txnFailures uint64
		consecutive int
		lastTxnErr  error
	)
	for id := uint64(FirstDynamicNodeID); id < maxNode; id++ {
		// ctx 已取消/超时后每个 Txn 都会瞬时失败,不检查会空转完整个区间。
		if ctxErr := ctx.Err(); ctxErr != nil {
			_, _ = cli.Revoke(context.Background(), lease.ID)
			_ = cli.Close()
			return nil, fmt.Errorf("etcdnode: acquire canceled at id=%d: %w", id, ctxErr)
		}
		scanned++
		key := keyPrefix + strconv.FormatUint(id, 10)
		txnCtx, cancelTxn := context.WithTimeout(ctx, dial)
		resp, txnErr := cli.Txn(txnCtx).
			// key 不存在(CreateRevision==0)才写,实现独占抢占。
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease.ID))).
			Commit()
		cancelTxn()
		if txnErr != nil {
			// txnErr 只说明**响应没拿到**,不能断定服务端没执行:若 Put 实际已生效,该 key
			// 已挂上本进程的 lease,继续往下抢会让一个 lease 挂两个 key —— 那个幽灵 key 会被
			// KeepAlive 一路续活到进程退出,永久占着一个本不属于自己的 nodeID。
			// 所以先复核归属:已是自己的就直接认领(响应丢失而已),别人的/不存在才继续扫。
			if owned, checkErr := keyOwnedByLease(ctx, cli, key, lease.ID, dial); checkErr == nil && owned {
				klog.Warnf("[snowflake] etcdnode txn id=%d response lost but put landed, adopting: %v", id, txnErr)
				acquiredID, acquiredKey, acquired = id, key, true
				break
			}
			// 单个 key 抢占失败(网络抖动),继续试下一个;不直接放弃整次 Acquire。
			// 只打前几条:etcd 整体不可用时区间扫描会产生大量失败,不能逐条刷日志。
			txnFailures++
			consecutive++
			lastTxnErr = txnErr
			if txnFailures <= 3 {
				klog.Warnf("[snowflake] etcdnode txn id=%d err=%v", id, txnErr)
			}
			if consecutive >= maxConsecutiveTxnFailures {
				_, _ = cli.Revoke(context.Background(), lease.ID)
				_ = cli.Close()
				return nil, fmt.Errorf("etcdnode: %d consecutive txn failures at id=%d, etcd unavailable: %w",
					consecutive, id, txnErr)
			}
			continue
		}
		consecutive = 0
		if resp.Succeeded {
			acquiredID, acquiredKey, acquired = id, key, true
			break
		}
	}

	if !acquired {
		_, _ = cli.Revoke(context.Background(), lease.ID)
		_ = cli.Close()
		if txnFailures == scanned && lastTxnErr != nil {
			// 一次事务都没提交成功:是 etcd 不可用,不是号段真被占满,不能误报 ErrNoFreeNode
			// (那会误导运维去扩号段而不是修 etcd)。
			return nil, fmt.Errorf("etcdnode: all %d txn attempts failed, etcd unavailable? last: %w",
				txnFailures, lastTxnErr)
		}
		if txnFailures > 0 {
			klog.Warnf("[snowflake] etcdnode scan: %d/%d txn attempts failed, last err=%v",
				txnFailures, scanned, lastTxnErr)
		}
		return nil, ErrNoFreeNode
	}

	h := &Holder{
		node:    snowflake.NewNode(acquiredID),
		nodeID:  acquiredID,
		key:     acquiredKey,
		cli:     cli,
		leaseID: lease.ID,
		ttlSec:  lease.TTL, // 服务端实际授予值(可能与请求值不同),自 fencing 按它算
		lost:    make(chan struct{}),
	}

	// 3. 启动 KeepAlive 续租。channel 关闭 = 租约丢失 → 触发 Lost。
	kaCtx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	kaCh, err := cli.KeepAlive(kaCtx, lease.ID)
	if err != nil {
		cancel()
		_, _ = cli.Revoke(context.Background(), lease.ID)
		_ = cli.Close()
		return nil, fmt.Errorf("etcdnode: keepalive: %w", err)
	}
	go h.keepAliveLoop(kaCh)

	klog.Infof("[snowflake] etcdnode acquired node_id=%d service=%s lease=%x ttl=%ds",
		acquiredID, service, lease.ID, ttl)
	return h, nil
}

// keepAliveLoop 持续消费 KeepAlive 确认,并维护自 fencing deadline。
//
// 只等 channel 关闭是不够的:clientv3(v3.5.x lease.go)把 client 端 deadline 记为
// 「收到续租响应时刻 + TTL」,恒晚于服务端的「处理续租时刻 + TTL」,且 deadlineLoop
// 每 1s 才扫一轮 —— 网络分区时旧 holder 会在服务端 lease 已过期、nodeID 已可被新副本
// 抢走之后,再滞后 1~2s 才感知,期间发出的号与新 holder 同秒重号。
//
// 因此这里以「上一次续租确认」为锚:超过 TTL 的 2/3 仍无确认(续租周期为 TTL/3,
// 即容忍丢一拍,第二拍丢失中途触发),就视为无法再证明租约属于自己,**先于服务端
// 过期点**触发 Lost 停止发号。宁可偶发多退一次进程(k8s 会拉起重抢),不可双活。
func (h *Holder) keepAliveLoop(kaCh <-chan *clientv3.LeaseKeepAliveResponse) {
	selfFence := time.Duration(h.ttlSec) * time.Second * 2 / 3
	timer := time.NewTimer(selfFence)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-kaCh:
			if !ok {
				// channel 关闭:要么 Close() 主动取消(intentional),要么真丢租约。
				if h.intentional.Load() {
					return
				}
				klog.Errorf("[snowflake] etcdnode lease LOST node_id=%d key=%s — caller must stop generating and exit",
					h.nodeID, h.key)
				h.signalLost()
				return
			}
			// 每拍确认都重置自 fencing deadline;丢一两拍由 etcd 自身重试。
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(selfFence)
		case <-timer.C:
			if h.intentional.Load() {
				return
			}
			klog.Errorf("[snowflake] etcdnode lease UNCONFIRMED for %v (self-fence, ttl=%ds) node_id=%d key=%s — caller must stop generating and exit",
				selfFence, h.ttlSec, h.nodeID, h.key)
			h.signalLost()
			return
		}
	}
}

func (h *Holder) signalLost() {
	h.lostOnce.Do(func() { close(h.lost) })
}

// Node 返回绑定该 nodeID 的雪花生成器。
func (h *Holder) Node() *snowflake.Node { return h.node }

// NodeID 返回抢占到的 nodeID。
func (h *Holder) NodeID() uint64 { return h.nodeID }

// Lost 在 lease 丢失时关闭。调用方必须 select 它,收到后停止发号并退出进程。
func (h *Holder) Lost() <-chan struct{} { return h.lost }

// Close 主动停止续租并断开 etcd(进程正常退出路径)。幂等。
//
// 刻意**不** Revoke lease:立即释放会让 nodeID 在同一个日历秒内就被新副本抢走 ——
// 本进程这一秒已发出的号,与新 holder 从 step 0 重新数起的号逐位相同(优雅滚更收尾 +
// Go 进程亚秒启动抢号即可复现;跨机器时钟偏差还会放大窗口)。停止续租后 lease 在服务端
// 于「最后一次续租 + TTL」自然过期,天然形成 ≥ TTL*2/3(默认 ≥10s)的复用隔离期,
// 远大于 1s 的 snowflake 秒粒度与现实 NTP 偏差;nodeID 空间共 131072 个,短暂多占
// 一个号无稀缺压力。
func (h *Holder) Close() error {
	var err error
	h.closeOnce.Do(func() {
		h.intentional.Store(true)
		if h.cancel != nil {
			h.cancel()
		}
		err = h.cli.Close()
	})
	return err
}

// keyOwnedByLease 复核 key 是否已经挂在 leaseID 上。
// 用于 Txn 响应丢失后判定"服务端到底执行没执行":已挂本 lease = Put 其实成功了。
func keyOwnedByLease(
	ctx context.Context,
	cli *clientv3.Client,
	key string,
	leaseID clientv3.LeaseID,
	timeout time.Duration,
) (bool, error) {
	getCtx, cancel := context.WithTimeout(ctx, timeout)
	resp, err := cli.Get(getCtx, key)
	cancel()
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) != 1 {
		return false, nil
	}
	return clientv3.LeaseID(resp.Kvs[0].Lease) == leaseID, nil
}

// holderIdentity 组装 lease value,便于运维排查"这个 nodeID 现在被谁占着"。
func holderIdentity() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("host=%s pid=%d ts=%d", host, os.Getpid(), time.Now().Unix())
}
