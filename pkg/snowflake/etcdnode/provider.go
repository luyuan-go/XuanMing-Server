package etcdnode

import (
	"context"
	"fmt"
	"io"
	"os"

	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/snowflake"
)

// noopCloser 是 static 模式下返回的空 Closer(无 etcd lease 可释放)。
type noopCloser struct{}

func (noopCloser) Close() error { return nil }

// ProvideSnowflake 按 sf.NodeIDSource 一次性装配服务的雪花发号器,
// 把「static 直接发号」与「etcd 抢占独占 nodeID + fencing 退出」两条路径收敛成一个调用,
// 让每个服务 main.go 接线只需一行,避免各自重复实现 Lost() 退出契约而漏写导致双活发重号。
//
// 语义:
//   - sf.NodeIDSource 为空 / "static":用 staticNodeID(来自 node.node_id)本地发号;
//     返回的 io.Closer 是 no-op,不引入任何 etcd 依赖行为。
//   - sf.NodeIDSource == "etcd":调用 Acquire 在 etcd 里抢占一个独占 nodeID,并**在内部**
//     起一个 goroutine 履行 fencing 契约——一旦 Holder.Lost()(续租失败 / lease 被 revoke /
//     自 fencing 超时未确认)就 os.Exit(1) 主动退出,交给 k8s 重新拉起重新抢号,杜绝同
//     nodeID 双活。返回的 io.Closer 即 *Holder,进程正常退出时 Close() 停止续租,lease 于
//     TTL 自然过期后才释放 nodeID(**刻意不 Revoke**:立即释放会让新副本在同一秒内领到同号,
//     与本进程刚发的号逐位重号,详见 Holder.Close 注释)。
//
// 用法(进入多副本阶段的服务 main.go):
//
//	sf, sfCloser, err := etcdnode.ProvideSnowflake(ctx, serviceName, cfg.Node.NodeId, cfg.Snowflake)
//	if err != nil {
//	    helper.Errorw("msg", "snowflake_provider_failed", "err", err)
//	    os.Exit(1)
//	}
//	defer func() { _ = sfCloser.Close() }()
//
// err 来源:static 模式仅当 node.node_id 越界(必须落在 [1, NodeMask],0 为 DS 保留);
// etcd 模式还包括 etcd 不可达 / 号段占满。
func ProvideSnowflake(
	ctx context.Context,
	service string,
	staticNodeID uint32,
	sf config.SnowflakeConf,
) (*snowflake.Node, io.Closer, error) {
	nodes, closer, err := ProvideSnowflakeN(ctx, service, staticNodeID, sf, 1)
	if err != nil {
		return nil, nil, err
	}
	return nodes[0], closer, nil
}

// ProvideSnowflakeN 与 ProvideSnowflake 完全同一套 nodeID 获取 + fencing 逻辑,
// 区别只在于返回 n 个**彼此独立**的发号器,它们**共用同一个 nodeID**。
//
// 用途:一个服务内有多个互不相干的 ID 空间(如 team 的 team_id 与 invite_id)时,
// 每个空间各持一个 *snowflake.Node。每个 Node 有自己的 15bit step 池,
// 因此合计发号上限是 n × 32768/s(实测线性叠加),而不是被切成 n 份。
//
// ⚠️ 共用 nodeID 的直接后果:**不同 Node 会发出逐位相同的 ID**——同一秒里
// 各自的第 K 个号必然相同,这是常态而非小概率。因此:
//   - 每个 ID 空间的值必须存放在各自独立的表 / key 前缀 / 唯一键里;
//   - 禁止把两个空间的 ID 放进同一张表、同一个 map 或同一个唯一键;
//   - **跨服务共享的 ID 空间不适用本函数**(例:instance_id 由 inventory 与 mail
//     两个服务共同铸造,且 player_item_instance.instance_id 是全局主键)——
//     那种空间的唯一性只能靠各铸造方持有不同 nodeID 来保证,本地拆多少个 Node 都无效。
//
// n 个发号器在每个可观察维度上完全等价(同 nodeID、同布局、各自独立 state),
// 因此返回切片的下标顺序不承载语义,调用方取哪个都一样。
//
// 无论 n 为多少,etcd 模式下都只抢占**一个** nodeID、只维护**一个** lease、
// 只有**一处** Lost() → 退出,失败面不随 n 增长。
func ProvideSnowflakeN(
	ctx context.Context,
	service string,
	staticNodeID uint32,
	sf config.SnowflakeConf,
	n int,
) ([]*snowflake.Node, io.Closer, error) {
	if n <= 0 {
		return nil, nil, fmt.Errorf("etcdnode: snowflake count must be >= 1, got %d", n)
	}

	if sf.NodeIDSource != "etcd" {
		// static 号段合法性在这里 fail-closed,不能落到 snowflake.NewNode 里 panic:
		//   - 0        :保留给 UE DS 本地发号器(机器号恒 0),服务端拿 0 会与 DS 本地 guid
		//                撞进同一玩家背包键空间;
		//   - >NodeMask:超出 17bit node 段会被静默截断,与别的副本撞号。
		// 走 error 而非 panic,Must* 才能打出带服务名的配置错误日志再退出。
		if staticNodeID == 0 || uint64(staticNodeID) > snowflake.NodeMask {
			return nil, nil, fmt.Errorf(
				"etcdnode: service %s static node_id=%d out of range [1,%d] (0 is reserved for the UE DS local generator)",
				service, staticNodeID, snowflake.NodeMask)
		}
		return buildNodes(uint64(staticNodeID), n, nil), noopCloser{}, nil
	}

	holder, err := Acquire(ctx, Config{
		Endpoints:   sf.EtcdEndpoints,
		Service:     etcdKeyService(service, sf.EtcdServiceName),
		Prefix:      sf.EtcdPrefix,
		LeaseTTLSec: sf.EtcdLeaseTTLSec,
	})
	if err != nil {
		return nil, nil, err
	}

	// fencing 契约(见 package doc):失租必须停止发号并退出进程。集中在此实现,
	// 各服务无需自行 select Lost(),避免漏写导致同 nodeID 双活发重号。
	go func() {
		<-holder.Lost()
		klog.Errorf("[snowflake] nodeID lease LOST service=%s node_id=%d — exiting to avoid dual-active",
			service, holder.NodeID())
		os.Exit(1)
	}()

	return buildNodes(holder.NodeID(), n, holder.Node()), holder, nil
}

// buildNodes 用同一个 nodeID 建 n 个彼此独立的发号器。
// first 非 nil 时复用它作为第 0 个——Acquire 内部已经建好一个 Node,不必丢弃重建。
func buildNodes(nodeID uint64, n int, first *snowflake.Node) []*snowflake.Node {
	nodes := make([]*snowflake.Node, n)
	if first != nil {
		nodes[0] = first
	} else {
		nodes[0] = snowflake.NewNode(nodeID)
	}
	for i := 1; i < n; i++ {
		// 同 nodeID、各自独立的 state → 各自独立的 step 池。
		nodes[i] = snowflake.NewNode(nodeID)
	}
	return nodes
}

// MustProvideSnowflake 是 ProvideSnowflake 的便捷版:自管 context,失败(仅 etcd 模式可能)
// 直接 os.Exit(1)。static 模式恒不失败,退化为 snowflake.NewNode。
//
// 让每个服务 main.go 接线只需两行、且无需引入 context:
//
//	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
//	defer func() { _ = sfCloser.Close() }()
func MustProvideSnowflake(
	service string,
	staticNodeID uint32,
	sf config.SnowflakeConf,
) (*snowflake.Node, io.Closer) {
	node, closer, err := ProvideSnowflake(context.Background(), service, staticNodeID, sf)
	if err != nil {
		klog.Errorf("[snowflake] MustProvideSnowflake failed service=%s err=%v", service, err)
		os.Exit(1)
	}
	return node, closer
}

// MustProvideSnowflakeN 是 ProvideSnowflakeN 的便捷版,语义与约束见 ProvideSnowflakeN。
// 服务内每个独立 ID 空间取一个,例如 team 的 team_id / invite_id:
//
//	sfs, sfCloser := etcdnode.MustProvideSnowflakeN(serviceName, cfg.Node.NodeId, cfg.Snowflake, 2)
//	defer func() { _ = sfCloser.Close() }()
//	teamSF, inviteSF := sfs[0], sfs[1]
func MustProvideSnowflakeN(
	service string,
	staticNodeID uint32,
	sf config.SnowflakeConf,
	n int,
) ([]*snowflake.Node, io.Closer) {
	nodes, closer, err := ProvideSnowflakeN(context.Background(), service, staticNodeID, sf, n)
	if err != nil {
		klog.Errorf("[snowflake] MustProvideSnowflakeN failed service=%s n=%d err=%v", service, n, err)
		os.Exit(1)
	}
	return nodes, closer
}

// etcdKeyService 决定 etcd key 命名空间:显式配了 etcd_service_name 用它,否则用服务名。
// 不同服务类型各自一套 [0, MaxNodeID) 空间,故 node_id 可跨服务复用、同服务内唯一。
func etcdKeyService(service, override string) string {
	if override != "" {
		return override
	}
	return service
}
