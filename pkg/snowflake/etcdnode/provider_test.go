package etcdnode

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/snowflake"
)

// nodeSegment 从 ID 里取出 [node:17] 段。
func nodeSegment(id uint64) uint64 {
	return (id >> snowflake.StepBits) & snowflake.NodeMask
}

// TestProvideSnowflakeN_StaticSharesNodeID 锁定 static 模式的核心契约:
// n 个发号器是**不同对象**、共用**同一 nodeID**、各自有**独立的 step 池**。
func TestProvideSnowflakeN_StaticSharesNodeID(t *testing.T) {
	const staticNodeID = 7
	const n = 3

	nodes, closer, err := ProvideSnowflakeN(
		context.Background(), "team", staticNodeID, config.SnowflakeConf{}, n)
	if err != nil {
		t.Fatalf("static 模式不应失败: %v", err)
	}
	defer func() { _ = closer.Close() }()

	if len(nodes) != n {
		t.Fatalf("want %d 个发号器, got %d", n, len(nodes))
	}

	// 必须是 n 个不同对象——同一个对象重复返回就没有独立 step 池。
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if nodes[i] == nodes[j] {
				t.Fatalf("nodes[%d] 与 nodes[%d] 是同一对象,不构成独立 ID 空间", i, j)
			}
		}
	}

	// 每个发号器都必须用配置里的 nodeID,且自身序列严格递增。
	for i, nd := range nodes {
		prev := uint64(0)
		for k := 0; k < 100; k++ {
			id := nd.Generate()
			if got := nodeSegment(id); got != staticNodeID {
				t.Fatalf("nodes[%d] 第 %d 个 ID 的 node 段=%d, want %d", i, k, got, staticNodeID)
			}
			if id <= prev {
				t.Fatalf("nodes[%d] 序列非严格递增: %d 之后出现 %d", i, prev, id)
			}
			prev = id
		}
	}
}

// TestProvideSnowflakeN_CrossSpaceIDsCollideByDesign 把「共用 nodeID ⇒ 跨空间重号」
// 这一**已知且刻意**的契约钉死。
//
// 它不是 bug:唯一性只在单个发号器内成立,业务必须把各空间的 ID 分表 / 分 key 存放。
// 若将来有人"顺手修好"让跨空间也不重号(例如给每个 Node 分不同 nodeID),本用例会失败,
// 提醒他同时复核 ProvideSnowflakeN 的文档契约与 nodeID 预算(空间数 × 副本数 ≤ 131072)。
func TestProvideSnowflakeN_CrossSpaceIDsCollideByDesign(t *testing.T) {
	nodes, closer, err := ProvideSnowflakeN(
		context.Background(), "team", 1, config.SnowflakeConf{}, 2)
	if err != nil {
		t.Fatalf("static 模式不应失败: %v", err)
	}
	defer func() { _ = closer.Close() }()

	a, b := nodes[0].Generate(), nodes[1].Generate()
	if a != b {
		t.Fatalf("共用 nodeID 的两个发号器,同一秒的首个 ID 应当相同(契约): a=%d b=%d", a, b)
	}
}

// TestProvideSnowflakeN_RejectsNonPositiveCount 保证 n<=0 是显式错误,
// 不会返回空切片让调用方在 nodes[0] 上 panic。
func TestProvideSnowflakeN_RejectsNonPositiveCount(t *testing.T) {
	for _, n := range []int{0, -1} {
		nodes, closer, err := ProvideSnowflakeN(
			context.Background(), "team", 1, config.SnowflakeConf{}, n)
		if err == nil {
			if closer != nil {
				_ = closer.Close()
			}
			t.Fatalf("n=%d 应当报错, got nodes=%v", n, nodes)
		}
	}
}

// TestProvideSnowflake_DelegatesToN 保证既有单发号器入口在改为委托后行为不变:
// static 模式不报错、返回可用发号器、Closer 是 no-op。
func TestProvideSnowflake_DelegatesToN(t *testing.T) {
	node, closer, err := ProvideSnowflake(
		context.Background(), "team", 9, config.SnowflakeConf{})
	if err != nil {
		t.Fatalf("static 模式不应失败: %v", err)
	}
	defer func() { _ = closer.Close() }()

	if node == nil {
		t.Fatal("static 模式必须返回可用发号器")
	}
	if got := nodeSegment(node.Generate()); got != 9 {
		t.Fatalf("node 段=%d, want 9", got)
	}
	if _, isNoop := closer.(noopCloser); !isNoop {
		t.Fatalf("static 模式的 Closer 应为 noopCloser, got %T", closer)
	}
}

// TestProvideSnowflakeN_RejectsReservedAndOutOfRangeStaticNodeID 钉住 static 号段守卫:
// 0 是 UE DS 本地发号器(机器号恒 0)的保留号,服务端拿到它会与 DS 本地 guid 撞进同一
// 玩家背包键空间;>NodeMask 会被 node 段静默截断成别人的号。两者都必须是**显式错误**,
// 不能落到 snowflake.NewNode 里 panic —— 那会绕过 Must* 带服务名的配置错误日志。
func TestProvideSnowflakeN_RejectsReservedAndOutOfRangeStaticNodeID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		nodeID uint32
	}{
		{"reserved-zero", 0},
		{"exceeds-node-mask", uint32(snowflake.NodeMask) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes, closer, err := ProvideSnowflakeN(
				context.Background(), "team", tc.nodeID, config.SnowflakeConf{}, 1)
			if err == nil {
				if closer != nil {
					_ = closer.Close()
				}
				t.Fatalf("static node_id=%d 应当报错, got nodes=%v", tc.nodeID, nodes)
			}
		})
	}
}

// TestProvideSnowflakeN_AcceptsFirstStaticNodeID 保证守卫没有误伤合法下界:
// 全部 dev 配置用的都是 1 / 2,必须照常放行。
func TestProvideSnowflakeN_AcceptsFirstStaticNodeID(t *testing.T) {
	for _, nodeID := range []uint32{1, 2, uint32(snowflake.NodeMask)} {
		nodes, closer, err := ProvideSnowflakeN(
			context.Background(), "team", nodeID, config.SnowflakeConf{}, 1)
		if err != nil {
			t.Fatalf("static node_id=%d 应当放行: %v", nodeID, err)
		}
		if got := nodeSegment(nodes[0].Generate()); got != uint64(nodeID) {
			t.Fatalf("node 段=%d, want %d", got, nodeID)
		}
		_ = closer.Close()
	}
}

// TestFirstDynamicNodeIDIsAboveStaticSegment 钉住"动态段必须在静态段之上"这条不变量:
// static→etcd 的滚更共存窗口里,静态旧副本不写 etcd、对 Acquire 不可见,只有靠号段
// 不相交才能保证新旧副本永不同号。全部 dev 配置当前用 1 / 2。
func TestFirstDynamicNodeIDIsAboveStaticSegment(t *testing.T) {
	const maxStaticNodeIDInUse = 2
	if FirstDynamicNodeID <= maxStaticNodeIDInUse {
		t.Fatalf("FirstDynamicNodeID=%d 必须大于在用的最大静态 node_id=%d",
			FirstDynamicNodeID, maxStaticNodeIDInUse)
	}
	if FirstDynamicNodeID == 0 {
		t.Fatal("nodeID 0 为 UE DS 本地发号器保留,动态段不得从 0 起")
	}
}
