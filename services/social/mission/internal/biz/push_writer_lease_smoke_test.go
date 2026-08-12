package biz

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
)

// TestPushWriterLeaseRealElection 是 push_writer_lease=enforce 分支的**真实执行**冒烟。
//
// 在此之前该分支只被 fake 租约与清单契约覆盖过,writerlease.Start() 这条路径一次都没跑过
// —— §14.2 要求"开关打开后的分支必须是完整可用的真实实现",代码完整但未执行不算数。
//
// 本用例验三件事:
//  1. Start 能真的连上 etcd 并选出 leader(Current() 返回 held=true、token>0);
//  2. 第二个副本在第一个持有期间**拿不到**领导权(单写者真的成立);
//  3. 把 *writerlease.Lease 直接当 PushWriterLease 用 —— 接口形状对得上(编译期 + 运行期)。
//
// 门控:未设 PANDORA_TEST_ETCD_ENDPOINTS 时 Skip(与真库用例同纪律)。
func TestPushWriterLeaseRealElection(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("PANDORA_TEST_ETCD_ENDPOINTS"))
	if raw == "" {
		t.Skip("跳过 writerlease 真实选举冒烟:未设置 PANDORA_TEST_ETCD_ENDPOINTS")
	}
	endpoints := strings.Split(raw, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	newLease := func(identity string) *writerlease.Lease {
		l, err := writerlease.Start(ctx, writerlease.Config{
			Endpoints:   endpoints,
			Election:    "mission/push_publisher_smoke",
			Identity:    identity,
			LeaseTTLSec: 5,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("writerlease.Start(%s): %v", identity, err)
		}
		return l
	}

	a := newLease("replica-a")
	defer func() { _ = a.Close() }()

	// 等 A 当选(竞选是异步的)。
	var tokenA uint64
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if tok, held := a.Current(); held {
			tokenA = tok
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if tokenA == 0 {
		t.Fatal("副本 A 在 20s 内没能当选 —— enforce 分支接线不通(这正是本用例存在的理由)")
	}

	// 接口形状:*writerlease.Lease 必须能直接充当 PushWriterLease。
	var _ PushWriterLease = a
	// 用真实构造函数(直接 &MissionUsecase{} 会漏掉 log,pushIsLeader 的跃迁日志会 nil-deref)。
	uc := newTestUsecase(t, baseCatalog(), newFakeRepo(testPlayer), &fakeItemGranter{}, &fakeExpGranter{}, nil)
	uc.SetPushWriterLease(a)
	if !uc.pushIsLeader() {
		t.Fatal("当选副本的 pushIsLeader() 必须为 true")
	}

	// 单写者:B 在 A 持有期间不得拿到领导权。
	b := newLease("replica-b")
	defer func() { _ = b.Close() }()
	time.Sleep(3 * time.Second)
	if _, held := b.Current(); held {
		t.Fatal("A 仍持有期间 B 也当选 = 单写者不成立,推送会交错乱序")
	}
	ucB := newTestUsecase(t, baseCatalog(), newFakeRepo(testPlayer), &fakeItemGranter{}, &fakeExpGranter{}, nil)
	ucB.SetPushWriterLease(b)
	if ucB.pushIsLeader() {
		t.Fatal("热备副本的 pushIsLeader() 必须为 false —— 否则两个发布器同时跑")
	}
	t.Logf("真实选举通过:A token=%d 持有,B 热备", tokenA)
}
