// source_revision_mint_test.go — Hub assignment 来源版本铸号侧(INC-20260818-003)。
//
// 铸号是整条链的上游:铸出来的号只要不是严格单调的,下游 Owner 的高水位比较就会开始
// 随机拒绝合法请求(或者更糟,放行本该被拒的旧来源)。而铸号出错的典型形态是
// **同任期内序号没涨**或**换任期后序号回退**,两种都不会让任何既有用例变红。
package biz

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
)

// 写者租约 fake 复用 writer_gate_test.go 里的 fakeWriterFence(同包),不另建一份 ——
// 两份 fake 会各自漂移,而它们模拟的是同一个契约。

// TestMintSourceRevisionStrictlyIncreasesWithinTerm:同一任期内连续铸号必须严格递增。
func TestMintSourceRevisionStrictlyIncreasesWithinTerm(t *testing.T) {
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 7, held: true}}

	var prev uint64
	for i := 0; i < 100; i++ {
		got, err := u.mintSourceRevision()
		if err != nil {
			t.Fatalf("第 %d 次铸号失败: %v", i, err)
		}
		if got <= prev {
			t.Fatalf("同任期内序号没涨:第 %d 次铸出 %d,不大于前一个 %d", i, got, prev)
		}
		prev = got
	}
}

// TestMintSourceRevisionKeepsIncreasingAcrossTerms:换任期后仍必须严格递增。
//
// 这是最容易写错的一格:任期变化时序号归零(设计如此),若高位没起作用,新任期的第一个号
// 就会**小于**旧任期的最后一个号 —— 于是新写者刚当选就被 Owner 当成旧来源全线拒绝。
func TestMintSourceRevisionKeepsIncreasingAcrossTerms(t *testing.T) {
	fence := &fakeWriterFence{token: 7, held: true}
	u := &HubUsecase{writerFence: fence}

	var last uint64
	for i := 0; i < 5; i++ {
		got, err := u.mintSourceRevision()
		if err != nil {
			t.Fatalf("旧任期铸号失败: %v", err)
		}
		last = got
	}

	// 继任:任期号变大,进程内序号从头开始。
	fence.token = 8
	first, err := u.mintSourceRevision()
	if err != nil {
		t.Fatalf("新任期铸号失败: %v", err)
	}
	if first <= last {
		t.Fatalf("换任期后号回退了:新任期首号 %d,旧任期末号 %d", first, last)
	}
	if term, seq := placement.SplitSourceRevision(first); term != 8 || seq != 1 {
		t.Fatalf("新任期首号应是 (8,1),实际 (%d,%d)", term, seq)
	}
}

// TestMintSourceRevisionFailsClosedWithoutLease:持不住写者租约时必须拒绝铸号。
//
// 不能退化成返回 0(legacy):失主副本的写恰恰是本机制要挡的那一类,给它发一个 legacy
// 号等于让它在兼容窗内畅通无阻。
func TestMintSourceRevisionFailsClosedWithoutLease(t *testing.T) {
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 7, held: false}}

	got, err := u.mintSourceRevision()
	if err == nil {
		t.Fatalf("失主副本却铸出了号 %d(应 fail-closed)", got)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("错误码 %v,期望 ErrUnavailable(可重试,重试会路由到新写者)", errcode.As(err))
	}
	if got != placement.SourceRevisionLegacy {
		t.Fatalf("失败路径不得返回可用号,实际 %d", got)
	}
}

// TestMintSourceRevisionReturnsLegacyWhenFenceDisabled:未启用写者租约的部署返回 legacy。
//
// dev / mock / 单副本 Recreate 里根本不存在「两个写者并存」,本门无事可做;强行要求铸号
// 会让这些部署一行 assignment 都写不出来。
func TestMintSourceRevisionReturnsLegacyWhenFenceDisabled(t *testing.T) {
	u := &HubUsecase{} // writerFence == nil

	got, err := u.mintSourceRevision()
	if err != nil {
		t.Fatalf("未启用 fence 的部署铸号不该报错: %v", err)
	}
	if got != placement.SourceRevisionLegacy {
		t.Fatalf("未启用 fence 应返回 legacy(0),实际 %d", got)
	}
}
