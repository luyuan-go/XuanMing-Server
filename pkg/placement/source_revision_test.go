package placement

import "testing"

// TestComposeSourceRevisionIsTotallyOrdered 钉住整个机制赖以成立的那一条性质:
// 按(任期, 序号)字典序铸出来的号,数值比较就是新旧比较。
//
// 这条塌了,Owner 的高水位比较就变成了随机拒绝/随机放行 —— 而它塌的方式很隐蔽:
// 位宽算错只在跨任期时才暴露,同任期测试全绿。所以这里刻意跨任期取样。
func TestComposeSourceRevisionIsTotallyOrdered(t *testing.T) {
	// (任期, 序号)按字典序递增的一串样本;含"上一任期的大序号 vs 下一任期的小序号"
	// 这个唯一容易翻车的相邻对。
	samples := []struct{ term, seq uint64 }{
		{1, 1},
		{1, 2},
		{1, SourceRevisionMaxSeq},
		{2, 1}, // ← 关键相邻对:任期涨了,序号回到 1,revision 仍必须变大
		{2, 2},
		{100, 1},
		{SourceRevisionMaxTerm, SourceRevisionMaxSeq},
	}
	var prev uint64
	for _, s := range samples {
		got, err := ComposeSourceRevision(s.term, s.seq)
		if err != nil {
			t.Fatalf("ComposeSourceRevision(%d,%d): %v", s.term, s.seq, err)
		}
		if got <= prev {
			t.Fatalf("全序被破坏:term=%d seq=%d 铸出 %d,不大于前一个 %d", s.term, s.seq, got, prev)
		}
		prev = got
		if term, seq := SplitSourceRevision(got); term != s.term || seq != s.seq {
			t.Fatalf("拆解不还原:%d → (%d,%d),期望 (%d,%d)", got, term, seq, s.term, s.seq)
		}
	}
}

// TestComposeSourceRevisionFailsClosed:铸不出号时必须报错,绝不回绕。
//
// 回绕(截断高位 / 序号溢出归零)会让一个**更旧**的来源铸出**更大**的号,那正好是本机制
// 要防的事,而且是以"门看起来还在工作"的形式失效 —— 比直接没有门更糟。
func TestComposeSourceRevisionFailsClosed(t *testing.T) {
	cases := []struct {
		name      string
		term, seq uint64
	}{
		{"没有任期就不该铸号", 0, 1},
		{"序号 0 是 legacy 哨兵保留值", 1, 0},
		{"任期越界", SourceRevisionMaxTerm + 1, 1},
		{"序号耗尽", 1, SourceRevisionMaxSeq + 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ComposeSourceRevision(c.term, c.seq)
			if err == nil {
				t.Fatalf("越界却铸出了 %d(应 fail-closed 报错)", got)
			}
			if got != 0 {
				t.Fatalf("失败路径必须返回 0,实际 %d", got)
			}
		})
	}
}

// TestSourceRevisionLegacyIsNotAValidMintedValue:legacy 哨兵必须与任何真实铸号不相交。
//
// 若某组合能合法铸出 0,「0 = 没有版本」这个判据就失效了,带版本的新写者会被当成旧写者。
func TestSourceRevisionLegacyIsNotAValidMintedValue(t *testing.T) {
	if SourceRevisionLegacy != 0 {
		t.Fatalf("legacy 哨兵改了值(%d),owner 侧的判据要同步复核", SourceRevisionLegacy)
	}
	// 最小的合法铸号 = (1,1),必须严格大于哨兵。
	min, err := ComposeSourceRevision(1, 1)
	if err != nil {
		t.Fatalf("ComposeSourceRevision(1,1): %v", err)
	}
	if min <= SourceRevisionLegacy {
		t.Fatalf("最小铸号 %d 未超过 legacy 哨兵 %d", min, SourceRevisionLegacy)
	}
}
