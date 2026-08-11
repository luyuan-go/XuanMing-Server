package namecheck

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// 本文件针对**真实产物** configtable/lexicon/dist 跑,与 pkg/configtable/realdist_test.go 同思路:
// 单元测试用占位词证明逻辑正确,真实产物测试证明这批词库拿到线上不会炸(规模、误杀、延迟)。
// 产物缺失时 skip —— 让只拉了代码没跑导入器的环境仍能 go test。

func realDistDir() string { return filepath.Join("..", "..", "configtable", "lexicon", "dist") }

func loadRealLexicon(t testing.TB) *Lexicon {
	t.Helper()
	dir := realDistDir()
	if _, err := os.Stat(filepath.Join(dir, LexiconManifestFile)); err != nil {
		t.Skipf("未找到词库产物(%s),先跑 go run ./tools/lexicon-import", dir)
	}
	lex, err := LoadLexicon(dir)
	if err != nil {
		t.Fatalf("加载真实词库失败: %v", err)
	}
	return lex
}

// 规模与内存:词库是每个 pod 常驻的只读结构,规模失控要在这里被发现,不是在线上 OOM 时。
func TestRealLexicon_SizeAndFootprint(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	lex := loadRealLexicon(t)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	words, nodes, edges := lex.Stats()
	heapMB := float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
	t.Logf("词库 version=%d source_rev=%q", lex.Version(), lex.SourceRev())
	t.Logf("规模:词 %d / 节点 %d / 边 %d", words, nodes, edges)
	t.Logf("常驻堆增量:%.1f MB", heapMB)

	if words < 10000 {
		t.Fatalf("词库只有 %d 条,疑似导入残缺", words)
	}
	// 上限是**容量护栏**不是性能目标:每个 go 服务都要常驻这份索引,失控要立刻可见。
	// 触发时先看是不是上游快照暴涨,再决定是提高护栏还是按分类裁剪。
	if heapMB > 256 {
		t.Fatalf("词库常驻内存 %.1f MB 超护栏 256 MB", heapMB)
	}
}

// 误杀:十万词砸下来,正常昵称被大面积拦掉的话功能就是不可用的。
// 这批样本是**普通玩家会取的名字**,一条都不许命中敏感词。
func TestRealLexicon_NoFalsePositiveOnOrdinaryNames(t *testing.T) {
	dir := realDistDir()
	if _, err := os.Stat(filepath.Join(dir, LexiconManifestFile)); err != nil {
		t.Skipf("未找到词库产物(%s)", dir)
	}
	// 昵称走**拒绝档**:只用精编分类。全量档实测对正常游戏用词有 10.4% 误杀
	// (见 Profile 注释),拿它判昵称等于挡住十分之一的玩家创角。
	lex, err := LoadLexiconProfile(dir, RejectProfile())
	if err != nil {
		t.Fatalf("加载拒绝档失败: %v", err)
	}
	t.Logf("拒绝档词条 %d(全量档 %d)", lex.Words(), loadRealLexicon(t).Words())
	c := NewChecker(Rule{
		MaxRunes:         12,
		MaxBytes:         192,
		ReservedPrefixes: []string{"Player_"},
		ReservedWords:    []string{"GM", "系统", "官方", "客服", "管理员"},
	})
	c.SetLexicon(lex)

	ordinary := []string{
		"夜行的猫", "小明", "张伟", "李娜", "王小虎",
		"星辰大海", "一叶知秋", "北方有佳人", "追风少年", "冷月无声",
		"剑影流光", "青山不改", "白衣胜雪", "十步杀一人", "醉卧沙场",
		"Neko99", "DarkKnight", "ShadowBlade", "Lucky7", "IronWolf",
		"火焰法师", "圣光骑士", "暗夜刺客", "森林游侠", "山丘之王",
	}
	var hit []string
	for _, n := range ordinary {
		if r := c.Check(n); r.Reason == ReasonSensitive {
			hit = append(hit, n+"("+r.Detail+")")
		}
	}
	if len(hit) > 0 {
		// 误杀清单直接打出来:这些是**正常词**,不是敏感词,打日志不构成词库泄露。
		// 修法是往 configtable/lexicon/source/local/allow.txt 加白名单后重跑导入器。
		t.Fatalf("正常昵称被误判为敏感词 %d 条: %v", len(hit), hit)
	}
}

// 分类漂移必须 fail-closed:上游新增词库文件时,加载直接失败,强制人工决定它归哪一档。
// 静默排除 = 昵称档漏放违禁词;静默纳入 = 聊天档突然大面积误杀。两者都不许发生。
func TestRealLexicon_ProfileDriftFailsClosed(t *testing.T) {
	dir := realDistDir()
	if _, err := os.Stat(filepath.Join(dir, LexiconManifestFile)); err != nil {
		t.Skipf("未找到词库产物(%s)", dir)
	}

	// ① 少声明一个分类 → 必须报错(模拟上游新增了词库文件)。
	p := RejectProfile()
	p.Exclude = p.Exclude[:len(p.Exclude)-1]
	if _, err := LoadLexiconProfile(dir, p); err == nil {
		t.Fatal("有分类未声明时必须拒绝加载")
	}

	// ② 引用一个不存在的分类 → 必须报错(模拟上游删了词库文件)。
	p2 := RejectProfile()
	p2.Include = append(p2.Include, "konsheng/根本不存在的分类")
	if _, err := LoadLexiconProfile(dir, p2); err == nil {
		t.Fatal("引用不存在的分类时必须拒绝加载")
	}

	// ③ 默认档必须与当前产物完全闭合(这条一红就说明该重新核对分类划分并重跑误杀探测)。
	if _, err := LoadLexiconProfile(dir, RejectProfile()); err != nil {
		t.Fatalf("默认拒绝档与当前词库产物不闭合: %v", err)
	}
}

// 延迟:昵称校验在创角同步路径上,聊天过滤在每条消息路径上,都不能是瓶颈。
func BenchmarkCheckNickname(b *testing.B) {
	lex := loadRealLexicon(b)
	c := NewChecker(Rule{MaxRunes: 12, MaxBytes: 192, ReservedPrefixes: []string{"Player_"}})
	c.SetLexicon(lex)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Check("夜行的猫Neko99")
	}
}

func BenchmarkMaskChatLine(b *testing.B) {
	lex := loadRealLexicon(b)
	c := NewChecker(Rule{MaxRunes: 12, MaxBytes: 192})
	c.SetLexicon(lex)
	line := "今天这把打得不错,下一局我出装换一下,大家注意一下小龙刷新时间,别送了"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = c.Mask(line, '*')
	}
}

// 建索引耗时进启动路径,十万词的构建不能拖到服务起不来。
func BenchmarkLoadLexicon(b *testing.B) {
	dir := realDistDir()
	if _, err := os.Stat(filepath.Join(dir, LexiconManifestFile)); err != nil {
		b.Skip("未找到词库产物")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := LoadLexicon(dir); err != nil {
			b.Fatal(err)
		}
	}
}
