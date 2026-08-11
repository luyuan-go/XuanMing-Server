package namecheck

import (
	"strings"
	"testing"
)

// 测试词库刻意用**无害占位词**,不嵌真实敏感词:
// 真词库是审核资产,写进测试文件等于把它复制进每一次 CI 日志与代码检索结果。
// 匹配逻辑与词的语义无关,占位词覆盖度等价。
func testChecker(t *testing.T, rule Rule) *Checker {
	t.Helper()
	words := []string{"badword", "坏词", "禁语组合"}
	folded := make([]string, 0, len(words))
	for _, w := range words {
		folded = append(folded, Fold(Normalize(w)))
	}
	lex, err := NewLexicon(1, "test", []LexiconGroup{{Category: "test/deny", Words: folded}})
	if err != nil {
		t.Fatalf("建测试词库失败: %v", err)
	}
	c := NewChecker(rule)
	c.SetLexicon(lex)
	return c
}

func defaultRule() Rule {
	return Rule{
		MaxRunes:         12,
		MaxBytes:         40,
		ReservedPrefixes: []string{"Player_"},
		ReservedWords:    []string{"GM", "系统", "官方", "客服", "管理员"},
	}
}

// ── design doc §10 验收矩阵 ────────────────────────────────────────────────

func TestCheck_AcceptanceMatrix(t *testing.T) {
	c := testChecker(t, defaultRule())

	cases := []struct {
		name string
		in   string
		want Reason
	}{
		// 归一化后为空串
		{"空串", "", ReasonEmpty},
		{"全空白", "   \t  ", ReasonEmpty},
		// 零宽字符是 \p{Cf} 不是空白,Normalize 不吞它;由白名单拒,错误码比 empty 更准确。
		{"零宽字符独占", "​​", ReasonIllegalChar},

		// 全角绕过:NFKC 必须把全角打平,否则下面这条会绕过词库
		{"全角敏感词", "ｂａｄｗｏｒｄ", ReasonSensitive},
		{"全角保留前缀", "Ｐｌａｙｅｒ＿123", ReasonReserved},

		// 零宽 / RTL override:白名单拒 \p{Cf}
		{"零宽插桩", "ba​dword", ReasonIllegalChar},
		{"RTL override", "abc‮def", ReasonIllegalChar},

		// Zalgo:连续组合符号超限
		{"Zalgo", "à́̂̃b", ReasonZalgo},
		{"组合符号未超限", "à", ReasonOK},

		// 私用区
		{"私用区码点", "abcd", ReasonIllegalChar},

		// 长度双上限
		{"超 rune 未超字节", strings.Repeat("a", 13), ReasonTooLong},
		// 用 CJK 扩展 B 的 𠀀(U+20000):NFKC 稳定、属 \p{L} 过白名单、utf8 占 4 字节。
		// 不能用 𝕬 这类数学字母 —— NFKC 会把它打平成 ASCII,字节数缩水,测不到字节上限。
		{"超字节未超 rune", strings.Repeat("𠀀", 12), ReasonTooManyBytes},
		{"贴边合法", strings.Repeat("字", 12), ReasonOK},

		// 保留前缀冒充
		{"Player_ 前缀冒充", "Player_12345", ReasonReserved},
		{"小写变体也拦", "player_12345", ReasonReserved},
		{"保留词子串", "官方客服", ReasonReserved},

		// 敏感词及其变体绕过
		{"直接命中", "badword", ReasonSensitive},
		{"大小写变体", "BadWord", ReasonSensitive},
		{"中文命中", "我是坏词啊", ReasonSensitive},
		{"分隔符绕过", "bad_word", ReasonSensitive},
		{"叠字绕过", "坏坏词", ReasonSensitive},

		// 正常名字
		{"普通中文名", "夜行的猫", ReasonOK},
		{"中英数混合", "Neko99", ReasonOK},
		{"词间单空格", "dark knight", ReasonOK},

		// emoji 默认拒(design doc §4:需策划显式决策,当前口径不放行)
		{"emoji 默认拒", "cat🐱", ReasonIllegalChar},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Check(tc.in)
			if got.Reason != tc.want {
				t.Fatalf("Check(%q) = %s (detail=%q), want %s",
					tc.in, got.Reason, got.Detail, tc.want)
			}
		})
	}
}

// 西里尔同形字冒充:折叠后必须与拉丁写法同键,DB 唯一键才能挡住。
func TestFold_CyrillicHomoglyphImpersonation(t *testing.T) {
	latin := Fold(Normalize("admin"))
	// 'а' 是西里尔 U+0430,'о' 是西里尔 U+043E —— 视觉与拉丁 a / o 无法区分。
	cyrillic := Fold(Normalize("аdmіn"))
	if latin != cyrillic {
		t.Fatalf("同形字未折叠到同一键: latin=%q cyrillic=%q", latin, cyrillic)
	}
}

// 校验对象与入库对象必须是同一个归一化串(design doc §2)。
func TestCheck_DisplayIsNormalized(t *testing.T) {
	c := testChecker(t, defaultRule())
	got := c.Check("  Ｎｅｋｏ   ９９  ")
	if !got.OK() {
		t.Fatalf("期望通过,实际 %s", got.Reason)
	}
	if got.Display != "Neko 99" {
		t.Fatalf("Display 未归一化: %q", got.Display)
	}
	if got.Fold != "neko 99" {
		t.Fatalf("Fold 不符预期: %q", got.Fold)
	}
	// 再次校验入库串必须仍然通过(幂等),否则说明归一化不是不动点。
	if again := c.Check(got.Display); !again.OK() || again.Display != got.Display {
		t.Fatalf("归一化不是不动点: %q → %q (%s)", got.Display, again.Display, again.Reason)
	}
}

// 词库不可用必须 fail-closed(§9.22),绝不能当成"没命中"放行。
func TestCheck_FailClosedWhenLexiconMissing(t *testing.T) {
	c := NewChecker(defaultRule()) // 刻意不 SetLexicon
	got := c.Check("完全正常的名字")
	if got.Reason != ReasonUnavailable {
		t.Fatalf("词库缺失时必须 fail-closed,实际 %s", got.Reason)
	}
	if got.OK() {
		t.Fatal("词库缺失时 OK() 必须为 false")
	}
}

// 保留前缀必须读配置,不得硬编码:换掉配置后旧前缀应放行、新前缀应拦。
func TestCheck_ReservedPrefixFollowsConfig(t *testing.T) {
	rule := defaultRule()
	rule.ReservedPrefixes = []string{"Hero_"}
	c := testChecker(t, rule)

	if got := c.Check("Player_1"); got.Reason == ReasonReserved {
		t.Fatal("配置已改,旧前缀不该再被拦(说明硬编码了字面量)")
	}
	if got := c.Check("Hero_1"); got.Reason != ReasonReserved {
		t.Fatalf("新配置前缀未拦: %s", got.Reason)
	}
}

// ── 匹配器 ────────────────────────────────────────────────────────────────

func TestMatcher_OverlappingAndSuffixHits(t *testing.T) {
	m, err := BuildMatcher([]string{"abc", "bc", "cd"})
	if err != nil {
		t.Fatal(err)
	}
	spans := m.Match([]rune("abcd"))
	// 期望三段:abc[0,3)、bc[1,3)、cd[2,4)。后缀命中靠 output 链枚举,漏了说明 fail 链有问题。
	want := map[Span]bool{{0, 3}: true, {1, 3}: true, {2, 4}: true}
	if len(spans) != len(want) {
		t.Fatalf("命中数 %d,期望 %d: %+v", len(spans), len(want), spans)
	}
	for _, s := range spans {
		if !want[s.Span] {
			t.Fatalf("意外命中 %+v", s)
		}
		if !s.LatinOnly {
			t.Fatalf("纯拉丁词条应标 LatinOnly: %+v", s)
		}
	}
}

func TestMatcher_RejectsEmptyPatternSet(t *testing.T) {
	if _, err := BuildMatcher([]string{"", ""}); err == nil {
		t.Fatal("空词表必须拒绝构建")
	}
}

// 打星必须打在**原串**上,且插桩绕过要整段盖住。
func TestMask_SpansMapBackToOriginal(t *testing.T) {
	c := testChecker(t, defaultRule())

	got, hits, ok := c.Mask("你 bad_word 了", '*')
	if !ok {
		t.Fatal("词库可用时 Mask 必须返回 ok")
	}
	if hits == 0 {
		t.Fatal("未命中,分隔符绕过没被破")
	}
	if strings.Contains(got, "bad") || strings.Contains(got, "word") {
		t.Fatalf("原词残留: %q", got)
	}
	if len([]rune(got)) != len([]rune("你 bad_word 了")) {
		t.Fatalf("打星必须等长: %q", got)
	}
	if !strings.HasPrefix(got, "你 ") || !strings.HasSuffix(got, " 了") {
		t.Fatalf("命中区间外的字符被误伤: %q", got)
	}
}

func TestMask_FailClosedSignal(t *testing.T) {
	c := NewChecker(defaultRule())
	got, hits, ok := c.Mask("随便什么话", '*')
	if ok {
		t.Fatal("词库缺失时 Mask 必须返回 ok=false,由调用方决定拒发还是降级")
	}
	if hits != 0 || got != "随便什么话" {
		t.Fatalf("词库缺失时不得改写文本: %q hits=%d", got, hits)
	}
}

func TestMask_NoHitReturnsOriginal(t *testing.T) {
	c := testChecker(t, defaultRule())
	in := "今天天气不错"
	got, hits, ok := c.Mask(in, '*')
	if !ok || hits != 0 || got != in {
		t.Fatalf("无命中时应原样返回: %q hits=%d ok=%v", got, hits, ok)
	}
}

// ── 归一化 ────────────────────────────────────────────────────────────────

func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"  abc  ", "abc"},
		{"a   b", "a b"},
		{"ａｂｃ", "abc"}, // 全角 → 半角
		{"ﬁ", "fi"},    // 兼容合字
		{"a　b", "a b"}, // 表意空格
		{"\t a \n b \t", "a b"},
	}
	for _, tc := range cases {
		if got := Normalize(tc.in); got != tc.want {
			t.Fatalf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFold_DigitLetterConfusables(t *testing.T) {
	// design doc §5 点名要折 0/O 与 1/l/I。副作用(abc123 与 abcl23 同键)是刻意接受的。
	if Fold("O0") != Fold("oo") {
		t.Fatalf("0 未折叠到 o: %q vs %q", Fold("O0"), Fold("oo"))
	}
	if Fold("I1") != Fold("il") {
		t.Fatalf("1 未折叠到 l: %q vs %q", Fold("I1"), Fold("il"))
	}
}
