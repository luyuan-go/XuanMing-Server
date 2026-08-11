package namecheck

import (
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// Reason 拒绝原因。**每一种都必须能映射到不同错误码** —— design doc §8:前端要给出准确
// 提示(太长 / 非法字符 / 命中保留词 / 命中敏感词 / 已被占用),一律返回 ErrInvalidArg
// 等于没做。占用(uk 冲突)不在本包判定,由 DB 唯一键冲突映射(见 §5:先查再写是 TOCTOU)。
type Reason uint8

const (
	ReasonOK Reason = iota
	// ReasonUnavailable 词库未就绪。**必须 fail-closed 当拒绝处理**(§9.22):
	// 查询失败不得冒充"没命中"放行。调用方应映射到可重试的错误码,不是参数错误。
	ReasonUnavailable
	ReasonEmpty        // 归一化后为空串(全空白 / 全被剥掉)
	ReasonTooShort     // 短于 MinRunes
	ReasonTooLong      // 超 MaxRunes(业务上限,按 rune)
	ReasonTooManyBytes // 超 MaxBytes(DB 列上限,按 utf8 字节)
	ReasonIllegalChar  // 命中字符白名单之外的码点
	ReasonZalgo        // 连续组合符号超限
	ReasonReserved     // 命中保留前缀 / 保留词
	ReasonSensitive    // 命中敏感词库
)

// String 便于日志与测试断言。不面向玩家(玩家提示由各服务按错误码本地化)。
func (r Reason) String() string {
	switch r {
	case ReasonOK:
		return "ok"
	case ReasonUnavailable:
		return "lexicon_unavailable"
	case ReasonEmpty:
		return "empty"
	case ReasonTooShort:
		return "too_short"
	case ReasonTooLong:
		return "too_long"
	case ReasonTooManyBytes:
		return "too_many_bytes"
	case ReasonIllegalChar:
		return "illegal_char"
	case ReasonZalgo:
		return "zalgo"
	case ReasonReserved:
		return "reserved"
	case ReasonSensitive:
		return "sensitive"
	default:
		return "unknown"
	}
}

// Rule 校验参数。全部来自服务配置,**不在本包硬编码字面量** —— design doc §6:
// 保留前缀必须与生成默认昵称的那份配置同源,否则改配置后规则漂移,玩家又能冒充默认名。
type Rule struct {
	// MinRunes 最短 rune 数。默认 1。
	MinRunes int
	// MaxRunes 最长 rune 数(业务上限)。默认 32,与 player 的 max_nickname_len 同源。
	MaxRunes int
	// MaxBytes utf8 字节上限,**兜住 DB 列容量**。players.nickname 是 VARCHAR(64)
	// utf8mb4 = 最多 64 字符 / 256 字节;32 个 CJK rune 只有 96 字节,但 32 个
	// 四字节码点就是 128 字节 —— 两条上限缺一不可(design doc §3)。默认 192。
	MaxBytes int
	// MaxCombiningRun 允许的连续组合符号数,超出判 Zalgo。默认 2。
	MaxCombiningRun int
	// AllowSymbols 是否放行 \p{S}(emoji 属此类)。默认 false。
	// design doc §4 明确 emoji「需策划显式决策,不能默认放行也不能默认禁止」,
	// 因此这里是必填的配置项而不是包内常量;当前策划口径 = 不放行。
	AllowSymbols bool
	// ReservedPrefixes 保留前缀(折叠后比对)。必须包含 player 服务的
	// default_nickname_prefix(当前 "Player_"),否则玩家可自取默认名冒充他人。
	ReservedPrefixes []string
	// ReservedWords 保留词(折叠后按子串比对)。如 GM / 系统 / 官方 / 客服 / 管理员。
	ReservedWords []string
}

func (r Rule) withDefaults() Rule {
	if r.MinRunes <= 0 {
		r.MinRunes = 1
	}
	if r.MaxRunes <= 0 {
		r.MaxRunes = 32
	}
	if r.MaxBytes <= 0 {
		r.MaxBytes = 192
	}
	if r.MaxCombiningRun <= 0 {
		r.MaxCombiningRun = 2
	}
	return r
}

// Result 校验结论。Display / Fold 只在 Reason == ReasonOK 时有意义。
type Result struct {
	// Display 归一化后的展示串 —— **入库 nickname 列存这个**,不是玩家原始输入。
	Display string
	// Fold 折叠串 —— 入库 nickname_normalized 列,唯一键建在它上面。
	Fold string
	// Reason 拒绝原因,ReasonOK = 通过。
	Reason Reason
	// Detail 供服务端日志定位(命中了哪条保留词等),**不回传玩家**:
	// 把命中的敏感词原样回显给玩家等于送一份词库探针。
	Detail string
}

// OK 是否通过。
func (r Result) OK() bool { return r.Reason == ReasonOK }

// Checker 名字校验器。词库指针原子替换,构造后可并发使用。
type Checker struct {
	rule Rule
	lex  atomic.Pointer[Lexicon]
}

// NewChecker 构造。词库需另行 SetLexicon 注入;未注入前 Check 一律返回
// ReasonUnavailable(fail-closed),不会静默放行。
func NewChecker(rule Rule) *Checker {
	return &Checker{rule: rule.withDefaults()}
}

// SetLexicon 原子换上新词库(热更入口)。传 nil 会让后续 Check 立即 fail-closed。
func (c *Checker) SetLexicon(l *Lexicon) { c.lex.Store(l) }

// Lexicon 取当前词库(可能为 nil)。
func (c *Checker) Lexicon() *Lexicon { return c.lex.Load() }

// Check 执行五层校验。顺序不可颠倒:归一化必须最先(否则全角绕过白名单),
// 敏感词必须最后(前面几层已经把畸形输入挡掉,词库只面对合法字符集的串)。
func (c *Checker) Check(raw string) Result {
	display := Normalize(raw)
	if display == "" {
		return Result{Reason: ReasonEmpty}
	}

	// ① 长度双上限:rune 管业务口径,字节管 DB 列容量。
	runes := []rune(display)
	if len(runes) < c.rule.MinRunes {
		return Result{Reason: ReasonTooShort}
	}
	if len(runes) > c.rule.MaxRunes {
		return Result{Reason: ReasonTooLong}
	}
	if len(display) > c.rule.MaxBytes {
		// 注意这里量的是归一化后的字节数 —— 入库的就是它。
		return Result{Reason: ReasonTooManyBytes}
	}

	// ② 字符白名单 + Zalgo。空格只允许出现在词间(Normalize 已保证不在首尾、不连续)。
	combiningRun := 0
	for _, r := range runes {
		if isCombining(r) {
			combiningRun++
			if combiningRun > c.rule.MaxCombiningRun {
				return Result{Reason: ReasonZalgo}
			}
			continue
		}
		combiningRun = 0
		if !classify(r, c.rule.AllowSymbols) {
			return Result{Reason: ReasonIllegalChar, Detail: "rune U+" + upperHex(r)}
		}
	}

	fold := Fold(display)

	// ③ 保留前缀 / 保留词(在折叠串上比对,顺带挡住全角与同形字变体)。
	for _, p := range c.rule.ReservedPrefixes {
		if p == "" {
			continue
		}
		if strings.HasPrefix(fold, Fold(Normalize(p))) {
			return Result{Reason: ReasonReserved, Detail: "prefix " + p}
		}
	}
	for _, w := range c.rule.ReservedWords {
		if w == "" {
			continue
		}
		if strings.Contains(fold, Fold(Normalize(w))) {
			return Result{Reason: ReasonReserved, Detail: "word " + w}
		}
	}

	// ④ 敏感词。词库不可用一律 fail-closed(§9.22),绝不当成"没命中"放行。
	lex := c.lex.Load()
	if lex == nil || lex.matcher == nil {
		return Result{Reason: ReasonUnavailable}
	}
	foldRunes := []rune(fold)
	if spans := scanHits(lex, foldRunes); len(spans) > 0 {
		// Detail 记命中片段供**服务端日志**定位误杀(误杀修正流程见
		// configtable/lexicon/source/local/allow.txt)。
		// ⚠️ 绝不能回传玩家:把命中词原样回显等于送一份词库探针。
		s := spans[0]
		return Result{
			Reason: ReasonSensitive,
			Detail: "hit=" + string(foldRunes[s.Start:s.End]),
		}
	}

	return Result{Display: display, Fold: fold, Reason: ReasonOK}
}

// Mask 把文本里命中的敏感词整段替换为等长 mask 字符,返回替换后文本与命中段数。
//
// 供 chat 使用(替换 chat.go 里那个按配置 []string 线性遍历的玩具版)。与 Check 不同,
// 聊天是**打星不是拒绝**,因此:
//   - 不做字符白名单 / 长度判定(那是名字的事,聊天有自己的 MaxContentLen);
//   - 词库不可用时返回 ok=false,由调用方按 §9.22 决定拒发还是降级,本包不替它做主。
//
// 匹配在折叠串上进行、打星打在**原始文本**上,因此 `傻 * 逼` 会被整段盖掉而不是只盖中间。
func (c *Checker) Mask(text string, mask rune) (string, int, bool) {
	lex := c.lex.Load()
	if lex == nil || lex.matcher == nil {
		return text, 0, false
	}
	orig := []rune(text)
	// 折叠必须逐 rune 与原串对齐才能映射回去。Normalize 会改变 rune 数(折叠空白),
	// 所以聊天路径只做逐 rune 的 Fold(不做 Normalize 的空白折叠),保持一一对应。
	folded := make([]rune, len(orig))
	for i, r := range orig {
		folded[i] = foldRune(r)
	}

	spans := scanHits(lex, folded)
	if len(spans) == 0 {
		return text, 0, true
	}
	hide := make([]bool, len(orig))
	for _, s := range spans {
		for i := s.Start; i < s.End && i < len(hide); i++ {
			hide[i] = true
		}
	}
	hits := len(spans)
	out := make([]rune, len(orig))
	for i, r := range orig {
		if hide[i] {
			out[i] = mask
			continue
		}
		out[i] = r
	}
	return string(out), hits, true
}

// scanHits 在折叠串上跑三个匹配变体,把命中映射回 folded 的 rune 下标,
// 并对**纯拉丁**命中施加词边界规则。Check 与 Mask 共用,保证两条路径判定完全一致。
//
// 边界规则只施加于拉丁词:中文没有词边界,子串匹配才是正确的;拉丁不加边界就会出现
// `anal` 命中 `Analysis`、`da` 命中 `DarkKnight` 这类误杀(实测已发生)。
//
// 边界判定落在**原串下标**上而不是变体串上:`b.a.d.w.o.r.d` 经 v1 去符号后命中,
// 映射回原串是 [0,13),两端无字母数字 → 仍算命中,插桩绕过照破。
func scanHits(lex *Lexicon, folded []rune) []Span {
	var out []Span
	for _, v := range buildVariants(folded) {
		for _, h := range lex.matcher.Match(v.runes) {
			os := v.origSpan(h.Span)
			if os.End <= os.Start {
				continue
			}
			if h.LatinOnly && !hasWordBoundary(folded, os) {
				continue
			}
			out = append(out, os)
		}
	}
	return out
}

// hasWordBoundary 命中区间两侧都不是字母 / 数字才算独立成词。
func hasWordBoundary(runes []rune, s Span) bool {
	if s.Start > 0 && isWordRune(runes[s.Start-1]) {
		return false
	}
	if s.End < len(runes) && isWordRune(runes[s.End]) {
		return false
	}
	return true
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

// foldRune 单 rune 折叠(小写 + 同形字),供需要保持 rune 一一对应的路径使用。
// 与 Fold 的差别只在于不经 strings.ToLower 的整串处理 —— 对本包关心的码点范围两者等价。
func foldRune(r rune) rune {
	lower := toLowerRune(r)
	if m, ok := confusables[lower]; ok {
		return m
	}
	return lower
}

func toLowerRune(r rune) rune {
	if r < utf8.RuneSelf {
		if 'A' <= r && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}
	return []rune(strings.ToLower(string(r)))[0]
}

const hexDigits = "0123456789ABCDEF"

func upperHex(r rune) string {
	if r == 0 {
		return "0000"
	}
	var buf [8]byte
	i := len(buf)
	for v := uint32(r); v > 0; v >>= 4 {
		i--
		buf[i] = hexDigits[v&0xF]
	}
	s := string(buf[i:])
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}
