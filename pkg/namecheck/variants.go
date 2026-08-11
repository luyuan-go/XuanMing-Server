package namecheck

import "unicode"

// variant 一个参与匹配的输入变体:变形后的 rune 串 + 每个 rune 回指原串的下标。
//
// 有了 orig 映射,命中区间才能映射回**原始文本**去打星。否则在变形串上算出的
// [start,end) 拿去截原串就是错位的(变形过程删过字符)。
type variant struct {
	runes []rune
	orig  []int // orig[i] = runes[i] 在原串中的 rune 下标
}

// buildVariants 产出敏感词匹配用的三种输入形态(design doc §7:
// 「匹配前先去掉分隔符与重复字符,否则 `傻*逼`、`傻傻逼` 可绕过」)。
//
//	v0 原样        —— 命中词库里本身含重复字/符号的词(如 `嘻嘻`)
//	v1 去非字母数字 —— 破 `傻*逼` / `傻 逼` / `傻-逼` 这类插桩绕过
//	v2 v1 再折叠连续重复 rune —— 破 `傻傻逼` 这类叠字绕过
//
// 三个变体共用同一棵自动机各扫一遍。词库侧**不做**任何变形:变形只施加于输入,
// 这样 v0 保住"词本身含叠字"的场景,v2 保住"输入含叠字"的场景,两头都不漏。
//
// 三遍扫描的代价是 O(3n),n 是文本长度(昵称 ≤32 rune、聊天 ≤256 rune),
// 与词数无关,可忽略。
func buildVariants(runes []rune) []variant {
	v0 := variant{runes: runes, orig: make([]int, len(runes))}
	for i := range runes {
		v0.orig[i] = i
	}

	v1 := variant{runes: make([]rune, 0, len(runes)), orig: make([]int, 0, len(runes))}
	for i, r := range runes {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			v1.runes = append(v1.runes, r)
			v1.orig = append(v1.orig, i)
		}
	}

	v2 := variant{runes: make([]rune, 0, len(v1.runes)), orig: make([]int, 0, len(v1.runes))}
	for i, r := range v1.runes {
		if i > 0 && v1.runes[i-1] == r {
			continue
		}
		v2.runes = append(v2.runes, r)
		v2.orig = append(v2.orig, v1.orig[i])
	}

	// v1 / v2 与 v0 等长时是同一个串,跳过重复扫描。
	out := make([]variant, 0, 3)
	out = append(out, v0)
	if len(v1.runes) != len(v0.runes) {
		out = append(out, v1)
	}
	if len(v2.runes) != len(v1.runes) {
		out = append(out, v2)
	}
	return out
}

// origSpan 把变体上的命中区间映射回原串 rune 区间 [start, end)。
//
// 注意映射回去的区间可能**比词本身长**:`傻 * 逼` 命中的是三 rune 的词,但原串上
// 覆盖五个 rune。打星要按原串区间整段打,否则会留下 `傻 * 逼` → `* * *` 之外的残字。
func (v variant) origSpan(s Span) Span {
	if s.Start < 0 || s.End > len(v.orig) || s.Start >= s.End {
		return Span{}
	}
	return Span{Start: v.orig[s.Start], End: v.orig[s.End-1] + 1}
}
