// Package namecheck 玩家可见名字(角色昵称 / 公会名 / 未来宠物名)的服务端权威校验。
//
// 契约见 docs/design/player-name-validation.md,五层顺序**不可颠倒**:
//
//	归一化 → 长度双上限 → 字符白名单 → 保留词 → 敏感词
//
// 本包是纯函数 + 只读内存索引,不引服务依赖(不碰 DB / Redis / gRPC),
// 可被 player(角色昵称)/ login(创角取名)/ guild(公会名)/ chat(发言过滤)复用。
//
// 为什么不复用 pkg/configtable 的加载器:词库源不是策划维护的 xlsx(是 GitHub 上游快照),
// 发布节奏与策划一键导表解耦;且 login / chat 目前都不 import configtable,不该为读一张
// 词表把全部生成表代码编译进去。词库自带同契约的 manifest(version + sha256 + staging +
// 加载成功才切换),见 lexicon.go —— CLAUDE.md §9.15 要求的是这套纪律,不是同一个目录。
package namecheck

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalize 产出**入库与校验共用**的展示串(design doc §2)。
//
// 顺序:NFKC → 去首尾空白 → 把任意 Unicode 空白折叠成单个 ASCII 空格。
// 保留大小写(展示用);唯一键用 Fold 另算。
//
// NFKC 必须在最前:它把全角 `Ａ` 打平成 `A`、把兼容字符合并。跳过这步,后面的字符白名单
// 与敏感词匹配都能被全角 / 兼容字符绕过。
//
// ⚠️ 调用方必须把本函数的返回值同时用于校验和入库。校验归一化串、却存原始串 = 校验等于没做。
func Normalize(raw string) string {
	s := norm.NFKC.String(raw)
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	wrote := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			// 首部空白直接丢;中间空白记一个待写标记,连续多个只落一个空格;
			// 尾部空白因为「只在下一个非空白字符前落」而自然被丢弃。
			if wrote {
				pendingSpace = true
			}
			continue
		}
		if pendingSpace {
			b.WriteByte(' ')
			pendingSpace = false
		}
		b.WriteRune(r)
		wrote = true
	}
	return b.String()
}

// Fold 产出**唯一键 / 保留词比对**用的折叠串(design doc §5)。
//
// 在 Normalize 之上再做:小写化 + 同形字折叠(西里尔 / 希腊字母 → 拉丁,0→o,1→l)。
// 目的是防仿冒而不只是防重复:MySQL 的 utf8mb4_0900_ai_ci 只做到大小写 / 重音不敏感,
// **挡不住西里尔 `а` 冒充拉丁 `a`**。
//
// ⚠️ 副作用是真实存在的、且是刻意接受的代价:`abc123` 与 `abcl23`、`Player_1` 与 `Player_l`
// 折叠后同键,后取名的一方会撞唯一键被拒。这是"宁可拒掉一个合法名,也不放过一个冒充名"的
// 取舍(design doc §5 明确要求折叠 0/O、1/l/I)。放宽须先改 design doc 再改这里。
func Fold(normalized string) string {
	var b strings.Builder
	b.Grow(len(normalized))
	for _, r := range strings.ToLower(normalized) {
		if m, ok := confusables[r]; ok {
			b.WriteRune(m)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// confusables 同形字 → 拉丁小写映射。
//
// 只收**视觉上与拉丁字母难以区分**的码点,不做语义转换(不含繁简转换 —— 那属于敏感词
// 匹配层的变体,见 matcher.go,不能混进唯一键,否则「张三」与「張三」会撞名)。
// 全角 / 半角不在此表:NFKC 已经打平。
var confusables = map[rune]rune{
	// 数字 ↔ 字母(design doc §5 点名)
	'0': 'o',
	'1': 'l',
	// 西里尔小写 → 拉丁
	'а': 'a', 'в': 'b', 'с': 'c', 'ԁ': 'd', 'е': 'e', 'ѕ': 's',
	'һ': 'h', 'і': 'i', 'ј': 'j', 'к': 'k', 'ӏ': 'l', 'м': 'm',
	'н': 'h', 'о': 'o', 'р': 'p', 'т': 't', 'у': 'y', 'х': 'x',
	'ү': 'y', 'ғ': 'f', 'ԛ': 'q', 'ѡ': 'w', 'ѵ': 'v', 'ᴦ': 'r',
	// 希腊小写 → 拉丁
	'α': 'a', 'β': 'b', 'ε': 'e', 'ζ': 'z', 'η': 'n', 'ι': 'i',
	'κ': 'k', 'ν': 'v', 'ο': 'o', 'ρ': 'p', 'τ': 't', 'υ': 'u',
	'χ': 'x', 'γ': 'y', 'μ': 'u', 'σ': 'o', 'ϲ': 'c', 'ϳ': 'j',
	// 其它常见冒充
	'ⅰ': 'i', 'ⅴ': 'v', 'ⅹ': 'x', 'ǀ': 'l', 'ɩ': 'l', 'ɑ': 'a',
	'ɡ': 'g', 'ɪ': 'i', 'ʏ': 'y', 'ʙ': 'b', 'ʜ': 'h', 'ᴏ': 'o',
}

// classify 判定单个 rune 是否落在名字字符白名单内(design doc §4)。
//
// 用白名单不用黑名单 —— 黑名单永远漏。允许:
//   - \p{L} 字母(含 CJK、日文假名、韩文)
//   - \p{N} 数字
//   - `_` 下划线
//   - 词间单个空格(Normalize 已把连续空白折叠成一个,首尾已剥)
//
// 显式落在白名单外因而被拒的类别(注释说明理由,便于 review 时确认没漏):
//   - \p{Cc} 控制字符:换行 / NUL 污染日志与协议
//   - \p{Cf} 格式字符:零宽空格、ZWJ、RTL override(U+202E)可伪造显示顺序
//   - \p{Co} 私用区:各端渲染成豆腐块或他人图标,跨端不一致
//   - 未分配码点:跨端渲染不一致(白名单天然拒绝,无需单独判定)
//   - emoji(\p{So} 等):默认拒,由 Rule.AllowSymbols 显式放开
func classify(r rune, allowSymbols bool) bool {
	switch {
	case r == ' ' || r == '_':
		return true
	case unicode.IsLetter(r) || unicode.IsDigit(r):
		return true
	case allowSymbols && unicode.IsSymbol(r):
		return true
	default:
		return false
	}
}

// isCombining 判断是否组合符号(Zalgo 检测用)。
// 连续组合符号超过 Rule.MaxCombiningRun 即判非法 —— Zalgo 文本会撑爆 UI 布局。
// 变体选择符(U+FE00–FE0F 等)也归在 Mn,一并被这条与白名单拦掉。
func isCombining(r rune) bool {
	return unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r)
}
