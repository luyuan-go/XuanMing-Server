package namecheck

import (
	"fmt"
	"sort"
	"unicode"
)

// Matcher 是词库的只读匹配索引:rune 粒度的 Aho-Corasick 自动机。
//
// 为什么是 AC 而不是逐词 strings.Contains:词库合并后是十万量级,逐词扫描是
// O(词数 × 文本长),一条聊天要跑十几万次子串搜索。AC 一次扫描 O(文本长 + 命中数),
// 与词数无关 —— 这正是替换 chat 里那个 []string 线性遍历版本(chat.go maskSensitive)的理由。
//
// 结构用扁平数组而非 per-node map:十万词展开约六十万节点,每节点一个 map 会是几百 MB;
// 扁平化后节点表 + 孩子表合计约 20MB 量级(实测数字见 tools/lexicon-import 输出)。
//
// 构造完成后只读,可被多 goroutine 并发使用;热更走 lexicon.go 的整体指针切换。
type Matcher struct {
	// 节点表。索引 0 恒为根。
	fail       []int32 // 失配链
	patLen     []int32 // >0 表示本节点是某个词的终点,值 = 该词 rune 长度
	outLink    []int32 // 最近的「是终点」的 fail 祖先,-1 = 无;用于枚举后缀命中
	childStart []int32 // 孩子在 childCh / childIdx 中的起始下标
	childEnd   []int32 // 孩子结束下标(不含)
	// latinOnly 该终点词是否**不含 CJK**。决定命中判定用子串还是词边界:
	//   - 中文没有词边界,子串匹配是唯一正确做法(「傻逼」出现在任何位置都算);
	//   - 拉丁有词边界,子串匹配会制造经典 Scunthorpe 误杀
	//     (`anal` 命中 `Analysis`、`ass` 命中 `Classic`)。
	// 实测依据:合并库里 `da` / `ma` / `js` / `die` 这类短拉丁词条会毙掉
	// DarkKnight / Mage / Soldier 等完全正常的昵称。
	latinOnly []bool

	// 孩子表(按 rune 升序,二分查找)。
	childCh  []rune
	childIdx []int32

	patterns int
}

// Span 一次命中在**被匹配串**上的 rune 区间 [Start, End)。
type Span struct {
	Start int
	End   int
}

// childKey 建树期的一条边。用**一个全局 map** 而不是每节点一个 map:
// 六十万条边放一个 map 约几十 MB,拆成六十万个 map 光 map 头就上百 MB。
type childKey struct {
	parent int32
	ch     rune
}

// BuildMatcher 用已归一化的词表建自动机。
//
// patterns 必须是调用方**先经 Normalize + Fold 处理过**的串 —— 词库入库形态与运行时
// 匹配形态必须同源,否则全角 / 大小写变体在两侧算出不同结果,匹配等于没做。
// 空串与重复词会被静默跳过(词库合并天然带重复,不算错误)。
func BuildMatcher(patterns []string) (*Matcher, error) {
	// ── 建树 ──────────────────────────────────────────────────────────────
	edges := make(map[childKey]int32, len(patterns)*3)
	// 节点 0 = 根。先用可增长切片记录每个节点的终止词长与脚本属性。
	patLen := []int32{0}
	latinOnly := []bool{false}
	nodeCount := int32(1)
	distinct := 0

	for _, p := range patterns {
		rs := []rune(p)
		if len(rs) == 0 {
			continue
		}
		cur := int32(0)
		for _, r := range rs {
			k := childKey{parent: cur, ch: r}
			next, ok := edges[k]
			if !ok {
				next = nodeCount
				nodeCount++
				edges[k] = next
				patLen = append(patLen, 0)
				latinOnly = append(latinOnly, false)
			}
			cur = next
		}
		if patLen[cur] == 0 {
			distinct++
		}
		// 同一节点被不同词命中只可能是同一个词(路径唯一),直接覆盖即可。
		patLen[cur] = int32(len(rs))
		latinOnly[cur] = !containsCJK(rs)
	}
	if distinct == 0 {
		return nil, fmt.Errorf("词表为空(去重后 0 条),拒绝构建空匹配器")
	}

	m := &Matcher{
		fail:       make([]int32, nodeCount),
		patLen:     patLen,
		latinOnly:  latinOnly,
		outLink:    make([]int32, nodeCount),
		childStart: make([]int32, nodeCount),
		childEnd:   make([]int32, nodeCount),
		childCh:    make([]rune, 0, len(edges)),
		childIdx:   make([]int32, 0, len(edges)),
		patterns:   distinct,
	}

	// ── 边表扁平化:按 (parent, ch) 排序后连续存放,每节点记 [start,end) ──
	type edge struct {
		parent int32
		ch     rune
		child  int32
	}
	flat := make([]edge, 0, len(edges))
	for k, v := range edges {
		flat = append(flat, edge{parent: k.parent, ch: k.ch, child: v})
	}
	edges = nil // 尽早交还建树期的临时内存
	sort.Slice(flat, func(i, j int) bool {
		if flat[i].parent != flat[j].parent {
			return flat[i].parent < flat[j].parent
		}
		return flat[i].ch < flat[j].ch
	})
	for i := range m.childStart {
		m.childStart[i] = 0
		m.childEnd[i] = 0
	}
	for i := 0; i < len(flat); {
		p := flat[i].parent
		start := int32(len(m.childCh))
		j := i
		for ; j < len(flat) && flat[j].parent == p; j++ {
			m.childCh = append(m.childCh, flat[j].ch)
			m.childIdx = append(m.childIdx, flat[j].child)
		}
		m.childStart[p] = start
		m.childEnd[p] = int32(len(m.childCh))
		i = j
	}
	flat = nil

	// ── BFS 建 fail 链与 output 链 ────────────────────────────────────────
	m.fail[0] = 0
	m.outLink[0] = -1
	queue := make([]int32, 0, nodeCount)
	for i := m.childStart[0]; i < m.childEnd[0]; i++ {
		c := m.childIdx[i]
		m.fail[c] = 0
		m.outLink[c] = -1
		queue = append(queue, c)
	}
	for head := 0; head < len(queue); head++ {
		cur := queue[head]
		for i := m.childStart[cur]; i < m.childEnd[cur]; i++ {
			ch, child := m.childCh[i], m.childIdx[i]
			// fail(child) = 沿 fail(cur) 上溯,第一个有 ch 边的节点的那条边的目标。
			f := m.fail[cur]
			for {
				if nxt, ok := m.child(f, ch); ok {
					m.fail[child] = nxt
					break
				}
				if f == 0 {
					m.fail[child] = 0
					break
				}
				f = m.fail[f]
			}
			// output 链:fail 目标本身是终点就指它,否则继承它的 outLink。
			fl := m.fail[child]
			if m.patLen[fl] > 0 {
				m.outLink[child] = fl
			} else {
				m.outLink[child] = m.outLink[fl]
			}
			queue = append(queue, child)
		}
	}
	return m, nil
}

// child 在 node 的孩子表里二分查找字符 ch。
func (m *Matcher) child(node int32, ch rune) (int32, bool) {
	lo, hi := m.childStart[node], m.childEnd[node]
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case m.childCh[mid] < ch:
			lo = mid + 1
		case m.childCh[mid] > ch:
			hi = mid
		default:
			return m.childIdx[mid], true
		}
	}
	return 0, false
}

// step 走一步(带 fail 回退),返回新状态。
func (m *Matcher) step(state int32, r rune) int32 {
	for {
		if nxt, ok := m.child(state, r); ok {
			return nxt
		}
		if state == 0 {
			return 0
		}
		state = m.fail[state]
	}
}

// Hit 一次命中:区间 + 该词是否为纯拉丁(决定调用方是否要求词边界)。
type Hit struct {
	Span
	// LatinOnly 命中词不含 CJK。调用方**必须**对这类命中在原串上校验词边界,
	// 否则 `anal` 会命中 `Analysis`。CJK 命中不做边界校验(中文无词边界)。
	LatinOnly bool
}

// Match 枚举全部命中(含后缀重叠命中)。
// 返回区间是 runes 上的下标,调用方负责映射回原串并按 LatinOnly 施加边界规则。
func (m *Matcher) Match(runes []rune) []Hit {
	var hits []Hit
	state := int32(0)
	for i, r := range runes {
		state = m.step(state, r)
		if m.patLen[state] > 0 {
			hits = append(hits, Hit{
				Span:      Span{Start: i + 1 - int(m.patLen[state]), End: i + 1},
				LatinOnly: m.latinOnly[state],
			})
		}
		for o := m.outLink[state]; o >= 0; o = m.outLink[o] {
			hits = append(hits, Hit{
				Span:      Span{Start: i + 1 - int(m.patLen[o]), End: i + 1},
				LatinOnly: m.latinOnly[o],
			})
		}
	}
	return hits
}

// containsCJK 判断词里是否含中日韩表意文字 / 假名 / 谚文。
// 含 = 按子串匹配;不含 = 按词边界匹配。
func containsCJK(rs []rune) bool {
	for _, r := range rs {
		if unicode.Is(unicode.Han, r) ||
			unicode.Is(unicode.Hiragana, r) ||
			unicode.Is(unicode.Katakana, r) ||
			unicode.Is(unicode.Hangul, r) {
			return true
		}
	}
	return false
}

// Stats 返回索引规模,供启动日志与容量核对(词数 / 节点数 / 边数)。
func (m *Matcher) Stats() (patterns, nodes, edges int) {
	return m.patterns, len(m.fail), len(m.childCh)
}
