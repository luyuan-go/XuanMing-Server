package namecheck

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LexiconManifestFile 词库批次清单文件名。
const LexiconManifestFile = "manifest.json"

// LexiconDataFile 词库数据文件名。清单里的 file 字段必须恰为此值(防路径逃逸)。
const LexiconDataFile = "lexicon.json"

// LexiconManifest 词库批次清单。
//
// 与 pkg/configtable.Manifest **同契约不同目录**:version 单调递增防回退、sha256 校验防
// 截断篡改、staging 目录 + 全部校验通过才原子切换、失败保留旧批次(CLAUDE.md §9.15)。
// 刻意不复用 configtable 的结构体:词库源是 GitHub 上游快照而非策划 xlsx,发布节奏与
// 「策划一键导表」解耦;且 login / chat 都不 import configtable,不该为读一张词表把全部
// 生成表代码编译进去。为此付出的代价是这三十行结构定义的重复,已按 §15.4 举证。
type LexiconManifest struct {
	Version       uint64 `json:"version"`
	GeneratedAtMs uint64 `json:"generated_at_ms"`
	Generator     string `json:"generator"`
	// SourceRev 上游溯源,形如 "houbb@fe6fc29 konsheng@5a8da94"。必填,空值拒绝加载。
	SourceRev string `json:"source_rev"`
	File      string `json:"file"`
	Checksum  string `json:"checksum"` // "sha256:<hex64 小写>"
	Words     uint32 `json:"words"`    // 去重后词条数,加载后断言一致(防截断)
}

// LexiconFile 词库数据文件结构。按分类分组存放而不是逐词一个对象:
// 十万词条逐个 {"w":...,"c":...} 会让文件从 ~1.5MB 涨到 ~4MB,而分类信息
// 每组只需存一次。每个词在**全库只出现一次**(合并期已按分类优先级去重)。
type LexiconFile struct {
	Groups []LexiconGroup `json:"groups"`
}

// LexiconGroup 一个分类下的词。Category 形如 "konsheng/色情词库" / "houbb/dict"。
type LexiconGroup struct {
	Category string   `json:"category"`
	Words    []string `json:"words"`
}

// Lexicon 已加载并建好索引的词库(只读)。
type Lexicon struct {
	version   uint64
	sourceRev string
	words     int
	// categories 词条数按分类统计,供启动日志与运维核对词库构成。
	categories map[string]int
	matcher    *Matcher
}

// Version 批次版本号。
func (l *Lexicon) Version() uint64 { return l.version }

// SourceRev 上游溯源串。
func (l *Lexicon) SourceRev() string { return l.sourceRev }

// Words 去重后词条总数。
func (l *Lexicon) Words() int { return l.words }

// Categories 各分类词条数(副本,调用方可自由持有)。
func (l *Lexicon) Categories() map[string]int {
	out := make(map[string]int, len(l.categories))
	for k, v := range l.categories {
		out[k] = v
	}
	return out
}

// Stats 索引规模,供启动日志:词数 / 节点数 / 边数。
func (l *Lexicon) Stats() (words, nodes, edges int) { return l.matcher.Stats() }

// ChecksumOf 按清单口径算内容哈希("sha256:<hex64 小写>")。
// 导入器产出与加载器校验共用本函数,避免两侧口径漂移。
func ChecksumOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Profile 匹配档:声明哪些分类参与匹配。
//
// 为什么需要分档(实测证据,不是预设性设计):上游两批词库合并后 106,759 条,拿 135 个
// **正常游戏用词**(职业 / 装备 / 称号 / 常见人名)探测,误杀 14 条(10.4%),
// 包括「希望」「头盔」「盾牌」「匕首」「女王」「骑士」。按分类拆开看,误杀 100% 来自
// 四个「大而全的通用审核表」:
//
//	houbb/dict(56175)、konsheng/网易前端过滤敏感词库(7322)、
//	konsheng/GFW补充词库(6157)、konsheng/零时-Tencent(33836)
//
// 而 13 个**精编定向分类**(色情 / 反动 / 暴恐 / 政治 / 广告 / 贪腐 / 涉枪涉爆 …,合计 3269 条)
// 误杀 0。原因是前者本质是电商与通用内容审核词表,收了大量「语境敏感但本身正常」的词。
//
// 因此两个调用场景必须用不同档,**这不是可选优化**:
//   - 昵称(拒绝语义):只用精编分类。误拒会直接挡住玩家创角,精确率优先于召回率。
//   - 聊天(打星语义):用全量。误打星只是难看,漏放才是事故。
//
// Include 与 Exclude 必须**共同覆盖文件里的全部分类**,且不得出现文件里没有的分类:
// 上游快照新增一个词库文件时,两边都不匹配 → 加载直接失败,强制人工决定它归哪一档。
// 不这么做的话,新分类会被静默排除(昵称档漏放)或静默纳入(聊天档突然误杀),
// 而这两种漂移都不会有任何报错(§9.22 fail-closed)。
type Profile struct {
	Include []string
	Exclude []string
}

// RejectProfile 昵称拒绝档的默认分类划分(对应 source_rev
// "houbb@fe6fc292 konsheng@5a8da94c" 那批快照的实测结论)。
//
// 服务配置可覆盖;但上游快照一换,分类清单必须重新核对并重跑误杀探测,
// 不许直接照抄(见 pkg/namecheck/realdist_test.go 的误杀用例)。
func RejectProfile() Profile {
	return Profile{
		Include: []string{
			"local/deny",
			"konsheng/COVID-19词库",
			"konsheng/其他词库",
			"konsheng/反动词库",
			"konsheng/广告类型",
			"konsheng/政治类型",
			"konsheng/新思想启蒙",
			"konsheng/暴恐词库",
			"konsheng/民生词库",
			"konsheng/涉枪涉爆",
			"konsheng/色情类型",
			"konsheng/色情词库",
			"konsheng/补充词库",
			"konsheng/贪腐词库",
		},
		Exclude: []string{
			// 通用 / 电商审核表:召回高但误杀重,只进聊天打星档。
			"houbb/dict",
			"konsheng/GFW补充词库",
			"konsheng/网易前端过滤敏感词库",
			"konsheng/零时-Tencent",
			// 纯 URL 表。实测 14588/14594 已被 零时-Tencent 完全包含,合并后本身为空;
			// 仍显式登记,防上游哪天补了独有条目又被静默漏掉。
			"konsheng/非法网址",
		},
	}
}

// LoadLexicon 从 dir 读清单 + 数据,校验 checksum 与词数后建**全量**索引(聊天打星档)。
//
// 任一步失败返回 error 且**不产出半成品** —— 调用方保留旧 Lexicon 继续服务(§9.15
// 「加载成功才切换,失败保留旧配置」)。首次加载失败应让服务启动失败:词库缺失时
// Check 会 fail-closed 拒绝一切取名,带着这个状态起来只会让玩家创不了角还查不出原因。
func LoadLexicon(dir string) (*Lexicon, error) { return loadLexicon(dir, nil) }

// LoadLexiconProfile 按匹配档加载(昵称拒绝档用这个,见 Profile 的实测依据)。
// 分类漂移一律报错,不静默放行也不静默收紧。
func LoadLexiconProfile(dir string, p Profile) (*Lexicon, error) { return loadLexicon(dir, &p) }

func loadLexicon(dir string, profile *Profile) (*Lexicon, error) {
	raw, err := os.ReadFile(filepath.Join(dir, LexiconManifestFile))
	if err != nil {
		return nil, fmt.Errorf("读词库 manifest 失败: %w", err)
	}
	var m LexiconManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("解析词库 manifest 失败: %w", err)
	}
	if m.Version == 0 {
		return nil, fmt.Errorf("词库 manifest version 必须 > 0")
	}
	if strings.TrimSpace(m.SourceRev) == "" {
		// 与 configtable-gen 的 -source-rev 门禁同因:不可追溯的批次一律拒,
		// 否则线上出了误杀词条,查不到是哪次上游快照带进来的。
		return nil, fmt.Errorf("词库 manifest source_rev 为空,拒绝加载不可追溯批次")
	}
	if m.File != LexiconDataFile {
		return nil, fmt.Errorf("词库 manifest file 必须是 %q,实为 %q", LexiconDataFile, m.File)
	}
	if !strings.HasPrefix(m.Checksum, "sha256:") {
		return nil, fmt.Errorf("词库 manifest checksum 缺少 sha256: 前缀")
	}

	data, err := os.ReadFile(filepath.Join(dir, m.File))
	if err != nil {
		return nil, fmt.Errorf("读词库数据失败: %w", err)
	}
	got := ChecksumOf(data)
	if got != m.Checksum {
		return nil, fmt.Errorf("词库 checksum 不匹配: 声明 %s, 实际 %s", m.Checksum, got)
	}

	var f LexiconFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("解析词库数据失败: %w", err)
	}

	// 先做**全量**结构校验与词数断言:清单声明的词数是对整个文件的,分档过滤必须在
	// 断言之后,否则挑了子集再比对总数必然对不上,截断就查不出来了。
	fileCats := make(map[string]int, len(f.Groups))
	fileTotal := 0
	for _, g := range f.Groups {
		if g.Category == "" {
			return nil, fmt.Errorf("词库存在空分类名")
		}
		if _, dup := fileCats[g.Category]; dup {
			return nil, fmt.Errorf("词库分类重复 %q", g.Category)
		}
		fileCats[g.Category] = len(g.Words)
		fileTotal += len(g.Words)
	}
	if uint32(fileTotal) != m.Words {
		return nil, fmt.Errorf("词库词数不符: 清单声明 %d, 实际 %d(数据被截断?)", m.Words, fileTotal)
	}

	keep, err := resolveProfile(profile, fileCats)
	if err != nil {
		return nil, err
	}

	cats := make(map[string]int, len(f.Groups))
	total := 0
	words := make([]string, 0, fileTotal)
	for _, g := range f.Groups {
		if !keep[g.Category] {
			continue
		}
		cats[g.Category] = len(g.Words)
		total += len(g.Words)
		words = append(words, g.Words...)
	}

	matcher, err := BuildMatcher(words)
	if err != nil {
		return nil, fmt.Errorf("建词库索引失败: %w", err)
	}
	return &Lexicon{
		version:    m.Version,
		sourceRev:  m.SourceRev,
		words:      total,
		categories: cats,
		matcher:    matcher,
	}, nil
}

// resolveProfile 定出参与匹配的分类集合,并对**任何方向的漂移**报错。
//
// profile == nil 表示全量档(聊天打星),直接全收。
// 否则强制双向闭合:
//   - Include / Exclude 里列了文件中不存在的分类 → 配置引用了已消失的上游文件;
//   - 文件里有分类既不在 Include 也不在 Exclude → 上游新增了词库文件。
//
// 第二条是关键:不这么做的话,上游新增文件会被静默排除(昵称档漏放违禁词)或
// 静默纳入(聊天档突然大面积误杀),两种漂移都不报错、也不会有人发现。
func resolveProfile(profile *Profile, fileCats map[string]int) (map[string]bool, error) {
	keep := make(map[string]bool, len(fileCats))
	if profile == nil {
		for c := range fileCats {
			keep[c] = true
		}
		return keep, nil
	}

	declared := make(map[string]bool, len(profile.Include)+len(profile.Exclude))
	var missing []string
	for _, c := range profile.Include {
		if _, ok := fileCats[c]; !ok {
			missing = append(missing, c)
			continue
		}
		declared[c] = true
		keep[c] = true
	}
	for _, c := range profile.Exclude {
		if _, ok := fileCats[c]; !ok {
			missing = append(missing, c)
			continue
		}
		if keep[c] {
			return nil, fmt.Errorf("分类 %q 同时出现在 Include 与 Exclude", c)
		}
		declared[c] = true
	}

	var undeclared []string
	for c := range fileCats {
		if !declared[c] {
			undeclared = append(undeclared, c)
		}
	}
	sort.Strings(missing)
	sort.Strings(undeclared)
	if len(undeclared) > 0 {
		return nil, fmt.Errorf(
			"词库分类 %v 未在匹配档中声明(上游新增了词库文件?)—— 必须显式归入 Include 或 Exclude,"+
				"静默排除会让昵称档漏放,静默纳入会让聊天档误杀", undeclared)
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("匹配档引用了词库中不存在的分类 %v(上游删了文件?)", missing)
	}
	if len(keep) == 0 {
		return nil, fmt.Errorf("匹配档没有纳入任何分类,拒绝构建空词库")
	}
	return keep, nil
}

// NewLexicon 用内存词表建索引,不经文件。
//
// 给单元测试与「运维临时压一批词」用。**不绕过任何校验纪律**:走的是与 LoadLexicon
// 完全相同的 BuildMatcher,词表形态要求也一样(调用方须先 Fold(Normalize(w)))。
// 生产热更仍必须走 LoadLexicon —— 只有它校验 checksum 与 version 单调。
func NewLexicon(version uint64, sourceRev string, groups []LexiconGroup) (*Lexicon, error) {
	if version == 0 {
		return nil, fmt.Errorf("version 必须 > 0")
	}
	if strings.TrimSpace(sourceRev) == "" {
		return nil, fmt.Errorf("source_rev 不能为空")
	}
	cats := make(map[string]int, len(groups))
	total := 0
	var words []string
	for _, g := range groups {
		if g.Category == "" {
			return nil, fmt.Errorf("分类名不能为空")
		}
		if _, dup := cats[g.Category]; dup {
			return nil, fmt.Errorf("分类重复 %q", g.Category)
		}
		cats[g.Category] = len(g.Words)
		total += len(g.Words)
		words = append(words, g.Words...)
	}
	matcher, err := BuildMatcher(words)
	if err != nil {
		return nil, err
	}
	return &Lexicon{
		version:    version,
		sourceRev:  sourceRev,
		words:      total,
		categories: cats,
		matcher:    matcher,
	}, nil
}

// SortedCategories 按词条数降序返回分类名,供启动日志稳定输出。
func (l *Lexicon) SortedCategories() []string {
	out := make([]string, 0, len(l.categories))
	for k := range l.categories {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if l.categories[out[i]] != l.categories[out[j]] {
			return l.categories[out[i]] > l.categories[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}
