// lexicon-import 敏感词库导入器:把 configtable/lexicon/source 下的上游快照
// 合并、归一化、去重后,产出 configtable/lexicon/dist/{lexicon.json,manifest.json}。
//
// 用法(仓库根目录):
//
//	go run ./tools/lexicon-import -source-rev "houbb@fe6fc29 konsheng@5a8da94"
//
// 为什么不走 configtable-gen:那条流水线是 xlsx 驱动的(excel.proto 的 (excel_file) 注解),
// 每行还要求 uint32 主键。词库是 GitHub 上游快照、十万量级、无稳定主键,塞进策划的
// 「一键导表」既拖慢每次导表,又要人工维持词条 id 稳定。因此另开一条产物目录,但
// **热更纪律完全一致**(version 单调 + sha256 + 加载成功才切换),见 pkg/namecheck/lexicon.go。
//
// 三条纪律:
//  1. 归一化用 pkg/namecheck 的 Normalize+Fold —— 与运行时**同一份函数**。词库入库形态
//     与运行时匹配形态必须同源,否则全角 / 大小写变体两侧算出不同结果,匹配等于没做。
//  2. 产物字节稳定:分类与词条全排序、2 空格缩进、纯 LF、带尾换行。内容不变则版本号不动
//     (幂等重跑不产生无意义的版本递增)。
//  3. 本工具**从不打印词条内容**,只打印统计。词库是审核资产,打进 CI 日志等于公开发布。
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/namecheck"
)

const generatorName = "lexicon-import@0.1.0"

// utf8BOM 字节序标记。上游词库有几份是带 BOM 的 UTF-8,首行不剥会让第一个词永远匹配不上。
// 用 rune 构造而不是字面量:源文件里嵌一个真 BOM 会让 Go 编译器直接报
// "invalid BOM in the middle of the file"。
var utf8BOM = string(rune(0xFEFF))

// katalog 描述一个上游来源文件如何映射到分类。
type katalog struct {
	category string // 输出分类名
	path     string // 相对 -source 的路径
}

func main() {
	sourceDir := flag.String("source", filepath.Join("configtable", "lexicon", "source"),
		"上游词库快照目录")
	outDir := flag.String("out", filepath.Join("configtable", "lexicon", "dist"),
		"产物输出目录")
	sourceRev := flag.String("source-rev", "",
		"上游溯源标注(必填,如 \"houbb@fe6fc29 konsheng@5a8da94\")")
	minRunes := flag.Int("min-runes", 2,
		"词条最短 rune 数,短于此值丢弃。单字词会把正常昵称大面积误杀,默认 2")
	forceVersion := flag.Uint64("version", 0, "强制指定版本号(默认:内容变化则自动 +1)")
	flag.Parse()

	if strings.TrimSpace(*sourceRev) == "" {
		fmt.Fprintln(os.Stderr, "缺少 -source-rev(产物溯源必填,不允许不可追溯批次)")
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*sourceDir, *outDir, strings.TrimSpace(*sourceRev), *minRunes, *forceVersion); err != nil {
		fmt.Fprintf(os.Stderr, "导入失败: %v\n", err)
		os.Exit(1)
	}
}

func run(sourceDir, outDir, sourceRev string, minRunes int, forceVersion uint64) error {
	cats, err := discover(sourceDir)
	if err != nil {
		return err
	}
	if len(cats) == 0 {
		return fmt.Errorf("在 %s 下没有发现任何词库文件", sourceDir)
	}

	// ── 白名单:先收集,合并末尾统一剔除 ─────────────────────────────────
	allow := map[string]bool{}
	for _, p := range []string{
		filepath.Join(sourceDir, "houbb", "sensitive_word_allow.txt"),
		filepath.Join(sourceDir, "local", "allow.txt"),
	} {
		words, _, err := readWords(p, minRunes)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, w := range words {
			allow[w] = true
		}
	}

	// ── 合并去重。分类优先级 = cats 顺序;一个词只归属最先声明它的分类 ──
	seen := make(map[string]bool, 200000)
	groups := make([]namecheck.LexiconGroup, 0, len(cats))
	var rawTotal, dropShort, dropDup, dropAllow int

	for _, c := range cats {
		words, stats, err := readWords(filepath.Join(sourceDir, c.path), minRunes)
		if err != nil {
			return fmt.Errorf("读 %s: %w", c.path, err)
		}
		rawTotal += stats.raw
		dropShort += stats.short
		kept := make([]string, 0, len(words))
		for _, w := range words {
			if allow[w] {
				dropAllow++
				continue
			}
			if seen[w] {
				dropDup++
				continue
			}
			seen[w] = true
			kept = append(kept, w)
		}
		// 空分类**照样登记**,不能跳过。分类清单是匹配档(namecheck.Profile)双向闭合的
		// 依据:某个上游文件本轮去重后贡献 0 个独有词(如 konsheng/非法网址 已被
		// 零时-Tencent 完全包含),它仍然存在于上游,分类消失会让匹配档引用不到而加载失败,
		// 也会掩盖"这个文件本轮全是重复"这个事实。
		sort.Strings(kept)
		groups = append(groups, namecheck.LexiconGroup{Category: c.category, Words: kept})
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].Category < groups[j].Category })

	total := 0
	for _, g := range groups {
		total += len(g.Words)
	}
	if total == 0 {
		return fmt.Errorf("合并后词条为 0,拒绝产出空词库")
	}

	// ── 序列化(2 空格缩进 / 纯 LF / 带尾换行,与 configtable/dist 口径一致)──
	dataBytes, err := marshalStable(namecheck.LexiconFile{Groups: groups})
	if err != nil {
		return err
	}

	// ── 版本号:内容不变则沿用旧版本,幂等重跑不递增 ──────────────────────
	version := forceVersion
	prevVersion, prevSum := readPrev(outDir)
	newSum := namecheck.ChecksumOf(dataBytes)
	switch {
	case forceVersion > 0:
		// 显式指定,照用。
	case prevSum == newSum && prevVersion > 0:
		version = prevVersion
	default:
		version = prevVersion + 1
	}
	if version <= 0 {
		version = 1
	}

	man := namecheck.LexiconManifest{
		Version:       version,
		GeneratedAtMs: uint64(time.Now().UnixMilli()),
		Generator:     generatorName,
		SourceRev:     sourceRev,
		File:          namecheck.LexiconDataFile,
		Checksum:      newSum,
		Words:         uint32(total),
	}
	// 内容未变时连 generated_at_ms 都保持旧值,产物字节完全幂等(否则每次跑都脏一个文件)。
	if prevSum == newSum && prevVersion > 0 {
		if old, err := os.ReadFile(filepath.Join(outDir, namecheck.LexiconManifestFile)); err == nil {
			var om namecheck.LexiconManifest
			if json.Unmarshal(old, &om) == nil && om.SourceRev == sourceRev {
				man.GeneratedAtMs = om.GeneratedAtMs
			}
		}
	}
	manBytes, err := marshalStable(man)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, namecheck.LexiconDataFile), dataBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, namecheck.LexiconManifestFile), manBytes, 0o644); err != nil {
		return err
	}

	// ── 自检:立刻按运行时那条路径把产物加载回来,建索引成功才算这批产出可用 ──
	lex, err := namecheck.LoadLexicon(outDir)
	if err != nil {
		return fmt.Errorf("产物自检加载失败(不要发布这批): %w", err)
	}
	words, nodes, edges := lex.Stats()

	// 昵称拒绝档必须与本批分类**双向闭合**。上游快照增删词库文件时在这里就炸,
	// 而不是等到服务启动才发现 —— 后者的表现是「线上突然创不了角」或
	// 「新分类被静默漏放」,都比导表失败贵得多。
	reject, err := namecheck.LoadLexiconProfile(outDir, namecheck.RejectProfile())
	if err != nil {
		return fmt.Errorf("昵称拒绝档与本批分类不闭合(不要发布这批): %w\n"+
			"  上游增删了词库文件时,必须在 namecheck.RejectProfile() 里把新分类显式归入\n"+
			"  Include(精编定向表)或 Exclude(大而全通用审核表),并重跑误杀探测用例\n"+
			"  go test ./pkg/namecheck/ -run TestRealLexicon", err)
	}

	// ── 统计(只打数字,不打词条)──────────────────────────────────────
	fmt.Printf("词库产出 version=%d source_rev=%q\n", man.Version, man.SourceRev)
	fmt.Printf("  上游原始行 %d → 去重后 %d 条(短于 %d rune 丢弃 %d,重复 %d,白名单剔除 %d)\n",
		rawTotal, total, minRunes, dropShort, dropDup, dropAllow)
	fmt.Printf("  索引规模:词 %d / 节点 %d / 边 %d\n", words, nodes, edges)
	fmt.Printf("  昵称拒绝档:%d 条(全量档 %d 条,差额是只用于聊天打星的通用审核表)\n",
		reject.Words(), lex.Words())
	fmt.Printf("  数据文件 %d 字节,checksum %s\n", len(dataBytes), man.Checksum)
	fmt.Println("  分类构成:")
	for _, name := range lex.SortedCategories() {
		fmt.Printf("    %-40s %6d\n", name, lex.Categories()[name])
	}
	return nil
}

// discover 扫描来源目录,产出**确定顺序**的分类清单(顺序即去重优先级)。
//
// 优先级:local/deny(本仓自有,运维可即时补充)> konsheng 各分类(文件名即人类可读分类)
// > houbb/dict(单一大词典,无分类信息)。一个词落在最先声明它的分类里,因此两边都收录的
// 词会带上 konsheng 那份更细的分类标签。
func discover(sourceDir string) ([]katalog, error) {
	var cats []katalog

	if _, err := os.Stat(filepath.Join(sourceDir, "local", "deny.txt")); err == nil {
		cats = append(cats, katalog{category: "local/deny", path: filepath.Join("local", "deny.txt")})
	}

	konDir := filepath.Join(sourceDir, "konsheng")
	entries, err := os.ReadDir(konDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	var konNames []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".txt") {
			continue
		}
		konNames = append(konNames, e.Name())
	}
	sort.Strings(konNames) // 确定顺序 → 确定的分类归属 → 确定的 checksum
	for _, n := range konNames {
		cats = append(cats, katalog{
			category: "konsheng/" + strings.TrimSuffix(n, ".txt"),
			path:     filepath.Join("konsheng", n),
		})
	}

	if _, err := os.Stat(filepath.Join(sourceDir, "houbb", "sensitive_word_dict.txt")); err == nil {
		cats = append(cats, katalog{
			category: "houbb/dict",
			path:     filepath.Join("houbb", "sensitive_word_dict.txt"),
		})
	}
	return cats, nil
}

type readStats struct {
	raw   int
	short int
}

// readWords 逐行读词并归一化。**整行(去首尾空白后)即一个词** —— 不按空白切分:
// konsheng 有 800+ 条词本身含空格(英文短语 / 带空格的变体),切分会把它们拆成碎片,
// 既漏判又制造大量单字误杀。
func readWords(path string, minRunes int) ([]string, readStats, error) {
	var st readStats
	f, err := os.Open(path)
	if err != nil {
		return nil, st, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	out := make([]string, 0, 4096)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), utf8BOM))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		st.raw++
		w := namecheck.Fold(namecheck.Normalize(line))
		if w == "" {
			st.short++
			continue
		}
		if len([]rune(w)) < minRunes {
			st.short++
			continue
		}
		out = append(out, w)
	}
	if err := sc.Err(); err != nil {
		return nil, st, err
	}
	return out, st, nil
}

// marshalStable 2 空格缩进 + 纯 LF + 尾换行,且不转义 <、>、& (SetEscapeHTML(false)),
// 保证同样输入产出同样字节 —— checksum 才有意义。
func marshalStable(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// json.Encoder.Encode 已带尾换行;在 Windows 上写文件不会自动转 CRLF(os.WriteFile 是字节写)。
	return buf.Bytes(), nil
}

// readPrev 读旧产物的版本号与 checksum(不存在返回 0/"")。
func readPrev(outDir string) (uint64, string) {
	raw, err := os.ReadFile(filepath.Join(outDir, namecheck.LexiconManifestFile))
	if err != nil {
		return 0, ""
	}
	var m namecheck.LexiconManifest
	if json.Unmarshal(raw, &m) != nil {
		return 0, ""
	}
	return m.Version, m.Checksum
}
