package protosync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// NewField 一个待追加的 proto 字段(由新增列解析而来)。
type NewField struct {
	Header  string // 表头列名
	Name    string // proto 字段名
	Type    string // proto 标量类型
	Number  int32  // 字段编号(§5.4:只往后取,不回填空洞)
	Default string // (excel_default),空则不写
	Source  string // 类型 / 命名来源说明(写进注释,便于 review)
}

// Plan 一张表的可执行改写计划。
type Plan struct {
	Diff      TableDiff
	NewFields []NewField
}

// Override 人工指定新增列的字段名 / 类型(-sync-col)。
type Override struct {
	Name string
	Type string
}

// Overrides key = "<表名>.<列名>"。
type Overrides map[string]Override

// ParseOverrides 解析 -sync-col 参数:`<表名>.<列名>=<字段名>:<类型>`(类型可省)。
func ParseOverrides(args []string) (Overrides, error) {
	out := Overrides{}
	for _, a := range args {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		key, spec, ok := strings.Cut(a, "=")
		if !ok || !strings.Contains(key, ".") {
			return nil, fmt.Errorf("-sync-col 格式应为 <表名>.<列名>=<字段名>[:<类型>],实为 %q", a)
		}
		name, typ, _ := strings.Cut(spec, ":")
		name = strings.TrimSpace(name)
		typ = strings.TrimSpace(typ)
		if name == "" {
			return nil, fmt.Errorf("-sync-col %q 缺少字段名", a)
		}
		if typ != "" && !validProtoType(typ) {
			return nil, fmt.Errorf("-sync-col %q 类型 %q 不受支持(允许 %s)", a, typ, strings.Join(allowedTypes, " / "))
		}
		out[strings.TrimSpace(key)] = Override{Name: name, Type: typ}
	}
	return out, nil
}

var allowedTypes = []string{"uint32", "uint64", "int32", "int64", "float", "double", "bool", "string"}

func validProtoType(t string) bool {
	for _, a := range allowedTypes {
		if a == t {
			return true
		}
	}
	return false
}

// PlanInput 构建改写计划所需的表侧事实。
type PlanInput struct {
	Table      string
	ExcelFile  string
	Grid       [][]string // xlsx 全量网格(推断类型用)
	DataStart  int        // 数据区起始行号(1 基)
	NextNumber int32      // 行 message 下一个可用字段编号
	Existing   []string   // 行 message 已有字段名(防重名)
}

// BuildPlan 把 Adds 解析成可写入的字段定义。
// 命名 / 类型优先取客户端列登记(两仓同名同类型,天然不漂移),查不到再按数据推断;
// -sync-col 覆盖一切。required / prefix / fk 一律不自动加——那是服务端决策。
func BuildPlan(diff TableDiff, in PlanInput, reg ClientRegistry, ov Overrides) (Plan, error) {
	plan := Plan{Diff: diff}
	if len(diff.Adds) == 0 {
		return plan, nil
	}
	used := map[string]bool{}
	for _, n := range in.Existing {
		used[n] = true
	}
	next := in.NextNumber
	for _, add := range diff.Adds {
		f := NewField{Header: add.Header, Number: next}
		next++

		if o, ok := ov[in.Table+"."+add.Header]; ok {
			f.Name, f.Type, f.Source = o.Name, o.Type, "人工指定(-sync-col)"
			if f.Type == "" {
				f.Type = inferType(columnCells(in.Grid, add.Pos, in.DataStart))
				f.Source = "字段名人工指定,类型按数据推断"
			}
		} else if c, ok := reg.Lookup(in.ExcelFile, add.Header); ok && c.FieldName != "" {
			f.Name, f.Type, f.Default = c.FieldName, c.ProtoType, c.Default
			f.Source = "客户端列登记"
		} else {
			f.Name = "col_" + strings.ToLower(ColName(add.Pos))
			f.Type = inferType(columnCells(in.Grid, add.Pos, in.DataStart))
			f.Source = "按数据推断(客户端未登记本列,字段名为占位,请改成有语义的名字)"
		}

		if used[f.Name] {
			return Plan{}, fmt.Errorf("%s 新增列 %q 推出的字段名 %q 与已有字段重名,请用 -sync-col %s.%s=<字段名>[:<类型>] 指定",
				in.Table, add.Header, f.Name, in.Table, add.Header)
		}
		if !identRe.MatchString(f.Name) {
			return Plan{}, fmt.Errorf("%s 新增列 %q 推出的字段名 %q 不是合法 proto 标识符,请用 -sync-col 指定",
				in.Table, add.Header, f.Name)
		}
		used[f.Name] = true
		plan.NewFields = append(plan.NewFields, f)
	}
	return plan, nil
}

var identRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func columnCells(grid [][]string, col, dataStart int) []string {
	if dataStart < 1 {
		dataStart = 1
	}
	var out []string
	for i := dataStart - 1; i < len(grid); i++ {
		if col < len(grid[i]) {
			out = append(out, grid[i][col])
		}
	}
	return out
}

// inferType 按数据列取值推断类型(保守:拿不准一律 string)。
// 只用于客户端没登记的新列,且推断结果会写进注释供 review——不追求全对,
// 只保证「不会因为猜错类型导致下次导表整批失败」时人能一眼看出来改哪儿。
func inferType(cells []string) string {
	seen := false
	allUint, allFloat, maxU := true, true, uint64(0)
	for _, c := range cells {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		seen = true
		if u, err := strconv.ParseUint(c, 10, 64); err == nil {
			if u > maxU {
				maxU = u
			}
		} else {
			allUint = false
		}
		if _, err := strconv.ParseFloat(c, 64); err != nil {
			allFloat = false
		}
	}
	switch {
	case !seen:
		return "string"
	case allUint && maxU <= 4294967295:
		return "uint32"
	case allUint:
		return "uint64"
	case allFloat:
		return "float"
	default:
		return "string"
	}
}

// Apply 把计划写进 .proto 文本,返回改写后的内容。
// 改名只替换 (excel_col) 字面量(字段名 / 编号 / 注释一律不动);
// 新增字段追加在行 message 末尾。
func Apply(src string, rowMessage string, plan Plan) (string, error) {
	out := src
	for _, r := range plan.Diff.Renames {
		re, err := regexp.Compile(`\(excel_col\)\s*=\s*"` + regexp.QuoteMeta(r.Old) + `"`)
		if err != nil {
			return "", fmt.Errorf("构造 %q 的匹配式失败: %w", r.Old, err)
		}
		m := re.FindAllStringIndex(out, -1)
		if len(m) != 1 {
			return "", fmt.Errorf("字段 %s 的 (excel_col) = %q 在文件中出现 %d 次(应为 1 次),拒绝自动改写",
				r.FieldName, r.Old, len(m))
		}
		out = out[:m[0][0]] + `(excel_col) = "` + r.New + `"` + out[m[0][1]:]
	}
	if len(plan.NewFields) == 0 {
		return out, nil
	}

	lines := strings.Split(out, "\n")
	closeIdx, err := findMessageClose(lines, rowMessage)
	if err != nil {
		return "", err
	}
	var ins []string
	for _, f := range plan.NewFields {
		opts := fmt.Sprintf(`[(excel_col) = %q`, f.Header)
		if f.Default != "" {
			opts += fmt.Sprintf(`, (excel_default) = %q`, f.Default)
		}
		opts += "]"
		ins = append(ins,
			fmt.Sprintf("  // %s(configtable-sync 追加:xlsx 新增列;类型 / 命名来源:%s)。", f.Header, f.Source),
			"  // 服务端决策未自动填:如需 (excel_required) / (excel_prefix) / (excel_fk) 请人工补,并补本列业务注释。",
			fmt.Sprintf("  %s %s = %d %s;", f.Type, f.Name, f.Number, opts),
		)
	}
	merged := append([]string{}, lines[:closeIdx]...)
	merged = append(merged, ins...)
	merged = append(merged, lines[closeIdx:]...)
	return strings.Join(merged, "\n"), nil
}

// findMessageClose 返回 message <name> 块闭合 '}' 所在行下标(大括号配对)。
func findMessageClose(lines []string, name string) (int, error) {
	open := -1
	for i, l := range lines {
		t := strings.TrimSpace(stripLineComment(l))
		rest, ok := strings.CutPrefix(t, "message "+name)
		// 必须整词匹配:message FooRow 不能命中 message FooRowExtra。
		if !ok || !strings.HasPrefix(strings.TrimSpace(rest), "{") {
			continue
		}
		open = i
		break
	}
	if open < 0 {
		return 0, fmt.Errorf("找不到 message %s 的定义", name)
	}
	depth := 0
	for i := open; i < len(lines); i++ {
		code := stripLineComment(lines[i])
		for _, r := range code {
			switch r {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					return i, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("message %s 的大括号没有闭合", name)
}

func stripLineComment(l string) string {
	if i := strings.Index(l, "//"); i >= 0 {
		return l[:i]
	}
	return l
}
