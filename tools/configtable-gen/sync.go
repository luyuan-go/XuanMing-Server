package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/protosync"
	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/tablegen"
	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/xlsxlite"
)

// 表头漂移同步子命令(-sync / -sync-write)。
//
// 存在理由:策划改列名 / 加列后,导表会整批失败并要求程序同步 proto 注解。
// 「改名」和「加列」这两件事是机械的,不该手写;而类型、required、prefix、fk、enum
// 是服务端决策,不能从 xlsx 反推(详见
// docs/design/decision-revisit-configtable-proto-generation.md)。本子命令只自动做前者。
//
// 退出码:0 = 无漂移;1 = 有漂移待人处理;3 = 已改写 proto,需重跑 proto_gen + 重建生成器。
const (
	syncExitClean   = 0
	syncExitPending = 1
	syncExitWrote   = 3
)

type syncOptions struct {
	tablesRoot string
	protoRoot  string
	registry   string
	write      bool
	overrides  []string
}

// stringList 支持重复传入的字符串参数(-sync-col a -sync-col b)。
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func runSync(opt syncOptions) int {
	defs, err := tablegen.Discover()
	if err != nil {
		fmt.Fprintf(os.Stderr, "发现配置表失败: %v\n", err)
		return syncExitPending
	}
	ov, err := protosync.ParseOverrides(opt.overrides)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return syncExitPending
	}
	registryDir := opt.registry
	if registryDir == "" {
		registryDir = defaultClientRegistry(opt.tablesRoot)
	}
	reg, err := protosync.LoadClientRegistry(registryDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return syncExitPending
	}
	if len(reg) > 0 {
		fmt.Printf("客户端列登记: %s(新增列的字段名 / 类型优先取这里,保证两仓同名同类型)\n", registryDir)
	} else {
		fmt.Printf("客户端列登记: 未找到(%s),新增列将按数据推断类型 + 占位字段名\n", registryDir)
	}

	drift, wrote, blocked := 0, 0, 0
	for i := range defs {
		def := &defs[i]
		path := filepath.Join(opt.tablesRoot, filepath.FromSlash(def.ExcelFile))
		sheet, err := xlsxlite.ReadFirstSheet(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERR] 读 %s 失败: %v\n", path, err)
			blocked++
			continue
		}
		if len(sheet.Rows) == 0 {
			fmt.Fprintf(os.Stderr, "[ERR] %s: 空表\n", def.ExcelFile)
			blocked++
			continue
		}

		want := make([]protosync.Column, 0, len(def.ColumnViews()))
		for _, c := range def.ColumnViews() {
			want = append(want, protosync.Column{Header: c.Header, FieldName: c.FieldName})
		}
		diff := protosync.Compare(want, sheet.Rows[0])
		if diff.Empty() {
			continue
		}
		drift++
		diff.Table, diff.ExcelFile = def.Name, def.ExcelFile
		diff.ProtoFile, diff.RowMessage = def.ProtoFile(), def.RowMessage()

		plan, perr := protosync.BuildPlan(diff, protosync.PlanInput{
			Table:      def.Name,
			ExcelFile:  def.ExcelFile,
			Grid:       sheet.Rows,
			DataStart:  def.DataStart,
			NextNumber: def.NextFieldNumber(),
			Existing:   def.FieldNames(),
		}, reg, ov)
		if perr != nil {
			printDiff(diff, protosync.Plan{Diff: diff})
			fmt.Fprintf(os.Stderr, "  [ERR] %v\n", perr)
			blocked++
			continue
		}
		printDiff(diff, plan)

		if !diff.Writable() {
			blocked++
			continue
		}
		if !opt.write {
			continue
		}
		protoPath := filepath.Join(opt.protoRoot, filepath.FromSlash(def.ProtoFile()))
		if err := applyToFile(protoPath, def.RowMessage(), plan); err != nil {
			fmt.Fprintf(os.Stderr, "  [ERR] 改写 %s 失败: %v\n", protoPath, err)
			blocked++
			continue
		}
		fmt.Printf("  [WRITE] %s\n", protoPath)
		wrote++
	}

	fmt.Println()
	switch {
	case drift == 0:
		fmt.Println("[OK] 全部表头与 proto 注解一致,无漂移。")
		return syncExitClean
	case wrote > 0 && blocked == 0:
		fmt.Printf("[OK] 已改写 %d 张表的 proto。接下来必须按序执行(否则导表仍报旧错):\n", wrote)
		fmt.Println("       1. pwsh tools/scripts/proto_gen.ps1        # 重生 pb 描述符")
		fmt.Println("       2. go build -o run/artifacts/windows/bin/configtable-gen.exe ./tools/configtable-gen")
		fmt.Println("       3. pwsh tools/scripts/configtable_gen.ps1  # 重跑导表验证")
		fmt.Println("     新增列默认没有 required / prefix / fk,请 review 后按业务补齐。")
		return syncExitWrote
	case wrote > 0:
		fmt.Printf("[!!] 已改写 %d 张表,另有 %d 张需人工处理(见上方 [BLOCK])。\n", wrote, blocked)
		return syncExitPending
	case opt.write:
		fmt.Printf("[!!] %d 张表有漂移但都需人工处理(见上方 [BLOCK])。\n", blocked)
		return syncExitPending
	default:
		fmt.Printf("[!!] %d 张表有漂移。确认无误后加 -sync-write 自动改写可机械处理的部分。\n", drift)
		return syncExitPending
	}
}

func printDiff(d protosync.TableDiff, plan protosync.Plan) {
	fmt.Printf("\n[DIFF] %s ← %s(proto: %s)\n", d.Table, d.ExcelFile, d.ProtoFile)
	for _, r := range d.Renames {
		fmt.Printf("  改名 第 %s 列: %q → %q(字段 %s,编号不变)\n",
			protosync.ColName(r.Pos), r.Old, r.New, r.FieldName)
	}
	byHeader := map[string]protosync.NewField{}
	for _, f := range plan.NewFields {
		byHeader[f.Header] = f
	}
	for _, a := range d.Adds {
		if f, ok := byHeader[a.Header]; ok {
			fmt.Printf("  新增 第 %s 列 %q → %s %s = %d(%s)\n",
				protosync.ColName(a.Pos), a.Header, f.Type, f.Name, f.Number, f.Source)
			continue
		}
		fmt.Printf("  新增 第 %s 列 %q\n", protosync.ColName(a.Pos), a.Header)
	}
	for _, r := range d.Removes {
		fmt.Printf("  删除 第 %s 列 %q(字段 %s)——不自动改:删列须走 reserved(CLAUDE.md §5.4)\n",
			protosync.ColName(r.Pos), r.Header, r.FieldName)
	}
	for _, b := range d.Blocked {
		fmt.Printf("  [BLOCK] %s\n", b)
	}
}

func applyToFile(path, rowMessage string, plan protosync.Plan) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	out, err := protosync.Apply(string(raw), rowMessage, plan)
	if err != nil {
		return err
	}
	if out == string(raw) {
		return fmt.Errorf("改写后内容无变化(可能注解格式超出预期),请人工检查")
	}
	return os.WriteFile(path, []byte(out), 0o644)
}

// defaultClientRegistry 由策划表根目录推出客户端列登记目录
// (<Table>/../Tool/Table/Cs/Proto);推不出来就返回空,退回推断路径。
func defaultClientRegistry(tablesRoot string) string {
	abs, err := filepath.Abs(tablesRoot)
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Dir(abs), "Tool", "Table", "Cs", "Proto")
}
