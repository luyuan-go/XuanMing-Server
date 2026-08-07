package protosync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cols(pairs ...string) []Column {
	out := make([]Column, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, Column{Header: pairs[i], FieldName: pairs[i+1]})
	}
	return out
}

func TestCompareNoDrift(t *testing.T) {
	d := Compare(cols("ID", "id", "备注", "remark"), []string{"ID", "备注", "", ""})
	if !d.Empty() {
		t.Fatalf("尾部空列应被忽略,实为 %+v", d)
	}
}

func TestCompareRenameAndAdd(t *testing.T) {
	// 本次真实场景:D 列改名 + E 列新增。
	want := cols("ID", "id", "备注", "remark", "位置选项", "anchor", "圆形集合", "circles")
	got := []string{"ID", "备注", "位置选项", "范围内圆形集合", "范围外圆形集合"}
	d := Compare(want, got)
	if len(d.Blocked) != 0 {
		t.Fatalf("不该阻塞: %v", d.Blocked)
	}
	if len(d.Renames) != 1 || d.Renames[0].Pos != 3 || d.Renames[0].Old != "圆形集合" ||
		d.Renames[0].New != "范围内圆形集合" || d.Renames[0].FieldName != "circles" {
		t.Fatalf("改名识别错误: %+v", d.Renames)
	}
	if len(d.Adds) != 1 || d.Adds[0].Pos != 4 || d.Adds[0].Header != "范围外圆形集合" {
		t.Fatalf("新增识别错误: %+v", d.Adds)
	}
	if !d.Writable() {
		t.Fatal("应可自动改写")
	}
}

func TestCompareMoveIsBlocked(t *testing.T) {
	// B/C 对调:位置对齐会各看成一次改名,但新名都已登记 → 必须阻塞,
	// 否则 (excel_col) 会指向错误字段,整批数据错列。
	want := cols("ID", "id", "攻击", "atk", "防御", "def")
	d := Compare(want, []string{"ID", "防御", "攻击"})
	if len(d.Blocked) == 0 {
		t.Fatalf("挪位必须阻塞,实为 %+v", d)
	}
	if d.Writable() {
		t.Fatal("阻塞时不可写")
	}
}

func TestCompareRemoveOnlyReportsOnly(t *testing.T) {
	want := cols("ID", "id", "攻击", "atk", "防御", "def")
	d := Compare(want, []string{"ID", "攻击"})
	if len(d.Removes) != 1 || d.Removes[0].FieldName != "def" {
		t.Fatalf("删列识别错误: %+v", d.Removes)
	}
	if d.Writable() {
		t.Fatal("删列须走 reserved,不可自动改写")
	}
}

func TestCompareRemovePlusRenameBlocked(t *testing.T) {
	want := cols("ID", "id", "攻击", "atk", "防御", "def")
	d := Compare(want, []string{"ID", "攻击力"})
	if len(d.Blocked) == 0 {
		t.Fatalf("改名+删列必须阻塞,实为 %+v", d)
	}
}

func TestCompareDuplicateNewHeaderBlocked(t *testing.T) {
	want := cols("ID", "id", "攻击", "atk")
	d := Compare(want, []string{"ID", "攻击", "攻击"})
	if len(d.Blocked) == 0 {
		t.Fatalf("新增列与已登记列重名必须阻塞,实为 %+v", d)
	}
}

func TestCompareInteriorEmptyBlocked(t *testing.T) {
	want := cols("ID", "id", "攻击", "atk")
	d := Compare(want, []string{"ID", "", "攻击"})
	if len(d.Blocked) == 0 {
		t.Fatalf("中间空列名必须阻塞,实为 %+v", d)
	}
}

func TestColName(t *testing.T) {
	for _, c := range []struct {
		i    int
		want string
	}{{0, "A"}, {3, "D"}, {25, "Z"}, {26, "AA"}, {27, "AB"}, {51, "AZ"}, {52, "BA"}, {701, "ZZ"}, {702, "AAA"}} {
		if got := ColName(c.i); got != c.want {
			t.Errorf("ColName(%d)=%s want %s", c.i, got, c.want)
		}
	}
}

func TestToSnake(t *testing.T) {
	for in, want := range map[string]string{
		"Id":              "id",
		"OutRangeCircles": "out_range_circles",
		"kill_exp":        "kill_exp",
		"MaxHP":           "max_hp",
		"HPMax":           "hp_max",
		"Lv":              "lv",
		"CritDamage":      "crit_damage",
	} {
		if got := toSnake(in); got != want {
			t.Errorf("toSnake(%q)=%q want %q", in, got, want)
		}
	}
}

func TestClientProtoType(t *testing.T) {
	if got := clientProtoType("Int32", "None"); got != "uint32" {
		t.Errorf("Int32 → %s", got)
	}
	if got := clientProtoType("Int32", "List"); got != "string" {
		t.Errorf("集合列应按文本承载,实为 %s", got)
	}
	if got := clientProtoType("Int64", ""); got != "uint64" {
		t.Errorf("Int64 → %s", got)
	}
	if got := clientProtoType("SomeStruct", "None"); got != "string" {
		t.Errorf("未知类型应保守取 string,实为 %s", got)
	}
}

func TestInferType(t *testing.T) {
	for _, c := range []struct {
		in   []string
		want string
	}{
		{nil, "string"},
		{[]string{"", " "}, "string"},
		{[]string{"1", "2", "300"}, "uint32"},
		{[]string{"1", "99999999999"}, "uint64"},
		{[]string{"1.5", "2"}, "float"},
		{[]string{"1", "a"}, "string"},
		{[]string{"-1"}, "float"}, // 负数不是无符号,退到 float(人 review 时改 int32)
	} {
		if got := inferType(c.in); got != c.want {
			t.Errorf("inferType(%v)=%s want %s", c.in, got, c.want)
		}
	}
}

const sampleProto = `syntax = "proto3";

package pandora.config.v1;

// 技能圆形方位表。
message SkillCircleRow {
  uint32 id = 1 [(excel_col) = "ID", (excel_required) = true];
  string remark = 2 [(excel_col) = "备注"];
  uint32 anchor = 3 [(excel_col) = "位置选项", (excel_required) = true];
  string circles = 4 [(excel_col) = "圆形集合"];
}

message SkillCircleRowExtra {
  uint32 id = 1;
}

message SkillCircleTableData {
  option (excel_file) = "技能/j_技能_方位类型_圆形.xlsx";
  repeated SkillCircleRow rows = 1;
}
`

func TestApplyRenameAndAdd(t *testing.T) {
	want := cols("ID", "id", "备注", "remark", "位置选项", "anchor", "圆形集合", "circles")
	d := Compare(want, []string{"ID", "备注", "位置选项", "范围内圆形集合", "范围外圆形集合"})
	plan, err := BuildPlan(d, PlanInput{
		Table:      "skill_circle",
		ExcelFile:  "技能/j_技能_方位类型_圆形.xlsx",
		Grid:       [][]string{{"ID", "备注", "位置选项", "范围内圆形集合", "范围外圆形集合"}, {"5", "", "1", "a", "b"}},
		DataStart:  2,
		NextNumber: 5,
		Existing:   []string{"id", "remark", "anchor", "circles"},
	}, ClientRegistry{}, Overrides{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewFields) != 1 || plan.NewFields[0].Number != 5 || plan.NewFields[0].Name != "col_e" {
		t.Fatalf("新字段解析错误: %+v", plan.NewFields)
	}

	out, err := Apply(sampleProto, "SkillCircleRow", plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `(excel_col) = "范围内圆形集合"`) || strings.Contains(out, `"圆形集合"`) {
		t.Fatalf("改名未生效:\n%s", out)
	}
	newIdx := strings.Index(out, `(excel_col) = "范围外圆形集合"`)
	if newIdx < 0 {
		t.Fatalf("新字段未追加:\n%s", out)
	}
	// 必须落在 SkillCircleRow 块内,而不是同前缀的 SkillCircleRowExtra 或表容器里。
	if newIdx > strings.Index(out, "message SkillCircleRowExtra") {
		t.Fatalf("新字段插错 message:\n%s", out)
	}
	if !strings.Contains(out, "string col_e = 5") {
		t.Fatalf("字段行格式不符:\n%s", out)
	}
	// 服务端决策不得自动填。
	if strings.Contains(out, `col_e = 5 [(excel_col) = "范围外圆形集合", (excel_required)`) {
		t.Fatal("不应自动加 required")
	}
}

func TestApplyRenameAmbiguousRefuses(t *testing.T) {
	src := sampleProto + "\nmessage Other {\n  string x = 1 [(excel_col) = \"圆形集合\"];\n}\n"
	d := TableDiff{Renames: []Rename{{Old: "圆形集合", New: "范围内圆形集合", FieldName: "circles"}}}
	if _, err := Apply(src, "SkillCircleRow", Plan{Diff: d}); err == nil {
		t.Fatal("同名注解出现多次时必须拒绝改写")
	}
}

func TestApplyUsesClientRegistry(t *testing.T) {
	reg := ClientRegistry{
		"技能/j_技能_方位类型_圆形.xlsx": {
			"范围外圆形集合": {FieldName: "out_range_circles", ProtoType: "string", Default: ""},
		},
	}
	d := Compare(cols("ID", "id"), []string{"ID", "范围外圆形集合"})
	plan, err := BuildPlan(d, PlanInput{
		Table:      "skill_circle",
		ExcelFile:  "技能/j_技能_方位类型_圆形.xlsx",
		NextNumber: 5,
		Existing:   []string{"id"},
	}, reg, Overrides{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.NewFields[0].Name != "out_range_circles" || plan.NewFields[0].Source != "客户端列登记" {
		t.Fatalf("应优先取客户端登记: %+v", plan.NewFields[0])
	}
}

func TestBuildPlanOverrideAndCollision(t *testing.T) {
	d := Compare(cols("ID", "id"), []string{"ID", "新列"})
	ov, err := ParseOverrides([]string{"skill_circle.新列=extra:uint32"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(d, PlanInput{Table: "skill_circle", NextNumber: 2, Existing: []string{"id"}}, ClientRegistry{}, ov)
	if err != nil {
		t.Fatal(err)
	}
	if plan.NewFields[0].Name != "extra" || plan.NewFields[0].Type != "uint32" {
		t.Fatalf("覆盖未生效: %+v", plan.NewFields[0])
	}

	ov2, _ := ParseOverrides([]string{"skill_circle.新列=id"})
	if _, err := BuildPlan(d, PlanInput{Table: "skill_circle", NextNumber: 2, Existing: []string{"id"}}, ClientRegistry{}, ov2); err == nil {
		t.Fatal("字段重名必须报错")
	}
}

func TestParseOverridesBadInput(t *testing.T) {
	for _, bad := range []string{"没有等号", "table.col=", "table.col=x:int128", "nodot=x"} {
		if _, err := ParseOverrides([]string{bad}); err == nil {
			t.Errorf("%q 应报错", bad)
		}
	}
}

func TestLoadClientRegistryMergeAndConflict(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.json", `{"items":[{"name":"Atk","type":"Int32","collection":"None","colName":"攻击","defaultValue":"0"}],
	  "excels":[{"src":"程序\\y_游戏模块.xlsx"}]}`)
	write("b.json", `{"items":[{"name":"Def","type":"Int32","collection":"None","colName":"防御","defaultValue":""}],
	  "excels":[{"src":"./程序/y_游戏模块.xlsx"}]}`)
	// 与 a.json 对同一列给出冲突登记 → 整列作废,退回推断。
	write("c.json", `{"items":[{"name":"AtkOther","type":"String","collection":"None","colName":"攻击"}],
	  "excels":[{"src":"程序/y_游戏模块.xlsx"}]}`)
	write("broken.json", `{ not json`)

	reg, err := LoadClientRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := reg.Lookup("程序/y_游戏模块.xlsx", "防御"); !ok || c.FieldName != "def" {
		t.Fatalf("跨文件合并失败: %+v ok=%v", c, ok)
	}
	if _, ok := reg.Lookup("程序/y_游戏模块.xlsx", "攻击"); ok {
		t.Fatal("冲突登记必须作废,不能猜哪份对")
	}
}

func TestLoadClientRegistryMissingDir(t *testing.T) {
	reg, err := LoadClientRegistry(filepath.Join(t.TempDir(), "nope"))
	if err != nil || len(reg) != 0 {
		t.Fatalf("目录不存在应返回空表不报错: %v %v", reg, err)
	}
}
