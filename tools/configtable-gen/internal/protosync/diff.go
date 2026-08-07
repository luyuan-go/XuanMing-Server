// Package protosync 表头漂移同步:比对策划 xlsx 的表头行与配置表 proto 的
// (excel_col) 注解,报告差异,并把**机械的那一半**自动改写回 .proto。
//
// 边界(为什么不是「从 xlsx 全量生成 proto」):Pandora 策划表没有类型元数据行,
// 类型 / required / prefix / fk / enum 全是服务端决策,详见
// docs/design/decision-revisit-configtable-proto-generation.md。本包因此只做三件事:
//
//	改名 → 就地替换 (excel_col) 字面量(字段名、编号、注释一律不动);
//	新增 → 在行 message 末尾追加字段,取下一个未用编号(§5.4 编号不复用不回填);
//	删除 / 挪位 → 只报告,不自动改(涉及 reserved 语义,必须人判断)。
package protosync

import (
	"fmt"
	"strings"
)

// Rename 一列被改名(位置不变)。
type Rename struct {
	Pos       int    // 0 基列序
	Old       string // proto 当前 (excel_col)
	New       string // xlsx 表头现值
	FieldName string // 所属 proto 字段名(定位用)
}

// Add 一列被新增(出现在已登记列之后)。
type Add struct {
	Pos    int
	Header string
}

// Remove 一列在 xlsx 里消失(proto 仍登记着)。
type Remove struct {
	Pos       int
	Header    string
	FieldName string
}

// TableDiff 一张表的表头漂移。
type TableDiff struct {
	Table      string
	ExcelFile  string
	ProtoFile  string
	RowMessage string

	Renames []Rename
	Adds    []Add
	Removes []Remove

	// Blocked 非空 = 本表不可自动改写(挪位 / 重复列 / 空列名等),只报告。
	Blocked []string
}

// Empty 无任何漂移。
func (d TableDiff) Empty() bool {
	return len(d.Renames) == 0 && len(d.Adds) == 0 && len(d.Removes) == 0 && len(d.Blocked) == 0
}

// Writable 可自动改写(有改动、且没有阻塞项)。
func (d TableDiff) Writable() bool {
	return len(d.Blocked) == 0 && (len(d.Renames) > 0 || len(d.Adds) > 0)
}

// Compare 位置对齐比对:want 为 proto 已登记列(顺序即字段序),
// got 为 xlsx 第 1 行原始表头(允许尾部空列,内部空列视为非法)。
//
// 判定刻意保守:同位置不同名一律先当「改名」候选,但只要同时出现删除、
// 或新名/新增列与已登记列重名(说明是挪位而非改名),即整表转为只报告——
// 把改名和挪位混为一谈会让 (excel_col) 指向错误字段,后果是整批数据错列。
func Compare(want []Column, got []string) TableDiff {
	d := TableDiff{}
	headers := trimTrailingEmpty(got)

	for i, h := range headers {
		if strings.TrimSpace(h) == "" {
			d.Blocked = append(d.Blocked,
				fmt.Sprintf("表头第 %s 列为空但右侧仍有列(空列名不合法,须策划补名或删列)", ColName(i)))
		}
	}

	wantSet := make(map[string]int, len(want))
	for i, c := range want {
		wantSet[c.Header] = i
	}

	n := len(want)
	if len(headers) < n {
		n = len(headers)
	}
	for i := 0; i < n; i++ {
		if want[i].Header == headers[i] {
			continue
		}
		d.Renames = append(d.Renames, Rename{
			Pos: i, Old: want[i].Header, New: headers[i], FieldName: want[i].FieldName,
		})
		if j, dup := wantSet[headers[i]]; dup && j != i {
			d.Blocked = append(d.Blocked, fmt.Sprintf(
				"第 %s 列现为 %q,而该列名已登记在第 %s 列(%s)——这是挪位不是改名,须人工处理",
				ColName(i), headers[i], ColName(j), want[j].FieldName))
		}
	}
	for i := n; i < len(headers); i++ {
		if j, dup := wantSet[headers[i]]; dup {
			d.Blocked = append(d.Blocked, fmt.Sprintf(
				"新增第 %s 列 %q 与已登记的第 %s 列(%s)重名,须人工处理",
				ColName(i), headers[i], ColName(j), want[j].FieldName))
			continue
		}
		d.Adds = append(d.Adds, Add{Pos: i, Header: headers[i]})
	}
	for i := n; i < len(want); i++ {
		d.Removes = append(d.Removes, Remove{Pos: i, Header: want[i].Header, FieldName: want[i].FieldName})
	}

	if len(d.Removes) > 0 && len(d.Renames) > 0 {
		d.Blocked = append(d.Blocked,
			"同时出现改名与删列:无法区分「改名」和「删一列+挪位」,须人工确认后手改")
	}
	return d
}

// Column proto 侧已登记列(由 tablegen.ColumnView 转入,避免本包反向依赖描述符)。
type Column struct {
	Header    string
	FieldName string
}

func trimTrailingEmpty(in []string) []string {
	end := len(in)
	for end > 0 && strings.TrimSpace(in[end-1]) == "" {
		end--
	}
	return in[:end]
}

// ColName 0 基列序 → Excel 列名(0→A,25→Z,26→AA)。
func ColName(i int) string {
	name := ""
	for i >= 0 {
		name = string(rune('A'+i%26)) + name
		i = i/26 - 1
	}
	return name
}
