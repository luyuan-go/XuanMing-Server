package tablegen

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// 只读视图:把 TableDef 内部的描述符细节以最小形态暴露给「表头漂移同步」
// (internal/protosync)。刻意不暴露 columnSpec / 描述符本身——同步工具只需要
// 「哪一列对应哪个字段、字段编号是多少、这张表的 proto 源文件在哪」,不该有能力
// 改写导表语义(required / prefix / fk 是服务端决策,见
// docs/design/decision-revisit-configtable-proto-generation.md §3.3)。

// ColumnView 一列的只读视图(列序 = 切片下标 = 表头列序)。
type ColumnView struct {
	Header    string // (excel_col) 表头列名
	FieldName string // proto 字段名(snake_case)
	FieldNum  int32  // proto 字段编号(§5.4:上线后不复用)
}

// ColumnViews 全部导表列(顺序即表头列序)。
func (d TableDef) ColumnViews() []ColumnView {
	out := make([]ColumnView, 0, len(d.columns))
	for _, c := range d.columns {
		out = append(out, ColumnView{
			Header:    c.header,
			FieldName: string(c.fd.Name()),
			FieldNum:  int32(c.fd.Number()),
		})
	}
	return out
}

// ProtoFile 行 message 所属 .proto 的仓内相对路径(如 pandora/config/v1/skill_circle.proto)。
func (d TableDef) ProtoFile() string {
	return d.rowsField.Message().ParentFile().Path()
}

// RowMessage 行 message 名(如 SkillCircleRow)。
func (d TableDef) RowMessage() string { return d.RowType }

// FieldNames 行 message 全部字段名(含无 (excel_col) 的服务端派生字段;追加新列时防重名)。
func (d TableDef) FieldNames() []string {
	fields := d.rowsField.Message().Fields()
	out := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		out = append(out, string(fields.Get(i).Name()))
	}
	return out
}

// NextFieldNumber 行 message 下一个可用字段编号。
// 取「全部已声明字段 + 全部 reserved 区间」的最大值 +1:§5.4 要求上线后编号不复用,
// 已删字段一律 reserved,新列只能往后取,绝不填补空洞。
func (d TableDef) NextFieldNumber() int32 {
	row := d.rowsField.Message()
	maxNum := int32(0)
	fields := row.Fields()
	for i := 0; i < fields.Len(); i++ {
		if n := int32(fields.Get(i).Number()); n > maxNum {
			maxNum = n
		}
	}
	ranges := row.ReservedRanges()
	for i := 0; i < ranges.Len(); i++ {
		// ReservedRanges 的 end 是开区间上界;末尾 reserved 到最大合法编号时不参与推进
		// (那种表已经不允许再加列,交由 protoc 报错)。
		end := int32(ranges.Get(i)[1]) - 1
		if end > maxNum && end < int32(protowire.MaxValidNumber) {
			maxNum = end
		}
	}
	return maxNum + 1
}
