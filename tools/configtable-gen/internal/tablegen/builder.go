package tablegen

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// 通用行构建器:xlsx 网格 → 整表容器 message,完全由 TableDef(proto 描述符 + excel 注解)驱动。
// 校验执行 docs/design/config-table-hotreload.md §7:表头对齐、类型合规、枚举合法、
// 主键唯一、必填 / 前缀,任一失败整批不产出,错误定位到「表 + 行 + 列」。
//
// 源表格式契约(Pandora 策划表通用版式,与旧项目 mmorpg 20 行版式不同):
//
//	第 1 行 = 列名(与 (excel_col) 注解逐列精确一致,列序 = 字段号序);
//	默认第 2-4 行 = 策划注释 / 取值说明(跳过);
//	默认第 5 行起 = 数据区；特殊版式由 (excel_data_start_row) 显式覆盖。
//
// 数据区整行全空跳过,主键列为空但行内有值 → 报错。
const headerRow = 0

// Build 网格 → (容器 message, 行数)。
func (d *TableDef) Build(grid [][]string) (proto.Message, int, error) {
	if len(grid) <= headerRow {
		return nil, 0, fmt.Errorf("%s: 空表", d.ExcelFile)
	}
	if err := d.checkHeaders(grid[headerRow]); err != nil {
		return nil, 0, err
	}

	container := d.container.New()
	list := container.Mutable(d.rowsField).List()
	seen := make(map[uint64]int)          // 主键 → 已出现的 xlsx 行号
	seenKeys := make(map[int]map[any]int) // 唯一二级键列序 → 取值 → xlsx 行号((excel_key) §7.4 同级查重)
	for i := d.DataStart - 1; i < len(grid); i++ {
		xlsxRow := i + 1
		cells := padTo(grid[i], len(d.columns))
		if allEmpty(cells) {
			continue
		}
		elem := list.NewElement()
		rm := elem.Message()
		for j, cs := range d.columns {
			if err := d.setCell(rm, cs, cells[j]); err != nil {
				return nil, 0, fmt.Errorf("%s 第 %d 行 %s: %w", d.ExcelFile, xlsxRow, cs.header, err)
			}
			if cs.unique {
				val := rm.Get(cs.fd).Interface()
				if seenKeys[j] == nil {
					seenKeys[j] = make(map[any]int)
				}
				if prev, dup := seenKeys[j][val]; dup {
					return nil, 0, fmt.Errorf("%s 第 %d 行 %s: 唯一键取值 %v 与第 %d 行重复",
						d.ExcelFile, xlsxRow, cs.header, val, prev)
				}
				seenKeys[j][val] = xlsxRow
			}
		}
		key := rm.Get(d.keyField).Uint()
		if key == 0 {
			return nil, 0, fmt.Errorf("%s 第 %d 行: 主键须为正整数", d.ExcelFile, xlsxRow)
		}
		if prev, dup := seen[key]; dup {
			return nil, 0, fmt.Errorf("%s 第 %d 行: 主键 %d 与第 %d 行重复", d.ExcelFile, xlsxRow, key, prev)
		}
		seen[key] = xlsxRow
		list.Append(elem)
	}
	if list.Len() == 0 {
		return nil, 0, fmt.Errorf("%s: 数据区没有任何行", d.ExcelFile)
	}
	return container.Interface(), list.Len(), nil
}

func (d *TableDef) checkHeaders(header []string) error {
	got := padTo(header, len(d.columns))
	for i, cs := range d.columns {
		if got[i] != cs.header {
			return fmt.Errorf("%s 表头第 %s 列: 期望 %q 实为 %q(列被改名 / 挪位,须同步 proto 注解)",
				d.ExcelFile, colName(i), cs.header, got[i])
		}
	}
	for i := len(d.columns); i < len(header); i++ {
		if header[i] != "" {
			return fmt.Errorf("%s 表头出现未登记的第 %s 列 %q(新列须先加 proto 字段 + (excel_col) 注解)",
				d.ExcelFile, colName(i), header[i])
		}
	}
	return nil
}

// setCell 解析一个单元格并写入行 message:空值走 required/default,非空走类型解析 + 前缀校验。
func (d *TableDef) setCell(rm protoreflect.Message, cs columnSpec, cell string) error {
	if cell == "" {
		if cs.required {
			return fmt.Errorf("必填列为空")
		}
		if cs.def == "" {
			return nil // 无默认值:保持类型零值
		}
		cell = cs.def
	} else if cs.prefix != "" && !strings.HasPrefix(cell, cs.prefix) {
		return fmt.Errorf("须以 %q 开头,实为 %q", cs.prefix, cell)
	}
	v, err := parseCell(cs.fd, cell)
	if err != nil {
		return err
	}
	rm.Set(cs.fd, v)
	return nil
}

// parseCell 按字段类型解析单元格文本(§7.2 类型合规 / §7.3 枚举合法)。
func parseCell(fd protoreflect.FieldDescriptor, cell string) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(cell), nil
	case protoreflect.BoolKind:
		switch cell {
		case "0":
			return protoreflect.ValueOfBool(false), nil
		case "1":
			return protoreflect.ValueOfBool(true), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("布尔列只认 0/1/空,实为 %q", cell)
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := strconv.ParseUint(cell, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为非负整数,实为 %q", cell)
		}
		return protoreflect.ValueOfUint32(uint32(n)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := strconv.ParseUint(cell, 10, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为非负整数,实为 %q", cell)
		}
		return protoreflect.ValueOfUint64(n), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := strconv.ParseInt(cell, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为整数,实为 %q", cell)
		}
		return protoreflect.ValueOfInt32(int32(n)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := strconv.ParseInt(cell, 10, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为整数,实为 %q", cell)
		}
		return protoreflect.ValueOfInt64(n), nil
	case protoreflect.FloatKind:
		// 概率 / 倍率 / 坐标这类列策划就是按小数填的(暴击率 0.05、缩放 1.5)。
		// 32 位解析:超出 float32 表示范围直接报错,不静默变成 +Inf——配置表里出现
		// Inf 会一路带进战斗数值计算,比生成失败难查得多。
		f, err := strconv.ParseFloat(cell, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为数字(允许小数),实为 %q", cell)
		}
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return protoreflect.Value{}, fmt.Errorf("数值非法(Inf/NaN),实为 %q", cell)
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case protoreflect.DoubleKind:
		f, err := strconv.ParseFloat(cell, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("须为数字(允许小数),实为 %q", cell)
		}
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return protoreflect.Value{}, fmt.Errorf("数值非法(Inf/NaN),实为 %q", cell)
		}
		return protoreflect.ValueOfFloat64(f), nil
	case protoreflect.EnumKind:
		n, err := strconv.ParseInt(cell, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("枚举列须为数字,实为 %q", cell)
		}
		ev := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(n))
		if n == 0 || ev == nil {
			return protoreflect.Value{}, fmt.Errorf("取值 %d 不在 %s 枚举内(0=UNSPECIFIED 不允许;新增取值须先改 proto)",
				n, fd.Enum().Name())
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("列类型 %s 暂不支持导表", fd.Kind())
	}
}

func padTo(cells []string, n int) []string {
	if len(cells) >= n {
		return cells
	}
	out := make([]string, n)
	copy(out, cells)
	return out
}

func allEmpty(cells []string) bool {
	for _, c := range cells {
		if c != "" {
			return false
		}
	}
	return true
}

// colName 0 → "A"。
func colName(i int) string {
	name := ""
	for i >= 0 {
		name = string(rune('A'+i%26)) + name
		i = i/26 - 1
	}
	return name
}
