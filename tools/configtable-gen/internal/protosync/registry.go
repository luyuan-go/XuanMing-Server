package protosync

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 客户端列登记读取。
//
// 这些 json(Pandora-Client-SVN/Tool/Table/Cs/Proto/*.json)**不是** schema 权威——
// 它只登记客户端要用的列,实测覆盖不全(j_角色等级 登记 10 列而源表 12 列、z_专精 整张表
// 没登记),所以不能拿来生成整个 proto(详见 decision-revisit-configtable-proto-generation.md §3.1)。
// 但对**新增列**而言它是最好的命名 / 类型来源:同一列在两仓用同一个名字和类型,
// 天然消除「同一张 xlsx 被两端解释成两种数据」的漂移。查不到就退回按数据推断 + 占位列名。

// ClientColumn 客户端登记的一列。
type ClientColumn struct {
	FieldName string // 已转 snake_case 的 proto 字段名
	ProtoType string // 已映射到 proto 标量类型
	Default   string // (excel_default) 候选
}

// ClientRegistry xlsx 相对路径(正斜杠) → 列名 → 列登记。
type ClientRegistry map[string]map[string]ClientColumn

type clientRegFile struct {
	Items []struct {
		Name         string `json:"name"`
		Type         string `json:"type"`
		Collection   string `json:"collection"`
		ColName      string `json:"colName"`
		DefaultValue string `json:"defaultValue"`
	} `json:"items"`
	Excels []struct {
		Src string `json:"src"`
	} `json:"excels"`
}

// LoadClientRegistry 读取整个登记目录;目录不存在返回空表(退回推断路径,不报错)。
func LoadClientRegistry(dir string) (ClientRegistry, error) {
	reg := ClientRegistry{}
	if dir == "" {
		return reg, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return nil, fmt.Errorf("读客户端列登记目录 %s: %w", dir, err)
	}
	// 同一张 xlsx 可能被多份登记引用(如 程序/y_游戏模块.xlsx 被 y_游戏模块 / y_世界模块
	// 各登记一部分列)。按列名合并;同名列若登记不一致则整列作废退回推断,
	// 不猜哪一份对。
	conflict := map[string]map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.EqualFold(filepath.Ext(e.Name()), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("读 %s: %w", e.Name(), err)
		}
		var f clientRegFile
		if err := json.Unmarshal(raw, &f); err != nil {
			// 登记目录里混着 .bak 之类的坏文件不该阻断同步,跳过即可。
			continue
		}
		for _, ex := range f.Excels {
			src := normalizeExcelPath(ex.Src)
			if src == "" {
				continue
			}
			cols := reg[src]
			if cols == nil {
				cols = map[string]ClientColumn{}
				reg[src] = cols
				conflict[src] = map[string]bool{}
			}
			for _, it := range f.Items {
				if it.ColName == "" || conflict[src][it.ColName] {
					continue
				}
				cur := ClientColumn{
					FieldName: toSnake(it.Name),
					ProtoType: clientProtoType(it.Type, it.Collection),
					Default:   it.DefaultValue,
				}
				if prev, ok := cols[it.ColName]; ok && prev != cur {
					delete(cols, it.ColName)
					conflict[src][it.ColName] = true
					continue
				}
				cols[it.ColName] = cur
			}
		}
	}
	return reg, nil
}

// Lookup 查一列的客户端登记。
func (r ClientRegistry) Lookup(excelFile, colName string) (ClientColumn, bool) {
	cols, ok := r[normalizeExcelPath(excelFile)]
	if !ok {
		return ClientColumn{}, false
	}
	c, ok := cols[colName]
	return c, ok
}

func normalizeExcelPath(p string) string {
	return strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(p), "\\", "/"), "./")
}

// clientProtoType 客户端登记类型 → proto 标量类型。
// 非标量(List / Dic / 自定义结构)一律按文本列承载 string——现有 skill_circle.circles、
// weapon.model_info 就是这么处理的(复合值由两端各自解析)。
// 整型统一映射到无符号(CLAUDE.md §5.12):客户端历史上把等级 / 数量都写成 Int32,
// 但服务端非负字段一律 uint;真需要负值的列(偏移 / 增量)靠人在 review 时改。
func clientProtoType(t, collection string) string {
	if collection != "" && !strings.EqualFold(collection, "None") {
		return "string"
	}
	switch strings.ToLower(t) {
	case "int8", "int16", "int32", "uint8", "uint16", "uint32", "byte", "short", "int":
		return "uint32"
	case "int64", "uint64", "long":
		return "uint64"
	case "float", "single":
		return "float"
	case "double":
		return "double"
	case "bool", "boolean":
		return "bool"
	case "string":
		return "string"
	default:
		return "string"
	}
}

// toSnake PascalCase / camelCase → snake_case,连续大写按缩写整体处理
// (Id→id,OutRangeCircles→out_range_circles,MaxHP→max_hp)。
func toSnake(s string) string {
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		if r < 'A' || r > 'Z' {
			b.WriteRune(r)
			continue
		}
		if i > 0 {
			prev := runes[i-1]
			prevLowerOrDigit := prev >= 'a' && prev <= 'z' || prev >= '0' && prev <= '9'
			// 缩写末位后接小写才断词:HPMax → hp_max,而 MaxHP → max_hp。
			prevUpper := prev >= 'A' && prev <= 'Z'
			nextLower := i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'
			if prevLowerOrDigit || (prevUpper && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(r - 'A' + 'a')
	}
	return b.String()
}
