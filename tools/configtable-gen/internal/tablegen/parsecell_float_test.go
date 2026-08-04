// parsecell_float_test.go — 浮点列导表支持的回归测试。
//
// 背景:parseCell 原先只支持 string/bool/整型/enum,浮点列走 default 分支报
// 「列类型 float 暂不支持导表」。而策划表里概率 / 倍率 / 坐标本来就是小数
// (暴击率 0.05、缩放 1.5、刷怪点坐标),导致这些表根本登记不进 configtable。
//
// 用 wrapperspb 的 FloatValue / DoubleValue 取真实字段描述符,避免为一个类型分支
// 去改夹具 proto 并重新生成。
package tablegen

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// floatFD 取一个 FloatKind 字段描述符(google.protobuf.FloatValue.value)。
func floatFD(t *testing.T) protoreflect.FieldDescriptor {
	t.Helper()
	fd := (&wrapperspb.FloatValue{}).ProtoReflect().Descriptor().Fields().ByName("value")
	if fd == nil || fd.Kind() != protoreflect.FloatKind {
		t.Fatalf("取 FloatKind 描述符失败: %v", fd)
	}
	return fd
}

// doubleFD 取一个 DoubleKind 字段描述符(google.protobuf.DoubleValue.value)。
func doubleFD(t *testing.T) protoreflect.FieldDescriptor {
	t.Helper()
	fd := (&wrapperspb.DoubleValue{}).ProtoReflect().Descriptor().Fields().ByName("value")
	if fd == nil || fd.Kind() != protoreflect.DoubleKind {
		t.Fatalf("取 DoubleKind 描述符失败: %v", fd)
	}
	return fd
}

func TestParseCellFloatAcceptsDecimals(t *testing.T) {
	fd := floatFD(t)
	cases := []struct {
		cell string
		want float32
	}{
		{"0.05", 0.05},   // 暴击率这类概率列
		{"1", 1},         // 整数写法也必须收(策划填 1 表示 100%)
		{"0", 0},         // 零值
		{"1.5", 1.5},     // 倍率
		{"-12.75", -12.75}, // 坐标偏移可以为负
		{"1e-3", 0.001},  // Excel 有时把小数存成科学计数法
	}
	for _, c := range cases {
		v, err := parseCell(fd, c.cell)
		if err != nil {
			t.Fatalf("parseCell(float, %q) 意外报错: %v", c.cell, err)
		}
		if got := v.Float(); float32(got) != c.want {
			t.Fatalf("parseCell(float, %q) = %v, 期望 %v", c.cell, got, c.want)
		}
	}
}

func TestParseCellDoubleAcceptsDecimals(t *testing.T) {
	fd := doubleFD(t)
	v, err := parseCell(fd, "3.141592653589793")
	if err != nil {
		t.Fatalf("parseCell(double) 意外报错: %v", err)
	}
	if v.Float() != 3.141592653589793 {
		t.Fatalf("parseCell(double) = %v, 期望 3.141592653589793", v.Float())
	}
}

func TestParseCellFloatRejectsGarbage(t *testing.T) {
	fd := floatFD(t)
	// 非数字必须报错,不能静默变 0——配置表里一个错字段静默归零比生成失败难查得多。
	for _, cell := range []string{"abc", "0.5%", "１.５", ""} {
		if _, err := parseCell(fd, cell); err == nil {
			t.Fatalf("parseCell(float, %q) 应当报错但没有", cell)
		}
	}
}

func TestParseCellFloatRejectsInfAndNaN(t *testing.T) {
	fd := floatFD(t)
	// Inf/NaN 会一路带进战斗数值计算,必须在导表阶段拦住。
	for _, cell := range []string{"Inf", "+Inf", "-Inf", "NaN", "1e40"} {
		_, err := parseCell(fd, cell)
		if err == nil {
			t.Fatalf("parseCell(float, %q) 应当报错但没有", cell)
		}
		// 1e40 超出 float32 范围,ParseFloat 返回 ErrRange 且值为 +Inf;
		// 两条路径(解析报错 / Inf 拦截)都算拦住,只要不放行。
		if !strings.Contains(err.Error(), "数字") && !strings.Contains(err.Error(), "非法") {
			t.Fatalf("parseCell(float, %q) 报错信息应说明原因,实为 %v", cell, err)
		}
	}
}
