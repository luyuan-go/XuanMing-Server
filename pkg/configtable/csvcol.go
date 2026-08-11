package configtable

import (
	"fmt"
	"strconv"
	"strings"
)

// csvcol.go — 「逗号分隔 uint32 数组」列的解析件(手写,非生成)。
//
// 导表器不支持 repeated/map 列((excel_col) 限标量),多值列按约定存逗号分隔原文
// (role.proto skill_ids 先例)。任务域三表(mission/condition/reward)是服务端第一个
// 真正消费数组列的地方:格式合法性在各表 validate<Name>Row 里用 parseUint32CSV 挡在
// 加载边界,业务代码用 mustUint32CSV 取已校验数据。

// parseUint32CSV 解析逗号分隔的 uint32 数组。空串 = 空数组(合法);
// 空元素("1,,2" / 尾逗号)、非十进制数字、超出 uint32 一律报错。
// 元素取值 0 是否合法由各列语义决定(如「条件目标」0=不覆盖),本函数不拦。
func parseUint32CSV(s string) ([]uint32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]uint32, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("第 %d 个元素为空(连续/尾部逗号)", i+1)
		}
		v, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("第 %d 个元素 %q 不是合法 uint32: %w", i+1, p, err)
		}
		out = append(out, uint32(v))
	}
	return out, nil
}

// mustUint32CSV 取已过加载期校验的数组列。防御:万一拿到未校验文本(理论不可达,
// 因为坏格式批次根本切不进来),返回 nil 而不是脏数据。
func mustUint32CSV(s string) []uint32 {
	out, err := parseUint32CSV(s)
	if err != nil {
		return nil
	}
	return out
}
