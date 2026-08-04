package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// role_level.go — RoleLevelTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateRoleLevelRow;域方法(业务语义查询)也加在本文件。

// RoleLevelKeyLevelSpan 是主键里"角色 ID"与"等级"的进制:主键 = 角色ID×1000 + 等级。
// 该口径来自源表 j_角色等级.xlsx 的 ID 公式列(B×1000+D),两端必须一致。
// 也因此**等级不得 ≥1000**,否则会进位污染角色段(见 validateRoleLevelRow)。
const RoleLevelKeyLevelSpan = 1000

// validateRoleLevelRow 逐行业务校验(生成的 newRoleLevelTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验,
// 这里只写服务端仍须 fail-closed 的业务约束)。
//
// 与生成阶段校验重复是有意的 fail-closed:服务端不信任产物一定出自本生成器。
func validateRoleLevelRow(row *configpb.RoleLevelRow) error {
	if row.GetRoleId() == 0 {
		return fmt.Errorf("角色ID(role_id)为 0")
	}
	if row.GetLevel() == 0 {
		return fmt.Errorf("角色等级(level)为 0")
	}
	// 等级进位污染:等级 ≥1000 会让主键落到别的角色段上,产生"两个角色抢同一主键"
	// 或"查某角色某级查到别人"的静默错值。宁可整批拒绝加载。
	if row.GetLevel() >= RoleLevelKeyLevelSpan {
		return fmt.Errorf("角色等级 %d 超出主键进制 %d(会进位污染角色段)",
			row.GetLevel(), RoleLevelKeyLevelSpan)
	}
	// 主键必须与 (角色ID, 等级) 自洽:源表 ID 是公式列,策划改行后若 Excel 没重算,
	// 缓存值就会与两侧列漂移(xlsxlite 读的正是缓存值)。这条把漂移挡在加载期。
	if want := roleLevelKey(row.GetRoleId(), row.GetLevel()); row.GetId() != want {
		return fmt.Errorf("主键 %d 与 (角色ID=%d, 等级=%d) 不自洽(应为 %d;"+
			"源表 ID 是公式列,改行后须让 Excel 重算并保存)",
			row.GetId(), row.GetRoleId(), row.GetLevel(), want)
	}
	return nil
}

// roleLevelKey 按源表公式口径组合主键。
func roleLevelKey(roleID, level uint32) uint32 {
	return roleID*RoleLevelKeyLevelSpan + level
}

// ByRoleLevel 查某角色在某等级的数值行。
// level 传 0 按 1 级处理(旧批次刷怪点表没有「怪物等级」列,导表后取到 0,
// 此时必须与"等级硬编码 1"的历史行为一致)。
func (t *RoleLevelTable) ByRoleLevel(roleID, level uint32) (*configpb.RoleLevelRow, bool) {
	if roleID == 0 {
		return nil, false
	}
	if level == 0 {
		level = 1
	}
	if level >= RoleLevelKeyLevelSpan {
		return nil, false
	}
	return t.ByID(roleLevelKey(roleID, level))
}

// KillExpOf 查击杀某角色某等级形态的整份经验。
//
// 这是击杀经验换算的**唯一权威**(CLAUDE.md §9.6「数值不信 DS」:DS 只报"杀了哪只怪、
// 什么等级、占多少归属权重",经验值一律在此查表)。
//
// 返回 ok=false 表示**该 (角色, 等级) 在表里没有行** —— 调用方必须把它与
// "有行但经验为 0"区分开:前者是配置漏项(该告警),后者是策划有意不给经验(如玩家英雄行)。
func (t *RoleLevelTable) KillExpOf(roleID, level uint32) (uint64, bool) {
	row, ok := t.ByRoleLevel(roleID, level)
	if !ok {
		return 0, false
	}
	return row.GetKillExp(), true
}
