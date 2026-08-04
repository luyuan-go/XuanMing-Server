package configtable

import (
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// skill_sector.go — SkillSectorTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateSkillSectorRow;域方法(业务语义查询)也加在本文件。

// validateSkillSectorRow 逐行业务校验(生成的 newSkillSectorTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验,
// 这里只写服务端仍须 fail-closed 的业务约束;无约束保持 return nil)。
func validateSkillSectorRow(row *configpb.SkillSectorRow) error {
	_ = row
	return nil
}
