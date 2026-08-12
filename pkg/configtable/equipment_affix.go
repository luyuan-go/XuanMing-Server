package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// equipment_affix.go — EquipmentAffixTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateEquipmentAffixRow;域方法(业务语义查询)也加在本文件。

// validateEquipmentAffixRow 逐行业务校验(生成的 newEquipmentAffixTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验,
// 这里只写服务端仍须 fail-closed 的业务约束;无约束保持 return nil)。
func validateEquipmentAffixRow(row *configpb.EquipmentAffixRow) error {
	if row.GetPoolId() == 0 {
		return fmt.Errorf("词条池ID(pool_id)必须 > 0")
	}
	if row.GetAttrCount() == 0 || row.GetAttrCount() > 8 {
		return fmt.Errorf("抽取数量(attr_count)必须在 [1,8],实为 %d", row.GetAttrCount())
	}
	if row.GetAttrId() == 0 {
		return fmt.Errorf("属性ID(attr_id)必须 > 0")
	}
	if row.GetWeight() == 0 || row.GetWeight() > 1_000_000 {
		return fmt.Errorf("权重(weight)必须在 [1,1000000],实为 %d", row.GetWeight())
	}
	if row.GetMinValue() <= 0 || row.GetMaxValue() < row.GetMinValue() || row.GetMaxValue() > 1_000_000_000 {
		return fmt.Errorf("数值区间必须满足 0 < min_value <= max_value <= 1000000000,实为 [%d,%d]",
			row.GetMinValue(), row.GetMaxValue())
	}
	return nil
}
