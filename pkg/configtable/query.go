package configtable

import (
	"cmp"
	"slices"
)

// 配置表通用投影查询(手写,**非生成产物**)。
//
// 为什么不进 tools/configtable-gen 模板:这几个函数只依赖「每行都有 uint32 主键」这一条
// 全表共有的形状(discover.go 的 buildDef 强制:行 message 必须有名为 id 的 uint32 字段,
// CLAUDE.md §5.6),泛型一次写完即覆盖全部 23 张表,加新表零成本。生成逐表副本除了让模板
// 变长、每次改动都要全量重生 + 过 TestGeneratedFilesUpToDate 之外没有任何收益,而且会在
// 出现第一个消费者之前先摊 23 份死代码(§15.2「能复用现有组件不新增」/ §15.3「拒绝预设性
// 复杂化」)。生成器只该生成手写不出来的东西 —— 索引结构与类型安全的按键查询。
//
// 快照语义(§9.15):入参一律是某个 *Tables 快照里 All() 返回的行切片。Store 热更是整批
// 新建 + atomic.Pointer 换指针([store.go] Load),旧批次的 Tables 与其 rows **永不被原地
// 修改**,所以本文件返回的是「取样那一刻的快照」——不会读到撕裂数据,但也不会自动跟随热更。
// 调用方纪律不变且不因本文件改变:每次请求开头取一次 store.Tables(),整个请求内用同一个 tb。
//
// 所有权:返回值一律是**调用方独占的新切片**,不是表内部切片的别名,可随意 sort / 持有 /
// 修改,不会影响表,也不存在「别人 sort 一下把表内部顺序打乱」的隐患(这正是这里刻意不返回
// iter.Seq、也不返回预计算共享切片的原因:iter.Seq 惰性求值会把旧快照无声钉死在一个看起来
// 像「查询」的值里,共享切片则把只读约定压在注释上)。
//
// 代价与判据:每次调用一次分配。当前最大表 spawn_point 402 行(manifest v20260804002),
// 全表合计约 900 行,量级微秒,且这些 API 都不在每帧 / 每 tick / 每次移动路径上——真正的
// 热路径是 ByID() 的 O(1) map 查,本文件完全不碰。**若哪天某个调用点进了逐帧路径,立刻改成
// 在 new<X>Table 构建期预计算**:那时零分配才值钱,也才值得为它承担共享切片的所有权风险。

// rowWithID 是全部配置表行 message 的公共形状。
// 生成器强制每张表的行都有 uint32 主键 id,所以每个 *configpb.XxxRow 都自动满足。
type rowWithID interface{ GetId() uint32 }

// IDs 按加载序取全部主键(与 All() 一一对应)。
//
//	ids := configtable.IDs(tb.Level.All())
func IDs[R rowWithID](rows []R) []uint32 {
	return Values(rows, func(row R) uint32 { return row.GetId() })
}

// Values 按加载序取某列取值(投影)。**不去重**,长度与 rows 一致,要键集合用 DistinctValues。
//
//	levelIDs := configtable.Values(tb.SpawnPoint.All(), (*configpb.SpawnPointRow).GetLevelId)
func Values[R, V any](rows []R, get func(R) V) []V {
	out := make([]V, 0, len(rows))
	for _, row := range rows {
		out = append(out, get(row))
	}
	return out
}

// DistinctValues 取某列的去重取值(键集合),**按升序**返回。
//
// 升序而非加载序是有意的:键集合没有「表里的顺序」这一说,升序是唯一规范序,
// 也让结果与行的排列无关(同一批数据换个行序,键集合完全一致)。需要加载序用 Values。
//
//	levelIDs := configtable.DistinctValues(tb.SpawnPoint.All(), (*configpb.SpawnPointRow).GetLevelId)
func DistinctValues[R any, V cmp.Ordered](rows []R, get func(R) V) []V {
	out := Values(rows, get)
	slices.Sort(out)
	return slices.Compact(out)
}
