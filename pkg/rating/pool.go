// Package rating 定义「段位池」(rating_pool)这一个跨服务共享的分区键规范。
//
// 存在理由(2026-08-11):产品口径「3v3 与 5v5 不共用同一份段位」要求段位按池分区存储。
// 分区键的取值来自关卡表「段位池」列,经 matchmaker 成局定格 → ds_allocator → canonical
// BattleStorageRecord → battle_result 结算 → player 分区入账,横跨四个服务。默认池名与
// 归一化规则若各服务各写一份,漂移的后果是**同一个玩家在两个服务眼里属于不同段位池**
// (一边入账 "default"、一边查 ""),表现为分怎么打都不涨。收成一处即可从机制上排除。
//
// ⚠️ 本包**刻意不维护合法池名白名单**:池名是策划在关卡表里自由填的标识符,语义只有
// 「同值即同一份段位」。加白名单等于每开一档玩法都要改代码发版,与 §17.1「差异进表」相悖。
// 新增一份段位 = 表里填一个新值,玩家在该值下从基线分起步,不污染任何既有段位。
package rating

import "strings"

// DefaultPool 是 rating_pool 为空时归一化到的池名。
//
// 空值来自两处、都必须有确定落点(§9.22 不得因缺字段就静默丢弃写入):
//   - 滚动升级期的旧 matchmaker / 旧批次表(本列上线前的对局);
//   - 未按 rating_mode=ELO 配套填池的漏配行(加载期已拒,这里是纵深防御)。
//
// 取「default」而不是空串:空串在 SQL 主键、Redis key、日志里都难与"缺字段"区分,
// 排查时无法回答"这一分到底记到哪儿了"。给它一个能被 SELECT 出来的名字。
const DefaultPool = "default"

// MaxPoolLen 是池名长度上限,与 `player_mmr.rating_pool` 列宽 VARCHAR(32) 一致。
//
// 必须与列宽同源:超长写入在非严格 sql_mode 下会**静默截断**(§9.24),
// 而截断后的池名与原值不同 = 玩家的分被记进了另一份段位且无任何报错。
// 加载期按本值拒表,是把这个失败模式挡在配置边界而不是数据库边界。
const MaxPoolLen = 32

// Normalize 把 rating_pool 归一成存储/查询用的规范形式:去首尾空白,空则取 DefaultPool。
//
// 只做这两件事——**不做大小写折叠**:池名是策划填的标识符,"PVP" 与 "pvp" 若被折叠成
// 同一份段位,等于替策划决定了两张图共用一份分;不折叠则它们各是一份,漏配一眼看得出来。
// 归一化必须在**写入与读取两侧都调用**,否则会出现写 "default" / 读 "" 的分裂。
func Normalize(pool string) string {
	if trimmed := strings.TrimSpace(pool); trimmed != "" {
		return trimmed
	}
	return DefaultPool
}
