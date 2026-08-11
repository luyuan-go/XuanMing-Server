// budgets.go — player 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 预算口径与 fail-open 语义见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const (
	planPlayers = 100_000
	planDAU     = 10_000
)

// Budgets 是 pandora_player 库的容量预算。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "players", MaxRows: planPlayers * 3, MaxAvgRowBytes: 512},
		{Table: "player_heroes", MaxRows: planPlayers * 100 * 3, MaxAvgRowBytes: 128},
		{Table: "player_attributes", MaxRows: planPlayers * 16 * 3, MaxAvgRowBytes: 128},
		{Table: "player_equipment", MaxRows: planPlayers * 16 * 3, MaxAvgRowBytes: 128},
		{Table: "player_talents", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 128},
		{
			// 每场对局每人一行,保留期 90 天(清理默认关):日活 1 万 × 每天 10 局 × 90 × 3。
			Table: "mmr_history", MaxRows: planDAU * 10 * 90 * 3, MaxAvgRowBytes: 256,
			Note: "MMR 幂等历史;清理默认关(history_cleanup_enabled),超限先确认是否该开清理",
		},
		{
			Table: "exp_history", MaxRows: planDAU * 100 * 7 * 3, MaxAvgRowBytes: 256,
			Note: "经验幂等收据,保留期 7 天;超限查 exp_history_cleanup_enabled 与上游 progress 出箱重试是否有界",
		},
		{Table: "attr_point_grants", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 192},
		{Table: "talent_point_grants", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 192},
		{
			// 发卡幂等收据。idempotency_key 是 VARCHAR(128)(比另外两张的 64 宽一倍,
			// 抽卡侧的键通常带批次 + 卡池 + 序号),故 avg_row 预算相应放宽。
			Table: "skill_card_grants", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 256,
			Note: "抽卡/活动发卡幂等收据;超限查 history_cleanup_enabled 与抽卡侧幂等键是否过长",
		},
		{
			// 每玩家每卡至多一行,被技能卡配置表行数有界。
			Table: "player_skill_cards", MaxRows: planPlayers * 200, MaxAvgRowBytes: 128,
			Note: "行数上界 = 玩家数 × 技能卡表行数;远超说明发了配置表里没有的卡",
		},
		{
			// 每玩家至多 SkillSlotCount(4) 行。
			Table: "player_skill_slots", MaxRows: planPlayers * 8, MaxAvgRowBytes: 128,
			Note: "行数上界 = 玩家数 × 卡槽数(4);超限说明卡槽越界校验被绕过",
		},
		{
			// 出箱表:投递成功即删,稳态应接近空。这里给的是"积压告警线"而非容量上限。
			Table: "player_push_outbox", MaxRows: 100_000, MaxAvgRowBytes: 1024,
			Note: "推送出箱应即时排空;行数堆积 = kafka 投递链堵塞,查 RunPushOutboxPublisher 日志",
		},
		{
			// 每玩家一行,但**单行**是 LONGBLOB(4GB):这里 avg_row 才是关键信号。
			// 设计期望:永久来源 ≤64 条 + 活动实例 ≤256 条,单条位图按真实档位数通常 <1KB。
			Table: "player_reward_claims", MaxRows: planPlayers * 3, MaxAvgRowBytes: 8 * 1024,
			Note: "record 是 LONGBLOB(DB 层不设防);avg_row 超 8KB 说明位图条目数异常膨胀——" +
				"查是否有客户端刷任意 source/activity_instance_id(pkg/rewardclaim 已加条目数上限),或活动未 EraseActivity 回收",
		},
	}
}

// BigFields 是列级字节预算(全表扫描,仅人工/低频触发)。
func BigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "player_reward_claims", Column: "record", MaxBytes: 64 * 1024,
			Note: "领奖位图集合;超限用 dbcheck -size-check -top-rows 定位 player_id,再 dump 反序列化看是 permanent 还是 activity 条目爆了"},
	}
}
