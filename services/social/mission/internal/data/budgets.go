// budgets.go — mission 服务的库容量预算(CLAUDE.md §9.24;口径见 inventory 同名文件)。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const planPlayers = 100_000

// Budgets 是任务域表的容量预算(pandora_mission 独库,本服务全责)。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "player_mission_active", MaxRows: planPlayers * 50 * 3, MaxAvgRowBytes: 384,
			Note: "每玩家 ≤ max_active_missions(50);超限查接取上限校验是否失效(§9.18)"},
		{Table: "player_mission_done", MaxRows: planPlayers * 2000 * 3, MaxAvgRowBytes: 128,
			Note: "被任务表行数有界(预算按 2000 行任务表);超限查任务表规模或完成行重复"},
		{Table: "mission_reward_log", MaxRows: planPlayers * 2000 * 3, MaxAvgRowBytes: 512,
			Note: "GRANTED 90 天清;超限先查 mission sweep 是否在跑、PENDING 是否堆积(发放链故障)"},
		{Table: "mission_fact_receipts", MaxRows: planPlayers * 500 * 3, MaxAvgRowBytes: 256,
			Note: "清理默认关(同 exp_history);持续增长属预期,超限评估开启收据清理的前置条件"},
		{Table: "mission_push_outbox", MaxRows: 100_000, MaxAvgRowBytes: 1024,
			Note: "出箱稳态应接近空;超限 = 发布器停转或 kafka 不可用"},
		{Table: "mission_player_guards", MaxRows: planPlayers * 3, MaxAvgRowBytes: 64,
			Note: "每玩家至多 1 行的写守卫(TiDB 无 gap 锁);超限只能是玩家规模超预算"},
	}
}

// BigFields 大字段体检登记(dbcheck -size-check 用;写入侧闸在 mission_repo 的
// PayloadLimit 三处,此处只是审计口径)。
func BigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "player_mission_active", Column: "progress", MaxBytes: 256},
		{Table: "mission_reward_log", Column: "reward_pb", MaxBytes: 2048},
		{Table: "mission_push_outbox", Column: "payload", MaxBytes: 2048},
	}
}
