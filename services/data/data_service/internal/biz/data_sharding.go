// data_sharding.go 是 data_service 玩家数据 owner cell 锚定的服务内纯逻辑(nil-safe 接线)。
//
// 背景(scale-cellular-20m.md §4.2 owner 不变量,line 142):
//
//	「同一 player_id 的所有 owner 数据(档案 / 背包 / 段位 / 好友)必落同一 region_id 同一 cell_id」。
//
// data_service 是按 player_id 的 PlayerData 行(cache-aside:MySQL 事实源 + Redis 旁路缓存),属
// 最核心的 owner 数据,其 MySQL 行 + 缓存键必须锚定玩家 owner cell(PlayerDataShardKey = player_id),
// 保证玩家数据与档案 / 背包 / 好友等同源 owner 数据同 cell,避免跨 cell 读写放大与缓存键漂移。
//
// 本文件只落服务内纯逻辑,不改现状 cache-aside 编排(MySQL 乐观锁写 + 写后删缓存)实现:
//   - 统一玩家数据存储分片键口径(PlayerDataShardKey = player_id),为分片落地把 PlayerData 行锚定到玩家
//     owner cell 提供单一口径,**不取 version / data 内容 / 任何配置 ID**(与落点无关)。
//   - 用确定性 cellroute.Router 解析玩家 owner (region, cell),在写(WritePlayer)成功后接观测,
//     供分片上线核对「玩家数据落点 == 玩家 owner cell」。
//
// 边界(AGENTS.md §11.1):真正的 MySQL 按 owner cell 分库 / 缓存按 cell 分区属基础设施,
// 由 Codex/人接;本轮 router 为 nil(单 Cell)时行为不变,只在注入后打观测日志。
package biz

import (
	"context"
	"strconv"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// PlayerDataShardKey 是玩家数据存储的分片键口径(canonical)。
//
// 口径统一:= player_id 十进制串(玩家 owner cell 决定者,§4.2 line 142)。同一玩家的 PlayerData / 档案 /
// 背包 / 好友必落同一 owner cell;**不取 version / data 内容 / 任何配置 ID**(与落点无关)。
// 纯函数,确定性;player_id 为 0 时返回 "0"(调用方应先校验非 0)。
func PlayerDataShardKey(playerID uint64) string {
	return strconv.FormatUint(playerID, 10)
}

// PlayerDataOwner 是玩家数据的 owner 落点(region + cell)。
type PlayerDataOwner struct {
	RegionID uint32
	CellID   uint32
}

// playerDataOwner 经确定性路由解析玩家 owner 落点。
// router 为 nil(单 Cell / dev)或 player_id=0 或路由失败 → 返回 (PlayerDataOwner{}, false),调用方
// 退化为不做观测(单 Cell 落点语义不变)。
func (u *DataUsecase) playerDataOwner(playerID uint64) (PlayerDataOwner, bool) {
	if u.router == nil || playerID == 0 {
		return PlayerDataOwner{}, false
	}
	loc, err := u.router.Route(playerID)
	if err != nil {
		// 分片部署下 Route 失败 = cellroute/etcd 表故障,落点观测变盲。per-write 路径,DEBUG 避免刷屏。
		u.log.Debugw("msg", "player_data_route_failed", "player_id", playerID, "err", err)
		return PlayerDataOwner{}, false
	}
	return PlayerDataOwner{RegionID: loc.RegionID, CellID: loc.CellID}, true
}

// logPlayerDataPlacement 在 router 注入后,把一次玩家数据写后的 owner 落点打成观测日志。
//
// 仅可观测,不改 cache-aside 路径:MySQL 按 owner cell 分库属基础设施(AGENTS.md §11.1,由
// Codex/人接);本处只暴露「本次写所属玩家的 owner (region, cell) 与分片键」,供分片上线核对玩家数据
// 落点 == 玩家 owner cell(§4.2 line 142)。router 为 nil(单 Cell)时不调用此路径,行为不变。
func (u *DataUsecase) logPlayerDataPlacement(ctx context.Context, playerID uint64, op string) {
	owner, ok := u.playerDataOwner(playerID)
	if !ok {
		return
	}
	plog.With(ctx).Debugw("msg", "player_data_placement",
		"player_id", playerID,
		"op", op,
		"region", owner.RegionID,
		"cell", owner.CellID,
		"shard_key", PlayerDataShardKey(playerID))
}
