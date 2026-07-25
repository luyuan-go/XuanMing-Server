// dialogue_sharding.go 是 NPC 对话会话 owner cell 锚定 + 会话存储分片键口径的服务内纯逻辑(nil-safe 接线)。
//
// 背景(scale-cellular-20m.md §4.2 owner 不变量):同一 player_id 的所有 owner 数据(档案 / 背包 /
// 段位 / 好友)必落同一 region_id 同一 cell_id,region 是 owner 边界最外层。dialogue 服务端会话
// 是该玩家的服务端权威状态(归属用 player_id 校验,R5),属 owner 数据,必须锚定该玩家 owner cell:
// StartDialogue / ChooseOption / EndDialogue 三步须落同一 cell,否则会话读不回(会话归属错乱)。
//
// 关键口径:dialogue_id 是 snowflake(全局唯一但与玩家落点无关),**不能**当会话存储分片键;
// 会话分片键必须取 player_id(owner cell 的决定者),保证会话与玩家其余 owner 数据同 cell。
//
// 本文件只落服务内纯逻辑,不改现状会话存储(SessionStore by dialogue_id)实现:
//   - 统一会话存储分片键口径(SessionShardKey = player_id),为分片落地时把会话路由到玩家
//     owner cell 提供单一口径,避免误用 dialogue_id 分片导致会话与 owner 数据跨 cell。
//   - 用确定性 cellroute.Router 把玩家解析到 owner (region, cell),作为可观测信号,供分片上线
//     核对「会话落点 == 玩家 owner cell」。
//
// 边界(AGENTS.md §11.1):真正的会话存储按 owner cell 分片 / 边缘网关按 region+cell 定向连接属
// 基础设施,由 Codex/人接;本轮 router 为 nil(单 Cell)时行为不变,只在注入后打观测日志。
package biz

import (
	"context"
	"strconv"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// SessionShardKey 是 dialogue 服务端会话的存储分片键口径(canonical)。
//
// 口径统一:= player_id 十进制串(owner cell 决定者,scale-cellular-20m.md §4.2 owner 不变量)。
// 会话归属单个玩家,必须锚定该玩家 owner cell;**不取 dialogue_id**(snowflake 与落点无关,
// 误用会让会话与玩家其余 owner 数据落不同 cell)。纯函数,确定性;player_id 为 0 时返回 "0"
// (调用方应先校验非 0)。
func SessionShardKey(playerID uint64) string {
	return strconv.FormatUint(playerID, 10)
}

// SessionLocation 是一名玩家会话锚定的 owner 落点(只取分片落点判定需要的维度)。
type SessionLocation struct {
	RegionID uint32
	CellID   uint32
}

// sessionOwner 解析一名玩家会话锚定的 owner 落点 (region, cell)。
// router 为 nil(单 Cell / dev)或路由失败 / player_id 为 0 → 返回 (SessionLocation{}, false),
// 调用方退化为不做观测(单 Cell 本地会话语义不变)。
func (u *DialogueUsecase) sessionOwner(playerID uint64) (SessionLocation, bool) {
	if u.router == nil || playerID == 0 {
		return SessionLocation{}, false
	}
	loc, err := u.router.Route(playerID)
	if err != nil {
		return SessionLocation{}, false
	}
	return SessionLocation{RegionID: loc.RegionID, CellID: loc.CellID}, true
}

// logSessionPlacement 在 router 注入后,把一次会话创建锚定的 owner 落点打成观测日志。
//
// 仅可观测,不改会话路径:会话存储按 owner cell 分片 / 边缘网关定向连接属基础设施
// (AGENTS.md §11.1,由 Codex/人接);本处只暴露「本会话锚定哪个 region/cell」信号,供分片上线
// 核对会话落点与玩家其余 owner 数据同 cell。router 为 nil(单 Cell)时不调用此路径,行为不变。
func (u *DialogueUsecase) logSessionPlacement(ctx context.Context, dialogueID, playerID uint64) {
	loc, ok := u.sessionOwner(playerID)
	if !ok {
		// router 已注入且 player_id 非 0 时,!ok 只可能是 cellroute.Route 解析失败(路由表
		// 缺 player→region/cell 映射):此前静默吞错,分片部署下落点校验变盲还查不到原因。
		if u.router != nil && playerID != 0 {
			plog.With(ctx).Debugw("msg", "dialogue_session_route_failed",
				"dialogue_id", dialogueID, "player_id", playerID,
				"hint", "cellroute 解析失败,会话落点观测降级")
		}
		return
	}
	plog.With(ctx).Debugw("msg", "dialogue_session_placement",
		"dialogue_id", dialogueID,
		"player_id", playerID,
		"region", loc.RegionID,
		"cell", loc.CellID,
		"shard_key", SessionShardKey(playerID))
}
