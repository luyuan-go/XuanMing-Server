// chat_routing.go 是私聊跨 region 投递落点 + 全局桥 key 口径的服务内纯逻辑(nil-safe 接线)。
//
// 背景(scale-cellular-20m.md §4.4 / §5):每 region 一条区域消息总线,跨 region 仅必要弱实时
// 事件(好友 / 私聊)走全局桥(跨 region Kafka topic,key=接收方 player_id),投递对方 region 的
// social/push,频率低、秒级延迟可接受;禁止跨 region 强一致 owner 写(§5.3 红线③)。
//
// 本文件只落服务内纯逻辑,不改现状单总线推送实现:
//   - 统一私聊跨 region 桥的 partition key 口径(PrivateBridgeKey),固定 = 接收方 player_id
//     (§4.4),保证同一接收方私聊有序(对齐不变量 §9 kafka key=业务实体 ID)。
//   - 用确定性 cellroute.Router 把发件方 / 收件方解析到各自 owner region,判定一条私聊是否
//     需走跨 region 桥(CrossRegionPrivate),作为可观测信号(评估跨 region 私聊占比)。
//
// 边界(AGENTS.md §11.1):真正的跨 region Kafka 桥 topic / 区域总线拆分 / 对端 region 投递属
// 基础设施,由 Codex/人接;本轮 router 为 nil(单 Cell)时行为不变,只在注入后打观测日志。
package biz

import (
	"context"
	"fmt"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// PrivateBridgeKey 是私聊跨 region 桥的 partition key 口径。
//
// 口径统一:固定 = 接收方 player_id 的十进制字符串(§4.4「key=接收方 player_id」)。
// 跨 region 桥 at-least-once 重投时,同一接收方所有私聊落同一 partition → 有序投递
// (对齐不变量 §9 kafka key=业务实体 ID)。纯函数,确定性;to_player_id 为 0(世界广播)
// 不应走此键,调用方须先确保是私聊。
func PrivateBridgeKey(toPlayerID uint64) string {
	return fmt.Sprintf("%d", toPlayerID)
}

// PrivatePeers 是一条私聊的发件方 / 收件方 owner region 落点(只取跨 region 判定需要的维度)。
type PrivatePeers struct {
	SenderRegionID uint32
	TargetRegionID uint32
}

// CrossRegionPrivate 判断一条私聊是否需走跨 region 全局桥(发件方与收件方 owner region 不同)。
// 同 region(区域总线本地投递)→ false。
func (p PrivatePeers) CrossRegionPrivate() bool {
	return p.SenderRegionID != p.TargetRegionID
}

// privatePeers 解析一条私聊发件方 / 收件方的 owner region。
// router 为 nil(单 Cell / dev)或任一方路由失败 / player_id 为 0 → 返回 (PrivatePeers{}, false),
// 调用方退化为不做观测(同 region 本地投递语义不变)。
func (u *ChatUsecase) privatePeers(senderID, targetID uint64) (PrivatePeers, bool) {
	if u.router == nil || senderID == 0 || targetID == 0 {
		return PrivatePeers{}, false
	}
	sloc, err := u.router.Route(senderID)
	if err != nil {
		return PrivatePeers{}, false
	}
	tloc, err := u.router.Route(targetID)
	if err != nil {
		return PrivatePeers{}, false
	}
	return PrivatePeers{SenderRegionID: sloc.RegionID, TargetRegionID: tloc.RegionID}, true
}

// logPrivateRouting 在 router 注入后,把一条私聊的跨 region 投递落点打成观测日志。
//
// 仅可观测,不改投递路径:跨 region Kafka 桥 / 区域总线拆分属基础设施(AGENTS.md §11.1,
// 由 Codex/人接);本处只暴露「本条私聊是否跨 region、走全局桥还是区域总线」信号,供区域总线
// 上线前评估跨 region 私聊占比。router 为 nil(单 Cell)时不调用此路径,行为不变。
// 跨 region 私聊额外带 bridge_key(PrivateBridgeKey 口径样例 = 接收方 player_id),作排障锚点。
func (u *ChatUsecase) logPrivateRouting(ctx context.Context, senderID, targetID uint64) {
	peers, ok := u.privatePeers(senderID, targetID)
	if !ok {
		// router 已注入且双方 id 非 0 时,!ok 只可能是 cellroute.Route 解析失败(路由表缺失/
		// 降级)——此前 err 被完全吞掉,跨 region 私聊落点观测会静默变盲,排障时无法区分
		// 「单 Cell 本就不打」与「router 解析炸了不打」。DEBUG 级即可(观测用途)。
		if u.router != nil && senderID != 0 && targetID != 0 {
			plog.With(ctx).Debugw("msg", "chat_private_route_failed",
				"sender_id", senderID, "target_id", targetID,
				"hint", "cellroute 解析失败,跨 region 私聊落点观测降级")
		}
		return
	}
	if peers.CrossRegionPrivate() {
		plog.With(ctx).Debugw("msg", "chat_private_routing",
			"cross_region", true,
			"sender_region", peers.SenderRegionID,
			"target_region", peers.TargetRegionID,
			"bridge_key", PrivateBridgeKey(targetID))
		return
	}
	plog.With(ctx).Debugw("msg", "chat_private_routing",
		"cross_region", false,
		"region", peers.SenderRegionID)
}
