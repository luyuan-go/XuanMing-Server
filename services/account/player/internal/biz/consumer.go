// consumer.go — kafka 消费 handler(W4 ④,2026-06-06)。
//
// player 订阅 pandora.player.update(battle_result 结算后发,单事件类型 topic:只承载
// PlayerUpdateEvent),解 proto → 幂等 UpdateMMR(idempotency_key=match_id,不变量 §2)。
// decode 失败 / 非法 event_type header 用 kafkax.Poison 包装(毒丸消息)→ 消费者直接投
// DLQ;业务瞬时错误走重试→耗尽进 DLQ(不丢 MMR 更新)。
package biz

import (
	"context"
	"strconv"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// PlayerUpdateHandler 返回 pandora.player.update 的消费 handler(幂等 UpdateMMR)。
//
// pandora.player.update 是单事件类型 topic(只承载 PlayerUpdateEvent;经验事件走独立
// topic pandora.player.experience,原因见 kafkax.TopicPlayerUpdate 注释)。本 handler
// 仍防御性校验 event_type header:
//   - 缺失 / "0" → 旧事件,正常处理(兼容旧 producer 不写 header);
//   - 合法非 0 → 不属本消费者的未来事件,跳过并告警(不得按 MMR 误解码);
//   - 存在但非法(非数字/溢出)→ 毒丸进 DLQ 留证,绝不能降级当旧事件解码。
func (u *PlayerUsecase) PlayerUpdateHandler() kafkax.Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		et, ok := headerEventType(msg.Headers)
		if !ok {
			return kafkax.Poison(errcode.New(errcode.ErrInvalidArg,
				"malformed event_type header on player.update offset=%d", msg.Offset))
		}
		if et != uint32(playerv1.PlayerPushEventType_PLAYER_PUSH_EVENT_TYPE_LEGACY_UPDATE) {
			// kafka key = player_id(§9.9):带上它,静默跳过才能事后按玩家 join。
			plog.With(ctx).Warnw("msg", "player_update_unexpected_event_type_skipped",
				"event_type", et, "offset", msg.Offset, "key", string(msg.Key))
			return nil
		}
		evt := &playerv1.PlayerUpdateEvent{}
		if err := proto.Unmarshal(msg.Value, evt); err != nil {
			return kafkax.Poison(errcode.New(errcode.ErrInvalidArg, "decode player.update offset=%d: %v", msg.Offset, err))
		}
		if evt.GetPlayerId() == 0 {
			plog.With(ctx).Warnw("msg", "player_update_missing_player_id", "offset", msg.Offset)
			return nil
		}
		if evt.GetMatchId() == 0 {
			// 幂等键缺失:无法保证不变量 §2,丢弃(battle_result 正常路径必带 match_id)
			plog.With(ctx).Warnw("msg", "player_update_missing_match_id",
				"player_id", evt.GetPlayerId(), "offset", msg.Offset)
			return nil
		}
		key := strconv.FormatUint(evt.GetMatchId(), 10)
		// 段位池随事件带来(battle_result 从 canonical BattleStorageRecord 定格值填);
		// 空 = 旧 battle_result 或旧对局,由 UpdateMMR 归一到默认池(§9.21)。
		_, _, err := u.UpdateMMR(ctx, evt.GetPlayerId(), evt.GetMmrDelta(), evt.GetReason(), key, evt.GetRatingPool())
		return err
	}
}

// headerEventType 从 kafka headers 解析 event_type。
// 返回 (值, 是否合法):缺失 → (0, true)(兼容旧 producer 不写 header);
// 存在且为合法 uint32 → (v, true);存在但非法(非数字/负数/溢出)→ (0, false),
// 调用方必须按毒丸处理,不得降级当旧事件解码(误解码会污染 MMR)。
func headerEventType(headers []*sarama.RecordHeader) (uint32, bool) {
	for _, h := range headers {
		if h == nil || string(h.Key) != kafkax.HeaderEventType {
			continue
		}
		v, err := strconv.ParseUint(string(h.Value), 10, 32)
		if err != nil {
			return 0, false
		}
		return uint32(v), true
	}
	return 0, true
}
