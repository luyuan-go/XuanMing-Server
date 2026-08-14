// consumer.go — kafka 消费 handler(W4 ③,2026-06-06)。
//
// battle_result 订阅两个 topic,各用一个 kafkax.KeyOrderedConsumer(cmd 层装配),
// handler 在此定义:解 proto → 调 usecase。decode 失败用 kafkax.Poison 包装
// (毒丸消息,重试无意义)→ 消费者直接投 DLQ;业务瞬时错误走重试→耗尽进 DLQ。
package biz

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

// BattleResultHandler 返回旧 pandora.battle.result 的消费 handler(幂等落库 + MMR)。
// 该消息不携带 Model-B credential；authority_mode=redis 时 Config 启动校验禁止注册本 handler，
// 结算唯一入口改为受 Guard + Redis active + receipt 保护的同步 ReportResult RPC。
func (u *BattleResultUsecase) BattleResultHandler() kafkax.Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		result := &battlev1.BattleResult{}
		if err := proto.Unmarshal(msg.Value, result); err != nil {
			return kafkax.Poison(errcode.New(errcode.ErrBattleResultDecode, "decode battle.result offset=%d: %v", msg.Offset, err))
		}
		// 同 DSLifecycleHandler:kafka 消费面无 trace_id,现铸一个并写入 match_id join key。
		ctx = plog.WithMatchID(plog.WithTraceID(ctx, uuid.NewString()), result.GetMatchId())
		_, err := u.ReportResult(ctx, result, 0)
		return err
	}
}

// DSLifecycleHandler 返回 pandora.ds.lifecycle 的消费 handler。
// 只处理 ABANDONED(W4 ③ 补偿,不变量 §4),其余阶段忽略。
func (u *BattleResultUsecase) DSLifecycleHandler() kafkax.Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		evt := &dsv1.DSLifecycleEvent{}
		if err := proto.Unmarshal(msg.Value, evt); err != nil {
			return kafkax.Poison(errcode.New(errcode.ErrBattleResultDecode, "decode ds.lifecycle offset=%d: %v", msg.Offset, err))
		}
		if evt.GetPhase() != dsv1.DSLifecyclePhase_DS_LIFECYCLE_PHASE_ABANDONED {
			plog.With(ctx).Debugw("msg", "ds_lifecycle_ignored", "phase", evt.GetPhase().String(), "match_id", evt.GetMatchId())
			return nil
		}
		// kafkax 不透传 trace_id(pkg/kafkax 里没有任何 trace 接线),消费侧 ctx 恒空。
		// DS 崩溃补偿是「打完没结算」的另一半答案,必须能按 match_id 查、按 trace_id 串起
		// 本次消费产生的全部下游写(§9.8 / §11.3 R3)。同一 offset 重投会拿到新 trace_id,
		// 跨重试的关联键是 match_id。
		ctx = plog.WithMatchID(plog.WithTraceID(ctx, uuid.NewString()), evt.GetMatchId())
		plog.With(ctx).Infow("msg", "ds_lifecycle_abandoned_received",
			"match_id", evt.GetMatchId(), "players", len(evt.GetPlayerIds()),
			"map_id", evt.GetMapId(), "game_mode", evt.GetGameMode(),
			"ts_ms", evt.GetTsMs(), "kafka_offset", msg.Offset)
		return u.HandleAbandoned(ctx, evt.GetMatchId(), evt.GetPlayerIds(), evt.GetMapId(), evt.GetGameMode(), evt.GetTsMs())
	}
}
