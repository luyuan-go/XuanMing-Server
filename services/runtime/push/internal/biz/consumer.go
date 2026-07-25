// Package biz —— W3 ④(2026-06-05)KafkaConsumer 包装层。
//
// 职责:订阅一个 push topic,把 kafka 消息转为 PushFrame,在线玩家路由到 ConnectionManager.SendTo,
// 离线玩家写入 OfflineCacheRepo(redis ZSET)。
//
// 设计要点:
//   - **每个 topic 一个 consumer**,共享同一 GroupID(简化,后期可重构为单 consumer 多 topic)
//   - kafka key 必须是 strconv.FormatUint(player_id, 10)(不变量 §9);非数字 key 作为毒丸进 DLQ 留证,
//     避免单条脏数据静默丢失或阻塞整个 partition
//   - trace_id 从 sarama.Headers["trace_id"] 取(业务 producer 没塞则空,允许)
//   - Handler 返回非 nil → kafkax 按 RetryPolicy 重试,耗尽后投 DLQ(main.go 装配,2026-07-06)
//
// 不变量 §1 落地:同 player_id 多 partition 路由不会发生(一致性哈希),
// 同时 ConnectionManager.Register 已自动顶号(W2 ⑤),所以 SendTo 永远落到当前唯一 stream。
package biz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pushv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/push/v1"

	"github.com/luyuancpp/pandora/services/runtime/push/internal/data"
)

// FrameSender 抽象 ConnectionManager 的唤醒面(便于 consumer 单测注入)。
// SendTo 只传唤醒信号(帧本体已在投递缓冲);返回是否在线,仅作观测——不在线不是
// 错误,缓冲已有,轮询/重连恢复。跨 Pod / 跨 topic 定序由 Redis 单 key Lua 保证,
// 进程内不再需要玩家锁(审计 v2)。
type FrameSender interface {
	SendTo(playerID uint64) (online bool)
}

// FrameBroadcaster 抽象 ConnectionManager.Broadcast(广播类 topic 用,便于单测注入)。
type FrameBroadcaster interface {
	Broadcast(frame *pushv1.PushFrame) (sent int, failed int)
}

// FrameRouter 是 SendTo + Broadcast 的并集;ConnectionManager 同时满足两者。
// KafkaConsumer 按 topic 是否广播类选择 SendTo / Broadcast。
type FrameRouter interface {
	FrameSender
	FrameBroadcaster
}

// WakePublisher 跨 Pod 唤醒信号发布端(R5 复审 P2-10 + 复审 P1-5:写完投递缓冲后**无条件**
// 广播 player_id——不以「本地有连接」抑制,因本地 slot 可能是被顶号的陈旧残留;持有该玩家
// 真连接的 Pod 收到后立即拉取投递,消除跨 Pod 场景对 30s 兜底轮询的依赖)。
// best-effort:失败只记日志,交付正确性由缓冲 + 轮询保证。nil = 未装配(单测/联调)。
type WakePublisher interface {
	PublishWake(ctx context.Context, playerID uint64) error
}

// KafkaConsumer 包装一个 topic 的消费循环。
type KafkaConsumer struct {
	topic     string
	broadcast bool // 广播类 topic(chat.world / system.notify):走 cm.Broadcast,不按 player_id key 解析
	cm        FrameRouter
	offline   data.OfflineCacheRepo
	consumer  *kafkax.KeyOrderedConsumer

	// router / selfRegion / selfCell:本 push 实例所在 cell 的身份 + 确定性路由器
	// (scale-cellular-20m.md §4.2)。router 为 nil = 单 Cell,本实例拥有全部玩家,handle 不做
	// 归属漂移告警(行为不变)。分片部署时由 main 经 SetCellOwnership 注入。nil-safe。
	router     *cellroute.Router
	selfRegion uint32
	selfCell   uint32

	// wake 跨 Pod 唤醒信号发布端(R5 复审 P2-10);nil = 未装配,跨 Pod 只剩兜底轮询。
	wake WakePublisher

	// 依赖降级日志限流(模式 C):handle 是**每条 kafka 消息**执行一次的热路径,
	// Redis / pub-sub 一挂就会按消息速率刷屏(定向消息速率 × Pod 数,delivery 那条还
	// 要乘 kafkax 进程内重试次数)。这三个限流器把「逐条打」压成「首错 + 每
	// degradeLogWindowMs 一条 + 期间累计计数 + 恢复时一条」,信号不丢、行数有界。
	// 每实例(= 每 topic)各一份,无 key ⇒ 内存天然有界;handle 按 partition 并发,
	// plog.Window 内部用 atomic 保证并发安全。
	wakeFailLog     plog.Window
	bufferFailLog   plog.Window
	bcastDroppedLog plog.Window
}

// degradeLogWindowMs 是依赖降级日志的最小重打间隔(毫秒)。
const degradeLogWindowMs = 5000

// SetWakePublisher 注入跨 Pod 唤醒信号发布端(main 装配;nil-safe)。
func (k *KafkaConsumer) SetWakePublisher(w WakePublisher) { k.wake = w }

// NewKafkaConsumer 构造但不启动;调用 Start() 才开始消费。
//
// brokers / groupID / partitionCnt 由 cfg.Kafka 提供。
// 广播类 topic(kafkax.IsBroadcastTopic)在 handle 里走 cm.Broadcast,不依赖 player_id key。
// retry / dlq:业务瞬时错误(典型是 offline.Append 碰到 redis 抖动)进程内重试,
// 耗尽后投 DLQ(可回放,不再静默丢帧);dlq 为 nil 时退化为 log+ack(仅供单测/联调)。
func NewKafkaConsumer(
	brokers []string,
	groupID string,
	topic string,
	partitionCnt int32,
	cm FrameRouter,
	offline data.OfflineCacheRepo,
	retry kafkax.RetryPolicy,
	dlq kafkax.DLQProducer,
) (*KafkaConsumer, error) {
	if cm == nil {
		return nil, errors.New("FrameRouter must not be nil")
	}
	if offline == nil {
		return nil, errors.New("OfflineCacheRepo must not be nil")
	}
	if topic == "" {
		return nil, errors.New("topic must not be empty")
	}

	kc := &KafkaConsumer{topic: topic, broadcast: kafkax.IsBroadcastTopic(topic), cm: cm, offline: offline}

	initialOffset := int64(0) // 缺省 OffsetOldest(定向 topic:组内断点续传)
	if kc.broadcast {
		// 广播 topic 每 Pod 独立 consumer group(审计 P1:共享 group 只有一个 Pod 消费
		// 到广播,其他 Pod 上的在线玩家收不到;广播不缓存,必须每 Pod 都消费)。
		// fresh group 从 Newest 起消费:广播丢失容忍,新 Pod 不回放历史公告刷屏。
		// 同时关闭 offset 提交(审计 R4 P1 回滚重放):group 名按 hostname 派生,Pod
		// 重启同名(StatefulSet/主机名复用)时若存在 committed offset,sarama 会忽略
		// Newest 从旧位点续读,把停机窗口积压的广播整段重放给全部在线连接。纯实时
		// 消费不留位点,每次启动恒从 Newest 开始,停机窗口内广播有意丢弃(契约)。
		host, herr := os.Hostname()
		if herr != nil || host == "" {
			host = fmt.Sprintf("anon-%d", time.Now().UnixNano())
		}
		groupID = groupID + "-bcast-" + host
		initialOffset = sarama.OffsetNewest
	}

	c, err := kafkax.NewKeyOrderedConsumer(kafkax.ConsumerConfig{
		Brokers:             brokers,
		Topic:               topic,
		GroupID:             groupID,
		PartitionCount:      partitionCnt,
		RetryPolicy:         retry,
		DLQ:                 dlq,
		InitialOffset:       initialOffset,
		DisableOffsetCommit: kc.broadcast,
	}, kc.handle)
	if err != nil {
		return nil, err
	}
	kc.consumer = c
	return kc, nil
}

// Start 启动消费循环。
func (k *KafkaConsumer) Start() { k.consumer.Start() }

// Close 优雅关闭。
func (k *KafkaConsumer) Close() error { return k.consumer.Close() }

// handle 是单条 kafka 消息的处理逻辑;暴露为方法便于单测直接调。
//
// 返回值约定:
//   - nil → kafkax 走 ack(常规路径:在线发送成功、离线写入成功)
//   - kafkax.Poison(err) → key 非 player_id 数字(毒丸,重试无意义)直接投 DLQ 留证
//   - errcode.ErrPushOfflineCorrupted (9301) → offline.Append 失败(redis 不可达)。
//     kafkax 按 RetryPolicy 进程内重试(瞬时抖动自愈),耗尽后投 DLQ 可回放;
//     同时 pandora_push_offline_append_failed_total 计数器触发告警。
//
// W3 ④ 二次修复(Opus 审查 R2):offline.Append 失败由“只 log”升级为“metric 计数 + 返 errcode”。
// 2026-07-06 三次修复:接入 kafkax RetryPolicy + DLQ,彻底消除“redis down → ack 丢帧”。
func (k *KafkaConsumer) handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	h := plog.With(ctx)

	// 广播类 topic(chat.world / system.notify):key 为空,给本 Pod 全部在线玩家投递
	// (每 Pod 独立 consumer group,全 Pod 都消费同一条,见 NewKafkaConsumer)。
	// 不写投递缓冲(广播无 per-player 归属;离线玩家重连后不补推全服广播)。
	if k.broadcast {
		// event_type 存在但非法 → 毒丸(R5 复审 P2-3,见 parseEventTypeHeader 契约)。
		eventType, eterr := parseEventTypeHeader(msg.Headers)
		if eterr != nil {
			h.Warnw("msg", "kafka_push_invalid_event_type",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "err", eterr)
			return kafkax.Poison(eterr)
		}
		// ts_ms 置 0(审计 P1:客户端用所有帧的最大 ts_ms 推进恢复游标,广播若携带
		// kafka 时间戳会永久越过较小的玩家专属游标,导致定向帧被补推跳过。广播不参与
		// 游标体系;客户端 max(cursor, 0) 恒 no-op)。
		frame := &pushv1.PushFrame{
			Topic:     msg.Topic,
			Payload:   msg.Value,
			TsMs:      0,
			TraceId:   headerStr(msg.Headers, "trace_id"),
			EventType: eventType,
		}
		sent, failed := k.cm.Broadcast(frame)
		if failed > 0 {
			// 广播箱满即丢是显式契约(离线不补推);但世界频道 20 msg/s × 每 Pod 独立
			// consumer group 下,一个慢客户端就能持续刷屏。限流后首条即带累计丢弃帧数,
			// 比逐条打更能回答「跟不上广播的规模有多大」。
			total := k.bcastDroppedLog.AddExtra(int64(failed))
			if ok, msgs := k.bcastDroppedLog.Admit(time.Now().UnixMilli(), degradeLogWindowMs); ok {
				h.Warnw("msg", "push_broadcast_partial_dropped",
					"topic", msg.Topic, "sent", sent, "dropped", failed,
					"dropped_total", total, "msgs_with_drop", msgs)
			}
		} else if n, dropped := k.bcastDroppedLog.Recovered(); n > 0 {
			h.Infow("msg", "push_broadcast_drop_recovered",
				"topic", msg.Topic, "msgs_with_drop", n, "dropped_total", dropped)
		}
		return nil
	}

	// 1. 取 player_id(不变量 §9:key 必须是 player_id 序列化字符串)
	playerID, err := strconv.ParseUint(string(msg.Key), 10, 64)
	if err != nil {
		h.Warnw(
			"msg", "kafka_push_invalid_key",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
			"err", err,
		)
		return kafkax.Poison(err)
	}
	// player_id=0 不是合法业务 ID(Snowflake 恒非 0,§9.11):key="0" 可过 ParseUint,
	// 旧实现会写进 player 0 的缓冲并 ACK = 静默吞掉一条定向消息(R5 复审 P2-2)。
	// 毒丸进 DLQ 留证——producer 用零值 key 是 bug,必须暴露。
	if playerID == 0 {
		h.Warnw("msg", "kafka_push_zero_player_key",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		return kafkax.Poison(fmt.Errorf("kafka key resolves to player_id=0 (topic=%s)", msg.Topic))
	}

	// 2. Cell 归属:非本 cell 玩家的消息**毒丸投 DLQ,不本地处理**(审计 P1:本 cell
	// Redis 对连接所在 cell 不可见,"照常交付"实为写错缓存 + ACK = 静默丢;DLQ 留证
	// 可由基础设施 / 人工重投到 owner cell)。router 为 nil(单 Cell)不判定。
	// ⚠️ 诚实标注(审计 R4:路由闭环未实现):当前没有自动转投——业务生产者按
	// player_id 路由到正确 cell 的 kafka 集群是**部署面契约**(多 cell 上线前置项),
	// 本判定只是错配的兜底暴露(DLQ 告警 = 生产者路由或 cell 表配置有 bug),
	// 不是跨 cell 消息通道。单 Cell 部署(当前唯一形态)不受影响。
	if owner, owned, known := k.ownsPlayer(playerID); known && !owned {
		h.Errorw("msg", "push_player_not_owned_poisoned",
			"player_id", playerID, "topic", msg.Topic,
			"self_region", k.selfRegion, "self_cell", k.selfCell,
			"owner_region", owner.RegionID, "owner_cell", owner.CellID)
		return kafkax.Poison(fmt.Errorf("player %d owned by region=%d cell=%d, not self region=%d cell=%d",
			playerID, owner.RegionID, owner.CellID, k.selfRegion, k.selfCell))
	}

	// 3. 构 PushFrame(payload 直接是业务 Event proto bytes;ts_ms 初值为 kafka 消息
	// 时间,AssignAndBuffer 会重铸为该玩家的投递游标——原始事件时间由业务 payload 自带)。
	// event_type 存在但非法 → 毒丸(R5 复审 P2-3,见 parseEventTypeHeader 契约)。
	eventType, eterr := parseEventTypeHeader(msg.Headers)
	if eterr != nil {
		h.Warnw("msg", "kafka_push_invalid_event_type",
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset,
			"player_id", playerID, "err", eterr)
		return kafkax.Poison(eterr)
	}
	frame := &pushv1.PushFrame{
		Topic:     msg.Topic,
		Payload:   msg.Value,
		TsMs:      msg.Timestamp.UnixMilli(),
		TraceId:   headerStr(msg.Headers, "trace_id"),
		EventType: eventType,
	}

	// 4. 交付(审计 v2):① 单 Lua 原子「分配游标 + 入投递缓冲」(Redis 单点定序,
	// 跨 Pod / 跨 topic 并发安全,无进程锁;失败拒 ack → kafkax 重试/DLQ);
	// ② 唤醒本 Pod 连接写者拉取(跨 Pod 写入由写者定时轮询兜底)。
	// kafka 重投(ack 前崩溃 / rebalance)会给同一业务事件分配新游标 → 可能重复投递,
	// at-least-once 诚实契约(push.proto),业务侧幂等/按业务 ID 判重。
	if _, err := k.offline.AssignAndBuffer(ctx, playerID, frame); err != nil {
		// 指标逐次 Inc(告警走指标不走日志行数);日志按窗口限流:本条是 Error 级且
		// 返 errcode 会被 kafkax 按 RetryPolicy 重试,每次重试都会再走一遍 handle,
		// Redis 一挂就是「定向消息速率 ×(1+MaxRetries)」行 Error。
		// 重试/DLQ/返回码等不丢帧的正确性行为一字不动,只压日志。
		OfflineAppendFailed.WithLabelValues(msg.Topic).Inc()
		if ok, streak := k.bufferFailLog.Admit(time.Now().UnixMilli(), degradeLogWindowMs); ok {
			h.Errorw(
				"msg", "push_delivery_buffer_failed",
				"topic", msg.Topic,
				"player_id", playerID,
				"code", errcode.ErrPushOfflineCorrupted,
				"streak", streak,
				"err", err,
			)
		}
		return errcode.New(errcode.ErrPushOfflineCorrupted, "delivery buffer failed: %v", err)
	}
	if n, _ := k.bufferFailLog.Recovered(); n > 0 {
		h.Infow("msg", "push_delivery_buffer_recovered", "topic", msg.Topic, "failed_total", n)
	}
	// 唤醒(R5 复审 P2-10 + 复审 P1-5):先本地快路径唤醒,再**无条件**发跨 Pod 唤醒
	// 信号。不能以「本地有 slot」抑制跨 Pod 信号——本地 slot 可能是半死连接/已被顶号
	// 的陈旧残留(SendTo 只看索引,不验证连接活性),真持有新连接的 Pod 会被迫等
	// 30s 兜底轮询。wake 是 size-1 去重的幂等信号,重复唤醒零代价;PUBLISH 每消息
	// 一次,成本可接受。best-effort:publish 失败不影响 ack(帧已入缓冲,轮询必达)。
	k.cm.SendTo(playerID)
	if k.wake != nil {
		if werr := k.wake.PublishWake(ctx, playerID); werr != nil {
			// best-effort 通道(帧已入缓冲,30s 兜底轮询必达),但每条定向消息都发一次:
			// Redis pub/sub failover 期会按消息速率刷屏 → 限流为首错 + 每窗口一条。
			if ok, streak := k.wakeFailLog.Admit(time.Now().UnixMilli(), degradeLogWindowMs); ok {
				h.Warnw("msg", "push_wake_publish_failed",
					"topic", msg.Topic, "player_id", playerID, "streak", streak, "err", werr)
			}
		} else if n, _ := k.wakeFailLog.Recovered(); n > 0 {
			h.Infow("msg", "push_wake_publish_recovered", "topic", msg.Topic, "failed_total", n)
		}
	}
	return nil
}

// headerStr 在 sarama.RecordHeader 列表里找指定 key,返字符串(找不到返空)。
func headerStr(headers []*sarama.RecordHeader, key string) string {
	for _, h := range headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// parseEventTypeHeader 解析 event_type header(R5 复审 P2-3,收紧原 headerUint32 语义):
//   - 缺失 / 空值 → (0, nil):旧 producer 不填 event_type,legacy 0 是显式兼容契约;
//   - 存在但非法(非十进制 / 负数 / 越界)→ error:**不得降级为 legacy 0**——把新事件
//     按旧 message 路由,客户端会用错误的 proto 解析 payload(可能凑巧对上字段,
//     误弹提示 / 污染缓存)。producer 写坏 header 是 bug,毒丸进 DLQ 暴露并留证。
func parseEventTypeHeader(headers []*sarama.RecordHeader) (uint32, error) {
	// s 是目标 header 的十进制文本;空值同时覆盖 header 缺失和值为空(legacy 兼容)。
	s := headerStr(headers, kafkax.HeaderEventType)
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("malformed %s header %q: %w", kafkax.HeaderEventType, s, err)
	}
	// ParseUint 已完成 32 位范围校验,此处转换不会截断。
	return uint32(v), nil
}

// 让 *ConnectionManager 自动满足 FrameSender / FrameRouter(编译期检查)。
var (
	_ FrameSender = (*ConnectionManager)(nil)
	_ FrameRouter = (*ConnectionManager)(nil)
)
