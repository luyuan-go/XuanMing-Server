// Package offlinewatch 是「玩家离线满 N 秒后做点什么」的通用消费骨架。
//
// # 为什么需要它
//
// player_locator 能回答「此刻在不在线」,也能回答「最后一次被观测到离开是什么时候」,
// 但它不知道也不该知道各业务的阈值和动作(组队 180s 后退队、将来别的功能可能 30s)。
// 于是每个想按离线时长做决策的服务都要重复同一段:订事件 → 排到期 → 到点回查 → 动作。
// 本包把那段抽出来,业务只实现 Handler。
//
// # 唯一权威在 locator,本包只有可重建的调度状态
//
// 「玩家什么时候离开的」这个事实**只有 locator 一份**(§9.22 不重复影子状态)。
// 本包在 Redis 里存的 ZSET 只是「下次该复查谁」的调度提示:
//   - 独立 key(`pandora:offlinewatch:{ns}:due`),不寄生在任何有业务语义的索引上
//     (反例见 §16.10:ds_allocator 曾把退避写进 active ZSET 的 last_heartbeat_ms);
//   - 丢了 / 清空了不影响正确性,只是那一次掉线少一个加速信号,由业务自己的兜底
//     复查路径(Inspect)补上;
//   - 因此重启、扩缩容、Redis 清库都不需要迁移它。
//
// # 三段链路
//
//	locator.ReportDisconnect ──kafka: pandora.player.presence──▶ Watcher.Enqueue
//	                                                                  │ ZADD GT(离开时刻+阈值)
//	                                                                  ▼
//	                                            Watcher.Sweep(ticker,只取到期项,有预算封顶)
//	                                                                  │ 回查 locator 权威
//	                                                                  ▼
//	                                     online→丢弃 / waiting→推迟 / offline→Handler / unknown→退避重试
//
// # 事件是加速器,不是唯一触发源
//
// kafka 会丢;Hub DS 整台挂掉时压根不会调 ReportDisconnect,永远不会有事件。
// 所以业务**必须**另有一条兜底:在自己本来就要读该实体的路径上调 Inspect 顺手复查
// (与 ListMyPendingInvites / ListTeamApplications 同一条原则:推送是加速器,拉取才是权威)。
// 只接 Enqueue 不接 Inspect 的用法会留下永远清不掉的残留。
package offlinewatch

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
)

// Handler 是业务侧要实现的唯一东西:某玩家已确认离线满阈值,去处理你自己的实体。
//
// 契约:
//   - **必须幂等**。事件 at-least-once、多副本各自扫、兜底复查也会重复触发,
//     同一玩家被调多次是常态而不是异常。
//   - 返回 nil = 处理完成(该玩家出调度队列)。
//   - 返回非 nil = 本次没处理成(依赖不可用等),骨架按退避重排,下轮再来。
//     **业务判定「不需要处理」时应返回 nil 而不是 error**(例:这个玩家根本不在任何队伍里),
//     否则会一直重试到保留期结束。
//   - 不要在 Handler 里再判一次「他是不是真离线」——骨架已经用 locator 权威判过了;
//     业务只需判自己那边的前置条件(比如这支队伍是不是正被一场对局占住)。
type Handler interface {
	OnPlayerOffline(ctx context.Context, playerID uint64, offlineSinceMs int64) error
}

// PresenceReader 是骨架对 locator 的只读依赖(抽成接口便于单测注入)。
//
// 两个方法都必须严格区分「查到了」「没查到」「查不通」三态:
// 整批失败一律返回 error,**绝不允许把查不通压成空 map**——那等价于宣布全体离线。
type PresenceReader interface {
	// BatchOnline 返回这批玩家里此刻在 locator 有位置记录的集合(在场即在线,
	// 不区分 HUB/MATCHING/BATTLE)。
	BatchOnline(ctx context.Context, playerIDs []uint64) (map[uint64]bool, error)
	// BatchLastSeen 返回有记录玩家的「最后一次被观测到离开 Hub 的时刻」(unix ms)。
	// 缺席 = UNKNOWN。
	BatchLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error)
}

// Options 是 Watcher 的配置。
type Options struct {
	// Namespace 区分不同业务的调度队列(Redis key 与 kafka consumer group 都用它)。
	// 必填,建议就用服务名(如 "team")。
	Namespace string

	// Threshold 离线多久才算数。必填(<=0 报错):这个值没有安全的默认,
	// 猜一个默认值等于替业务决定了什么时候踢人。
	Threshold time.Duration

	// Interval 复查轮询周期。默认 15s。
	//
	// 不必比阈值精细太多:上游的检测本身就有 ~60~90s 的模糊(UE 连接超时 60s +
	// locator TTL),把周期压到秒级只是成倍花钱买不到精度。
	Interval time.Duration

	// Budget 单轮最多处理多少个到期项。默认 200。
	// 防止 Redis / 下游抖动恢复后积压一次性打爆下游;超出的下轮继续。
	Budget int

	// BatchSize 单次向 locator 查询的玩家数。默认 500。
	BatchSize int

	// RetryBackoff 判定为 UNKNOWN 或 Handler 失败时,推迟多久再试。默认 = Interval。
	RetryBackoff time.Duration
}

func (o *Options) normalize() error {
	if o.Namespace == "" {
		return fmt.Errorf("offlinewatch: Namespace required")
	}
	if o.Threshold <= 0 {
		return fmt.Errorf("offlinewatch: Threshold must be > 0 (ns=%s)", o.Namespace)
	}
	if o.Interval <= 0 {
		o.Interval = 15 * time.Second
	}
	if o.Budget <= 0 {
		o.Budget = 200
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 500
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = o.Interval
	}
	return nil
}

// Watcher 是骨架本体。
type Watcher struct {
	rdb    redis.UniversalClient
	reader PresenceReader
	h      Handler
	opts   Options

	dueKey string

	// now 可注入,单测用受控时钟驱动 Sweep(不 sleep 真实时间)。
	now func() time.Time
}

// New 构造 Watcher。不启动任何后台循环(Start 才启)。
func New(rdb redis.UniversalClient, reader PresenceReader, h Handler, opts Options) (*Watcher, error) {
	if rdb == nil {
		return nil, fmt.Errorf("offlinewatch: redis client required")
	}
	if reader == nil {
		return nil, fmt.Errorf("offlinewatch: PresenceReader required")
	}
	if h == nil {
		return nil, fmt.Errorf("offlinewatch: Handler required")
	}
	if err := opts.normalize(); err != nil {
		return nil, err
	}
	return &Watcher{
		rdb:    rdb,
		reader: reader,
		h:      h,
		opts:   opts,
		// hash tag 括住 namespace:整个队列固定落一个 slot,ZRANGEBYSCORE 才能在
		// Redis Cluster 下正常工作(单 key 操作,无 CROSSSLOT)。
		dueKey: fmt.Sprintf("pandora:offlinewatch:{%s}:due", opts.Namespace),
		now:    time.Now,
	}, nil
}

// Start 挂上周期复查循环(复用 safego.Loop,不新建 timer 状态机,§15.2 / §16.10)。
// ctx 取消即停。
func (w *Watcher) Start(ctx context.Context) {
	plog.With(ctx).Infow("msg", "offlinewatch_started",
		"namespace", w.opts.Namespace,
		"threshold", w.opts.Threshold.String(),
		"interval", w.opts.Interval.String(),
		"budget", w.opts.Budget)
	safego.Loop(ctx, "offlinewatch_"+w.opts.Namespace, w.opts.Interval, w.Sweep)
}

// Enqueue 把一个玩家排进调度队列,到期时间 = 离开时刻 + 阈值。
//
// ZADD GT:只把到期时间**往后推**。重复 / 迟到的事件(kafka at-least-once、
// 同一玩家短时间内多次离开)只会让复查更晚发生,不会把已经排好的项拉早 ——
// 拉早意味着还没到阈值就去查,白查一轮。
func (w *Watcher) Enqueue(ctx context.Context, playerID uint64, leftAtMs int64) error {
	if playerID == 0 {
		return fmt.Errorf("offlinewatch: playerID must > 0")
	}
	if leftAtMs <= 0 {
		leftAtMs = w.now().UnixMilli()
	}
	dueMs := leftAtMs + w.opts.Threshold.Milliseconds()
	return w.rdb.ZAddArgs(ctx, w.dueKey, redis.ZAddArgs{
		GT:      true,
		Members: []redis.Z{{Score: float64(dueMs), Member: strconv.FormatUint(playerID, 10)}},
	}).Err()
}

// EnqueueDue 把一批玩家排成「立刻到期」,下一轮 Sweep 就会复查并动作。
//
// 给兜底路径用:业务在自己的读路径上用 Inspect 发现某些玩家已经超阈值了,但**不该在
// 读路径上同步做写动作**(读请求要快、要能在依赖抖动时照常返回快照)。于是把它们排进
// 队列,由 Sweep 在一个 Interval 内完成实际处理 —— 读路径保持只读。
//
// 用 ZADD(不带 GT):这里的意图恰恰是「尽快查」,不能被队列里某个更晚的旧到期时间压住。
func (w *Watcher) EnqueueDue(ctx context.Context, playerIDs []uint64) error {
	ids := dedupeIDs(playerIDs)
	if len(ids) == 0 {
		return nil
	}
	nowMs := w.now().UnixMilli()
	members := make([]redis.Z, 0, len(ids))
	for _, pid := range ids {
		members = append(members, redis.Z{Score: float64(nowMs), Member: strconv.FormatUint(pid, 10)})
	}
	return w.rdb.ZAddArgs(ctx, w.dueKey, redis.ZAddArgs{Members: members}).Err()
}

// Inspect 同步复查一批玩家(**兜底路径**,不依赖 kafka 事件)。
//
// 业务在自己本来就要读该实体的地方顺手调它:玩家一打开面板,残留的离线成员当场被
// 判出来,不用等事件也不用等 ticker。这条路径是事件丢失 / Hub DS 整台挂掉时的唯一补救。
//
// 返回 map 对每个入参玩家都有一项(查不通则整体返回 error,不返回半份结果 —— 半份
// 结果会让调用方把「没查到的」误当成 UNKNOWN 之外的什么)。
func (w *Watcher) Inspect(ctx context.Context, playerIDs []uint64) (map[uint64]Verdict, error) {
	out := make(map[uint64]Verdict, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	nowMs := w.now().UnixMilli()
	for _, chunk := range chunkIDs(dedupeIDs(playerIDs), w.opts.BatchSize) {
		online, lastSeen, err := w.readPresence(ctx, chunk)
		if err != nil {
			return nil, err
		}
		for _, pid := range chunk {
			v, _ := classify(nowMs, pid, online, lastSeen, 0, w.opts.Threshold)
			out[pid] = v
		}
	}
	return out, nil
}

// Sweep 跑一轮到期复查。导出是为了单测能用受控时钟直接驱动,不必等真实 ticker。
func (w *Watcher) Sweep(ctx context.Context) {
	now := w.now()
	nowMs := now.UnixMilli()

	due, err := w.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     w.dueKey,
		Start:   "-inf",
		Stop:    strconv.FormatInt(nowMs, 10),
		ByScore: true,
		Offset:  0,
		Count:   int64(w.opts.Budget),
	}).Result()
	if err != nil && err != redis.Nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_due_scan_failed",
			"namespace", w.opts.Namespace, "err", err)
		return
	}
	if len(due) == 0 {
		return
	}

	ids := make([]uint64, 0, len(due))
	for _, m := range due {
		pid, perr := strconv.ParseUint(m, 10, 64)
		if perr != nil || pid == 0 {
			// 坏成员:直接摘掉,不让它每轮占一个预算名额。
			w.remove(ctx, m)
			continue
		}
		ids = append(ids, pid)
	}
	if len(ids) == 0 {
		return
	}

	// 先把这批的到期时间推到 now+RetryBackoff 再处理:多副本同时扫到同一批时,
	// 后来者看到的到期时间已被推走,少一次重复 Handler 调用。
	// 这只是**调度提示,不是锁**——推失败、进程半路挂掉都只会导致重复处理,
	// 而 Handler 本来就要求幂等,不影响正确性。
	w.reschedule(ctx, ids, nowMs+w.opts.RetryBackoff.Milliseconds())

	online, lastSeen, err := w.readPresence(ctx, ids)
	if err != nil {
		// 查不通 = UNKNOWN,fail-closed:一个都不处理,等下轮(已推到 RetryBackoff 之后)。
		// 绝不能因为 locator 抖动就把这一整批当成离线。
		plog.With(ctx).Warnw("msg", "offlinewatch_presence_unavailable",
			"namespace", w.opts.Namespace, "count", len(ids), "err", err)
		return
	}

	var acted, online0, waiting, unknown, failed int
	for _, pid := range ids {
		verdict, sinceMs := classify(nowMs, pid, online, lastSeen, 0, w.opts.Threshold)
		switch verdict {
		case VerdictOnline:
			online0++
			w.remove(ctx, strconv.FormatUint(pid, 10))
		case VerdictWaiting:
			waiting++
			w.reschedule(ctx, []uint64{pid}, sinceMs+w.opts.Threshold.Milliseconds())
		case VerdictOffline:
			if herr := w.h.OnPlayerOffline(ctx, pid, sinceMs); herr != nil {
				failed++
				plog.With(ctx).Warnw("msg", "offlinewatch_handler_failed",
					"namespace", w.opts.Namespace, "player_id", pid, "err", herr)
				continue // 已推到 RetryBackoff 之后,下轮重试
			}
			acted++
			w.remove(ctx, strconv.FormatUint(pid, 10))
		default:
			// UNKNOWN:locator 答了(整批不可用会在上面就 return),只是这个玩家没有离开时刻。
			//
			// 出队而不是留着重试 —— 这是个**持久**条件不是抖动:离场事件与 last-seen 是
			// 同一次守卫通过时一起写的,所以能进队列却查不到时刻,只可能是时刻已超
			// last_seen_retention(例:本服务停了比保留期还久,重启后队列里全是陈年条目)。
			// 留着只会每轮白查一次、永不收敛,把队列变成一个只增不减的坑。
			//
			// 不动作是对的:不知道离开多久就动手等于猜。真有残留成员时,业务读路径的
			// Inspect 兜底会重新发现(那时若仍拿不到时刻,同样判 UNKNOWN 不动作 ——
			// 本就不该在没有依据的情况下踢人)。
			unknown++
			w.remove(ctx, strconv.FormatUint(pid, 10))
		}
	}

	plog.With(ctx).Infow("msg", "offlinewatch_swept",
		"namespace", w.opts.Namespace, "scanned", len(ids),
		"acted", acted, "online", online0, "waiting", waiting,
		"unknown", unknown, "handler_failed", failed,
		// 扫到预算上限说明还有积压,下轮继续;持续打满要么调大 Budget 要么查下游慢在哪。
		"budget_saturated", len(due) >= w.opts.Budget)
}

// readPresence 分批读 locator 的两份事实。任一批失败即整体失败(不返回半份结果)。
func (w *Watcher) readPresence(ctx context.Context, ids []uint64) (map[uint64]bool, map[uint64]int64, error) {
	online := make(map[uint64]bool, len(ids))
	lastSeen := make(map[uint64]int64, len(ids))
	for _, chunk := range chunkIDs(ids, w.opts.BatchSize) {
		o, err := w.reader.BatchOnline(ctx, chunk)
		if err != nil {
			return nil, nil, err
		}
		l, err := w.reader.BatchLastSeen(ctx, chunk)
		if err != nil {
			return nil, nil, err
		}
		for k, v := range o {
			online[k] = v
		}
		for k, v := range l {
			lastSeen[k] = v
		}
	}
	return online, lastSeen, nil
}

// reschedule 把一批成员的到期时间改成 dueMs(调度提示,非权威;失败只记日志)。
func (w *Watcher) reschedule(ctx context.Context, ids []uint64, dueMs int64) {
	if len(ids) == 0 {
		return
	}
	members := make([]redis.Z, 0, len(ids))
	for _, pid := range ids {
		members = append(members, redis.Z{Score: float64(dueMs), Member: strconv.FormatUint(pid, 10)})
	}
	// XX:只改已存在的成员。并发的 remove 之后不该被这里重新塞回队列。
	if err := w.rdb.ZAddArgs(ctx, w.dueKey, redis.ZAddArgs{XX: true, Members: members}).Err(); err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_reschedule_failed",
			"namespace", w.opts.Namespace, "count", len(ids), "err", err)
	}
}

func (w *Watcher) remove(ctx context.Context, member string) {
	if err := w.rdb.ZRem(ctx, w.dueKey, member).Err(); err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_remove_failed",
			"namespace", w.opts.Namespace, "member", member, "err", err)
	}
}

// NewConsumer 构造订阅 pandora.player.presence 的 kafka 消费者,把每条离场事件转成 Enqueue。
//
// 调用方拿到后自己 Start() / Close()(生命周期跟着服务走)。
// consumer group 用 namespace 隔离:每个业务各自消费全量事件,互不抢分区。
//
// 解码失败按 Poison 处理(直接进 DLQ,不重试)——格式坏了的消息重试多少次都还是坏的。
// Enqueue 失败(Redis 抖动)按普通错误返回,由消费者按 RetryPolicy 重试;彻底失败也
// 只是丢一个加速信号,业务的 Inspect 兜底仍会补上。
func (w *Watcher) NewConsumer(kcfg config.KafkaConfig, partitionCount int32) (*kafkax.KeyOrderedConsumer, error) {
	return kafkax.NewKeyOrderedConsumer(kafkax.ConsumerConfig{
		Brokers:        kcfg.Brokers,
		Topic:          kafkax.TopicPlayerPresence,
		GroupID:        "offlinewatch-" + w.opts.Namespace,
		PartitionCount: partitionCount,
	}, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var evt locatorv1.PlayerLeftHubEvent
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			return kafkax.Poison(err)
		}
		if evt.GetPlayerId() == 0 {
			return kafkax.Poison(fmt.Errorf("offlinewatch: event without player_id"))
		}
		return w.Enqueue(ctx, evt.GetPlayerId(), evt.GetLeftAtMs())
	})
}

// dedupeIDs 去重并剔除 0(保持首次出现顺序,便于测试断言稳定)。
func dedupeIDs(ids []uint64) []uint64 {
	out := make([]uint64, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// chunkIDs 按 size 切批。
func chunkIDs(ids []uint64, size int) [][]uint64 {
	if size <= 0 || len(ids) <= size {
		return [][]uint64{ids}
	}
	var out [][]uint64
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}
