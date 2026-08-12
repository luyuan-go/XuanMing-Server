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
// 「玩家什么时候离开的」这个权威事实仍只有 locator 一份(§9.22 不重复影子状态)。
// 本包只存可重建的复查状态:
//   - due ZSET 是「下次该复查谁」;evidence HASH 只保存事件 / locator last-seen;
//     locator key miss 不能证明持续离线,不得用本地时钟补 evidence;
//   - 独立 key(`pandora:offlinewatch:{ns}:due|evidence`),不寄生在业务索引上
//     (反例见 §16.10:ds_allocator 曾把退避写进 active ZSET 的 last_heartbeat_ms);
//   - 丢了 / 清空了只会 fail-closed 延迟动作;若 locator 仍有 last-seen,业务兜底
//     路径 Observe 可重建排期,没有权威依据时则保持 UNKNOWN;
//   - 因此重启、扩缩容、Redis 清库都不需要迁移它。
//
// # 三段链路
//
//	locator.ReportDisconnect ──kafka: pandora.player.presence──▶ Watcher.Enqueue
//	                                                                  │ evidence + due 原子排期
//	                                                                  ▼
//	                                            Watcher.Sweep(ticker,只取到期项,有预算封顶)
//	                                                                  │ 回查 locator 权威
//	                                                                  ▼
//	                              online→条件清理 / waiting→推迟 / offline→Handler / error→退避重试
//
// # 事件是加速器,不是唯一触发源
//
// kafka 会丢,所以业务**必须**另有一条兜底:在自己本来就要读该实体的路径上调 Observe,
// 用 locator 保留的权威 last-seen 补排(推送是加速器,拉取才是权威)。Hub DS 整台挂掉时
// 可能既没有事件也没有 last-seen;单纯 key miss 无法排除期间发生过未观测重连,本包会
// 保持 UNKNOWN。若产品要求该场景自动清理,必须另接 owner fencing / Hub 死亡 roster 等
// 权威信号,不能让 Observe 用本地时钟猜。
//
// 本包只是触发 / 观察深模块,不拥有业务写权威。最终 locator 复核与随后 Handler 之间
// 仍是跨服务窗口;破坏性业务必须另有版本、CAS 或 operation fence 闭环。
package offlinewatch

import (
	"context"
	"errors"
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
//   - 返回非 nil = 本次没处理成(依赖不可用等),骨架按退避重排,下轮再来;
//     已知的业务暂缓条件用 ErrDeferred(或包装它),避免打成故障告警。
//     **业务判定「不需要处理」时应返回 nil 而不是 error**(例:这个玩家根本不在任何队伍里),
//     否则会一直重试到保留期结束。
//   - 必须遵守 ctx deadline。不要在 Handler 里再判一次「他是不是真离线」——骨架已在
//     调用前做最终 locator 复核;但这次读取**不能**与另一服务的业务写组成线性化点,
//     只能缩小竞态窗口。破坏性 Handler 仍必须在自己的权威写路径用版本 / CAS /
//     operation fence,与重连、开局等竞争操作原子互斥;做不到就必须保持功能关闭。
type Handler interface {
	OnPlayerOffline(ctx context.Context, playerID uint64, offlineSinceMs int64) error
}

// ErrDeferred 表示业务前置条件当前不满足,但任务不能视为完成。
//
// 例如玩家所在队伍正被一场对局占住:本轮不能摘人,对局结束后仍要继续复查。
// Handler 可直接返回本值或包装本值;Watcher 会按 RetryBackoff 保留任务,但不会把它
// 当作依赖故障打 WARN。
var ErrDeferred = errors.New("offlinewatch: deferred")

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

	// RetryBackoff presence 查询 / Handler 失败或业务暂缓时,推迟多久再试。默认 = Interval。
	RetryBackoff time.Duration

	// RosterScanBatch 是每轮兜底提名的批量上限。默认 2000。
	//
	// 与 Budget 分开:Budget 管「到期项」(有依据、要动手),本值管「提名候选」
	// (只是拿去 Observe 判一判)。两者共享同一轮时间预算。
	//
	// **取值必须让全量扫描周期 <= Threshold**,否则残留成员最坏要等一个远超阈值的时间
	// 才轮得到复查(阈值 180s 却两小时才扫到,等于没生效):
	//
	//	RosterScanBatch >= 候选总量 / (Threshold / Interval)
	//
	// 例:10 万有队伍的玩家、Threshold=180s、Interval=15s → 12 轮扫完 → 每轮 >= 8334。
	// 默认 2000 覆盖到约 2.4 万候选;超过这个规模**必须**按上式调大,
	// 并用 RosterSource 侧的「完整遍历一轮」日志核对实际周期(见 team 的 teamRosterSource)。
	RosterScanBatch int

	// AttemptTimeout 限制单个玩家「最终 presence 复核 + Handler」一次尝试的总时长。
	// 默认 5s。PresenceReader 与 Handler 都必须遵守 ctx;本包不会在超时后遗弃仍在写的
	// goroutine,否则会制造后台僵尸写与下一轮重试并发执行。
	AttemptTimeout time.Duration
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
	if o.AttemptTimeout <= 0 {
		o.AttemptTimeout = 5 * time.Second
	}
	if o.RosterScanBatch <= 0 {
		o.RosterScanBatch = 2000
	}
	return nil
}

// Watcher 是骨架本体。
type Watcher struct {
	rdb    redis.UniversalClient
	reader PresenceReader
	h      Handler
	opts   Options

	dueKey      string
	evidenceKey string

	// roster 是可选的兜底候选源(见 RosterSource)。nil = 不启用。
	roster RosterSource

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
		// Redis Cluster 下正常工作。due + evidence 共用同一 hash tag,Lua 才能原子
		// claim / finish / retry,旧 Sweep 也不会覆盖或删除新一轮离线任务。
		dueKey:      fmt.Sprintf("pandora:offlinewatch:{%s}:due", opts.Namespace),
		evidenceKey: fmt.Sprintf("pandora:offlinewatch:{%s}:evidence", opts.Namespace),
		now:         time.Now,
	}, nil
}

// due 只表示「下一次何时尝试」,会因 claim / retry 改动;不能拿它冒充离线时刻。
// evidence 保存本轮由 locator last-seen / 离场事件给出的离线时刻。单纯 key miss 不能
// 证明玩家持续离线,不得拿本地 now 补 evidence。两键必须由 Lua 同步维护。
var (
	upsertEvidenceScript = redis.NewScript(`
local member = ARGV[1]
local candidate = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local current = tonumber(redis.call('HGET', KEYS[2], member))
if (not current) or candidate > current then
  -- 时间戳仍在 Lua number 的精确整数范围内，但 tostring(大数)可能输出科学计数法；
  -- evidence 是跨 Go/Lua 的十进制整数协议，必须原样保存调用方的 ARGV 字符串。
  redis.call('HSET', KEYS[2], member, ARGV[2])
  redis.call('ZADD', KEYS[1], candidate + threshold, member)
  return candidate
end
if not redis.call('ZSCORE', KEYS[1], member) then
  redis.call('ZADD', KEYS[1], current + threshold, member)
end
return current
`)

	claimScript = redis.NewScript(`
local member = ARGV[1]
local expected_due = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local claim_until = tonumber(ARGV[4])
local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
if (not current_due) or current_due ~= expected_due or current_due > now then
  return {0, 0}
end
local evidence = tonumber(redis.call('HGET', KEYS[2], member))
if not evidence then
  -- 兼容升级前只有 due、没有 evidence 的旧调度项。单纯 key miss 不能证明玩家
  -- 持续离线,也不能从本地 now 猜起点;条件摘掉无依据的调度提示,等待 Observe
  -- 将来拿到权威 last-seen 后重新排期。
  redis.call('ZREM', KEYS[1], member)
  return {2, 0}
end
redis.call('ZADD', KEYS[1], claim_until, member)
return {1, evidence}
`)

	finishIfEvidenceScript = redis.NewScript(`
local member = ARGV[1]
local expected = ARGV[2]
local expected_due = ARGV[3]
local current = redis.call('HGET', KEYS[2], member)
if expected == '' then
  if current then return 0 end
else
  if (not current) or tonumber(current) ~= tonumber(expected) then return 0 end
end
if expected_due ~= '' then
  local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
  if (not current_due) or current_due ~= tonumber(expected_due) then return 0 end
end
if current then redis.call('HDEL', KEYS[2], member) end
redis.call('ZREM', KEYS[1], member)
return 1
`)

	retryIfEvidenceScript = redis.NewScript(`
local member = ARGV[1]
local expected = tonumber(ARGV[2])
local expected_due = tonumber(ARGV[3])
local retry_at = tonumber(ARGV[4])
local current = tonumber(redis.call('HGET', KEYS[2], member))
if (not current) or current ~= expected then return 0 end
local current_due = tonumber(redis.call('ZSCORE', KEYS[1], member))
if (not current_due) or current_due ~= expected_due then return 0 end
redis.call('ZADD', KEYS[1], retry_at, member)
return 1
`)

	removeAllScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)
)

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

// Enqueue 记录离场事件并排期。较旧 / 重复事件不得倒退 evidence,也不得覆盖正在处理的
// 新一轮任务;这两个条件由同 slot Lua 一次完成。
//
// leftAtMs 同时是当前协议能提供的代次标识:相同毫秒的事件按 at-least-once 重投幂等
// 处理。若上游将来需要区分同一毫秒内两次真实会话切换,协议必须提供 operation_id;
// 本包不能在消费者侧凭空消除这种 ABA。
func (w *Watcher) Enqueue(ctx context.Context, playerID uint64, leftAtMs int64) error {
	if playerID == 0 {
		return fmt.Errorf("offlinewatch: playerID must > 0")
	}
	if leftAtMs <= 0 {
		return fmt.Errorf("offlinewatch: leftAtMs must > 0 (player=%d)", playerID)
	}
	_, err := w.upsertEvidence(ctx, playerID, leftAtMs)
	return err
}

// Observe 观测并排期一批玩家(**兜底路径**,不依赖 kafka 事件)。
//
// 「判定 + 排期」完全封装在本方法内,调用方拿不到中间判定或第二阶段排期入口,
// 因此以后新增业务只能走同一条安全路径。实际业务动作仍只在 Sweep。
//
// Hub DS 整机挂掉时没有 ReportDisconnect,locator 也可能没有 last-seen。此时 key miss
// 不能证明期间没有发生一次未被本 Watcher 观测到的重连,必须按 UNKNOWN fail-closed,
// 不排破坏性任务;只有权威 last-seen / 离场事件出现后才开始按 Threshold 判断。
func (w *Watcher) Observe(ctx context.Context, playerIDs []uint64) error {
	ids := dedupeIDs(playerIDs)
	if len(ids) == 0 {
		return nil
	}
	for _, chunk := range chunkIDs(ids, w.opts.BatchSize) {
		// 必须在 locator 读取前抓 evidence 版本。若读取期间发生新离场事件,在线分支只能
		// 条件清旧版本,不能无条件删掉刚排上的新任务。
		snapshot, err := w.readEvidence(ctx, chunk)
		if err != nil {
			return err
		}
		online, lastSeen, err := w.readPresence(ctx, chunk)
		if err != nil {
			return err
		}
		for _, pid := range chunk {
			if online[pid] {
				expected, ok := snapshot[pid]
				if err := w.finishObservedEvidence(ctx, pid, expected, ok); err != nil {
					return err
				}
				continue
			}

			if ms := lastSeen[pid]; ms > 0 {
				_, err = w.upsertEvidence(ctx, pid, ms)
			} else if observed, ok := snapshot[pid]; ok {
				// 已有 evidence 只能来自先前的权威 last-seen / 离场事件。复用 upsert
				// 同时修复极端情况下 evidence 存在但 due 缺失的调度索引漂移。
				_, err = w.upsertEvidence(ctx, pid, observed)
			} else {
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Sweep 跑一轮到期复查。导出是为了单测能用受控时钟直接驱动,不必等真实 ticker。
func (w *Watcher) Sweep(ctx context.Context) {
	nowMs := w.now().UnixMilli()

	due, err := w.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
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
		// **队列空恰恰是缺口场景**:整队一起掉线时既没有 kafka 事件、也没人打开面板,
		// 到期项自然为空。兜底提名必须在这条分支也跑,否则那些玩家永远排不进来。
		w.sweepRoster(ctx)
		return
	}

	var acted, online0, waiting, deferred, failed, claimed int
	for _, item := range due {
		member, ok := item.Member.(string)
		if !ok {
			member = fmt.Sprint(item.Member)
		}
		pid, perr := strconv.ParseUint(member, 10, 64)
		if perr != nil || pid == 0 {
			w.removeAll(ctx, member)
			continue
		}

		expectedDue := int64(item.Score)
		claimNow := w.now().UnixMilli()
		claimUntil := claimNow + w.opts.AttemptTimeout.Milliseconds() + w.opts.RetryBackoff.Milliseconds()
		status, evidence, cerr := w.claim(ctx, pid, expectedDue, claimNow, claimUntil)
		if cerr != nil {
			plog.With(ctx).Warnw("msg", "offlinewatch_claim_failed",
				"namespace", w.opts.Namespace, "player_id", pid, "err", cerr)
			continue
		}
		if status != 1 {
			// status=0:旧扫描结果,已被新事件 / 其他副本推进。
			// status=2:升级前旧 due 无 evidence,已 fail-closed 摘掉无依据提示;
			//           将来 Observe 拿到权威 last-seen 才会重新排期。
			continue
		}
		claimed++

		attemptCtx, cancel := context.WithTimeout(ctx, w.opts.AttemptTimeout)
		online, lastSeen, rerr := w.readPresence(attemptCtx, []uint64{pid})
		if rerr != nil {
			cancel()
			failed++
			w.retry(ctx, pid, evidence, claimUntil, w.now().UnixMilli()+w.opts.RetryBackoff.Milliseconds())
			plog.With(ctx).Warnw("msg", "offlinewatch_presence_unavailable",
				"namespace", w.opts.Namespace, "player_id", pid, "err", rerr)
			continue
		}

		// Handler 前再读一次 locator,只负责挡住已经完成的重连并缩小窗口。它不是
		// 跨服务事务或线性化点;破坏性 Handler 仍须在业务权威写路径自行做版本/CAS/fence。
		if online[pid] {
			cancel()
			online0++
			if err := w.finishClaim(ctx, pid, evidence, claimUntil); err != nil {
				plog.With(ctx).Warnw("msg", "offlinewatch_finish_online_failed",
					"namespace", w.opts.Namespace, "player_id", pid, "err", err)
			}
			continue
		}

		if latest := lastSeen[pid]; latest > evidence {
			// locator 看到了更晚一轮离场。推进 evidence 后交给下一轮,旧任务不能沿旧阈值动作。
			cancel()
			if _, err := w.upsertEvidence(ctx, pid, latest); err != nil {
				failed++
				plog.With(ctx).Warnw("msg", "offlinewatch_evidence_advance_failed",
					"namespace", w.opts.Namespace, "player_id", pid, "err", err)
			}
			continue
		}

		verdict, sinceMs := classify(w.now().UnixMilli(), pid, online, lastSeen, evidence, w.opts.Threshold)
		switch verdict {
		case verdictWaiting:
			cancel()
			waiting++
			w.retry(ctx, pid, evidence, claimUntil, sinceMs+w.opts.Threshold.Milliseconds())
		case verdictOffline:
			herr := w.h.OnPlayerOffline(attemptCtx, pid, sinceMs)
			cancel()
			if herr != nil {
				w.retry(ctx, pid, evidence, claimUntil, w.now().UnixMilli()+w.opts.RetryBackoff.Milliseconds())
				if errors.Is(herr, ErrDeferred) {
					deferred++
					plog.With(ctx).Debugw("msg", "offlinewatch_handler_deferred",
						"namespace", w.opts.Namespace, "player_id", pid, "err", herr)
					continue
				}
				failed++
				plog.With(ctx).Warnw("msg", "offlinewatch_handler_failed",
					"namespace", w.opts.Namespace, "player_id", pid, "err", herr)
				continue
			}
			acted++
			if err := w.finishClaim(ctx, pid, evidence, claimUntil); err != nil {
				plog.With(ctx).Warnw("msg", "offlinewatch_finish_failed",
					"namespace", w.opts.Namespace, "player_id", pid, "err", err)
			}
		default:
			cancel()
			failed++
			// claim 保证 evidence>0,正常不应到这里。保留任务而不是把不确定压成完成。
			w.retry(ctx, pid, evidence, claimUntil, w.now().UnixMilli()+w.opts.RetryBackoff.Milliseconds())
		}
	}

	plog.With(ctx).Infow("msg", "offlinewatch_swept",
		"namespace", w.opts.Namespace, "scanned", len(due), "claimed", claimed,
		"acted", acted, "online", online0, "waiting", waiting,
		"deferred", deferred, "failed", failed,
		// 扫到预算上限说明还有积压,下轮继续;持续打满要么调大 Budget 要么查下游慢在哪。
		"budget_saturated", len(due) >= w.opts.Budget)

	// 处理完到期项后再提名一批兜底候选,共享同一轮时间预算。
	w.sweepRoster(ctx)
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

func (w *Watcher) upsertEvidence(ctx context.Context, playerID uint64, candidateMs int64) (int64, error) {
	if playerID == 0 || candidateMs <= 0 {
		return 0, fmt.Errorf("offlinewatch: invalid evidence player=%d since=%d", playerID, candidateMs)
	}
	result, err := upsertEvidenceScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey},
		strconv.FormatUint(playerID, 10), candidateMs, w.opts.Threshold.Milliseconds()).Result()
	if err != nil {
		return 0, err
	}
	return redisResultInt64(result)
}

// readEvidence 返回读取时存在且合法的 evidence。坏值按错误返回,不能把损坏静默压成
// 「没有基线」;claim 路径另有保守迁移,读路径则应把异常暴露给运维。
func (w *Watcher) readEvidence(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	out := make(map[uint64]int64, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	fields := make([]string, 0, len(playerIDs))
	for _, pid := range playerIDs {
		fields = append(fields, strconv.FormatUint(pid, 10))
	}
	values, err := w.rdb.HMGet(ctx, w.evidenceKey, fields...).Result()
	if err != nil {
		return nil, err
	}
	for i, raw := range values {
		if raw == nil {
			continue
		}
		ms, err := redisResultInt64(raw)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("offlinewatch: bad evidence player=%d value=%v", playerIDs[i], raw)
		}
		out[playerIDs[i]] = ms
	}
	return out, nil
}

// claim 按扫描到的 expectedDue 做 CAS。status:0=已变化/被别的副本拿走;
// 1=本副本拿到;2=升级前旧 due 没有权威 evidence,已 fail-closed 摘掉提示。
func (w *Watcher) claim(ctx context.Context, playerID uint64, expectedDue, nowMs, claimUntilMs int64) (int64, int64, error) {
	result, err := claimScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey},
		strconv.FormatUint(playerID, 10), expectedDue, nowMs, claimUntilMs).Result()
	if err != nil {
		return 0, 0, err
	}
	items, ok := result.([]interface{})
	if !ok || len(items) != 2 {
		return 0, 0, fmt.Errorf("offlinewatch: bad claim result %T %v", result, result)
	}
	status, err := redisResultInt64(items[0])
	if err != nil {
		return 0, 0, err
	}
	evidence, err := redisResultInt64(items[1])
	if err != nil {
		return 0, 0, err
	}
	return status, evidence, nil
}

func (w *Watcher) finishObservedEvidence(ctx context.Context, playerID uint64, expected int64, exists bool) error {
	expectedArg := ""
	if exists {
		expectedArg = strconv.FormatInt(expected, 10)
	}
	_, err := finishIfEvidenceScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey},
		strconv.FormatUint(playerID, 10), expectedArg, "").Result()
	return err
}

func (w *Watcher) finishClaim(ctx context.Context, playerID uint64, expectedEvidence, expectedClaimUntil int64) error {
	_, err := finishIfEvidenceScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey},
		strconv.FormatUint(playerID, 10), expectedEvidence, expectedClaimUntil).Result()
	return err
}

func (w *Watcher) retry(ctx context.Context, playerID uint64, expectedEvidence, expectedClaimUntil, retryAtMs int64) {
	if _, err := retryIfEvidenceScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey},
		strconv.FormatUint(playerID, 10), expectedEvidence, expectedClaimUntil, retryAtMs).Result(); err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_retry_schedule_failed",
			"namespace", w.opts.Namespace, "player_id", playerID, "err", err)
	}
}

func (w *Watcher) removeAll(ctx context.Context, member string) {
	if _, err := removeAllScript.Run(ctx, w.rdb, []string{w.dueKey, w.evidenceKey}, member).Result(); err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_remove_failed",
			"namespace", w.opts.Namespace, "member", member, "err", err)
	}
}

func redisResultInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case uint64:
		if n > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("offlinewatch: redis integer overflows int64: %d", n)
		}
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	case []byte:
		return strconv.ParseInt(string(n), 10, 64)
	default:
		return 0, fmt.Errorf("offlinewatch: unexpected redis integer %T %v", v, v)
	}
}

// NewConsumer 构造订阅 pandora.player.presence 的 kafka 消费者,把每条离场事件转成 Enqueue。
//
// 调用方拿到后自己 Start() / Close()(生命周期跟着服务走)。
// consumer group 用 namespace 隔离:每个业务各自消费全量事件,互不抢分区。
//
// 解码失败按 Poison 处理(直接进 DLQ,不重试)——格式坏了的消息重试多少次都还是坏的。
// Enqueue 失败(Redis 抖动)按普通错误返回,由消费者按 RetryPolicy 重试;彻底失败也
// 只是丢一个加速信号,业务的 Observe 兜底仍会补上。
func (w *Watcher) NewConsumer(kcfg config.KafkaConfig, partitionCount int32) (*kafkax.KeyOrderedConsumer, error) {
	return kafkax.NewKeyOrderedConsumer(w.consumerConfig(kcfg, partitionCount), func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		var evt locatorv1.PlayerLeftHubEvent
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			return kafkax.Poison(err)
		}
		if evt.GetPlayerId() == 0 {
			return kafkax.Poison(fmt.Errorf("offlinewatch: event without player_id"))
		}
		if evt.GetLeftAtMs() <= 0 {
			// 没有权威离场时刻时不能用消费端 now 猜基线;这是永久坏消息,重试无意义。
			return kafkax.Poison(fmt.Errorf("offlinewatch: event without left_at_ms player=%d", evt.GetPlayerId()))
		}
		return w.Enqueue(ctx, evt.GetPlayerId(), evt.GetLeftAtMs())
	})
}

func (w *Watcher) consumerConfig(kcfg config.KafkaConfig, partitionCount int32) kafkax.ConsumerConfig {
	return kafkax.ConsumerConfig{
		Brokers:        kcfg.Brokers,
		Topic:          kafkax.TopicPlayerPresence,
		GroupID:        "offlinewatch-" + w.opts.Namespace,
		PartitionCount: partitionCount,
		// Enqueue 的 Redis 抖动是瞬时错误,不能像零值策略那样首次失败就直接丢事件。
		// 事件仍只是加速器,三次有限重试耗尽后由 Observe 兜底,不能无限阻塞 Kafka 分区。
		RetryPolicy: kafkax.RetryPolicy{MaxRetries: 3, Backoff: 200 * time.Millisecond},
	}
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

// RosterSource 是**可选**的兜底候选来源:每轮 Sweep 顺带拉一小批「本服务当前关心的玩家」
// 交给 Observe 判定并排期。
//
// # 为什么必须有它(2026-08-12 实测发现的缺口)
//
// 排期原本只有两条触发链,它们在同一种情况下会**同时失效**:
//   - kafka 离场事件:依赖 Hub DS 走完 Logout 调 ReportDisconnect。Hub DS 整机崩溃 /
//     被 OOM kill / 网络分区时压根不会调,事件永远不会产生;
//   - 读路径 Observe:依赖有活人调 GetMyTeam 打开面板。
//
// 于是「整支队伍一起掉线」就成了永久残留:没有事件,也没人打开面板,那些玩家永远
// 排不进复查队列。last-seen / last_alive_ms 解决的是「**能不能判**」,这里解决的是
// 「**谁来触发判**」—— 两者缺一都不成立。真实现场:两名玩家的 hubmeta 只有
// last_alive_ms 没有 left_at_ms(印证 Hub 侧没走 Logout),而调度队列是空的。
//
// 实现方自己维护游标做**增量**扫描,一轮只返回一小批;返回空切片表示本轮无候选。
// 判定与排期仍全部由 Observe 负责,本接口只负责"提名候选",不做任何判断。
type RosterSource interface {
	NextBatch(ctx context.Context, limit int) ([]uint64, error)
}

// SetRosterSource 注入兜底候选源。nil = 不启用(行为与历史一致)。
//
// 刻意复用 Sweep 已有的 ticker 而不是新起一个循环(§16.10:不得为此新建第二套
// timer 状态机);扫描量由 RosterScanBatch 封顶,与到期项共享同一轮的时间预算。
func (w *Watcher) SetRosterSource(src RosterSource) { w.roster = src }

// sweepRoster 是每轮 Sweep 末尾的兜底提名。全程 best-effort:
// 失败只记日志,绝不影响本轮到期项的处理(那才是主路径)。
func (w *Watcher) sweepRoster(ctx context.Context) {
	if w.roster == nil {
		return
	}
	ids, err := w.roster.NextBatch(ctx, w.opts.RosterScanBatch)
	if err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_roster_scan_failed",
			"namespace", w.opts.Namespace, "err", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	if err := w.Observe(ctx, ids); err != nil {
		plog.With(ctx).Warnw("msg", "offlinewatch_roster_observe_failed",
			"namespace", w.opts.Namespace, "count", len(ids), "err", err)
		return
	}
	plog.With(ctx).Debugw("msg", "offlinewatch_roster_observed",
		"namespace", w.opts.Namespace, "count", len(ids))
}
