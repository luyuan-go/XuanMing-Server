// presence.go — 好友在线态扇出优化 fan-out worker(2026-06-19)。
//
// 落地 docs/design/friend-distributed-scaling.md §13.4 / §13.5:
//   §13.4.1 只推订阅者:内存订阅倒排索引(watchedID → 订阅者集合);
//            好友上线事件只推给「此刻正盯着这一行看的人」,扇出从 N 降到个位数。
//   §13.4.2 去抖(debounce):变更进 debounce 窗口,窗口内回退到原状态判为抖动不推。
//   §13.4.3 合并(coalesce):tick(默认 1s)把同一订阅者的多条变更攒成一条 PresenceBatchEvent。
//   §13.4.4 降采样:只推粗粒度 PresenceStatus(在线/离线/游戏中),细节点详情再单独拉。
//   §13.5  洪峰降级:挂 pkg/killswitch,降级时丢事件退回纯拉模式,保主链路。
//
// 架构取舍(v1):订阅倒排索引是「单实例内存态」,与 push 服务的 ConnectionManager 同档
// (一个玩家的 stream / 订阅都粘在一个实例上)。多实例水平扩展需把倒排索引下沉 Redis
// (presence:sub:{watchedID} set),并让 SetLocation 的 presence 变更走 Kafka 分区(key=player_id)
// 到单一 fan-out 消费组——列为后续。本版默认 presence.enabled=false(§13.7 先拉后推)。
package biz

import (
	"context"
	"sync"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
)

// biz 层粗粒度在线态(§13.4.4 降采样;与 proto PresenceStatus 数值 1:1,service 层互转)。
const (
	PresenceUnspecified int32 = 0
	PresenceOffline     int32 = 1
	PresenceOnline      int32 = 2
	PresenceInGame      int32 = 3
)

// 默认窗口(无配置时)。
const (
	defaultDebounceWindow = 8 * time.Second
	defaultCoalesceTick   = 1 * time.Second
)

// coarsePresence 把细粒度 LocationState 映射到粗粒度在线态(§13.4.4)。
func coarsePresence(state int32) int32 {
	switch state {
	case LocationStateLoginPending, LocationStateHub:
		return PresenceOnline
	case LocationStateMatching, LocationStateBattle:
		return PresenceInGame
	default: // Unspecified / Offline
		return PresenceOffline
	}
}

// PresenceChangeOut 是推给订阅者的单条变更(biz→proto 由 pusher 适配)。
type PresenceChangeOut struct {
	PlayerID uint64
	Status   int32
	TsMs     int64
}

// PresencePusher 把合并后的一批变更推给某订阅者本人(适配 kafka→push 服务)。
type PresencePusher interface {
	PushPresence(ctx context.Context, subscriberID uint64, changes []PresenceChangeOut) error
}

// KillSwitchFunc 抽象洪峰降级判定(默认接 pkg/killswitch 全局,nil = 永不降级)。
// 返回 (disabled=true, reason) 时 fan-out 退回纯拉模式,丢弃在途事件。
type KillSwitchFunc func() (disabled bool, reason string)

// pendingChange 是去抖窗口内某玩家待结算的最新变更。
type pendingChange struct {
	status   int32
	tsMs     int64
	deadline time.Time // 首次变更时间 + debounce 窗口;窗口到点才结算广播
}

// PresenceHub 是 presence 订阅 + 去抖 + 合并 fan-out worker。
//
// 并发模型:Subscribe/Unsubscribe/Notify 与后台 tick 共用一把锁;推送 I/O 在锁外做。
type PresenceHub struct {
	pusher   PresencePusher
	ks       KillSwitchFunc
	debounce time.Duration
	tick     time.Duration
	clock    func() time.Time

	mu       sync.Mutex
	watchers map[uint64]map[uint64]struct{}            // watchedID → set(subscriberID):谁在关注 watchedID
	watching map[uint64]map[uint64]struct{}            // subscriberID → set(watchedID):订阅者清理用
	pending  map[uint64]pendingChange                  // watchedID → 去抖窗口内的最新变更
	lastSent map[uint64]int32                          // watchedID → 上次已广播的粗状态(去抖去重基线)
	buffer   map[uint64]map[uint64]PresenceChangeOut   // subscriberID → (watchedID → 合并变更)
	degraded bool                                      // 当前是否处降级(纯拉)态,用于日志去抖

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewPresenceHub 构造 fan-out worker(未启动;Start() 才起后台 tick)。
//
// debounce<=0 用 8s,tick<=0 用 1s。ks 可为 nil(永不降级)。
func NewPresenceHub(pusher PresencePusher, debounce, tick time.Duration, ks KillSwitchFunc) *PresenceHub {
	if debounce <= 0 {
		debounce = defaultDebounceWindow
	}
	if tick <= 0 {
		tick = defaultCoalesceTick
	}
	return &PresenceHub{
		pusher:   pusher,
		ks:       ks,
		debounce: debounce,
		tick:     tick,
		clock:    time.Now,
		watchers: map[uint64]map[uint64]struct{}{},
		watching: map[uint64]map[uint64]struct{}{},
		pending:  map[uint64]pendingChange{},
		lastSent: map[uint64]int32{},
		buffer:   map[uint64]map[uint64]PresenceChangeOut{},
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Subscribe 注册订阅者关注的一批好友(§13.4.1)。
//
// 「替换」语义:每次打开/刷新好友面板都发全量 watchedIDs,覆盖该订阅者的旧订阅。
// subscriberID==0 或空列表视为退订。
func (h *PresenceHub) Subscribe(subscriberID uint64, watchedIDs []uint64) {
	if subscriberID == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unsubscribeLocked(subscriberID)
	if len(watchedIDs) == 0 {
		return
	}
	set := make(map[uint64]struct{}, len(watchedIDs))
	for _, wid := range watchedIDs {
		if wid == 0 || wid == subscriberID {
			continue // 跳过非法 id 与自订阅
		}
		set[wid] = struct{}{}
		ws := h.watchers[wid]
		if ws == nil {
			ws = map[uint64]struct{}{}
			h.watchers[wid] = ws
		}
		ws[subscriberID] = struct{}{}
	}
	if len(set) > 0 {
		h.watching[subscriberID] = set
	}
}

// Unsubscribe 清掉订阅者的全部关注(关闭面板时调)。
func (h *PresenceHub) Unsubscribe(subscriberID uint64) {
	if subscriberID == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unsubscribeLocked(subscriberID)
}

// unsubscribeLocked 在持锁下移除订阅者的倒排索引项与合并缓冲。
func (h *PresenceHub) unsubscribeLocked(subscriberID uint64) {
	for wid := range h.watching[subscriberID] {
		if ws := h.watchers[wid]; ws != nil {
			delete(ws, subscriberID)
			if len(ws) == 0 {
				delete(h.watchers, wid)
			}
		}
	}
	delete(h.watching, subscriberID)
	delete(h.buffer, subscriberID)
}

// Notify 上报某玩家的(细粒度)位置变更;Hub 内部转粗粒度并进去抖窗口。
// 由 LocatorUsecase 在 SetLocation/ClearLocation 成功后调用,非阻塞。
func (h *PresenceHub) Notify(playerID uint64, state int32) {
	if playerID == 0 {
		return
	}
	status := coarsePresence(state)
	now := h.clock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.pending[playerID]; ok {
		// 去抖窗口内再次变更:保留原 deadline(持续 hold),只更新到最新状态/时间。
		cur.status = status
		cur.tsMs = now.UnixMilli()
		h.pending[playerID] = cur
		return
	}
	h.pending[playerID] = pendingChange{
		status:   status,
		tsMs:     now.UnixMilli(),
		deadline: now.Add(h.debounce),
	}
}

// Start 起后台 tick goroutine(去抖结算 + 合并 flush)。
func (h *PresenceHub) Start() {
	go func() {
		defer close(h.doneCh)
		t := time.NewTicker(h.tick)
		defer t.Stop()
		for {
			select {
			case <-h.stopCh:
				return
			case <-t.C:
				h.stepSafely(context.Background(), h.clock())
			}
		}
	}()
}

// stepSafely 单轮 tick 的 panic 兜底(压测审核【必修-6】):此前 step panic 会崩整个
// locator 进程(presence 只是可降级投影,不值一个进程)。recover 后跳过本轮,下 tick 继续;
// step 持有的 h.mu 在 panic 展开时由 defer 链保证已释放(step 内锁外推送,锁窗口无 I/O)。
func (h *PresenceHub) stepSafely(ctx context.Context, now time.Time) {
	defer safego.Recover(ctx, "presence_hub_tick")
	h.step(ctx, now)
}

// Close 停止后台 tick 并等其退出。
func (h *PresenceHub) Close() {
	select {
	case <-h.stopCh:
		// 已关闭
	default:
		close(h.stopCh)
	}
	<-h.doneCh
}

// presenceBatch 是一个订阅者本 tick 要收的合并批次。
type presenceBatch struct {
	sub     uint64
	changes []PresenceChangeOut
}

// step 是单次 tick 的核心(暴露为方法便于单测用受控时钟直接驱动):
//  1. killswitch 降级判定:降级 → 丢弃在途 pending/buffer,退纯拉;
//  2. 去抖结算:deadline 到点的 pending,与 lastSent 比对,变化才进合并缓冲;
//  3. 合并 flush:每个订阅者的缓冲攒成一条 PresenceBatchEvent,锁外推送。
func (h *PresenceHub) step(ctx context.Context, now time.Time) {
	batches := h.collectBatches(ctx, now)

	if h.pusher == nil {
		return
	}
	// 模式 C:本 flush 由 ticker(默认 1s)驱动,对每个有缓冲变更的订阅者各推一次。
	// push 不可用时逐订阅者打 Warn = 每秒数百条同因日志 → 改为本轮累加、轮末汇总一条。
	var failed, failedChanges int
	var firstErr error
	var sampleSub uint64
	for _, b := range batches {
		if err := h.pusher.PushPresence(ctx, b.sub, b.changes); err != nil {
			failed++
			failedChanges += len(b.changes)
			if firstErr == nil {
				firstErr, sampleSub = err, b.sub
			}
		}
	}
	if failed > 0 {
		plog.With(ctx).Warnw("msg", "presence_push_failed",
			"subscribers", len(batches), "failed", failed, "failed_changes", failedChanges,
			"sample_subscriber_id", sampleSub, "first_err", firstErr)
	}
}

// collectBatches 是 step 的持锁段:降级判定 + 去抖结算 + 抽批清缓冲。
// defer 解锁(而非显式 Unlock):stepSafely 的 recover 兜底要求锁区间内 panic 时
// 锁必须已释放,否则 Notify/Subscribe 会在悬挂的 mu 上永久阻塞(比崩进程更糟)。
func (h *PresenceHub) collectBatches(ctx context.Context, now time.Time) []presenceBatch {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 1. 洪峰降级(§13.5):退回纯拉,丢弃在途事件。
	if h.ks != nil {
		if disabled, reason := h.ks(); disabled {
			if !h.degraded {
				h.degraded = true
				plog.With(ctx).Warnw("msg", "presence_fanout_degraded", "reason", reason)
			}
			h.pending = map[uint64]pendingChange{}
			h.buffer = map[uint64]map[uint64]PresenceChangeOut{}
			return nil
		}
		if h.degraded {
			h.degraded = false
			plog.With(ctx).Infow("msg", "presence_fanout_recovered")
		}
	}

	// 2. 去抖结算:deadline 到点 → 取窗口内最终状态,与上次广播比对。
	for wid, pc := range h.pending {
		if pc.deadline.After(now) {
			continue // 窗口未到,继续 hold
		}
		delete(h.pending, wid)

		prev, ok := h.lastSent[wid]
		if !ok {
			prev = PresenceOffline // 缺省基线 = 离线
		}
		if pc.status == prev {
			continue // 抖动吸收 / 无净变化:不推
		}
		if pc.status == PresenceOffline {
			delete(h.lastSent, wid) // 离线 = 基线,回收内存(缺省即离线)
		} else {
			h.lastSent[wid] = pc.status
		}

		// 扇给当前订阅 wid 的人(通常 0~几个);进各订阅者的合并缓冲。
		change := PresenceChangeOut{PlayerID: wid, Status: pc.status, TsMs: pc.tsMs}
		for sub := range h.watchers[wid] {
			sb := h.buffer[sub]
			if sb == nil {
				sb = map[uint64]PresenceChangeOut{}
				h.buffer[sub] = sb
			}
			sb[wid] = change
		}
	}

	// 3. 抽出本 tick 要推的批次,清空缓冲(推送在锁外做)。
	var batches []presenceBatch
	for sub, sb := range h.buffer {
		if len(sb) == 0 {
			continue
		}
		changes := make([]PresenceChangeOut, 0, len(sb))
		for _, c := range sb {
			changes = append(changes, c)
		}
		batches = append(batches, presenceBatch{sub: sub, changes: changes})
	}
	h.buffer = map[uint64]map[uint64]PresenceChangeOut{}
	return batches
}
