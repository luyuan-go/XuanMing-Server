// Package data 是 trade 服务的数据层(订单存 Redis,2026-06-16)。
//
// Redis key 模板(所有业务 ID 用 uint64,%d 格式化):
//
//	pandora:trade:order:{%d}   → protobuf bytes(trade/v1.Order)
//	                             hashtag {} 确保同订单的 key 落同一 redis cluster slot(兜底)
//	pandora:trade:player:%d    → set(成员是 order_id,uint64 文本),供 ListMyOrders 反查;
//	                             写入经 ReserveOrderSlot(Lua SCARD+SADD)限额,不变量 §18
//
// 订单主体直接使用 proto trade/v1.Order 序列化为 bytes 存 Redis value:
//   - Order 已是完整的客户端可见结构,且无服务端独有隐藏字段,故存储 / 视图同构,
//     不再额外造 OrderStorageRecord(CLAUDE.md §5.10 仅在有存储独有字段时强制分离);
//   - 结算扣减的幂等键 = order_id,由 biz 层 ResourceLedger 保证,不落在 Order 里。
//
// 状态机写用 WATCH/MULTI/EXEC 乐观锁:
//
//	GET(proto bytes) → fn(modify) → MULTI/SET/EXEC
//	EXEC 失败(key 被并发改) → 重试至 maxRetry → 返 ErrTradeLockFailed(7005)
package data

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	tradev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/trade/v1"
)

// orderKey returns "pandora:trade:order:{orderID}" — hashtag 括住 orderID 保 cluster slot 一致。
func orderKey(orderID uint64) string {
	return fmt.Sprintf("pandora:trade:order:{%d}", orderID)
}

// playerKey returns "pandora:trade:player:playerID"(set of order_id)。
func playerKey(playerID uint64) string {
	return fmt.Sprintf("pandora:trade:player:%d", playerID)
}

// TradeRepo 是 trade 数据层抽象。biz 只依赖此接口,不依赖 redis。
type TradeRepo interface {
	// CreateOrder 只写订单主体 proto value(TTL=orderTTL),**不写反查索引**。
	// 写序铁律(镜像 team/matchmaker 的结论):先写主体、后 ReserveOrderSlot 预留索引名额。
	// 主体先落地时 orderID 是新发 snowflake、无人引用,天然安全;由此「索引成员指向 X 而 X
	// 主体不在」≡ 真死成员,配额清理(pruneDeadOrderSlots)绝不会误删 in-flight 预留。
	CreateOrder(ctx context.Context, order *tradev1.Order, orderTTL time.Duration) error

	// DeleteOrder 无条件删订单主体。仅供 CreateOrder 回滚(配额预留失败)使用:
	// 回滚时 orderID 尚未对外返回、反查索引未建,无条件 DEL 安全(镜像 team DeleteTeam)。
	DeleteOrder(ctx context.Context, orderID uint64) error

	// ReserveOrderSlot 原子预留玩家反查索引名额(Lua:SCARD < max 才 SADD+EXPIRE)。
	// 返 (false, nil) = 已满(不变量 §18 写入侧总量上限)。幂等:成员已在 → 直接成功。
	ReserveOrderSlot(ctx context.Context, playerID, orderID uint64, maxOrders int, ttl time.Duration) (bool, error)

	// ReleaseOrderSlot 从玩家反查索引里移除一个 order_id(SREM,幂等)。
	// 用于:① CreateOrder 失败回滚预留;② 配额满时清理已过期/已终态的死成员。
	ReleaseOrderSlot(ctx context.Context, playerID, orderID uint64) error

	// GetOrder 读订单。not found → (nil, false, nil)。
	GetOrder(ctx context.Context, orderID uint64) (*tradev1.Order, bool, error)

	// UpdateWithLock WATCH/MULTI/EXEC 读-改-写订单 value。
	//   fn 返回业务错误 → 透传不重试;EXEC 冲突 → 重试,耗尽返 ErrTradeLockFailed。
	UpdateWithLock(ctx context.Context, orderID uint64, maxRetry int, fn func(*tradev1.Order) error, orderTTL time.Duration) error

	// ListPlayerOrderIDs 读玩家 order set 里的全部 order_id。
	// 集合大小被 ReserveOrderSlot 的 max 硬上限兕定(默认 200),全量读安全。
	ListPlayerOrderIDs(ctx context.Context, playerID uint64) ([]uint64, error)
}

// RedisTradeRepo 是基于 go-redis/v9 的 TradeRepo 实现。
type RedisTradeRepo struct {
	rdb redis.UniversalClient
}

// NewRedisTradeRepo 构造。
func NewRedisTradeRepo(rdb redis.UniversalClient) *RedisTradeRepo {
	return &RedisTradeRepo{rdb: rdb}
}

// CreateOrder 只写订单主体(权威单键,单 slot 原子)。反查索引由 biz 层在主体落地后
// 经 ReserveOrderSlot 原子预留(含配额上限,不变量 §18),不在本方法内写。
// 注:资源扣减原子性(CLAUDE.md §9 #7)在 biz 层 ResourceLedger,不在本方法。
func (r *RedisTradeRepo) CreateOrder(ctx context.Context, order *tradev1.Order, orderTTL time.Duration) error {
	payload, err := proto.Marshal(order)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "marshal order %d: %v", order.GetOrderId(), err)
	}
	if err := r.rdb.Set(ctx, orderKey(order.GetOrderId()), payload, orderTTL).Err(); err != nil {
		return errcode.New(errcode.ErrInternal, "create order %d: %v", order.GetOrderId(), err)
	}
	return nil
}

// DeleteOrder 见接口注释:仅供创建回滚,无条件 DEL。
func (r *RedisTradeRepo) DeleteOrder(ctx context.Context, orderID uint64) error {
	return r.rdb.Del(ctx, orderKey(orderID)).Err()
}

// reserveOrderSlotScript 配额预留:成员已在 → 刷 TTL 幂等成功;SCARD 达上限 → 拒;
// 否则 SADD + PEXPIRE。单 key 单 slot,Cluster 安全。
var reserveOrderSlotScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return 1
end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1`)

// ReserveOrderSlot 见接口注释。
func (r *RedisTradeRepo) ReserveOrderSlot(ctx context.Context, playerID, orderID uint64, maxOrders int, ttl time.Duration) (bool, error) {
	res, err := reserveOrderSlotScript.Run(ctx, r.rdb,
		[]string{playerKey(playerID)},
		strconv.FormatUint(orderID, 10), maxOrders, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "reserve order slot player %d order %d: %v", playerID, orderID, err)
	}
	return res == 1, nil
}

// ReleaseOrderSlot 见接口注释。SREM 幂等。
func (r *RedisTradeRepo) ReleaseOrderSlot(ctx context.Context, playerID, orderID uint64) error {
	return r.rdb.SRem(ctx, playerKey(playerID), strconv.FormatUint(orderID, 10)).Err()
}

func (r *RedisTradeRepo) GetOrder(ctx context.Context, orderID uint64) (*tradev1.Order, bool, error) {
	b, err := r.rdb.Get(ctx, orderKey(orderID)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "get order %d: %v", orderID, err)
	}
	order := &tradev1.Order{}
	if err := proto.Unmarshal(b, order); err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "unmarshal order %d: %v", orderID, err)
	}
	return order, true, nil
}

func (r *RedisTradeRepo) UpdateWithLock(
	ctx context.Context,
	orderID uint64,
	maxRetry int,
	fn func(*tradev1.Order) error,
	orderTTL time.Duration,
) error {
	key := orderKey(orderID)

	for attempt := 0; attempt <= maxRetry; attempt++ {
		var fnErr error

		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			b, err := tx.Get(ctx, key).Bytes()
			if err == redis.Nil {
				return errcode.New(errcode.ErrTradeOrderNotFound, "order %d not found", orderID)
			}
			if err != nil {
				return err
			}
			order := &tradev1.Order{}
			if err := proto.Unmarshal(b, order); err != nil {
				return errcode.New(errcode.ErrInternal, "unmarshal order %d: %v", orderID, err)
			}

			if fnErr = fn(order); fnErr != nil {
				return fnErr
			}

			payload, err := proto.Marshal(order)
			if err != nil {
				return errcode.New(errcode.ErrInternal, "marshal order %d: %v", orderID, err)
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, payload, orderTTL)
				return nil
			})
			return err
		}, key)

		if txErr == nil {
			return nil
		}
		// fn 自身返回的业务错误 — 不重试,直接透传。
		if fnErr != nil && txErr == fnErr {
			return fnErr
		}
		// WATCH 冲突 — 重试。
		if txErr == redis.TxFailedErr {
			continue
		}
		// 其他 redis 错误 — 不重试。
		return txErr
	}
	// WATCH/MULTI/EXEC 乐观锁重试耗尽:热点订单锁竞争(§16 TOCTOU / 重试耗尽盲点),
	// 经 in-band 码返回被 access log 记成泛化失败,无法与其它失败区分 → WARN 带 order_id + 竞争强度。
	plog.With(ctx).Warnw("msg", "trade_update_lock_exhausted", "order_id", orderID, "max_retry", maxRetry)
	return errcode.New(errcode.ErrTradeLockFailed, "order %d update concurrent retry exhausted", orderID)
}

func (r *RedisTradeRepo) ListPlayerOrderIDs(ctx context.Context, playerID uint64) ([]uint64, error) {
	members, err := r.rdb.SMembers(ctx, playerKey(playerID)).Result()
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list player orders %d: %v", playerID, err)
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		id, perr := strconv.ParseUint(m, 10, 64)
		if perr != nil {
			continue // 跳过脏成员
		}
		ids = append(ids, id)
	}
	return ids, nil
}
