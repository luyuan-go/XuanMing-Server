// Package biz 是 inventory 服务的业务逻辑层(W5 ③,2026-06-18)。
//
// 职责(docs/design/go-services.md 服务清册 economy 域;inventory 暂无专章,§2.9 是 trade):
//   - 背包道具持有 / 货币余额读
//   - 系统驱动幂等发放(GrantItems:战后掉落 / 活动 / 购买到账)
//   - 大厅态道具使用(UseItem:开箱 / 经验书)与出售换金币(SellItem)
//
// 边界(ds-arch.md §0.1):战斗内即时用道具 / 出装 / 购买道具走 UE GAS,不经 gRPC。
//
// 关键不变量(CLAUDE.md §9.7):发放 / 扣减必须原子 + 幂等键;校验数量在 data 层
// SELECT ... FOR UPDATE 锁行内做,避免并发超扣。usable / sellable 规则在 biz 层用配置裁决。
package biz

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// snowflakeGen 是 snowflake.Node 的最小接口(生成装备实例 instance_id,W5 ④)。
//
// GenerateInto 是批量预留:一次 CAS 拿整段,替代按件数循环调用 Generate。
// 语义与逐个 Generate 完全一致(严格递增、互不重复、可与 Generate 混用),
// 只保证「递增 + 唯一」,不保证相邻连续(跨秒会有空洞)。
type snowflakeGen interface {
	Generate() uint64
	GenerateInto(dst []uint64)
}

// InventoryUsecase 是 inventory 服务业务逻辑核心。
type InventoryUsecase struct {
	repo data.InventoryRepo
	cfg  conf.InventoryConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,拍卖成交跨分片对转落点观测退化为不打日志
	// (行为不变)。分片部署时由 main 经 SetCellRouter 注入,SettleAuctionMatch 成功后额外打一条
	// 跨分片对转落点观测(买卖双方跨 Cell → 拆 Kafka 结算出箱幂等消费)。nil-safe。
	router *cellroute.Router

	// sf 生成装备实例 instance_id(W5 ④)。可为 nil:未装配时 GrantInstances 返回
	// ErrInvalidArg(实例背包未启用),不影响堆叠计数背包(GrantItems/UseItem/SellItem)。
	// 由 main 经 SetSnowflake 注入(与 SetCellRouter 一致,避免旧调用点被迫改签名)。
	sf snowflakeGen

	// randIntn 返回 [0,n) 的随机整数(装备鉴定 roll 随机属性用,W5 ④)。
	// 默认全局 math/rand;测试经 SetRandSource 注入确定性序列。n<=0 时约定返回 0。
	randIntn func(n int64) int64
}

// NewInventoryUsecase 构造。
func NewInventoryUsecase(repo data.InventoryRepo, cfg conf.InventoryConf) *InventoryUsecase {
	return &InventoryUsecase{
		repo:     repo,
		cfg:      cfg,
		randIntn: defaultRandIntn,
	}
}

// defaultRandIntn 是默认随机源(全局 math/rand;n<=0 返回 0)。
func defaultRandIntn(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int63n(n)
}

// SetSnowflake 注入装备实例 ID 生成器(W5 ④)。nil-safe:不调用时实例背包禁用。
func (u *InventoryUsecase) SetSnowflake(sf snowflakeGen) { u.sf = sf }

// SetRandSource 注入鉴定 roll 随机源(测试用确定性序列;不调用则用默认 math/rand)。
func (u *InventoryUsecase) SetRandSource(fn func(n int64) int64) {
	if fn != nil {
		u.randIntn = fn
	}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),SettleAuctionMatch 不做跨分片对转落点
// 观测,行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result / friend / chat / trade / dialogue 一致)。Router 内部读路径无锁,并发安全。
func (u *InventoryUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// GetInventory 读玩家背包(货币 + 道具堆叠)。
func (u *InventoryUsecase) GetInventory(ctx context.Context, playerID uint64) (int64, []data.ItemStack, error) {
	if playerID == 0 {
		return 0, nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetInventory(ctx, playerID)
}

// GetInventoryFull 读玩家完整背包(货币 + 堆叠道具 + 容量 + 装备实例,W5 ④)。
// capacity 来自服务配置(所有玩家同一基础容量;<=0 表示未启用实例背包)。
// 未启用实例背包时不读 player_item_instance 表(既有库可能尚未迁移出该表,
// 避免 GetInventory 全量报内部错;启用时 main 启动期已做 schema 检查)。
func (u *InventoryUsecase) GetInventoryFull(ctx context.Context, playerID uint64) (int64, []data.ItemStack, int32, []data.ItemInstance, error) {
	if playerID == 0 {
		return 0, nil, 0, nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	gold, items, err := u.repo.GetInventory(ctx, playerID)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	if u.cfg.Capacity <= 0 {
		return gold, items, 0, nil, nil
	}
	instances, ierr := u.repo.ListInstances(ctx, playerID)
	if ierr != nil {
		return 0, nil, 0, nil, ierr
	}
	return gold, items, u.cfg.Capacity, instances, nil
}

// GrantInstances 幂等发放装备实例(系统驱动:掉落 / 活动 / 购买到账,W5 ④)。
// 为每个 itemConfigID 用 snowflake 生成一件独立实例(默认未鉴定);容量满 → ErrInventoryCapacityFull。
func (u *InventoryUsecase) GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) ([]data.ItemInstance, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if len(itemConfigIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "nothing to grant")
	}
	for _, id := range itemConfigIDs {
		if id == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
	}
	if u.sf == nil {
		return nil, errcode.New(errcode.ErrInvalidArg, "instance inventory not enabled (no id generator)")
	}
	// 一次 CAS 预留整段,替代循环调用 Generate len(itemConfigIDs) 次。
	// 产出严格递增且唯一,但不保证连续(跨秒有空洞);此处只按下标一一对应取用。
	instanceIDs := make([]uint64, len(itemConfigIDs))
	u.sf.GenerateInto(instanceIDs)
	detail := fmt.Sprintf("grant_inst n=%d", len(itemConfigIDs))
	insts, already, err := u.repo.GrantInstances(ctx, playerID, instanceIDs, itemConfigIDs, u.cfg.Capacity, idempotencyKey, detail)
	if err != nil {
		return nil, err
	}
	if already {
		plog.With(ctx).Infow("msg", "grant_instances_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "count", len(insts))
	}
	return insts, nil
}

// IdentifyItem 鉴定一件未鉴定装备实例:服务端权威 roll 随机属性(反作弊,不变量 §6)后落库。
// 幂等:已鉴定 → 回放已落定属性(不重复 roll)。无鉴定规则 → 只置 identified 无属性(不阻断)。
func (u *InventoryUsecase) IdentifyItem(ctx context.Context, playerID, instanceID uint64) (data.ItemInstance, error) {
	if playerID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	// 先锁读实例拿到 item_config_id 才能查规则 roll —— 但为避免两次读,直接把 roll 结果传给
	// 数据层,由数据层在 FOR UPDATE 里裁决"已鉴定则回放、否则落定"。roll 依赖 item_config_id,
	// 而此处尚未读到实例。折中:数据层先读实例;若已鉴定回放,否则回调 roll。这里改为两段式:
	// 1) 读实例(不锁)拿 config_id 预 roll;2) 数据层 FOR UPDATE 再判(已鉴定则忽略预 roll)。
	// 由于 roll 结果只在"未鉴定→落定"分支被采用,预读到 config 变更的概率为零(实例 config 不可变)。
	insts, lerr := u.repo.ListInstances(ctx, playerID)
	if lerr != nil {
		return data.ItemInstance{}, lerr
	}
	var configID uint32
	found := false
	for i := range insts {
		if insts[i].InstanceID == instanceID {
			configID = insts[i].ItemConfigID
			found = true
			break
		}
	}
	if !found {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found player=%d id=%d", playerID, instanceID)
	}
	attrs := u.rollIdentifyAttrs(configID)
	inst, already, err := u.repo.IdentifyInstance(ctx, playerID, instanceID, attrs)
	if err != nil {
		return data.ItemInstance{}, err
	}
	if already {
		plog.With(ctx).Infow("msg", "identify_item_idempotent_hit",
			"player_id", playerID, "instance_id", instanceID)
	}
	return inst, nil
}

// rollIdentifyAttrs 按配置从属性池不放回抽 AttrCount 条,每条在 [Min,Max] 均匀 roll 数值。
// 无规则 / 空池 → 返回 nil(鉴定只置 identified 无属性)。
func (u *InventoryUsecase) rollIdentifyAttrs(itemConfigID uint32) []data.ItemAttribute {
	rule := u.cfg.IdentifyRuleOf(itemConfigID)
	if rule == nil || len(rule.Pool) == 0 || rule.AttrCount <= 0 {
		return nil
	}
	// 洗牌 pool 下标(Fisher-Yates,用注入随机源),取前 min(AttrCount,len(pool)) 条。
	idx := make([]int, len(rule.Pool))
	for i := range idx {
		idx[i] = i
	}
	for i := len(idx) - 1; i > 0; i-- {
		j := int(u.randIntn(int64(i + 1)))
		idx[i], idx[j] = idx[j], idx[i]
	}
	pick := rule.AttrCount
	if pick > len(idx) {
		pick = len(idx)
	}
	out := make([]data.ItemAttribute, 0, pick)
	for k := 0; k < pick; k++ {
		p := rule.Pool[idx[k]]
		span := p.Max - p.Min + 1 // 闭区间
		value := p.Min
		if span > 0 {
			value = p.Min + u.randIntn(span)
		}
		out = append(out, data.ItemAttribute{AttrID: p.AttrID, Value: value})
	}
	return out
}

// MoveInstance 移动一件装备实例到新格子(纯大厅整理,不影响属性,W5 ④)。
func (u *InventoryUsecase) MoveInstance(ctx context.Context, playerID, instanceID uint64, toSlot int32) (data.ItemInstance, error) {
	if playerID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	if u.cfg.Capacity <= 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "instance inventory disabled")
	}
	return u.repo.MoveInstance(ctx, playerID, instanceID, toSlot, u.cfg.Capacity)
}

// DiscardInstance 丢弃一件装备实例(从背包永久删除,W5 ④)。幂等:已丢弃 → OK no-op。
func (u *InventoryUsecase) DiscardInstance(ctx context.Context, playerID, instanceID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	return u.repo.DiscardInstance(ctx, playerID, instanceID)
}

// GrantItems 幂等发放道具 + 货币(系统驱动,idempotency_key 防重复入账)。
func (u *InventoryUsecase) GrantItems(ctx context.Context, playerID uint64, items []data.ItemGrant, gold int64, idempotencyKey string) (int64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if len(items) == 0 && gold == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "nothing to grant")
	}
	if gold < 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "gold must not be negative")
	}
	for _, it := range items {
		if it.ItemConfigID == 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
		if it.Count <= 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive: item=%d", it.ItemConfigID)
		}
	}
	detail := fmt.Sprintf("grant items=%d gold=%d", len(items), gold)
	newGold, already, err := u.repo.GrantItems(ctx, playerID, items, gold, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "grant_items_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "gold", newGold)
	}
	return newGold, nil
}

// UseItem 大厅态使用消耗品(不可大厅使用 → ErrInventoryItemNotUsable;数量不足 → ErrInventoryInsufficient)。
func (u *InventoryUsecase) UseItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if itemConfigID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if count <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	rule := u.cfg.RuleOf(itemConfigID)
	if rule == nil || !rule.Usable {
		return 0, errcode.New(errcode.ErrInventoryItemNotUsable, "item not usable in lobby: %d", itemConfigID)
	}
	detail := fmt.Sprintf("use item=%d count=%d", itemConfigID, count)
	remaining, already, err := u.repo.UseItem(ctx, playerID, itemConfigID, count, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "use_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "item", itemConfigID, "remaining", remaining)
	}
	return remaining, nil
}

// SellItem 出售道具换金币(不可出售 → ErrInventoryNotSellable;数量不足 → ErrInventoryInsufficient)。
func (u *InventoryUsecase) SellItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, int64, error) {
	if playerID == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if itemConfigID == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if count <= 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	rule := u.cfg.RuleOf(itemConfigID)
	if rule == nil || !rule.Sellable {
		return 0, 0, errcode.New(errcode.ErrInventoryNotSellable, "item not sellable: %d", itemConfigID)
	}
	// 防御:可出售必须单价 > 0(启动时已校验,此处兜底防配置漂移/负价扣币)。
	if rule.SellUnitPrice <= 0 {
		return 0, 0, errcode.New(errcode.ErrInventoryNotSellable, "item not sellable (non-positive price): %d", itemConfigID)
	}
	// 防御:单价 * 数量 int64 溢出会变负数进而少扣/反加金币,溢出直接拒。
	gold, ok := safeMulInt64(rule.SellUnitPrice, count)
	if !ok || gold <= 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "sell amount overflow item=%d price=%d count=%d", itemConfigID, rule.SellUnitPrice, count)
	}
	detail := fmt.Sprintf("sell item=%d count=%d gold=%d", itemConfigID, count, gold)
	remaining, newGold, already, err := u.repo.SellItem(ctx, playerID, itemConfigID, count, gold, idempotencyKey, detail)
	if err != nil {
		return 0, 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "sell_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "item", itemConfigID, "remaining", remaining, "gold", newGold)
	}
	return remaining, newGold, nil
}

// SettleAuctionMatch 原子结算一笔拍卖成交(系统驱动,幂等键基于 match_id)。
//
// 在 inventory 一个本地事务里从双方 escrow 消费完成资产对转:
//   - 卖家:从卖单 escrow 消费 quantity 个 itemConfigID 交付买家,卖家加 quantity*unitPrice 金币;
//   - 买家:从买单 escrow 消费 quantity*unitPrice 金币付给卖家,买家加 quantity 个 itemConfigID。
//
// 资产已在 FreezeForOrder 冻结进 escrow,成交不会因余额不足失败。
// 幂等键 = "auction:settle:<match_id>",同一成交重复结算只生效一次(不变量 §9.2 / §9.7)。
func (u *InventoryUsecase) SettleAuctionMatch(ctx context.Context, matchID, sellerID, buyerID, sellOrderID, buyOrderID uint64, itemConfigID uint32, quantity, unitPrice int64) error {
	if matchID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if sellerID == 0 || buyerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "seller_id / buyer_id required")
	}
	if sellOrderID == 0 || buyOrderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "sell_order_id / buy_order_id required")
	}
	if sellerID == buyerID {
		// 自成交净额为零且会让同一玩家同 key 写两条流水冲突;视为非法,撮合侧应避免自撮合。
		return errcode.New(errcode.ErrInvalidArg, "seller and buyer must differ: %d", sellerID)
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if quantity <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "quantity must be positive")
	}
	if unitPrice <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "unit_price must be positive")
	}
	totalGold, ok := safeMulInt64(unitPrice, quantity)
	if !ok || totalGold <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "settle amount overflow match=%d price=%d qty=%d", matchID, unitPrice, quantity)
	}
	idempotencyKey := fmt.Sprintf("auction:settle:%d", matchID)
	detail := fmt.Sprintf("auction settle match=%d item=%d qty=%d gold=%d", matchID, itemConfigID, quantity, totalGold)
	already, err := u.repo.SettleAuctionMatch(ctx, matchID, sellerID, buyerID, sellOrderID, buyOrderID, itemConfigID, quantity, totalGold, idempotencyKey, detail)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_settle_idempotent_hit",
			"match_id", matchID, "seller_id", sellerID, "buyer_id", buyerID, "item", itemConfigID, "qty", quantity, "gold", totalGold)
	}
	// 分片:成交对转成功后观测本笔的跨分片落点(买卖双方背包跨 Cell → 跨分片对转,
	// 拆 Kafka 结算出箱幂等消费)。router 为 nil(单 Cell)→ 不打。
	u.logAuctionSettlementRouting(ctx, matchID, sellerID, buyerID)
	return nil
}

// SettlePlayerTrade 原子结算一笔玩家间点对点交易(系统驱动,幂等键基于 order_id)。
//
// 在 inventory 一个本地事务里从双方活跃余额扣转完成对转(无 escrow 预冻结):
//   - 卖家:交付 sellerItems 给买家,收 buyerItems + price 金币;
//   - 买家:交付 buyerItems + price 金币给卖家,收 sellerItems。
//
// 与拍卖不同,P2P 无预冻结,任一方资产不足 → ErrInventoryInsufficient,整笔回滚(成交失败)。
// 幂等键 = "trade:settle:<order_id>",同一订单重复结算只生效一次(不变量 §9.7)。
func (u *InventoryUsecase) SettlePlayerTrade(ctx context.Context, orderID, sellerID, buyerID uint64, sellerItems, buyerItems []data.ItemGrant, price int64) error {
	if orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "order_id required")
	}
	if sellerID == 0 || buyerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "seller_id / buyer_id required")
	}
	if sellerID == buyerID {
		// 自交易净额为零且会让同一玩家同 key 写两条流水冲突;视为非法。
		return errcode.New(errcode.ErrInvalidArg, "seller and buyer must differ: %d", sellerID)
	}
	if price < 0 {
		return errcode.New(errcode.ErrInvalidArg, "price must not be negative")
	}
	if len(sellerItems) == 0 && len(buyerItems) == 0 && price == 0 {
		return errcode.New(errcode.ErrInvalidArg, "nothing to settle")
	}
	validate := func(items []data.ItemGrant, side string) error {
		for _, it := range items {
			if it.ItemConfigID == 0 {
				return errcode.New(errcode.ErrInvalidArg, "%s item_config_id required", side)
			}
			if it.Count <= 0 {
				return errcode.New(errcode.ErrInvalidArg, "%s count must be positive: item=%d", side, it.ItemConfigID)
			}
		}
		return nil
	}
	if err := validate(sellerItems, "seller"); err != nil {
		return err
	}
	if err := validate(buyerItems, "buyer"); err != nil {
		return err
	}
	idempotencyKey := fmt.Sprintf("trade:settle:%d", orderID)
	detail := fmt.Sprintf("trade settle order=%d seller_items=%d buyer_items=%d price=%d",
		orderID, len(sellerItems), len(buyerItems), price)
	already, err := u.repo.SettlePlayerTrade(ctx, orderID, sellerID, buyerID, sellerItems, buyerItems, price, idempotencyKey, detail)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "trade_settle_idempotent_hit",
			"order_id", orderID, "seller_id", sellerID, "buyer_id", buyerID, "price", price)
	}
	return nil
}

// FreezeForOrder 拍卖挂单冻结资产(系统驱动,幂等键 = order_id)。
//
// SELL:冻结 quantity 个 itemConfigID(卖家下架道具);BUY:冻结 quantity*unitPrice 金币(买家锁价)。
// 资产移入 escrow,挂单期间不可被别处消耗;道具 / 金币不足 → ErrInventoryInsufficient。
func (u *InventoryUsecase) FreezeForOrder(ctx context.Context, playerID, orderID uint64, side EscrowSide, itemConfigID uint32, quantity, unitPrice int64) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if quantity <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "quantity must be positive")
	}
	if unitPrice <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "unit_price must be positive")
	}
	var (
		kind       data.EscrowKind
		frozenGold int64
	)
	switch side {
	case EscrowSideSell:
		kind = data.EscrowKindItem
	case EscrowSideBuy:
		kind = data.EscrowKindGold
		g, ok := safeMulInt64(unitPrice, quantity)
		if !ok || g <= 0 {
			return errcode.New(errcode.ErrInvalidArg, "freeze amount overflow order=%d price=%d qty=%d", orderID, unitPrice, quantity)
		}
		frozenGold = g
	default:
		return errcode.New(errcode.ErrInvalidArg, "unknown escrow side %d", side)
	}
	already, err := u.repo.FreezeForOrder(ctx, playerID, orderID, kind, itemConfigID, quantity, frozenGold)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_freeze_idempotent_hit",
			"player_id", playerID, "order_id", orderID, "side", side, "item", itemConfigID, "qty", quantity)
	}
	return nil
}

// EnsureAuctionEscrow 为旧版本已进入订单状态机、但可能没有成功冻结资产的订单补齐 escrow。
// 已有满足剩余量的 active escrow 只校验不重复扣；缺失时由 repo 在一个 MySQL 事务中补冻并建行。
func (u *InventoryUsecase) EnsureAuctionEscrow(
	ctx context.Context,
	playerID, orderID uint64,
	side EscrowSide,
	itemConfigID uint32,
	remainingQuantity, unitPrice uint64,
) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if remainingQuantity == 0 || unitPrice == 0 {
		return errcode.New(errcode.ErrInvalidArg, "remaining_quantity / unit_price must be positive")
	}
	const maxInt64AsUint64 = uint64(^uint64(0) >> 1)
	if remainingQuantity > maxInt64AsUint64 || unitPrice > maxInt64AsUint64 {
		return errcode.New(errcode.ErrInvalidArg,
			"ensure escrow amount exceeds int64 order=%d remaining=%d price=%d",
			orderID, remainingQuantity, unitPrice)
	}

	var kind data.EscrowKind
	switch side {
	case EscrowSideSell:
		kind = data.EscrowKindItem
	case EscrowSideBuy:
		kind = data.EscrowKindGold
		if _, ok := safeMulInt64(int64(unitPrice), int64(remainingQuantity)); !ok {
			return errcode.New(errcode.ErrInvalidArg,
				"ensure escrow amount overflow order=%d price=%d remaining=%d",
				orderID, unitPrice, remainingQuantity)
		}
	default:
		return errcode.New(errcode.ErrInvalidArg, "unknown escrow side %d", side)
	}

	already, err := u.repo.EnsureAuctionEscrow(ctx, playerID, orderID, kind, itemConfigID,
		int64(remainingQuantity), int64(unitPrice))
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_ensure_escrow_idempotent_hit",
			"player_id", playerID, "order_id", orderID, "side", side,
			"item", itemConfigID, "remaining", remainingQuantity)
	}
	return nil
}

// ReleaseEscrow 退还某挂单 escrow 残余资产到玩家活跃余额(撤单 / 过期 / 完全成交后,幂等键 = order_id)。
func (u *InventoryUsecase) ReleaseEscrow(ctx context.Context, playerID, orderID uint64) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	already, err := u.repo.ReleaseEscrow(ctx, playerID, orderID)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_release_noop", "player_id", playerID, "order_id", orderID)
	}
	return nil
}

// EscrowSide 是 biz 层冻结方向(对齐 proto EscrowSide:SELL=1 / BUY=2)。
type EscrowSide int32

const (
	// EscrowSideSell 卖单:冻道具。
	EscrowSideSell EscrowSide = 1
	// EscrowSideBuy 买单:冻金币。
	EscrowSideBuy EscrowSide = 2
)

// safeMulInt64 做溢出安全的 int64 乘法(a,b 均已保证为正)。溢出返回 (0, false)。
func safeMulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	c := a * b
	if c/b != a {
		return 0, false
	}
	return c, true
}

// MaxCheckItemsOwned 是一次 CheckItemsOwned 可查询的道具配置数上限(§9.18 读取侧上限)。
//
// 唯一调用方 player.SetEquipment 一次最多提交装备部位数量个配置(个位数),
// 取 64 留足余量;超限直接拒而不是静默截断——截断会把「未查」伪装成「未持有」,
// 让拥有权校验在超长请求下静默放行或静默误拒。
const MaxCheckItemsOwned = 64

// CheckItemsOwned 批量查询玩家持有情况,返回入参集合中**确实持有**的子集(去重,升序)。
//
// 「持有」= 可堆叠计数 > 0(player_items)或存在该配置的装备实例(player_item_instance),
// 两条路任一成立即算持有:装备走实例模型,消耗品走堆叠计数,调用方不必关心底层形态。
//
// 实现刻意复用既有 GetInventory / ListInstances 而不新写一条定向 SQL(§15.2 最少复杂度):
// 单玩家背包被容量与堆叠行数有界,而本接口只在大厅态改出战预设时调用,属低频路径。
func (u *InventoryUsecase) CheckItemsOwned(ctx context.Context, playerID uint64, itemConfigIDs []uint32) ([]uint32, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(itemConfigIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "item_config_ids required")
	}
	if len(itemConfigIDs) > MaxCheckItemsOwned {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"too many item_config_ids: %d > %d", len(itemConfigIDs), MaxCheckItemsOwned)
	}
	want := make(map[uint32]bool, len(itemConfigIDs))
	for _, id := range itemConfigIDs {
		if id == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
		want[id] = true
	}

	_, items, err := u.repo.GetInventory(ctx, playerID)
	if err != nil {
		return nil, err
	}
	owned := make(map[uint32]bool, len(want))
	for _, it := range items {
		if it.Count > 0 && want[it.ItemConfigID] {
			owned[it.ItemConfigID] = true
		}
	}

	// 未启用实例背包时不读 player_item_instance(既有库可能尚未迁移出该表,
	// 与 GetInventoryFull 同一条件),此时装备类道具只能靠堆叠计数判定。
	if u.cfg.Capacity > 0 {
		instances, ierr := u.repo.ListInstances(ctx, playerID)
		if ierr != nil {
			return nil, ierr
		}
		for _, inst := range instances {
			if want[inst.ItemConfigID] {
				owned[inst.ItemConfigID] = true
			}
		}
	}

	result := make([]uint32, 0, len(owned))
	for id := range owned {
		result = append(result, id)
	}
	// map 遍历顺序不稳定,定序返回,让调用方与用例拿到确定结果。
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}
