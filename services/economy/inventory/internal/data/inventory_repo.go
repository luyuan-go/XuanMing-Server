// Package data 是 inventory 服务的数据层(MySQL 货币 / 道具 / 幂等流水)。
//
// 库表(deploy/mysql-init/08-inventory-tables.sql,pandora_trade 库):
//
//	player_currency   玩家货币余额(PK player_id)
//	player_items      背包道具堆叠(uk player_id+item_config_id)
//	inventory_ledger  发放 / 使用 / 出售幂等流水(uk player_id+idempotency_key)
//
// 反作弊 / 一致性(不变量 §9.7):GrantItems / UseItem / SellItem 全部在一个事务里
// 先 INSERT inventory_ledger(命中 uk → 幂等已处理),再原子改 player_items / player_currency;
// 扣减用 SELECT ... FOR UPDATE 锁行 + 数量校验,避免并发超扣。
//
// player_items / player_currency 是结构化列(CLAUDE.md §5.9 不强制 proto 化),直接映射字段。
package data

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
)

// ItemStack 是背包里某配置道具的持有堆叠。
type ItemStack struct {
	ItemConfigID uint32
	Count        int64
}

// ItemGrant 是一次发放里对某配置道具增加的数量(Count>0)。
type ItemGrant struct {
	ItemConfigID uint32
	Count        int64
}

// EscrowKind 是拍卖挂单托管的资产类型(对齐 auction_escrow.kind)。
type EscrowKind int8

const (
	// EscrowKindItem 卖单冻结道具。
	EscrowKindItem EscrowKind = 1
	// EscrowKindGold 买单冻结金币。
	EscrowKindGold EscrowKind = 2
)

// escrow 行状态(对齐 auction_escrow.status)。
const (
	escrowStatusActive int8 = 1
	escrowStatusClosed int8 = 2
)

// InventoryRepo 是 inventory 数据层抽象。biz 只依赖此接口,不依赖 *sql.DB。
type InventoryRepo interface {
	// GetInventory 读玩家货币 + 道具堆叠(按 item_config_id 排序;未建档 → gold=0 空道具)。
	GetInventory(ctx context.Context, playerID uint64) (gold int64, items []ItemStack, err error)

	// GrantItems 幂等发放道具 + 货币(事务:INSERT ledger 命中 uk → 已处理读回 gold;
	// 否则 upsert player_items 累加、player_currency 累加 gold)。返回发放后 gold。
	GrantItems(ctx context.Context, playerID uint64, items []ItemGrant, gold int64, idempotencyKey, detail string) (newGold int64, already bool, err error)

	// UseItem 幂等扣减道具(事务:INSERT ledger;SELECT count FOR UPDATE 校验 >= n;扣减)。
	// 数量不足 → ErrInventoryInsufficient;道具不存在 → ErrInventoryItemNotFound。返回剩余数量。
	UseItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (remaining int64, already bool, err error)

	// ConsumeBattleItem 幂等扣减可信战斗事实对应的局内消耗；与大厅 use 分开流水。
	ConsumeBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (remaining int64, already bool, err error)

	// DiscardBattleItem 幂等扣减可信战斗丢弃事实；与客户端 discard 分开流水/权限。
	DiscardBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (remaining int64, already bool, err error)

	// DiscardItem 幂等丢弃可堆叠道具；与 UseItem 分开记 op/fingerprint，审计语义不混淆。
	DiscardItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (remaining int64, already bool, err error)

	// SellItem 幂等出售(事务:INSERT ledger;扣道具 + 加 gold)。返回剩余数量 + 出售后 gold。
	SellItem(ctx context.Context, playerID uint64, itemConfigID uint32, count, gold int64, idempotencyKey, detail string) (remaining, newGold int64, already bool, err error)

	// SettleAuctionMatch 原子结算一笔拍卖成交(一个本地事务内卖↔买双方资产对转):
	//   从卖单 escrow(sellOrderID)消费 quantity 个 itemConfigID 交付买家;
	//   从买单 escrow(buyOrderID)消费 totalGold 金币付给卖家;
	//   买家加 quantity 个道具、卖家加 totalGold 金币。
	// 因双方资产已在 FreezeForOrder 冻结进 escrow,成交不会因余额不足失败。
	// idempotencyKey(= 业务层基于 match_id 派生)在事务内给买卖双方各记一条流水,
	// 重复结算命中 uk → already=true(资产只转一次,不变量 §9.2 / §9.7)。
	SettleAuctionMatch(ctx context.Context, matchID, sellerID, buyerID, sellOrderID, buyOrderID uint64, itemConfigID uint32, quantity, totalGold int64, idempotencyKey, detail string) (already bool, err error)

	// SettlePlayerTrade 原子结算一笔玩家间点对点交易(一个本地事务内卖↔买双方资产对转):
	//   与拍卖不同,P2P 交易无 escrow 预冻,直接从双方活跃背包 / 余额扣转 ——
	//     卖家交付 sellerItems 给买家、收 buyerItems + price 金币;
	//     买家交付 buyerItems + price 金币给卖家、收 sellerItems。
	//   任一方道具 / 金币不足 → ErrInventoryInsufficient,整笔回滚。
	// 防死锁:对 player_items / player_currency 行锁全部按 player_id 升序、道具按 item_config_id 升序获取。
	// 幂等键(= 业务层基于 order_id 派生)在事务内给买卖双方各记一条流水,
	// 重复结算命中 uk → already=true(资产只转一次,不变量 §9.7)。
	SettlePlayerTrade(ctx context.Context, orderID, sellerID, buyerID uint64, sellerItems, buyerItems []ItemGrant, price int64, idempotencyKey, detail string) (already bool, err error)

	// FreezeForOrder 拍卖挂单冻结资产(一个本地事务内把活跃资产移入 escrow):
	//   EscrowKindItem:扣 quantity 个 itemConfigID,记 item escrow(frozenGold 忽略);
	//   EscrowKindGold:扣 frozenGold 金币,记 gold escrow(itemConfigID/quantity 仅记录道具上下文)。
	// 幂等键 = (playerID, orderID),重复冻结命中 uk → already=true(只冻一次)。
	// 道具 / 金币不足 → ErrInventoryInsufficient,整笔回滚(escrow 行一并回滚)。
	FreezeForOrder(ctx context.Context, playerID, orderID uint64, kind EscrowKind, itemConfigID uint32, quantity, frozenGold int64) (already bool, err error)

	// EnsureAuctionEscrow 为旧版本遗留的 OPEN/PARTIAL 订单补齐托管:
	//   - 已有 active escrow:锁行并严格核对 kind/item/status，且 item 余量 >= remainingQuantity
	//     或 gold 余量 >= remainingQuantity*unitPrice；满足即幂等成功，不再扣活跃资产；
	//   - escrow 不存在:在一个事务内从活跃资产扣除剩余量并创建 active escrow。
	// 唯一键冲突必须回滚后重新锁行校验，不能直接当幂等成功。closed/参数冲突返回
	// ErrInventoryIdempotencyConflict，托管或活跃资产不足返回 ErrInventoryInsufficient。
	EnsureAuctionEscrow(ctx context.Context, playerID, orderID uint64, kind EscrowKind, itemConfigID uint32, remainingQuantity, unitPrice int64) (already bool, err error)

	// ReleaseEscrow 退还某挂单 escrow 残余资产到玩家活跃余额并关闭托管(撤单 / 过期 / 完全成交后)。
	//   item escrow:退剩余 frozen_qty 道具;gold escrow:退剩余 frozen_gold 金币。
	// 幂等:escrow 不存在或已 closed → already=true no-op(只退一次)。
	ReleaseEscrow(ctx context.Context, playerID, orderID uint64) (already bool, err error)

	// ── 装备实例(W5 ④ 实例化背包)──

	// ListInstances 读玩家全部装备实例(按 instance_id 升序;未建档 → 空)。
	ListInstances(ctx context.Context, playerID uint64) ([]ItemInstance, error)

	// CheckInstancesOwned 精确返回 instance_id + item_config_id 都与玩家当前实例行一致的
	// 权威实例快照（按 instance_id 升序）。返回详情而非仅 ID，使 player.GetLoadout
	// 能把 identified/attributes 保真带到 DS，避免战斗链二次跨域查询。
	CheckInstancesOwned(ctx context.Context, playerID uint64, queries []InstanceOwnershipQuery) ([]ItemInstance, error)

	// GrantInstances 幂等发放装备实例(事务:INSERT ledger 命中 uk → 回放已发实例;
	// 否则锁玩家实例行校验 count+n<=capacity,给每件分配最低空闲格并 INSERT)。
	// instanceIDs 由 biz 用 snowflake 预生成(与 itemConfigIDs 等长一一对应)。
	// 格子已满 → ErrInventoryCapacityFull。返回本次(或回放)发放的实例。
	GrantInstances(ctx context.Context, playerID uint64, instanceIDs []uint64, itemConfigIDs []uint32, capacity int32, idempotencyKey, detail string) (instances []ItemInstance, already bool, err error)

	// IdentifyInstance 鉴定一件装备实例(事务:SELECT ... FOR UPDATE)。
	//   实例不存在 / 非本人 → ErrInventoryItemNotFound;
	//   已鉴定 → already=true,返回已落定属性(不用传入 attrs,幂等回放);
	//   未鉴定 → 落 identified=1 + attrs(biz 已 roll)。返回最终实例。
	IdentifyInstance(ctx context.Context, playerID, instanceID uint64, attrs []ItemAttribute) (inst ItemInstance, already bool, err error)

	// MoveInstance 移动实例到新格子(事务)。toSlot 须 [0,capacity);
	//   实例不存在 / 非本人 → ErrInventoryItemNotFound;目标格越界 / 被别的实例占用 → ErrInventorySlotOccupied;
	//   已在该格 → no-op。返回最终实例。
	MoveInstance(ctx context.Context, playerID, instanceID uint64, toSlot, capacity int32) (inst ItemInstance, err error)

	// DiscardInstance 丢弃实例(DELETE WHERE instance_id AND player_id)。
	// 幂等:不存在(已丢弃)→ OK no-op(auth 已保证只能删自己的)。
	DiscardInstance(ctx context.Context, playerID, instanceID uint64) error

	// SellInstance 原子出售唯一实例：锁实例、拒绝 bound、删除实例、金币入账与 ledger 同事务。
	SellInstance(ctx context.Context, playerID, instanceID uint64, itemConfigID uint32, gold int64, idempotencyKey, detail string) (newGold int64, already bool, err error)

	// ── 邮件 transfer 附件实例托管(2026-07-22,bag-domain.md §7.1;inventory_transfer.go)──

	// EscrowOutInstances 从源玩家同事务扣出实例并托管(bound 实例拒);幂等键 = escrowKey
	// (指纹含 to_player+ids)。返回托管快照(调用方装 TransferAttachment.item,原样不改)。
	EscrowOutInstances(ctx context.Context, sourcePlayerID, toPlayerID uint64, instanceIDs []uint64, escrowKey, detail string) ([]EscrowedInstance, bool, error)

	// ClaimTransferInstances 托管行原样搬进领取人实例表(同事务;只认托管行:缺行 / 收件人
	// 不符 / config 漂移 → ErrInventoryItemNotFound 整批拒;容量满 → ErrInventoryCapacityFull)。
	ClaimTransferInstances(ctx context.Context, toPlayerID uint64, items []TransferClaimItem, capacity int32, idempotencyKey, detail string) (already bool, err error)

	// ReleaseTransferEscrow 托管释放回各行 source 玩家(saga 补偿;行缺失 no-op 幂等,
	// 不设容量闸,slot NULL 入包)。返回实际释放行数。
	ReleaseTransferEscrow(ctx context.Context, instanceIDs []uint64) (released int, err error)

	// ConsumeTransferEscrow 消托管行不物化(bag phase 2 DS 领取链;资产已经 journal 入包)。
	// 存在的行必须 destined to 该玩家;行缺失 no-op 幂等。返回实际消费行数。
	ConsumeTransferEscrow(ctx context.Context, toPlayerID uint64, instanceIDs []uint64) (consumed int, err error)

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// SweepLedgerBefore 处理 created_at 超过保留期的幂等流水。
	//
	// **mode 默认 ModeReportOnly:只统计待清理量并 WARN 告警,一行都不删**(用户指令:
	// 不允许"因为数据大了"自动删数据);只有配置显式 retention_mode=delete 才真删。
	// 真删语义(仅供开启前评估):保留期必须远大于一切发放/使用/出售/结算的重试窗口
	// (分钟级),行删除后同 key 重放不再被 uk 拦截,靠"对应操作早已终态"保证不重复入账。
	SweepLedgerBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error)

	// SweepClosedEscrowBefore 处理已关闭(status=closed)且 updated_at 超保留期的托管行。
	// **mode 默认 ModeReportOnly(只报告不删)**;active 行无论如何都不在处理范围
	// (EnsureAuctionEscrow 依赖其存在性核对遗留订单)。
	// 真删语义:删后迟到 ReleaseEscrow 命中 ErrNoRows → already no-op,fail-safe。
	SweepClosedEscrowBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error)
}

// MySQLInventoryRepo 是基于 database/sql 的 InventoryRepo 实现。
type MySQLInventoryRepo struct {
	db *sql.DB
}

// NewMySQLInventoryRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_trade 库)。
func NewMySQLInventoryRepo(db *sql.DB) *MySQLInventoryRepo {
	return &MySQLInventoryRepo{db: db}
}

func (r *MySQLInventoryRepo) GetInventory(ctx context.Context, playerID uint64) (int64, []ItemStack, error) {
	var gold int64
	gerr := r.db.QueryRowContext(ctx, `SELECT gold FROM player_currency WHERE player_id = ? LIMIT 1`, playerID).Scan(&gold)
	if gerr != nil && !errors.Is(gerr, sql.ErrNoRows) {
		return 0, nil, errcode.New(errcode.ErrInternal, "read gold player=%d: %v", playerID, gerr)
	}

	const q = `SELECT item_config_id, count FROM player_items WHERE player_id = ? AND count > 0 ORDER BY item_config_id`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return 0, nil, errcode.New(errcode.ErrInternal, "query items player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var items []ItemStack
	for rows.Next() {
		var it ItemStack
		if serr := rows.Scan(&it.ItemConfigID, &it.Count); serr != nil {
			return 0, nil, errcode.New(errcode.ErrInternal, "scan item player=%d: %v", playerID, serr)
		}
		items = append(items, it)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, nil, errcode.New(errcode.ErrInternal, "iterate items player=%d: %v", playerID, rerr)
	}
	return gold, items, nil
}

// ── 幂等指纹 ────────────────────────────────────────────────────────────────
//
// 同一 idempotency_key 复用到**不同客户端意图**(op/item/count/instance 等不同)会被静默当 no-op
// 是反作弊隐患;指纹把 key 绑定到请求内容:首次执行记录指纹 + 结果快照,
// 重复请求指纹不一致 → ErrInventoryIdempotencyConflict;一致 → 回放首次结果快照。

// GrantFingerprint 计算发放请求指纹(items 按 item_config_id 排序后规范化 + gold)。
func GrantFingerprint(items []ItemGrant, gold int64) string {
	sorted := append([]ItemGrant(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ItemConfigID < sorted[j].ItemConfigID })
	var b strings.Builder
	b.WriteString("grant")
	for _, it := range sorted {
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(uint64(it.ItemConfigID), 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(it.Count, 10))
	}
	b.WriteString("|gold=")
	b.WriteString(strconv.FormatInt(gold, 10))
	return hashHex(b.String())
}

// UseFingerprint 计算使用请求指纹。
func UseFingerprint(itemConfigID uint32, count int64) string {
	return hashHex(fmt.Sprintf("use|%d:%d", itemConfigID, count))
}

// DiscardFingerprint 计算堆叠道具丢弃请求指纹。
func DiscardFingerprint(itemConfigID uint32, count int64) string {
	return hashHex(fmt.Sprintf("discard|%d:%d", itemConfigID, count))
}

// BattleConsumeFingerprint 计算局内消费事实请求指纹。
func BattleConsumeFingerprint(itemConfigID uint32, count int64) string {
	return hashHex(fmt.Sprintf("battle_consume|%d:%d", itemConfigID, count))
}

// BattleDiscardFingerprint 计算局内丢弃事实请求指纹。
func BattleDiscardFingerprint(itemConfigID uint32, count int64) string {
	return hashHex(fmt.Sprintf("battle_discard|%d:%d", itemConfigID, count))
}

// SellFingerprint 只绑定客户端出售意图。售价是服务端热配置，不得参与新指纹；否则
// 首次响应丢失后热更价格会把同 key 重试误判为冲突，甚至诱发二次出售。
func SellFingerprint(itemConfigID uint32, count int64) string {
	return hashHex(fmt.Sprintf("sell|%d:%d", itemConfigID, count))
}

// SellInstanceFingerprint 只绑定客户端选中的唯一实例和配置一致性字段。
func SellInstanceFingerprint(instanceID uint64, itemConfigID uint32) string {
	return hashHex(fmt.Sprintf("sell_inst|%d|item=%d", instanceID, itemConfigID))
}

// legacy*Fingerprint 仅用于安全识别升级前已提交的 ledger。不能拿当前热更价格计算：
// 必须从旧行 detail 恢复首次价格并严格验证完整意图后再接受。
func legacySellFingerprint(itemConfigID uint32, count, gold int64) string {
	return hashHex(fmt.Sprintf("sell|%d:%d|gold=%d", itemConfigID, count, gold))
}

func legacySellInstanceFingerprint(instanceID uint64, itemConfigID uint32, gold int64) string {
	return hashHex(fmt.Sprintf("sell_inst|%d|item=%d|gold=%d", instanceID, itemConfigID, gold))
}

// AuctionSettleFingerprint 计算拍卖结算请求指纹(双方 + 道具 + 数量 + 总价)。
// 同一 idempotency_key 复用到不同成交内容 → 指纹不一致判冲突,防 key 复用串改账。
func AuctionSettleFingerprint(sellerID, buyerID uint64, itemConfigID uint32, quantity, totalGold int64) string {
	return hashHex(fmt.Sprintf("auction_settle|seller=%d|buyer=%d|item=%d|qty=%d|gold=%d",
		sellerID, buyerID, itemConfigID, quantity, totalGold))
}

// PlayerTradeSettleFingerprint 计算玩家间交易结算请求指纹(双方 + 双向道具 + 金币)。
// 同一 idempotency_key 复用到不同交易内容 → 指纹不一致判冲突,防 key 复用串改账。
func PlayerTradeSettleFingerprint(sellerID, buyerID uint64, sellerItems, buyerItems []ItemGrant, price int64) string {
	write := func(b *strings.Builder, tag string, items []ItemGrant) {
		sorted := append([]ItemGrant(nil), items...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].ItemConfigID < sorted[j].ItemConfigID })
		b.WriteString(tag)
		for _, it := range sorted {
			b.WriteByte('|')
			b.WriteString(strconv.FormatUint(uint64(it.ItemConfigID), 10))
			b.WriteByte(':')
			b.WriteString(strconv.FormatInt(it.Count, 10))
		}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("trade_settle|seller=%d|buyer=%d|", sellerID, buyerID))
	write(&b, "sell", sellerItems)
	write(&b, "|buy", buyerItems)
	b.WriteString("|price=")
	b.WriteString(strconv.FormatInt(price, 10))
	return hashHex(b.String())
}

func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// claimLedger 在事务里声明幂等键 + 记录请求指纹。
//   - 首次:插入成功 → already=false
//   - 重复(uk 1062):读回已存指纹 + 结果快照;
//     指纹不一致 → ErrInventoryIdempotencyConflict;一致 → already=true + 首次结果快照(回放)
func claimLedger(ctx context.Context, tx *sql.Tx, playerID uint64, idempotencyKey, op, fingerprint, detail string) (already bool, snapRemaining, snapGold int64, err error) {
	const ins = `INSERT INTO inventory_ledger (player_id, idempotency_key, op, request_fingerprint, detail) VALUES (?, ?, ?, ?, ?)`
	if _, lerr := tx.ExecContext(ctx, ins, playerID, idempotencyKey, op, fingerprint, detail); lerr != nil {
		if !isDupErr(lerr) {
			return false, 0, 0, errcode.New(errcode.ErrInternal, "insert ledger player=%d key=%s: %v", playerID, idempotencyKey, lerr)
		}
		// 幂等命中:读回首次请求指纹 + 结果快照比对。
		var storedFP string
		qerr := tx.QueryRowContext(ctx,
			`SELECT request_fingerprint, result_remaining, result_gold FROM inventory_ledger WHERE player_id = ? AND idempotency_key = ? LIMIT 1`,
			playerID, idempotencyKey).Scan(&storedFP, &snapRemaining, &snapGold)
		if qerr != nil {
			return false, 0, 0, errcode.New(errcode.ErrInternal, "read ledger player=%d key=%s: %v", playerID, idempotencyKey, qerr)
		}
		if storedFP != fingerprint {
			// 同键不同请求内容:发放/扣减/结算的完整性冲突(防 key 复用串改账),fail-closed 留证。
			plog.With(ctx).Warnw("msg", "inventory_idempotency_conflict",
				"player_id", playerID, "idempotency_key", idempotencyKey, "op", "ledger")
			return false, 0, 0, errcode.New(errcode.ErrInventoryIdempotencyConflict,
				"idempotency_key reused for different request player=%d key=%s", playerID, idempotencyKey)
		}
		return true, snapRemaining, snapGold, nil
	}
	return false, 0, 0, nil
}

type saleLedgerIntent struct {
	op           string
	itemConfigID uint32
	count        int64
	instanceID   uint64
}

// claimSaleLedger 是售价热更安全的幂等声明：新行只存客户端意图指纹；旧行只有在
// op、detail 中的完整首次意图和由首次 gold 重算的旧指纹全部一致时才允许回放。
func claimSaleLedger(ctx context.Context, tx *sql.Tx, playerID uint64, idempotencyKey, fingerprint, detail string, intent saleLedgerIntent) (already bool, snapRemaining, snapGold int64, err error) {
	const ins = `INSERT INTO inventory_ledger (player_id, idempotency_key, op, request_fingerprint, detail) VALUES (?, ?, ?, ?, ?)`
	if _, lerr := tx.ExecContext(ctx, ins, playerID, idempotencyKey, intent.op, fingerprint, detail); lerr == nil {
		return false, 0, 0, nil
	} else if !isDupErr(lerr) {
		return false, 0, 0, errcode.New(errcode.ErrInternal,
			"insert sale ledger player=%d key=%s: %v", playerID, idempotencyKey, lerr)
	}

	var storedOp, storedFP, storedDetail string
	if qerr := tx.QueryRowContext(ctx, `SELECT op,request_fingerprint,detail,result_remaining,result_gold
FROM inventory_ledger WHERE player_id=? AND idempotency_key=? LIMIT 1`, playerID, idempotencyKey).
		Scan(&storedOp, &storedFP, &storedDetail, &snapRemaining, &snapGold); qerr != nil {
		return false, 0, 0, errcode.New(errcode.ErrInternal,
			"read sale ledger player=%d key=%s: %v", playerID, idempotencyKey, qerr)
	}
	matched := storedOp == intent.op && storedFP == fingerprint
	if !matched && storedOp == intent.op {
		matched = legacySaleLedgerMatches(storedFP, storedDetail, intent)
	}
	if !matched {
		plog.With(ctx).Warnw("msg", "inventory_idempotency_conflict",
			"player_id", playerID, "idempotency_key", idempotencyKey, "op", intent.op)
		return false, 0, 0, errcode.New(errcode.ErrInventoryIdempotencyConflict,
			"idempotency_key reused for different sale request player=%d key=%s", playerID, idempotencyKey)
	}
	return true, snapRemaining, snapGold, nil
}

func legacySaleLedgerMatches(storedFP, detail string, intent saleLedgerIntent) bool {
	switch intent.op {
	case "sell":
		var itemID uint32
		var count, gold int64
		n, err := fmt.Sscanf(detail, "sell item=%d count=%d gold=%d", &itemID, &count, &gold)
		if err != nil || n != 3 || detail != fmt.Sprintf("sell item=%d count=%d gold=%d", itemID, count, gold) ||
			itemID != intent.itemConfigID || count != intent.count || gold <= 0 {
			return false
		}
		return storedFP == legacySellFingerprint(itemID, count, gold)
	case "sell_inst":
		var instanceID uint64
		var itemID uint32
		var gold int64
		n, err := fmt.Sscanf(detail, "sell instance=%d item=%d gold=%d", &instanceID, &itemID, &gold)
		if err != nil || n != 3 || detail != fmt.Sprintf("sell instance=%d item=%d gold=%d", instanceID, itemID, gold) ||
			instanceID != intent.instanceID || itemID != intent.itemConfigID || gold <= 0 {
			return false
		}
		return storedFP == legacySellInstanceFingerprint(instanceID, itemID, gold)
	default:
		return false
	}
}

// updateLedgerResult 在事务里把首次执行的结果快照写回流水(供后续幂等回放返回稳定值)。
func updateLedgerResult(ctx context.Context, tx *sql.Tx, playerID uint64, idempotencyKey string, remaining, gold int64) error {
	const upd = `UPDATE inventory_ledger SET result_remaining = ?, result_gold = ? WHERE player_id = ? AND idempotency_key = ?`
	if _, uerr := tx.ExecContext(ctx, upd, remaining, gold, playerID, idempotencyKey); uerr != nil {
		return errcode.New(errcode.ErrInternal, "update ledger result player=%d key=%s: %v", playerID, idempotencyKey, uerr)
	}
	return nil
}

// readGoldTx 在事务里读 gold(无行 → 0)。
func readGoldTx(ctx context.Context, tx *sql.Tx, playerID uint64) (int64, error) {
	var gold int64
	err := tx.QueryRowContext(ctx, `SELECT gold FROM player_currency WHERE player_id = ? LIMIT 1`, playerID).Scan(&gold)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "read gold player=%d: %v", playerID, err)
	}
	return gold, nil
}

func (r *MySQLInventoryRepo) GrantItems(ctx context.Context, playerID uint64, items []ItemGrant, gold int64, idempotencyKey, detail string) (int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	already, _, snapGold, lerr := claimLedger(ctx, tx, playerID, idempotencyKey, "grant", GrantFingerprint(items, gold), detail)
	if lerr != nil {
		return 0, false, lerr
	}
	if already {
		return snapGold, true, nil
	}

	const upItem = `INSERT INTO player_items (player_id, item_config_id, count) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE count = count + VALUES(count)`
	for _, it := range items {
		if _, ierr := tx.ExecContext(ctx, upItem, playerID, it.ItemConfigID, it.Count); ierr != nil {
			return 0, false, errcode.New(errcode.ErrInternal, "grant item player=%d item=%d: %v", playerID, it.ItemConfigID, ierr)
		}
	}

	const upGold = `INSERT INTO player_currency (player_id, gold) VALUES (?, ?)
ON DUPLICATE KEY UPDATE gold = gold + VALUES(gold)`
	if _, gerr := tx.ExecContext(ctx, upGold, playerID, gold); gerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "grant gold player=%d: %v", playerID, gerr)
	}

	newGold, rerr := readGoldTx(ctx, tx, playerID)
	if rerr != nil {
		return 0, false, rerr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, 0, newGold); uerr != nil {
		return 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "commit grant player=%d: %v", playerID, cerr)
	}
	return newGold, false, nil
}

// deductItemTx 在事务里锁道具行并扣减 count。
//   - 行不存在 → ErrInventoryItemNotFound
//   - count < n → ErrInventoryInsufficient
//   - 成功 → 返回扣减后剩余数量
func deductItemTx(ctx context.Context, tx *sql.Tx, playerID uint64, itemConfigID uint32, n int64) (int64, error) {
	var have int64
	err := tx.QueryRowContext(ctx,
		`SELECT count FROM player_items WHERE player_id = ? AND item_config_id = ? FOR UPDATE`,
		playerID, itemConfigID).Scan(&have)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errcode.New(errcode.ErrInventoryItemNotFound, "item not found player=%d item=%d", playerID, itemConfigID)
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "lock item player=%d item=%d: %v", playerID, itemConfigID, err)
	}
	if have < n {
		return 0, errcode.New(errcode.ErrInventoryInsufficient, "insufficient item player=%d item=%d need=%d have=%d", playerID, itemConfigID, n, have)
	}
	remaining := have - n
	if remaining == 0 {
		// 堆叠扣空即删行(2026-07-22 用户要求):不留 count=0 死行(读侧本就过滤 count>0,
		// 留行只会让 player_items 无界堆积)。后续再发放同 config 走 GrantItems 的
		// upsert(INSERT ... ON DUPLICATE)重建行,行为不变。
		if _, derr := tx.ExecContext(ctx,
			`DELETE FROM player_items WHERE player_id = ? AND item_config_id = ?`,
			playerID, itemConfigID); derr != nil {
			return 0, errcode.New(errcode.ErrInternal, "delete emptied item player=%d item=%d: %v", playerID, itemConfigID, derr)
		}
		return 0, nil
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE player_items SET count = ? WHERE player_id = ? AND item_config_id = ?`,
		remaining, playerID, itemConfigID); uerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "deduct item player=%d item=%d: %v", playerID, itemConfigID, uerr)
	}
	return remaining, nil
}

func (r *MySQLInventoryRepo) UseItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	already, snapRemaining, _, lerr := claimLedger(ctx, tx, playerID, idempotencyKey, "use", UseFingerprint(itemConfigID, count), detail)
	if lerr != nil {
		return 0, false, lerr
	}
	if already {
		// 幂等命中:回放首次执行的剩余数量快照(不重新读当前状态,避免随后续操作漂移)。
		return snapRemaining, true, nil
	}

	remaining, derr := deductItemTx(ctx, tx, playerID, itemConfigID, count)
	if derr != nil {
		return 0, false, derr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, remaining, 0); uerr != nil {
		return 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "commit use player=%d: %v", playerID, cerr)
	}
	return remaining, false, nil
}

func (r *MySQLInventoryRepo) DiscardItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	already, snapRemaining, _, lerr := claimLedger(ctx, tx, playerID, idempotencyKey,
		"discard", DiscardFingerprint(itemConfigID, count), detail)
	if lerr != nil {
		return 0, false, lerr
	}
	if already {
		return snapRemaining, true, nil
	}
	remaining, derr := deductItemTx(ctx, tx, playerID, itemConfigID, count)
	if derr != nil {
		return 0, false, derr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, remaining, 0); uerr != nil {
		return 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "commit discard player=%d: %v", playerID, cerr)
	}
	return remaining, false, nil
}

func (r *MySQLInventoryRepo) ConsumeBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin battle consume tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	already, snapRemaining, _, lerr := claimLedger(ctx, tx, playerID, idempotencyKey,
		"battle_consume", BattleConsumeFingerprint(itemConfigID, count), detail)
	if lerr != nil {
		return 0, false, lerr
	}
	if already {
		return snapRemaining, true, nil
	}
	remaining, derr := deductItemTx(ctx, tx, playerID, itemConfigID, count)
	if derr != nil {
		return 0, false, derr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, remaining, 0); uerr != nil {
		return 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal,
			"commit battle consume player=%d: %v", playerID, cerr)
	}
	return remaining, false, nil
}

func (r *MySQLInventoryRepo) DiscardBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, detail string) (int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin battle discard tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	already, snapRemaining, _, lerr := claimLedger(ctx, tx, playerID, idempotencyKey,
		"battle_discard", BattleDiscardFingerprint(itemConfigID, count), detail)
	if lerr != nil {
		return 0, false, lerr
	}
	if already {
		return snapRemaining, true, nil
	}
	remaining, derr := deductItemTx(ctx, tx, playerID, itemConfigID, count)
	if derr != nil {
		return 0, false, derr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, remaining, 0); uerr != nil {
		return 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal,
			"commit battle discard player=%d: %v", playerID, cerr)
	}
	return remaining, false, nil
}

func (r *MySQLInventoryRepo) SellItem(ctx context.Context, playerID uint64, itemConfigID uint32, count, gold int64, idempotencyKey, detail string) (int64, int64, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	already, snapRemaining, snapGold, lerr := claimSaleLedger(ctx, tx, playerID, idempotencyKey,
		SellFingerprint(itemConfigID, count), detail, saleLedgerIntent{
			op: "sell", itemConfigID: itemConfigID, count: count,
		})
	if lerr != nil {
		return 0, 0, false, lerr
	}
	if already {
		// 幂等命中:回放首次执行的剩余数量 + 金币快照。
		return snapRemaining, snapGold, true, nil
	}
	if gold <= 0 {
		return 0, 0, false, errcode.New(errcode.ErrInventoryNotSellable,
			"item not sellable player=%d item=%d", playerID, itemConfigID)
	}

	remaining, derr := deductItemTx(ctx, tx, playerID, itemConfigID, count)
	if derr != nil {
		return 0, 0, false, derr
	}

	const upGold = `INSERT INTO player_currency (player_id, gold) VALUES (?, ?)
ON DUPLICATE KEY UPDATE gold = gold + VALUES(gold)`
	if _, gerr := tx.ExecContext(ctx, upGold, playerID, gold); gerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "add gold player=%d: %v", playerID, gerr)
	}
	newGold, rerr := readGoldTx(ctx, tx, playerID)
	if rerr != nil {
		return 0, 0, false, rerr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, idempotencyKey, remaining, newGold); uerr != nil {
		return 0, 0, false, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "commit sell player=%d: %v", playerID, cerr)
	}
	return remaining, newGold, false, nil
}

// deductGoldTx 在事务里锁货币行并扣减 n。
//   - 行不存在(无货币记录,余额 0)或余额 < n → ErrInventoryInsufficient
//   - 成功 → 返回扣减后余额
func deductGoldTx(ctx context.Context, tx *sql.Tx, playerID uint64, n int64) (int64, error) {
	var have int64
	err := tx.QueryRowContext(ctx,
		`SELECT gold FROM player_currency WHERE player_id = ? FOR UPDATE`, playerID).Scan(&have)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errcode.New(errcode.ErrInventoryInsufficient, "insufficient gold player=%d need=%d have=0", playerID, n)
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "lock gold player=%d: %v", playerID, err)
	}
	if have < n {
		return 0, errcode.New(errcode.ErrInventoryInsufficient, "insufficient gold player=%d need=%d have=%d", playerID, n, have)
	}
	remaining := have - n
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE player_currency SET gold = ? WHERE player_id = ?`, remaining, playerID); uerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "deduct gold player=%d: %v", playerID, uerr)
	}
	return remaining, nil
}

// addGoldTx 在事务里给玩家加金币(upsert,无行则建)。
func addGoldTx(ctx context.Context, tx *sql.Tx, playerID uint64, n int64) error {
	const upGold = `INSERT INTO player_currency (player_id, gold) VALUES (?, ?)
ON DUPLICATE KEY UPDATE gold = gold + VALUES(gold)`
	if _, err := tx.ExecContext(ctx, upGold, playerID, n); err != nil {
		return errcode.New(errcode.ErrInternal, "add gold player=%d: %v", playerID, err)
	}
	return nil
}

// addItemTx 在事务里给玩家加道具(upsert 堆叠,无行则建)。
func addItemTx(ctx context.Context, tx *sql.Tx, playerID uint64, itemConfigID uint32, n int64) error {
	const upItem = `INSERT INTO player_items (player_id, item_config_id, count) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE count = count + VALUES(count)`
	if _, err := tx.ExecContext(ctx, upItem, playerID, itemConfigID, n); err != nil {
		return errcode.New(errcode.ErrInternal, "add item player=%d item=%d: %v", playerID, itemConfigID, err)
	}
	return nil
}

// SettleAuctionMatch 在一个本地事务里从双方 escrow 消费完成拍卖成交的卖↔买资产对转。
//
// 因卖家道具与买家金币已在 FreezeForOrder 冻结进 escrow,本步只做「消费 escrow + 入账对手」,
// 不再触活跃余额扣减,故成交不会因余额不足失败(escrow 充足由冻结阶段保证)。
//
// 防死锁:对 escrow / player_items / player_currency 的行锁全部按 player_id 升序、
// 同一玩家内「先 escrow 后入账」的总顺序获取,杜绝并发结算(尤其角色对调的两笔)成环。
// 幂等:买卖双方各记一条同 idempotency_key 的流水,任一命中 uk → already=true 回放(不重复转)。
func (r *MySQLInventoryRepo) SettleAuctionMatch(ctx context.Context, matchID, sellerID, buyerID, sellOrderID, buyOrderID uint64, itemConfigID uint32, quantity, totalGold int64, idempotencyKey, detail string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fp := AuctionSettleFingerprint(sellerID, buyerID, itemConfigID, quantity, totalGold)

	// 1) 幂等流水:按 player_id 升序声明两条(同 key),避免并发交叉插入死锁。
	loID, hiID := sellerID, buyerID
	loOp, hiOp := "auction_sell", "auction_buy"
	if buyerID < sellerID {
		loID, hiID = buyerID, sellerID
		loOp, hiOp = "auction_buy", "auction_sell"
	}
	loAlready, _, _, lerr := claimLedger(ctx, tx, loID, idempotencyKey, loOp, fp, detail)
	if lerr != nil {
		return false, lerr
	}
	hiAlready, _, _, herr := claimLedger(ctx, tx, hiID, idempotencyKey, hiOp, fp, detail)
	if herr != nil {
		return false, herr
	}
	if loAlready || hiAlready {
		// 已结算过(双方流水原子写入,正常下同真同假;异常单边脏数据也按已处理回滚防双扣)。
		return true, nil
	}

	// 2) 资产对转。卖家腿:消费卖单道具 escrow + 加金币;买家腿:消费买单金币 escrow + 加道具。
	//    两条腿都「先 escrow 后入账」,配合 player 升序保证全局锁序一致,防死锁。
	sellerLeg := func() error {
		if cerr := consumeItemEscrowTx(ctx, tx, sellerID, sellOrderID, itemConfigID, quantity); cerr != nil {
			return cerr
		}
		return addGoldTx(ctx, tx, sellerID, totalGold)
	}
	buyerLeg := func() error {
		if cerr := consumeGoldEscrowTx(ctx, tx, buyerID, buyOrderID, totalGold); cerr != nil {
			return cerr
		}
		return addItemTx(ctx, tx, buyerID, itemConfigID, quantity)
	}
	first, second := sellerLeg, buyerLeg
	if buyerID < sellerID {
		first, second = buyerLeg, sellerLeg
	}
	if ferr := first(); ferr != nil {
		return false, ferr
	}
	if serr := second(); serr != nil {
		return false, serr
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit auction settle match=%d: %v", matchID, cerr)
	}
	return false, nil
}

// SettlePlayerTrade 原子结算一笔玩家间点对点交易(一个本地事务内卖↔买双方资产对转)。
//
// 与 SettleAuctionMatch 的差异:P2P 交易无 escrow 预冻结,直接从双方活跃余额扣转,
// 故任一方道具 / 金币不足都会让整笔事务回滚并返回 ErrInventoryInsufficient(成交可能失败)。
//
// 防死锁:对 player_items / player_currency 的行锁按 player_id 升序、道具按 item_config_id
// 升序获取(指纹/腿内均先排序),杜绝并发结算(尤其买卖角色对调的两笔)成环。
// 幂等:买卖双方各记一条同 idempotency_key 的流水,任一命中 uk → already=true 回放(不重复转)。
func (r *MySQLInventoryRepo) SettlePlayerTrade(ctx context.Context, orderID, sellerID, buyerID uint64, sellerItems, buyerItems []ItemGrant, price int64, idempotencyKey, detail string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	fp := PlayerTradeSettleFingerprint(sellerID, buyerID, sellerItems, buyerItems, price)

	// 1) 幂等流水:按 player_id 升序声明两条(同 key),避免并发交叉插入死锁。
	loID, hiID := sellerID, buyerID
	loOp, hiOp := "trade_sell", "trade_buy"
	if buyerID < sellerID {
		loID, hiID = buyerID, sellerID
		loOp, hiOp = "trade_buy", "trade_sell"
	}
	loAlready, _, _, lerr := claimLedger(ctx, tx, loID, idempotencyKey, loOp, fp, detail)
	if lerr != nil {
		return false, lerr
	}
	hiAlready, _, _, herr := claimLedger(ctx, tx, hiID, idempotencyKey, hiOp, fp, detail)
	if herr != nil {
		return false, herr
	}
	if loAlready || hiAlready {
		return true, nil
	}

	// 2) 资产对转。腿内锁序必须全局一致:同一玩家的道具行按 item_config_id 升序
	//    「扣/加合并成一趟」处理(同 ID 先扣后加),金币行统一放腿尾。
	//    若像旧实现那样「先扣完再加」,两笔买卖方向对调的并发交易
	//    (A 卖 item1 换 item2 vs A 卖 item2 换 item1)会在同一玩家的行上
	//    以相反顺序加锁 → InnoDB 死锁(1213)。
	sortedSeller := append([]ItemGrant(nil), sellerItems...)
	sort.Slice(sortedSeller, func(i, j int) bool { return sortedSeller[i].ItemConfigID < sortedSeller[j].ItemConfigID })
	sortedBuyer := append([]ItemGrant(nil), buyerItems...)
	sort.Slice(sortedBuyer, func(i, j int) bool { return sortedBuyer[i].ItemConfigID < sortedBuyer[j].ItemConfigID })

	// itemOps 对同一玩家把「扣 deducts + 加 adds」按 item_config_id 升序归并成单趟;
	// 同一 ID 同时出现在两边时先扣后加(保守:先过余额校验)。
	itemOps := func(playerID uint64, deducts, adds []ItemGrant) error {
		di, ai := 0, 0
		for di < len(deducts) || ai < len(adds) {
			if ai >= len(adds) || (di < len(deducts) && deducts[di].ItemConfigID <= adds[ai].ItemConfigID) {
				if _, derr := deductItemTx(ctx, tx, playerID, deducts[di].ItemConfigID, deducts[di].Count); derr != nil {
					return derr
				}
				di++
				continue
			}
			if aerr := addItemTx(ctx, tx, playerID, adds[ai].ItemConfigID, adds[ai].Count); aerr != nil {
				return aerr
			}
			ai++
		}
		return nil
	}

	// 卖家腿:交付 sellerItems(扣) + 收 buyerItems(加),金币(加)收尾。
	sellerLeg := func() error {
		if oerr := itemOps(sellerID, sortedSeller, sortedBuyer); oerr != nil {
			return oerr
		}
		if price > 0 {
			return addGoldTx(ctx, tx, sellerID, price)
		}
		return nil
	}
	// 买家腿:交付 buyerItems(扣) + 收 sellerItems(加),金币(扣)收尾(与卖家腿同序防死锁)。
	buyerLeg := func() error {
		if oerr := itemOps(buyerID, sortedBuyer, sortedSeller); oerr != nil {
			return oerr
		}
		if price > 0 {
			if _, derr := deductGoldTx(ctx, tx, buyerID, price); derr != nil {
				return derr
			}
		}
		return nil
	}
	first, second := sellerLeg, buyerLeg
	if buyerID < sellerID {
		first, second = buyerLeg, sellerLeg
	}
	if ferr := first(); ferr != nil {
		return false, ferr
	}
	if serr := second(); serr != nil {
		return false, serr
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit player trade settle order=%d: %v", orderID, cerr)
	}
	return false, nil
}

// FreezeForOrder 把挂单资产从活跃余额移入 escrow(一个本地事务)。幂等键 = (playerID, orderID)。
func (r *MySQLInventoryRepo) FreezeForOrder(ctx context.Context, playerID, orderID uint64, kind EscrowKind, itemConfigID uint32, quantity, frozenGold int64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1) 幂等:插入 escrow 行(uk player+order)。命中 → 已冻结,直接 already(资产已扣,不重复扣)。
	const ins = `INSERT INTO auction_escrow (player_id, order_id, kind, item_config_id, frozen_qty, frozen_gold, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	var frozenQty int64
	if kind == EscrowKindItem {
		frozenQty = quantity
	}
	if _, ierr := tx.ExecContext(ctx, ins, playerID, orderID, int8(kind), itemConfigID, frozenQty, frozenGold, escrowStatusActive); ierr != nil {
		if isDupErr(ierr) {
			return true, nil
		}
		return false, errcode.New(errcode.ErrInternal, "insert escrow player=%d order=%d: %v", playerID, orderID, ierr)
	}

	// 2) 从活跃余额扣减(不足 → ErrInventoryInsufficient,整笔回滚含 escrow 行)。
	switch kind {
	case EscrowKindItem:
		if _, derr := deductItemTx(ctx, tx, playerID, itemConfigID, quantity); derr != nil {
			return false, derr
		}
	case EscrowKindGold:
		if _, derr := deductGoldTx(ctx, tx, playerID, frozenGold); derr != nil {
			return false, derr
		}
	default:
		return false, errcode.New(errcode.ErrInvalidArg, "unknown escrow kind %d", kind)
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit freeze player=%d order=%d: %v", playerID, orderID, cerr)
	}
	return false, nil
}

const ensureAuctionEscrowMaxAttempts = 3

// EnsureAuctionEscrow 确保旧版本遗留订单具备足够的 active escrow。
//
// 并发无行时直接争用 uk_player_order 的 INSERT：胜者在同一事务扣活跃资产并提交；失败者收到
// 1062 后必须先回滚，再以新事务 SELECT ... FOR UPDATE 严格核对胜者提交的整行。这样唯一键冲突
// 只是“转入校验路径”的信号，绝不等价于幂等成功，也不会发生两个事务都扣活跃资产。
func (r *MySQLInventoryRepo) EnsureAuctionEscrow(
	ctx context.Context,
	playerID, orderID uint64,
	kind EscrowKind,
	itemConfigID uint32,
	remainingQuantity, unitPrice int64,
) (bool, error) {
	if playerID == 0 || orderID == 0 || itemConfigID == 0 || remainingQuantity <= 0 || unitPrice <= 0 {
		return false, errcode.New(errcode.ErrInvalidArg,
			"invalid ensure escrow player=%d order=%d kind=%d item=%d remaining=%d price=%d",
			playerID, orderID, kind, itemConfigID, remainingQuantity, unitPrice)
	}
	if kind != EscrowKindItem && kind != EscrowKindGold {
		return false, errcode.New(errcode.ErrInvalidArg, "unknown escrow kind %d", kind)
	}
	var requiredGold int64
	if kind == EscrowKindGold {
		var ok bool
		requiredGold, ok = safeMulPositiveInt64(remainingQuantity, unitPrice)
		if !ok {
			return false, errcode.New(errcode.ErrInvalidArg,
				"ensure escrow amount overflow order=%d remaining=%d price=%d", orderID, remainingQuantity, unitPrice)
		}
	}

	for attempt := 1; attempt <= ensureAuctionEscrowMaxAttempts; attempt++ {
		created, duplicate, err := r.tryCreateAuctionEscrow(
			ctx, playerID, orderID, kind, itemConfigID, remainingQuantity, requiredGold)
		if err != nil {
			return false, err
		}
		if created {
			return false, nil
		}
		if !duplicate {
			return false, errcode.New(errcode.ErrInternal,
				"ensure escrow neither created nor duplicate player=%d order=%d", playerID, orderID)
		}

		found, err := r.validateExistingAuctionEscrow(
			ctx, playerID, orderID, kind, itemConfigID, remainingQuantity, requiredGold)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
		// auction_escrow 的 active 行正常流程从不 DELETE(保留期清理只删 closed 且超期 90 天
		// 的行,见 DeleteClosedEscrowBefore;本函数只服务 OPEN/PARTIAL 遗留订单,其 escrow 若存在
		// 必为 active,不会被清理命中)。此处仅防御外部清理恰好发生在 1062 与复查之间;
		// 重新竞争 INSERT,仍不把不可解释的消失当成功。
	}
	return false, errcode.New(errcode.ErrInternal,
		"escrow disappeared after duplicate player=%d order=%d attempts=%d",
		playerID, orderID, ensureAuctionEscrowMaxAttempts)
}

func (r *MySQLInventoryRepo) tryCreateAuctionEscrow(
	ctx context.Context,
	playerID, orderID uint64,
	kind EscrowKind,
	itemConfigID uint32,
	remainingQuantity, requiredGold int64,
) (created, duplicate bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, errcode.New(errcode.ErrInternal, "begin ensure escrow tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var frozenQty, frozenGold int64
	if kind == EscrowKindItem {
		frozenQty = remainingQuantity
	} else {
		frozenGold = requiredGold
	}
	const ins = `INSERT INTO auction_escrow
        (player_id, order_id, kind, item_config_id, frozen_qty, frozen_gold, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, ins,
		playerID, orderID, int8(kind), itemConfigID, frozenQty, frozenGold, escrowStatusActive); err != nil {
		if isDupErr(err) {
			// defer Rollback 在返回给复查路径前完成；不能在仍持有失败事务时读取并宣称成功。
			return false, true, nil
		}
		return false, false, errcode.New(errcode.ErrInternal,
			"insert ensured escrow player=%d order=%d: %v", playerID, orderID, err)
	}

	switch kind {
	case EscrowKindItem:
		if _, err := deductItemTx(ctx, tx, playerID, itemConfigID, remainingQuantity); err != nil {
			return false, false, err
		}
	case EscrowKindGold:
		if _, err := deductGoldTx(ctx, tx, playerID, requiredGold); err != nil {
			return false, false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return false, false, errcode.New(errcode.ErrInternal,
			"commit ensure escrow player=%d order=%d: %v", playerID, orderID, err)
	}
	return true, false, nil
}

func (r *MySQLInventoryRepo) validateExistingAuctionEscrow(
	ctx context.Context,
	playerID, orderID uint64,
	wantKind EscrowKind,
	wantItemConfigID uint32,
	remainingQuantity, requiredGold int64,
) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin validate escrow tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		kind         int8
		itemConfigID uint32
		frozenQty    int64
		frozenGold   int64
		status       int8
	)
	err = tx.QueryRowContext(ctx,
		`SELECT kind, item_config_id, frozen_qty, frozen_gold, status
         FROM auction_escrow WHERE player_id = ? AND order_id = ? FOR UPDATE`,
		playerID, orderID).Scan(&kind, &itemConfigID, &frozenQty, &frozenGold, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal,
			"lock ensured escrow player=%d order=%d: %v", playerID, orderID, err)
	}
	if status != escrowStatusActive {
		return true, errcode.New(errcode.ErrInventoryIdempotencyConflict,
			"escrow is not active player=%d order=%d status=%d", playerID, orderID, status)
	}
	if EscrowKind(kind) != wantKind || itemConfigID != wantItemConfigID {
		return true, errcode.New(errcode.ErrInventoryIdempotencyConflict,
			"escrow identity conflict player=%d order=%d kind=%d item=%d want_kind=%d want_item=%d",
			playerID, orderID, kind, itemConfigID, wantKind, wantItemConfigID)
	}

	switch wantKind {
	case EscrowKindItem:
		if frozenGold != 0 {
			return true, errcode.New(errcode.ErrInventoryIdempotencyConflict,
				"item escrow carries gold player=%d order=%d frozen_gold=%d", playerID, orderID, frozenGold)
		}
		if frozenQty < remainingQuantity {
			return true, errcode.New(errcode.ErrInventoryInsufficient,
				"item escrow short player=%d order=%d frozen=%d need=%d",
				playerID, orderID, frozenQty, remainingQuantity)
		}
	case EscrowKindGold:
		if frozenQty != 0 {
			return true, errcode.New(errcode.ErrInventoryIdempotencyConflict,
				"gold escrow carries item quantity player=%d order=%d frozen_qty=%d", playerID, orderID, frozenQty)
		}
		if frozenGold < requiredGold {
			return true, errcode.New(errcode.ErrInventoryInsufficient,
				"gold escrow short player=%d order=%d frozen=%d need=%d",
				playerID, orderID, frozenGold, requiredGold)
		}
	}
	return true, nil
}

func safeMulPositiveInt64(a, b int64) (int64, bool) {
	if a <= 0 || b <= 0 || a > (int64(^uint64(0)>>1))/b {
		return 0, false
	}
	return a * b, true
}

// ReleaseEscrow 退还某挂单 escrow 残余到玩家活跃余额并关闭托管(一个本地事务)。幂等键 = escrow 行状态。
func (r *MySQLInventoryRepo) ReleaseEscrow(ctx context.Context, playerID, orderID uint64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		kind         int8
		itemConfigID uint32
		frozenQty    int64
		frozenGold   int64
		status       int8
	)
	qerr := tx.QueryRowContext(ctx,
		`SELECT kind, item_config_id, frozen_qty, frozen_gold, status FROM auction_escrow WHERE player_id = ? AND order_id = ? FOR UPDATE`,
		playerID, orderID).Scan(&kind, &itemConfigID, &frozenQty, &frozenGold, &status)
	if errors.Is(qerr, sql.ErrNoRows) {
		// 无 escrow(冻结失败的挂单从未建 escrow)→ 无可退,幂等 no-op。
		return true, nil
	}
	if qerr != nil {
		return false, errcode.New(errcode.ErrInternal, "lock escrow player=%d order=%d: %v", playerID, orderID, qerr)
	}
	if status == escrowStatusClosed {
		return true, nil // 已退还,幂等 no-op。
	}

	switch EscrowKind(kind) {
	case EscrowKindItem:
		if frozenQty > 0 {
			if aerr := addItemTx(ctx, tx, playerID, itemConfigID, frozenQty); aerr != nil {
				return false, aerr
			}
		}
	case EscrowKindGold:
		if frozenGold > 0 {
			if aerr := addGoldTx(ctx, tx, playerID, frozenGold); aerr != nil {
				return false, aerr
			}
		}
	}

	if _, uerr := tx.ExecContext(ctx,
		`UPDATE auction_escrow SET frozen_qty = 0, frozen_gold = 0, status = ? WHERE player_id = ? AND order_id = ?`,
		escrowStatusClosed, playerID, orderID); uerr != nil {
		return false, errcode.New(errcode.ErrInternal, "close escrow player=%d order=%d: %v", playerID, orderID, uerr)
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit release player=%d order=%d: %v", playerID, orderID, cerr)
	}
	return false, nil
}

// consumeItemEscrowTx 在事务里锁卖单道具 escrow 并消费 qty(成交交付)。
//   - escrow 不存在 / 非 item / 余量不足 → 错误(正常流程不应发生,escrow 充足由冻结保证)。
func consumeItemEscrowTx(ctx context.Context, tx *sql.Tx, playerID, orderID uint64, itemConfigID uint32, qty int64) error {
	var (
		kind      int8
		itemID    uint32
		frozenQty int64
		status    int8
	)
	err := tx.QueryRowContext(ctx,
		`SELECT kind, item_config_id, frozen_qty, status FROM auction_escrow WHERE player_id = ? AND order_id = ? FOR UPDATE`,
		playerID, orderID).Scan(&kind, &itemID, &frozenQty, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInventoryInsufficient, "item escrow not found player=%d order=%d", playerID, orderID)
	}
	if err != nil {
		return errcode.New(errcode.ErrInternal, "lock item escrow player=%d order=%d: %v", playerID, orderID, err)
	}
	if EscrowKind(kind) != EscrowKindItem || itemID != itemConfigID {
		return errcode.New(errcode.ErrInternal, "escrow kind/item mismatch player=%d order=%d kind=%d item=%d want item=%d", playerID, orderID, kind, itemID, itemConfigID)
	}
	if status == escrowStatusClosed || frozenQty < qty {
		return errcode.New(errcode.ErrInventoryInsufficient, "item escrow short player=%d order=%d frozen=%d need=%d", playerID, orderID, frozenQty, qty)
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE auction_escrow SET frozen_qty = frozen_qty - ? WHERE player_id = ? AND order_id = ?`,
		qty, playerID, orderID); uerr != nil {
		return errcode.New(errcode.ErrInternal, "consume item escrow player=%d order=%d: %v", playerID, orderID, uerr)
	}
	return nil
}

// consumeGoldEscrowTx 在事务里锁买单金币 escrow 并消费 gold(成交付款)。
func consumeGoldEscrowTx(ctx context.Context, tx *sql.Tx, playerID, orderID uint64, gold int64) error {
	var (
		kind       int8
		frozenGold int64
		status     int8
	)
	err := tx.QueryRowContext(ctx,
		`SELECT kind, frozen_gold, status FROM auction_escrow WHERE player_id = ? AND order_id = ? FOR UPDATE`,
		playerID, orderID).Scan(&kind, &frozenGold, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInventoryInsufficient, "gold escrow not found player=%d order=%d", playerID, orderID)
	}
	if err != nil {
		return errcode.New(errcode.ErrInternal, "lock gold escrow player=%d order=%d: %v", playerID, orderID, err)
	}
	if EscrowKind(kind) != EscrowKindGold {
		return errcode.New(errcode.ErrInternal, "escrow kind mismatch player=%d order=%d kind=%d want gold", playerID, orderID, kind)
	}
	if status == escrowStatusClosed || frozenGold < gold {
		return errcode.New(errcode.ErrInventoryInsufficient, "gold escrow short player=%d order=%d frozen=%d need=%d", playerID, orderID, frozenGold, gold)
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE auction_escrow SET frozen_gold = frozen_gold - ? WHERE player_id = ? AND order_id = ?`,
		gold, playerID, orderID); uerr != nil {
		return errcode.New(errcode.ErrInternal, "consume gold escrow player=%d order=%d: %v", playerID, orderID, uerr)
	}
	return nil
}

// ── 保留期清理(CLAUDE.md §9 不变量 24)────────────────────────────────────────
//
// inventory_ledger / auction_escrow(closed) 是只增表,靠 biz/sweep.go 周期批量删除保证有界。
// DELETE ... LIMIT 幂等,多副本并发跑只多花空批,不需要锁(对齐 mail sweep)。

func (r *MySQLInventoryRepo) SweepLedgerBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_trade", "inventory_ledger",
		"created_at < DATE_SUB(NOW(), INTERVAL ? DAY)", limit, retentionDays)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep ledger: %v", err)
	}
	return out, nil
}

func (r *MySQLInventoryRepo) SweepClosedEscrowBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_trade", "auction_escrow",
		"status = ? AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY)", limit, escrowStatusClosed, retentionDays)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep closed escrow: %v", err)
	}
	return out, nil
}

func (r *MySQLInventoryRepo) execAffected(ctx context.Context, op, q string, args ...any) (int64, error) {
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "%s: %v", op, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// isDupErr 判断是否 MySQL 1062 唯一键冲突(go-sql-driver 错误串含 "Error 1062")。
func isDupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}
