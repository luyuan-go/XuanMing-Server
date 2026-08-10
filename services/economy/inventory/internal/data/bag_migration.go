// bag_migration.go — 旧 inventory 存量 → 背包域仓库段迁移
// (decision-revisit-bag-replay-semantics.md D5,bag-domain.md §10 phase 3,2026-07-22)。
//
// 时序纪律:迁移作业只能在旧写路径(GrantItems/UseItem/SellItem/escrow)全部冻结后运行
// (biz 配置门 legacy_migration_enabled 默认关,contract 阶段才开);跨库(pandora_trade →
// pandora_bag)无法同事务,幂等由 bag_migration 一玩家一行永久闸承担:
//   - 读侧:legacy 快照为普通读(冻结窗口内静止);
//   - 写侧:bag 库单事务 = 锁 bag_meta 行(串行化该玩家全部背包写,不 CAS epoch——迁移
//     不是 owner 写者,不得推进/受制于 owner_epoch)→ 查迁移闸 → 仓库段合并入段(容量
//     豁免,超容落位只出不进,§3.2)→ upsert bag_section + INSERT bag_migration,原子提交。
//   - 玩家在线也安全:仓库是后端驻留段(存储侧权威,不 checkout 进 DS),并发的
//     journal 写与迁移写都锁同一 bag_meta 行,天然串行。
//
// bound 实例 fail-closed:BagItem 尚无 bound 字段(phase 3 proto 批次补),绑定实例迁移
// 会静默丢失绑定约束 → 整玩家拒迁并报错,绝不静默降级(同 §7.1 transfer 接线前纪律)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// ListLegacyBagPlayers 游标枚举仍有存量(堆叠 count>0 或持有实例)的玩家(升序,含两表并集)。
func (r *MySQLInventoryRepo) ListLegacyBagPlayers(ctx context.Context, afterPlayerID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT player_id FROM (
  SELECT DISTINCT player_id FROM player_items WHERE count > 0 AND player_id > ?
  UNION
  SELECT DISTINCT player_id FROM player_item_instance WHERE player_id > ?
) u ORDER BY player_id LIMIT ?`, afterPlayerID, afterPlayerID, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list legacy bag players after=%d: %v", afterPlayerID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []uint64
	for rows.Next() {
		var id uint64
		if serr := rows.Scan(&id); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan legacy bag player: %v", serr)
		}
		out = append(out, id)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate legacy bag players: %v", rerr)
	}
	return out, nil
}

// LoadLegacyBagStock 读取单玩家 legacy 存量快照(堆叠 + 实例),转成 bag 域 BagItem 形状。
// bound=1 实例 fail-closed 拒(见文件头);attributes pb 解码失败同样拒(不静默丢词条)。
func (r *MySQLInventoryRepo) LoadLegacyBagStock(ctx context.Context, playerID uint64) ([]*bagv1.BagItem, error) {
	var items []*bagv1.BagItem

	stackRows, err := r.db.QueryContext(ctx,
		`SELECT item_config_id, count FROM player_items WHERE player_id = ? AND count > 0 ORDER BY item_config_id`,
		playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "load legacy stacks player=%d: %v", playerID, err)
	}
	defer func() { _ = stackRows.Close() }()
	for stackRows.Next() {
		var (
			configID uint32
			count    int64
		)
		if serr := stackRows.Scan(&configID, &count); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan legacy stack player=%d: %v", playerID, serr)
		}
		if count <= 0 {
			continue
		}
		if count > int64(math.MaxUint32) {
			return nil, errcode.New(errcode.ErrInvalidArg,
				"legacy stack count overflows uint32 player=%d config=%d count=%d", playerID, configID, count)
		}
		items = append(items, &bagv1.BagItem{ItemConfigId: configID, Count: uint32(count)})
	}
	if rerr := stackRows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate legacy stacks player=%d: %v", playerID, rerr)
	}

	instRows, err := r.db.QueryContext(ctx, `
SELECT instance_id, item_config_id, identified, attributes, bound
FROM player_item_instance WHERE player_id = ? ORDER BY instance_id`, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "load legacy instances player=%d: %v", playerID, err)
	}
	defer func() { _ = instRows.Close() }()
	for instRows.Next() {
		var (
			instanceID uint64
			configID   uint32
			identified bool
			attrsRaw   []byte
			bound      bool
		)
		if serr := instRows.Scan(&instanceID, &configID, &identified, &attrsRaw, &bound); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan legacy instance player=%d: %v", playerID, serr)
		}
		if bound {
			return nil, errcode.New(errcode.ErrInvalidState,
				"legacy bound instance blocks migration player=%d instance=%d (BagItem 尚无 bound 字段,phase 3 proto 批次补齐后放开;拒迁防绑定约束静默丢失)",
				playerID, instanceID)
		}
		item := &bagv1.BagItem{
			ItemConfigId: configID,
			Count:        1,
			InstanceId:   instanceID,
			Identified:   identified,
		}
		attrs, derr := decodeInstanceAttrs(attrsRaw)
		if derr != nil {
			return nil, errcode.New(errcode.ErrInternal,
				"decode legacy instance attrs player=%d instance=%d: %v", playerID, instanceID, derr)
		}
		for _, a := range attrs {
			item.Attrs = append(item.Attrs, &bagv1.BagItemAttribute{AttrId: a.AttrID, Value: a.Value})
		}
		items = append(items, item)
	}
	if rerr := instRows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate legacy instances player=%d: %v", playerID, rerr)
	}
	return items, nil
}

// legacyMigrationTotals 统计 legacy 快照的对账三元组。
func legacyMigrationTotals(items []*bagv1.BagItem) (stackKinds uint32, stackTotal uint64, instanceCount uint32) {
	kinds := map[uint32]bool{}
	for _, it := range items {
		if it.GetInstanceId() != 0 {
			instanceCount++
			continue
		}
		kinds[it.GetItemConfigId()] = true
		stackTotal += uint64(it.GetCount())
	}
	return uint32(len(kinds)), stackTotal, instanceCount
}

// SeedLegacyWarehouse 把 legacy 快照合并进仓库段(bag 库单事务;容量豁免超容落位)。
// 返回 (true, nil) = 本次完成迁移;(false, nil) = 迁移闸已存在(幂等重放 no-op)。
func (r *MySQLBagRepo) SeedLegacyWarehouse(ctx context.Context, playerID uint64, items []*bagv1.BagItem, maxStack BagMaxStack) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin seed tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 锁 bag_meta 行串行化该玩家全部背包写;迁移不是 owner 写者,不 CAS 不推进 epoch。
	if _, ierr := tx.ExecContext(ctx,
		`INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) VALUES (?, 0, 0)`, playerID); ierr != nil {
		return false, errcode.New(errcode.ErrInternal, "ensure bag_meta player=%d: %v", playerID, ierr)
	}
	var epochIgnored uint64
	if qerr := tx.QueryRowContext(ctx,
		`SELECT owner_epoch FROM bag_meta WHERE player_id = ? FOR UPDATE`, playerID).Scan(&epochIgnored); qerr != nil {
		return false, errcode.New(errcode.ErrInternal, "lock bag_meta player=%d: %v", playerID, qerr)
	}

	// 幂等闸:行已存在 = 已迁移,no-op(多副本并发/断点重跑安全)。
	var one int
	gerr := tx.QueryRowContext(ctx,
		`SELECT 1 FROM bag_migration WHERE player_id = ?`, playerID).Scan(&one)
	if gerr == nil {
		return false, nil
	}
	if !errors.Is(gerr, sql.ErrNoRows) {
		return false, errcode.New(errcode.ErrInternal, "check bag_migration player=%d: %v", playerID, gerr)
	}

	stackKinds, stackTotal, instanceCount := legacyMigrationTotals(items)
	if len(items) > 0 {
		// 加载仓库段既有内容(phase 2 期间的转移/领取可能已建段),合并入段。
		sec := &bagv1.BagSection{BagType: BagWarehouseType}
		var blob []byte
		serr := tx.QueryRowContext(ctx,
			`SELECT section FROM bag_section WHERE player_id = ? AND bag_type = ? FOR UPDATE`,
			playerID, BagWarehouseType).Scan(&blob)
		if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
			return false, errcode.New(errcode.ErrInternal, "lock warehouse section player=%d: %v", playerID, serr)
		}
		if serr == nil {
			if uerr := proto.Unmarshal(blob, sec); uerr != nil {
				return false, errcode.New(errcode.ErrInternal, "decode warehouse section player=%d: %v", playerID, uerr)
			}
			sec.BagType = BagWarehouseType
		}
		// 容量豁免:capacity 传 uint32 上限 = 迁移一次性超容落位;其后新开格被真实容量
		// 拒(sectionAddItems 容量门只拦新开格),即 §3.2 的"只出不进",随取用自愈。
		// 拆堆/实例查重复用 journal 写路径同一函数,语义单源。
		if aerr := sectionAddItems(sec, items, math.MaxUint32, maxStack); aerr != nil {
			return false, aerr
		}
		secBlob, merr := proto.Marshal(sec)
		if merr != nil {
			return false, errcode.New(errcode.ErrInternal, "marshal warehouse section player=%d: %v", playerID, merr)
		}
		const up = `INSERT INTO bag_section (player_id, bag_type, generation, section) VALUES (?, ?, 0, ?)
ON DUPLICATE KEY UPDATE section = VALUES(section)`
		if _, uerr := tx.ExecContext(ctx, up, playerID, BagWarehouseType, secBlob); uerr != nil {
			return false, errcode.New(errcode.ErrInternal, "upsert warehouse section player=%d: %v", playerID, uerr)
		}
	}

	if _, ierr := tx.ExecContext(ctx,
		`INSERT INTO bag_migration (player_id, stack_kinds, stack_total, instance_count) VALUES (?, ?, ?, ?)`,
		playerID, stackKinds, stackTotal, instanceCount); ierr != nil {
		return false, errcode.New(errcode.ErrInternal, "insert bag_migration player=%d: %v", playerID, ierr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit seed player=%d: %v", playerID, cerr)
	}
	return true, nil
}

// VerifyLegacyWarehouse 迁后对账(只在割接窗口、读流量放开前有意义):
//   - 每个 legacy 实例必须在仓库段中(逐 instance_id);
//   - 每个 legacy config 的仓库段总数 ≥ legacy 总数(段内可能含 phase 2 既有同 config 存量);
//   - bag_migration 记录的三元组必须与 legacy 快照一致(冻结窗口被违反时立刻暴露)。
func (r *MySQLBagRepo) VerifyLegacyWarehouse(ctx context.Context, playerID uint64, legacy []*bagv1.BagItem) error {
	var (
		stackKinds    uint32
		stackTotal    uint64
		instanceCount uint32
	)
	gerr := r.db.QueryRowContext(ctx,
		`SELECT stack_kinds, stack_total, instance_count FROM bag_migration WHERE player_id = ?`,
		playerID).Scan(&stackKinds, &stackTotal, &instanceCount)
	if errors.Is(gerr, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInvalidState, "verify before migration player=%d", playerID)
	}
	if gerr != nil {
		return errcode.New(errcode.ErrInternal, "read bag_migration player=%d: %v", playerID, gerr)
	}
	wantKinds, wantTotal, wantInstances := legacyMigrationTotals(legacy)
	if stackKinds != wantKinds || stackTotal != wantTotal || instanceCount != wantInstances {
		return errcode.New(errcode.ErrInvalidState,
			"migration totals drift player=%d recorded=(%d,%d,%d) legacy=(%d,%d,%d) — 冻结窗口被违反?",
			playerID, stackKinds, stackTotal, instanceCount, wantKinds, wantTotal, wantInstances)
	}

	sec := &bagv1.BagSection{}
	var blob []byte
	serr := r.db.QueryRowContext(ctx,
		`SELECT section FROM bag_section WHERE player_id = ? AND bag_type = ?`,
		playerID, BagWarehouseType).Scan(&blob)
	if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInternal, "read warehouse section player=%d: %v", playerID, serr)
	}
	if serr == nil {
		if uerr := proto.Unmarshal(blob, sec); uerr != nil {
			return errcode.New(errcode.ErrInternal, "decode warehouse section player=%d: %v", playerID, uerr)
		}
	}
	haveInstances := map[uint64]bool{}
	haveCounts := map[uint32]uint64{}
	for _, it := range sec.GetItems() {
		if it.GetInstanceId() != 0 {
			haveInstances[it.GetInstanceId()] = true
			continue
		}
		haveCounts[it.GetItemConfigId()] += uint64(it.GetCount())
	}
	wantCounts := map[uint32]uint64{}
	for _, it := range legacy {
		if it.GetInstanceId() != 0 {
			if !haveInstances[it.GetInstanceId()] {
				return errcode.New(errcode.ErrInvalidState,
					"migrated instance missing player=%d instance=%d", playerID, it.GetInstanceId())
			}
			continue
		}
		wantCounts[it.GetItemConfigId()] += uint64(it.GetCount())
	}
	for configID, want := range wantCounts {
		if haveCounts[configID] < want {
			return errcode.New(errcode.ErrInvalidState,
				"migrated stack short player=%d config=%d have=%d want>=%d",
				playerID, configID, haveCounts[configID], want)
		}
	}
	return nil
}
