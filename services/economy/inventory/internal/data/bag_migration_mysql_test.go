// bag_migration_mysql_test.go — 存量迁移真 MySQL 集成测试(D5;双库:trade + bag)。
// 门控:PANDORA_TEST_MYSQL_DSN(不带 database 的服务器 DSN)。
package data

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// TestBagLegacyMigration_MySQL 全链:枚举 → 快照(bound fail-closed)→ 幂等落位(拆堆 +
// 超容)→ 对账 → 超容段"只出不进"(真实 journal 写路径)→ 漂移暴露。
func TestBagLegacyMigration_MySQL(t *testing.T) {
	inv := openInventoryMySQLFixture(t)
	bag := openBagMySQLFixture(t)
	invRepo := NewMySQLInventoryRepo(inv.db)
	bagRepo := NewMySQLBagRepo(bag.db)
	ctx := context.Background()

	const (
		playerOK    = uint64(301)
		playerBound = uint64(302)
	)
	// legacy 存量:堆叠 250(拆 99+99+52)+ 5;实例 2 件(一件已鉴定带词条)。
	mustExec(t, inv.db, `INSERT INTO player_items (player_id, item_config_id, count) VALUES (?, 10001, 250), (?, 10002, 5)`,
		playerOK, playerOK)
	attrsPB, err := encodeInstanceAttrs(ctx, "player_item_instance", []ItemAttribute{{AttrID: 7, Value: 42}})
	if err != nil {
		t.Fatalf("编码存量实例词条: %v", err)
	}
	mustExec(t, inv.db, `INSERT INTO player_item_instance (instance_id, player_id, item_config_id, identified, attributes, bound)
VALUES (9001, ?, 20001, 1, ?, 0), (9002, ?, 20001, 0, NULL, 0)`, playerOK, attrsPB, playerOK)
	// bound 实例玩家:fail-closed 拒迁。
	mustExec(t, inv.db, `INSERT INTO player_item_instance (instance_id, player_id, item_config_id, identified, attributes, bound)
VALUES (9101, ?, 20002, 0, NULL, 1)`, playerBound)

	t.Run("枚举与快照", func(t *testing.T) {
		players, err := invRepo.ListLegacyBagPlayers(ctx, 0, 10)
		if err != nil || len(players) != 2 || players[0] != playerOK || players[1] != playerBound {
			t.Fatalf("枚举不符: %v err=%v", players, err)
		}
		// 游标续读。
		players, err = invRepo.ListLegacyBagPlayers(ctx, playerOK, 10)
		if err != nil || len(players) != 1 || players[0] != playerBound {
			t.Fatalf("游标续读不符: %v err=%v", players, err)
		}
		stock, err := invRepo.LoadLegacyBagStock(ctx, playerOK)
		if err != nil || len(stock) != 4 {
			t.Fatalf("快照不符: %d 项 err=%v", len(stock), err)
		}
		var identified *bagv1.BagItem
		for _, it := range stock {
			if it.GetInstanceId() == 9001 {
				identified = it
			}
		}
		if identified == nil || !identified.GetIdentified() || len(identified.GetAttrs()) != 1 ||
			identified.GetAttrs()[0].GetAttrId() != 7 || identified.GetAttrs()[0].GetValue() != 42 {
			t.Fatalf("实例鉴定态/词条未保真: %+v", identified)
		}
		// bound fail-closed。
		if _, err := invRepo.LoadLegacyBagStock(ctx, playerBound); errcode.As(err) != errcode.ErrInvalidState {
			t.Fatalf("bound 实例应 fail-closed 拒迁: %v", err)
		}
	})

	stock, err := invRepo.LoadLegacyBagStock(ctx, playerOK)
	if err != nil {
		t.Fatalf("重取快照: %v", err)
	}

	t.Run("幂等落位与对账", func(t *testing.T) {
		migrated, err := bagRepo.SeedLegacyWarehouse(ctx, playerOK, stock, bagTestMaxStack)
		if err != nil || !migrated {
			t.Fatalf("首迁应完成: migrated=%v err=%v", migrated, err)
		}
		// 幂等重放 no-op(多副本并发/断点重跑)。
		migrated, err = bagRepo.SeedLegacyWarehouse(ctx, playerOK, stock, bagTestMaxStack)
		if err != nil || migrated {
			t.Fatalf("重放应 no-op: migrated=%v err=%v", migrated, err)
		}
		// 拆堆落位:250→(99,99,52) + 5 一格 + 实例两格 = 6 格。
		secs, err := bagRepo.GetSections(ctx, playerOK, []uint32{BagWarehouseType}, bagTestCaps)
		if err != nil || len(secs) != 1 {
			t.Fatalf("读仓库段: %v err=%v", secs, err)
		}
		if got := len(secs[0].GetItems()); got != 6 {
			t.Fatalf("拆堆后应 6 格,实际 %d: %+v", got, secs[0].GetItems())
		}
		if err := bagRepo.VerifyLegacyWarehouse(ctx, playerOK, stock); err != nil {
			t.Fatalf("对账应通过: %v", err)
		}
	})

	t.Run("超容只出不进走真实journal路径", func(t *testing.T) {
		// bagTestCaps 仓库容量 10,当前 6 格未超;换 4 格容量视角 = 超容段。
		fourSlots := func(bagType uint32) uint32 {
			if bagType == BagWarehouseType {
				return 4
			}
			return 0
		}
		// 新开格拒(超容只进不了新格)。
		_, err := bagRepo.AppendJournal(ctx, playerOK, 1, []*bagv1.BagJournalEntry{
			mkClaimEntry(1, "over-grant", BagWarehouseType, 0, stackItem(30001, 1)),
		}, fourSlots, bagTestMaxStack, 0)
		if errcode.As(err) != errcode.ErrBagCapacityFull {
			t.Fatalf("超容段新开格应拒: %v", err)
		}
		// 扣减(转出到随身)照常 —— 只出不进。
		if _, err := bagRepo.AppendJournal(ctx, playerOK, 1, []*bagv1.BagJournalEntry{
			{JournalSeq: 1, BagType: BagWarehouseType, IdempotencyKey: "over-drain",
				Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
					ToBagType: 0, Items: []*bagv1.BagItem{stackItem(10001, 200)},
				}}},
		}, fourSlots, bagTestMaxStack, 0); err != nil {
			t.Fatalf("超容段扣减应照常: %v", err)
		}
	})

	t.Run("冻结窗口漂移暴露", func(t *testing.T) {
		// 模拟冻结纪律被违反:迁后 legacy 又长了 1 个 → 对账必须报警。
		mustExec(t, inv.db, `UPDATE player_items SET count = count + 1 WHERE player_id = ? AND item_config_id = 10002`, playerOK)
		drifted, err := invRepo.LoadLegacyBagStock(ctx, playerOK)
		if err != nil {
			t.Fatalf("重取漂移快照: %v", err)
		}
		if err := bagRepo.VerifyLegacyWarehouse(ctx, playerOK, drifted); errcode.As(err) != errcode.ErrInvalidState {
			t.Fatalf("漂移应被对账暴露: %v", err)
		}
	})
}
