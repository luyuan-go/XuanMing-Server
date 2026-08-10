// inventory_transfer_mysql_test.go — 邮件 transfer 实例托管的真实 MySQL 集成测试
// (2026-07-22,bag-domain.md §7.1)。门控与 fixture 同 inventory_repo_mysql_test.go:
// 未设置 PANDORA_TEST_MYSQL_DSN 明确 Skip。
//
// 覆盖三不变量:①扣出与托管同事务、实例互斥存在;②领取只改归属、鉴定态/词条/绑定
// 原样保留;③领取只认托管行(缺行/收件人不符/config 漂移整批拒)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// seedInstanceRow 直插一行装备实例(identified/attrs/bound 全字段,slot 可为 NULL)。
// attrs 传 pb 二进制(列 VARBINARY),空切片 → 写 NULL。
func seedInstanceRow(t *testing.T, db *sql.DB, playerID, instanceID uint64, configID uint32, identified bool, attrsPB []byte, slot any, bound bool) {
	t.Helper()
	var attrs any
	if len(attrsPB) > 0 {
		attrs = attrsPB
	}
	if _, err := db.Exec(
		`INSERT INTO player_item_instance (instance_id, player_id, item_config_id, identified, attributes, slot_index, bound)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		instanceID, playerID, configID, identified, attrs, slot, bound); err != nil {
		t.Fatalf("seed instance player=%d id=%d: %v", playerID, instanceID, err)
	}
}

// queryInstanceOwner 返回实例当前归属玩家(不存在 → 0)。
func queryInstanceOwner(t *testing.T, db *sql.DB, instanceID uint64) uint64 {
	t.Helper()
	var owner uint64
	err := db.QueryRow(`SELECT player_id FROM player_item_instance WHERE instance_id = ?`, instanceID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("query instance owner id=%d: %v", instanceID, err)
	}
	return owner
}

// queryEscrowExists 返回托管行是否存在。
func queryEscrowExists(t *testing.T, db *sql.DB, instanceID uint64) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mail_transfer_escrow WHERE instance_id = ?`, instanceID).Scan(&n); err != nil {
		t.Fatalf("count escrow id=%d: %v", instanceID, err)
	}
	return n > 0
}

// queryInstanceFull 读实例全字段(领取后逐字段核对用)。
func queryInstanceFull(t *testing.T, db *sql.DB, instanceID uint64) (owner uint64, identified bool, attrs []byte, slot sql.NullInt32, bound bool) {
	t.Helper()
	var identifiedI8, boundI8 int8
	if err := db.QueryRow(
		`SELECT player_id, identified, attributes, slot_index, bound FROM player_item_instance WHERE instance_id = ?`,
		instanceID).Scan(&owner, &identifiedI8, &attrs, &slot, &boundI8); err != nil {
		t.Fatalf("query instance full id=%d: %v", instanceID, err)
	}
	return owner, identifiedI8 != 0, attrs, slot, boundI8 != 0
}

func TestMailTransferEscrow_MySQL(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)
	ctx := context.Background()

	attrsPB, err := encodeInstanceAttrs(ctx, "player_item_instance", []ItemAttribute{{AttrID: 1, Value: 42}, {AttrID: 7, Value: -3}})
	if err != nil {
		t.Fatalf("encode seed attrs: %v", err)
	}

	t.Run("EscrowOutMovesRowAtomically", func(t *testing.T) {
		seedInstanceRow(t, f.db, 201, 9001, 5001, true, attrsPB, 0, false)
		rows, already, err := repo.EscrowOutInstances(ctx, 201, 202, []uint64{9001}, "gift:1", "d")
		if err != nil {
			t.Fatalf("escrow out: %v", err)
		}
		if already {
			t.Fatal("首次托管不应命中幂等")
		}
		if len(rows) != 1 || rows[0].InstanceID != 9001 || rows[0].ItemConfigID != 5001 ||
			!rows[0].Identified || len(rows[0].Attributes) != 2 || rows[0].ToPlayerID != 202 {
			t.Fatalf("托管快照字段错: %+v", rows)
		}
		// 互斥存在:实例表无行,托管表有行。
		if owner := queryInstanceOwner(t, f.db, 9001); owner != 0 {
			t.Fatalf("扣出后实例仍在玩家 %d 背包", owner)
		}
		if !queryEscrowExists(t, f.db, 9001) {
			t.Fatal("扣出后托管行不存在")
		}

		// 幂等回放:同 key 同请求 → 返回同快照,不重复搬移。
		rows2, already2, err := repo.EscrowOutInstances(ctx, 201, 202, []uint64{9001}, "gift:1", "d")
		if err != nil || !already2 || len(rows2) != 1 || rows2[0].InstanceID != 9001 {
			t.Fatalf("escrow out replay: rows=%+v already=%v err=%v", rows2, already2, err)
		}

		// key 复用到不同内容 → 指纹冲突。
		if _, _, err := repo.EscrowOutInstances(ctx, 201, 203, []uint64{9001}, "gift:1", "d"); asCode(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("key 复用应判指纹冲突, got %v", err)
		}
	})

	t.Run("EscrowOutRejectsBoundAndMissing", func(t *testing.T) {
		seedInstanceRow(t, f.db, 204, 9002, 5001, false, nil, nil, true) // bound
		if _, _, err := repo.EscrowOutInstances(ctx, 204, 205, []uint64{9002}, "gift:2", "d"); asCode(err) != errcode.ErrInventoryInstanceBound {
			t.Fatalf("绑定实例应拒, got %v", err)
		}
		// 整批回滚:bound 拒后实例仍在原处。
		if owner := queryInstanceOwner(t, f.db, 9002); owner != 204 {
			t.Fatalf("拒后实例归属漂移: owner=%d", owner)
		}
		if _, _, err := repo.EscrowOutInstances(ctx, 204, 205, []uint64{99999}, "gift:3", "d"); asCode(err) != errcode.ErrInventoryItemNotFound {
			t.Fatalf("不存在实例应拒, got %v", err)
		}
	})

	t.Run("ClaimOnlyTrustsEscrowRow", func(t *testing.T) {
		seedInstanceRow(t, f.db, 206, 9003, 5002, true, attrsPB, 1, false)
		if _, _, err := repo.EscrowOutInstances(ctx, 206, 207, []uint64{9003}, "gift:4", "d"); err != nil {
			t.Fatalf("escrow out: %v", err)
		}

		// 收件人不符 → 拒,托管行保持。
		if _, err := repo.ClaimTransferInstances(ctx, 208, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 5002}}, 10, "xfer:w1", "d"); asCode(err) != errcode.ErrInventoryItemNotFound {
			t.Fatalf("非预期领取人应拒, got %v", err)
		}
		// config 漂移 → 拒。
		if _, err := repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 9999}}, 10, "xfer:w2", "d"); asCode(err) != errcode.ErrInventoryItemNotFound {
			t.Fatalf("config 漂移应拒, got %v", err)
		}
		// 未托管实例 → 拒。
		if _, err := repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 88888, ItemConfigID: 5002}}, 10, "xfer:w3", "d"); asCode(err) != errcode.ErrInventoryItemNotFound {
			t.Fatalf("未托管实例应拒, got %v", err)
		}
		if !queryEscrowExists(t, f.db, 9003) {
			t.Fatal("拒后托管行不应消失")
		}

		// 容量满 → 拒,可重试。
		if _, err := repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 5002}}, 0, "xfer:c1", "d"); asCode(err) != errcode.ErrInventoryCapacityFull {
			t.Fatalf("容量 0 应拒, got %v", err)
		}

		// 正常领取:只改归属,鉴定态/词条原样。
		already, err := repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 5002}}, 10, "xfer:ok", "d")
		if err != nil || already {
			t.Fatalf("claim: already=%v err=%v", already, err)
		}
		owner, identified, attrs, slot, bound := queryInstanceFull(t, f.db, 9003)
		if owner != 207 || !identified || bound || len(attrs) == 0 || !slot.Valid {
			t.Fatalf("领取后字段错: owner=%d identified=%v attrs=%d bytes slot=%v bound=%v", owner, identified, len(attrs), slot, bound)
		}
		if queryEscrowExists(t, f.db, 9003) {
			t.Fatal("领取后托管行应删除")
		}

		// 幂等回放:同 key → already 成功(mail crash-after-claim 重试恰好一次)。
		already, err = repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 5002}}, 10, "xfer:ok", "d")
		if err != nil || !already {
			t.Fatalf("claim replay: already=%v err=%v", already, err)
		}
		// 换 key 再领(伪重复)→ 托管行已无,fail-closed。
		if _, err := repo.ClaimTransferInstances(ctx, 207, []TransferClaimItem{{InstanceID: 9003, ItemConfigID: 5002}}, 10, "xfer:again", "d"); asCode(err) != errcode.ErrInventoryItemNotFound {
			t.Fatalf("重复领取新 key 应拒, got %v", err)
		}
	})

	t.Run("ReleaseReturnsToSourceUnconditionally", func(t *testing.T) {
		seedInstanceRow(t, f.db, 209, 9004, 5003, false, nil, 0, false)
		if _, _, err := repo.EscrowOutInstances(ctx, 209, 210, []uint64{9004}, "gift:5", "d"); err != nil {
			t.Fatalf("escrow out: %v", err)
		}
		released, err := repo.ReleaseTransferEscrow(ctx, []uint64{9004})
		if err != nil || released != 1 {
			t.Fatalf("release: n=%d err=%v", released, err)
		}
		owner, _, _, slot, _ := queryInstanceFull(t, f.db, 9004)
		if owner != 209 || slot.Valid {
			t.Fatalf("释放应回源玩家且 slot NULL: owner=%d slot=%v", owner, slot)
		}
		// 幂等:行已释放 → no-op。
		released, err = repo.ReleaseTransferEscrow(ctx, []uint64{9004})
		if err != nil || released != 0 {
			t.Fatalf("release replay 应 no-op: n=%d err=%v", released, err)
		}
	})
}

// asCode 取业务错误码(非业务错误 → 0)。
func asCode(err error) errcode.Code {
	var ec *errcode.Error
	if errors.As(err, &ec) {
		return ec.Code
	}
	return 0
}
