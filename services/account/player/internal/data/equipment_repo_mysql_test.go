package data

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

func createEquipmentTestTable(t *testing.T, f *attributeMySQLFixture) {
	t.Helper()
	if _, err := f.db.Exec(`CREATE TABLE player_equipment (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        slot INT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        instance_id BIGINT UNSIGNED NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_player_slot (player_id, slot),
        UNIQUE KEY uk_player_instance (player_id, instance_id),
        KEY idx_player (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("创建 player_equipment: %v", err)
	}
}

func TestEquipmentRepoExactInstanceAndLegacyRead_MySQL(t *testing.T) {
	f := openAttributeTestDB(t)
	createEquipmentTestTable(t, f)
	repo := NewMySQLPlayerRepo(f.db)
	ctx := context.Background()

	// 模拟 000006 前旧二进制：INSERT 不带新列，数据库写 NULL。
	if _, err := f.db.ExecContext(ctx,
		`INSERT INTO player_equipment (player_id, slot, item_config_id) VALUES (7001, 1, 10003)`); err != nil {
		t.Fatalf("写旧预设: %v", err)
	}
	legacy, err := repo.GetEquipment(ctx, 7001)
	if err != nil || len(legacy) != 1 || legacy[0].InstanceID != 0 {
		t.Fatalf("旧预设只读映射错误: got=%+v err=%v", legacy, err)
	}

	want := []EquipmentSlot{
		{Slot: 1, ItemConfigID: 10003, InstanceID: 9001},
		{Slot: 2, ItemConfigID: 10027, InstanceID: 9002},
	}
	if err := repo.SetEquipment(ctx, 7001, want); err != nil {
		t.Fatalf("写精确预设: %v", err)
	}
	got, err := repo.GetEquipment(ctx, 7001)
	if err != nil || len(got) != 2 {
		t.Fatalf("读精确预设: got=%+v err=%v", got, err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("槽 %d 精确实例漂移: got=%+v want=%+v", i, got[i], want[i])
		}
	}

	// repo 层也拒绝新写 0，不能只依赖 biz 防线。
	if err := repo.SetEquipment(ctx, 7001, []EquipmentSlot{{Slot: 1, ItemConfigID: 10003}}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("repo 缺 instance_id 应拒: %v", err)
	}
	still, err := repo.GetEquipment(ctx, 7001)
	if err != nil || len(still) != 2 {
		t.Fatalf("预校验失败不得清掉旧预设: got=%+v err=%v", still, err)
	}

	// 空数组是全量卸下：只做 DELETE，不写 instance_id=0/NULL 的“空槽行”。
	if err := repo.SetEquipment(ctx, 7001, nil); err != nil {
		t.Fatalf("卸下全部: %v", err)
	}
	empty, err := repo.GetEquipment(ctx, 7001)
	if err != nil || len(empty) != 0 {
		t.Fatalf("卸下后应无持久化空槽行: got=%+v err=%v", empty, err)
	}
}

func TestEquipmentRepoDuplicateInstanceRollsBack_MySQL(t *testing.T) {
	f := openAttributeTestDB(t)
	createEquipmentTestTable(t, f)
	repo := NewMySQLPlayerRepo(f.db)
	ctx := context.Background()

	original := []EquipmentSlot{{Slot: 1, ItemConfigID: 10003, InstanceID: 9001}}
	if err := repo.SetEquipment(ctx, 7001, original); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := repo.SetEquipment(ctx, 7001, []EquipmentSlot{
		{Slot: 1, ItemConfigID: 10003, InstanceID: 9002},
		{Slot: 2, ItemConfigID: 10027, InstanceID: 9002},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("同实例双槽应由唯一键拒绝: %v", err)
	}
	got, gerr := repo.GetEquipment(ctx, 7001)
	if gerr != nil || len(got) != 1 || got[0] != original[0] {
		t.Fatalf("唯一键失败必须回滚 delete+insert 全事务: got=%+v err=%v", got, gerr)
	}
}

func TestValidateEquipmentSchemaRejectsMalformedUniqueIndex_MySQL(t *testing.T) {
	f := openAttributeTestDB(t)
	createEquipmentTestTable(t, f)
	repo := NewMySQLPlayerRepo(f.db)
	ctx := context.Background()
	if err := repo.ValidateEquipmentSchema(ctx); err != nil {
		t.Fatalf("权威 v6 schema 应通过: %v", err)
	}
	if _, err := f.db.ExecContext(ctx,
		`ALTER TABLE player_equipment DROP INDEX uk_player_instance,
		 ADD UNIQUE KEY uk_player_instance (instance_id, player_id)`); err != nil {
		t.Fatalf("制造错序唯一键: %v", err)
	}
	if err := repo.ValidateEquipmentSchema(ctx); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("同名错序唯一键必须 fail-fast: %v", err)
	}
}
