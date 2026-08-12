package data

import (
	"context"
	"database/sql"
	"strings"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// ── 出战装备预设 / 天赋树 ──────────────────────────────────────────────────────

// SetEquipment 全量替换出战装备预设(事务:删旧 + 按 slot 插新)。
// uk_player_slot 保证槽位唯一，uk_player_instance 保证同一实例不会占玩家的两个槽。
func (r *MySQLPlayerRepo) SetEquipment(ctx context.Context, playerID uint64, slots []EquipmentSlot) error {
	for _, s := range slots {
		if s.InstanceID == 0 {
			return errcode.New(errcode.ErrInvalidArg,
				"instance_id required for equipment write player=%d slot=%d", playerID, s.Slot)
		}
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, derr := tx.ExecContext(ctx, `DELETE FROM player_equipment WHERE player_id = ?`, playerID); derr != nil {
		return errcode.New(errcode.ErrInternal, "clear equipment player=%d: %v", playerID, derr)
	}
	const ins = `INSERT INTO player_equipment (player_id, slot, item_config_id, instance_id) VALUES (?, ?, ?, ?)`
	for _, s := range slots {
		if _, ierr := tx.ExecContext(ctx, ins, playerID, s.Slot, s.ItemConfigID, s.InstanceID); ierr != nil {
			if isDupErr(ierr) {
				return errcode.New(errcode.ErrInvalidArg,
					"duplicate equipment slot or instance player=%d slot=%d instance=%d", playerID, s.Slot, s.InstanceID)
			}
			return errcode.New(errcode.ErrInternal, "insert equipment player=%d slot=%d instance=%d: %v",
				playerID, s.Slot, s.InstanceID, ierr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrInternal, "commit equipment player=%d: %v", playerID, cerr)
	}
	return nil
}

func (r *MySQLPlayerRepo) GetEquipment(ctx context.Context, playerID uint64) ([]EquipmentSlot, error) {
	// instance_id 允许 NULL 仅用于兼容 000006 前旧二进制写下的配置级预设；领域层以 0 表示。
	const q = `SELECT slot, item_config_id, COALESCE(instance_id, 0) FROM player_equipment WHERE player_id = ? ORDER BY slot`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query equipment player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var slots []EquipmentSlot
	for rows.Next() {
		var s EquipmentSlot
		if serr := rows.Scan(&s.Slot, &s.ItemConfigID, &s.InstanceID); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan equipment player=%d: %v", playerID, serr)
		}
		slots = append(slots, s)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate equipment player=%d: %v", playerID, rerr)
	}
	return slots, nil
}

// ValidateEquipmentSchema 在副本 Ready 前确认 000006 已落地且唯一键形态正确。
// 新二进制的读写 SQL 都引用 instance_id；等首个玩家请求才暴露漏迁移会形成部分 Ready。
func (r *MySQLPlayerRepo) ValidateEquipmentSchema(ctx context.Context) error {
	var dataType, columnType, nullable string
	if err := r.db.QueryRowContext(ctx,
		`SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' AND COLUMN_NAME = 'instance_id'`).
		Scan(&dataType, &columnType, &nullable); err != nil {
		return errcode.New(errcode.ErrInternal, "player equipment instance_id schema invalid: %v", err)
	}
	if !strings.EqualFold(dataType, "bigint") || !strings.Contains(strings.ToLower(columnType), "unsigned") || nullable != "YES" {
		return errcode.New(errcode.ErrInternal,
			"player equipment instance_id malformed data_type=%s column_type=%s nullable=%s",
			dataType, columnType, nullable)
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT SEQ_IN_INDEX, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment'
		   AND INDEX_NAME = 'uk_player_instance' AND NON_UNIQUE = 0
		 ORDER BY SEQ_IN_INDEX`)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "probe player equipment instance unique index: %v", err)
	}
	defer func() { _ = rows.Close() }()
	want := []string{"player_id", "instance_id"}
	index := 0
	for rows.Next() {
		var (
			seq     int
			name    string
			subPart sql.NullInt64
		)
		if err := rows.Scan(&seq, &name, &subPart); err != nil {
			return errcode.New(errcode.ErrInternal, "scan player equipment instance unique index: %v", err)
		}
		if index >= len(want) || seq != index+1 || name != want[index] || subPart.Valid {
			return errcode.New(errcode.ErrInternal,
				"player equipment uk_player_instance malformed at index=%d seq=%d column=%s prefix=%v",
				index, seq, name, subPart.Valid)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return errcode.New(errcode.ErrInternal, "iterate player equipment instance unique index: %v", err)
	}
	if index != len(want) {
		return errcode.New(errcode.ErrInternal,
			"player equipment uk_player_instance missing columns=%d want=%d", index, len(want))
	}
	return nil
}
