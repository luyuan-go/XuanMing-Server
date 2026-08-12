package data

import (
	"context"
	"testing"
	"time"
)

// TestTerminalReleaseMySQLTwoPhaseCAS 是可选真实 MySQL 集成测。默认跳过；CI/本地用
// PANDORA_TEST_MYSQL_DSN 指向不带 database 的测试实例；夹具创建随机库并重放正式
// battle schema，避免与同包其他 MySQL 测试的 DSN 契约冲突或污染共享库。
func TestTerminalReleaseMySQLTwoPhaseCAS(t *testing.T) {
	db := openBattleRetentionDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo := NewMySQLBattleRepo(db)
	if err := repo.ValidateTerminalReleaseSchema(ctx); err != nil {
		t.Fatalf("exact schema probe: %v", err)
	}

	matchID := uint64(time.Now().UnixNano())
	const insertSQL = `INSERT INTO terminal_release_outbox
(match_id, allocation_id, ds_pod_name, gameserver_uid, instance_epoch,
 auth_gen, auth_jti, auth_exp_ms, auth_kid, auth_token_sha256, auth_writer_epoch,
 authorized_at_ms, release_after_ms, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	res, err := db.ExecContext(ctx, insertSQL,
		matchID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "battle-integration", "uid-integration", 1,
		1, "jti-integration", time.Now().Add(time.Hour).UnixMilli(), "kid-integration",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 2,
		time.Now().UnixMilli(), time.Now().UnixMilli(), time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil || id <= 0 {
		t.Fatalf("insert id=%d err=%v", id, err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM terminal_release_outbox WHERE id=?`, id)
	})

	// Future/regressed caller cannot delete pending proof; SQL precondition makes it a no-op.
	if err := repo.DeleteTerminalReleaseOutbox(ctx, uint64(id)); err != nil {
		t.Fatal(err)
	}
	var releasedAt int64
	if err := db.QueryRowContext(ctx,
		`SELECT released_at_ms FROM terminal_release_outbox WHERE id=?`, id).Scan(&releasedAt); err != nil || releasedAt != 0 {
		t.Fatalf("pending row was deleted/changed: released_at=%d err=%v", releasedAt, err)
	}

	nowMs := time.Now().UnixMilli()
	marked, err := repo.MarkTerminalReleaseReleased(ctx, uint64(id), nowMs)
	if err != nil || !marked {
		t.Fatalf("phase1 mark=%v err=%v", marked, err)
	}
	marked, err = repo.MarkTerminalReleaseReleased(ctx, uint64(id), nowMs+1)
	if err != nil || marked {
		t.Fatalf("duplicate phase1 mark=%v err=%v", marked, err)
	}
	if err := repo.DeleteTerminalReleaseOutbox(ctx, uint64(id)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM terminal_release_outbox WHERE id=?`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("released row not deleted: count=%d err=%v", count, err)
	}
	// ACK response-loss retry is idempotent after the row is already gone.
	if err := repo.DeleteTerminalReleaseOutbox(ctx, uint64(id)); err != nil {
		t.Fatalf("duplicate delete: %v", err)
	}
}
