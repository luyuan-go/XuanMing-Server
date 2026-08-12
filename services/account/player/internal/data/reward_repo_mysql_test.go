// reward_repo_mysql_test.go — 领奖记录乐观锁的真实 MySQL 集成回归(2026-08-11 补)。
//
// SaveRewardClaims 的全部安全性来自一条 `UPDATE ... WHERE player_id=? AND version=?`
// 的**受影响行数**,而受影响行数正是 sqlmock 里最容易被"配置成想要的值"的东西。
// 真库能验而 fake 验不了的三件事:
//   - expectVersion=0 的 INSERT 撞主键时必须映射成 ErrPlayerVersionMismatch(而不是 ErrInternal),
//     否则 biz 的"冲突就重读重试"分支永远走不到,首次领奖并发下会直接把错误抛给玩家;
//   - 陈旧 version 的 UPDATE 必须匹配 0 行 —— 依赖驱动默认的 affected-rows 语义
//     (若 DSN 打开 clientFoundRows,匹配但未改动也会返回 >0,乐观锁当场失效);
//   - version 必须每次真的 +1,否则两个写者能各自"成功"地覆盖对方的 record bytes
//     (领奖位图是全量快照,后写覆盖先写 = 已领奖记录凭空消失,可重复领)。
package data

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const rewardClaimsTestTimeout = 20 * time.Second

// TestRewardClaimsOptimisticLock_MySQL —— 建行 → 递增 → 陈旧版本被拒的完整往返。
func TestRewardClaimsOptimisticLock_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), rewardClaimsTestTimeout)
	defer cancel()

	const playerID = uint64(72_001)
	seedPlayerProfile(t, repo, playerID, 1500)

	// 未建行:读到 (nil, 0),这是 biz 判"首次领奖"的依据。
	record, version, err := repo.LoadRewardClaims(ctx, playerID)
	if err != nil {
		t.Fatalf("首次读取: %v", err)
	}
	if record != nil || version != 0 {
		t.Fatalf("未建行读到 record=%v version=%d, want nil/0", record, version)
	}

	if err := repo.SaveRewardClaims(ctx, playerID, []byte("claim-v1"), 0); err != nil {
		t.Fatalf("首次写入: %v", err)
	}
	record, version, err = repo.LoadRewardClaims(ctx, playerID)
	if err != nil {
		t.Fatalf("回读 v1: %v", err)
	}
	if string(record) != "claim-v1" || version != 1 {
		t.Fatalf("回读 record=%q version=%d, want claim-v1/1", record, version)
	}

	if err := repo.SaveRewardClaims(ctx, playerID, []byte("claim-v2"), 1); err != nil {
		t.Fatalf("按 v1 更新: %v", err)
	}
	record, version, err = repo.LoadRewardClaims(ctx, playerID)
	if err != nil {
		t.Fatalf("回读 v2: %v", err)
	}
	if string(record) != "claim-v2" || version != 2 {
		t.Fatalf("回读 record=%q version=%d, want claim-v2/2", record, version)
	}

	// 陈旧版本:必须拒绝,且**一个字节都不能改**。
	err = repo.SaveRewardClaims(ctx, playerID, []byte("claim-stale"), 1)
	if errcode.As(err) != errcode.ErrPlayerVersionMismatch {
		t.Fatalf("陈旧版本写入 err=%v, want ErrPlayerVersionMismatch", err)
	}
	record, version, err = repo.LoadRewardClaims(ctx, playerID)
	if err != nil {
		t.Fatalf("拒绝后回读: %v", err)
	}
	if string(record) != "claim-v2" || version != 2 {
		t.Fatalf("拒绝后 record=%q version=%d, want claim-v2/2(拒绝路径写了库)", record, version)
	}
}

// TestRewardClaimsDuplicateInsertMapsToVersionMismatch_MySQL —— expectVersion=0 撞主键
// 必须映射成版本冲突而不是内部错误:并发首次领奖时,biz 靠这个错误码决定"重读后重试"。
func TestRewardClaimsDuplicateInsertMapsToVersionMismatch_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), rewardClaimsTestTimeout)
	defer cancel()

	const playerID = uint64(72_002)
	seedPlayerProfile(t, repo, playerID, 1500)

	if err := repo.SaveRewardClaims(ctx, playerID, []byte("first"), 0); err != nil {
		t.Fatalf("首次写入: %v", err)
	}
	err := repo.SaveRewardClaims(ctx, playerID, []byte("second"), 0)
	if errcode.As(err) != errcode.ErrPlayerVersionMismatch {
		t.Fatalf("重复建行 err=%v, want ErrPlayerVersionMismatch", err)
	}
	record, version, lerr := repo.LoadRewardClaims(ctx, playerID)
	if lerr != nil {
		t.Fatalf("回读: %v", lerr)
	}
	if string(record) != "first" || version != 1 {
		t.Fatalf("重复建行后 record=%q version=%d, want first/1", record, version)
	}
}

// TestRewardClaimsConcurrentSaveOnlyOneWins_MySQL —— N 个写者读到同一 version 后同时写,
// 只允许一个成功;其余必须拿到版本冲突,而不是"都成功"地互相覆盖领奖位图。
func TestRewardClaimsConcurrentSaveOnlyOneWins_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), rewardClaimsTestTimeout)
	defer cancel()

	const (
		playerID   = uint64(72_003)
		concurrent = 8
	)
	seedPlayerProfile(t, repo, playerID, 1500)
	if err := repo.SaveRewardClaims(ctx, playerID, []byte("base"), 0); err != nil {
		t.Fatalf("建行: %v", err)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		won      int
		otherErr error
	)
	start := make(chan struct{})
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// 所有写者都基于 version=1(它们都在同一时刻读到了 base)。
			err := repo.SaveRewardClaims(ctx, playerID, []byte{byte('a' + i)}, 1)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errcode.As(err) == errcode.ErrPlayerVersionMismatch:
				// 预期的拒绝
			default:
				otherErr = err
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if otherErr != nil {
		t.Fatalf("并发写入出现非预期错误: %v", otherErr)
	}
	if won != 1 {
		t.Fatalf("并发写入成功 %d 次, want 1(乐观锁被穿透,领奖位图会互相覆盖)", won)
	}
	_, version, err := repo.LoadRewardClaims(ctx, playerID)
	if err != nil {
		t.Fatalf("回读: %v", err)
	}
	if version != 2 {
		t.Fatalf("并发后 version=%d, want 2(每次成功写必须且只能 +1)", version)
	}
}
