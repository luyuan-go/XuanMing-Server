// friend_guard_lock_order_mysql_test.go — 守卫锁序的死锁回归(2026-08-11)。
//
// 背景:`TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB` 在**真 MySQL 上确定性 1213
// 死锁**(3/3 复现),而此前 CI 从不设 `PANDORA_TEST_MYSQL_DSN`,该用例长期只在 TiDB 侧跑
// (TiDB 无 gap 锁 → 天然不复现),于是缺陷被"SKIP 在报告里等于 ok"盖住。
//
// 根因不是守卫本身,而是**守卫取得的时机**:未命中记录的 `SELECT ... FOR UPDATE` 在
// InnoDB RR 下锁的是**键所在的间隙**而不是"某一行",N 个不同 requester 指向同一 target 时
// 全部落进 `uk_requester_target` 的同一个 supremum 间隙。间隙锁彼此相容(都拿得到),
// 排他点在随后的守卫行:谁抢到守卫谁就去 INSERT,而插入意向被其余事务仍持有的间隙锁挡住
// → 环。原代码把 player 守卫放在三条探针之后,正好让间隙锁落在守卫的串行化之外。
//
// 本文件是**针对该锁序的独立回归**:上面那个用例验的是"上限不被穿透"(业务性质),
// 这里验的是"并发路径不产生 1213"(锁序性质)。两者会因不同的改动而红,不能互相替代 ——
// 把守卫挪回探针之后,业务断言仍可能碰巧过(先失败的事务被算成 ErrFriendRequestLimit
// 之外的错误才会暴露),而本文件必红。
package data

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// assertNoDeadlock 把 1213 单独挑出来报:混在 ErrInternal 里只会看到一句
// "意外错误",看不出这是锁序问题。
func assertNoDeadlock(t *testing.T, backend string, err error, what string) {
	t.Helper()
	if err == nil {
		return
	}
	var my *drivermysql.MySQLError
	if errors.As(err, &my) && my.Number == 1213 {
		t.Fatalf("%s %s 触发 InnoDB 死锁(1213):守卫必须先于任何锁定读取得,"+
			"否则未命中探针的间隙锁会落在守卫串行化之外 —— %v", backend, what, err)
	}
	if strings.Contains(err.Error(), "Error 1213") { // 错误被 errcode 包过一层时的兜底
		t.Fatalf("%s %s 触发 InnoDB 死锁(1213,已被 errcode 包装): %v", backend, what, err)
	}
}

// TestFriendCreateRequestGuardBeforeGapLocks —— N 个不同 requester 并发向同一 target 申请。
// 这是死锁的原始复现形状:所有事务共享 friend_requests 的同一个 supremum 间隙,
// 又都要抢同一把 target 守卫。
func TestFriendCreateRequestGuardBeforeGapLocks(t *testing.T) {
	forEachFriendCapacityBackend(t, func(t *testing.T, backend string, db *sql.DB) {
		repo := NewMySQLFriendRepo(db)
		const (
			targetID    = uint64(8_101)
			concurrent  = 16 // 高于业务用例的 8:锁序缺陷随并发度升高才稳定复现
			maxIncoming = 4
		)
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
		)
		start := make(chan struct{})
		for i := 0; i < concurrent; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, _, err := repo.CreateRequest(context.Background(),
					uint64(30_000+i), uint64(3_001+i), targetID, maxIncoming)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(i)
		}
		close(start)
		wg.Wait()

		succeeded := 0
		for _, err := range errs {
			assertNoDeadlock(t, backend, err, "并发 CreateRequest")
			switch errcode.As(err) {
			case 0:
				succeeded++
			case errcode.ErrFriendRequestLimit:
			default:
				t.Fatalf("%s 并发 CreateRequest 非预期错误: %v", backend, err)
			}
		}
		// 顺带守住业务性质:锁序修复不得放松上限。
		if succeeded != maxIncoming {
			t.Fatalf("%s 成功 %d 条, want %d", backend, succeeded, maxIncoming)
		}
	})
}

// TestFriendCreateRequestGuardAcrossDistinctTargets —— 不同 target 的并发申请。
//
// 这一形状下守卫行各不相同(不构成排他点),暴露的是"探针间隙锁本身跨事务共享":
// 空表时所有 (requester,target) 键都落在同一个 supremum 间隙,插入意向互相阻塞。
// 与上一个用例一起,覆盖"共享守卫"和"不共享守卫"两侧。
func TestFriendCreateRequestGuardAcrossDistinctTargets(t *testing.T) {
	forEachFriendCapacityBackend(t, func(t *testing.T, backend string, db *sql.DB) {
		repo := NewMySQLFriendRepo(db)
		const concurrent = 16
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
		)
		start := make(chan struct{})
		for i := 0; i < concurrent; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, _, err := repo.CreateRequest(context.Background(),
					uint64(31_000+i), uint64(4_001+i), uint64(8_200+i), 10)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(i)
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			assertNoDeadlock(t, backend, err, "并发 CreateRequest(不同 target)")
			if errcode.As(err) != 0 {
				t.Fatalf("%s 不同 target 的并发申请不应失败: %v", backend, err)
			}
		}
		var pending int
		if err := db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM friend_requests WHERE status = ?`, requestStatusPending).Scan(&pending); err != nil {
			t.Fatalf("%s 统计 pending: %v", backend, err)
		}
		if pending != concurrent {
			t.Fatalf("%s 落库 pending=%d, want %d", backend, pending, concurrent)
		}
	})
}

// TestFriendBlockGuardBeforeGapLocks —— Block 与 CreateRequest 同构:
// `blocks` 存在性探针(FOR UPDATE)同样排在 player 守卫之前,未命中时同样取间隙锁。
// 同一玩家并发拉黑多个目标 = 共享守卫 + 共享间隙,与原始死锁形状一致。
func TestFriendBlockGuardBeforeGapLocks(t *testing.T) {
	forEachFriendCapacityBackend(t, func(t *testing.T, backend string, db *sql.DB) {
		repo := NewMySQLFriendRepo(db)
		const (
			blockerID  = uint64(8_301)
			concurrent = 16
			maxBlocks  = 6
		)
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
		)
		start := make(chan struct{})
		for i := 0; i < concurrent; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				err := repo.Block(context.Background(), blockerID, uint64(8_400+i), maxBlocks)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(i)
		}
		close(start)
		wg.Wait()

		succeeded := 0
		for _, err := range errs {
			assertNoDeadlock(t, backend, err, "并发 Block")
			switch errcode.As(err) {
			case 0:
				succeeded++
			case errcode.ErrFriendBlockLimit:
			default:
				t.Fatalf("%s 并发 Block 非预期错误: %v", backend, err)
			}
		}
		if succeeded != maxBlocks {
			t.Fatalf("%s 成功拉黑 %d 个, want %d(上限被并发穿透或被误拒)", backend, succeeded, maxBlocks)
		}
	})
}
