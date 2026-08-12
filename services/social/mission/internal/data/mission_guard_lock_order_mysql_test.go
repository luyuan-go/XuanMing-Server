// mission_guard_lock_order_mysql_test.go — 任务域写事务的 1213 死锁排查回归(2026-08-11)。
//
// 来由:friend 域在真 MySQL 上被抓到确定性死锁,而 mission 的 `acquirePlayerGuard` 与
// friend 是**同一种写法**(`INSERT ... ON DUPLICATE KEY UPDATE` 守卫行)。逐字读代码得到的
// 结论是"mission 不受影响"——因为它的守卫**恒为事务第一把锁**(mission_repo.go 注释原话),
// 探针的间隙锁不会落在守卫之外。但 friend 的教训恰恰是**读代码得出的锁序结论是错的**
// (原注释断言"行锁只属于本 pair",真实是间隙锁跨 pair 共享),所以这里用真库把结论钉死,
// 而不是再信一次推理。
//
// 两个用例分别对应 friend 暴露出的两种形状:
//
//	① 同一玩家高并发 —— 共享守卫行(既有 TestMissionPlayerGuard_* 已覆盖 12 并发,这里提到 24);
//	② **不同玩家**并发 —— 不共享任何守卫行,考的是 `loadState(forUpdate=true)` 在零行时
//	   对 player_mission_active / player_mission_done 取的间隙锁会不会与随后的 INSERT
//	   形成 insert-intention 环。friend 的 `TestFriendCreateRequestGuardAcrossDistinctTargets`
//	   就是死在这一形状上,且**重排守卫顺序解决不了**(没有共享守卫可排)。
package data

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// assertNoMissionDeadlock 把 1213 单独挑出来报,避免混进"非预期错误"里看不出性质。
func assertNoMissionDeadlock(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "Error 1213") || strings.Contains(err.Error(), "Deadlock found") {
		t.Fatalf("%s 触发 InnoDB 死锁(1213):%v", what, err)
	}
}

// newLockOrderCatalog 造 n 条互不互斥的任务(sub_type=0 = 不参与类型互斥),
// 把"活跃数上限"与"类型互斥"两条业务性质排除在外,只留锁行为。
func newLockOrderCatalog(n int) guardTestCatalog {
	catalog := guardTestCatalog{
		missions:   map[uint32]*configpb.MissionRow{},
		conditions: map[uint32]*configpb.ConditionRow{1: {Id: 1, ConditionCategory: 1, TargetCount: 1}},
	}
	for i := 1; i <= n; i++ {
		catalog.missions[uint32(i)] = &configpb.MissionRow{
			Id: uint32(i), MissionType: 10, MissionSubType: 0, ConditionIds: "1",
		}
	}
	return catalog
}

// TestMissionGuardNoDeadlockSamePlayer —— 同一玩家 24 并发接取:共享守卫行的形状。
func TestMissionGuardNoDeadlockSamePlayer(t *testing.T) {
	const (
		playerID  = uint64(9101)
		attempts  = 24
		maxActive = 6
	)
	forEachMissionBackend(t, func(t *testing.T, db *sql.DB) {
		uc := newGuardUsecase(db, newLockOrderCatalog(attempts), maxActive)
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			errs []error
		)
		start := make(chan struct{})
		for i := 1; i <= attempts; i++ {
			wg.Add(1)
			go func(missionID uint32) {
				defer wg.Done()
				<-start
				_, err := uc.Accept(context.Background(), playerID, missionID)
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(uint32(i))
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			assertNoMissionDeadlock(t, err, "同玩家并发接取")
		}
		if got := countActive(t, db, playerID); got != maxActive {
			t.Fatalf("落库活跃任务 %d 行, want %d", got, maxActive)
		}
	})
}

// TestMissionGuardNoDeadlockDistinctPlayers —— **不同玩家**并发接取。
//
// 这一形状没有任何共享的守卫行,所以它验的不是锁序而是间隙锁:表为空时所有 player_id
// 都落在 player_mission_active 主键的同一个 supremum 间隙里,若 `loadState(forUpdate=true)`
// 的零行 FOR UPDATE 取了间隙锁,随后的 INSERT 就会互相挡成环(friend 域正是这样炸的)。
func TestMissionGuardNoDeadlockDistinctPlayers(t *testing.T) {
	const (
		concurrent = 24
		maxActive  = 6
	)
	forEachMissionBackend(t, func(t *testing.T, db *sql.DB) {
		uc := newGuardUsecase(db, newLockOrderCatalog(4), maxActive)
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
				// 每个 goroutine 一个独立玩家:守卫行互不相同,只剩间隙锁这一个共享面。
				_, err := uc.Accept(context.Background(), uint64(9_200+i), uint32(1+i%4))
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}(i)
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			assertNoMissionDeadlock(t, err, "跨玩家并发接取")
			if err != nil {
				t.Fatalf("跨玩家并发接取不应失败(各自独立,无上限冲突): %v", err)
			}
		}
		for i := 0; i < concurrent; i++ {
			if got := countActive(t, db, uint64(9_200+i)); got != 1 {
				t.Fatalf("玩家 %d 落库活跃任务 %d 行, want 1", 9_200+i, got)
			}
		}
	})
}
