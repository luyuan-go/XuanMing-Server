// mission_repo_mysql_test.go — 任务域玩家级写守卫的**真 MySQL / 真 TiDB** 双后端回归。
//
// 为什么必须跑真库、且必须两个后端都跑:被验证的性质是引擎的加锁语义,fake 无法替代。
// TiDB 悲观事务**没有 gap/next-key 锁**,`SELECT ... FROM player_mission_active
// WHERE player_id=? FOR UPDATE` 在该玩家零活跃行时一把锁都不加 —— 两个并发
// AcceptMission 各自读到空活跃集,双双通过 max_active_missions 上限(§9.18)与
// (type,sub_type) 类型互斥校验,然后各插一行。InnoDB 有间隙锁,同样的代码在 MySQL 上
// 恰好无症状,所以只跑 MySQL 等于没测。
//
// 修法 = 临界区入口先取 mission_player_guards 守卫行点锁(存在行的点锁两库语义一致)。
// 本文件两个用例就是该缺陷的针对性回归:**修复前 TiDB 必炸(活跃数超上限 / 互斥双活),
// 修复后两端全绿**。
//
// 门控(沿用仓库 friend/guild/login 双后端约定):DSN **必须不带库名**,测试自建随机
// 临时库并在结束时删掉;对应变量未设置时该后端明确 Skip,已设置但不可达则硬失败。
//
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:13306)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	$env:PANDORA_TEST_TIDB_DSN='root:@tcp(127.0.0.1:4000)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	go test ./services/social/mission/internal/data/ -count=1 -run MissionPlayerGuard -v
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	klog "github.com/go-kratos/kratos/v2/log"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
)

var missionTestDBPattern = regexp.MustCompile(`^pandora_mission_it_[0-9]+_[0-9a-f]{12}$`)

type missionBackend struct {
	name string
	env  string
}

// forEachMissionBackend 对 MySQL / TiDB 各跑一遍同一组断言。
// 两个 DSN 都没设置时逐后端 Skip(而不是静默全绿)。
func forEachMissionBackend(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	t.Helper()
	for _, backend := range [...]missionBackend{
		{name: "mysql", env: "PANDORA_TEST_MYSQL_DSN"},
		{name: "tidb", env: "PANDORA_TEST_TIDB_DSN"},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			dsn := strings.TrimSpace(os.Getenv(backend.env))
			if dsn == "" {
				t.Skipf("跳过 %s 任务域集成测试:未设置 %s", backend.name, backend.env)
			}
			fn(t, newMissionTestDB(t, backend.name, dsn))
		})
	}
}

// newMissionTestDB 建随机临时库并重放 deploy/mysql-init/16-mission-tables.sql
// (直接重放发布用 schema,杜绝"测试里另抄一份 DDL"的漂移)。
func newMissionTestDB(t *testing.T, backend, dsn string) *sql.DB {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析 %s 测试 DSN: %v", backend, err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("%s 测试 DSN 必须不带库名(拿到 %q):测试要自建临时库,不能碰业务库", backend, cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 %s 管理连接: %v", backend, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设置 %s 测试 DSN 但后端不可达: %v", backend, err)
	}

	seed := make([]byte, 6)
	if _, rerr := rand.Read(seed); rerr != nil {
		t.Fatalf("生成随机测试库后缀: %v", rerr)
	}
	dbName := fmt.Sprintf("pandora_mission_it_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(seed))
	if !missionTestDBPattern.MatchString(dbName) { // 二次校验,永不误删业务库
		t.Fatalf("随机测试库名未通过安全校验: %q", dbName)
	}
	if _, err := admin.ExecContext(ctx,
		"CREATE DATABASE `"+dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("创建随机测试库 %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		if !missionTestDBPattern.MatchString(dbName) {
			return
		}
		if _, err := admin.ExecContext(dctx, "DROP DATABASE IF EXISTS `"+dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", dbName, err)
		}
		_ = admin.Close()
	})
	if _, err := admin.ExecContext(ctx, readMissionSchema(t, dbName)); err != nil {
		t.Fatalf("初始化 16-mission-tables.sql: %v", err)
	}

	testCfg := cfg.Clone()
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机测试库: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接随机测试库: %v", err)
	}
	return db
}

func readMissionSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位测试文件")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..",
		"deploy", "mysql-init", "16-mission-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_mission`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("16-mission-tables.sql USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

// ── 最小 Catalog 假件(只需接取校验用得上的字段)─────────────────────────────

type guardTestCatalog struct {
	missions   map[uint32]*configpb.MissionRow
	conditions map[uint32]*configpb.ConditionRow
}

func (c guardTestCatalog) MissionByID(id uint32) (*configpb.MissionRow, bool) {
	row, ok := c.missions[id]
	return row, ok
}

func (c guardTestCatalog) ConditionByID(id uint32) (*configpb.ConditionRow, bool) {
	row, ok := c.conditions[id]
	return row, ok
}

func (c guardTestCatalog) RewardByID(uint32) (*configpb.RewardRow, bool) { return nil, false }
func (c guardTestCatalog) IsEquipment(uint32) bool                       { return false }

// newGuardUsecase 用真实 repo + 真实引擎装配 usecase(测的是生产路径,不是测试专用分支)。
func newGuardUsecase(db *sql.DB, catalog biz.Catalog, maxActive int) *biz.MissionUsecase {
	cfg := conf.MissionConf{MaxActiveMissions: maxActive, MaxFactsPerReport: 64}
	return biz.NewMissionUsecase(NewMySQLMissionRepo(db), biz.StaticCatalogSource{Catalog: catalog},
		nil, nil, nil, nil, cfg, klog.NewStdLogger(io.Discard))
}

func countActive(t *testing.T, db *sql.DB, playerID uint64) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM player_mission_active WHERE player_id = ?", playerID).Scan(&n); err != nil {
		t.Fatalf("统计活跃任务: %v", err)
	}
	return n
}

// TestMissionPlayerGuard_ConcurrentAcceptRespectsActiveLimit —— 并发接取不得突破
// max_active_missions(§9.18 写入侧上限)。
//
// 修复前在 TiDB 上必炸:零活跃行时 `FOR UPDATE` 不加任何锁,N 个并发事务都读到
// len(Active)=0 < 上限,全部通过校验各插一行,最终活跃数 = N 而不是上限。
func TestMissionPlayerGuard_ConcurrentAcceptRespectsActiveLimit(t *testing.T) {
	const (
		playerID  = uint64(9001)
		maxActive = 3
		attempts  = 12
	)
	forEachMissionBackend(t, func(t *testing.T, db *sql.DB) {
		catalog := guardTestCatalog{
			missions:   map[uint32]*configpb.MissionRow{},
			conditions: map[uint32]*configpb.ConditionRow{1: {Id: 1, ConditionCategory: 1, TargetCount: 1}},
		}
		for i := 1; i <= attempts; i++ {
			// sub_type=0 = 不参与类型互斥,单独隔离出"活跃数上限"这一条性质。
			catalog.missions[uint32(i)] = &configpb.MissionRow{
				Id: uint32(i), MissionType: 10, MissionSubType: 0, ConditionIds: "1",
			}
		}
		uc := newGuardUsecase(db, catalog, maxActive)

		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			okCount  int
			otherErr error
		)
		start := make(chan struct{})
		for i := 1; i <= attempts; i++ {
			wg.Add(1)
			go func(missionID uint32) {
				defer wg.Done()
				<-start // 尽量把 attempts 个事务挤进同一时刻
				_, err := uc.Accept(context.Background(), playerID, missionID)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					okCount++
				case errcode.As(err) == errcode.ErrMissionActiveLimit:
					// 预期的拒绝
				default:
					otherErr = err
				}
			}(uint32(i))
		}
		close(start)
		wg.Wait()

		if otherErr != nil {
			t.Fatalf("并发接取出现非预期错误: %v", otherErr)
		}
		if okCount != maxActive {
			t.Fatalf("成功接取 %d 条, want %d(守卫行未串行化,上限被并发穿透)", okCount, maxActive)
		}
		if got := countActive(t, db, playerID); got != maxActive {
			t.Fatalf("落库活跃任务 %d 行, want %d(§9.18 写入侧上限被突破)", got, maxActive)
		}
	})
}

// TestMissionPlayerGuard_ConcurrentAcceptRespectsTypeExclusivity —— 同 (type,sub_type)
// 至多一个活跃任务(D 版 typeFilter 语义)。
//
// 修复前在 TiDB 上必炸:互斥判定是"扫当前活跃行现算",零行时无锁 → 两个并发接取
// 都判定为不冲突,同一 (类型,子类型) 双双活跃。
func TestMissionPlayerGuard_ConcurrentAcceptRespectsTypeExclusivity(t *testing.T) {
	const (
		playerID = uint64(9002)
		attempts = 8
	)
	forEachMissionBackend(t, func(t *testing.T, db *sql.DB) {
		catalog := guardTestCatalog{
			missions:   map[uint32]*configpb.MissionRow{},
			conditions: map[uint32]*configpb.ConditionRow{1: {Id: 1, ConditionCategory: 1, TargetCount: 1}},
		}
		for i := 1; i <= attempts; i++ {
			// 全部同 (类型=7, 子类型=3):互斥键相同,至多一个能活跃。
			catalog.missions[uint32(i)] = &configpb.MissionRow{
				Id: uint32(i), MissionType: 7, MissionSubType: 3, ConditionIds: "1",
			}
		}
		// 上限设得足够宽,确保拦住第二条的只可能是类型互斥而不是活跃数上限。
		uc := newGuardUsecase(db, catalog, 50)

		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			okCount  int
			otherErr error
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
				switch {
				case err == nil:
					okCount++
				case errcode.As(err) == errcode.ErrMissionTypeConflict:
					// 预期的拒绝
				default:
					otherErr = err
				}
			}(uint32(i))
		}
		close(start)
		wg.Wait()

		if otherErr != nil {
			t.Fatalf("并发接取出现非预期错误: %v", otherErr)
		}
		if okCount != 1 {
			t.Fatalf("成功接取 %d 条, want 1(类型互斥被并发穿透)", okCount)
		}
		if got := countActive(t, db, playerID); got != 1 {
			t.Fatalf("落库活跃任务 %d 行, want 1(同 (type,sub_type) 双活)", got)
		}
	})
}

// TestDoneReadLimitTruncatesReadPathButNotTransaction 是「完成集读取侧上限」这条设计
// 判断本身的回归判据(§9.18)。
//
// 守的不是"有没有 LIMIT",而是**LIMIT 只能加在只读路径**:
//   - LoadPlayer(只读,喂 ListMissions)必须截断 —— 否则任务表涨到几千行时,一次
//     ListMissions 返回全部完成任务,且 push.resync 风暴按在线人数放大;
//   - MutatePlayer / ApplyFactsTx(FOR UPDATE 事务)**绝不能**截断 —— 那里用完成集判
//     「已完成不可重复接取」与领奖 CAS,截断会让超出窗口的已完成任务被判成可重新接取,
//     把一个展示问题升级成**重复发奖**。
//
// 把 loadState 里的 doneLimit 改成两条路径共用(即事务路径也截断),本用例必红。
func TestDoneReadLimitTruncatesReadPathButNotTransaction(t *testing.T) {
	forEachMissionBackend(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		const playerID uint64 = 7001
		const totalDone = 6
		const readLimit = 2 // 远小于 totalDone,截断效果肉眼可辨

		for i := 0; i < totalDone; i++ {
			if _, err := db.ExecContext(ctx,
				`INSERT INTO player_mission_done (player_id, mission_config_id, reward_state, completed_at_ms)
				 VALUES (?, ?, 0, 1)`, playerID, 100+i); err != nil {
				t.Fatalf("插入完成行 %d: %v", i, err)
			}
		}

		repo := NewMySQLMissionRepo(db)
		repo.doneReadLimitOverride = readLimit

		// ① 只读路径:必须被截断到 readLimit。
		st, err := repo.LoadPlayer(ctx, playerID)
		if err != nil {
			t.Fatalf("LoadPlayer: %v", err)
		}
		if len(st.Done) != readLimit {
			t.Fatalf("只读路径应截断到 %d 行,实为 %d —— 读取侧上限没生效", readLimit, len(st.Done))
		}
		// 截断必须按 mission_config_id 稳定序,与 biz.sortedDone 同序(否则每次返回的
		// 子集不同,客户端看到的完成列表会来回跳)。
		if _, ok := st.Done[100]; !ok {
			t.Fatalf("截断应保留最小的 mission_config_id,实际拿到 %v", st.Done)
		}

		// ② 事务路径:必须拿到**全部** totalDone 行。
		var seenInTx int
		if err := repo.MutatePlayer(ctx, playerID, func(s *biz.PlayerState) (*biz.Mutation, error) {
			seenInTx = len(s.Done)
			return &biz.Mutation{}, nil
		}); err != nil {
			t.Fatalf("MutatePlayer: %v", err)
		}
		if seenInTx != totalDone {
			t.Fatalf("事务路径必须看到全部 %d 行完成任务,实为 %d —— "+
				"截断会让已完成任务被判成可重新接取,导致重复发奖", totalDone, seenInTx)
		}
	})
}
