// allocator_noshow_penalty_test.go —— no-show 记账 → 进入侧退避(anti-abuse §6 第 8 项,
// 2026-08-10 拍板温和档)回归:
//   - 只有 reason=no_show 记账;all_disconnected(打过但掉线)绝不记 —— 网络差不受罚;
//   - 温和档:10min 窗内首次免罚,第 2 次起 30s→60s→120s→… 封顶 5min;
//   - 记账/布罚全程 fail-open,绝不阻断判弃收尾。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

func newNoShowRecorderForTest(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient, *data.RedisNoShowRecorder) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb, data.NewRedisNoShowRecorder(rdb)
}

func noShowPenaltyCfg() (window, base, cap time.Duration) {
	return 10 * time.Minute, 30 * time.Second, 5 * time.Minute
}

// TestNoShowAbandonFirstOffenseIsFree:首次 no-show 判弃只记账不布罚(温和档核心:
// 客户端崩溃 / 网络抖动导致的偶发 no-show 不惩罚玩家)。
func TestNoShowAbandonFirstOffenseIsFree(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(90 * time.Second)
	cfg.NoShowLedgerWindow = config.Duration(10 * time.Minute)
	cfg.NoShowPenaltyBase = config.Duration(30 * time.Second)
	cfg.NoShowPenaltyCap = config.Duration(5 * time.Minute)
	cfg.NoShowPenaltyFree = 1
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)
	_, penRdb, rec := newNoShowRecorderForTest(t)
	uc.SetNoShowRecorder(rec)

	res := allocateReadyNoShow(t, uc, repo, 71, []uint64{110, 120})
	backdateEmptySinceBy(t, repo, 71, 2*time.Minute)
	if hb, err := uc.Heartbeat(ctx, 71, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil || hb.Command != commandStop {
		t.Fatalf("no-show abandon heartbeat = (%+v, %v)", hb, err)
	}
	for _, pid := range []uint64{110, 120} {
		// 记账 1 次。
		if n, err := penRdb.Get(ctx, redisx.RLKey("match", "noshow", pid)).Int64(); err != nil || n != 1 {
			t.Fatalf("player %d ledger = (%d, %v), want 1", pid, n, err)
		}
		// 首次免罚:无退避窗。
		if d, err := redisx.PenaltyRemaining(ctx, penRdb, redisx.RLKey("match", "noshowcd", pid)); err != nil || d != 0 {
			t.Fatalf("player %d first offense penalty = (%v, %v), want none", pid, d, err)
		}
	}
}

// TestSecondNoShowArmsPenalty:窗口内第 2 次 no-show 布 30s 退避(matchmaker 侧执行拒绝)。
func TestSecondNoShowArmsPenalty(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(90 * time.Second)
	cfg.NoShowLedgerWindow = config.Duration(10 * time.Minute)
	cfg.NoShowPenaltyBase = config.Duration(30 * time.Second)
	cfg.NoShowPenaltyCap = config.Duration(5 * time.Minute)
	cfg.NoShowPenaltyFree = 1
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)
	_, penRdb, rec := newNoShowRecorderForTest(t)
	uc.SetNoShowRecorder(rec)

	for i, matchID := range []uint64{72, 73} {
		res := allocateReadyNoShow(t, uc, repo, matchID, []uint64{130})
		backdateEmptySinceBy(t, repo, matchID, 2*time.Minute)
		if hb, err := uc.Heartbeat(ctx, matchID, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil || hb.Command != commandStop {
			t.Fatalf("abandon #%d = (%+v, %v)", i+1, hb, err)
		}
	}
	d, err := redisx.PenaltyRemaining(ctx, penRdb, redisx.RLKey("match", "noshowcd", 130))
	if err != nil || d != 30*time.Second {
		t.Fatalf("second offense penalty = (%v, %v), want 30s", d, err)
	}
}

// TestAllDisconnectedAbandonDoesNotRecord:有人连入过、全员掉线超时的判弃**绝不记账**——
// 这是「打过但网络断了」,惩罚它等于惩罚网络差的正常玩家(拍板红线)。
func TestAllDisconnectedAbandonDoesNotRecord(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(90 * time.Second)
	cfg.NoShowLedgerWindow = config.Duration(10 * time.Minute)
	cfg.NoShowPenaltyBase = config.Duration(30 * time.Second)
	cfg.NoShowPenaltyCap = config.Duration(5 * time.Minute)
	cfg.NoShowPenaltyFree = 1
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)
	_, penRdb, rec := newNoShowRecorderForTest(t)
	uc.SetNoShowRecorder(rec)

	// 有人连入过(EverHadPlayers=true)→ 全员掉线 → 超过长阈值判弃。
	res := allocateReady(t, uc, repo, 74, []uint64{140, 150}, 1, "5v5_ranked")
	if _, err := uc.Heartbeat(ctx, 74, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	backdateEmptySinceBy(t, repo, 74, 6*time.Minute)
	hb, err := uc.Heartbeat(ctx, 74, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil || hb.Command != commandStop {
		t.Fatalf("all_disconnected abandon = (%+v, %v)", hb, err)
	}
	if got, _, _ := repo.GetBattle(ctx, 74); got.State != stateAbandoned {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
	for _, pid := range []uint64{140, 150} {
		if err := penRdb.Get(ctx, redisx.RLKey("match", "noshow", pid)).Err(); err != redis.Nil {
			t.Fatalf("player %d must have no ledger entry, got err=%v", pid, err)
		}
		if d, _ := redisx.PenaltyRemaining(ctx, penRdb, redisx.RLKey("match", "noshowcd", pid)); d != 0 {
			t.Fatalf("player %d must have no penalty, got %v", pid, d)
		}
	}
}

// TestRecordNoShowPenaltiesEscalationAndCap:纯档位逻辑——30s→60s→120s→240s→封顶 300s。
func TestRecordNoShowPenaltiesEscalationAndCap(t *testing.T) {
	ctx := context.Background()
	window, base, penCap := noShowPenaltyCfg()
	cfg := testCfg()
	cfg.NoShowLedgerWindow = config.Duration(window)
	cfg.NoShowPenaltyBase = config.Duration(base)
	cfg.NoShowPenaltyCap = config.Duration(penCap)
	cfg.NoShowPenaltyFree = 1
	u := &AllocatorUsecase{cfg: cfg}
	_, penRdb, rec := newNoShowRecorderForTest(t)
	u.SetNoShowRecorder(rec)

	want := []time.Duration{0, 30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for i, expect := range want {
		u.recordNoShowPenalties(ctx, uint64(9000+i), []uint64{160})
		got, err := redisx.PenaltyRemaining(ctx, penRdb, redisx.RLKey("match", "noshowcd", 160))
		if err != nil || got != expect {
			t.Fatalf("offense #%d penalty = (%v, %v), want %v", i+1, got, err, expect)
		}
	}
}

// TestRecordNoShowPenaltiesFailOpen:记罚 Redis 故障只 Warn,不 panic 不阻断(判弃收尾照走)。
func TestRecordNoShowPenaltiesFailOpen(t *testing.T) {
	cfg := testCfg()
	cfg.NoShowLedgerWindow = config.Duration(10 * time.Minute)
	cfg.NoShowPenaltyBase = config.Duration(30 * time.Second)
	u := &AllocatorUsecase{cfg: cfg}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close()
	u.SetNoShowRecorder(data.NewRedisNoShowRecorder(rdb))

	u.recordNoShowPenalties(context.Background(), 9100, []uint64{170}) // 不得 panic / 阻塞
}
