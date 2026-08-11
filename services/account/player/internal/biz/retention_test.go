// retention_test.go — 保留期 janitor 的循环语义(§9.24)。
//
// 直接测 drainRetention 而不是起 janitor 循环:janitor 的外层只是 ticker + panic 兜底,
// 真正会出错的是"什么时候继续下一批、什么时候停"。两个方向都出过事:
//   - delete 模式只删单批 → 追不平生产流入,积压只增不减(battle_result 审计 P1 的同款);
//   - report_only 模式还去循环 → 积压永远追不平,每轮固定跑满,把同一条 COUNT 重复执行。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
)

// scriptedSweep 按预设脚本逐次返回结果,并记录被调用了几次、每次拿到什么模式。
type scriptedSweep struct {
	outcomes []dbguard.Outcome
	err      error
	errAt    int // 第几次调用返回 err(1 基;0 = 不报错)
	calls    int
	modes    []dbguard.Mode
	limits   []int
}

func (s *scriptedSweep) fn(_ context.Context, mode dbguard.Mode, _ time.Time, limit int) (dbguard.Outcome, error) {
	s.calls++
	s.modes = append(s.modes, mode)
	s.limits = append(s.limits, limit)
	if s.errAt == s.calls {
		return dbguard.Outcome{Mode: mode}, s.err
	}
	if s.calls <= len(s.outcomes) {
		return s.outcomes[s.calls-1], nil
	}
	return dbguard.Outcome{Mode: mode}, nil
}

func TestDrainRetentionReportOnlyRunsExactlyOnce(t *testing.T) {
	// 即便结果标着 Truncated(delete 模式下的"还有积压"信号),report_only 也不能循环:
	// 那一轮的 COUNT 已经给出全量待清理规模,再转就是空转。
	script := &scriptedSweep{outcomes: []dbguard.Outcome{
		{Mode: dbguard.ModeReportOnly, Matched: 12_345, Truncated: true},
	}}
	uc := &PlayerUsecase{}
	uc.drainRetention(context.Background(), dbguard.ModeReportOnly, time.Now(),
		retentionSweep{table: "skill_card_grants", sweep: script.fn})

	if script.calls != 1 {
		t.Fatalf("report_only 调用了 %d 次,期望恰好 1 次(多余的都是空转)", script.calls)
	}
	if script.modes[0] != dbguard.ModeReportOnly {
		t.Fatalf("传给 repo 的模式=%v, 期望 report_only", script.modes[0])
	}
}

func TestRetentionJanitorRegistrations(t *testing.T) {
	uc := NewPlayerUsecase(newFakeRepo(), conf.PlayerConf{})
	assertTables := func(name string, sweeps []retentionSweep, want []string) {
		t.Helper()
		if len(sweeps) != len(want) {
			t.Fatalf("%s 登记数=%d, 期望 %d: %+v", name, len(sweeps), len(want), sweeps)
		}
		for i, table := range want {
			if sweeps[i].table != table {
				t.Errorf("%s 第 %d 项=%q, 期望 %q", name, i, sweeps[i].table, table)
			}
			if sweeps[i].sweep == nil {
				t.Errorf("%s 表 %q 未绑定 repo Sweep 方法", name, table)
			}
		}
	}

	assertTables("exp", uc.expHistoryRetentionSweeps(), []string{"exp_history"})
	assertTables("history", uc.historyRetentionSweeps(), []string{
		"mmr_history",
		"attr_point_grants",
		"talent_point_grants",
		"skill_card_grants",
	})
}

func TestDrainRetentionDeleteDrainsBacklog(t *testing.T) {
	// delete 模式必须一直删到短批为止,否则每轮只删 batch 行,追不平流入。
	script := &scriptedSweep{outcomes: []dbguard.Outcome{
		{Mode: dbguard.ModeDelete, Deleted: retentionSweepBatch, Truncated: true},
		{Mode: dbguard.ModeDelete, Deleted: retentionSweepBatch, Truncated: true},
		{Mode: dbguard.ModeDelete, Deleted: 7, Truncated: false},
	}}
	uc := &PlayerUsecase{}
	uc.drainRetention(context.Background(), dbguard.ModeDelete, time.Now(),
		retentionSweep{table: "mmr_history", sweep: script.fn})

	if script.calls != 3 {
		t.Fatalf("delete 调用了 %d 次,期望 3 次(满批续跑,短批收工)", script.calls)
	}
	for i, limit := range script.limits {
		if limit != retentionSweepBatch {
			t.Errorf("第 %d 批 limit=%d, 期望 %d(小批量防长事务锁表)", i+1, limit, retentionSweepBatch)
		}
	}
}

func TestDrainRetentionStopsOnError(t *testing.T) {
	// 失败立即收手:下一轮 ticker 自然重试,不在同一轮里对着坏依赖打满循环。
	script := &scriptedSweep{
		outcomes: []dbguard.Outcome{{Mode: dbguard.ModeDelete, Deleted: retentionSweepBatch, Truncated: true}},
		err:      errors.New("mysql gone"),
		errAt:    2,
	}
	uc := &PlayerUsecase{}
	uc.drainRetention(context.Background(), dbguard.ModeDelete, time.Now(),
		retentionSweep{table: "attr_point_grants", sweep: script.fn})

	if script.calls != 2 {
		t.Fatalf("出错后调用了 %d 次,期望 2 次(第二次报错即停)", script.calls)
	}
}

func TestDrainRetentionRespectsCanceledContext(t *testing.T) {
	// ctx 已取消时一次都不该发起(优雅退出期间不能再往库上压批删)。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	script := &scriptedSweep{}
	uc := &PlayerUsecase{}
	uc.drainRetention(ctx, dbguard.ModeDelete, time.Now(),
		retentionSweep{table: "talent_point_grants", sweep: script.fn})

	if script.calls != 0 {
		t.Fatalf("ctx 已取消却调用了 %d 次", script.calls)
	}
}
