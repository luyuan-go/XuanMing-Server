// sweep_test.go — 保留期清理单测:**默认只报告不删**(2026-07-22 用户指令)+
// 入参透传 + 单表失败不阻断其余表。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
)

// sweepRecordingRepo 记录两个清理方法收到的 mode 与入参,可注入错误(嵌入 fakeRepo 补齐其余接口)。
type sweepRecordingRepo struct {
	*fakeRepo
	ledgerMode, escrowMode   dbguard.Mode
	ledgerDays, ledgerLimit  int
	escrowDays, escrowLimit  int
	ledgerErr                error
	ledgerCalls, escrowCalls int
}

func (r *sweepRecordingRepo) SweepLedgerBefore(_ context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	r.ledgerCalls++
	r.ledgerMode, r.ledgerDays, r.ledgerLimit = mode, retentionDays, limit
	if r.ledgerErr != nil {
		return dbguard.Outcome{Mode: mode}, r.ledgerErr
	}
	if mode == dbguard.ModeDelete {
		return dbguard.Outcome{Mode: mode, Matched: 3, Deleted: 3}, nil
	}
	return dbguard.Outcome{Mode: mode, Matched: 3}, nil
}

func (r *sweepRecordingRepo) SweepClosedEscrowBefore(_ context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	r.escrowCalls++
	r.escrowMode, r.escrowDays, r.escrowLimit = mode, retentionDays, limit
	if mode == dbguard.ModeDelete {
		return dbguard.Outcome{Mode: mode, Matched: 2, Deleted: 2}, nil
	}
	return dbguard.Outcome{Mode: mode, Matched: 2}, nil
}

func newSweepUC(repo *sweepRecordingRepo, modeRaw string) *InventoryUsecase {
	return NewInventoryUsecase(repo, conf.InventoryConf{
		LedgerRetentionDays: 90, EscrowRetentionDays: 90, SweepBatch: 500,
		RetentionModeRaw: modeRaw,
	})
}

// TestSweepRetentionDefaultsToReportOnly 是本文件最重要的断言:
// **配置不写 retention_mode 时,清理必须以 report_only 模式跑(一行都不删)**。
// 这条守住"不能因为数据大了就自动删玩家数据"的默认立场——回归它等于回归数据安全。
func TestSweepRetentionDefaultsToReportOnly(t *testing.T) {
	repo := &sweepRecordingRepo{fakeRepo: newFakeRepo()}
	uc := newSweepUC(repo, "") // 配置留空 = 默认

	uc.SweepRetention(context.Background())

	if repo.ledgerMode != dbguard.ModeReportOnly {
		t.Fatalf("ledger 默认必须 report_only, got %v", repo.ledgerMode)
	}
	if repo.escrowMode != dbguard.ModeReportOnly {
		t.Fatalf("escrow 默认必须 report_only, got %v", repo.escrowMode)
	}
}

// TestSweepRetentionDeleteOnlyWhenExplicit 只有显式 "delete" 才切到实删模式。
func TestSweepRetentionDeleteOnlyWhenExplicit(t *testing.T) {
	t.Run("显式 delete 才实删", func(t *testing.T) {
		repo := &sweepRecordingRepo{fakeRepo: newFakeRepo()}
		newSweepUC(repo, "delete").SweepRetention(context.Background())
		if repo.ledgerMode != dbguard.ModeDelete || repo.escrowMode != dbguard.ModeDelete {
			t.Fatalf("显式 delete 应切实删: ledger=%v escrow=%v", repo.ledgerMode, repo.escrowMode)
		}
	})

	t.Run("拼错的值回落 report_only 而不是猜成 delete", func(t *testing.T) {
		for _, raw := range []string{"del", "true", "1", "on", "purge"} {
			repo := &sweepRecordingRepo{fakeRepo: newFakeRepo()}
			newSweepUC(repo, raw).SweepRetention(context.Background())
			if repo.ledgerMode != dbguard.ModeReportOnly {
				t.Fatalf("retention_mode=%q 必须回落 report_only, got %v", raw, repo.ledgerMode)
			}
		}
	})
}

// TestSweepRetentionPassesConfig 保留天数与批量按配置透传到 data 层。
func TestSweepRetentionPassesConfig(t *testing.T) {
	repo := &sweepRecordingRepo{fakeRepo: newFakeRepo()}
	newSweepUC(repo, "delete").SweepRetention(context.Background())

	if repo.ledgerCalls != 1 || repo.ledgerDays != 90 || repo.ledgerLimit != 500 {
		t.Fatalf("ledger sweep 入参错: calls=%d days=%d limit=%d", repo.ledgerCalls, repo.ledgerDays, repo.ledgerLimit)
	}
	if repo.escrowCalls != 1 || repo.escrowDays != 90 || repo.escrowLimit != 500 {
		t.Fatalf("escrow sweep 入参错: calls=%d days=%d limit=%d", repo.escrowCalls, repo.escrowDays, repo.escrowLimit)
	}
}

// TestSweepRetentionContinuesOnError ledger 失败不阻断 escrow(彼此独立,下一轮重试)。
func TestSweepRetentionContinuesOnError(t *testing.T) {
	repo := &sweepRecordingRepo{
		fakeRepo:  newFakeRepo(),
		ledgerErr: errcode.New(errcode.ErrInternal, "mysql down"),
	}
	newSweepUC(repo, "").SweepRetention(context.Background())

	if repo.ledgerCalls != 1 {
		t.Fatalf("ledger sweep 未调用: calls=%d", repo.ledgerCalls)
	}
	if repo.escrowCalls != 1 {
		t.Fatalf("ledger 失败后 escrow sweep 被阻断: calls=%d", repo.escrowCalls)
	}
}
