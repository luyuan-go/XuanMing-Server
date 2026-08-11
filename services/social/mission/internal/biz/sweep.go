// sweep.go — 保留期清理(CLAUDE.md §9.24;默认 report_only 只报告不删)。
//
//	mission_reward_log:GRANTED 且超期(默认 90 天)可清;PENDING/FAILED 是补发
//	  工作集永不清(陈年 PENDING = 发放链 bug 证据,靠补扫日志揭示)。
//	mission_fact_receipts:受双闸——retention_mode 之外还有 receipt_cleanup_enabled
//	  组级开关(默认关):上游 battle_progress_outbox 重试无总期限,删收据后迟到重放
//	  会把同批事实双计进进度;上游有界前不得开启(同 player exp_history 之因)。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/safego"
)

// RunRetentionSweep 周期保留期清理(多副本各自跑,DELETE 幂等无需锁;
// 单表失败只记日志继续下一表,单轮 panic 只丢本轮)。
func (uc *MissionUsecase) RunRetentionSweep(ctx context.Context) {
	ticker := time.NewTicker(uc.cfg.SweepInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "mission_retention_sweep", func() { uc.sweepOnce(ctx) })
		}
	}
}

func (uc *MissionUsecase) sweepOnce(ctx context.Context) {
	mode := uc.cfg.RetentionModeRaw
	if err := uc.repo.SweepRewardLog(ctx, mode, uc.cfg.RewardLogRetentionDays, uc.cfg.SweepBatch); err != nil {
		uc.log.Errorw("msg", "mission_sweep_reward_log_failed", "err", err)
	}
	if !uc.cfg.ReceiptCleanupEnabled {
		return // 组级闸默认关:收据连报告都不跑,防误读"待清理量"当成可清
	}
	if err := uc.repo.SweepReceipts(ctx, mode, uc.cfg.ReceiptRetentionDays, uc.cfg.SweepBatch); err != nil {
		uc.log.Errorw("msg", "mission_sweep_receipts_failed", "err", err)
	}
}
