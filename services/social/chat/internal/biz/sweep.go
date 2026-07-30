// sweep.go — chat 私聊历史保留期清理(2026-07-21,CLAUDE.md §9 不变量 24 落地)。
//
// 背景:chat_private_messages 每条私聊一行,只增不删,随消息量无界线性增长。
// 按雪花 message_id 时间段单调性删 message_id < MinIDAt(cutoff) 的行(主键范围,无需新索引):
// 私聊历史仅供 PullHistory 展示,超保留期(默认 90 天)删除只影响老历史可见性,无幂等/资产语义。
//
// 多副本:各副本独立跑,无锁(对齐 mail sweep)——DELETE 幂等,并发只多花空批。
// 每轮单批 limit 有界,积压跨轮摊平,不长事务锁表。
package biz

import (
	"context"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/snowflake"
)

// SweepHistory 跑一轮私聊历史清理(至多一批 cfg.SweepBatch),由调用方 ticker 驱动。
func (u *ChatUsecase) SweepHistory(ctx context.Context, nowMs int64) {
	cutoffSec := (nowMs - int64(u.cfg.HistoryRetentionDays)*86400_000) / 1000
	maxID := snowflake.MinIDAt(cutoffSec)
	if maxID == 0 {
		return // cutoff 早于雪花 Epoch(服务上线未满保留期),无可清理
	}
	// mode 默认 report_only:只统计待清理量并 WARN 告警,不删数据(dbguard.SweepTable 内已打日志)。
	if out, err := u.repo.SweepMessagesBefore(ctx, u.cfg.RetentionMode(), maxID, u.cfg.SweepBatch); err != nil {
		plog.With(ctx).Warnw("msg", "chat_history_sweep_failed", "err", err)
	} else if out.Cleaned() {
		plog.With(ctx).Infow("msg", "chat_history_swept", "deleted", out.Deleted, "retention_days", u.cfg.HistoryRetentionDays)
	}
}

// RunHistorySweep 周期跑私聊历史保留期清理,直到 ctx 取消。
func (u *ChatUsecase) RunHistorySweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "chat_history_sweep", func() { u.SweepHistory(ctx, time.Now().UnixMilli()) })
		}
	}
}
