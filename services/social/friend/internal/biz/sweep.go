// sweep.go — friend 终态请求保留期清理(2026-07-21,CLAUDE.md §9 不变量 24 落地)。
//
// 背景:friend_requests 每对 (requester,target) 至多一行(uk 复用),但 accepted/rejected/
// expired 终态行随社交图对数累积。终态行无资产语义(好友关系权威在 friendships),
// 超保留期(默认 90 天)删除后再次发起 = 全新 INSERT pending,行为等价。pending 永不清。
//
// 多副本:各副本独立跑,无锁(对齐 mail sweep)——DELETE 幂等,并发只多花空批。
package biz

import (
	"context"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
)

// SweepTerminalRequests 跑一轮保留期清理(各至多一批 cfg.SweepBatch),由调用方 ticker 驱动:
//   - 终态好友请求(accepted/rejected/expired,超 RequestRetentionDays);
//   - 关系对守卫行(friend_pair_guards,超 PairGuardRetentionDays;R9 复审 P1:
//     pair 守卫随社交图 O(n²) 累积无上界,守卫行仅锁载体删除安全,下次 acquire 重建)。
func (u *FriendUsecase) SweepTerminalRequests(ctx context.Context) {
	if n, err := u.repo.DeleteTerminalRequestsBefore(ctx, u.cfg.RequestRetentionDays, u.cfg.SweepBatch); err != nil {
		plog.With(ctx).Warnw("msg", "friend_request_sweep_failed", "err", err)
	} else if n > 0 {
		plog.With(ctx).Infow("msg", "friend_request_swept", "deleted", n, "retention_days", u.cfg.RequestRetentionDays)
	}
	if n, err := u.repo.DeletePairGuardsBefore(ctx, u.cfg.PairGuardRetentionDays, u.cfg.SweepBatch); err != nil {
		plog.With(ctx).Warnw("msg", "friend_pair_guard_sweep_failed", "err", err)
	} else if n > 0 {
		plog.With(ctx).Infow("msg", "friend_pair_guard_swept", "deleted", n, "retention_days", u.cfg.PairGuardRetentionDays)
	}
}

// RunRequestSweep 周期跑终态请求保留期清理,直到 ctx 取消。
func (u *FriendUsecase) RunRequestSweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "friend_request_sweep", func() { u.SweepTerminalRequests(ctx) })
		}
	}
}
