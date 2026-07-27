// sweep.go — guild 终态入会申请保留期清理(2026-07-21,CLAUDE.md §9 不变量 24 落地)。
//
// 背景:guild_join_requests 每对 (guild,player) 至多一行(uk 复用),但 approved/rejected
// 终态行随申请对数累积。终态行无资产语义(成员权威在 guild_members),超保留期(默认 90 天)
// 删除后再次申请 = 全新 INSERT pending,行为等价。pending 永不清。
// 临时群(chat_groups/chat_group_members)解散即删、总量被 §9.18 上限×玩家数有界,不在清理范围。
//
// 多副本:各副本独立跑,无锁(对齐 mail sweep)——DELETE 幂等,并发只多花空批。
package biz

import (
	"context"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
)

// SweepTerminalJoinRequests 跑一轮终态申请清理(至多一批 cfg.SweepBatch),由调用方 ticker 驱动。
func (u *GuildUsecase) SweepTerminalJoinRequests(ctx context.Context) {
	if n, err := u.repo.DeleteTerminalJoinRequestsBefore(ctx, u.cfg.RequestRetentionDays, u.cfg.SweepBatch); err != nil {
		plog.With(ctx).Warnw("msg", "guild_request_sweep_failed", "err", err)
	} else if n > 0 {
		plog.With(ctx).Infow("msg", "guild_request_swept", "deleted", n, "retention_days", u.cfg.RequestRetentionDays)
	}
}

// RunRequestSweep 周期跑终态申请保留期清理,直到 ctx 取消。
func (u *GuildUsecase) RunRequestSweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "guild_request_sweep", func() { u.SweepTerminalJoinRequests(ctx) })
		}
	}
}
