// retention.go — battle_result 保留期清理(2026-07-21,CLAUDE.md §9 不变量 24 落地)。
//
// 背景:battles / battle_player_stats 每场对局写 1 + N 行,battle_progress_stream /
// battle_progress_player 每场走实时通道的对局写 1 + N 行,均只增不删,随对局量无界线性增长。
// 本文件周期批量回收超保留期(默认 90 天)的行:
//
//	battles + battle_player_stats     服务端落库时间 created_at 超期后同事务批删
//	                                  (§9.6 数值不信 DS:ended_at_ms 是 DS 上报,不作清理依据)。
//	                                  match_id 幂等键只需覆盖结算重试窗口(小时级):删除后
//	                                  同 match 重放在凭据层(Guard / active match / roster)就被拒。
//	                                  玩家对局历史查询窗口同步受限于保留期。
//	progress stream + player          仅删已结算(settled_at_ms>0,服务端结算打标)且超期的行;
//	                                  未结算陈年行 = 补偿链 bug,保留证据并每轮告警,永不静默清。
//
// 出箱表(player_update / drop / progress / match_release / terminal_release / exit_proof)
// 均为投递成功即删,不在本文件清理范围(积压属告警问题,不是增长问题)。
//
// 吞吐:每轮每类小批量循环删到短批为止(每批独立短事务不锁表;单批封顶靠
// cfg.RetentionSweepBatch)。只删单批的话默认 200 场/小时 ≈ 4800 场/天,追不平
// 生产流入,积压只增不减(审计 P1)。
// 多副本:各副本独立跑,无锁——SELECT 候选 + 批删幂等,并发只多花空批。
package biz

import (
	"context"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
)

// RunRetentionSweep 周期跑保留期清理,直到 ctx 取消。
// 任一类失败只记日志继续下一类:清理彼此独立,幂等,下一轮自然重试。
func (u *BattleResultUsecase) RunRetentionSweep(ctx context.Context) {
	interval := u.cfg.RetentionSweepInterval.Std()
	if interval <= 0 {
		interval = time.Hour // 防御:未过 Defaults 的零值配置(NewTicker(0) 会 panic)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "battle_retention_sweep", func() { u.sweepRetentionOnce(ctx) })
		}
	}
}

// sweepRetentionOnce 跑一轮(每类小批量循环删到追平),独立函数便于单测。
func (u *BattleResultUsecase) sweepRetentionOnce(ctx context.Context) {
	log := plog.With(ctx)
	cutoffMs := time.Now().AddDate(0, 0, -u.cfg.HistoryRetentionDays).UnixMilli()

	if n := u.drainPurge(ctx, "battles", cutoffMs, u.repo.PurgeExpiredBattles); n > 0 {
		log.Infow("msg", "battle_retention_battles_purged", "matches", n, "retention_days", u.cfg.HistoryRetentionDays)
	}
	if n := u.drainPurge(ctx, "progress", cutoffMs, u.repo.PurgeSettledProgress); n > 0 {
		log.Infow("msg", "battle_retention_progress_purged", "matches", n, "retention_days", u.cfg.HistoryRetentionDays)
	}

	// 陈年未结算水位 = 结算补偿链 bug 证据:按设计永不清理,但必须持续告警暴露
	// (否则"保留证据待排查"退化成静默永久保留,§9.24 有界承诺落空)。
	if stale, err := u.repo.CountStaleUnsettledProgress(ctx, cutoffMs); err != nil {
		log.Warnw("msg", "battle_retention_stale_unsettled_count_failed", "err", err)
	} else if stale > 0 {
		log.Errorw("msg", "battle_retention_stale_unsettled_progress",
			"count", stale, "retention_days", u.cfg.HistoryRetentionDays,
			"hint", "存在超保留期未结算的进度水位行(结算补偿链 bug),永不自动清理,需人工排查对应 match 的结算链路")
	}
}

// drainPurge 单类清理:小批量循环删到短批(= 积压追平)为止。
// 每批独立短事务(repo 内部保证),不长事务锁表;满批说明可能还有积压,继续下一批;
// 失败中断本轮(幂等,下一轮重试)。清理期间新写入的行 created_at 必然晚于 cutoff,
// 不会被本轮追进来,循环必然终止。
func (u *BattleResultUsecase) drainPurge(ctx context.Context, kind string, cutoffMs int64, purge func(context.Context, int64, int) (int64, error)) int64 {
	batch := u.cfg.RetentionSweepBatch
	if batch <= 0 {
		batch = 200 // 防御:未过 Defaults 的零值配置(batch=0 时 n<batch 永假 → 死循环)
	}
	var total int64
	for ctx.Err() == nil {
		n, err := purge(ctx, cutoffMs, batch)
		if err != nil {
			plog.With(ctx).Warnw("msg", "battle_retention_purge_failed", "kind", kind, "err", err, "purged_before_fail", total)
			return total
		}
		total += n
		if n < int64(batch) {
			return total
		}
	}
	return total
}
