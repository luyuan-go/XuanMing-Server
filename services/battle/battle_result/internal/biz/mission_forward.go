// mission_forward.go — 任务事实转发出箱发布器(docs/design/mission.md §5.1)。
//
// 与 RunProgressPublisher 分开的理由(同迁移 000010 文件头)是**故障域隔离**:任务行
// 混进进度出箱会让 mission 不可用卡住队首、连带阻塞该玩家的经验 / 掉落投递 ——
// 把弱依赖变成强依赖。分表后 mission 故障只堵任务事实。
//
// 两张表**各自**都是每玩家严格 FIFO。任务事实并非"顺序无关":任务链前后两环的条件
// 类别通常不同(「杀 5 只狼」→「收集 3 张狼皮」),后环在前环完成时才被自动接取,
// 提前到达的后环事实匹配不上任何活跃任务,会被 mission 侧收据吸收后静默丢弃且永不
// 重放。所以单行失败退避时,同玩家后续行必须一起等(队首阻塞),不能越过它先投。
//
// 语义:at-least-once + mission 侧收据表(uk + 指纹)幂等吸收;行永不丢弃。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
)

// RunMissionForwarder 启动任务事实转发循环,直到 ctx 取消。
//
// reporter 未注入(mission_addr 未配)时直接返回:此时 ReportProgress 根本不产生任务
// 出箱行,没有可投递的东西(转发关闭的完整语义,不是"产了不投")。
func (u *BattleResultUsecase) RunMissionForwarder(ctx context.Context) {
	if u.missionReporter == nil {
		plog.With(ctx).Infow("msg", "mission_forwarder_disabled",
			"hint", "mission_addr 未配 → 不产生任务出箱行,任务进度不受战斗事实驱动")
		return
	}
	interval := u.cfg.ProgressPublishIntervalOrDefault()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "mission_forwarder_started",
		"interval", interval.String(), "batch", u.cfg.ProgressBatchSizeOrDefault())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "mission_forwarder_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_mission_forwarder", func() {
				if n, err := u.forwardMissionBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "mission_forward_batch_failed", "forwarded", n, "err", err)
				}
			})
		}
	}
}

// forwardMissionBatch 取一批到期任务事实投递,返回本轮成功条数。
//
// FetchMissionOutbox 每玩家只返回队首一条,所以本批内**至多一条属于同一玩家**:
// 单行失败退避只推迟该玩家自己的队列,其它玩家照常推进(跨玩家不互相拖)。
func (u *BattleResultUsecase) forwardMissionBatch(ctx context.Context) (int, error) {
	recs, err := u.repo.FetchMissionOutbox(ctx, u.cfg.ProgressBatchSizeOrDefault())
	if err != nil {
		return 0, err
	}
	forwarded := 0
	for _, r := range recs {
		if ctx.Err() != nil {
			return forwarded, ctx.Err()
		}
		rowCtx := withOutboxTrace(ctx)
		key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "mission")
		if ferr := u.missionReporter.ReportMissionFact(rowCtx, r.PlayerID,
			r.Category, r.SlotValue, r.Amount, key); ferr != nil {
			plog.With(rowCtx).Warnw("msg", "mission_fact_forward_failed",
				"id", r.ID, "match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID,
				"category", r.Category, "slot_value", r.SlotValue, "amount", r.Amount,
				"idempotency_key", key, "code", int32(errcode.As(ferr)), "err", ferr)
			if derr := u.repo.DeferMissionOutbox(rowCtx, r.ID); derr != nil {
				// 退避失败只告警:下轮 Fetch 仍会取到该行重试,at-least-once 不受影响。
				plog.With(rowCtx).Warnw("msg", "mission_outbox_defer_failed", "id", r.ID, "err", derr)
			}
			continue
		}
		if derr := u.repo.DeleteMissionOutbox(rowCtx, r.ID); derr != nil {
			// 删行失败 → 下轮重投同一事实;mission 侧收据 uk 幂等吸收,不会重复计进度。
			plog.With(rowCtx).Warnw("msg", "mission_outbox_delete_failed", "id", r.ID, "err", derr)
			continue
		}
		// 任务进度是玩家可见资产(领奖依据):失败有逐行日志、成功只有聚合 count 时,
		// 「这一局的杀怪为什么没计进任务」事后无从对账(出箱行已删)。
		plog.With(rowCtx).Infow("msg", "mission_fact_delivered",
			"id", r.ID, "match_id", r.MatchID, "seq", r.Seq, "player_id", r.PlayerID,
			"category", r.Category, "slot_value", r.SlotValue, "amount", r.Amount,
			"idempotency_key", key)
		forwarded++
	}
	if forwarded > 0 {
		plog.With(ctx).Debugw("msg", "mission_facts_forwarded", "count", forwarded)
	}
	return forwarded, nil
}
