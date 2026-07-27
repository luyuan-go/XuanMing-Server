// experience.go — 玩家等级经验入账 + 经验推送出箱发布器(实时成长,2026-07-20)。
//
// 设计(docs/design/realtime-progression.md §4.2):
//   - AddExperience 是**系统 RPC 的业务核心**:幂等入账(uk player_id+idempotency_key)、
//     按等级经验曲线循环进位(连升多级)、最高等级封顶、满级 no-op。
//   - 入账与经验推送出箱同一 MySQL 事务(不变量 §4);后台 RunPushOutboxPublisher 轮询
//     出箱表逐条投 kafka pandora.player.experience(event_type=EXPERIENCE header),
//     push 服务透明转发给客户端刷新经验条 / 播升级表现。
//   - 经验来源(battle_result progress 出箱 / 任务完成点 / GM)只报 delta,
//     等级曲线唯一权威在本服务(DS / 调用方不可信,不变量 §6 同构)。
//   - 多副本发布器无需 claim/fencing:事件是入账后的**全量权威快照**,客户端按
//     (level, exp_in_level) 单调不回退去重,重复投递 / 旧快照晚到都无副作用
//     (at-least-once + 快照语义,§16.2)。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"

	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// ExperiencePusher 把玩家推送出箱行投递到 kafka pandora.player.experience
// (key=player_id 保序,event_type 走 kafka header,push 服务透传)。
// 投递失败返回 error → 出箱行保留下轮重试(at-least-once,不变量 §4)。
type ExperiencePusher interface {
	PushPlayerEvent(ctx context.Context, playerID uint64, eventType uint32, payload []byte) error
}

// SetExperiencePusher 注入推送出箱发布器的 kafka producer 适配(nil-safe:
// brokers 未配 / producer 构造失败时不注入,出箱积压不丢,恢复后重启补发)。
func (u *PlayerUsecase) SetExperiencePusher(p ExperiencePusher) {
	u.expPusher = p
}

// AddExperience 幂等入账经验并结算等级(实时成长唯一入口,系统调用)。
// 返回 (入账后快照, 是否幂等命中, error)。
//
//	配置表未加载 → ErrPlayerFeatureDisabled(启动主链要求配置表加载成功；这里只做防御)
//	delta 超单次上限 → ErrInvalidArg(防异常调用方一次灌满;上限见 conf.MaxExpPerGrant)
//	满级 → no-op:返回满级快照,不加经验、不出箱,但仍消费幂等键落 no-op 收据
//	(重放命中收据返回 already=true;防曲线扩容后滞留事件重入账,审计 P2)
func (u *PlayerUsecase) AddExperience(ctx context.Context, playerID uint64, delta uint64, reason, idempotencyKey string) (data.ExpState, bool, error) {
	if playerID == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if delta == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg, "exp_delta must be positive")
	}
	if maxGrant := u.cfg.MaxExpPerGrantOrDefault(); delta > maxGrant {
		return data.ExpState{}, false, errcode.New(errcode.ErrInvalidArg,
			"exp_delta %d exceeds max_exp_per_grant %d", delta, maxGrant)
	}
	if !u.cfg.ExperienceEnabled {
		return data.ExpState{}, false, errcode.New(errcode.ErrPlayerFeatureDisabled, "experience disabled")
	}
	curve := u.experienceCurve()
	if len(curve) == 0 {
		return data.ExpState{}, false, errcode.New(errcode.ErrPlayerFeatureDisabled, "experience disabled (player level table unavailable)")
	}
	if err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return data.ExpState{}, false, err
	}

	st, already, err := u.repo.ApplyExperience(ctx, data.ExpApply{
		PlayerID:       playerID,
		Delta:          delta,
		Reason:         reason,
		IdempotencyKey: idempotencyKey,
		Curve:          curve,
	})
	if err != nil {
		return data.ExpState{}, false, err
	}
	if already {
		plog.With(ctx).Debugw("msg", "add_experience_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey,
			"level", st.Level, "exp_in_level", st.ExpInLevel)
		return st, true, nil
	}
	plog.With(ctx).Debugw("msg", "add_experience_applied",
		"player_id", playerID, "delta", delta, "reason", reason,
		"level", st.Level, "exp_in_level", st.ExpInLevel,
		"levels_gained", st.LevelsGained, "is_max", st.IsMaxLevel)
	return st, false, nil
}

// DecorateExperience 用等级曲线给档案补经验派生字段(GetProfile 出参装饰):
// 满级 → is_max_level=true 且级内经验按 0 展示(权威列已保证满级恒 0,此处防御性夹紧)。
// 曲线未配置(功能关闭)→ 不标满级,exp 原样(默认 0),行为与历史一致。
func (u *PlayerUsecase) DecorateExperience(level int32, expInLevel uint64) (uint64, bool) {
	if !u.cfg.ExperienceEnabled {
		return expInLevel, false
	}
	curve := u.experienceCurve()
	if len(curve) == 0 {
		return expInLevel, false
	}
	maxLevel := int32(len(curve)) + 1
	if level >= maxLevel {
		return 0, true
	}
	return expInLevel, false
}

// experienceCurve 从当前原子快照提取本次调用使用的曲线副本。
func (u *PlayerUsecase) experienceCurve() []uint64 {
	if u.expLevels == nil {
		return nil
	}
	return u.expLevels.ExperienceCurve()
}

// RunPushOutboxPublisher 启动后台玩家推送出箱发布循环,直到 ctx 取消
// (发布器纪律对齐 battle_result.RunOutboxPublisher:FIFO、失败中断本轮保序、成功才删行)。
func (u *PlayerUsecase) RunPushOutboxPublisher(ctx context.Context) {
	if u.expPusher == nil {
		plog.With(ctx).Infow("msg", "push_outbox_publisher_disabled",
			"hint", "kafka producer 未注入 → 经验推送出箱积压不丢,producer 可用后重启补发")
		return
	}
	interval := u.cfg.PushOutboxIntervalOrDefault()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "push_outbox_publisher_started",
		"interval", interval.String(), "batch", u.cfg.PushOutboxBatchOrDefault())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "push_outbox_publisher_stopped")
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,出箱行下轮重试。
			safego.Run(ctx, "player_push_outbox_publisher", func() {
				// 排空循环:满批说明还有积压,立即继续下一批,不等下个 tick
				// (否则吞吐被钉死在 batch/interval,持续流量下积压只增不减)。
				for ctx.Err() == nil {
					n, err := u.publishPushOutboxBatch(ctx)
					if err != nil {
						plog.With(ctx).Warnw("msg", "push_outbox_publish_batch_failed", "published", n, "err", err)
						break
					}
					if n < u.cfg.PushOutboxBatchOrDefault() {
						break // 未满批 = 已清空
					}
				}
			})
		}
	}
}

// RunExpHistoryJanitor 启动 exp_history 幂等收据的后台清理循环,直到 ctx 取消。
//
// **默认关闭**(审计 P1):battle_result progress 出箱是永久重试链(退避上限 5min,
// 无总重试期限)。入账成功但响应丢失/删行持续失败超过留存期时,清掉收据 = 同一事件
// 再次入账(双发)。幂等正确性优先于表增长;开启(exp_history_cleanup_enabled)的前置
// 条件是上游出箱先具备**小于留存期的有界重试/隔离期限**,由运维显式确认后配置。
// 多副本并发 DELETE ... LIMIT 安全(各删各的行);表按 PK 升序扫,老行在前。
func (u *PlayerUsecase) RunExpHistoryJanitor(ctx context.Context) {
	if !u.cfg.ExpHistoryCleanupEnabled {
		plog.With(ctx).Infow("msg", "exp_history_janitor_disabled",
			"hint", "progress 出箱无总重试期限,清收据会破坏幂等;上游具备有界重试后再显式开启")
		return
	}
	const (
		sweepInterval = time.Hour
		purgeBatch    = 1000
	)
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "player_exp_history_janitor", func() {
				cutoff := time.Now().Add(-u.cfg.ExpHistoryRetentionOrDefault())
				var total int64
				for ctx.Err() == nil {
					n, err := u.repo.PurgeExpHistory(ctx, cutoff, purgeBatch)
					if err != nil {
						plog.With(ctx).Warnw("msg", "exp_history_purge_failed", "purged", total, "err", err)
						break
					}
					total += n
					if n < purgeBatch {
						break
					}
				}
				if total > 0 {
					plog.With(ctx).Infow("msg", "exp_history_purged", "rows", total)
				}
			})
		}
	}
}

// RunHistoryJanitor 启动 mmr_history / attr_point_grants / talent_point_grants 幂等历史
// 的后台清理循环,直到 ctx 取消(CLAUDE.md §9 不变量 24:只增表必须有界)。
//
// **默认关闭**,与 RunExpHistoryJanitor 同理由:上游 kafka player.update 消费与授予补扫
// 是 at-least-once,清掉幂等行后同一事件重放 = 双发(重复加段位分/加点)。开启前置条件:
// 上游重放期限(kafka retention / 补扫窗口)必须小于留存期(默认 90 天,下限 30 天)。
// 多副本并发 DELETE ... LIMIT 安全(各删各的行)。
func (u *PlayerUsecase) RunHistoryJanitor(ctx context.Context) {
	if !u.cfg.HistoryCleanupEnabled {
		plog.With(ctx).Infow("msg", "history_janitor_disabled",
			"hint", "mmr/点数授予幂等历史不清理;确认上游重放期限 < 留存期后显式开启 history_cleanup_enabled")
		return
	}
	const (
		sweepInterval = time.Hour
		purgeBatch    = 1000
	)
	type purgeFn struct {
		name string
		fn   func(context.Context, time.Time, int) (int64, error)
	}
	purges := []purgeFn{
		{"mmr_history", u.repo.PurgeMMRHistory},
		{"attr_point_grants", u.repo.PurgeAttrPointGrants},
		{"talent_point_grants", u.repo.PurgeTalentPointGrants},
	}
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "player_history_janitor", func() {
				cutoff := time.Now().Add(-u.cfg.HistoryRetentionOrDefault())
				for _, p := range purges {
					var total int64
					for ctx.Err() == nil {
						n, err := p.fn(ctx, cutoff, purgeBatch)
						if err != nil {
							plog.With(ctx).Warnw("msg", "history_purge_failed", "table", p.name, "purged", total, "err", err)
							break
						}
						total += n
						if n < purgeBatch {
							break
						}
					}
					if total > 0 {
						plog.With(ctx).Infow("msg", "history_purged", "table", p.name, "rows", total)
					}
				}
			})
		}
	}
}

// publishPushOutboxBatch 取一批推送出箱记录投递,返回本轮成功投递并删除的条数。
// 投递失败立即中断本轮(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递(不变量 §9)。
func (u *PlayerUsecase) publishPushOutboxBatch(ctx context.Context) (int, error) {
	if u.expPusher == nil {
		return 0, nil
	}
	recs, err := u.repo.FetchPushOutbox(ctx, u.cfg.PushOutboxBatchOrDefault())
	if err != nil {
		return 0, err
	}
	published := 0
	for _, r := range recs {
		if perr := u.expPusher.PushPlayerEvent(ctx, r.PlayerID, r.EventType, r.Payload); perr != nil {
			return published, perr // 本轮中断,保留出箱行下轮重试
		}
		if derr := u.repo.DeletePushOutbox(ctx, r.ID); derr != nil {
			return published, derr
		}
		published++
	}
	if published > 0 {
		plog.With(ctx).Debugw("msg", "push_outbox_published", "count", published)
	}
	return published, nil
}
