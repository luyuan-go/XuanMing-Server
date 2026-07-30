// retention.go — auction 保留期清理数据层(2026-07-21,CLAUDE.md §9 不变量 24 落地)。
//
// 背景:auction_orders 终态行(FILLED/CANCELED/EXPIRED)、auction_matches 已结算行、
// auction_idempotency_keys 映射行均只增不删,随挂单/成交量无界线性增长。
// 本文件按分片(DBRouter.All())批删超保留期的行:
//
//	auction_orders           终态且 release_pending=0 / match_pending=0(escrow 已释放、
//	                         无续跑意图)且 updated_at_ms 超期。uk_owner_idem 幂等键随行
//	                         删除:客户端挂单幂等键是每次请求生成的,重试窗口分钟级,90 天
//	                         后同 key 重放不可能是同一笔业务请求。
//	auction_matches          settlement_status=COMPLETED 且 event_pending=0 且 matched_at_ms
//	                         超期(结算/事件补偿只扫 PENDING 行,删已完成行不影响补偿闭环;
//	                         inventory 侧结算幂等流水同为 90 天保留,窗口一致)。
//	auction_idempotency_keys created_at_ms 超期(canonical 映射只需覆盖挂单重试窗口;
//	                         与 orders 分片不同——keys 在 owner 分片,orders 在 market 分片,
//	                         无法 join 删,按创建时间独立清理)。
//
// 不清理:auction_owner_guards(每 owner 一行,被玩家数有界)、auction_shard_topology(单行)。
// 多副本:各副本独立逐分片跑,DELETE 幂等无需锁;单批 limit 有界。
package data

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
)

// RetentionOutcome 汇总一轮三类清理的结果(逐分片累加)。
type RetentionOutcome struct {
	Orders   dbguard.Outcome
	Matches  dbguard.Outcome
	IdemKeys dbguard.Outcome
}

// Cleaned 返回本轮是否真的删了数据(供调用方决定日志措辞)。
func (o RetentionOutcome) Cleaned() bool {
	return o.Orders.Deleted+o.Matches.Deleted+o.IdemKeys.Deleted > 0
}

// PendingTotal 返回三类待清理量之和(report-only 下的积压规模)。
func (o RetentionOutcome) PendingTotal() int64 {
	return o.Orders.Matched + o.Matches.Matched + o.IdemKeys.Matched
}

// SweepRetention 对所有分片跑一轮保留期清理(每分片每表至多一批 limit 行)。
//
// **mode 默认 ModeReportOnly:只统计各表待清理量并 WARN 告警,一行都不删**(用户指令:
// 不允许"因为数据大了"自动删数据);只有配置显式 retention_mode=delete 才真删。
// 三类的 Count 与 Delete 共用同一 where(dbguard.SweepTable 保证),条件只写一遍。
//
// 任一分片/表失败立即返回错误(下一轮重试,不破坏幂等)。逐分片累加:各分片可能连不同
// 实例,累加值是"整个 auction 域"的口径,而 metric 由 SweepTable 按 db+table 打(分片间同 label
// 会互相覆盖 gauge —— 这是有意的近似:分片容量应大致均衡,单分片异常会被 dbcheck 逐库巡检抓到)。
func (r *MySQLAuctionRepo) SweepRetention(ctx context.Context, mode dbguard.Mode, cutoffMs int64, limit int) (RetentionOutcome, error) {
	var out RetentionOutcome
	out.Orders.Mode, out.Matches.Mode, out.IdemKeys.Mode = mode, mode, mode

	for _, db := range r.r.All() {
		o, oerr := dbguard.SweepTable(ctx, db, mode, "pandora_auction", "auction_orders",
			"status IN (?, ?, ?) AND release_pending = 0 AND match_pending = 0 AND updated_at_ms < ?",
			limit, StatusFilled, StatusCanceled, StatusExpired, cutoffMs)
		if oerr != nil {
			return out, errcode.New(errcode.ErrInternal, "sweep terminal orders: %v", oerr)
		}
		out.Orders.Matched += o.Matched
		out.Orders.Deleted += o.Deleted

		m, merr := dbguard.SweepTable(ctx, db, mode, "pandora_auction", "auction_matches",
			"settlement_status = ? AND event_pending = 0 AND matched_at_ms < ?",
			limit, SettlementCompleted, cutoffMs)
		if merr != nil {
			return out, errcode.New(errcode.ErrInternal, "sweep settled matches: %v", merr)
		}
		out.Matches.Matched += m.Matched
		out.Matches.Deleted += m.Deleted

		k, kerr := dbguard.SweepTable(ctx, db, mode, "pandora_auction", "auction_idempotency_keys",
			"created_at_ms < ?", limit, cutoffMs)
		if kerr != nil {
			return out, errcode.New(errcode.ErrInternal, "sweep idempotency keys: %v", kerr)
		}
		out.IdemKeys.Matched += k.Matched
		out.IdemKeys.Deleted += k.Deleted
	}
	return out, nil
}
