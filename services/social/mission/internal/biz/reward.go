// reward.go — 任务发奖链(docs/design/mission.md §6;完全同构 leaderboard 模式)。
//
// mission_reward_log 是发放的唯一权威工作集:
//   - 完成扇出 / 领奖事务里落 PENDING(reward_pb 快照 = 补发重放唯一入参);
//   - 提交后同步尝试发放一次(请求 ctx 有界),失败留 PENDING/FAILED;
//   - RunRewardRetry 周期补扫 status<>GRANTED 的行重放(多副本并发安全:靠下游幂等键)。
//
// 发放路由(任一类失败整条不 GRANTED,下轮全量重放,下游幂等键各自去重):
//
//	堆叠/货币 → inventory.GrantItems      键 mission:<p>:<m>:stack
//	装备实例 → inventory.GrantInstances   键 mission:<p>:<m>:inst
//	  满包    → mail.SendOverflowMail     同 inst 键作 instance_grant_key(至多一次)
//	经验      → player.AddExperience      键 quest:<p>:<m>(player.proto 预留口径,reason="quest")
//
// 三下游各用独立幂等键:inventory_ledger uk 是 (player_id, idempotency_key),
// GrantItems 与 GrantInstances 同键会撞收据指纹冲突(fail-closed),必须分键。
package biz

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/safego"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"
)

// grantEntriesBestEffort 事务提交后的立即发放尝试(失败只记日志,补扫兜底)。
func (uc *MissionUsecase) grantEntriesBestEffort(ctx context.Context, playerID uint64, logs []*RewardLogEntry) {
	for _, entry := range logs {
		if entry == nil || entry.ID == 0 {
			continue
		}
		row := &RewardLogRow{
			ID: entry.ID, PlayerID: playerID,
			MissionConfigID: entry.MissionConfigID, Key: entry.Key, RewardPB: entry.RewardPB,
		}
		if err := uc.grantOne(ctx, row); err != nil {
			uc.log.Warnw("msg", "mission_reward_grant_deferred",
				"player_id", playerID, "mission", entry.MissionConfigID, "key", entry.Key, "err", err,
				"hint", "留 PENDING/FAILED 给补扫,不影响任务状态")
		}
	}
}

// grantOne 发放一条流水并落终态标记。
// 成功 → GRANTED;失败 → FAILED(补扫按 status<>GRANTED 继续捞,FAILED 只是审计区分)。
func (uc *MissionUsecase) grantOne(ctx context.Context, row *RewardLogRow) error {
	gerr := uc.deliver(ctx, row)
	now := uc.nowMs()
	if merr := uc.repo.MarkReward(ctx, row.ID, gerr == nil, now); merr != nil {
		// 发放成功但标记失败:行滞留 → 补扫重放 → 下游幂等键吸收,至多一次不被破坏。
		uc.log.Errorw("msg", "mission_reward_mark_failed", "id", row.ID, "granted", gerr == nil, "err", merr)
		if gerr == nil {
			return merr
		}
	}
	return gerr
}

// deliver 按 reward_pb 快照逐类发放(不回读配置表:热更不影响在途发放)。
func (uc *MissionUsecase) deliver(ctx context.Context, row *RewardLogRow) error {
	record := &missionv1.MissionRewardStorageRecord{}
	if err := proto.Unmarshal(row.RewardPB, record); err != nil {
		// 快照坏行:永远发不出去,ERROR 揭示而不是静默重试到天荒地老。
		return fmt.Errorf("reward_pb 解码失败(坏行需人工处置) id=%d: %w", row.ID, err)
	}

	var stacks []RewardItem
	var instances []uint32
	for _, it := range record.GetItems() {
		if it.GetCount() == 0 {
			continue
		}
		// 路由形态优先用快照里的冻结位(见 mission.proto MissionRewardItem.equipment):
		// 补扫重放永远走当初那条路由,配置热更 / 滚动升级期的批次差异都不会让同一条奖励
		// 换到另一个幂等键上重发一次。缺省 = 冻结位上线前写的旧行,回退读配置表(旧行为)。
		equipment := it.Equipment != nil && it.GetEquipment()
		if it.Equipment == nil {
			equipment = uc.catalog.IsEquipment(it.GetItemConfigId())
			uc.log.Warnw("msg", "mission_reward_route_unfrozen",
				"id", row.ID, "player_id", row.PlayerID, "mission", row.MissionConfigID,
				"item", it.GetItemConfigId(), "equipment", equipment,
				"hint", "冻结位上线前的历史快照,按当前道具表路由;这类行清空后本分支可删")
		}
		if equipment {
			// 装备按件展开:数量**就是**切片长度。加载期
			// (ValidateMissionCrossTables)已按同一上限拒批次,这里仍必须自带闸——
			// reward_pb 是历史快照,可能来自早于该上限的批次,也可能是道具在热更里
			// 从堆叠改成了装备。没有这道闸,一条坏快照会让补扫每轮都按数量分配内存,
			// 一路 OOM 拖垮进程(§16.5 容量边界)。
			if it.GetCount() > configtable.MaxRewardEquipmentInstances ||
				len(instances)+int(it.GetCount()) > configtable.MaxRewardEquipmentInstances {
				// 与坏行同一处置:发不出去就显式暴露,不静默截断也不无限重试到 OOM。
				return fmt.Errorf("装备发放数量越界(坏快照需人工处置) id=%d item=%d count=%d already=%d max=%d",
					row.ID, it.GetItemConfigId(), it.GetCount(), len(instances), configtable.MaxRewardEquipmentInstances)
			}
			for n := uint32(0); n < it.GetCount(); n++ {
				instances = append(instances, it.GetItemConfigId())
			}
		} else {
			stacks = append(stacks, RewardItem{ItemConfigID: it.GetItemConfigId(), Count: it.GetCount()})
		}
	}

	if len(stacks) > 0 {
		key := row.Key + ":stack"
		if err := uc.items.GrantItems(ctx, row.PlayerID, key, stacks); err != nil {
			return fmt.Errorf("grant items: %w", err)
		}
	}
	if len(instances) > 0 {
		key := row.Key + ":inst"
		capacityFull, err := uc.items.GrantInstances(ctx, row.PlayerID, key, instances)
		if err != nil {
			return fmt.Errorf("grant instances: %w", err)
		}
		if capacityFull {
			// 满包溢出转邮件(battle_result 模式):同键传 instance_grant_key,
			// 直发链与邮件领取链共享幂等键 → 至多一次。
			if uc.mail == nil {
				return fmt.Errorf("背包满且 mail_addr 未配,装备无法投递(留补扫) player=%d", row.PlayerID)
			}
			if merr := uc.mail.SendOverflowMail(ctx, row.PlayerID, instances, key); merr != nil {
				return fmt.Errorf("overflow mail: %w", merr)
			}
		}
	}
	if record.GetExp() > 0 {
		// 幂等键 quest:<player>:<mission> 是 player.proto AddExperienceRequest 注释的预留口径。
		key := fmt.Sprintf("quest:%d:%d", row.PlayerID, row.MissionConfigID)
		if err := uc.exp.AddExperience(ctx, row.PlayerID, record.GetExp(), key); err != nil {
			return fmt.Errorf("add experience: %w", err)
		}
	}
	return nil
}

// RunRewardRetry 发奖补扫 worker(对齐 leaderboard RetryUngrantedRewards 接线:
// 周期扫、grace 挡在途、单轮限量、safego 兜 panic;多副本并发安全)。
func (uc *MissionUsecase) RunRewardRetry(ctx context.Context) {
	interval := uc.cfg.RewardRetryInterval.Std()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "mission_reward_retry", func() { uc.retryUngrantedOnce(ctx) })
		}
	}
}

func (uc *MissionUsecase) retryUngrantedOnce(ctx context.Context) {
	olderThan := uc.nowMs() - uc.cfg.RewardRetryGrace.Std().Milliseconds()
	rows, err := uc.repo.ListUngrantedRewards(ctx, olderThan, uc.cfg.RewardRetryBatch)
	if err != nil {
		uc.log.Errorw("msg", "mission_reward_retry_list_failed", "err", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	var granted, failed int
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		if err := uc.grantOne(ctx, row); err != nil {
			failed++
			uc.log.Warnw("msg", "mission_reward_retry_failed",
				"id", row.ID, "player_id", row.PlayerID, "mission", row.MissionConfigID, "err", err)
			continue
		}
		granted++
	}
	uc.log.Infow("msg", "mission_reward_retry_round", "scanned", len(rows), "granted", granted, "failed", failed)
}

// RunPushPublisher 推送出箱发布 worker(FIFO 按 id 序;成功即删行,失败中断本轮保序;
// pusher 未接线时直接退化为清扫禁用——出箱行会堆积,由 dbcheck outbox 检查揭示)。
//
// **已知代价,刻意不在本服务单点改**(2026-08-11 对抗式复核确认后归档):这里是**全局**
// FIFO,不是每玩家 FIFO。某个 kafka 分区 leader 不可用时,队首行恰好哈希到该分区就会
// 卡住整轮,期间**其它分区健康的玩家**推送同样延迟,出箱按全服写入速率堆积。分区恢复后
// 按原序自动排空,无丢失无重复;窗口内客户端仍可靠 ListMissions / push.resync 拿到正确态,
// 故不是正确性问题。
//
// 不单改的理由:battle_result 的 `player_update_outbox` 与 player 的 `player_push_outbox`
// 是同一套写法,只把 mission 改成逐行退避会在三条同类链路上留下两套纪律。要收敛就三处
// 一起改,并按 §15.4 先拿出真实压测 / 故障证据(通用最佳实践不构成改的理由)。
func (uc *MissionUsecase) RunPushPublisher(ctx context.Context) {
	if uc.pusher == nil {
		uc.log.Warnw("msg", "mission_push_publisher_disabled",
			"hint", "kafka 未配置;mission_push_outbox 将堆积,客户端只能靠 ListMissions 兜底")
		return
	}
	ticker := time.NewTicker(uc.cfg.PushPublishInterval.Std())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "mission_push_publish", func() { uc.publishPushBatch(ctx) })
		}
	}
}

func (uc *MissionUsecase) publishPushBatch(ctx context.Context) {
	for {
		rows, err := uc.repo.FetchPushOutbox(ctx, uc.cfg.PushPublishBatch)
		if err != nil {
			uc.log.Errorw("msg", "mission_push_fetch_failed", "err", err)
			return
		}
		if len(rows) == 0 {
			return
		}
		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}
			if err := uc.pusher.PushMissionUpdate(ctx, row.PlayerID, row.Payload); err != nil {
				// 失败中断本轮:同玩家事件必须保序(kafka key=player_id,分区内有序,
				// 但先跳过失败行再发后续行会乱序)。
				uc.log.Warnw("msg", "mission_push_publish_failed", "id", row.ID, "err", err)
				return
			}
			if err := uc.repo.DeletePushOutbox(ctx, row.ID); err != nil {
				// 删行失败 → 下轮重发同一行,push 契约本就 at-least-once,客户端按业务判重。
				uc.log.Warnw("msg", "mission_push_delete_failed", "id", row.ID, "err", err)
				return
			}
		}
		if len(rows) < uc.cfg.PushPublishBatch {
			return // 没满批 = 已排空
		}
	}
}
