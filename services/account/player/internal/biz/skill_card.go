package biz

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// skill_card.go — 技能卡:持有(GrantSkillCards)、培养(UpgradeSkillCard)、更换(SetSkillSlots)。
//
// 权威分工与天赋一致:卡是否存在、等级上限、每级消耗都读配置表,表未加载一律 fail-closed;
// 玩家侧写路径(升级 / 换卡)受 LoadoutCustomizeEnabled 开关约束,系统发放不受(与 GrantTalentPoints 同)。

// SkillSlotCount 是卡槽数量,对应战斗内的 Q/W/E/R 四个技能位。
//
// 刻意做成常量而不是配置项:槽位数同时被客户端 UI 布局、DS 给技能的循环和本校验读,
// 三处必须一致。做成可配会让"改了服务端配置但客户端还是 4 个格子"变成一类可能的事故,
// 而这个数字并没有按环境变化的需求(§15.3 不为假想的扩展提前加开关)。
const SkillSlotCount uint32 = 4

// GrantSkillCards 幂等发放技能卡 / 碎片(系统 RPC;抽卡 / 活动 / GM)。
//
// 不受 LoadoutCustomizeEnabled 约束:开关管的是玩家自助改配装,发放是系统行为。
func (u *PlayerUsecase) GrantSkillCards(ctx context.Context, playerID uint64, grants []data.SkillCardGrant, idempotencyKey string) ([]data.SkillCard, bool, error) {
	if playerID == 0 {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(grants) == 0 {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "grants required")
	}
	if idempotencyKey == "" {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}

	seen := make(map[uint32]struct{}, len(grants))
	for _, g := range grants {
		if g.CardID == 0 {
			return nil, false, errcode.New(errcode.ErrInvalidArg, "card_id required")
		}
		if _, dup := seen[g.CardID]; dup {
			// 同一批里同一张卡出现两次,两条 ON DUPLICATE KEY UPDATE 会各加一次碎片,
			// 结果对但很难对账。要求调用方自己合并,发放意图才始终是一目了然的。
			return nil, false, errcode.New(errcode.ErrInvalidArg, "duplicate card_id %d in one grant", g.CardID)
		}
		seen[g.CardID] = struct{}{}
		// 发放的卡必须在配置表里:发一张表里没有的卡,玩家背包里会出现一张永远打不开、
		// 升不了、装不上的幽灵卡,且没有任何报错。
		if u.skillCardRules == nil {
			return nil, false, errcode.New(errcode.ErrInternal, "skill card config table unavailable")
		}
		if !u.skillCardRules.CardExists(g.CardID) {
			return nil, false, errcode.New(errcode.ErrInvalidArg, "unknown skill card %d", g.CardID)
		}
	}

	if err := u.repo.EnsureProfile(ctx, playerID, u.defaultNickname(playerID), u.cfg.BaseMMR); err != nil {
		return nil, false, err
	}
	cards, already, err := u.repo.GrantSkillCards(ctx, playerID, grants, idempotencyKey)
	if err != nil {
		return nil, false, err
	}
	if already {
		plog.With(ctx).Debugw("msg", "grant_skill_cards_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey)
	}
	return cards, already, nil
}

// UpgradeSkillCard 消耗碎片把一张卡升一级,返回升级后状态与本次消耗。
func (u *PlayerUsecase) UpgradeSkillCard(ctx context.Context, playerID uint64, cardID uint32) (data.SkillCard, uint32, error) {
	if playerID == 0 {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if cardID == 0 {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrInvalidArg, "card_id required")
	}
	if !u.cfg.LoadoutCustomizeEnabled {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrPlayerFeatureDisabled, "loadout customize disabled")
	}
	if u.skillCardRules == nil {
		return data.SkillCard{}, 0, errcode.New(errcode.ErrInternal, "skill card config table unavailable")
	}

	// 整条曲线交给 repo 在事务内按锁到的等级查价:先读等级再算价会让并发两次升级
	// 都按同一级的价钱扣(§16.1 TOCTOU)。
	curve, maxLevel, err := u.skillCardRules.UpgradeCurve(cardID)
	if err != nil {
		return data.SkillCard{}, 0, err
	}
	return u.repo.UpgradeSkillCard(ctx, playerID, cardID, curve, maxLevel)
}

// SetSkillSlots 全量替换卡槽装配(更换技能卡)。
//
// card_id=0 表示显式清空该槽 —— 清空不落行,直接从待写集合里剔除。
func (u *PlayerUsecase) SetSkillSlots(ctx context.Context, playerID uint64, slots []data.SkillSlot) ([]data.SkillSlot, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if !u.cfg.LoadoutCustomizeEnabled {
		return nil, errcode.New(errcode.ErrPlayerFeatureDisabled, "loadout customize disabled")
	}
	if u.skillCardRules == nil {
		return nil, errcode.New(errcode.ErrInternal, "skill card config table unavailable")
	}

	filled := make([]data.SkillSlot, 0, len(slots))
	seenSlot := make(map[uint32]struct{}, len(slots))
	seenCard := make(map[uint32]struct{}, len(slots))
	for _, s := range slots {
		if s.Slot >= SkillSlotCount {
			return nil, errcode.New(errcode.ErrSkillCardSlotInvalid,
				"slot %d out of range [0,%d)", s.Slot, SkillSlotCount)
		}
		if _, dup := seenSlot[s.Slot]; dup {
			return nil, errcode.New(errcode.ErrSkillCardSlotInvalid, "duplicate slot %d", s.Slot)
		}
		seenSlot[s.Slot] = struct{}{}

		if s.CardID == 0 {
			continue // 显式清空:不落行。
		}
		if _, dup := seenCard[s.CardID]; dup {
			// 同一张卡占两个槽。库上有 uk_player_card_once 兜底,但那会返回一条
			// 面向并发的错误;在这里判能给出准确的"是哪张卡重复了"。
			return nil, errcode.New(errcode.ErrSkillCardSlotInvalid,
				"skill card %d assigned to more than one slot", s.CardID)
		}
		seenCard[s.CardID] = struct{}{}

		if !u.skillCardRules.CardExists(s.CardID) {
			return nil, errcode.New(errcode.ErrInvalidArg, "unknown skill card %d", s.CardID)
		}
		filled = append(filled, s)
	}

	// 持有校验在 repo 的事务内做(与删旧插新同一把锁),这里不预查:
	// 预查会引入"查到持有 → 期间卡被消耗 → 装上了没有的卡"的窗口。
	if err := u.repo.SetSkillSlots(ctx, playerID, filled); err != nil {
		return nil, err
	}
	return filled, nil
}

// GetSkillCards 读玩家全部持卡与当前卡槽装配(一次往返拿齐)。
func (u *PlayerUsecase) GetSkillCards(ctx context.Context, playerID uint64) ([]data.SkillCard, []data.SkillSlot, error) {
	if playerID == 0 {
		return nil, nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	cards, err := u.repo.GetSkillCards(ctx, playerID)
	if err != nil {
		return nil, nil, err
	}
	slots, err := u.repo.GetSkillSlots(ctx, playerID)
	if err != nil {
		return nil, nil, err
	}
	return cards, slots, nil
}
