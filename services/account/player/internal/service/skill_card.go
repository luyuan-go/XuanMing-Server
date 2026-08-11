package service

import (
	"context"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// skill_card.go — 技能卡 RPC 边界(持有 / 培养 / 更换)。
//
// 鉴权分两类,与天赋一致:
//   GrantSkillCards 是系统 RPC(抽卡 / 活动 / GM 驱动),systemOnly + Envoy 路由层 403 双保险;
//   Upgrade / SetSlots / GetSkillCards 是玩家自助,一律以调用者身份为准(selfPlayerID),
//   不接受请求体里的他人 player_id。

// GrantSkillCards 幂等发放技能卡 / 碎片(系统 RPC,不对客户端开放)。
func (s *PlayerService) GrantSkillCards(ctx context.Context, req *playerv1.GrantSkillCardsRequest) (*playerv1.GrantSkillCardsResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.GrantSkillCardsResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 {
		return &playerv1.GrantSkillCardsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	grants := make([]data.SkillCardGrant, 0, len(req.GetGrants()))
	for _, g := range req.GetGrants() {
		grants = append(grants, data.SkillCardGrant{CardID: g.GetCardId(), Shards: g.GetShards()})
	}
	cards, already, err := s.uc.GrantSkillCards(ctx, req.GetPlayerId(), grants, req.GetIdempotencyKey())
	if err != nil {
		return &playerv1.GrantSkillCardsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GrantSkillCardsResponse{
		Code:    commonv1.ErrCode_OK,
		Cards:   toProtoSkillCards(cards),
		Already: already,
	}, nil
}

// UpgradeSkillCard 消耗碎片把一张卡升一级。以调用者身份为准。
func (s *PlayerService) UpgradeSkillCard(ctx context.Context, req *playerv1.UpgradeSkillCardRequest) (*playerv1.UpgradeSkillCardResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.UpgradeSkillCardResponse{Code: code}, nil
	}
	card, cost, err := s.uc.UpgradeSkillCard(ctx, playerID, req.GetCardId())
	if err != nil {
		return &playerv1.UpgradeSkillCardResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.UpgradeSkillCardResponse{
		Code: commonv1.ErrCode_OK,
		Card: &playerv1.SkillCard{
			CardId: card.CardID,
			Level:  card.Level,
			Shards: card.Shards,
		},
		ShardCost: cost,
	}, nil
}

// SetSkillSlots 全量替换卡槽装配。以调用者身份为准。
func (s *PlayerService) SetSkillSlots(ctx context.Context, req *playerv1.SetSkillSlotsRequest) (*playerv1.SetSkillSlotsResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.SetSkillSlotsResponse{Code: code}, nil
	}
	slots := make([]data.SkillSlot, 0, len(req.GetSlots()))
	for _, sl := range req.GetSlots() {
		slots = append(slots, data.SkillSlot{Slot: sl.GetSlot(), CardID: sl.GetCardId()})
	}
	applied, err := s.uc.SetSkillSlots(ctx, playerID, slots)
	if err != nil {
		return &playerv1.SetSkillSlotsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.SetSkillSlotsResponse{Code: commonv1.ErrCode_OK, Slots: toProtoSkillSlots(applied)}, nil
}

// GetSkillCards 读持卡与卡槽装配。以调用者身份为准。
func (s *PlayerService) GetSkillCards(ctx context.Context, req *playerv1.GetSkillCardsRequest) (*playerv1.GetSkillCardsResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetSkillCardsResponse{Code: code}, nil
	}
	cards, slots, err := s.uc.GetSkillCards(ctx, playerID)
	if err != nil {
		return &playerv1.GetSkillCardsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GetSkillCardsResponse{
		Code:  commonv1.ErrCode_OK,
		Cards: toProtoSkillCards(cards),
		Slots: toProtoSkillSlots(slots),
	}, nil
}

func toProtoSkillCards(cards []data.SkillCard) []*playerv1.SkillCard {
	out := make([]*playerv1.SkillCard, 0, len(cards))
	for _, c := range cards {
		out = append(out, &playerv1.SkillCard{CardId: c.CardID, Level: c.Level, Shards: c.Shards})
	}
	return out
}

func toProtoSkillSlots(slots []data.SkillSlot) []*playerv1.SkillSlot {
	out := make([]*playerv1.SkillSlot, 0, len(slots))
	for _, s := range slots {
		out = append(out, &playerv1.SkillSlot{Slot: s.Slot, CardId: s.CardID})
	}
	return out
}
