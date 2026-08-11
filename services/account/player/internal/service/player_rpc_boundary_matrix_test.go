package service

import (
	"context"
	"testing"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

type playerRPCProbe func(context.Context) (commonv1.ErrCode, error)

func assertPlayerRPCMatrix(t *testing.T, ctx context.Context, want commonv1.ErrCode, tests []struct {
	name string
	call playerRPCProbe
}) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(ctx)
			if err != nil || got != want {
				t.Fatalf("code=%v err=%v, want code=%v", got, err, want)
			}
		})
	}
}

// TestPlayerRPCCrossPlayerIdentityMatrix 逐个覆盖所有客户端读写 RPC 与系统 RPC，证明
// 请求在触及 usecase 前统一拒绝 IDOR 与玩家 JWT 越权。
func TestPlayerRPCCrossPlayerIdentityMatrix(t *testing.T) {
	svc := NewPlayerService(nil)
	const callerID, otherID = uint64(1001), uint64(2002)

	crossPlayer := []struct {
		name string
		call playerRPCProbe
	}{
		{"GetProfile", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetProfile(ctx, &playerv1.GetProfileRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"UpdateNickname", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UpdateNickname(ctx, &playerv1.UpdateNicknameRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"ListHeroes", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ListHeroes(ctx, &playerv1.ListHeroesRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetMMR", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetMMR(ctx, &playerv1.GetMMRRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"SelectHero", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.SelectHero(ctx, &playerv1.SelectHeroRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetActiveHero", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetActiveHero(ctx, &playerv1.GetActiveHeroRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"AllocateAttributePoints", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.AllocateAttributePoints(ctx, &playerv1.AllocateAttributePointsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"ResetAttributes", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ResetAttributes(ctx, &playerv1.ResetAttributesRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetAttributes", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetAttributes(ctx, &playerv1.GetAttributesRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"SetEquipment", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.SetEquipment(ctx, &playerv1.SetEquipmentRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetEquipment", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetEquipment(ctx, &playerv1.GetEquipmentRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"SetTalents", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.SetTalents(ctx, &playerv1.SetTalentsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"ResetTalents", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ResetTalents(ctx, &playerv1.ResetTalentsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetTalents", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetTalents(ctx, &playerv1.GetTalentsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetLoadout", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetLoadout(ctx, &playerv1.GetLoadoutRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"ClaimReward", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ClaimReward(ctx, &playerv1.ClaimRewardRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetRewardClaims", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetRewardClaims(ctx, &playerv1.GetRewardClaimsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"UpgradeSkillCard", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UpgradeSkillCard(ctx, &playerv1.UpgradeSkillCardRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"SetSkillSlots", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.SetSkillSlots(ctx, &playerv1.SetSkillSlotsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
		{"GetSkillCards", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetSkillCards(ctx, &playerv1.GetSkillCardsRequest{PlayerId: otherID})
			return r.GetCode(), e
		}},
	}
	assertPlayerRPCMatrix(t, withCaller(callerID), commonv1.ErrCode_ERR_PERMISSION_DENY, crossPlayer)

	systemOnly := []struct {
		name string
		call playerRPCProbe
	}{
		{"UnlockHero", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UnlockHero(ctx, &playerv1.UnlockHeroRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"UpdateMMR", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UpdateMMR(ctx, &playerv1.UpdateMMRRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"GrantAttributePoints", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantAttributePoints(ctx, &playerv1.GrantAttributePointsRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"AddExperience", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.AddExperience(ctx, &playerv1.AddExperienceRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"GrantTalentPoints", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantTalentPoints(ctx, &playerv1.GrantTalentPointsRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"GrantSkillCards", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantSkillCards(ctx, &playerv1.GrantSkillCardsRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
	}
	assertPlayerRPCMatrix(t, withCaller(callerID), commonv1.ErrCode_ERR_PERMISSION_DENY, systemOnly)
}

// TestPlayerRPCInvalidArgumentMatrix 验证内部读、系统写和带附加 ID 的自助写入口在
// 调用业务层前 fail-closed，避免零 ID/零奖励穿透到持久化层。
func TestPlayerRPCInvalidArgumentMatrix(t *testing.T) {
	svc := NewPlayerService(nil)
	internalReads := []struct {
		name string
		call playerRPCProbe
	}{
		{"GetProfile", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetProfile(ctx, &playerv1.GetProfileRequest{})
			return r.GetCode(), e
		}},
		{"ListHeroes", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ListHeroes(ctx, &playerv1.ListHeroesRequest{})
			return r.GetCode(), e
		}},
		{"GetMMR", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetMMR(ctx, &playerv1.GetMMRRequest{})
			return r.GetCode(), e
		}},
		{"GetActiveHero", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetActiveHero(ctx, &playerv1.GetActiveHeroRequest{})
			return r.GetCode(), e
		}},
		{"GetAttributes", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetAttributes(ctx, &playerv1.GetAttributesRequest{})
			return r.GetCode(), e
		}},
		{"GetEquipment", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetEquipment(ctx, &playerv1.GetEquipmentRequest{})
			return r.GetCode(), e
		}},
		{"GetTalents", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetTalents(ctx, &playerv1.GetTalentsRequest{})
			return r.GetCode(), e
		}},
		{"GetLoadout", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetLoadout(ctx, &playerv1.GetLoadoutRequest{})
			return r.GetCode(), e
		}},
		{"GetRewardClaims", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GetRewardClaims(ctx, &playerv1.GetRewardClaimsRequest{})
			return r.GetCode(), e
		}},
	}
	assertPlayerRPCMatrix(t, context.Background(), commonv1.ErrCode_ERR_INVALID_ARG, internalReads)

	invalidSystemWrites := []struct {
		name string
		call playerRPCProbe
	}{
		{"UnlockHero", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UnlockHero(ctx, &playerv1.UnlockHeroRequest{})
			return r.GetCode(), e
		}},
		{"UpdateMMR", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.UpdateMMR(ctx, &playerv1.UpdateMMRRequest{})
			return r.GetCode(), e
		}},
		{"GrantAttributePoints", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantAttributePoints(ctx, &playerv1.GrantAttributePointsRequest{})
			return r.GetCode(), e
		}},
		{"AddExperience", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.AddExperience(ctx, &playerv1.AddExperienceRequest{})
			return r.GetCode(), e
		}},
		{"GrantTalentPoints", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantTalentPoints(ctx, &playerv1.GrantTalentPointsRequest{})
			return r.GetCode(), e
		}},
		{"GrantSkillCards", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.GrantSkillCards(ctx, &playerv1.GrantSkillCardsRequest{})
			return r.GetCode(), e
		}},
	}
	assertPlayerRPCMatrix(t, context.Background(), commonv1.ErrCode_ERR_INVALID_ARG, invalidSystemWrites)

	const callerID = uint64(1001)
	invalidSelfWrites := []struct {
		name string
		call playerRPCProbe
	}{
		{"SelectHero/hero_id=0", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.SelectHero(ctx, &playerv1.SelectHeroRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
		{"ClaimReward/reward_id=0", func(ctx context.Context) (commonv1.ErrCode, error) {
			r, e := svc.ClaimReward(ctx, &playerv1.ClaimRewardRequest{PlayerId: callerID})
			return r.GetCode(), e
		}},
	}
	assertPlayerRPCMatrix(t, withCaller(callerID), commonv1.ErrCode_ERR_INVALID_ARG, invalidSelfWrites)
}
