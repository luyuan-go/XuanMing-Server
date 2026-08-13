// Package service 是 player 服务的 gRPC service 层(W4 ④,2026-06-06)。
//
// 职责:
//   - 实现 playerv1.PlayerServiceServer 接口
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 鉴权边界(2026-07-08 安全审查:开放客户端入口前修 IDOR,对齐 inventory 服务):
//   - 客户端自助写 RPC(改昵称 / 选英雄 / 加点 / 洗点 / 出装 / 天赋 / 领奖):以 Envoy jwt_authn
//     注入的调用者身份为准(selfPlayerID,pmw.PlayerIDFromContext),**不信任请求体 player_id**;
//     未鉴权直连或请求体 player_id 与调用者不一致直接拒,防止伪造 player_id 改他人存档。
//   - 客户端读 RPC(档案 / 属性 / 出装 / 天赋 / 出战快照 / 领奖记录):双模(resolvePlayerID)——
//     内部直连(callerID==0,如 battle_result reader / 开局快照注入)信任请求体;
//     经 Envoy 的客户端(callerID>0)强制只能查自己,不得读他人存档。
//   - 系统 RPC(UpdateMMR / UnlockHero / GrantAttributePoints / GrantTalentPoints):只允许后端
//     内部直连(callerID==0);带玩家 JWT 的客户端调用一律拒绝,并且不在 Envoy 暴露这些路由。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/rating"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/services/account/player/internal/biz"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// selfPlayerID 取经鉴权的调用者身份并校验请求体 player_id 一致性(客户端自助写 RPC 用)。
//
//	未鉴权(callerID==0,直连内网无网关注入) → ERR_UNAUTHORIZED
//	请求体 player_id 与调用者不一致           → ERR_PERMISSION_DENY
//
// 返回权威 player_id(= 调用者身份),后续业务一律用它,不信任 req.PlayerId。
// 写接口不该被后端内部直连(那类操作走系统 RPC),故未鉴权一律拒。
func selfPlayerID(ctx context.Context, reqPlayerID uint64) (uint64, commonv1.ErrCode) {
	callerID := pmw.PlayerIDFromContext(ctx)
	if callerID == 0 {
		logAuthzDeny(ctx, "self_write_unauthenticated", callerID, reqPlayerID)
		return 0, commonv1.ErrCode_ERR_UNAUTHORIZED
	}
	if reqPlayerID != 0 && reqPlayerID != callerID {
		logAuthzDeny(ctx, "self_write_player_id_mismatch", callerID, reqPlayerID)
		return 0, commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return callerID, commonv1.ErrCode_OK
}

// logAuthzDeny 记录鉴权拒绝(IDOR 尝试 / 越权调系统 RPC / 未鉴权直连写口)。
//
// 这些是安全 fail-closed 分支,却以 response Code + nil transport error 返回 → 统一
// access log 记 rpc_ok(DEBUG),线上对「伪造 player_id 改他人存档」「带玩家 JWT 调系统
// RPC」的尝试零可见性。本文件大段注释都在防 IDOR,拒绝事件必须可审计。
func logAuthzDeny(ctx context.Context, reason string, callerID, reqPlayerID uint64) {
	plog.With(ctx).Warnw("msg", "player_authz_denied",
		"reason", reason, "caller_id", callerID, "req_player_id", reqPlayerID)
}

// resolvePlayerID 读接口双模取权威 player_id:
//
//	内部直连(callerID==0):信任请求体 player_id(内部 reader / 开局快照注入),body==0 → ERR_INVALID_ARG
//	客户端(callerID>0):强制只能查自己,body 与调用者不一致 → ERR_PERMISSION_DENY
//
// 既不破坏未来内部 reader / 快照注入调用,又杜绝客户端读他人存档。
func resolvePlayerID(ctx context.Context, reqPlayerID uint64) (uint64, commonv1.ErrCode) {
	callerID := pmw.PlayerIDFromContext(ctx)
	if callerID == 0 {
		if reqPlayerID == 0 {
			return 0, commonv1.ErrCode_ERR_INVALID_ARG
		}
		return reqPlayerID, commonv1.ErrCode_OK
	}
	if reqPlayerID != 0 && reqPlayerID != callerID {
		// 客户端试图读他人存档(IDOR 探测)。
		logAuthzDeny(ctx, "read_other_player_denied", callerID, reqPlayerID)
		return 0, commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return callerID, commonv1.ErrCode_OK
}

// systemOnly 系统接口鉴权:经 Envoy 的客户端(callerID>0)一律拒,合法调用者是后端内部直连。
func systemOnly(ctx context.Context) commonv1.ErrCode {
	if callerID := pmw.PlayerIDFromContext(ctx); callerID != 0 {
		// 带玩家 JWT 调系统 RPC(UpdateMMR / Grant* / UnlockHero / AddExperience 等)= 越权尝试。
		logAuthzDeny(ctx, "system_rpc_by_client", callerID, 0)
		return commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return commonv1.ErrCode_OK
}

// PlayerService 实现 playerv1.PlayerServiceServer。
type PlayerService struct {
	playerv1.UnimplementedPlayerServiceServer
	uc *biz.PlayerUsecase

	// dsGuard DS 回调令牌守卫;nil = 未启用(mode=off),Check 直接放行。
	// 只有 GetLoadout 用它:见该方法注释,它是本服务里唯一挂在 Envoy DS 面上的 RPC。
	dsGuard *pmw.DSCallbackGuard
}

// NewPlayerService 构造。
func NewPlayerService(uc *biz.PlayerUsecase) *PlayerService {
	return &PlayerService{uc: uc}
}

// SetDSCallbackGuard 注入 DS 回调令牌守卫(main 接线;nil 等价 mode=off)。
func (s *PlayerService) SetDSCallbackGuard(g *pmw.DSCallbackGuard) { s.dsGuard = g }

// GetProfile 读玩家档案(懒创建)。客户端只能读自己;内部直连信任请求体。
func (s *PlayerService) GetProfile(ctx context.Context, req *playerv1.GetProfileRequest) (*playerv1.GetProfileResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetProfileResponse{Code: code}, nil
	}
	profile, err := s.uc.GetProfile(ctx, playerID)
	if err != nil {
		return &playerv1.GetProfileResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GetProfileResponse{Code: commonv1.ErrCode_OK, Profile: profile}, nil
}

// UpdateNickname 改昵称。以调用者身份为准。
func (s *PlayerService) UpdateNickname(ctx context.Context, req *playerv1.UpdateNicknameRequest) (*playerv1.UpdateNicknameResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.UpdateNicknameResponse{Code: code}, nil
	}
	if err := s.uc.UpdateNickname(ctx, playerID, req.GetNickname()); err != nil {
		return &playerv1.UpdateNicknameResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.UpdateNicknameResponse{Code: commonv1.ErrCode_OK}, nil
}

// ListHeroes 列出玩家已解锁英雄。客户端只能读自己。
func (s *PlayerService) ListHeroes(ctx context.Context, req *playerv1.ListHeroesRequest) (*playerv1.ListHeroesResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.ListHeroesResponse{Code: code}, nil
	}
	heroes, err := s.uc.ListHeroes(ctx, playerID)
	if err != nil {
		return &playerv1.ListHeroesResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.ListHeroesResponse{Code: commonv1.ErrCode_OK, HeroIds: heroes}, nil
}

// UnlockHero 解锁英雄(系统接口:购买 / 奖励到账后后端内部调;客户端不得自助解锁)。
func (s *PlayerService) UnlockHero(ctx context.Context, req *playerv1.UnlockHeroRequest) (*playerv1.UnlockHeroResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.UnlockHeroResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 || req.GetHeroId() == 0 {
		return &playerv1.UnlockHeroResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.UnlockHero(ctx, req.GetPlayerId(), req.GetHeroId(), req.GetSource()); err != nil {
		return &playerv1.UnlockHeroResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.UnlockHeroResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetMMR 读玩家在某段位池下的分(供 battle_result 当 reader;客户端只能读自己)。
// rating_pool 空 = 默认池。
func (s *PlayerService) GetMMR(ctx context.Context, req *playerv1.GetMMRRequest) (*playerv1.GetMMRResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetMMRResponse{Code: code}, nil
	}
	mmr, found, err := s.uc.GetMMR(ctx, playerID, req.GetRatingPool())
	if err != nil {
		return &playerv1.GetMMRResponse{Code: toProtoCode(err)}, nil
	}
	// 回显归一后的池 + found:调用方靠 found 区分"没打过"与"分刚好是基线"。
	return &playerv1.GetMMRResponse{Code: commonv1.ErrCode_OK, Mmr: int32(mmr),
		RatingPool: rating.Normalize(req.GetRatingPool()), Found: found}, nil
}

// UpdateMMR 幂等改 MMR(系统接口;同步兜底,正常链路走 kafka 消费 player.update)。
func (s *PlayerService) UpdateMMR(ctx context.Context, req *playerv1.UpdateMMRRequest) (*playerv1.UpdateMMRResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.UpdateMMRResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 {
		return &playerv1.UpdateMMRResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if req.GetIdempotencyKey() == "" {
		return &playerv1.UpdateMMRResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	newMMR, _, err := s.uc.UpdateMMR(ctx, req.GetPlayerId(), req.GetDelta(), req.GetReason(),
		req.GetIdempotencyKey(), req.GetRatingPool())
	if err != nil {
		return &playerv1.UpdateMMRResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.UpdateMMRResponse{Code: commonv1.ErrCode_OK, NewMmr: int32(newMMR)}, nil
}

// ── 出战养成 ──────────────────────────────────────────────────────────────────

// SelectHero 设定出战英雄。以调用者身份为准。
func (s *PlayerService) SelectHero(ctx context.Context, req *playerv1.SelectHeroRequest) (*playerv1.SelectHeroResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.SelectHeroResponse{Code: code}, nil
	}
	if req.GetHeroId() == 0 {
		return &playerv1.SelectHeroResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.SelectHero(ctx, playerID, req.GetHeroId()); err != nil {
		return &playerv1.SelectHeroResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.SelectHeroResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetActiveHero 读出战英雄(未选定返回 hero_id=0)。客户端只能读自己。
func (s *PlayerService) GetActiveHero(ctx context.Context, req *playerv1.GetActiveHeroRequest) (*playerv1.GetActiveHeroResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetActiveHeroResponse{Code: code}, nil
	}
	heroID, err := s.uc.GetActiveHero(ctx, playerID)
	if err != nil {
		return &playerv1.GetActiveHeroResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GetActiveHeroResponse{Code: commonv1.ErrCode_OK, HeroId: heroID}, nil
}

// GrantAttributePoints 幂等授予可分配点(系统接口:升级 / 活动授予;客户端不得自助)。
func (s *PlayerService) GrantAttributePoints(ctx context.Context, req *playerv1.GrantAttributePointsRequest) (*playerv1.GrantAttributePointsResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.GrantAttributePointsResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 {
		return &playerv1.GrantAttributePointsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	unspent, err := s.uc.GrantAttributePoints(ctx, req.GetPlayerId(), req.GetPoints(), req.GetIdempotencyKey())
	if err != nil {
		return &playerv1.GrantAttributePointsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GrantAttributePointsResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// AddExperience 幂等入账经验并结算等级(实时成长)。系统 RPC:只允许后端内部直连
// (battle_result progress 出箱 worker / 任务完成点 / GM),带玩家 JWT 一律拒,不在 Envoy 暴露。
func (s *PlayerService) AddExperience(ctx context.Context, req *playerv1.AddExperienceRequest) (*playerv1.AddExperienceResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.AddExperienceResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 || req.GetExpDelta() == 0 || req.GetIdempotencyKey() == "" {
		return &playerv1.AddExperienceResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	st, already, err := s.uc.AddExperience(ctx, req.GetPlayerId(), req.GetExpDelta(), req.GetReason(), req.GetIdempotencyKey())
	if err != nil {
		return &playerv1.AddExperienceResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.AddExperienceResponse{
		Code:         commonv1.ErrCode_OK,
		Level:        st.Level,
		ExpInLevel:   st.ExpInLevel,
		IsMaxLevel:   st.IsMaxLevel,
		LevelsGained: st.LevelsGained,
		Already:      already,
	}, nil
}

// AllocateAttributePoints 分配属性点。以调用者身份为准。
func (s *PlayerService) AllocateAttributePoints(ctx context.Context, req *playerv1.AllocateAttributePointsRequest) (*playerv1.AllocateAttributePointsResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.AllocateAttributePointsResponse{Code: code}, nil
	}
	allocs := make([]data.AttrAllocation, 0, len(req.GetAllocations()))
	for _, a := range req.GetAllocations() {
		allocs = append(allocs, data.AttrAllocation{Key: a.GetAttrKey(), Points: a.GetPoints()})
	}
	unspent, err := s.uc.AllocateAttributePoints(ctx, playerID, allocs)
	if err != nil {
		return &playerv1.AllocateAttributePointsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.AllocateAttributePointsResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// ResetAttributes 洗点。以调用者身份为准。
func (s *PlayerService) ResetAttributes(ctx context.Context, req *playerv1.ResetAttributesRequest) (*playerv1.ResetAttributesResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.ResetAttributesResponse{Code: code}, nil
	}
	unspent, err := s.uc.ResetAttributes(ctx, playerID)
	if err != nil {
		return &playerv1.ResetAttributesResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.ResetAttributesResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// GetAttributes 读已分配属性点 + 未分配点。客户端只能读自己。
func (s *PlayerService) GetAttributes(ctx context.Context, req *playerv1.GetAttributesRequest) (*playerv1.GetAttributesResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetAttributesResponse{Code: code}, nil
	}
	attrs, unspent, err := s.uc.GetAttributes(ctx, playerID)
	if err != nil {
		return &playerv1.GetAttributesResponse{Code: toProtoCode(err)}, nil
	}
	out := make([]*playerv1.AttributeAllocation, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, &playerv1.AttributeAllocation{AttrKey: a.Key, Points: a.Points})
	}
	return &playerv1.GetAttributesResponse{Code: commonv1.ErrCode_OK, Attributes: out, UnspentPoints: int32(unspent)}, nil
}

// SetEquipment 全量替换出战装备预设。以调用者身份为准。
func (s *PlayerService) SetEquipment(ctx context.Context, req *playerv1.SetEquipmentRequest) (*playerv1.SetEquipmentResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.SetEquipmentResponse{Code: code}, nil
	}
	slots := toDataEquipment(req.GetEquipment())
	if err := s.uc.SetEquipment(ctx, playerID, slots); err != nil {
		return &playerv1.SetEquipmentResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.SetEquipmentResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetEquipment 读出战装备预设。客户端只能读自己。
func (s *PlayerService) GetEquipment(ctx context.Context, req *playerv1.GetEquipmentRequest) (*playerv1.GetEquipmentResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetEquipmentResponse{Code: code}, nil
	}
	slots, err := s.uc.GetEquipment(ctx, playerID)
	if err != nil {
		return &playerv1.GetEquipmentResponse{Code: toProtoCode(err)}, nil
	}
	out := toProtoEquipment(slots)
	return &playerv1.GetEquipmentResponse{Code: commonv1.ErrCode_OK, Equipment: out}, nil
}

func toDataEquipment(in []*playerv1.LoadoutEquipment) []data.EquipmentSlot {
	out := make([]data.EquipmentSlot, 0, len(in))
	for _, e := range in {
		out = append(out, data.EquipmentSlot{
			Slot: e.GetSlot(), ItemConfigID: e.GetItemConfigId(), InstanceID: e.GetInstanceId(),
		})
	}
	return out
}

func toProtoEquipment(in []data.EquipmentSlot) []*playerv1.LoadoutEquipment {
	out := make([]*playerv1.LoadoutEquipment, 0, len(in))
	for _, e := range in {
		out = append(out, &playerv1.LoadoutEquipment{
			Slot: e.Slot, ItemConfigId: e.ItemConfigID, InstanceId: e.InstanceID,
		})
	}
	return out
}

// GrantTalentPoints 幂等授予天赋点(系统接口:升级 / 活动授予;客户端不得自助)。
func (s *PlayerService) GrantTalentPoints(ctx context.Context, req *playerv1.GrantTalentPointsRequest) (*playerv1.GrantTalentPointsResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &playerv1.GrantTalentPointsResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 {
		return &playerv1.GrantTalentPointsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	unspent, err := s.uc.GrantTalentPoints(ctx, req.GetPlayerId(), req.GetPoints(), req.GetIdempotencyKey())
	if err != nil {
		return &playerv1.GrantTalentPointsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GrantTalentPointsResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// SetTalents 全量重置天赋分配。以调用者身份为准。
func (s *PlayerService) SetTalents(ctx context.Context, req *playerv1.SetTalentsRequest) (*playerv1.SetTalentsResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.SetTalentsResponse{Code: code}, nil
	}
	talents := make([]data.TalentLevel, 0, len(req.GetTalents()))
	for _, t := range req.GetTalents() {
		talents = append(talents, data.TalentLevel{TalentID: t.GetTalentId(), Level: t.GetLevel()})
	}
	unspent, err := s.uc.SetTalents(ctx, playerID, talents)
	if err != nil {
		return &playerv1.SetTalentsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.SetTalentsResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// ResetTalents 清空天赋分配。以调用者身份为准。
func (s *PlayerService) ResetTalents(ctx context.Context, req *playerv1.ResetTalentsRequest) (*playerv1.ResetTalentsResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.ResetTalentsResponse{Code: code}, nil
	}
	unspent, err := s.uc.ResetTalents(ctx, playerID)
	if err != nil {
		return &playerv1.ResetTalentsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.ResetTalentsResponse{Code: commonv1.ErrCode_OK, UnspentPoints: int32(unspent)}, nil
}

// GetTalents 读已点天赋 + 可点天赋点。客户端只能读自己。
func (s *PlayerService) GetTalents(ctx context.Context, req *playerv1.GetTalentsRequest) (*playerv1.GetTalentsResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetTalentsResponse{Code: code}, nil
	}
	talents, unspent, err := s.uc.GetTalents(ctx, playerID)
	if err != nil {
		return &playerv1.GetTalentsResponse{Code: toProtoCode(err)}, nil
	}
	out := make([]*playerv1.TalentNode, 0, len(talents))
	for _, t := range talents {
		out = append(out, &playerv1.TalentNode{TalentId: t.TalentID, Level: t.Level})
	}
	return &playerv1.GetTalentsResponse{Code: commonv1.ErrCode_OK, Talents: out, UnspentPoints: int32(unspent)}, nil
}

// GetLoadout 组装开战前快照(出战英雄 + 属性点 + 装备预设 + 天赋)。
// 双模:内部(DS 开局快照注入,callerID==0)信请求体;客户端只能读自己。
//
// **本方法是 player 服务里唯一挂在 Envoy DS 面(:8444)上的 RPC**(2026-08-13 补路由,
// 供 Hub/Battle DS 在 PlayerState 初始化时拉专精与预设装备)。那个监听器没有 jwt_authn,
// 经它进来的调用 callerID 恒为 0 —— 于是 resolvePlayerID 的「callerID==0 即后端内部可信,
// 信请求体 player_id」在本方法上不再成立:任何能连到 :8444、或绕过 Envoy 直连业务端口
// 20002 的进程,都能拿到任意玩家的出战快照(装备实例 ID / 词条 / 天赋)。
// 故 callerID==0 这一支必须自证是 DS,不能只靠「没带玩家 JWT」。
//
// 只在 callerID==0 时过门:带玩家 JWT 的客户端(自助查自己的出战快照,走 :8443)其
// authorization 头里是**玩家** JWT,拿去 VerifyDSCallback 必然验签失败 —— 无条件过门
// 会把正常客户端一起拒掉。这条分支纪律有回归测试钉死。
//
// scope 与 team.GetPlayerTeam / guild.GetPlayerGuild 逐条同构:
//   - RequireToken:全仓无内部 Go 调用方(已 grep 确认),故直连无令牌也拒,
//     堵住「绕过 Envoy 直连业务端口、无标记无令牌却被当内部东西向信任」的旁路;
//   - 不绑 Type / Pod / MatchID:Hub 与 Battle DS 都在进场时查这一次,与哪台 DS、哪一局无关。
//
// 档位(2026-08-13 起 player 已在 DS 回调密钥权威清单内,本门是真门不是纸面门):
// dev=permissive(验签与范围校验全跑,失败只 warn 放行,便于在切 enforce 前拿到真实证据),
// prod=enforce(无有效令牌一律拒)。UE 侧对应契约:GetLoadout 属
// IsPlayerLoadoutMethod 的 hub|battle 双类型 Active 方法,DS 本来就带 Bearer 令牌发这条请求。
func (s *PlayerService) GetLoadout(ctx context.Context, req *playerv1.GetLoadoutRequest) (*playerv1.GetLoadoutResponse, error) {
	if pmw.PlayerIDFromContext(ctx) == 0 {
		if err := s.dsGuard.Check(ctx, pmw.DSScope{RequireToken: true}); err != nil {
			return &playerv1.GetLoadoutResponse{Code: toProtoCode(err)}, nil
		}
	}
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetLoadoutResponse{Code: code}, nil
	}
	loadout, err := s.uc.GetLoadout(ctx, playerID)
	if err != nil {
		return &playerv1.GetLoadoutResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GetLoadoutResponse{Code: commonv1.ErrCode_OK, Loadout: loadout}, nil
}

// ── 领奖 ──────────────────────────────────────────────────────────────────────

// ClaimReward 领取一档奖励(客户端权威领取,幂等;已领取返回 ERR_REWARD_ALREADY_CLAIMED)。以调用者身份为准。
func (s *PlayerService) ClaimReward(ctx context.Context, req *playerv1.ClaimRewardRequest) (*playerv1.ClaimRewardResponse, error) {
	playerID, code := selfPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.ClaimRewardResponse{Code: code}, nil
	}
	if req.GetRewardId() == 0 {
		return &playerv1.ClaimRewardResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.ClaimReward(ctx, playerID, req.GetSourceType(), req.GetSource(), req.GetActivityInstanceId(), req.GetRewardId()); err != nil {
		return &playerv1.ClaimRewardResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.ClaimRewardResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetRewardClaims 查询某来源已领取的奖励配置 ID 列表(客户端可见最小视图)。客户端只能读自己。
func (s *PlayerService) GetRewardClaims(ctx context.Context, req *playerv1.GetRewardClaimsRequest) (*playerv1.GetRewardClaimsResponse, error) {
	playerID, code := resolvePlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &playerv1.GetRewardClaimsResponse{Code: code}, nil
	}
	ids, err := s.uc.GetRewardClaims(ctx, playerID, req.GetSourceType(), req.GetSource(), req.GetActivityInstanceId())
	if err != nil {
		return &playerv1.GetRewardClaimsResponse{Code: toProtoCode(err)}, nil
	}
	return &playerv1.GetRewardClaimsResponse{Code: commonv1.ErrCode_OK, ClaimedRewardIds: ids}, nil
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
