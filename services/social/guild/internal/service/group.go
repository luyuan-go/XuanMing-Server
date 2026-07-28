// group.go 是 guild 服务里临时群(GroupService)的 gRPC service 层(2026-06-27)。
//
// 与 GuildService 同进程、同 package;复用 callerID / toProtoCode 辅助。
// 协议原则(R5):写 RPC 强制用 ctx 中的 player_id,忽略请求体 player_id 字段。
package service

import (
	"context"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	groupv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/group/v1"

	"github.com/luyuancpp/pandora/services/social/guild/internal/biz"
)

// GroupService 实现 groupv1.GroupServiceServer。
type GroupService struct {
	groupv1.UnimplementedGroupServiceServer
	uc *biz.GroupUsecase

	// group_id 独立成一个 ID 空间,与 GuildService 的 guild_id / request_id 各用各的发号器。
	// ⚠️ 三者共用同一 nodeID,发出的 ID 会逐位相同(见 etcdnode.ProvideSnowflakeN),
	// 必须各自留在自己的表 / 唯一键里。
	groupSF snowflakeGen
}

// NewGroupService 构造。groupSF 必须与 GuildService 用的两个发号器是不同实例。
func NewGroupService(uc *biz.GroupUsecase, groupSF snowflakeGen) *GroupService {
	return &GroupService{uc: uc, groupSF: groupSF}
}

// CreateGroup 建群。建群者以 JWT ctx 为准(R5)。
func (s *GroupService) CreateGroup(ctx context.Context, req *groupv1.CreateGroupRequest) (*groupv1.CreateGroupResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.CreateGroupResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	groupID, err := s.uc.CreateGroup(ctx, playerID, req.GetName(), req.GetMemberIds(), s.groupSF.Generate())
	if err != nil {
		return &groupv1.CreateGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.CreateGroupResponse{Code: commonv1.ErrCode_OK, GroupId: groupID}, nil
}

// InviteToGroup 拉人入群。操作人以 JWT ctx 为准(R5)。
func (s *GroupService) InviteToGroup(ctx context.Context, req *groupv1.InviteToGroupRequest) (*groupv1.InviteToGroupResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.InviteToGroupResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetGroupId() == 0 || req.GetTargetId() == 0 {
		return &groupv1.InviteToGroupResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.InviteToGroup(ctx, playerID, req.GetGroupId(), req.GetTargetId()); err != nil {
		return &groupv1.InviteToGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.InviteToGroupResponse{Code: commonv1.ErrCode_OK}, nil
}

// LeaveGroup 退群。player_id 以 JWT ctx 为准(R5)。
func (s *GroupService) LeaveGroup(ctx context.Context, req *groupv1.LeaveGroupRequest) (*groupv1.LeaveGroupResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.LeaveGroupResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetGroupId() == 0 {
		return &groupv1.LeaveGroupResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.LeaveGroup(ctx, playerID, req.GetGroupId()); err != nil {
		return &groupv1.LeaveGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.LeaveGroupResponse{Code: commonv1.ErrCode_OK}, nil
}

// KickFromGroup 踢人。群主以 JWT ctx 为准(R5)。
func (s *GroupService) KickFromGroup(ctx context.Context, req *groupv1.KickFromGroupRequest) (*groupv1.KickFromGroupResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.KickFromGroupResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetGroupId() == 0 || req.GetTargetId() == 0 {
		return &groupv1.KickFromGroupResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.KickFromGroup(ctx, playerID, req.GetGroupId(), req.GetTargetId()); err != nil {
		return &groupv1.KickFromGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.KickFromGroupResponse{Code: commonv1.ErrCode_OK}, nil
}

// DisbandGroup 解散群。群主以 JWT ctx 为准(R5)。
func (s *GroupService) DisbandGroup(ctx context.Context, req *groupv1.DisbandGroupRequest) (*groupv1.DisbandGroupResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.DisbandGroupResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetGroupId() == 0 {
		return &groupv1.DisbandGroupResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.DisbandGroup(ctx, playerID, req.GetGroupId()); err != nil {
		return &groupv1.DisbandGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.DisbandGroupResponse{Code: commonv1.ErrCode_OK}, nil
}

// TransferOwner 转让群主。现任群主以 JWT ctx 为准(R5)。
func (s *GroupService) TransferOwner(ctx context.Context, req *groupv1.TransferOwnerRequest) (*groupv1.TransferOwnerResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.TransferOwnerResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetGroupId() == 0 || req.GetTargetId() == 0 {
		return &groupv1.TransferOwnerResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.TransferOwner(ctx, playerID, req.GetGroupId(), req.GetTargetId()); err != nil {
		return &groupv1.TransferOwnerResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.TransferOwnerResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetGroup 查群(只读)。
func (s *GroupService) GetGroup(ctx context.Context, req *groupv1.GetGroupRequest) (*groupv1.GetGroupResponse, error) {
	if req.GetGroupId() == 0 {
		return &groupv1.GetGroupResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	g, err := s.uc.GetGroup(ctx, req.GetGroupId())
	if err != nil {
		return &groupv1.GetGroupResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.GetGroupResponse{Code: commonv1.ErrCode_OK, Group: g}, nil
}

// ListGroupMembers 列群成员(只读)。
func (s *GroupService) ListGroupMembers(ctx context.Context, req *groupv1.ListGroupMembersRequest) (*groupv1.ListGroupMembersResponse, error) {
	if req.GetGroupId() == 0 {
		return &groupv1.ListGroupMembersResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	members, err := s.uc.ListGroupMembers(ctx, req.GetGroupId())
	if err != nil {
		return &groupv1.ListGroupMembersResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.ListGroupMembersResponse{Code: commonv1.ErrCode_OK, Members: members}, nil
}

// ListMyGroups 列玩家所在群。player_id 以 JWT ctx 为准(R5)。
func (s *GroupService) ListMyGroups(ctx context.Context, _ *groupv1.ListMyGroupsRequest) (*groupv1.ListMyGroupsResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &groupv1.ListMyGroupsResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	groups, err := s.uc.ListMyGroups(ctx, playerID)
	if err != nil {
		return &groupv1.ListMyGroupsResponse{Code: toProtoCode(err)}, nil
	}
	return &groupv1.ListMyGroupsResponse{Code: commonv1.ErrCode_OK, Groups: groups}, nil
}
