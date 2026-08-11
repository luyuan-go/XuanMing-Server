// granter.go — 发奖下游 gRPC 客户端(docs/design/mission.md §6)。
//
// 接线对齐 leaderboard reward_client / battle_result mail_client:内网 insecure 直连,
// 无 JWT(GrantItems / GrantInstances / AddExperience 都是系统 RPC,只认 callerID==0)。
// 幂等键由 biz 层给定(stack / inst / quest 三通道分键,见 biz/reward.go 文件头)。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
	mailv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mail/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
)

// ── inventory(道具 / 装备)───────────────────────────────────────────────────

// GrpcItemGranter 实现 biz.ItemGranter。
type GrpcItemGranter struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// NewGrpcItemGranter 直连 inventory 服务(host:port,内网 insecure)。
func NewGrpcItemGranter(addr string) *GrpcItemGranter {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcItemGranter{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcItemGranter) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// GrantItems 幂等发放可堆叠道具。
func (g *GrpcItemGranter) GrantItems(ctx context.Context, playerID uint64, idemKey string, items []biz.RewardItem) error {
	grants := make([]*inventoryv1.ItemGrant, 0, len(items))
	for _, it := range items {
		if it.Count == 0 {
			continue
		}
		grants = append(grants, &inventoryv1.ItemGrant{ItemConfigId: it.ItemConfigID, Count: int64(it.Count)})
	}
	if len(grants) == 0 {
		return nil
	}
	resp, err := g.cli.GrantItems(ctx, &inventoryv1.GrantItemsRequest{
		PlayerId: playerID, Items: grants, IdempotencyKey: idemKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"grant items player=%d key=%s code=%d", playerID, idemKey, int32(resp.GetCode()))
	}
	return nil
}

// GrantInstances 幂等发放装备实例;背包满返回 capacityFull=true(调用方转邮件)。
func (g *GrpcItemGranter) GrantInstances(ctx context.Context, playerID uint64, idemKey string, itemConfigIDs []uint32) (bool, error) {
	if len(itemConfigIDs) == 0 {
		return false, nil
	}
	resp, err := g.cli.GrantInstances(ctx, &inventoryv1.GrantInstancesRequest{
		PlayerId: playerID, ItemConfigIds: itemConfigIDs, IdempotencyKey: idemKey,
	})
	if err != nil {
		return false, err
	}
	switch resp.GetCode() {
	case commonv1.ErrCode_OK:
		return false, nil
	case commonv1.ErrCode_ERR_INVENTORY_CAPACITY_FULL:
		return true, nil
	default:
		return false, errcode.New(errcode.Code(resp.GetCode()),
			"grant instances player=%d key=%s code=%d", playerID, idemKey, int32(resp.GetCode()))
	}
}

// ── player(经验)────────────────────────────────────────────────────────────

// GrpcExpGranter 实现 biz.ExpGranter(player.AddExperience,reason="quest")。
type GrpcExpGranter struct {
	conn *grpc.ClientConn
	cli  playerv1.PlayerServiceClient
}

// NewGrpcExpGranter 直连 player 服务。
func NewGrpcExpGranter(addr string) *GrpcExpGranter {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcExpGranter{conn: conn, cli: playerv1.NewPlayerServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcExpGranter) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// AddExperience 幂等入账经验(幂等命中 already 也视为成功)。
func (g *GrpcExpGranter) AddExperience(ctx context.Context, playerID uint64, expDelta uint64, idemKey string) error {
	resp, err := g.cli.AddExperience(ctx, &playerv1.AddExperienceRequest{
		PlayerId: playerID, ExpDelta: expDelta, Reason: "quest", IdempotencyKey: idemKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"add experience player=%d key=%s code=%d", playerID, idemKey, int32(resp.GetCode()))
	}
	return nil
}

// ── mail(满包溢出转邮件)─────────────────────────────────────────────────────

// 溢出邮件文案(对齐 battle_result 内置最小可用文案)。
const (
	overflowMailTitle = "任务奖励"
	overflowMailBody  = "背包已满,任务奖励的装备已放入邮件,请清理背包后领取。"
)

// GrpcOverflowMailSender 实现 biz.OverflowMailSender。
type GrpcOverflowMailSender struct {
	conn *grpc.ClientConn
	cli  mailv1.MailServiceClient
}

// NewGrpcOverflowMailSender 直连 mail 服务。
func NewGrpcOverflowMailSender(addr string) *GrpcOverflowMailSender {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcOverflowMailSender{conn: conn, cli: mailv1.NewMailServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcOverflowMailSender) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// SendOverflowMail 把满包装备按 config_id 聚合成 instance 附件发个人邮件。
// grantKey 传直发链同款 inst 键作 instance_grant_key:邮件领取走 GrantInstances 用
// 同键去重 → 直发链与邮件链至多一次(battle_result mail_client 同款)。
func (g *GrpcOverflowMailSender) SendOverflowMail(ctx context.Context, playerID uint64, itemConfigIDs []uint32, grantKey string) error {
	if len(itemConfigIDs) == 0 {
		return nil
	}
	order := make([]uint32, 0, len(itemConfigIDs))
	counts := make(map[uint32]uint32, len(itemConfigIDs))
	for _, id := range itemConfigIDs {
		if _, ok := counts[id]; !ok {
			order = append(order, id)
		}
		counts[id]++
	}
	atts := make([]*mailv1.MailAttachment, 0, len(order))
	for _, id := range order {
		atts = append(atts, &mailv1.MailAttachment{
			Body: &mailv1.MailAttachment_Instance{
				Instance: &mailv1.InstanceAttachment{ItemConfigId: id, Count: counts[id]},
			},
		})
	}
	resp, err := g.cli.SendPersonalMail(ctx, &mailv1.SendPersonalMailRequest{
		ToPlayerId: playerID, Title: overflowMailTitle, Body: overflowMailBody,
		Attachments: atts, InstanceGrantKey: grantKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "overflow mail player=%d code=%d", playerID, resp.GetCode())
	}
	return nil
}
