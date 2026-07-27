// locator_client.go — login → player_locator gRPC 客户端封装(W3 ⑤,2026-06-05)。
//
// 设计:
//   - data 层暴露 LocationNotifier 接口,biz 只依赖接口
//   - 实际实现是 GrpcLocationNotifier,内嵌 *grpc.ClientConn + PlayerLocatorServiceClient
//   - main.go 用 pkg/grpcclient.MustDialInsecure 拨号,失败 panic
//
// 调用语义:
//   - LoginUsecase 在 sessions.Set 之后调 Notify(playerID, deviceID)
//   - 内部把 state 写成 LOGIN_PENDING(2),后续 hub DS 拿到玩家后会上报到 HUB(3)
package data

import (
	"context"
	"time"

	"google.golang.org/grpc"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// locatorProbeTimeout 是登录链上 locator 调用的独立子预算(压测审核【必修-1】)。
//
// prod 登录总 deadline 5s,MySQL/Redis 基线已占 1~2s,链上还有 matchmaker 探测与
// AssignHub;locator 慢化(GC/failover)时若无子预算,单腿就能吃光整条链让登录整体
// 超时。WithTimeout 只收紧不放宽;到期后的错误路径与原慢失败完全一致(重试/降级
// 语义由 biz 层决定,此处不改),只是把失败提前、给后续步骤留出预算。
// 取值:locator 是单键 Redis 读写,健康 P99 几十 ms,取 ~40 倍保守偏大值;待实测复核。
const locatorProbeTimeout = 2 * time.Second

// BattleLocation 是玩家当前 BATTLE 位置的最小快照(断线重连检测用,
// docs/design/battle-reconnect.md §2.1)。仅当玩家确实处于 BATTLE 态且 match_id /
// battle 地址齐全时 InBattle=true;其余情况(离线 / 大厅 / 撮合中 / locator 不可用)均为 false。
type BattleLocation struct {
	InBattle   bool
	MatchID    uint64
	BattleAddr string // battle DS 直连地址(locator 的 battle_pod 存的就是 ds_addr)
	// PresenceState is retained for conservative placement migration. MATCHING
	// and BATTLE are never interpreted as NOT_BATTLE; UNSPECIFIED is UNKNOWN.
	PresenceState locatorv1.LocationState
}

// LocationNotifier 给 login.biz 上报玩家"登录中"状态 + 查询玩家当前是否在战斗中。
// addr 未配 → main 注入 nil,biz 检查 nil 直接跳过。
type LocationNotifier interface {
	NotifyLoginPending(ctx context.Context, playerID uint64, deviceID string) error
	// GetBattleLocation 查玩家当前是否在 battle DS 中(断线重连检测)。
	// 弱依赖:locator 不可用 / 玩家不在战斗 → 返回 InBattle=false(绝不阻断登录)。
	GetBattleLocation(ctx context.Context, playerID uint64) (BattleLocation, error)
}

// GrpcLocationNotifier 实现 LocationNotifier,内嵌 grpc client。
type GrpcLocationNotifier struct {
	conn   *grpc.ClientConn
	client locatorv1.PlayerLocatorServiceClient
}

// NewGrpcLocationNotifier 用现成的 *grpc.ClientConn 包出 notifier。
//
// 调用方负责 conn 生命周期管理(main.go defer conn.Close())。
func NewGrpcLocationNotifier(conn *grpc.ClientConn) *GrpcLocationNotifier {
	return &GrpcLocationNotifier{
		conn:   conn,
		client: locatorv1.NewPlayerLocatorServiceClient(conn),
	}
}

// NotifyLoginPending 调 PlayerLocatorService.SetLocation(state=LOGIN_PENDING)。
//
// 不变量 §1 入口:这一行写完,locator 就把该 player_id 标记为"正在登录",
// 后续 hub DS 拿到玩家后改 state=HUB;客户端如果重复登录会再次刷此 key + TTL。
func (n *GrpcLocationNotifier) NotifyLoginPending(ctx context.Context, playerID uint64, deviceID string) error {
	ctx, cancel := context.WithTimeout(ctx, locatorProbeTimeout)
	defer cancel()
	req := &locatorv1.SetLocationRequest{
		PlayerId: playerID,
		Location: &locatorv1.Location{
			State: locatorv1.LocationState_LOCATION_STATE_LOGIN_PENDING,
		},
	}
	resp, err := n.client.SetLocation(ctx, req)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "locator SetLocation rpc: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "locator SetLocation code=%d", resp.GetCode())
	}
	_ = deviceID // 当前 LOGIN_PENDING 状态不需要 device_id,保留参数便于以后扩展
	return nil
}

// GetBattleLocation 调 PlayerLocatorService.GetLocation,判断玩家是否正处于 battle DS 中。
//
// 断线重连(docs/design/battle-reconnect.md §2.1):玩家在 battle DS 掉线重登时,login 据此
// 直接下发原对局的 battle DS 直连信息,而非把玩家丢回大厅。
// 只有 state==BATTLE 且 match_id!=0 且 battle_pod!="" 才认定"在战斗中";其余一律 InBattle=false。
func (n *GrpcLocationNotifier) GetBattleLocation(ctx context.Context, playerID uint64) (BattleLocation, error) {
	ctx, cancel := context.WithTimeout(ctx, locatorProbeTimeout)
	defer cancel()
	resp, err := n.client.GetLocation(ctx, &locatorv1.GetLocationRequest{PlayerId: playerID})
	if err != nil {
		return BattleLocation{}, errcode.New(errcode.ErrInternal, "locator GetLocation rpc: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return BattleLocation{}, errcode.New(errcode.Code(resp.GetCode()), "locator GetLocation code=%d", resp.GetCode())
	}
	loc := resp.GetLocation()
	if loc.GetState() != locatorv1.LocationState_LOCATION_STATE_BATTLE ||
		loc.GetMatchId() == 0 || loc.GetBattlePod() == "" {
		return BattleLocation{PresenceState: loc.GetState(), MatchID: loc.GetMatchId()}, nil
	}
	return BattleLocation{
		InBattle:      true,
		MatchID:       loc.GetMatchId(),
		BattleAddr:    loc.GetBattlePod(),
		PresenceState: loc.GetState(),
	}, nil
}
