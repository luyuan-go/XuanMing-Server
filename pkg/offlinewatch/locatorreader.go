package offlinewatch

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
)

// GrpcPresenceReader 用 player_locator gRPC client 实现 PresenceReader。
//
// 与 friend 服务那份 locator 客户端的关键区别:**这里绝不降级**。
// friend 查在线态是为了渲染好友面板,查不通就全按离线显示,最坏是列表灰一片;
// 本包的判定会导致「把玩家踢出队伍」这类不可逆动作,查不通必须原样抛错,
// 让上层 fail-closed(§9.22:不确定不得冒充 OFFLINE)。
type GrpcPresenceReader struct {
	cli locatorv1.PlayerLocatorServiceClient
}

// NewGrpcPresenceReader 用现成的 *grpc.ClientConn 包出 reader。
// 调用方负责 conn 生命周期(main.go defer conn.Close())。
func NewGrpcPresenceReader(conn *grpc.ClientConn) *GrpcPresenceReader {
	return &GrpcPresenceReader{cli: locatorv1.NewPlayerLocatorServiceClient(conn)}
}

// BatchOnline 返回此刻在 locator 有位置记录的玩家集合。
//
// 「在场即在线」:HUB / MATCHING / BATTLE / LOGIN_PENDING 都算在线。
// 尤其是 MATCHING / BATTLE —— 玩家 travel 去打比赛时位置正是这两个状态,
// 那恰恰是**最不该**按离线处理的时刻。
func (g *GrpcPresenceReader) BatchOnline(ctx context.Context, playerIDs []uint64) (map[uint64]bool, error) {
	out := make(map[uint64]bool, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	resp, err := g.cli.BatchGetLocation(ctx, &locatorv1.BatchGetLocationRequest{PlayerIds: playerIDs})
	if err != nil {
		return nil, fmt.Errorf("offlinewatch: locator BatchGetLocation: %w", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, fmt.Errorf("offlinewatch: locator BatchGetLocation code=%d", resp.GetCode())
	}
	for pid, loc := range resp.GetLocations() {
		state := loc.GetState()
		if state != locatorv1.LocationState_LOCATION_STATE_OFFLINE &&
			state != locatorv1.LocationState_LOCATION_STATE_UNSPECIFIED {
			out[pid] = true
		}
	}
	return out, nil
}

// BatchLastSeen 返回「最后一次被观测到离开 Hub 的时刻」;缺席 = UNKNOWN。
func (g *GrpcPresenceReader) BatchLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	out := make(map[uint64]int64, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	resp, err := g.cli.BatchGetLastSeen(ctx, &locatorv1.BatchGetLastSeenRequest{PlayerIds: playerIDs})
	if err != nil {
		return nil, fmt.Errorf("offlinewatch: locator BatchGetLastSeen: %w", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, fmt.Errorf("offlinewatch: locator BatchGetLastSeen code=%d", resp.GetCode())
	}
	for pid, ms := range resp.GetLastSeenMs() {
		out[pid] = ms
	}
	return out, nil
}
