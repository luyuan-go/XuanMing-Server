// ds_allocator.go 实现 biz.DSAllocator:通过 gRPC 调 ds_allocator 服务申请战斗 DS,
// 并在 matchmaker 侧为每个玩家签发 battle DSTicket(JWT,不变量 §3 短时效 5min)。
//
// 设计(W4 ②,2026-06-06):
//   - ds_allocator 服务只负责"拉一个 DS pod"并返回 ds_addr / pod_name,不签票据
//     (战斗结果 MMR 在 battle_result 算,DS 不可信,不变量 §6;票据由可信后端签)
//   - DSTicket 由 matchmaker 用 pkg/auth.Signer 签(dsType=battle + match_id),
//     客户端拿票据连 DS,DS 转交后端校验
package data

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// GrpcDSAllocator 用 ds_allocator 服务 gRPC client 实现 biz.DSAllocator。
type GrpcDSAllocator struct {
	conn   *grpc.ClientConn
	cli    dsv1.DSAllocatorServiceClient
	signer *auth.Signer
	// v2 非 nil 时启用 DSTicket v2(RS256,方案 B):battle 票绑死 DS 实例
	// (ds_uid / ds_instance_epoch / allocation_id),不再签 legacy HS256 票。
	v2        *auth.DSTicketSigner
	abortAuth *internalrpcauth.Signer
	mapID     uint32
	gameMode  string
	// sessGate 会话现行性权威只读视图(R7 复审 P0-2,SetSessionGate 注入)。非 nil 时
	// READY 批签的每张 battle 票都携带该玩家当前会话 jti(sjti claim),Login 兑换点
	// 复核现行性——被新登录顶掉的旧设备即使还留着 READY 推送的票也无法入场。
	sessGate sessiongate.Gate
	// tables 配置表快照容器(SetConfigTables 注入,可为 nil = 未启用配置表的 dev 口径)。
	// 本层只读一列:关卡表 rating_mode —— 本局「算不算段位」在**发出 AllocateBattle 那一刻**
	// 按 map_id 定格进请求,此后由 allocator 存进 canonical BattleStorageRecord。
	// 与 biz.SetConfigTables 同型:每次经 Tables() 取当前批次,天然读到热更后的表。
	tables *configtable.Store
}

// SetSessionGate 注入会话现行性权威(启动期、撮合循环开跑前调用;nil = dev 无权威)。
func (g *GrpcDSAllocator) SetSessionGate(gate sessiongate.Gate) {
	g.sessGate = gate
}

// SetConfigTables 注入配置表容器(启动期,可选)。与 biz.MatchUsecase.SetConfigTables 同型:
// 用 setter 而非构造参数,避免未启用配置表的调用点被迫改签名。
func (g *GrpcDSAllocator) SetConfigTables(s *configtable.Store) {
	g.tables = s
}

// ratingModeForMap 取本局计分模式(关卡表 rating_mode 列)。
//
// 这是「算不算段位」的定格点:结果写进 AllocateBattleRequest → canonical
// BattleStorageRecord,battle_result 结算只认那个定格值,不在结算那一刻重查表
// (热更改本列会改写正在打的那一局的规则)。
//
// **拿不到就返回 UNSPECIFIED(0),绝不猜 ELO**:未启用配置表 / 表未加载 / 行不存在时,
// 0 让 battle_result 回落到本列上线前的旧口径(canonical pve_coop 不计分、其余算 Elo),
// 与不带本字段的旧 matchmaker 行为逐字节一致(§9.21 共存窗口双向兼容)。
// 猜 ELO 会给合作副本玩家扣段位,而改段位不可逆——方向必须偏向"少算",不能偏向"乱算"。
func (g *GrpcDSAllocator) ratingModeForMap(mapID uint32) configpb.LevelRatingMode {
	if g.tables == nil {
		return configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED
	}
	tb := g.tables.Tables()
	if tb == nil {
		return configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED
	}
	row, ok := tb.Level.ByID(mapID)
	if !ok {
		return configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED
	}
	return row.GetRatingMode()
}

// NewGrpcDSAllocator 直连 ds_allocator 服务 endpoint(host:port,内网 insecure)。
// signer 用于给每个玩家签 battle DSTicket(v2Signer 非 nil 时改签 v2 实例绑定票);
// mapID / gameMode 透传给 ds_allocator。
// allocateTimeout 是 AllocateBattle 的客户端超时(服务端阻塞等 DS ready 心跳,
// 需覆盖 agones allocate + ready_wait 预算,不能用 15s 默认值);≤0 时用 grpcclient 默认。
func NewGrpcDSAllocator(dsAllocatorAddr string, signer *auth.Signer, v2Signer *auth.DSTicketSigner,
	abortAuth *internalrpcauth.Signer, mapID uint32, gameMode string, allocateTimeout time.Duration,
) *GrpcDSAllocator {
	conn := grpcclient.MustDialInsecureTimeout(dsAllocatorAddr, allocateTimeout)
	return &GrpcDSAllocator{
		conn:      conn,
		cli:       dsv1.NewDSAllocatorServiceClient(conn),
		signer:    signer,
		v2:        v2Signer,
		abortAuth: abortAuth,
		mapID:     mapID,
		gameMode:  gameMode,
	}
}

// AbortBattleAllocation invokes the allocator's destructive compensation RPC
// with a fresh nonce and a signature over the canonical full request. It does
// not reuse player JWT, Login resume, or DS callback keys.
func (g *GrpcDSAllocator) AbortBattleAllocation(
	ctx context.Context,
	matchID uint64,
	operationID string,
	allocation *model.BattleAllocation,
) error {
	if g.abortAuth == nil || allocation == nil {
		return errcode.New(errcode.ErrUnavailable,
			"battle allocation abort service auth unavailable for match %d", matchID)
	}
	request := battleabort.Request{MatchID: matchID, OperationID: operationID, Target: allocation.Target}
	if !request.Complete() {
		return errcode.New(errcode.ErrInvalidArg,
			"complete battle allocation abort tuple required for match %d", matchID)
	}
	signedCtx, err := g.abortAuth.SignContextWithPayload(ctx,
		dsv1.DSAllocatorService_AbortPreactiveBattle_FullMethodName,
		matchID, request.Canonical())
	if err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"sign battle allocation abort for match %d", matchID)
	}
	resp, err := g.cli.AbortPreactiveBattle(signedCtx, &dsv1.AbortPreactiveBattleRequest{
		MatchId: matchID, AllocationOperationId: operationID,
		DsPodName: allocation.Target.PodName, GameserverUid: allocation.Target.InstanceUID,
		InstanceEpoch: allocation.Target.InstanceEpoch, AllocationId: allocation.Target.AllocationID,
		ReleaseTrack: allocation.Target.ReleaseTrack,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"ds_allocator abort returned code=%d for match %d", resp.GetCode(), matchID)
	}
	return nil
}

// Close 关闭底层连接。
func (g *GrpcDSAllocator) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// AllocateBattle 调 ds_allocator.AllocateBattle 拉战斗 DS,再为每个玩家签 battle DSTicket。
// mapID 为本局副本编号(来自 match 记录):非 0 时按局透传给 ds_allocator 选副本地图;
// 为 0(旧客户端 / 未选)时回退到静态默认 g.mapID,保持向后兼容。
func (g *GrpcDSAllocator) AllocateBattle(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32) (*model.BattleAllocation, error) {
	return g.allocateBattle(ctx, matchID, playerIDs, nil, mapID)
}

// AllocateBattleWithCombatFactions 把 matchmaker 权威 MatchMember.side 完整下发给 allocator。
// 映射按 player_id canonical 化；多个玩家/队伍可共享 faction，也允许 faction>1。
func (g *GrpcDSAllocator) AllocateBattleWithCombatFactions(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
) (*model.BattleAllocation, error) {
	canonicalPlayers, _, err := dsmetadata.CanonicalCombatFactions(playerIDs, combatFactionByPlayer)
	if err != nil {
		return nil, errcode.New(errcode.ErrInvalidArg, "invalid combat factions: %v", err)
	}
	factions := make([]*dsv1.BattlePlayerCombatFaction, 0, len(canonicalPlayers))
	for _, playerID := range canonicalPlayers {
		factions = append(factions, &dsv1.BattlePlayerCombatFaction{
			PlayerId: playerID, CombatFactionId: combatFactionByPlayer[playerID],
		})
	}
	return g.allocateBattle(ctx, matchID, canonicalPlayers, factions, mapID)
}

func (g *GrpcDSAllocator) allocateBattle(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	combatFactions []*dsv1.BattlePlayerCombatFaction,
	mapID uint32,
) (*model.BattleAllocation, error) {
	effectiveMapID := mapID
	if effectiveMapID == 0 {
		effectiveMapID = g.mapID
	}
	// 计分模式按 effectiveMapID 定格(与 game_mode / map_id 同一时刻、同一请求)。
	ratingMode := g.ratingModeForMap(effectiveMapID)
	if ratingMode == configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED {
		// 战斗类关卡的本列应当显式填。落到这里只有两种情况:滚动升级期还在用旧批次表,
		// 或策划漏填这一列——两者都会让本局回落旧口径(pve_coop 不计分 / 其余算 Elo)。
		// 是"要去查表"的信号,不是错误:分配照常继续(§15.2 不为观测新增失败模式)。
		plog.With(ctx).Warnw("msg", "battle_rating_mode_unconfigured",
			"match_id", matchID, "map_id", effectiveMapID, "game_mode", g.gameMode,
			"hint", "关卡表 g_关卡.xlsx「计分模式」列未填,本局按旧口径结算(pve_coop 不计分 / 其余算 Elo)")
	}
	resp, err := g.cli.AllocateBattle(ctx, &dsv1.AllocateBattleRequest{
		MatchId:              matchID,
		PlayerIds:            playerIDs,
		MapId:                effectiveMapID,
		GameMode:             g.gameMode,
		PlayerCombatFactions: combatFactions,
		RatingMode:           ratingMode,
	})
	if err != nil {
		return nil, err
	}
	// Preserve the allocator's authoritative error classification. In particular,
	// ERR_UNAVAILABLE means the external allocation result is UNKNOWN (for
	// example the commit/fence response was lost). Collapsing it to
	// ErrDSAllocationFailed lets the match worker mark the match FAILED and
	// requeue players while a Battle DS may already exist.
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, errcode.New(errcode.Code(resp.GetCode()),
			"ds_allocator returned code=%d for match %d", resp.GetCode(), matchID)
	}
	if resp.GetDsAddr() == "" {
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"ds_allocator returned OK with empty addr for match %d", matchID)
	}
	target, terr := battleTargetFromResponse(resp, matchID)
	if terr != nil {
		return nil, terr
	}
	allocation := &model.BattleAllocation{
		Address: resp.GetDsAddr(),
		Target: placement.Target{
			PodName:       target.DSPodName,
			InstanceUID:   target.DSInstanceUID,
			InstanceEpoch: target.DSInstanceEpoch,
			AllocationID:  target.AllocationID,
			ReleaseTrack:  target.ReleaseTrack,
		},
	}
	return allocation, nil
}

func (g *GrpcDSAllocator) SignBattleTickets(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	allocation *model.BattleAllocation,
) (map[uint64]string, error) {
	tickets := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		token, err := g.SignBattleTicket(ctx, playerID, matchID, allocation)
		if err != nil {
			return nil, err
		}
		tickets[playerID] = token
	}
	return tickets, nil
}

// battleTargetFromResponse 从 AllocateBattleResponse 提取 v2 实例绑定。
// 三个实例字段缺一即拒(旧 ds_allocator / 降级路径),保证 v2 票永远带完整绑定。
func battleTargetFromResponse(resp *dsv1.AllocateBattleResponse, matchID uint64) (auth.DSTicketTarget, error) {
	return battleTargetFromFields(resp.GetDsPodName(), resp.GetGameserverUid(), resp.GetInstanceEpoch(),
		resp.GetAllocationId(), resp.GetReleaseTrack(), matchID)
}

func battleTargetFromFields(
	podName, gameserverUID string,
	instanceEpoch uint32,
	allocationID, releaseTrack string,
	matchID uint64,
) (auth.DSTicketTarget, error) {
	if podName == "" || gameserverUID == "" || instanceEpoch == 0 || allocationID == "" ||
		(releaseTrack != auth.ReleaseTrackStable && releaseTrack != auth.ReleaseTrackCanary) {
		return auth.DSTicketTarget{}, errcode.New(errcode.ErrDSAllocationFailed,
			"ds_allocator 未回填完整 DS 目标(pod=%q uid=%q epoch=%d alloc=%q track=%q),无法签 v2 票, match %d",
			podName, gameserverUID, instanceEpoch, allocationID, releaseTrack, matchID)
	}
	return auth.DSTicketTarget{
		DSPodName:       podName,
		DSInstanceUID:   gameserverUID,
		DSInstanceEpoch: instanceEpoch,
		ReleaseTrack:    releaseTrack,
		MatchID:         matchID,
		AllocationID:    allocationID,
	}, nil
}

// SignBattleTicket 只使用 READY match 持久化的 exact target。
// 不允许降级 legacy HMAC 票 —— 唯一例外是 Windows 本机联调档 local-off-v1,
// 此时 main 只注入 legacy signer、不注入 v2(两者互斥,见 cmd/matchmaker/main.go),
// 因为同机的 ds_allocator/UE DS 被硬锁在 HS256LocalOff 档、不交叉接受 v2 票。
//
// R7 复审 P0-2:sessGate 非 nil 时读玩家当前会话 jti 签进 sjti claim。
//   - 权威不可达 → fail-closed 拒签(票不能在"无法判定会话"时盲签);
//   - 无会话(已登出/过期) → 拒签:没有现行会话就不存在合法的入场交付对象,
//     重登后的重连链(login tryBattleReconnect)会用新会话重签。
func (g *GrpcDSAllocator) SignBattleTicket(ctx context.Context, playerID, matchID uint64, allocation *model.BattleAllocation) (string, error) {
	if allocation == nil || !allocation.Target.CompleteBattle() {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"complete target required, player %d match %d", playerID, matchID)
	}
	if g.v2 == nil && g.signer == nil {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"no battle ticket signer configured, player %d match %d", playerID, matchID)
	}
	var sessJTI string
	if g.sessGate != nil {
		jti, found, err := g.sessGate.CurrentJTI(ctx, playerID)
		if err != nil {
			return "", errcode.NewCause(errcode.ErrUnavailable, err,
				"session authority unavailable while signing battle ticket, player %d match %d", playerID, matchID)
		}
		if !found {
			return "", errcode.New(errcode.ErrUnauthorized,
				"player %d has no current session; battle ticket withheld, match %d", playerID, matchID)
		}
		sessJTI = jti
	}
	if g.v2 == nil {
		// local-off-v1:UE 侧走 HS256LocalOff 分支,只校验 player/match/exp。
		token, _, err := g.signer.SignDSTicket(playerID, auth.DSTypeBattle, matchID, uuid.NewString())
		if err != nil {
			return "", errcode.New(errcode.ErrDSAllocationFailed,
				"sign local-off-v1 battle ticket for player %d match %d failed: %v", playerID, matchID, err)
		}
		return token, nil
	}
	target := auth.DSTicketTarget{
		DSPodName: allocation.Target.PodName, DSInstanceUID: allocation.Target.InstanceUID,
		DSInstanceEpoch: allocation.Target.InstanceEpoch, ReleaseTrack: allocation.Target.ReleaseTrack,
		MatchID: matchID, AllocationID: allocation.Target.AllocationID,
		SessionJTI: sessJTI,
	}
	token, _, err := g.v2.SignBattleTicket(playerID, 0, 0, uuid.NewString(), target)
	if err != nil {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"sign bound v2 battle ticket for player %d match %d failed: %v", playerID, matchID, err)
	}
	return token, nil
}
