// ds_stub.go — W4 ① 的 DSAllocator 打桩实现。
//
// W4 ② ds_allocator 服务上线后,替换为 gRPC 调用 ds_allocator.AllocateBattle
// (Agones GameServerAllocation)。本桩仅返回固定 mock 地址 + 每玩家 mock 票据,
// 让撮合流水线 QUEUEING→FOUND→CONFIRM→READY 全链路可端到端跑通。
package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// StubDSAllocator 是 DSAllocator 的打桩实现(W4 ①)。
type StubDSAllocator struct {
	// MockAddr 是返回的固定战斗 DS 地址(dev 联调用)。
	MockAddr string
}

// NewStubDSAllocator 构造打桩分配器。addr 为空时用占位地址。
func NewStubDSAllocator(addr string) *StubDSAllocator {
	if addr == "" {
		addr = "127.0.0.1:7777"
	}
	return &StubDSAllocator{MockAddr: addr}
}

// AllocateBattle 返回固定地址 + 每个玩家一个 mock 票据(matchID-playerID)。mapID 桩里忽略。
func (s *StubDSAllocator) AllocateBattle(_ context.Context, matchID uint64, playerIDs []uint64, _ uint32) (*model.BattleAllocation, error) {
	return &model.BattleAllocation{
		Address: s.MockAddr,
		Target: placement.Target{
			PodName:       fmt.Sprintf("mock-battle-%d", matchID),
			InstanceUID:   fmt.Sprintf("mock-uid-%d", matchID),
			InstanceEpoch: 1,
			AllocationID:  fmt.Sprintf("mock-allocation-%d", matchID),
			ReleaseTrack:  "stable",
		},
	}, nil
}

// AllocateBattleWithCombatFactions 实现 CombatFactionDSAllocator。
//
// 桩不需要真的把阵营投递到任何地方，但**必须**实现这个接口:阵营已是对局定义的必填部分，
// 分配器不具备承载能力时 MatchUsecase 会定性失败。若本桩不实现它，无 ds_allocator_addr
// 的本地开发链路会在每次分配时硬失败。
// 同样按 fail-closed 校验:空阵营映射在这里就拒绝，让本地环境和生产暴露同一类错误。
func (s *StubDSAllocator) AllocateBattleWithCombatFactions(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
) (*model.BattleAllocation, error) {
	if len(combatFactionByPlayer) == 0 {
		return nil, fmt.Errorf("stub allocator: combat factions required for match %d", matchID)
	}
	for _, playerID := range playerIDs {
		if _, ok := combatFactionByPlayer[playerID]; !ok {
			return nil, fmt.Errorf("stub allocator: player %d missing combat faction", playerID)
		}
	}
	return s.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (s *StubDSAllocator) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	return nil
}

func (s *StubDSAllocator) SignBattleTickets(_ context.Context, matchID uint64, playerIDs []uint64, _ *model.BattleAllocation) (map[uint64]string, error) {
	tickets := make(map[uint64]string, len(playerIDs))
	for _, pid := range playerIDs {
		tickets[pid] = fmt.Sprintf("mock-ticket-%d-%d", matchID, pid)
	}
	return tickets, nil
}

// SignBattleTicket 桩：返回带纳秒后缀的 mock 票，模拟“每次新 jti”。实现 biz.DSAllocator。
func (s *StubDSAllocator) SignBattleTicket(_ context.Context, playerID, matchID uint64, _ *model.BattleAllocation) (string, error) {
	return fmt.Sprintf("mock-ticket-%d-%d-%d", matchID, playerID, time.Now().UnixNano()), nil
}
