// helpers.go — matchmaker biz 的纯函数辅助(装箱、MMR 窗口、进度转换、成员工具)。
package biz

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// withinWindow 判断票据 b 是否落在以票据 a 为锚点的动态 MMR 窗口内。
// 窗口 = min(MmrMaxWindow, MmrBaseWindow + MmrWidenPerSec × 组内最长等待秒数)。
func withinWindow(a, b *matchv1.MatchTicketStorageRecord, nowMs int64, cfg conf.MatchConf) bool {
	waitMs := nowMs - a.EnqueuedAtMs
	if w := nowMs - b.EnqueuedAtMs; w > waitMs {
		waitMs = w
	}
	waitSec := waitMs / 1000
	window := cfg.MmrBaseWindow + cfg.MmrWidenPerSec*int(waitSec)
	if window > cfg.MmrMaxWindow {
		window = cfg.MmrMaxWindow
	}
	diff := int(a.AvgMmr - b.AvgMmr)
	if diff < 0 {
		diff = -diff
	}
	return diff <= window
}

// binPack 用 largest-first 把票据装进两个容量 teamSize 的箱子(两边人数各恰好 teamSize)。
// 票据整队不可拆。成功返回 (sideA, sideB, true);无法精确装满返回 (_, _, false)。
// binPack 把若干张票据装箱成 sideCount 方,每方恰好 teamSize 人;装不满或装不下即失败。
// 票据不可拆分(一支队伍必须整体在同一方),所以这是「多背包恰好装满」问题,用
// 「大队优先 + 首个装得下的方」贪心——与原两方实现同一策略,只是把方数参数化。
//
// sideCount 由关卡表 side_count 列给出:PVE 合作=1、常规对抗=2、多队混战=N。
// sideCount<=0 由调用方兜底成 2(历史默认),本函数不做该兜底以免掩盖调用方的取值错误。
//
// 复杂度 O(票数 × sideCount);票数被 need=sideCount×teamSize 上界约束,方数是个位数,
// 不需要更聪明的装箱算法(§15.2:够用即可,不为理论最优增复杂度)。
func binPack(tickets []*matchv1.MatchTicketStorageRecord, teamSize, sideCount int) (sides [][]*matchv1.MatchTicketStorageRecord, ok bool) {
	if teamSize <= 0 || sideCount <= 0 {
		return nil, false
	}
	// 复制并按队伍人数降序(大队优先放置,降低装不下的概率)
	sorted := make([]*matchv1.MatchTicketStorageRecord, len(tickets))
	copy(sorted, tickets)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && len(sorted[j].Members) > len(sorted[j-1].Members); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	sides = make([][]*matchv1.MatchTicketStorageRecord, sideCount)
	remain := make([]int, sideCount)
	for i := range remain {
		remain[i] = teamSize
	}
	for _, t := range sorted {
		size := len(t.Members)
		placed := false
		for i := 0; i < sideCount; i++ {
			if size <= remain[i] {
				sides[i] = append(sides[i], t)
				remain[i] -= size
				placed = true
				break
			}
		}
		if !placed {
			return nil, false
		}
	}
	// 每一方都必须恰好装满:留空位说明这批票据凑不出完整对局,交回上层继续等。
	for _, r := range remain {
		if r != 0 {
			return nil, false
		}
	}
	return sides, true
}

// ── 进度转换 ──────────────────────────────────────────────────────────────────

// buildProgress 从成员列表构造 MatchProgress(分 team_a/team_b)。
// mapID 是本局副本编号(权威 ticket/match 记录的 map_id,0=未指定/默认):客户端在
// READY 收到时据此预置关卡上下文,不等 DS 握手后按地图名反查。
func buildProgress(matchID uint64, stage matchv1.MatchStage, members []*matchv1.MatchMemberStorageRecord, dsAddr, battleTicket string, mapID uint32) *matchv1.MatchProgress {
	teamA := make([]uint64, 0, len(members))
	teamB := make([]uint64, 0, len(members))
	for _, m := range members {
		if m.Side == 0 {
			teamA = append(teamA, m.PlayerId)
		} else {
			teamB = append(teamB, m.PlayerId)
		}
	}
	return &matchv1.MatchProgress{
		MatchId:      matchID,
		Stage:        stage,
		BattleDsAddr: dsAddr,
		BattleTicket: battleTicket,
		TeamA:        teamA,
		TeamB:        teamB,
		MapId:        mapID,
	}
}

// matchToProgress 把 MatchStorageRecord 转成客户端可见的 MatchProgress。
func matchToProgress(m *matchv1.MatchStorageRecord) *matchv1.MatchProgress {
	return buildProgress(m.MatchId, m.Stage, m.Members, m.BattleDsAddr, m.BattleTicket, m.MapId)
}

// ticketToProgress 把排队中的票据转成 QUEUEING 进度(用 ticket_id 作 match_id 句柄)。
func ticketToProgress(t *matchv1.MatchTicketStorageRecord) *matchv1.MatchProgress {
	return &matchv1.MatchProgress{
		MatchId: t.TicketId,
		Stage:   stageQueueing,
		MapId:   t.MapId,
	}
}

// ── 成员工具 ──────────────────────────────────────────────────────────────────

func memberIndex(members []*matchv1.MatchMemberStorageRecord, playerID uint64) int {
	for i, m := range members {
		if m.PlayerId == playerID {
			return i
		}
	}
	return -1
}

func allAccepted(members []*matchv1.MatchMemberStorageRecord) bool {
	if len(members) == 0 {
		return false
	}
	for _, m := range members {
		if m.Confirm != confirmAccepted {
			return false
		}
	}
	return true
}

func memberPlayerIDs(members []*matchv1.MatchMemberStorageRecord) []uint64 {
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.PlayerId)
	}
	return ids
}

// combatFactionsFromMembers 把持久化 MatchMember.side 转成独立的 match-local 战斗阵营。
// team_id/guild_id 不参与映射，因此多个队伍可共享阵营，也允许 0/1 之外的多阵营场景。
func combatFactionsFromMembers(members []*matchv1.MatchMemberStorageRecord) (map[uint64]uint32, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("match members are empty")
	}
	factions := make(map[uint64]uint32, len(members))
	players := make([]uint64, 0, len(members))
	for _, member := range members {
		if member == nil || member.GetPlayerId() == 0 {
			return nil, fmt.Errorf("match member player_id is empty")
		}
		if member.GetSide() < 0 || uint64(member.GetSide()) > uint64(dsmetadata.MaxCombatFactionID) {
			return nil, fmt.Errorf("player %d side %d exceeds combat faction range",
				member.GetPlayerId(), member.GetSide())
		}
		if _, duplicate := factions[member.GetPlayerId()]; duplicate {
			return nil, fmt.Errorf("duplicate match member player_id %d", member.GetPlayerId())
		}
		factions[member.GetPlayerId()] = uint32(member.GetSide())
		players = append(players, member.GetPlayerId())
	}
	if _, _, err := dsmetadata.CanonicalCombatFactions(players, factions); err != nil {
		return nil, err
	}
	return factions, nil
}

func cloneMatch(m *matchv1.MatchStorageRecord) *matchv1.MatchStorageRecord {
	return proto.Clone(m).(*matchv1.MatchStorageRecord)
}
