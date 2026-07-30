// binpack_test.go — 装箱分方(binPack)测试。
// 票据不可拆分(一支队伍整体在同一方),装箱须让每一方恰好坐满 team_size 人。
package biz

import (
	"testing"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

// ticketOf 造一张 n 人票据(装箱只看人数)。
func ticketOf(id uint64, n int) *matchv1.MatchTicketStorageRecord {
	members := make([]*matchv1.MatchMemberStorageRecord, 0, n)
	for i := 0; i < n; i++ {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: id*100 + uint64(i)})
	}
	return &matchv1.MatchTicketStorageRecord{TicketId: id, Members: members}
}

func sideSizes(sides [][]*matchv1.MatchTicketStorageRecord) []int {
	out := make([]int, len(sides))
	for i, s := range sides {
		for _, t := range s {
			out[i] += len(t.Members)
		}
	}
	return out
}

func TestBinPack(t *testing.T) {
	cases := []struct {
		name      string
		tickets   []*matchv1.MatchTicketStorageRecord
		teamSize  int
		sideCount int
		wantOK    bool
	}{
		{
			// 单排 5v5:10 张 1 人票凑两方。这条锁住「没有队伍也能匹配 PVP」。
			name: "单排 5v5",
			tickets: []*matchv1.MatchTicketStorageRecord{
				ticketOf(1, 1), ticketOf(2, 1), ticketOf(3, 1), ticketOf(4, 1), ticketOf(5, 1),
				ticketOf(6, 1), ticketOf(7, 1), ticketOf(8, 1), ticketOf(9, 1), ticketOf(10, 1),
			},
			teamSize: 5, sideCount: 2, wantOK: true,
		},
		{
			// 拼队:3 人队 + 2 人散排凑一方,对面同理。这条锁住「不满员组队匹配」。
			name: "3+2 拼队",
			tickets: []*matchv1.MatchTicketStorageRecord{
				ticketOf(1, 3), ticketOf(2, 2), ticketOf(3, 3), ticketOf(4, 2),
			},
			teamSize: 5, sideCount: 2, wantOK: true,
		},
		{
			name:      "PVE 合作单方 3 人",
			tickets:   []*matchv1.MatchTicketStorageRecord{ticketOf(1, 2), ticketOf(2, 1)},
			teamSize:  3,
			sideCount: 1,
			wantOK:    true,
		},
		{
			// 多队混战:4 方各 2 人。
			name: "四方混战",
			tickets: []*matchv1.MatchTicketStorageRecord{
				ticketOf(1, 2), ticketOf(2, 2), ticketOf(3, 2), ticketOf(4, 2),
			},
			teamSize: 2, sideCount: 4, wantOK: true,
		},
		{
			// 人数够但切不开:3 人队装不进只剩 2 空位的任何一方。
			name: "人数足但不可分割",
			tickets: []*matchv1.MatchTicketStorageRecord{
				ticketOf(1, 3), ticketOf(2, 3), ticketOf(3, 3), ticketOf(4, 3),
			},
			teamSize: 6, sideCount: 2, wantOK: true,
		},
		{
			name: "装不下:4 人队进 3 人一方",
			tickets: []*matchv1.MatchTicketStorageRecord{
				ticketOf(1, 4), ticketOf(2, 1), ticketOf(3, 1),
			},
			teamSize: 3, sideCount: 2, wantOK: false,
		},
		{
			name:      "人数不足:留空位即失败",
			tickets:   []*matchv1.MatchTicketStorageRecord{ticketOf(1, 1)},
			teamSize:  2,
			sideCount: 2,
			wantOK:    false,
		},
		{
			name:      "非法方数",
			tickets:   []*matchv1.MatchTicketStorageRecord{ticketOf(1, 1)},
			teamSize:  1,
			sideCount: 0,
			wantOK:    false,
		},
	}

	for _, c := range cases {
		sides, ok := binPack(c.tickets, c.teamSize, c.sideCount)
		if ok != c.wantOK {
			t.Fatalf("%s: ok=%v, 期望 %v", c.name, ok, c.wantOK)
		}
		if !ok {
			continue
		}
		if len(sides) != c.sideCount {
			t.Fatalf("%s: 分出 %d 方, 期望 %d", c.name, len(sides), c.sideCount)
		}
		for i, n := range sideSizes(sides) {
			if n != c.teamSize {
				t.Fatalf("%s: 第 %d 方 %d 人, 期望恰好 %d", c.name, i, n, c.teamSize)
			}
		}
		// 票据不可拆分:每张票只能出现在一方里,且总数守恒。
		seen := map[uint64]int{}
		for _, s := range sides {
			for _, tk := range s {
				seen[tk.TicketId]++
			}
		}
		if len(seen) != len(c.tickets) {
			t.Fatalf("%s: 票据数 %d, 期望 %d(票据不得丢失或重复)", c.name, len(seen), len(c.tickets))
		}
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("%s: 票据 %d 出现 %d 次(不得被拆分到多方)", c.name, id, n)
			}
		}
	}
}
