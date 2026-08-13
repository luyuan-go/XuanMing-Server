// player_guild_authority_test.go — DS 社交归属反查必须读权威,不吃玩家面板那条 cache-aside。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/social/guild/internal/data"
)

// TestGetPlayerGuildID_ReadsAuthorityNotStaleCache 钉死 GetPlayerGuildID 与 GetMyGuild 的分野。
//
// 两条路径吃同一份陈旧缓存的代价差一个量级:玩家面板读到旧值只是晚一拍(下次拉取自愈);
// 而 DS 只在进场时查这一次,陈旧值一旦写到实体上就会复制给全场、**整场不再纠正**
// (会友被显示成路人,或反过来)。写路径删缓存失败时仅告警、靠 TTL 兜底,所以
// 「删缓存基本都成功」不足以让 DS 这一侧也吃缓存。
//
// 本测试刻意把缓存与权威造成**不一致**:哪天有人图省事把 GetPlayerGuildID 改回复用
// GetMyGuild,这里会立刻红。
func TestGetPlayerGuildID_ReadsAuthorityNotStaleCache(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	ctx := context.Background()

	// 权威:玩家 1 现在在公会 100。
	if err := repo.CreateGuild(ctx, 100, 1, "authoritative", 100); err != nil {
		t.Fatalf("CreateGuild: %v", err)
	}
	// 缓存:一份陈旧反查,玩家 1 还挂在旧公会 999。连 999 的资料一起预置 —— 否则 info miss
	// 会让 GetMyGuild 自己落到权威读,两条路径就测不出差异了。
	if err := cache.SetMemberGuildID(ctx, 1, 999, time.Minute); err != nil {
		t.Fatalf("SetMemberGuildID: %v", err)
	}
	if err := cache.SetGuild(ctx, &data.GuildRow{
		GuildID: 999, Name: "stale", LeaderID: 1, MemberCount: 1, MaxMembers: 100,
	}, time.Minute); err != nil {
		t.Fatalf("SetGuild: %v", err)
	}

	// 玩家面板路径:吃缓存,拿到陈旧的 999。这是它的设计,不是 bug ——
	// 断言它保持原样,是为了证明下面那条的差异确实来自「路径不同」,而不是缓存压根没生效。
	view, err := uc.GetMyGuild(ctx, 1)
	if err != nil {
		t.Fatalf("GetMyGuild: %v", err)
	}
	if view == nil || view.GetGuildId() != 999 {
		t.Fatalf("前置不成立:GetMyGuild 应命中陈旧缓存返回 999,got=%v", view)
	}

	// DS 路径:必须绕开缓存拿到权威的 100。
	guildID, hasGuild, err := uc.GetPlayerGuildID(ctx, 1)
	if err != nil {
		t.Fatalf("GetPlayerGuildID: %v", err)
	}
	if !hasGuild || guildID != 100 {
		t.Fatalf("DS 反查必须读权威:want has=true id=100, got has=%v id=%d", hasGuild, guildID)
	}
}

// TestGetPlayerGuildID_NoGuildClearsStaleMemberCache 权威说「不在任何公会」时必须同时自愈缓存。
//
// 不清的话:玩家面板会在整个 TTL 内继续把一个已退会的人显示成旧公会的会员 ——
// 错的事实比缺失的事实更糟。与 GetMyGuild 的自愈同口径。
func TestGetPlayerGuildID_NoGuildClearsStaleMemberCache(t *testing.T) {
	repo := newFakeGuildRepo()
	cache := newFakeGuildCache()
	uc := newGuildUCWithCache(repo, cache)
	ctx := context.Background()

	// 权威:玩家 2 不在任何公会(repo 里没有他的成员行)。缓存:残留一条指向 999 的陈旧反查。
	if err := cache.SetMemberGuildID(ctx, 2, 999, time.Minute); err != nil {
		t.Fatalf("SetMemberGuildID: %v", err)
	}
	cache.resetDeletes()

	guildID, hasGuild, err := uc.GetPlayerGuildID(ctx, 2)
	if err != nil {
		t.Fatalf("GetPlayerGuildID: %v", err)
	}
	if hasGuild || guildID != 0 {
		t.Fatalf("权威无公会必须回 has=false:got has=%v id=%d", hasGuild, guildID)
	}
	if !contains(cache.delMem, 2) {
		t.Fatalf("权威无公会时必须清掉陈旧 member 反查缓存,delMem=%v", cache.delMem)
	}
}
