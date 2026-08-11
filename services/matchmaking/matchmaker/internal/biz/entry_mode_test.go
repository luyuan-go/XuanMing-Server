// entry_mode_test.go —— 进本入口的两条路(排队撮合 / 人不够自己进)与直进人数下限。
//
// 覆盖的是 CLAUDE.md §17 的三条:入口差异只进表不进接口签名(entry_mode 是关卡表一列 +
// 请求里的一个选择)、客户端只传选择(能不能这么进由服务端判)、准入条件只有服务端一份
// 权威判定(下限 fail-closed,且单人入口不得绕过)。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

const (
	entryMatchmake = configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE
	entryWalkIn    = configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN
	entryBoth      = configpb.LevelEntryMode_LEVEL_ENTRY_MODE_BOTH
)

// entryLevelRow 造一行战斗类关卡。game_mode 留空:validateMapID 只在该列非空时交叉校验
// 部署 game_mode,留空即对任何部署放行,让本文件专注入口判定。
func entryLevelRow(id uint32, mode configpb.LevelEntryMode, teamSize, minTeamSize uint32) *configpb.LevelRow {
	return &configpb.LevelRow{
		Id: id, Name: "副本", AssetPath: "/Game/L/Dungeon.Dungeon",
		Category:    configpb.LevelCategory_LEVEL_CATEGORY_BATTLE,
		EntryMode:   mode,
		TeamSize:    teamSize,
		MinTeamSize: minTeamSize,
		SideCount:   1,
	}
}

// loadEntryTables 把给定关卡行装进真实 configtable.Store 并挂到 usecase 上。
func loadEntryTables(t *testing.T, f *fixture, rows []*configpb.LevelRow) {
	t.Helper()
	dir := t.TempDir()
	writeLevelBatch(t, dir, 100, rows)
	store := configtable.NewStore()
	if _, err := store.Load(dir, 0); err != nil {
		t.Fatal(err)
	}
	f.uc.SetConfigTables(store)
}

// TestResolveEntryMode 「关卡表允许什么」× 「玩家选什么」求交。
// 关键行为:表填 BOTH 时请求必须明确选一种,留空 fail-closed —— 猜错的后果是玩家以为在
// 排队实则已单刷进本(反之亦然),而进本会消耗次数 / CD,不是重试能挽回的。
func TestResolveEntryMode(t *testing.T) {
	f := newFixtureWith(t, 9000, func(c *conf.MatchConf) { c.MapId = 1 })
	loadEntryTables(t, f, []*configpb.LevelRow{
		entryLevelRow(1, entryMatchmake, 5, 0),
		entryLevelRow(2, entryWalkIn, 5, 0),
		entryLevelRow(3, entryBoth, 5, 0),
	})

	cases := []struct {
		name    string
		mapID   uint32
		choice  configpb.LevelEntryMode
		want    configpb.LevelEntryMode
		wantErr bool
	}{
		{"只准撮合 + 没选 → 撮合(老客户端)", 1, entryUnset, entryMatchmake, false},
		{"只准撮合 + 选撮合 → 撮合", 1, entryMatchmake, entryMatchmake, false},
		{"只准撮合 + 选直进 → 拒", 1, entryWalkIn, 0, true},
		{"只准直进 + 没选 → 直进(老客户端)", 2, entryUnset, entryWalkIn, false},
		{"只准直进 + 选撮合 → 拒", 2, entryMatchmake, 0, true},
		{"两种都开 + 选撮合 → 撮合", 3, entryMatchmake, entryMatchmake, false},
		{"两种都开 + 选直进 → 直进", 3, entryWalkIn, entryWalkIn, false},
		{"两种都开 + 没选 → 拒(不替玩家猜入口)", 3, entryUnset, 0, true},
		{"请求填 BOTH → 拒(它不是一种进法)", 3, entryBoth, 0, true},
		{"单一模式关卡请求填 BOTH → 拒", 1, entryBoth, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := f.uc.resolveEntryMode(c.mapID, c.choice)
			if c.wantErr {
				if errcode.As(err) != errcode.ErrMatchEntryModeDenied {
					t.Fatalf("应拒 ErrMatchEntryModeDenied,得 got=%v err=%v", got, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got=%v err=%v,期望 %v", got, err, c.want)
			}
		})
	}
}

// TestResolveEntryModeFallsBackToDeploySwitch 关卡表未启用 / 行不存在 → 沿用部署级 walk_in
// 开关(§9.21 共存窗口:新二进制 + 旧批次表必须逐字节保持旧行为)。
func TestResolveEntryModeFallsBackToDeploySwitch(t *testing.T) {
	pve := newFixtureWith(t, 9100, func(c *conf.MatchConf) { c.WalkIn = true })
	if got, err := pve.uc.resolveEntryMode(6, entryUnset); err != nil || got != entryWalkIn {
		t.Fatalf("walk_in=true 部署 + 无表应回退直进,得 %v err=%v", got, err)
	}
	pvp := newFixtureWith(t, 9200, func(c *conf.MatchConf) { c.WalkIn = false })
	if got, err := pvp.uc.resolveEntryMode(6, entryUnset); err != nil || got != entryMatchmake {
		t.Fatalf("walk_in=false 部署 + 无表应回退撮合,得 %v err=%v", got, err)
	}
}

// TestMinTeamSizeForMap 下限读表 + 越界钳制。
// 钳到上限而不是放行,是防「手改 dist 绕过加载期校验」把下限填得比上限还大 —— 那会让该图
// 任何人数都进不去,是个没有任何日志的拒服务。
func TestMinTeamSizeForMap(t *testing.T) {
	f := newFixtureWith(t, 9300, func(c *conf.MatchConf) { c.MapId = 1 })
	if got := f.uc.minTeamSizeForMap(1); got != 0 {
		t.Fatalf("表未启用应无下限,得 %d", got)
	}
	loadEntryTables(t, f, []*configpb.LevelRow{
		entryLevelRow(1, entryWalkIn, 5, 3),
		entryLevelRow(2, entryWalkIn, 5, 0),
	})
	if got := f.uc.minTeamSizeForMap(1); got != 3 {
		t.Fatalf("min=3 应按表,得 %d", got)
	}
	if got := f.uc.minTeamSizeForMap(2); got != 0 {
		t.Fatalf("min 留空应无下限,得 %d", got)
	}
	if got := f.uc.minTeamSizeForMap(999); got != 0 {
		t.Fatalf("行不存在应无下限,得 %d", got)
	}
}

// TestStartMatchWalkInMinTeamSize 直进下限的进场闸:5 人本最少 3 人才准自己进。
//
// 三个断言各自守一件事:
//   - 2 人队被拒 → 下限真的生效,且错误码与「队伍没准备好」区分得开;
//   - **单人(team_id=0)被拒 → 下限没被单人入口绕过**。resolveMembers 对 teamID==0 直接
//     返回单人名单、不走任何人数校验,闸若写在那条分支之后,"不组队直接点进"整条绕开;
//   - 3 人队放行 → 下限不是"必须满员"。
func TestStartMatchWalkInMinTeamSize(t *testing.T) {
	f := newFixtureWith(t, 9400, func(c *conf.MatchConf) { c.MapId = 1; c.WalkIn = true })
	loadEntryTables(t, f, []*configpb.LevelRow{entryLevelRow(1, entryWalkIn, 5, 3)})
	ctx := context.Background()

	f.uc.reader = illegalStateTeamReader{team: readyIllegalStateTeam(9401, 9411, 9411, 9412)}
	if _, err := f.uc.StartMatch(ctx, 9451, 9401, 9411, 1, entryWalkIn); errcode.As(err) != errcode.ErrMatchTeamTooSmall {
		t.Fatalf("2 人直进 5 人本(下限 3)应拒 ErrMatchTeamTooSmall,得 %v", err)
	}

	// team_id=0 是单人入口,不查 team 服务;下限必须照样拦住。
	if _, err := f.uc.StartMatch(ctx, 9452, 0, 9421, 1, entryWalkIn); errcode.As(err) != errcode.ErrMatchTeamTooSmall {
		t.Fatalf("单人直进(下限 3)应拒 ErrMatchTeamTooSmall,得 %v", err)
	}

	f.uc.reader = illegalStateTeamReader{team: readyIllegalStateTeam(9402, 9431, 9431, 9432, 9433)}
	if _, err := f.uc.StartMatch(ctx, 9453, 9402, 9431, 1, entryWalkIn); err != nil {
		t.Fatalf("3 人直进 5 人本(下限 3)应放行,得 %v", err)
	}
}

// TestStartMatchMatchmakeIgnoresMinTeamSize 下限**只对直进生效**。
// 撮合的目标恒是凑满 team_size(加载期已校验 ≥ min),单人排队天经地义 —— 若这里也判下限,
// 玩家就会看到"人不够所以不准排队"这种自相矛盾的拒绝(排队正是为了凑人)。
func TestStartMatchMatchmakeIgnoresMinTeamSize(t *testing.T) {
	f := newFixtureWith(t, 9500, func(c *conf.MatchConf) { c.MapId = 1 })
	loadEntryTables(t, f, []*configpb.LevelRow{entryLevelRow(1, entryBoth, 5, 3)})
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 9551, 0, 9511, 1, entryMatchmake); err != nil {
		t.Fatalf("单人排队撮合不应被下限拦下,得 %v", err)
	}
	if _, err := f.uc.StartMatch(ctx, 9552, 0, 9521, 1, entryWalkIn); errcode.As(err) != errcode.ErrMatchTeamTooSmall {
		t.Fatalf("同一张图选直进应被下限拦下,得 %v", err)
	}
	if _, err := f.uc.StartMatch(ctx, 9553, 0, 9531, 1, entryUnset); errcode.As(err) != errcode.ErrMatchEntryModeDenied {
		t.Fatalf("表填 BOTH 而请求没选,应拒 ErrMatchEntryModeDenied,得 %v", err)
	}
}

// TestStartMatchPersistsEntryMode 落定的进法必须写进 durable saga 记录:StartMatch 之后
// 进程可能立刻重启,由后台 worker 接着推进并据此构造票据。若不落库而在撮合时回查关卡表,
// BOTH 的图就答不出"这张票当初选的是哪种"。
func TestStartMatchPersistsEntryMode(t *testing.T) {
	f := newFixtureWith(t, 9600, func(c *conf.MatchConf) { c.MapId = 1 })
	loadEntryTables(t, f, []*configpb.LevelRow{entryLevelRow(1, entryBoth, 5, 0)})

	if _, err := f.uc.StartMatch(context.Background(), 9651, 0, 9611, 1, entryWalkIn); err != nil {
		t.Fatalf("StartMatch: %v", err)
	}
	op, found, err := f.repo.GetStartOperation(context.Background(), 9651)
	if err != nil || !found {
		t.Fatalf("取 start operation 失败: found=%v err=%v", found, err)
	}
	if op.GetEntryMode() != entryWalkIn {
		t.Fatalf("saga 记录未落 entry_mode,得 %v", op.GetEntryMode())
	}
	if got := ticketFromStartOperation(op).GetEntryMode(); got != entryWalkIn {
		t.Fatalf("票据未继承 entry_mode,得 %v", got)
	}
}

// TestIsWalkInTicket 撮合循环的分流口径:票上落定的进法是权威,表只兜底存量旧票。
//
// 这一条是 BOTH 能成立的关键 —— 同一张图两个入口共存时,"这张票排队还是直进"回查关卡表
// 永远答不出来,只有票据自己知道。
func TestIsWalkInTicket(t *testing.T) {
	f := newFixtureWith(t, 9700, func(c *conf.MatchConf) { c.MapId = 1; c.WalkIn = true })
	loadEntryTables(t, f, []*configpb.LevelRow{entryLevelRow(1, entryBoth, 5, 0)})

	walkIn := &matchv1.MatchTicketStorageRecord{MapId: 1, EntryMode: entryWalkIn}
	if !f.uc.isWalkInTicket(walkIn) {
		t.Fatal("票上选了直进,应走直进")
	}
	matchmake := &matchv1.MatchTicketStorageRecord{MapId: 1, EntryMode: entryMatchmake}
	if f.uc.isWalkInTicket(matchmake) {
		t.Fatal("同一张 BOTH 图上选了撮合的票,不能被当成直进立即成局")
	}
	// 存量旧票(滚动升级期旧 matchmaker 写入,无本字段)。该图填的是 BOTH —— 旧二进制不认识
	// 这个值,其 switch 会落 default 用部署开关 cfg.WalkIn=true 判直进。新二进制必须做出同样的
	// 决定,否则 leader 一交棒同一张票的命运就变了(§9.21)。
	legacy := &matchv1.MatchTicketStorageRecord{MapId: 1}
	if !f.uc.isWalkInTicket(legacy) {
		t.Fatal("无 entry_mode 的旧票应复刻旧二进制的判定(BOTH → 落 cfg.WalkIn)")
	}
}

// TestIsWalkInTicketLegacyMatchesOldBinary 存量票兜底必须与旧二进制逐值一致。
// 表填单一模式时按表(旧代码 switch 的两个具名分支),填 BOTH 时按部署开关(旧代码的 default)。
func TestIsWalkInTicketLegacyMatchesOldBinary(t *testing.T) {
	for _, walkInDeploy := range []bool{true, false} {
		f := newFixtureWith(t, 9800+uint64(len(t.Name())), func(c *conf.MatchConf) {
			c.MapId = 1
			c.WalkIn = walkInDeploy
		})
		loadEntryTables(t, f, []*configpb.LevelRow{
			entryLevelRow(1, entryWalkIn, 5, 0),
			entryLevelRow(2, entryMatchmake, 5, 0),
			entryLevelRow(3, entryBoth, 5, 0),
		})
		legacy := func(mapID uint32) bool {
			return f.uc.isWalkInTicket(&matchv1.MatchTicketStorageRecord{MapId: mapID})
		}
		if !legacy(1) {
			t.Fatalf("walk_in=%v: 表填 WALK_IN 的旧票应直进", walkInDeploy)
		}
		if legacy(2) {
			t.Fatalf("walk_in=%v: 表填 MATCHMAKE 的旧票应撮合", walkInDeploy)
		}
		if got := legacy(3); got != walkInDeploy {
			t.Fatalf("walk_in=%v: 表填 BOTH 的旧票应落部署开关,得 %v", walkInDeploy, got)
		}
	}
}
