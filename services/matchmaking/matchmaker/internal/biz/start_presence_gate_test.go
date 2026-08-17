// start_presence_gate_test.go — StartMatch 在线闸(INC-20260813-001)。
//
// ⚠️ **这不是 INC-20260813-001 的第一根因**(v1 曾误判为是)。那次事故的缺席者全程没掉线 ——
// 他打完上一局先退了战斗,还在「结算 → 回大厅 → 重登」路上,队长 75 秒后就开了下一局。
// 第一根因是 team 侧没有 match-ended 复位路径(见 team 的 EndTeamMatch)。
//
// 本闸是**纵深防御**:拦「点开始匹配那一刻,队伍里有人已经离开大厅超过宽限窗」这一类,
// 覆盖真掉线、以及将来任何把不在场的人塞进名单的新路径(GM 拉人、新入口……)。
//
// 判据是「离开了多久」而不是「此刻查不查得到」——后者是 INC-20260724-001 已经踩过的坑
// (位置投影在正常路径上就会短暂缺席,按缺席判死是结构性 100% 假阳性)。
// 因此这里的用例必须成对出现:**真离开要拦住**,**投影缺席但人还在的不许误伤**。
package biz

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// fakePresence 是可控的 locator 双读。online / lastSeen 分开注入,才能造出
// 「查不到位置但有离开基线」与「查不到位置也没有任何基线」两种本质不同的局面。
type fakePresence struct {
	online        map[uint64]bool
	lastSeen      map[uint64]int64
	onlineErr     error
	lastSeenErr   error
	onlineCalls   int
	lastSeenCalls int
}

func (f *fakePresence) BatchOnline(_ context.Context, ids []uint64) (map[uint64]bool, error) {
	f.onlineCalls++
	if f.onlineErr != nil {
		return nil, f.onlineErr
	}
	out := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if f.online[id] {
			out[id] = true
		}
	}
	return out, nil
}

func (f *fakePresence) BatchLastSeen(_ context.Context, ids []uint64) (map[uint64]int64, error) {
	f.lastSeenCalls++
	if f.lastSeenErr != nil {
		return nil, f.lastSeenErr
	}
	out := make(map[uint64]int64, len(ids))
	for _, id := range ids {
		if ms, ok := f.lastSeen[id]; ok {
			out[id] = ms
		}
	}
	return out, nil
}

// presenceFixture 造一支 3 人队 + 可控 presence,与事故现场同形。
func presenceFixture(t *testing.T, mutate func(*conf.MatchConf)) (*fixture, *fakePresence, []uint64) {
	t.Helper()
	f := newFixtureWith(t, 9000, mutate)
	p := &fakePresence{online: map[uint64]bool{}, lastSeen: map[uint64]int64{}}
	f.uc.SetPresenceReader(p)
	return f, p, []uint64{7001, 7002, 7003}
}

func presenceMembers(ids []uint64) []*matchv1.MatchMemberStorageRecord {
	out := make([]*matchv1.MatchMemberStorageRecord, 0, len(ids))
	for _, id := range ids {
		out = append(out, &matchv1.MatchMemberStorageRecord{PlayerId: id})
	}
	return out
}

// 成员已离开 160s(超过 30s 宽限),必须拒,且错误码是 4011 不是别的。
func TestEnsureAllPresent_离开超过宽限窗必须拒(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	// 第三个人关掉了客户端:位置投影已过期,locator 记下了他离开的时刻。
	delete(p.online, ids[2])
	p.lastSeen[ids[2]] = time.Now().Add(-160 * time.Second).UnixMilli()

	err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids))
	if errcode.As(err) != errcode.ErrMatchMemberOffline {
		t.Fatalf("离开 160s 的成员必须被拦下: err=%v code=%d", err, errcode.As(err))
	}
	// 错误消息必须点名是谁,否则玩家只看到一个数字码,不知道该等谁。
	if !strings.Contains(err.Error(), strconv.FormatUint(ids[2], 10)) {
		t.Fatalf("错误消息必须带上离线成员 id: %q", err.Error())
	}
}

// 反向:同一批人,只是刚离开 5 秒(可能正在重连)→ 放行,交给 DS 侧 roster 期限兜底。
func TestEnsureAllPresent_刚离开在宽限窗内放行(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	delete(p.online, ids[2])
	p.lastSeen[ids[2]] = time.Now().Add(-5 * time.Second).UnixMilli()

	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatalf("宽限窗内不得拒(可能正在重连): %v", err)
	}
}

// ★ 防倒退用例:INC-20260724-001 的结构性假阳性。
// 玩家人坐在大厅里,但位置投影停在撮合失败后无人回写的 MATCHING 并已过期 —— locator
// 查不到他。**但 Hub DS 心跳一直把他报在 census 里**,所以 last_alive_ms 恒为「刚刚」。
// 按缺席判死会误伤他;按「离开多久」判就不会。这条用例锁死后者。
func TestEnsureAllPresent_位置投影缺席但心跳仍在不得误伤(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	delete(p.online, ids[1]) // 投影没了
	// 但 Hub DS 上一跳(5s 内)还把他报在场上。
	p.lastSeen[ids[1]] = time.Now().Add(-5 * time.Second).UnixMilli()

	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatalf("坐在大厅里的玩家绝不能因为位置投影缺席被判离线(INC-20260724-001): %v", err)
	}
}

// 拿不到任何离开基线 = UNKNOWN。§9.22 明令不确定不得冒充 OFFLINE:放行。
// (Hub DS 整台崩溃、超过保留期、这人从没上过线,都会落到这里。)
func TestEnsureAllPresent_无离开基线判UNKNOWN放行(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	delete(p.online, ids[0]) // 查不到位置,也没有任何 lastSeen

	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatalf("UNKNOWN 不得冒充 OFFLINE(§9.22): %v", err)
	}
}

// 全员在线时不得白白多打一次 BatchLastSeen —— 稳态是绝大多数情况,这一跳必须省掉。
func TestEnsureAllPresent_全员在线时不查lastSeen(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatal(err)
	}
	if p.lastSeenCalls != 0 {
		t.Fatalf("全员在线不该有第二次往返: lastSeenCalls=%d", p.lastSeenCalls)
	}
}

// 依赖不可用:默认 fail-closed 返回 ErrUnavailable(与 ensureNoneInBattle 同策略)。
func TestEnsureAllPresent_查询失败默认failClosed(t *testing.T) {
	sentinel := errors.New("locator down")
	for _, tc := range []struct {
		name string
		set  func(*fakePresence)
	}{
		{"batch_online", func(p *fakePresence) { p.onlineErr = sentinel }},
		{"batch_last_seen", func(p *fakePresence) { p.lastSeenErr = sentinel }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p, ids := presenceFixture(t, nil)
			for _, id := range ids {
				p.online[id] = true
			}
			delete(p.online, ids[2]) // 逼出第二次往返,才测得到 batch_last_seen 分支
			tc.set(p)
			err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids))
			if errcode.As(err) != errcode.ErrUnavailable {
				t.Fatalf("locator 查不通必须 fail-closed: err=%v code=%d", err, errcode.As(err))
			}
		})
	}
}

// dev 显式 battle_gate_fail_open=true 才降级放行(与 ensureNoneInBattle 共用同一开关)。
func TestEnsureAllPresent_failOpen显式开启后放行(t *testing.T) {
	f, p, ids := presenceFixture(t, func(c *conf.MatchConf) { c.BattleGateFailOpen = true })
	p.onlineErr = errors.New("locator down")
	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatalf("fail_open=true 应降级放行: %v", err)
	}
}

// 关闭开关(负值)= 整道闸不跑,一次 RPC 都不发。
func TestEnsureAllPresent_宽限窗负值关闭整道闸(t *testing.T) {
	f, p, ids := presenceFixture(t, func(c *conf.MatchConf) {
		c.StartPresenceGrace = config.Duration(-1)
	})
	p.lastSeen[ids[0]] = time.Now().Add(-time.Hour).UnixMilli()
	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids)); err != nil {
		t.Fatalf("闸关闭时不得拒: %v", err)
	}
	if p.onlineCalls != 0 {
		t.Fatalf("闸关闭时不该发 RPC: onlineCalls=%d", p.onlineCalls)
	}
}

// 未注入 reader(locator_addr 未配)= 行为与本闸落地前完全一致。
func TestEnsureAllPresent_未注入reader整道跳过(t *testing.T) {
	f := newFixture(t, 9000)
	if err := f.uc.ensureAllPresent(context.Background(), presenceMembers([]uint64{7001})); err != nil {
		t.Fatalf("未注入 presence reader 时必须跳过: %v", err)
	}
}

// 端到端:StartMatch 必须在**产生任何副作用之前**拒掉,不能只在撮合循环里拒。
// 判据是 durable operation 一条都没建 —— 建了就说明副作用已经落地。
func TestStartMatch_成员离线时不产生durableOperation(t *testing.T) {
	f, p, _ := presenceFixture(t, nil)
	ctx := context.Background()
	const captain = uint64(7001)
	p.lastSeen[captain] = time.Now().Add(-10 * time.Minute).UnixMilli()

	_, err := f.uc.StartMatch(ctx, 9101, 0, captain, 0, entryUnset)
	if errcode.As(err) != errcode.ErrMatchMemberOffline {
		t.Fatalf("StartMatch 应返回 4011: err=%v code=%d", err, errcode.As(err))
	}
	if _, found, gerr := f.repo.GetStartOperation(ctx, 9101); gerr != nil || found {
		t.Fatalf("被拒的 StartMatch 不得留下 durable operation: found=%v err=%v", found, gerr)
	}
}

// 4011 必须携带结构化缺席名单(2026-08-17 拍板):此前名单只拼在 error 文本里不过线,
// 客户端点不了名。service 层靠 errors.As 解出 MemberOfflineError 填
// StartMatchResponse.absent_player_ids;这里钉住 biz 侧契约:①类型可解;②名单是被判
// 缺席的那几个人;③errcode.As 经 Unwrap 链仍解析成 4011(老调用方零感知)。
func TestEnsureAllPresent_拒绝时携带结构化缺席名单(t *testing.T) {
	f, p, ids := presenceFixture(t, nil)
	for _, id := range ids {
		p.online[id] = true
	}
	delete(p.online, ids[2])
	p.lastSeen[ids[2]] = time.Now().Add(-160 * time.Second).UnixMilli()

	err := f.uc.ensureAllPresent(context.Background(), presenceMembers(ids))
	if errcode.As(err) != errcode.ErrMatchMemberOffline {
		t.Fatalf("错误码语义不得因换类型而变: err=%v code=%d", err, errcode.As(err))
	}
	var offline *MemberOfflineError
	if !errors.As(err, &offline) {
		t.Fatalf("必须能解出 MemberOfflineError(service 层填 absent_player_ids 的唯一来源): %T", err)
	}
	if len(offline.AbsentPlayerIDs) != 1 || offline.AbsentPlayerIDs[0] != ids[2] {
		t.Fatalf("缺席名单必须点名到人: got=%v want=[%d]", offline.AbsentPlayerIDs, ids[2])
	}
}
