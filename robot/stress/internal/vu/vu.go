// Package vu 实现单个虚拟玩家(VU)的状态机:
//
//	CONNECTING(login)→ LOBBY(订阅 push + 大厅操作循环)→ MATCH(组队/匹配/确认)
//	→ BATTLE(battle_result 上报)→ 回 LOBBY。
//
// 一个 VU = 一个 goroutine,几十万 VU 共享 client.Pool 里的少量连接(HTTP/2 多路复用)。
package vu

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/luyuancpp/pandora/robot/stress/internal/behavior"
	"github.com/luyuancpp/pandora/robot/stress/internal/client"
	"github.com/luyuancpp/pandora/robot/stress/internal/scenario"
	"github.com/luyuancpp/pandora/robot/stress/internal/stats"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	chatv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/chat/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	friendv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/friend/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	pushv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/push/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// VU 是一个虚拟玩家实例。
type VU struct {
	index int
	cfg   scenario.Config
	pool  *client.Pool
	sched *behavior.Scheduler
	stat  *stats.Collector
	rng   *rand.Rand

	account     string
	playerID    uint64
	sessionTok  string
	envoySample bool // 是否走 Envoy 对照链路登录
}

// New 创建一个 VU。seed 用于让每个 VU 的随机节奏互不相同。
func New(index int, cfg scenario.Config, pool *client.Pool, sched *behavior.Scheduler, stat *stats.Collector) *VU {
	rng := rand.New(rand.NewSource(int64(index)*2654435761 + time.Now().UnixNano()))
	return &VU{
		index:       index,
		cfg:         cfg,
		pool:        pool,
		sched:       sched,
		stat:        stat,
		rng:         rng,
		account:     cfg.AccountPrefix + strconv.Itoa(index),
		envoySample: rng.Float64() < cfg.EnvoySampleRatio,
	}
}

// Run 跑完整生命周期,直到 ctx 取消。
func (v *VU) Run(ctx context.Context) {
	if err := v.login(ctx); err != nil {
		v.stat.Counters.LoginFail.Add(1)
		return
	}
	v.stat.Counters.LoginOK.Add(1)
	v.stat.Counters.VUOnline.Add(1)
	defer v.stat.Counters.VUOnline.Add(-1)

	// 订阅 push(server stream),后台 drain。
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	go v.subscribePush(subCtx)

	// 大厅操作循环:加权挑动作 + 泊松抖动间隔。
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		v.doAction(ctx, v.sched.Pick(v.rng))

		wait := behavior.NextInterval(v.rng, float64(v.cfg.ActionIntervalMs))
		timer := time.NewTimer(time.Duration(wait) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// authCtx 返回带 player_id / trace_id metadata 的出站 context。
func (v *VU) authCtx(ctx context.Context) context.Context {
	return client.OutgoingContext(ctx, v.playerID)
}

// timed 包一次 RPC：记录时延 + 按调用点归因的错误计数。
// op 是低基数的稳定标签（service.Method），不要塞 player_id 等高基数值。
// 关停排空阶段 ctx 被取消造成的 context.Canceled 不算后端错误,单独归到 drain 分项,
// 不进 RPCErrors;其余错误(含 DeadlineExceeded 等真实失败)才计入 error。
func (v *VU) timed(op string, fn func() error) error {
	start := time.Now()
	err := fn()
	v.stat.ObserveRPC(float64(time.Since(start).Microseconds()) / 1000.0)
	if err != nil {
		if isShutdownCanceled(err) {
			v.stat.ObserveDrain(op)
		} else {
			v.stat.ObserveErr(op)
		}
	}
	return err
}

// isShutdownCanceled 判定一个错误是否是压测进程主动收敛(ctx 取消)导致的在途 RPC 中断。
// 命中 context.Canceled 或 gRPC codes.Canceled;DeadlineExceeded(真实超时)不算,仍是 error。
func isShutdownCanceled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return status.Code(err) == codes.Canceled
}

// login 真实走 LoginService,首次登录由 login 服务 devAutoRegister 自动建号。
func (v *VU) login(ctx context.Context) error {
	cli := v.pool.Login
	if v.envoySample && v.pool.LoginViaEnvoy != nil {
		cli = v.pool.LoginViaEnvoy
	}
	req := &loginv1.LoginRequest{
		Account:       v.account,
		PasswordHash:  "stressbot", // dev 环境 devSkipPassword=true,占位即可
		DeviceId:      "robot-" + strconv.Itoa(v.index),
		ClientVersion: "stress-1",
		Region:        "dev",
		Locale:        "zh-CN",
	}
	var resp *loginv1.LoginResponse
	err := v.timed("login.Login", func() error {
		var e error
		resp, e = cli.Login(client.OutgoingContext(ctx, 0), req)
		return e
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return fmt.Errorf("login code=%v", resp.GetCode())
	}
	v.playerID = resp.GetPlayerId()
	v.sessionTok = resp.GetSessionToken()
	if v.playerID == 0 {
		return fmt.Errorf("login 返回 player_id=0")
	}
	return nil
}

// subscribePush 建立 push server stream 并 drain,直到出错或 ctx 取消。
func (v *VU) subscribePush(ctx context.Context) {
	stream, err := v.pool.Push.Subscribe(v.authCtx(ctx), &pushv1.SubscribeRequest{
		SessionToken: v.sessionTok,
		LastSeenMs:   0,
	})
	if err != nil {
		if isShutdownCanceled(err) {
			v.stat.ObserveDrain("push.Subscribe")
		} else {
			v.stat.ObserveErr("push.Subscribe")
		}
		return
	}
	v.stat.Counters.SubscribeActive.Add(1)
	defer v.stat.Counters.SubscribeActive.Add(-1)
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

// doAction 执行一类大厅操作。
func (v *VU) doAction(ctx context.Context, a behavior.Action) {
	switch a {
	case behavior.ActionLocatorSetLocation:
		v.actLocator(ctx)
	case behavior.ActionPlayerGetProfile:
		v.actGetProfile(ctx)
	case behavior.ActionTeamGetMyTeam:
		v.actGetMyTeam(ctx)
	case behavior.ActionFriendListFriends:
		v.actListFriends(ctx)
	case behavior.ActionChatSendMessage:
		v.actSendMessage(ctx)
	case behavior.ActionAuctionListMarket:
		v.actListMarket(ctx)
	case behavior.ActionMatchFlow:
		v.actMatchFlow(ctx)
	}
}

func (v *VU) actLocator(ctx context.Context) {
	_ = v.timed("locator.SetLocation", func() error {
		_, e := v.pool.Locator.SetLocation(v.authCtx(ctx), &locatorv1.SetLocationRequest{
			PlayerId: v.playerID,
			Location: &locatorv1.Location{
				State:       locatorv1.LocationState_LOCATION_STATE_HUB,
				ShardId:     v.cfg.Router.CellID,
				UpdatedAtMs: time.Now().UnixMilli(),
			},
		})
		return e
	})
}

func (v *VU) actGetProfile(ctx context.Context) {
	_ = v.timed("player.GetProfile", func() error {
		_, e := v.pool.Player.GetProfile(v.authCtx(ctx), &playerv1.GetProfileRequest{PlayerId: v.playerID})
		return e
	})
}

func (v *VU) actGetMyTeam(ctx context.Context) {
	_ = v.timed("team.GetMyTeam", func() error {
		// e 是 GetMyTeam 的 RPC 错误;玩家身份由鉴权上下文提供,请求体故意留空。
		_, e := v.pool.Team.GetMyTeam(v.authCtx(ctx), &teamv1.GetMyTeamRequest{})
		return e
	})
}

func (v *VU) actListFriends(ctx context.Context) {
	_ = v.timed("friend.ListFriends", func() error {
		_, e := v.pool.Friend.ListFriends(v.authCtx(ctx), &friendv1.ListFriendsRequest{})
		return e
	})
}

func (v *VU) actSendMessage(ctx context.Context) {
	_ = v.timed("chat.SendMessage", func() error {
		_, e := v.pool.Chat.SendMessage(v.authCtx(ctx), &chatv1.SendMessageRequest{
			Channel:   chatv1.ChatChannel_CHAT_CHANNEL_WORLD,
			Content:   "stress hi",
			RequestId: client.NewTraceID(),
		})
		return e
	})
}

func (v *VU) actListMarket(ctx context.Context) {
	_ = v.timed("auction.ListMarket", func() error {
		_, e := v.pool.Auction.ListMarket(v.authCtx(ctx), &auctionv1.ListMarketRequest{
			MarketId: 1,
			Side:     auctionv1.OrderSide_ORDER_SIDE_UNSPECIFIED,
			Limit:    20,
		})
		return e
	})
}

// actMatchFlow 跑一条简化的组队→匹配→确认→战斗上报链路。
// 失败任意一步即提前返回(压测下后端可能限流 / 撮合不齐,容忍局部失败)。
func (v *VU) actMatchFlow(ctx context.Context) {
	// 1) 建队(单人队即可施压撮合 / 锚定埋点)。
	var teamID uint64
	if err := v.timed("team.CreateTeam", func() error {
		// resp 保存建队结果,e 保存 RPC 错误;player_id 已改为只信任鉴权上下文,请求体保持为空。
		resp, e := v.pool.Team.CreateTeam(v.authCtx(ctx), &teamv1.CreateTeamRequest{})
		if e != nil {
			return e
		}
		if resp.GetCode() != commonv1.ErrCode_OK {
			return fmt.Errorf("create_team code=%v", resp.GetCode())
		}
		teamID = resp.GetTeamId()
		return nil
	}); err != nil || teamID == 0 {
		return
	}

	// 本轮结束后尽力离队,避免下一轮 CreateTeam 撞 ErrTeamAlreadyInTeam(harness 清理,非压测指标)。
	defer v.leaveTeamBestEffort(ctx, teamID)

	// 2) SetReady。2026-08-17 起 ready 已非开局门槛(LoL 式流程,BeginTeamMatch 对
	// FORMING 直接放行),保留调用只为顺带覆盖 expand 期的兼容路径;失败不再中断本轮
	// (新 team 下它只是无关紧要的显示位,不该拖垮匹配漏斗)。
	_ = v.timed("team.SetReady", func() error {
		resp, e := v.pool.Team.SetReady(v.authCtx(ctx), &teamv1.SetReadyRequest{
			TeamId: teamID,
			Ready:  true,
			HeroId: 1,
		})
		if e != nil {
			return e
		}
		if resp.GetCode() != commonv1.ErrCode_OK {
			return fmt.Errorf("set_ready code=%v", resp.GetCode())
		}
		return nil
	})

	// 3) 入队匹配。StartMatch 返回 ticket_id 作为排队句柄(不是最终 match_id)。
	var ticketHandle uint64
	if err := v.timed("match.StartMatch", func() error {
		resp, e := v.pool.Matchmaker.StartMatch(v.authCtx(ctx), &matchv1.StartMatchRequest{TeamId: teamID})
		if e != nil {
			return e
		}
		if resp.GetCode() != commonv1.ErrCode_OK {
			return fmt.Errorf("start_match code=%v", resp.GetCode())
		}
		ticketHandle = resp.GetMatchId()
		return nil
	}); err != nil || ticketHandle == 0 {
		return
	}
	v.stat.Counters.MatchEnqueue.Add(1)

	// matched 标记本轮是否已成局(进入 READY/ALLOCATING 等确认后阶段),决定收尾是否取消票据。
	matched := false

	// 入队后本轮结束尽力退出匹配队列(释放 player→ticket claim):
	//   - 未成局(未撮到 / 放弃轮询)的票据若不取消,残留 claim 会让下一轮 StartMatch 撞
	//     ErrMatchAlreadyMatching(4002)——LeaveTeam best-effort 消不掉(它只释放 team claim,
	//     撮合票据 claim 在 matchmaker 侧,要靠 CancelMatch)。
	//   - 已成局的票据不取消:成功上报 battle 后由 battle_result.ReleaseMatch 清理;且对仍处
	//     确认期(CONFIRM)的已成局票据调 CancelMatch 会触发服务端 ConfirmMatch(false),把这局
	//     判失败,反而污染撮合 —— 与 auto_confirm 下 ConfirmMatch 报错同源。故成局后不取消。
	defer func() {
		if !matched {
			v.cancelMatchBestEffort(ctx)
		}
	}()

	// 4) 轮询撮合进度。pollMatch 同时回传服务端真正的 match_id
	//    (排队期回 ticket_id,成局后回 snowflake match_id)。
	stage, realMatchID := v.pollMatch(ctx, ticketHandle)

	// 仅手动确认模式(auto_confirm_match=false)才需要客户端确认来推进 match。
	// auto_confirm 下服务端撮合后自动确认并直接拉 DS:确认期 ~1s 内 match 仍持久化为 CONFIRM
	// (onAllConfirmed 在 AllocateBattle 返回后才写 READY),此时 VU 若抢发 ConfirmMatch 会与
	// 「自动确认→拉DS→上报→释放」流水线竞态,撞上已推进/已删除的 match 而报错(上一轮 76 个
	// match.ConfirmMatch error 即此)。故 auto 模式 VU 不发 ConfirmMatch,只轮询到 READY。
	if !v.cfg.AutoConfirmMatch &&
		(stage == matchv1.MatchStage_MATCH_STAGE_FOUND || stage == matchv1.MatchStage_MATCH_STAGE_CONFIRM) {
		if err := v.timed("match.ConfirmMatch", func() error {
			resp, e := v.pool.Matchmaker.ConfirmMatch(v.authCtx(ctx), &matchv1.ConfirmMatchRequest{
				MatchId: realMatchID,
				Accept:  true,
			})
			if e != nil {
				return e
			}
			if resp.GetCode() != commonv1.ErrCode_OK {
				return fmt.Errorf("confirm_match code=%v", resp.GetCode())
			}
			return nil
		}); err == nil {
			stage, realMatchID = v.pollMatch(ctx, ticketHandle)
		}
	}

	// 进入确认后阶段(READY/ALLOCATING)即视为成局,计一次 MatchConfirmed。不论自动/手动确认
	// 都按"观测到成局"口径计数,避免 auto 模式下 match_confirmed 恒为 0 让 enq→conf→disp→battle
	// 漏斗断裂、被误读成确认环节全挂。
	if stage == matchv1.MatchStage_MATCH_STAGE_READY ||
		stage == matchv1.MatchStage_MATCH_STAGE_ALLOCATING {
		matched = true
		v.stat.Counters.MatchConfirmed.Add(1)
	}

	// 5) 成局后,阶段 1 stub 模式由 robot 代 DS 上报 battle_result。
	//    必须用服务端真正的 match_id 上报:用 ticket_id 上报会让 battle_result.ReleaseMatch
	//    清不掉本局成员的 player→ticket claim,玩家回 Hub 再 StartMatch 必撞 4002。
	if v.cfg.DSMode == "stub" && matched {
		v.stat.Counters.MatchDispatched.Add(1)
		battleMatchID := realMatchID
		if battleMatchID == 0 {
			battleMatchID = ticketHandle
		}
		v.reportBattle(ctx, battleMatchID)
	}
}

// leaveTeamBestEffort 尽力离队收尾:记录时延,但「已在战斗 / 已解散 / 不在队」等预期业务码
// 不计入错误(不走 timed),避免每轮重复 CreateTeam 撞 ErrTeamAlreadyInTeam 污染 error 计数。
func (v *VU) leaveTeamBestEffort(ctx context.Context, teamID uint64) {
	start := time.Now()
	_, _ = v.pool.Team.LeaveTeam(v.authCtx(ctx), &teamv1.LeaveTeamRequest{
		TeamId: teamID,
	})
	v.stat.ObserveRPC(float64(time.Since(start).Microseconds()) / 1000.0)
}

// cancelMatchBestEffort 尽力退出匹配队列,释放本人 player→ticket claim,避免下一轮
// StartMatch 撞 ErrMatchAlreadyMatching(4002)。已成局并上报 battle 的票据此时已被
// battle_result.ReleaseMatch 清掉,CancelMatch 查无票据(ErrMatchNotFound)属预期,
// 记录时延但不计错误(不走 timed)。服务端 CancelMatch 以 JWT player_id 反查本人票据,
// 请求体字段被忽略,authCtx 已带身份;match_id 传 0 即可。
func (v *VU) cancelMatchBestEffort(ctx context.Context) {
	start := time.Now()
	_, _ = v.pool.Matchmaker.CancelMatch(v.authCtx(ctx), &matchv1.CancelMatchRequest{
		PlayerId: v.playerID,
	})
	v.stat.ObserveRPC(float64(time.Since(start).Microseconds()) / 1000.0)
}

// pollMatch 轮询匹配进度若干次,返回最后看到的阶段与服务端真正的 match_id。
// 入参 ticketHandle 是 StartMatch 返回的排队句柄;progress.match_id 在排队期回的是
// ticket_id,成局后回 snowflake match_id(上报 battle / ReleaseMatch 必须用它)。
func (v *VU) pollMatch(ctx context.Context, ticketHandle uint64) (matchv1.MatchStage, uint64) {
	stage := matchv1.MatchStage_MATCH_STAGE_UNSPECIFIED
	realMatchID := uint64(0)
	for i := 0; i < 30; i++ {
		select {
		case <-ctx.Done():
			return stage, realMatchID
		default:
		}
		_ = v.timed("match.GetMatchProgress", func() error {
			resp, e := v.pool.Matchmaker.GetMatchProgress(v.authCtx(ctx), &matchv1.GetMatchProgressRequest{MatchId: ticketHandle})
			if e != nil {
				return e
			}
			if p := resp.GetProgress(); p != nil {
				stage = p.GetStage()
				if id := p.GetMatchId(); id != 0 {
					realMatchID = id
				}
			}
			return nil
		})
		if stage == matchv1.MatchStage_MATCH_STAGE_READY ||
			stage == matchv1.MatchStage_MATCH_STAGE_FAILED {
			return stage, realMatchID
		}
		time.Sleep(300 * time.Millisecond)
	}
	return stage, realMatchID
}

// reportBattle 模拟 DS 结算上报(stub 模式下由 robot 代 DS,MMR 仍由后端算)。
func (v *VU) reportBattle(ctx context.Context, matchID uint64) {
	now := time.Now().UnixMilli()
	result := &battlev1.BattleResult{
		MatchId:     matchID,
		StartedAtMs: now - 600000,
		EndedAtMs:   now,
		WinnerTeam:  0,
		DsPodName:   "stressbot-stub",
		GameMode:    "5v5",
		MapId:       1,
		Outcome:     battlev1.BattleOutcome_BATTLE_OUTCOME_NORMAL,
		Stats: []*battlev1.PlayerStats{{
			PlayerId: v.playerID,
			HeroId:   1,
			Team:     0,
			Kills:    int32(v.rng.Intn(10)),
			Deaths:   int32(v.rng.Intn(10)),
			Assists:  int32(v.rng.Intn(15)),
		}},
	}
	if err := v.timed("battle.ReportResult", func() error {
		resp, e := v.pool.BattleResult.ReportResult(v.authCtx(ctx), &battlev1.ReportResultRequest{Result: result})
		if e != nil {
			return e
		}
		if resp.GetCode() != commonv1.ErrCode_OK {
			return fmt.Errorf("report_result code=%v", resp.GetCode())
		}
		return nil
	}); err == nil {
		v.stat.Counters.BattleReported.Add(1)
	}
}
