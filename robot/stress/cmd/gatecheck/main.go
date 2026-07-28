// Command gatecheck 是 INC-20260727-001 验收门 A/B 的一次性合成驱动:
// 用 stressbot 同款直连后端方式(注入 x-pandora-player-id,不走 Envoy/UDP)驱动
// login → SetLocation(HUB) → CreateTeam → SetReady → StartMatch(map_id) → 轮询进度,
// 拿到 READY 后保持 locator presence 存活 -watch 秒,期间由操作者在 allocator 侧
// 观测业务心跳连续性(门 A:≥60s/≥12 拍/最大间隔<15s/不删 Pod)或注入 warming Pod kill
// (门 B:~40s 内重试成功)。本进程不连 DS UDP —— 正好构成门 A 要求的「无客户端运行」。
//
// 用法(先对 login/team/matchmaker-pve/player-locator 做 kubectl port-forward):
//
//	go run ./cmd/gatecheck -map 8 -watch 90
//	go run ./cmd/gatecheck -map 8 -poll-timeout 240s   # 覆盖撮合+冷加载等待上限
//
// 注意:走 PVE walk-in matchmaker(单人直进副本);READY 后不 CancelMatch(对已成局
// 票据取消会触发服务端判负,污染验证),残局由 DS 无人进场逻辑/battle_result 正常收尾。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luyuancpp/pandora/robot/stress/internal/client"
	"github.com/luyuancpp/pandora/robot/stress/internal/scenario"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

func main() {
	var (
		loginAddr   = flag.String("login", "127.0.0.1:50001", "login gRPC 地址(port-forward)")
		teamAddr    = flag.String("team", "127.0.0.1:50010", "team gRPC 地址")
		matchAddr   = flag.String("match", "127.0.0.1:50018", "matchmaker gRPC 地址(默认 PVE walk-in 实例)")
		locatorAddr = flag.String("locator", "127.0.0.1:50006", "player_locator gRPC 地址")
		account     = flag.String("account", "", "登录账号(空=gatecheck-<时间戳>,dev devAutoRegister 自动建号)")
		mapID       = flag.Uint("map", 8, "StartMatch map_id(8=Artic01)")
		pollTimeout = flag.Duration("poll-timeout", 240*time.Second, "等 READY 的上限(含 warming 冷加载)")
		watch       = flag.Duration("watch", 90*time.Second, "READY 后保持存活的观测窗口")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *account == "" {
		*account = fmt.Sprintf("gatecheck-%d", time.Now().Unix())
	}
	logf("account=%s map_id=%d", *account, *mapID)

	pool, err := client.New(scenario.Targets{
		Login:      *loginAddr,
		Team:       *teamAddr,
		Matchmaker: *matchAddr,
		Locator:    *locatorAddr,
	})
	if err != nil {
		fatal("建连失败: %v", err)
	}
	defer pool.Close()

	// 1) 登录拿 player_id(dev devSkipPassword,PasswordHash 占位)。
	loginResp, err := pool.Login.Login(client.OutgoingContext(ctx, 0), &loginv1.LoginRequest{
		Account:       *account,
		PasswordHash:  "gatecheck",
		DeviceId:      "gatecheck-0",
		ClientVersion: "gatecheck-1",
		Region:        "dev",
		Locale:        "zh-CN",
	})
	if err != nil {
		fatal("login rpc: %v", err)
	}
	if loginResp.GetCode() != commonv1.ErrCode_OK || loginResp.GetPlayerId() == 0 {
		fatal("login code=%v player_id=%d", loginResp.GetCode(), loginResp.GetPlayerId())
	}
	playerID := loginResp.GetPlayerId()
	logf("login ok player_id=%d", playerID)
	auth := func(ctx context.Context) context.Context { return client.OutgoingContext(ctx, playerID) }

	// 2) HUB presence + 周期刷新(防 matchmaker liveness 判离线葬送在途局)。
	setPresence := func() {
		_, e := pool.Locator.SetLocation(auth(ctx), &locatorv1.SetLocationRequest{
			PlayerId: playerID,
			Location: &locatorv1.Location{
				State:       locatorv1.LocationState_LOCATION_STATE_HUB,
				UpdatedAtMs: time.Now().UnixMilli(),
			},
		})
		if e != nil {
			logf("locator.SetLocation err(容忍): %v", e)
		}
	}
	setPresence()
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				setPresence()
			}
		}
	}()

	// 3) 建单人队并 READY(matchmaker 的 team 校验前置)。
	teamResp, err := pool.Team.CreateTeam(auth(ctx), &teamv1.CreateTeamRequest{})
	if err != nil || teamResp.GetCode() != commonv1.ErrCode_OK || teamResp.GetTeamId() == 0 {
		fatal("create_team err=%v code=%v", err, teamResp.GetCode())
	}
	teamID := teamResp.GetTeamId()
	logf("team ok team_id=%d", teamID)
	if resp, e := pool.Team.SetReady(auth(ctx), &teamv1.SetReadyRequest{TeamId: teamID, Ready: true, HeroId: 1}); e != nil || resp.GetCode() != commonv1.ErrCode_OK {
		fatal("set_ready err=%v code=%v", e, resp.GetCode())
	}

	// 4) 入队(map_id 指定副本,PVE walk-in 单人直进)。
	startAt := time.Now()
	smResp, err := pool.Matchmaker.StartMatch(auth(ctx), &matchv1.StartMatchRequest{TeamId: teamID, MapId: uint32(*mapID)})
	if err != nil || smResp.GetCode() != commonv1.ErrCode_OK || smResp.GetMatchId() == 0 {
		fatal("start_match err=%v code=%v", err, smResp.GetCode())
	}
	ticket := smResp.GetMatchId()
	logf("start_match ok ticket=%d", ticket)

	// 5) 轮询进度直到 READY/FAILED/超时,打印每次阶段变化(带耗时)。
	var (
		lastStage matchv1.MatchStage
		matchID   uint64
		dsAddr    string
	)
	deadline := time.Now().Add(*pollTimeout)
poll:
	for {
		if time.Now().After(deadline) {
			fatal("等 READY 超时(%s),last_stage=%v", *pollTimeout, lastStage)
		}
		select {
		case <-ctx.Done():
			fatal("中断,last_stage=%v", lastStage)
		case <-time.After(1 * time.Second):
		}
		resp, e := pool.Matchmaker.GetMatchProgress(auth(ctx), &matchv1.GetMatchProgressRequest{MatchId: ticket})
		if e != nil {
			logf("get_progress err(容忍): %v", e)
			continue
		}
		p := resp.GetProgress()
		if p == nil {
			continue
		}
		if id := p.GetMatchId(); id != 0 {
			matchID = id
		}
		if a := p.GetBattleDsAddr(); a != "" {
			dsAddr = a
		}
		if p.GetStage() != lastStage {
			lastStage = p.GetStage()
			logf("stage=%v match_id=%d ds_addr=%q t+%.1fs", lastStage, matchID, dsAddr, time.Since(startAt).Seconds())
		}
		switch lastStage {
		case matchv1.MatchStage_MATCH_STAGE_READY:
			break poll
		case matchv1.MatchStage_MATCH_STAGE_FAILED:
			fatal("match FAILED match_id=%d t+%.1fs", matchID, time.Since(startAt).Seconds())
		}
	}
	logf("READY match_id=%d ds_addr=%s total=%.1fs —— 开始 %s 观测窗口(本进程不连 DS,构成门 A 无客户端场景)", matchID, dsAddr, time.Since(startAt).Seconds(), *watch)

	// 6) 观测窗口:保持 presence,把心跳观测留给操作者(allocator 日志/DS 日志)。
	select {
	case <-ctx.Done():
	case <-time.After(*watch):
	}
	logf("观测窗口结束,退出(已成局票据按约定不 CancelMatch)")
}

func logf(format string, args ...any) {
	fmt.Printf("[%s] "+format+"\n", append([]any{time.Now().UTC().Format("15:04:05.000")}, args...)...)
}

func fatal(format string, args ...any) {
	logf("FATAL "+format, args...)
	os.Exit(1)
}
