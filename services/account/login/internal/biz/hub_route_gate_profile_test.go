// hub_route_gate_profile_test.go — 战斗门降级档位判据回归测试(2026-07-29 审计修复)。
//
// 修复前的缺陷:战斗门的所有「查不到就放行」降级分支只按 requireHubAssignmentBinding 分档,
// 而 resolveHub 决定「这张票是不是生产级权威票」用的是 (requireHubAssignmentBinding ||
// rs256DSTicketProfileEnabled)。两轴正交,于是存在一个组合:
//
//	ds_ticket verifier 已配(RS256 v2 生效) + require_hub_assignment_binding=false
//
// 在该组合下,依赖抖动(locator 查询失败 / matchmaker RPC 失败 / 耐久权威 UNKNOWN)会让门
// 放行,而玩家随后拿到的是 hub_allocator 签发、DS 会正常接受的**正式绑定票**——于是仍在
// 对局中的玩家同时进入 Hub DS,违反 §9 不变量 1「玩家同一时刻只能在一个可操作 DS」。
//
// 修复:全部降级分支改用 strictBattleGateProfile()(= require || rs256),与出票档位判据同源。
// 本文件的每个用例在修复前都会失败(门放行),修复后必须 fail-closed 返回 ErrUnavailable。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// newRS256WeakBindingUsecase 构造审计命中的危险组合:RS256 v2 已启用,但归属绑定栅栏未激活。
func newRS256WeakBindingUsecase(t *testing.T, notifier data.LocationNotifier) *LoginUsecase {
	t.Helper()
	uc := newHubGateUsecase(t, nil, notifier, &loginBattleAuthorizerFake{}, false)
	keys := newHubV2TestKeys(t)
	uc.v2Verifier = keys.verifier
	if !uc.rs256DSTicketProfileEnabled() {
		t.Fatal("测试前提失效:v2Verifier 注入后 rs256 档位应为 true")
	}
	if uc.requireHubAssignmentBinding {
		t.Fatal("测试前提失效:本用例要求 require_hub_assignment_binding=false")
	}
	return uc
}

func TestStrictBattleGateProfile_TracksEitherAxis(t *testing.T) {
	// 判据必须是两轴的并集,且与 resolveHub 出票判据同源。
	cases := []struct {
		name    string
		require bool
		rs256   bool
		want    bool
	}{
		{"两轴都关=legacy HS256 dev 裸跑档,允许弱降级", false, false, false},
		{"仅归属绑定激活", true, false, true},
		{"仅 RS256 激活(修复前被漏判为弱档)", false, true, true},
		{"两轴都开", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := newHubGateUsecase(t, nil, nil, &loginBattleAuthorizerFake{}, tc.require)
			if tc.rs256 {
				uc.v2Verifier = newHubV2TestKeys(t).verifier
			}
			if got := uc.strictBattleGateProfile(); got != tc.want {
				t.Fatalf("strictBattleGateProfile() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHubRouteGate_LocatorError_RS256WeakBinding_FailClosed(t *testing.T) {
	// 修复前:require=false → 走弱降级放行 → 仍在对局的玩家可拿到正式绑定票进 Hub。
	notifier := &fakeNotifier{blErr: errors.New("locator dial timeout")}
	uc := newRS256WeakBindingUsecase(t, notifier)

	_, err := uc.guardHubRouteAgainstActiveBattle(context.Background(), 42)
	wantCode(t, err, errcode.ErrUnavailable)
}

func TestHubRouteGate_NoNotifier_RS256WeakBinding_FailClosed(t *testing.T) {
	// locator 未装配同样不得在 RS256 档放行:没有 presence 就无法证明玩家不在战斗。
	uc := newRS256WeakBindingUsecase(t, nil)

	_, err := uc.guardHubRouteAgainstActiveBattle(context.Background(), 42)
	wantCode(t, err, errcode.ErrUnavailable)
}

func TestResolveBattleAuthority_MatchResolverError_RS256WeakBinding_FailClosed(t *testing.T) {
	// presence 说“不在战斗”+ 耐久权威查询失败:这正是 READY 局 locator 投影蒸发的常态窗口,
	// 修复前会退回 presence 判定并放行,把仍在对局的玩家送进 Hub。
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newRS256WeakBindingUsecase(t, notifier)
	uc.matchResolver = &fakeMatchResolver{err: errors.New("matchmaker unavailable")}

	_, _, err := uc.resolveBattleAuthority(context.Background(), 42)
	wantCode(t, err, errcode.ErrUnavailable)
}
