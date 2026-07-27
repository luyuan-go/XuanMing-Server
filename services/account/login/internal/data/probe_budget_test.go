// probe_budget_test.go —— 登录链跨服务探测子预算回归(压测审核【必修-1】,2026-07-26)。
//
// 覆盖:locator / matchmaker 探测调用必须携带独立子预算(下游看到的 deadline 被收紧到
// ≤ 各自 probe timeout),且父 ctx 更紧时不放宽(WithTimeout 只收紧语义)。
package data

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

// fakeLocatorClient 只覆写本测试触达的两个方法,捕获下游看到的 ctx。
type fakeLocatorClient struct {
	locatorv1.PlayerLocatorServiceClient
	gotCtx context.Context
}

func (f *fakeLocatorClient) GetLocation(
	ctx context.Context, _ *locatorv1.GetLocationRequest, _ ...grpc.CallOption,
) (*locatorv1.GetLocationResponse, error) {
	f.gotCtx = ctx
	return &locatorv1.GetLocationResponse{Code: commonv1.ErrCode_OK}, nil
}

func (f *fakeLocatorClient) SetLocation(
	ctx context.Context, _ *locatorv1.SetLocationRequest, _ ...grpc.CallOption,
) (*locatorv1.SetLocationResponse, error) {
	f.gotCtx = ctx
	return &locatorv1.SetLocationResponse{Code: commonv1.ErrCode_OK}, nil
}

// wantDeadlineWithin 断言 ctx 带 deadline 且距 now 不超过 max(留 100ms 调度余量下限)。
func wantDeadlineWithin(t *testing.T, ctx context.Context, max time.Duration) {
	t.Helper()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("下游 ctx 必须带 deadline(子预算未生效)")
	}
	remain := time.Until(dl)
	if remain > max {
		t.Fatalf("下游 deadline 剩余 %v,超过子预算 %v(未收紧)", remain, max)
	}
	if remain <= 0 {
		t.Fatalf("下游 deadline 已过期: %v", remain)
	}
}

func TestGetBattleLocation_AppliesProbeBudget(t *testing.T) {
	fake := &fakeLocatorClient{}
	n := &GrpcLocationNotifier{client: fake}
	if _, err := n.GetBattleLocation(context.Background(), 42); err != nil {
		t.Fatalf("GetBattleLocation: %v", err)
	}
	wantDeadlineWithin(t, fake.gotCtx, locatorProbeTimeout)
}

func TestNotifyLoginPending_AppliesProbeBudget(t *testing.T) {
	fake := &fakeLocatorClient{}
	n := &GrpcLocationNotifier{client: fake}
	if err := n.NotifyLoginPending(context.Background(), 42, "dev-1"); err != nil {
		t.Fatalf("NotifyLoginPending: %v", err)
	}
	wantDeadlineWithin(t, fake.gotCtx, locatorProbeTimeout)
}

func TestResolvePlayerMatchContext_AppliesProbeBudget(t *testing.T) {
	fake := &fakeMatchServiceClient{resp: &matchv1.ResolvePlayerMatchContextResponse{
		Code: commonv1.ErrCode_OK,
	}}
	r := &GrpcMatchContextResolver{client: fake}
	if _, err := r.ResolvePlayerMatchContext(context.Background(), 42); err != nil {
		t.Fatalf("ResolvePlayerMatchContext: %v", err)
	}
	wantDeadlineWithin(t, fake.gotCtx, matchResolveTimeout)
}

func TestProbeBudget_NeverExtendsTighterParent(t *testing.T) {
	// 父 ctx 只剩 500ms 时,子预算不得把 deadline 放宽回 2s/3s。
	parent, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	fake := &fakeLocatorClient{}
	n := &GrpcLocationNotifier{client: fake}
	if _, err := n.GetBattleLocation(parent, 42); err != nil {
		t.Fatalf("GetBattleLocation: %v", err)
	}
	wantDeadlineWithin(t, fake.gotCtx, 500*time.Millisecond)
}
