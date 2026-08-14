// trace_test.go — Trace middleware 方向判定回归测试。
//
// 背景(2026-07-21 ds_allocator Heartbeat 崩溃):server handler 派生的异步 goroutine
// 经挂 Trace middleware 的 gRPC client 发下游调用时,ctx 同时携带入站请求的 server
// transport 与本次调用的 client transport;旧实现在 client hop 里也写 server 的
// ReplyHeader,与 gRPC 正在发送响应时对同一 metadata Map 的遍历并发,触发
// fatal error: concurrent map iteration and map write。
// 本文件用「server+client 双 transport」用例锁死修复行为,必须配 -race 跑。
package middleware

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kratos/kratos/v2/transport"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// headerMap 用裸 map 实现 transport.Header,写/遍历都是真实 map 操作,
// 让 race detector 能捕获与 gRPC metadata.MD 同型的并发读写。
type headerMap map[string][]string

func (h headerMap) Get(key string) string {
	if vs := h[key]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}
func (h headerMap) Set(key, value string) { h[key] = []string{value} }
func (h headerMap) Add(key, value string) { h[key] = append(h[key], value) }
func (h headerMap) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}
func (h headerMap) Values(key string) []string { return h[key] }

type mockTransport struct {
	reqHeader   headerMap
	replyHeader headerMap
}

func newMockTransport() *mockTransport {
	return &mockTransport{reqHeader: headerMap{}, replyHeader: headerMap{}}
}

func (m *mockTransport) Kind() transport.Kind            { return transport.KindGRPC }
func (m *mockTransport) Endpoint() string                { return "grpc://127.0.0.1:0" }
func (m *mockTransport) Operation() string               { return "/test.Svc/Op" }
func (m *mockTransport) RequestHeader() transport.Header { return m.reqHeader }
func (m *mockTransport) ReplyHeader() transport.Header   { return m.replyHeader }

func noopHandler(gotCtx *context.Context) func(ctx context.Context, req any) (any, error) {
	return func(ctx context.Context, req any) (any, error) {
		if gotCtx != nil {
			*gotCtx = ctx
		}
		return nil, nil
	}
}

// TestTraceServerHop:server hop 提取入站 trace_id,写 ctx + 回程 header。
func TestTraceServerHop(t *testing.T) {
	tr := newMockTransport()
	tr.reqHeader.Set(MetadataKeyTraceID, "trace-in")
	ctx := transport.NewServerContext(context.Background(), tr)

	var handlerCtx context.Context
	if _, err := Trace()(noopHandler(&handlerCtx))(ctx, nil); err != nil {
		t.Fatalf("Trace server hop: %v", err)
	}
	if got, _ := handlerCtx.Value(plog.CtxKeyTraceID).(string); got != "trace-in" {
		t.Fatalf("handler ctx trace_id = %q, want trace-in", got)
	}
	if got := tr.replyHeader.Get(MetadataKeyTraceID); got != "trace-in" {
		t.Fatalf("reply header trace_id = %q, want trace-in", got)
	}
}

// TestTraceServerHopGenerates:入站没带 trace_id 时 server hop 生成 UUID 并回写。
func TestTraceServerHopGenerates(t *testing.T) {
	tr := newMockTransport()
	ctx := transport.NewServerContext(context.Background(), tr)

	var handlerCtx context.Context
	if _, err := Trace()(noopHandler(&handlerCtx))(ctx, nil); err != nil {
		t.Fatalf("Trace server hop: %v", err)
	}
	got, _ := handlerCtx.Value(plog.CtxKeyTraceID).(string)
	if got == "" {
		t.Fatal("handler ctx trace_id empty, want generated UUID")
	}
	if reply := tr.replyHeader.Get(MetadataKeyTraceID); reply != got {
		t.Fatalf("reply header trace_id = %q, want %q", reply, got)
	}
}

// TestTraceClientHopKeepsServerReplyHeaderUntouched:回归用例。
// ctx 同时携带 server transport(继承自入站请求)与 client transport(本次下游调用):
// client hop 只写 client RequestHeader,绝不触碰 server ReplyHeader。
func TestTraceClientHopKeepsServerReplyHeaderUntouched(t *testing.T) {
	srvTr := newMockTransport()
	cliTr := newMockTransport()
	ctx := transport.NewServerContext(context.Background(), srvTr)
	ctx = plog.WithTraceID(ctx, "trace-abc")
	ctx = transport.NewClientContext(ctx, cliTr)

	if _, err := Trace()(noopHandler(nil))(ctx, nil); err != nil {
		t.Fatalf("Trace client hop: %v", err)
	}
	if got := cliTr.reqHeader.Get(MetadataKeyTraceID); got != "trace-abc" {
		t.Fatalf("client request header trace_id = %q, want trace-abc", got)
	}
	if n := len(srvTr.replyHeader); n != 0 {
		t.Fatalf("server reply header mutated by client hop: %v", srvTr.replyHeader)
	}
	if n := len(srvTr.reqHeader); n != 0 {
		t.Fatalf("server request header mutated by client hop: %v", srvTr.reqHeader)
	}
}

// TestTraceClientHopGeneratesWithoutServerCtx:纯 client 调用(无 server transport、
// ctx 无 trace_id)时生成 UUID 写 outgoing metadata,行为与旧版一致。
func TestTraceClientHopGeneratesWithoutServerCtx(t *testing.T) {
	cliTr := newMockTransport()
	ctx := transport.NewClientContext(context.Background(), cliTr)

	var handlerCtx context.Context
	if _, err := Trace()(noopHandler(&handlerCtx))(ctx, nil); err != nil {
		t.Fatalf("Trace client hop: %v", err)
	}
	sent := cliTr.reqHeader.Get(MetadataKeyTraceID)
	if sent == "" {
		t.Fatal("client request header trace_id empty, want generated UUID")
	}
	if got, _ := handlerCtx.Value(plog.CtxKeyTraceID).(string); got != sent {
		t.Fatalf("handler ctx trace_id = %q, want %q(与 outgoing metadata 一致)", got, sent)
	}
}

// TestTraceClientHopConcurrentWithReplySend:race 回归用例(必须 -race 跑)。
// 后台 goroutine 持续遍历 + 补写 server ReplyHeader(模拟 gRPC 发送响应时的
// metadata.Join / SetHeader),同时前台反复以「继承 server transport 的 ctx」跑
// client hop。旧实现会在 client hop 里写同一 ReplyHeader map,race detector 直接报
// concurrent map iteration and map write;修复后 client hop 不触碰 server transport。
func TestTraceClientHopConcurrentWithReplySend(t *testing.T) {
	srvTr := newMockTransport()
	baseCtx := transport.NewServerContext(context.Background(), srvTr)
	baseCtx = plog.WithTraceID(baseCtx, "trace-race")

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for k, vs := range srvTr.replyHeader { // 模拟响应发送侧遍历
				_, _ = k, vs
			}
			srvTr.replyHeader.Set("x-server-own", "1") // 模拟 server 自身补写
		}
	}()

	mw := Trace()
	for i := 0; i < 1000; i++ {
		cliTr := newMockTransport()
		ctx := transport.NewClientContext(baseCtx, cliTr)
		if _, err := mw(noopHandler(nil))(ctx, nil); err != nil {
			t.Fatalf("Trace client hop #%d: %v", i, err)
		}
		if got := cliTr.reqHeader.Get(MetadataKeyTraceID); got != "trace-race" {
			t.Fatalf("client request header trace_id = %q, want trace-race", got)
		}
	}
	close(stop)
	<-done
}

// TestTraceServerHopRejectsUnsafeInbound:入站 trace_id 必须先过「有界长度 + 安全字符集」闸。
//
// 背景:客户端面 envoy 只剥离身份头(x-pandora-player-id / x-pandora-jwt-payload),
// x-pandora-trace-id 原样透传,即该值由 UE 客户端 / DS 完全控制且会写进全链每条日志。
// 不合规的值必须被丢弃并改为服务端生成,绝不能原样落进 ctx。
func TestTraceServerHopRejectsUnsafeInbound(t *testing.T) {
	cases := []struct {
		name    string
		inbound string
	}{
		{"换行伪造日志行", "abc\ndef"},
		{"回车", "abc\rdef"},
		{"制表符", "abc\tdef"},
		{"空格", "abc def"},
		{"控制字符", "abc\x00def"},
		{"非 ASCII", "追踪-中文"},
		{"超长", strings.Repeat("a", MaxTraceIDLen+1)},
		{"JSON 注入", `abc","level":"error"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newMockTransport()
			tr.reqHeader.Set(MetadataKeyTraceID, tc.inbound)
			ctx := transport.NewServerContext(context.Background(), tr)

			var handlerCtx context.Context
			if _, err := Trace()(noopHandler(&handlerCtx))(ctx, nil); err != nil {
				t.Fatalf("Trace server hop: %v", err)
			}
			got, _ := handlerCtx.Value(plog.CtxKeyTraceID).(string)
			if got == tc.inbound {
				t.Fatalf("不安全的入站 trace_id 被原样采纳: %q", got)
			}
			if got == "" {
				t.Fatal("丢弃不安全入站值后未生成替代 trace_id")
			}
			if !isSafeTraceID(got) {
				t.Fatalf("生成的替代 trace_id 自身不合规: %q", got)
			}
		})
	}
}

// TestTraceServerHopAcceptsSafeInbound:合规入站值必须原样采纳,否则跨进程串不起来。
func TestTraceServerHopAcceptsSafeInbound(t *testing.T) {
	cases := []string{
		"550e8400-e29b-41d4-a716-446655440000", // UUID
		"0123456789abcdef",                     // hex
		"ue_client-42",                         // 下划线 + 连字符
		strings.Repeat("a", MaxTraceIDLen),     // 边界:恰好等于上限
	}
	for _, inbound := range cases {
		t.Run(inbound, func(t *testing.T) {
			tr := newMockTransport()
			tr.reqHeader.Set(MetadataKeyTraceID, inbound)
			ctx := transport.NewServerContext(context.Background(), tr)

			var handlerCtx context.Context
			if _, err := Trace()(noopHandler(&handlerCtx))(ctx, nil); err != nil {
				t.Fatalf("Trace server hop: %v", err)
			}
			if got, _ := handlerCtx.Value(plog.CtxKeyTraceID).(string); got != inbound {
				t.Fatalf("handler ctx trace_id = %q, want %q", got, inbound)
			}
			if got := tr.replyHeader.Get(MetadataKeyTraceID); got != inbound {
				t.Fatalf("reply header trace_id = %q, want %q", got, inbound)
			}
		})
	}
}
