// Package middleware 提供 Pandora 自研的 Kratos middleware。
//
// 跟 Kratos 自带 middleware(recovery / tracing / logging / metadata)的区别:
// 这里的 middleware 跟 Pandora 业务约定耦合,比如:
//   - trace.go     从 Pandora metadata key 提取 / 注入 trace_id(跟 mmorpg 风格对齐)
//   - auth.go      JWT 解析 + 注入 player_id 到 ctx
//   - metrics.go   Prometheus 指标命名按 docs/design/infra.md §10 规范
//   - logging.go   access log 字段约定按 docs/design/infra.md §11
//
// 设计上 gRPC server / HTTP server / gRPC client 都能复用同一个 middleware
// (Kratos middleware.Middleware 是协议无关的)。
package middleware

import (
	"context"
	"strconv"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/google/uuid"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// MetadataKeyTraceID 是 Pandora 跨服务传递的 trace_id metadata key。
//
// gRPC 走 grpc metadata,HTTP 走 header(Kratos transport 统一抽象)。
// 命名大小写不敏感,跟 mmorpg 风格对齐:`x-pandora-trace-id`。
const MetadataKeyTraceID = "x-pandora-trace-id"

// MetadataKeyPlayerID 是 player_id metadata key,Envoy / gateway 鉴权后注入。
const MetadataKeyPlayerID = "x-pandora-player-id"

// MetadataKeyAccountID 是 account_id metadata key(两步登录,2026-08-18),
// 由 Envoy 的**账号态 provider**(aud=pandora-account)从 sub claim 注入。
//
// ⚠️ 与 MetadataKeyPlayerID 是两个独立通道,绝不可互相顶替:
// account_id 是账号身份,player_id 是角色实体身份。账号态 token 只能解锁
// ListAccountRoles / EnterRole;若把 account_id 写进 x-pandora-player-id,
// 它会被全后端当成玩家身份 —— 直接越权。两条通道由不同 aud 的 provider 各自注入,
// 且都在入站第一时间被无条件剥离,只有验签成功才由 Envoy 重写。
const MetadataKeyAccountID = "x-pandora-account-id"

// MetadataKeyJWTPayload 是 Envoy jwt_authn 验签成功后透传的 JWT payload 头
// (forward_payload_header,base64url JSON)。客户端面入站第一时间无条件剥离本头,
// 只有验签成功才由 Envoy 重写,因此其中的 jti/sub 在 :8443 面可信
// (deploy/envoy/envoy.yaml header_mutation + jwt_authn 说明)。
// 该常量只统一可信请求头名称,不表示任意调用方自行写入的同名头都可信。
const MetadataKeyJWTPayload = "x-pandora-jwt-payload"

// Trace 是 trace_id 注入 / 透传 middleware,server / client 都用同一份。
//
// Server 侧:从 incoming metadata 找 x-pandora-trace-id;没有则生成 UUID;塞进 ctx + 回程 header。
// Client 侧:从 ctx(plog)取 trace_id;没有则生成 UUID;只写 outgoing metadata。
//
// 方向判定按「本次调用的 transport」二选一,client 分支不得触碰 server transport:
// server handler 内(含由请求 ctx 派生的异步 goroutine)发起下游调用时,ctx 里同时带着
// 入站请求的 server transport 和本次调用的 client transport;此时若再走
// FromServerContext 写 ReplyHeader,就会与 gRPC 正在发送响应时对同一 metadata Map 的
// 遍历并发(grpc metadata.Join / SetHeader),触发
// fatal error: concurrent map iteration and map write(2026-07-21 ds_allocator Heartbeat 崩溃)。
//
// 用法:
//
//	srv := kgrpc.NewServer(kgrpc.Middleware(middleware.Trace()))
func Trace() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			// Client 侧:本次调用存在 client transport ⇒ 这是 client hop。
			// trace_id 只从 ctx 值取(server 侧 middleware 已放入),不读 server transport。
			if tr, ok := transport.FromClientContext(ctx); ok {
				traceID, _ := ctx.Value(plog.CtxKeyTraceID).(string)
				if traceID == "" {
					traceID = uuid.NewString()
					ctx = plog.WithTraceID(ctx, traceID)
				}
				tr.RequestHeader().Set(MetadataKeyTraceID, traceID)
				return handler(ctx, req)
			}

			// Server 侧:从 incoming metadata 提取 / 生成,写 ctx + 回程 header。
			// 此时 handler 尚未开始组装响应,写 ReplyHeader 与响应发送不并发。
			traceID := extractTraceID(ctx)
			if traceID == "" {
				traceID = uuid.NewString()
			}
			ctx = plog.WithTraceID(ctx, traceID)
			if tr, ok := transport.FromServerContext(ctx); ok {
				tr.ReplyHeader().Set(MetadataKeyTraceID, traceID)
			}
			return handler(ctx, req)
		}
	}
}

// MaxTraceIDLen 是入站 trace_id 的长度上限。UUID 是 36 字符,留一倍余量给
// 「客户端本地 trace + 序号」这类拼接格式,超出即视为不可信、改为服务端重新生成。
const MaxTraceIDLen = 64

// extractTraceID 从 Kratos transport 抽象中拿 trace_id(server 入站方向)。
//
// ⚠️ 客户端面(:8443)的 envoy 只无条件剥离 x-pandora-player-id / x-pandora-jwt-payload
// 这两个**身份**头,x-pandora-trace-id 是原样透传的——即 UE 客户端 / DS 自带的 trace_id
// 会被后端全链采纳并写进每一条日志。trace_id 不是信任边界(不参与鉴权、不做 metric label),
// 采纳外部值正是跨进程串联所必需;但取值本身必须先过闸,否则一个畸形客户端就能往全服日志里
// 灌任意长度 / 任意字节的内容(日志膨胀、下游日志系统解析异常、伪造出以假乱真的 trace 行)。
// 故这里只接受「有界长度 + 安全字符集」的值,不合规一律丢弃并由调用方生成新的。
func extractTraceID(ctx context.Context) string {
	if tr, ok := transport.FromServerContext(ctx); ok {
		if v := tr.RequestHeader().Get(MetadataKeyTraceID); isSafeTraceID(v) {
			return v
		}
	}
	return ""
}

// isSafeTraceID 判定入站 trace_id 是否可直接采纳。
//
// 允许字符集 = ASCII 字母数字 + '-' + '_',覆盖 UUID、hex、base64url 风格的 ID。
// 显式排除空格 / 控制字符 / 换行 / 非 ASCII:换行会让按行切分的日志管道把一条日志
// 拆成多条(可伪造出看似来自其它服务的日志行),控制字符与多字节序列则会打穿下游
// 解析器。空值返回 false,让调用方走生成分支。
func isSafeTraceID(v string) bool {
	if v == "" || len(v) > MaxTraceIDLen {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_':
		default:
			return false
		}
	}
	return true
}

// extractPlayerID 从 metadata 拿 player_id(Envoy / gateway 鉴权后注入到 header)。
//
// Returns 0 if not present.
func extractPlayerID(ctx context.Context) uint64 {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return 0
	}
	v := tr.RequestHeader().Get(MetadataKeyPlayerID)
	if v == "" {
		return 0
	}
	id, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// AccountIDFromContext 取账号态身份(两步登录:ListAccountRoles / EnterRole 用)。
//
// 来源只有 Envoy 账号态 provider 注入的 x-pandora-account-id 一处。取不到返回 0,
// 由业务侧 fail-closed 拒绝(直连内网端口联调时无网关注入,同样按未鉴权处理 ——
// 账号态入口不存在"匿名可用"的语义)。
//
// 刻意**不**回退去读 x-pandora-player-id:那会让一张玩家 SessionToken 也能列角色 /
// 换角色,等于把「账号态」这层隔离拆掉。
func AccountIDFromContext(ctx context.Context) uint64 {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return 0
	}
	v := tr.RequestHeader().Get(MetadataKeyAccountID)
	if v == "" {
		return 0
	}
	id, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return id
}
