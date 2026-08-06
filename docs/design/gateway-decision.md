# Pandora 网关与推送架构决策

> **状态**:已决策(2026-06-04 终版)
> **关联否决方案**:`architecture-rejected-strict-ds-only.md`(严格 A 反面教材)
> **关联协议铁律**:`protocol-ordering-rules.md`(乱序原则)
> **本文档地位**:Client ↔ 后端的核心架构总纲。任何 AI / 新开发者改动客户端连接 / 推送 / 网关前必读。

## §0 架构总览(两连接 + Kratos + Envoy + gRPC-Web)

```
                          ┌─────────────────────────────────────┐
                          │           Client(UE 5.8)           │
                          │  - 引擎自带 NetDriver(连 DS)       │
                          │  - 引擎自带 FHttpModule(连 Envoy)  │
                          │  - 自研 grpc-web 客户端(~3-5 天)  │
                          │  - 零第三方 SDK / 零 SSL 冲突       │
                          └──┬───────────────────────────────┬──┘
                             │                               │
        ┌────────────────────┘                               └────────────────────┐
        │ ① UE NetDriver(UDP-like)                          ② FHttpModule         │
        │   仅游戏内同步 / GAS / Replication                    HTTP/2 + TLS         │
        │   30~60Hz tick                                       Content-Type:        │
        │                                                       application/         │
        │                                                       grpc-web+proto       │
        ▼                                                                            ▼
┌──────────────────┐                                          ┌──────────────────────┐
│ Hub DS / Battle  │                                          │ Envoy Edge Gateway  │
│ DS(UE,Agones)  │                                          │ 端口 8443(HTTPS)    │
└──────┬───────────┘                                          │                      │
       │ Heartbeat unary                                      │ 1. TLS 终止          │
       │ 每 5s                                                 │ 2. gRPC-Web → gRPC   │
       ▼                                                      │ 3. ALPN 协商         │
┌──────────────────┐                                          │ 4. JWT 鉴权          │
│ ds_allocator(go)│                                          │ 5. 限流 / 熔断       │
│ hub_allocator(go)│                                         │ 6. 路由              │
└──────────────────┘                                          └──────────┬───────────┘
                                                                         │ 标准 gRPC
                                                                         │ unary + server stream
                                                                         ▼
                                                              ┌──────────────────────────┐
                                                              │  Kratos 业务服(14 个)  │
                                                              │ login/player/team/match/ │
                                                              │ trade/dialogue/chat/    │
                                                              │ friend/locator/         │
                                                              │ data_service/           │
                                                              │ ds_allocator/           │
                                                              │ hub_allocator/          │
                                                              │ battle_result/          │
                                                              │ ★ push(server stream)  │
                                                              └──────────┬───────────────┘
                                                                         │ produce
                                                                         ▼
                                                              ┌──────────────────────┐
                                                              │ Kafka cluster        │
                                                              │ pandora.team.*       │
                                                              │ pandora.match.*      │
                                                              │ pandora.chat.*       │
                                                              │ pandora.player.*     │
                                                              │ pandora.battle.result│
                                                              └──────────▲───────────┘
                                                                         │ consume
                                                                         │
                                                                  push 服务 ─┘
                                                                  (集中持有玩家 stream,
                                                                   按 player_id 路由 kafka 事件
                                                                   转 gRPC server stream 推给客户端)
```

**核心性质**:
- **2 条客户端连接**(无第三连接)
- **零第三方 UE SDK**(全部 UE 引擎自带)
- **零 SSL 冲突**(只用 UE OpenSSL,不引入 BoringSSL)
- **协议全标准**(gRPC-Web 是 grpc.io 官方规范)
- **故障域清晰**:DS 崩 ≠ 业务挂,业务服崩 ≠ 推送挂

---

## §1 客户端两条连接

### 1.1 连接 ①:UE NetDriver → Hub DS / Battle DS

| 维度 | 内容 |
|---|---|
| 协议 | UE 原生(基于 UDP 的可靠/不可靠混合)|
| 频率 | 30~60Hz tick |
| 用途 | **仅游戏内同步**(玩家移动 / 技能释放 / HP / buff / 命中 / AOI / Replication / GAS) |
| 谁负责 | UE 引擎自带,零开发 |
| 不能做的事 | 业务请求(组队 / 商店 / 好友 / 段位查询)— 走 ② |
| 断线影响 | 玩家暂时看不到大厅 / 战斗世界,但 UI 业务不受影响(② 还在) |

### 1.2 连接 ②:UE FHttpModule → Envoy(gRPC-Web over HTTP/2 TLS)

| 维度 | 内容 |
|---|---|
| 协议 | gRPC-Web over **HTTP/2 + TLS**(UE 5.8 官方支持) |
| 频率 | 业务请求 1~10 req/s/玩家;推送 stream 长连接 |
| 用途 | **所有业务请求 + 所有推送**(unary + server stream 复用同协议)|
| 谁负责 | 客户端:UE FHttpModule(引擎自带)+ 自研 grpc-web 协议解析;服务端:Envoy + Kratos |
| 鉴权 | 首次 Login RPC 拿 session_token → 后续所有请求 header 携带 |
| 断线 | FHttpModule 自动重连(libcurl 内置);stream 断了 push 服务从 redis 拉离线消息补推 |

**单条连接做 unary + server stream + 推送**,这是 gRPC-Web 的核心能力,不需要分两条通道。

---

## §2 gRPC-Web 协议分层详解

很多人搞混"gRPC-Web 是不是 HTTP",这里彻底说清楚。

### 2.1 三层结构

```
┌─────────────────────────────────────────────┐
│ 应用层(开发者写代码看到的)               │
│ → gRPC service stub 调用                   │
│   matchmaker.Subscribe(...) returns stream │
│   for msg := range stream { ... }          │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ 转换层(grpc-web 客户端库 / Envoy)        │
│ → 把 gRPC 抽象转成 grpc-web 字节布局        │
│   每条 stream 消息 = 1 byte flag +          │
│                       4 bytes length +      │
│                       protobuf bytes        │
│   状态码用 trailer header(grpc-status)     │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ 传输层(HTTP/2 + TLS)                      │
│ → 真实网络字节,多路复用,标准 HTTPS         │
│   POST /pandora.team.v1.TeamService/Create  │
│   Content-Type: application/grpc-web+proto  │
│   ← (HTTP/2 stream chunk 1) [grpc-web frame]│
│   ← (HTTP/2 stream chunk 2) [grpc-web frame]│
│   ← (trailer) grpc-status: 0                │
└─────────────────────────────────────────────┘
```

### 2.2 关键事实

| 问题 | 答案 |
|---|---|
| **gRPC-Web 是不是 HTTP?** | 是的,底层就是 HTTP/2(或 HTTP/1.1) |
| **跟 HTTP/JSON 一样吗?** | 不一样。payload 是 protobuf 二进制,字节少 5-10 倍,CPU 快 5-10 倍 |
| **开发者要直接写 HTTP 吗?** | 不要。开发者写 gRPC 语义(stub 调用),库自动转换 |
| **能不能跑 server stream?** | ✅ 可以(HTTP/2 stream 或 HTTP/1.1 chunked)|
| **能不能跑 client stream?** | ❌ gRPC-Web 协议规范不支持(W2 Pandora 用 unary + server stream 即可)|
| **能不能跑双向流?** | ❌ 同上,不支持。Pandora 不需要 |

### 2.3 跟纯 HTTP/JSON 性能对比

实测 `MatchProgress{ stage: READY }`:

| 协议 | 字节数 | 解析 CPU |
|---|---|---|
| HTTP/1.1 + JSON | ~250 字节(header+JSON body)| 慢 |
| **gRPC-Web over HTTP/2** | ~50 字节(HTTP/2 帧头+grpc-web frame+protobuf)| **快** |
| 纯 gRPC over HTTP/2 | ~30 字节(无 grpc-web 额外 frame 头)| 最快 |

**gRPC-Web 比 HTTP/JSON 省 5 倍流量、快 5-10 倍**。Pandora 选 gRPC-Web 性能远优于 HTTP/JSON。

---

## §3 UE 5.8 FHttpModule HTTP/2 实现指南

### 3.1 验证依据(2026-06-04 直接挖 UE 5.8 源码确认)

UE 5.8 源码路径 `Engine/Source/Runtime/Online/HTTP/`,关键 API:

**HttpConstants.h**:
```cpp
static UE_API const TCHAR* const VERSION_2TLS;
static UE_API const TCHAR* const VERSION_1_1;
```

**IHttpRequest.h**:
```cpp
namespace HttpRequestOptions {
    static const FName HttpVersion("HttpVersion");
}

virtual void SetOption(const FName Option, const FString& OptionValue) = 0;

// ⭐ Server stream 接收核心 API(line 283)
HTTP_API bool SetResponseBodyReceiveStreamDelegateV2(FHttpRequestStreamDelegateV2 StreamDelegate);

// 委托签名(line 116)
using FHttpRequestStreamDelegateV2 = TTSDelegate<void(void*/*Ptr*/, int64&/*InOutLength*/)>;

// 辅助回调
virtual FHttpRequestHeaderReceivedDelegate& OnHeaderReceived() = 0;
virtual FHttpRequestStatusCodeReceivedDelegate& OnStatusCodeReceived() = 0;
virtual FHttpRequestProgressDelegate64& OnRequestProgress64() = 0;
```

**Private/Curl/CurlHttp.cpp**:
```cpp
void FCurlHttpRequest::SetupOptionHttpVersion()
{
    const FString HttpVersion = GetOption(HttpRequestOptions::HttpVersion);
    if (HttpVersion == FHttpConstants::VERSION_2TLS) {
        curl_easy_setopt(EasyHandle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    } else if (HttpVersion == FHttpConstants::VERSION_1_1) {
        curl_easy_setopt(EasyHandle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    }
}
```

**结论**:UE 5.8 官方暴露 HTTP/2 over TLS,libcurl 后端通过 `CURL_HTTP_VERSION_2TLS` 启用。

### 3.2 重要约束:HTTP/2 必须走 TLS

UE 5.8 用的常量是 `VERSION_2TLS`,**不支持明文 HTTP/2**(h2c)。
- 生产环境本来就需要 TLS,无影响
- 本地开发期用自签证书(mkcert / openssl)

### 3.3 Pandora UE 客户端代码模板(W2 实现)

```cpp
// 发 unary 请求(如 CreateTeam)
TSharedRef<IHttpRequest> Request = FHttpModule::Get().CreateRequest();
Request->SetURL("https://pandora-gw.example.com/pandora.team.v1.TeamService/CreateTeam");
Request->SetVerb("POST");
Request->SetHeader("Content-Type", "application/grpc-web+proto");
Request->SetHeader("X-Grpc-Web", "1");
Request->SetHeader("Authorization", "Bearer " + SessionToken);

// ⭐ 启用 HTTP/2 over TLS
Request->SetOption(HttpRequestOptions::HttpVersion, FHttpConstants::VERSION_2TLS);

// gRPC-Web frame: [1 byte flag=0x00][4 bytes BE length][protobuf bytes]
Request->SetContent(GrpcWebEncodeUnary(CreateTeamReqProto));

Request->OnProcessRequestComplete().BindLambda(
    [](FHttpRequestPtr Request, FHttpResponsePtr Response, bool bSuccess) {
        if (bSuccess) {
            CreateTeamResponse Result;
            GrpcWebDecodeUnary(Response->GetContent(), Result);
            UI->ShowTeam(Result.team);
        }
    }
);
Request->ProcessRequest();
```

```cpp
// 接 server stream(push 服务订阅)
TSharedRef<IHttpRequest> StreamRequest = FHttpModule::Get().CreateRequest();
StreamRequest->SetURL("https://pandora-gw.example.com/pandora.push.v1.PushService/Subscribe");
StreamRequest->SetVerb("POST");
StreamRequest->SetHeader("Content-Type", "application/grpc-web+proto");
StreamRequest->SetHeader("X-Grpc-Web", "1");
StreamRequest->SetHeader("Authorization", "Bearer " + SessionToken);
StreamRequest->SetOption(HttpRequestOptions::HttpVersion, FHttpConstants::VERSION_2TLS);
StreamRequest->SetContent(GrpcWebEncodeUnary(SubscribeReqProto));

// ⭐ 关键:用 StreamDelegateV2 接收 server stream 数据块
StreamRequest->SetResponseBodyReceiveStreamDelegateV2(
    FHttpRequestStreamDelegateV2::CreateLambda(
        [](void* Ptr, int64& InOutLength) {
            // libcurl 每收到一块字节,这里立刻被调用(非 game thread)
            FGrpcWebFrameParser::Instance().Feed(Ptr, InOutLength);
            // 解析出完整 frame 后,投递到 game thread 分发
        }
    )
);
StreamRequest->ProcessRequest();
```

### 3.4 UE 5.8 vs HTTP/1.1 fallback

如果某天发现 HTTP/2 有兼容性问题,代码降级**只改一行**:
```cpp
Request->SetOption(HttpRequestOptions::HttpVersion, FHttpConstants::VERSION_1_1);
```
其它代码不动。`SetResponseBodyReceiveStreamDelegateV2` 在 HTTP/1.1 + chunked 下同样工作。

**升级路径完美**。

### 3.5 实现现状(2026-06-23 落地)

W2/M1.5 期实际跑的是 **HTTP/1.1 + TLS + keep-alive**(`MakeGrpcWebRequest` 未指定 HTTP 版本,
依赖 libcurl 默认 ALPN,且早期文档误记为"走 Envoy 是 HTTP/1.1")。2026-06-23 已在
[`PandoraBackendSubsystem::MakeGrpcWebRequest`](../../../Pandora-Client-SVN/Pandora/Source/Pandora/Private/Net/PandoraBackendSubsystem.cpp)
显式 `SetOption(HttpVersion, VERSION_2TLS)`,**一处覆盖 unary + push stream**(push 也走同一入口)。

- 连接① UE NetDriver / UDP(移动/技能)**不受影响**,与本改动无关。
- DS 面 `:8444` 是**明文无 TLS**,`VERSION_2TLS` 不适用,**不改**。
- 服务端早已就绪:Envoy `:8443` listener `alpn_protocols: [ "h2", "http/1.1" ]`。
- 验证手段:[`tools/scripts/http2_probe.ps1`](../../tools/scripts/http2_probe.ps1)
  (ALPN 容器探测 + Envoy admin `http.pandora_hcm.downstream_cx_http2_total` 计数)。
  系统自带 Schannel curl 不支持 `--http2`,须用容器 curl 或 Envoy stats。

### 3.6 HTTP/2 vs 旧 HTTP/1.1 keep-alive:好处与代价(完整权衡)

> 基准是改之前的 **HTTP/1.1 keep-alive**(并发请求会被迫额外开多条 TCP,各自握手),
> 不是纯 HTTP/1.0 短连。对比只针对客户端连接②(UE → Envoy:8443)。

**好处:**

| 好处 | 原理 | 对 Pandora 的意义 |
|---|---|---|
| 多路复用 | 一条 TCP 上 N 个请求并发(各占 stream id) | login/team/match/push 共用一条连接,不再为并发开多条 TCP |
| 消除应用层队头阻塞 | h1.1 同连接串行,h2 各 stream 独立 | 慢 RPC 不拖住同连接其它 RPC,故障隔离更干净 |
| 握手只付一次 | 连接复用,不反复 TCP+TLS | 弱网/移动端首字节更快、更省电 |
| HPACK 头压缩 | 重复 header 增量压缩 | 每请求都带的 `Authorization: Bearer <长JWT>` + grpc-web 头被压掉,省流量 |
| 连接数下降 | 一条顶旧时多条 | 客户端 FD 少、Envoy 每玩家连接少,500 人大厅规模下 Envoy 内存/FD 压力小 |
| 协议级 PING 保活 | h2 内置 PING | push 长连防 NAT 超时更省心,少依赖应用层心跳 |
| server stream 更规范 | stream 是 h2 一等公民 | push server stream 更稳,trailer 语义更标准 |

**代价 / 坏处:**

| 坏处 | 严重度 | 说明 |
|---|---|---|
| 强制 TLS + ALPN 协商 | 低 | 已满足;但多一个"会不会回落 1.1"的监控点,故有 `http2_probe.ps1` 盯死 |
| **TCP 层队头阻塞未消除** | **中(弱网)** | h2 消的是应用层队头阻塞;所有 stream 仍挤一条 TCP,**丢包时 TCP 重传卡住该连接全部 stream**。极端弱网下 h2 单连接可能不如多条 h1.1。**根治需 HTTP/3(QUIC),UE FHttpModule 现阶段不上** |
| 连接黏边缘实例(sticky) | 低 | Envoy 滚动重启会断该长连、需重连;但 h1.1 keep-alive 本就如此,非 h2 新增 |
| 调试更难 | 低 | h2 二进制帧不能直接抓明文;Schannel curl 不支持 --http2 |
| 单连接并发上限 | 极低 | 受 `max_concurrent_streams` 限;单玩家 1~10 req/s 远够用 |
| 实现/版本敏感 | 低 | 依赖 `VERSION_2TLS` 常量 + libcurl 带 nghttp2;换引擎版本要复验 |
| 一条连接断影响面更大 | 低~中 | h2 一条连接断 = 其上所有进行中 stream 同断;有 ALPN 回落 + unary 重发兜底 |

**净判断:** 内网/WiFi/有线 → 几乎纯收益;移动弱网 → 大体收益但 TCP 队头阻塞(丢包)是 h2 固有局限,
要彻底解只能上 HTTP/3。业务服无状态**完全不受影响**(h2 长连到 Envoy 即终止,后端仍是独立 unary)。

---

## §4 后端 Kratos 框架

### 4.1 为什么 Kratos(回顾 2026-06-04 决策)

go-zero 的 zrpc 不支持 gRPC server stream(经多轮分析确认),而 Pandora 的推送架构**必须用 stream**(避免自研 WebSocket envelope + kafka→ws 路由层)。

Kratos 优势:
- 基于原生 grpc-go,**完整支持 unary + server stream + client stream + bidi**
- `transport/grpc`(主)+ `transport/http`(可选,自动从 proto google.api.http 注解生成)
- 可拔插 log / metrics / tracing(OpenTelemetry 标准)
- B 站官方维护,游戏后端有验证(米哈游也用)

### 4.2 Pandora 业务服 Kratos 风格(W2 写法)

```go
// 业务服 main.go 简化版
func main() {
    // 1. 加载配置(Kratos config)
    c := config.New(config.WithSource(file.NewSource("./etc/team.yaml")))
    c.Load()

    // 2. 创建 gRPC server
    grpcSrv := grpc.NewServer(
        grpc.Address(":20010"),
        grpc.Middleware(
            recovery.Recovery(),
            tracing.Server(),
            logging.Server(logger),
            metrics.Server(),
            jwt.Server(...),  // 鉴权拦截器
        ),
    )

    // 3. (可选)HTTP server,由 proto google.api.http 注解驱动
    httpSrv := http.NewServer(
        http.Address(":21010"),
        http.Middleware(...),
    )

    // 4. 注册业务实现
    teamSvc := team.NewTeamService(...)
    teampb.RegisterTeamServiceServer(grpcSrv, teamSvc)
    teampb.RegisterTeamServiceHTTPServer(httpSrv, teamSvc)  // 由 protoc-gen-go-http 生成

    // 5. 启动
    app := kratos.New(kratos.Server(grpcSrv, httpSrv))
    app.Run()
}
```

### 4.3 中间件 / 拦截器(对齐 W2 pkg 重写)

| 中间件 | 实现位置 | 用途 |
|---|---|---|
| `recovery` | Kratos 内置 | panic recover |
| `tracing` | Kratos 内置(OpenTelemetry)| trace_id 透传 |
| `logging` | Kratos 内置 | 标准 access log |
| `metrics` | Kratos 内置(prometheus)| RPC duration / total |
| `jwt` | Kratos jwt middleware | session_token 校验,注入 player_id 到 ctx |
| `ratelimit` | Kratos 内置 | 限流 |
| `pandora-trace` | `pkg/middleware/`(自研) | 跟 ds-arch.md §0 trace_id 字段对齐 |

---

## §5 Envoy Edge Gateway

### 5.1 职责

Envoy 是**唯一的对外入口**(Edge Gateway 模式),处理:

1. **TLS 终止**:客户端 HTTPS → Envoy 内网明文 gRPC(或 mTLS)
2. **gRPC-Web → gRPC 转换**(envoy 内置 `envoy.filters.http.grpc_web` filter)
3. **ALPN 协商**:自动选 HTTP/2 vs HTTP/1.1
4. **JWT 鉴权**(envoy 内置 `envoy.filters.http.jwt_authn`)
5. **限流 / 熔断 / 重试**
6. **路由**:按 gRPC service 名路由到 13 个业务服 + push 服务

### 5.2 部署模式(W7 实施)

| 模式 | 描述 | 选用 |
|---|---|---|
| **k8s Ingress** | Envoy 作为 k8s Ingress controller | ⭐ Pandora 推荐(生产)|
| **独立 Pod** | 单独部署 envoy Pod,Service 暴露 | 可选 |
| **Sidecar** | 每个业务 Pod 旁边一个 envoy | 不用(那是 service mesh,过度) |
| **本地 docker** | 开发期 docker-compose 跑 envoy | ⭐ 开发期用 |

### 5.3 最小 envoy.yaml 示例(W7 起草)

```yaml
static_resources:
  listeners:
  - name: pandora_listener
    address:
      socket_address: { address: 0.0.0.0, port_value: 8443 }
    filter_chains:
    - transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
          common_tls_context:
            tls_certificates:
            - certificate_chain: { filename: /etc/envoy/cert.pem }
              private_key:        { filename: /etc/envoy/key.pem }
            alpn_protocols: [ h2, http/1.1 ]
      filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: AUTO
          http_filters:
          - name: envoy.filters.http.grpc_web        # ⭐ 关键:grpc-web 转标准 gRPC
          - name: envoy.filters.http.jwt_authn       # JWT 鉴权
          - name: envoy.filters.http.router
          route_config:
            virtual_hosts:
            - name: pandora_vh
              domains: ["*"]
              routes:
              - match: { prefix: "/pandora.login.v1.LoginService/" }
                route: { cluster: login }
              - match: { prefix: "/pandora.team.v1.TeamService/" }
                route: { cluster: team }
              - match: { prefix: "/pandora.match.v1.MatchService/" }
                route: { cluster: matchmaker }
              # ... 其它 11 个业务服 + push 服务
              - match: { prefix: "/pandora.push.v1.PushService/" }
                route: { cluster: push, timeout: 0s }  # ⭐ stream 无超时
  
  clusters:
  - name: login
    connect_timeout: 1s
    type: STRICT_DNS
    lb_policy: ROUND_ROBIN
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config:
          http2_protocol_options: {}
    load_assignment:
      cluster_name: login
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address: { address: login-svc.pandora.svc.cluster.local, port_value: 20001 }
  # ... 其它 cluster
```

### 5.4 服务发现集成

生产环境 Envoy 走 **xDS 协议**(动态服务发现):
- Pandora 暂不上 Istio service mesh(过度工程)
- 用 k8s `Service` + DNS(Envoy STRICT_DNS)即可
- 业务服扩缩容由 k8s + Agones 自动处理

---

## §6 推送架构 — push 服务(集中 + server stream)

### 6.1 为什么集中 push 而不是每个业务服自己推

| 模式 | 优 | 劣 |
|---|---|---|
| 每业务服自己推 stream | 直推,低延迟 | 14 个业务服 = 客户端 14 条 stream(违反"客户端最多 2 连"原则)|
| **集中 push 服务**(选定) | 客户端 1 条 stream | push 多消费一次 kafka,几 ms 额外延迟 |

### 6.2 push 服务设计

```go
// proto/pandora/push/v1/push.proto(W2 §2.5 加)
service PushService {
  // Subscribe:客户端首次登录后立刻调,维持长连接
  // 服务端 server stream 推送所有 player_id 相关事件
  rpc Subscribe(SubscribeRequest) returns (stream PushFrame);
}

message SubscribeRequest {
  string session_token = 1;  // 鉴权(或走 Envoy JWT,这里冗余)
  int64  last_seen_ms  = 2;  // 重连补推用
}

message PushFrame {
  string topic     = 1;  // pandora.team.update / pandora.match.progress / ...(= 业务域,粗路由)
  bytes  payload   = 2;  // 业务 Event message 序列化(如 TeamUpdateEvent)
  int64  ts_ms     = 3;
  string trace_id  = 4;
  uint32 event_type = 5; // 域内事件类型(细路由);0=该 topic 现有旧事件。见 go-services.md「域内多事件类型路由」
}
```

### 6.3 push 服务运行时

```go
// push 服务 main 逻辑
func (s *PushService) Subscribe(req *SubscribeRequest, stream PushService_SubscribeServer) error {
    playerID := extractPlayerIDFromJWT(stream.Context())
    
    // 1. 注册 stream 到内存索引
    s.connections.Store(playerID, stream)
    defer s.connections.Delete(playerID)

    // 2. 补推离线消息(redis ZSET)
    offlineMsgs := s.redis.ZRangeByScore(
        fmt.Sprintf("pandora:push:offline:%d", playerID),
        req.LastSeenMs, time.Now().UnixMilli(),
    )
    for _, msg := range offlineMsgs {
        stream.Send(decodeFrame(msg))
    }
    s.redis.Del(...)  // 推完清理

    // 3. 阻塞等 client 断开(kafka consume 在另一 goroutine 推 stream)
    <-stream.Context().Done()
    return nil
}

// 单独的 kafka consumer goroutine
func (s *PushService) consumeLoop() {
    for msg := range kafkaConsumer.Messages() {
        envelope := decodeKafkaEnvelope(msg)
        playerID := extractKey(msg)
        
        if streamRaw, ok := s.connections.Load(playerID); ok {
            // ⭐ 玩家在线:直接通过 server stream 推
            stream := streamRaw.(PushService_SubscribeServer)
            stream.Send(&PushFrame{
                Topic:    envelope.Topic,
                Payload:  envelope.Payload,
                TsMs:     envelope.TsMs,
                TraceId:  envelope.TraceId,
            })
        } else {
            // ⭐ 玩家离线:存 redis ZSET(5 分钟过期)
            s.redis.ZAdd(
                fmt.Sprintf("pandora:push:offline:%d", playerID),
                envelope.TsMs, encodeFrame(envelope),
            )
            s.redis.Expire(..., 5*time.Minute)
        }
    }
}
```

### 6.4 多实例扩展(W6+)

push 单实例顶不住时:
- 多个 push 实例同 consumer group `pandora-push`(kafka 自动 partition 分配)
- 但 player_id → push_instance 路由要解决:`redis HSET pandora:push:route player_id instance_name`
- kafka 消息 key=player_id 落到 partition,但消费它的实例不一定是该玩家连着的 → 实例内部 gRPC 转发到目标实例

**W1-W4 单实例够用,这个后置优化。**

---

## §7 离线消息 + 重连恢复

### 7.1 离线消息策略

- 玩家在线:push 直接 server stream
- 玩家离线(stream 已断):写 redis ZSET `pandora:push:offline:<player_id>`,score=ts_ms,member=envelope_bytes
- 保留 5 分钟(`EXPIRE 300`),过期消息丢弃(MOBA 业务不需要永久离线消息;邮件等永久数据走 DB 拉)

### 7.2 重连流程

```
Client WebSocket 断 → UE 自动重连(libcurl 内置)
Client 重连 → 调 Push.Subscribe { session_token, last_seen_ms }
push 服务:
  - JWT 校验
  - 注册 stream 到 connections 索引
  - ZRangeByScore pandora:push:offline:<id>(score 在 last_seen_ms 到 now)
  - 按 ts_ms 排序顺序推
  - 推完 ZRemRangeByScore 清理
  - 阻塞等下次断开
Client:UI 增量刷新(去重靠 ts_ms,见 protocol-ordering-rules.md §5.3)
```

---

## §8 故障域分析

| 故障 | 影响 |
|---|---|
| Envoy 崩 | 客户端业务请求 + 推送全部不可用;游戏内同步(NetDriver)不受影响 |
| Hub DS 崩 | 玩家看不到大厅世界,但 UI 业务(组队/商店/战绩)正常,玩家可断 hub 但保持 ② |
| Battle DS 崩 | 这局战斗中断,已结算战绩通过 kafka 落库;玩家退回大厅 |
| push 服务崩 | 推送暂时不可用,客户端 UI 用轮询 GetXxx 兜底;业务请求 + 游戏同步正常 |
| 单个业务服(team/match)崩 | 该业务功能挂,其它正常 |
| login 崩 | 新玩家无法登录,已登录玩家不受影响(JWT 校验由 Envoy 做,不依赖 login) |
| kafka 崩 | 推送停了,业务请求 + 游戏同步正常 |
| etcd 崩 | 服务发现退化(已建立的连接还能用,新连接失败) |
| redis 崩 | session 丢,玩家被踢;业务功能瘫痪;游戏同步不受影响(DS 内存状态)|

**核心收益**:**故障域之间相互隔离**。任一组件崩不会全军覆没。

---

## §9 端到端时序示例

### 9.1 玩家组队邀请全链路

```
玩家 A(Hub DS)                       Envoy + 业务服              玩家 B(Hub DS)
  │                                        │                          │
  │ UI 点"邀请 B"                          │                          │
  │ ② FHttpModule POST                    │                          │
  │   /pandora.team.v1.TeamService/Invite │                          │
  │   gRPC-Web frame {InviteRequest{B}}       │                          │
  │   HTTP/2 + TLS                        │                          │
  │───────────────────────────────────────▶│                          │
  │                                        │ Envoy: 解 TLS / 鉴权     │
  │                                        │ Envoy: grpc-web→grpc     │
  │                                        │ → team:20010 unary       │
  │                                        │   写 redis 记录邀请       │
  │                                        │   produce kafka:         │
  │                                        │     topic=team.update    │
  │                                        │     key=B(只给被邀请方) │
  │                                        │ ← gRPC response {ok}     │
  │                                        │ Envoy: grpc→grpc-web    │
  │ gRPC-Web response                     │                          │
  │ {InviteResponse{ok}}                      │                          │
  │◀──────────────────────────────────────│                          │
  │ UI 显示"已邀请 B"                      │                          │
  │                                        │                          │
  │                                        │ kafka consume(push 服)   │
  │                                        │ → 找 B 的 stream         │
  │                                        │ → stream.Send(PushFrame{ │
  │                                        │     topic=team.update    │
  │                                        │     payload=TeamUpdate   │
  │                                        │       Event{invited})    │
  │                                        │───────────────────────────▶
  │                                        │                          │ UE FHttpModule
  │                                        │                          │ StreamDelegateV2
  │                                        │                          │ → 解 grpc-web frame
  │                                        │                          │ → UI 弹窗"A 邀请你"
```

**关键观察**:
- 玩家 A、B 各自走自己的 ② 连接,**没有任何消息经过 Hub DS**
- Hub DS 即使崩,A 仍能发邀请,B 仍能收推送
- 完全对齐 `architecture-rejected-strict-ds-only.md` 的故障域目标

### 9.2 玩家进战斗全链路

```
玩家 A(Hub,组好队)
  │ ② POST /pandora.match.v1.MatchService/StartMatch
  │   {team_id=T1}
  ▼
Envoy → matchmaker(gRPC unary)
  │
  │ matchmaker 撮合开始,写 redis 入队
  │ 撮合成功后(可能几秒):
  │   produce kafka pandora.match.progress
  │     key=A...A's player_id  payload=stage=FOUND
  │     key=B...                payload=stage=FOUND  (5 个 player_id 各一条)
  │
  ▼
push 服务 consume → 推 5 个客户端 stream
  │
  ▼
玩家 A UI:"找到对手!确认参战?"
玩家 A 点确认 → ② POST /pandora.match.v1.MatchService/ConfirmMatch
  │
  ▼
Envoy → matchmaker(unary)
  │
  │ 等所有 10 人确认 / 超时
  │ 全确认 → matchmaker.调 ds_allocator.AllocateBattle (gRPC unary)
  │
  │ ds_allocator 通过 Agones 拉起 battle DS pod
  │ ds_allocator 返回 ds_addr + ds_pod_name
  │
  │ matchmaker 签 battle_ticket,produce kafka pandora.match.progress
  │   payload=stage=READY,battle_ds_addr=...,battle_ticket=...
  │
  ▼
push 服务 → 推 10 个客户端 stream
  │
  ▼
玩家 A 客户端拿到 battle_ds_addr + battle_ticket
  │
  ▼
玩家 A 断开 ① NetDriver(Hub DS)
玩家 A 用 battle_ticket 连 Battle DS(新的 ① NetDriver)
  │
  ▼
战斗 25 分钟(纯 UE Replication + GAS,无后端干预)
  │
  ▼
战斗结束 → Battle DS 发 kafka pandora.battle.result(给 battle_result 落库)
         + Battle DS 用 UE ClientRPC 推 BattleEnded{result, hub_ds_addr, hub_ticket}
  │
  ▼
玩家 A 看战绩 10s → 断 Battle DS → 重连 Hub DS

⭐ 整个流程:② Envoy 连接保持(stream 始终在),只有 ① NetDriver 在 Hub / Battle 之间切换
```

---

## §10 W2+ 实现路线图

### W2(第一周)

1. **D2 pkg 重写**(~3-4 天)— 见 §11 详细清单
2. **写 login 服务**(Kratos)— 第一个 Kratos 业务服
3. **配 Envoy + 自签证书**(本地 docker-compose)— 验证 gRPC-Web 链路

### W2-W3

4. **写 push 服务**(server stream + kafka consumer)
5. **UE 客户端写 grpc-web 协议解析**(基于 FHttpModule,~3-5 天)
6. **端到端打通**:UE → Envoy → login → response 回到 UE 显示

### W3-W6

7. 其它 13 个业务服(team / match / chat / ...)
8. 各业务服 produce kafka 推送 topics
9. push 服务消费转发给客户端

### W7-W8

10. UE DS(Hub / Battle)骨架
11. Agones 集成

---

## §11 UE 客户端不用 gRPC 插件的决策

### 11.1 第三方 UE gRPC 插件清单(评估过的)

| 插件 | 出处 | 状态 |
|---|---|---|
| 社区 GrpcUEPlugin / fork 多个 | GitHub | 不活跃,UE 5.x 兼容性参差 |
| gRPCue / gRPC for Unreal | FAB Marketplace 收费 | 商业维护但只支持 unary |
| 腾讯 / 网易 内部 gRPC | 不开源 | 不可用 |

### 11.2 5 个共性坑

1. **包体爆炸**:都基于 grpc-cpp,+80MB
2. **SSL 冲突**:grpc-cpp 拉 BoringSSL,UE 自带 OpenSSL,**同进程两套 SSL 必然链接冲突**,每个 UE 版本都要重新调
3. **UE 5.x 兼容性差**:大部分插件 UE 4.27 时代写的,UE 5 改了 ModuleRules 经常 build 不过
4. **stream API 别扭**:UE Delegate 表达 stream 4 种回调(start/middle/end/error)很拗口
5. **跨平台编译痛**:iOS BoringSSL + ATS 冲突,Android NDK 版本对齐,Linux server target 复杂

### 11.3 大厂事实

**几乎没有大型游戏客户端走 gRPC**:
| 厂家 | 客户端协议 |
|---|---|
| 米哈游(原神 / 星铁) | 自研 TCP 长连接 + protobuf |
| 腾讯王者 / 和平精英 | MtgRPC 自研协议 |
| 网易(永劫无间) | 自研 + protobuf |
| Riot LoL | REST + RTC(LCU 自研) |
| 堡垒之夜 | REST + WebSocket |
| Epic EOS SDK | REST + WebSocket |

游戏客户端长连接的工业标准:**HTTP/2 + protobuf 自研协议** 或 **WebSocket + protobuf**。

### 11.4 Pandora 选择

✅ **自研 grpc-web 客户端基于 FHttpModule**(已验证 UE 5.8 完全支持)

工作量预估:~3-5 天
- 协议解析:grpc-web frame 格式公开,简单(1 字节 flag + 4 字节 length + payload)
- 客户端代码模板见 §3.3
- 单元测试用 Envoy 跑起来对接

收益:**零额外依赖、零 SSL 冲突、零跨平台编译坑**,符合 Pandora "标准协议优先" 铁律。

---

## §12 决策行(写入 pandora-arch.md §11)

| 日期 | 决策 | 原因 |
|---|---|---|
| 2026-06-04 | 切换后端框架:go-zero → **Kratos** | go-zero 不支持 gRPC stream,推送架构受限 |
| 2026-06-04 | 引入 **Envoy** 作为 Edge Gateway | 标准 gRPC-Web ↔ gRPC 协议转换 |
| 2026-06-04 | 客户端协议:**gRPC-Web over HTTP/2 TLS** | UE 5.8 FHttpModule 已暴露(SetOption "HttpVersion=2TLS") |
| 2026-06-04 | 推送架构:**集中 push 服务 + server stream** | 替代 kafka→ws 自研,延迟低 + 协议标准 |
| 2026-06-04 | 客户端实现:**自研 grpc-web 客户端基于 FHttpModule** | 不引入第三方 UE gRPC 插件(5 个共性坑) |
| 2026-06-04 | 服务清单 13 → **14**(新增 push)| Envoy 作为基础设施不计 go 服务 |
| 2026-06-04 | 客户端连接最终值 = **2 条**(NetDriver + FHttpModule)| 用户铁律确认 |

---

## §13 历史阻塞清单

- ✅ 本地 D7 已完成:minikube + Agones dev + 端到端 hello world 已跑通。
- ✅ JWT 鉴权已落地:login 签发真实 JWT,Envoy `jwt_authn` 校验。
- ⚠️ 生产 k8s 形态(ACK / 自建 / 其他)仍属发布前部署决策,不再作为 D7 阻塞项。

---

## §14 TLS 证书策略(dev vs 生产)

> 状态:**已决策(2026-06-10)**。
> 触发:本机 UE 客户端连 Envoy :8443 报 `libcurl error 35 / SSL_ERROR_SYSCALL`,
> 根因排查见本节 §14.4。结论:**dev 自签证书的"客户端不信任"问题在生产环境不存在**,
> 因为生产用公网 CA 证书,玩家设备出厂即信任。

### 14.1 核心区分:玩家连的不是 IP+自签证书

连接 ②(UE FHttpModule → Envoy)是 TLS。**证书信任链在 dev 和生产是两套完全不同的机制**,
不能用 dev 的现象推断生产:

| 维度 | dev(本机联调) | **生产(正确做法)** |
|---|---|---|
| 证书签发者 | **mkcert 本地 CA**(只有装过的机器信) | **公网 CA**(Let's Encrypt 免费 / 商业 CA) |
| 客户端是否信任 | ❌ 默认不信,需手动装 CA | ✅ 设备/UE 出厂预装公网 CA 根证书,自动信任 |
| 连接地址 | `127.0.0.1` / `localhost`(IP/本地名) | **真实域名** `gateway.<game>.com` |
| 证书 SAN | `localhost` / `127.0.0.1` / `host.docker.internal` | 真实域名(**不写 IP**) |
| 玩家侧配置 | 开发者手动信任一次 | **零配置、零感知** |

**关键认知**:玩家千万家客户端能不能连上,取决于"证书是不是公网 CA 签的 + 连的是不是域名"。
只要满足这两点,**所有玩家零配置直接握手成功**——这正是所有联网游戏/App 的通用做法。

### 14.2 生产正确做法(所有玩家都要能连 → 唯一推荐)

```
玩家客户端(UE)
  │  https://gateway.<game>.com:443      ← 真实域名,不是 IP
  ▼
公网 DNS → 网关公网 IP(LB / Ingress)
  ▼
Envoy / 边缘负载均衡
  - 证书:公网 CA 签发(Let's Encrypt 免费,certbot/acme 自动签发+续期)
  - 证书 SAN = gateway.<game>.com(真实域名)
  - 玩家设备/UE 自带 cacert.pem 内已含该 CA 根证书 → 握手 0 错误、0 配置
```

落地三要素:
1. **域名**:注册域名,把网关公网 IP 解析过去(A/AAAA 记录)。
2. **公网 CA 证书**:Let's Encrypt(免费,ACME 自动续期 90 天)或商业 CA。证书绑域名,**SAN 不写 IP**。
3. **客户端连域名**:UE `GatewayHost` 配真实域名(不是 `127.0.0.1`),其余不动。

> ✅ 这条满足"不是个人开发、所有人都要能连":玩家什么都不装,直接连。

### 14.3 dev 阶段(团队多人联调)怎么办

dev 用 mkcert 自签证书的代价是"客户端默认不信任"。团队联调三选一:

| 方案 | 做法 | 适用 | 谁执行 |
|---|---|---|---|
| **A 叠加 dev CA**(推荐) | 用 UE `[SSL] DebuggingCertificatePath` 指向客户端工程 `Config/Certificates/pandora-dev-rootCA.pem`, 叠加一张 dev CA 到引擎公网 CA 包之上 | 少数固定开发机 | Codex/人(项目侧脚本) |
| **B 共享 dev 域名 + 公网 CA** | dev 也用真实域名 + Let's Encrypt,等于提前搭生产链路 | 团队人多 / 提前贴近生产 | Codex/人 |
| C 关证书校验 | UE HTTP 层跳过校验(`bDevInsecureTls`) | 临时本机 | 不推荐(UE 版本敏感、易漏到生产) |

方案 A 命令(由 Codex/人执行,Claude 不碰 UE 运行验证):

```powershell
pwsh E:\work\Pandora\tools\scripts\import_dev_ca.ps1
# 重启 UE 编辑器后,UE OpenSSL 即信任本机 Envoy dev 证书
```

不要修改 `D:\UnrealEngine\Engine\Content\Certificates\ThirdParty\cacert.pem`。引擎目录是多项目共享环境,
升级会覆盖,也不随客户端仓库分发。工程 `Content/Certificates/cacert.pem` 也不要用:它会整包替换公网 CA
并进入发行包。`Config/` + `DebuggingCertificatePath` 才是不替换、不入包、不碰引擎的项目侧方案。

### 14.4 根因留档(2026-06-10 排查实证)

现象:UE 连 `https://127.0.0.1:8443` 报 `libcurl error 35 (SSL connect error) / SSL_ERROR_SYSCALL / error:00000000`。

排查链(关键是"用和 UE 同后端的 OpenSSL 复现",别用 schannel/curl -k 误判):

- `curl -k`(schannel + 跳过校验)→ 握手成功返 404 ⇒ **Envoy/证书本身正常**,问题在客户端信任。
- `curl` 不加 `-k`(schannel)→ `CRYPT_E_NO_REVOCATION_CHECK`,这是 **schannel 吊销检查**问题,
  与 UE 的 OpenSSL 后端**无关**,不能据此下结论。
- `openssl s_client -connect 127.0.0.1:8443`(**与 UE 同后端**)→
  `SSL handshake has read ... bytes`(握手实际完成)`Verify return code: 21
  (unable to verify the first certificate)` ⇒ **唯一失败点是证书链验证:OpenSSL 不信任 mkcert CA**。
- `openssl s_client ... -CAfile <mkcert rootCA.pem>` → `Verify return code: 0 (ok)` ⇒ **证实**:
  只要信任 mkcert CA,验证立即通过。

结论:UE 报的 `SSL_ERROR_SYSCALL / error:00000000` 是 **libcurl 校验回调失败后主动中止握手**的表象
(OpenSSL 错误队列为空 → libcurl 归类成 SYSCALL),本质就是 **dev 自签证书不被 UE OpenSSL 信任**。
生产用公网 CA 后该问题不存在。

### 14.5 成本与常见疑问(为什么要域名 / 要不要花钱)

**几乎不花钱。证书永久免费,唯一可能花钱的是域名(~¥30/年,常有免费替代)。**

| 项 | 费用 | 必须吗 |
|---|---|---|
| TLS 证书 | **0 元**(Let's Encrypt 永久免费,ACME 自动续期 90 天) | 是 |
| 域名 | **~¥30-70/年**(常有云厂商/Cloudflare 免费二级域名替代) | 建议(非技术硬性) |
| 服务器 | 本就要租(跑 Envoy + 后端 + DS) | 是 |

**为什么必须域名(不是行规,是 TLS 机制决定的)**:

- Let's Encrypt 等免费公网 CA **只给域名签证书,不给纯 IP 签**(技术 + 政策双重限制)。
- 玩家要"零配置自动信任" → 必须公网 CA 签的证书 → 必须有域名。
- 用 IP + 自签证书,就回到 dev 老问题:**每个玩家都要手动装你的 CA**,千万玩家不可能做到。
- 链路:**所有玩家零配置连上 → 必须公网 CA 证书 → 必须域名**。

**省钱选项**:

- 云服务器(阿里云/腾讯云)常**免费送二级域名**;Cloudflare 提供**免费 DNS + 免费证书**(回源还能免费签)。实际可做到**域名费 0 元**。
- 商业 CA / 通配符证书只在有合规或多子域需求时才考虑,**起步阶段不需要**。

**不要做的**:

- ❌ 纯 IP + 自签证书发给玩家(没人会装你的 CA)。
- ❌ 纯 IP + 关闭证书校验上线(传输无加密,account/password 可被中间人截获)。

**阶段建议**:

- 现在(本机 / 小团队联调):**不买任何东西**,走 §14.3 方案 A(`DebuggingCertificatePath` 叠加 dev CA)。
- 以后给真实玩家:租正式服务器时顺手注册域名(或用云厂商免费域名)+ Let's Encrypt,玩家零配置连上。

### 14.6 决策行(写入 pandora-arch.md §11)

| 日期 | 决策 | 原因 |
|---|---|---|
| 2026-06-10 | 生产连接 ② 证书 = **公网 CA(Let's Encrypt/商业)+ 真实域名**,SAN 不写 IP | 玩家设备出厂即信任公网 CA,零配置握手;满足"所有玩家都要能连" |
| 2026-06-10 | dev 自签(mkcert)证书"客户端不信任"**仅 dev 问题**,生产不存在 | dev/生产证书信任链是两套机制,不可互相推断 |
| 2026-06-10 | dev 联调默认走**方案 A**(`[SSL] DebuggingCertificatePath` 叠加 dev CA) | UE 用自带 OpenSSL,不读系统根库；项目侧方案不碰引擎、不替换公网 CA、不进发行包 |
| 2026-06-10 | TLS 证书选 **Let's Encrypt(免费)**,域名可用云厂商/Cloudflare 免费二级域名 | 公网 CA 不给纯 IP 签证书;成本可压到接近 0 |

### 14.7 FAQ:自带证书包行不行 / 域名 vs IP 到底是两回事

常见混淆:"玩家安装时自带证书包不就行了?这跟域名/IP 有啥关系?" —— **这是两个独立问题,生产都要对。**

**问题一:证书"谁签的"(自带 CA 行不行)**

- 客户端本来就**自带一份公网 CA 清单**(`cacert.pem`,几百个公网 CA)。生产的正确做法就是让 Envoy 证书**由这些公网 CA 签**,于是玩家零配置即可验证 —— 你说的"自带证书包"生产其实一直在用。
- "自带**私有 CA**(像 dev 的 mkcert)"技术上也能跑,但生产**不推荐**:

  | 维度 | 自带私有 CA | 公网 CA |
  |---|---|---|
  | 私钥泄露 | 根 CA 私钥在打包机,泄露 → 攻击者可伪造任意证书 MITM 全体玩家 | 私钥在 CA 的 HSM,有审计,你碰不到 |
  | 吊销被盗证书 | 只能推客户端更新给每个玩家 | OCSP/CRL 即时吊销,客户端自动拉 |
  | CA 过期 | 私有 CA 过期 → 重发整个客户端 | 续期在服务端,客户端无感 |
  | 第三方接入 | 网页端/支付/客服 SDK 不认你的私有 CA | 全世界默认都认 |

- 结论:**dev 自带私有 CA 没问题(现在做的);生产用公网 CA 签**,玩家不用额外塞东西。

**问题二:证书"写哪个地址"(域名 vs IP)**

跟"谁签的"无关。证书里有 **SAN** 字段写明"这张证书给哪个地址用"。TLS 握手要**同时**校验两件事:① 是不是可信 CA 签的;② **我连的地址 == 证书 SAN 写的地址**。

域名优于 IP 的原因是**运维灵活性**:

- **用域名** `gateway.yourgame.com`:SAN 写域名。换服务器/扩容/容灾**只改 DNS,IP 随便变,证书不动**。
- **用裸 IP**:SAN 写死 IP。服务器一搬家**证书就废**,得重签重发;且**公网 CA 基本不给裸 IP 签**(内网 IP 更绝对不签)。

**一句话类比(身份证)**:

- "谁签发" = 谁盖章。公安局盖章(公网 CA)全国认;自己刻章(私有 CA)只有信你章的人认,且刻章丢了能被伪造。
- "写哪个地址" = 证件上的地址。**域名 = 名字(搬家不变)**;**IP = 写死的门牌号(搬家就对不上)**。

**生产两个都要对**:公网 CA(玩家零配置)+ 真实域名(以后换服务器证书不废)。两者各自独立,缺一不可。

## §15 UE push stream 客户端解析器线程安全决策

### 15.1 结论

当前客户端 push stream 解析路径里的锁,保护的是 **`StreamParser` 解析器对象本身的生命周期**,
不是保护"收到的帧"传递。稳态游戏过程中,游戏线程不进入这把锁,不会因为玩家正常收 push 而卡住游戏线程。

双缓冲队列适合优化"已解析帧从接收线程交给游戏线程"这一段,但它不能替代解析器生命周期保护。
如果后续要彻底去掉锁,正确方向是改所有权模型:每条 HTTP stream 的接收回调闭包独占一个解析器,
游戏线程不再直接 Reset/替换解析器。

### 15.2 这把锁保护什么

`StreamParser` 内部有累积状态:

- `Buffer`:保存半包和已接收但尚未完整解析的数据。
- `Cursor`:记录当前解析进度。

它会被两个线程触碰:

| 线程 | 行为 | 风险点 |
|---|---|---|
| HTTP 接收线程 | `OnStreamBytes` 内 `Feed()` / `NextFrame()` | 追加 Buffer、移动 Cursor |
| 游戏线程 | `Subscribe()` / `CloseStream()` | 顶号重连时替换解析器,断线/关闭时 Reset 解析器 |

如果 HTTP 线程正在 `Feed()` 同一个解析器对象,游戏线程同时把成员 `TSharedPtr` Reset 或替换成新的解析器,
就是对同一个指针变量和同一份解析器内部状态的并发读写。`ESPMode::ThreadSafe` 只保证引用计数原子,
不保证 `TSharedPtr` 变量本身和被指向对象的业务状态可以被多线程无锁读写。

解析后的帧传递给 UI/BP 已经不依赖这把锁:当前方案通过 `AsyncTask(GameThread, ...)` 投递到游戏线程。
因此锁和"把消息送回游戏线程"不是一件事。

### 15.3 为什么不会卡住玩家游戏线程

锁的进入点很少:

| 线程 | 何时拿锁 | 频率 |
|---|---|---|
| HTTP 接收线程 | 每次 `OnStreamBytes` | 收包时,但不在游戏线程 |
| 游戏线程 | `Subscribe()` / `CloseStream()` / 关闭广播 | 登录、顶号、断线等低频路径 |

稳态游戏过程中,游戏线程只消费已经投递回来的 push frame 并执行 `OnPushFrame.Broadcast`,
不会每帧进入解析器锁。锁争用只可能出现在登录、重连、断线这类生命周期切换点。

### 15.4 双缓冲队列能解决什么,不能解决什么

双缓冲队列是 MPSC 模型:多个生产者线程 `put`,单个消费者线程每帧 `take` 一批。
它适合做"成品帧交接":

- 可以替代 `AsyncTask(GameThread, ...)`,把已解析的 `FPandoraPushFrame` 推进队列,
  再由游戏线程 Tick 中每帧取 N 个处理。
- 可以减少 task 调度和小分配,吞吐更稳,也方便做每帧消费上限。

但它不能解决解析器生命周期 race:

- 解析器仍然必须由某个线程独占地喂字节、维护半包和 cursor。
- `Subscribe()` / `CloseStream()` 如果仍从游戏线程直接替换或 Reset 解析器,仍需要同步。

所以双缓冲队列优化的是"传成品",当前锁保护的是"不要一边使用解析器一边拆解析器"。

### 15.5 真正的零锁方向

如果后续要做完全无锁版本,推荐采用"每条流独占解析器 + 队列回传帧":

1. 不再把 `StreamParser` 作为可被游戏线程替换/Reset 的共享成员。
2. `Subscribe()` 创建新 HTTP request 时,同时 `MakeShared` 一个解析器,并把它捕获进该 request 的接收回调闭包。
3. 旧流闭包持有旧解析器,新流闭包持有新解析器,顶号重连时两者天然隔离。
4. `CloseStream()` 只 `CancelRequest()`,不直接 Reset 解析器;解析器随回调链结束自然析构。
5. 已解析出的 `FPandoraPushFrame` 通过线程安全队列回到游戏线程,游戏线程 Tick 中批量消费。

这套模型的核心收益是:解析器所有权只属于对应 HTTP stream 的接收回调链,
游戏线程永远不碰解析器对象,因此不再需要用锁协调解析器拆建。

### 15.6 成品帧回传保持 AsyncTask

对 `PushService/Subscribe` 这条推送流,当前保持 `AsyncTask(GameThread, ...)` 回传已解析帧,
不引入双缓冲队列。

原因是 push stream 的真实流量是事件级,不是帧同步级:

- 稳态通常每秒 0 到几条,很多时候几秒才一条。
- 峰值如组队频繁操作、匹配状态变化、系统通知等,每秒几十条已经属于高峰。
- 一帧内通常只有 0 到 1 批 push 数据,双缓冲把多次调度合并成每帧一次 swap 的优势基本用不上。

在这个量级下,`AsyncTask` 的 task 调度和一次 `TArray` move 相对 16ms 帧预算是噪声,
吞吐瓶颈不在这里。`AsyncTask` 还具备几个工程优势:

| 维度 | AsyncTask | 双缓冲队列 |
|---|---|---|
| 延迟 | 收到后立即投递 GameThread,下一个 tick 可执行 | 必须等游戏线程主动 take |
| 驱动点 | 不需要额外 Tick | 需要 subsystem tick 或 ticker |
| 维护成本 | 引擎原生,代码少 | 需要引入/维护并发队列 |
| 背压控制 | 无显式每帧上限 | 可每帧限量消费 |
| 适用场景 | 低频事件推送 | 高频小消息热路径 |

双缓冲或 SPSC ring buffer 只在 push 流被错误地扩展成高频热路径时再考虑,
例如用它推实时战斗状态、单帧内 push frame 经常超过几十条,
或 profiler 明确显示 GameThread task 调度成为可见开销。

如果未来要在代码里留注释,建议写在 `OnStreamBytes` 投递帧的位置:

```cpp
// Push stream is event-level traffic, so AsyncTask keeps latency low and avoids
// a separate tick-driven queue. Revisit SPSC/double-buffer only if profiling
// shows high per-frame push volume or GameThread task scheduling overhead.
```

### 15.7 决策行

| 日期 | 决策 | 原因 |
|---|---|---|
| 2026-06-15 | 当前锁保护 `StreamParser` 生命周期,不是 push frame 传递 | `StreamParser` 有 Buffer/Cursor 累积状态,不能在 HTTP 线程 Feed 时被游戏线程 Reset/替换 |
| 2026-06-15 | 现有锁不会在稳态游戏过程中锁住游戏线程 | 游戏线程只在 Subscribe/CloseStream 等低频生命周期路径拿锁,正常 push 消费走游戏线程投递/广播 |
| 2026-06-15 | 双缓冲队列可用于替代 AsyncTask 传递已解析帧,但不能替代解析器生命周期同步 | 双缓冲解决成品交接,不解决解析器对象被跨线程拆建的问题 |
| 2026-06-15 | 若追求完全无锁,采用"每条 HTTP stream 闭包独占解析器 + 线程安全队列回传帧" | 消除共享解析器成员,让旧流/新流解析器自然隔离 |
| 2026-06-15 | 当前成品帧回传保持 `AsyncTask(GameThread, ...)`,不引入双缓冲队列 | push stream 是低频事件流,双缓冲吞吐优势用不上;AsyncTask 延迟更直接、维护成本更低 |
| 2026-06-15 | 仅当单帧 push frame 经常超过几十条或 profiler 显示 task 调度成为可见开销时,再评估 SPSC/double-buffer | 用性能数据触发复杂度升级,避免为非热路径过度设计 |

## §16 内网东西向安全态势(明文 + NetworkPolicy + 未来 mTLS)

> 状态:**已决策(2026-07-03)**。
> 触发:审查客户端连接是否"还有明文 grpc-web"。结论:**玩家链路(连接 ②)全程 TLS,无明文**;
> 但集群内业务服之间、业务服→基础设施(mysql/redis/kafka/etcd)是**明文 h2c / 明文 TCP**。
> 本节记录"内网明文是有意为之 + 用什么兜底"的决策,以及本次落地的 NetworkPolicy。

### 16.1 两类流量的加密边界

| 流量 | 方向 | 加密 | 谁负责 |
|---|---|---|---|
| 南北向(玩家) | Client → Envoy `:8443` | **TLS**(公网 CA,§14) | Envoy 终止 |
| DS 面(集群内) | DS(Pod) → Envoy `:8444` | **明文**(§3.5:DS 面无 TLS;本地与线上同构,Fleet `PANDORA_DS_ALLOCATOR_TLS=0`) | 无(属东西向,靠 NetworkPolicy/未来 mesh 兜底) |
| 东西向(内网) | 业务服 ↔ 业务服、业务服 → mysql/redis/kafka/etcd | **明文**(h2c / 明文 TCP) | 无(靠基础设施兜底) |

> 2026-07-10 勘正(回应审核 P1 契约冲突):旧版本表曾把 DS `:8444` 归入「南北向 TLS」,与 §3.5
> 「DS 面明文无 TLS」自相矛盾。权威口径:DS 跑在集群内(default ns Fleet),DS→Envoy 属集群内
> 东西向,与其它内网流量同样保持明文;start.ps1 online 的 `-DsGatewayTls` 默认也已对齐为 0。
> 若未来 DS 面真要挂集群外 TLS 边缘(DS 不在同集群),届时需同步配 `.svc`/边缘域名证书 + DS
> 信任链并显式 `-DsGatewayTls 1`,单独过设计评审。

**为什么内网不在应用层做 TLS**:这是主流实践。应用代码继续走明文,把"加密 + 身份"责任下沉到基础设施层
(mesh sidecar / 网络策略),业务零改动、无证书运维负担。客户端 `PandoraDSBackendSubsystem.bUseTls`
默认 `false`(env `PANDORA_DS_ALLOCATOR_TLS` 可覆盖)、Envoy 头注"mTLS 上行 W3 之后再加",都是这个取向。

### 16.2 兜底三层(成本从低到高)

1. **NetworkPolicy(本次落地,零代码)** — K8s 生产最低门槛。分层最小权限:跨命名空间默认拒绝;
   存储层(mysql/redis/kafka/etcd/zk)入站收敛到「仅业务层 + 必要 infra 对等边」;业务服之间暂保持全通(调用图复杂,收敛留给 mesh)。
2. **Service Mesh sidecar mTLS(未来,仍零业务代码)** — Istio/Linkerd 自动 mTLS,业务容器继续 `localhost` 明文 h2c。
   即 §5.1"(或 mTLS)"、envoy 头注"W3 之后再加"的那一步。届时应用层 `bUseTls` 仍保持 `false`,加密交给 sidecar。
3. **应用层 TLS(最不推荐)** — 仅在既不上 mesh 又要内网加密时才把 `bUseTls`/Envoy 上行切 TLS,证书负担压回业务。

> 业界趋势(Google BeyondProd / NIST 800-207 零信任)是"不信任内网、东西向也要 mTLS",
> 但实现方式仍是 **mesh/sidecar 托管,而非每个应用手写 TLS**。Pandora 现有接口(`bUseTls` 开关 + Envoy 终止)已预留正确形状。

### 16.3 本次 NetworkPolicy 落地(deploy/k8s/overlays/online/netpol.yaml)

分层最小权限(9 条策略),**只挂 online overlay(生产),dev base 不含**(dev 的 Envoy 在宿主 docker-compose,强制 default-deny 会挡联调):

| 策略 | 作用 |
|---|---|
| `default-deny-ingress` | `pandora` 命名空间所有 Pod 默认拒绝一切入站 |
| `allow-app-mesh` | 业务层(19 服务)之间全通 —— 有意保留的开放面,业务调用图收敛留给 mesh |
| `allow-mysql-ingress` / `allow-redis-ingress` / `allow-kafka-ingress` | 各存储仅接受业务层入站,只开各自端口(3306 / 6379 / 9092) |
| `allow-etcd-ingress` | 业务层连 client :2379;peer :2380 仅 etcd 自身 |
| `allow-zookeeper-ingress` | 仅 kafka 可连 :2181(唯一 infra→infra 边) |
| `allow-ingress-gateway` | 若 Envoy 在独立命名空间,给其命名空间打标签 `pandora.dev/role=ingress-gateway` 即放行网关→业务 gRPC `20001-20022`;未打标签则天然 inert |
| `allow-ds-to-envoy` | default 命名空间里**带 `agones.dev/role=gameserver` 标签的 GameServer Pod** → pandora-envoy `:8444`(DS 回调面;2026-07-10 由「整个 default ns」收紧为「仅 GameServer Pod」) |

**当前真实收益(不夸大):** 阻断其它命名空间→本命名空间(除标签网关);把存储层入站收敛到「仅业务层 + 必要 infra 对等边」,封住「基础设施互连」与「跨 ns」两类横向。**业务服之间仍全通**,那一层的横向最小化待 mesh(见 §16.2)。

`allow-ds-to-envoy` 只建立网络可达边界，不认证 GameServer workload；精确 method 白名单也不认证
允许的 `SetLocation` / `ReportResult` / Heartbeat / GM Poll/Ack。生产残留是补 mTLS、ext_authz 或
绑定 pod/match 的短时效 DS token，不能把现状描述成 DS 身份已可信。

> **2026-07-12 DS 回调鉴权复审修正：**明文 `:8444` 仍是当前开发态，但已升级为 Model B
> 生产启用阻断项。该链路会承载 DS Bearer、玩家入场票据以及 GM poll/stop/drain 响应；
> NetworkPolicy、method 白名单、应用 token 与 credential ACK 都不提供机密性或服务端身份。
> 生产执行 `Apply` 前必须由人拍板并交付 mesh `STRICT mTLS`（含可信 workload 身份/SPIFFE SAN、
> revision 化 CA 与不停服轮换）或等价的 Envoy 双向 TLS。此修正只把 DS `:8444` 提升为先行阻断，
> 不静默推翻其它东西向流量继续由基础设施层逐步接管的既有方向。

设计取舍:**只做 Ingress default-deny,不碰 Egress**(出站全通),避免误伤 DNS 解析、连基础设施、ds-allocator 调 Agones API。出站收紧留作阶段二。

**生效前提(否则"装了但没防"):** 集群 CNI 必须支持并强制 NetworkPolicy(Calico/Cilium/Antrea)。
minikube 默认 kindnet/bridge **不强制** → 策略被静默忽略;minikube 需 `--cni=calico`,云托管集群多数默认支持。

### 16.4 决策行(写入 pandora-arch.md §11)

| 日期 | 决策 | 原因 |
|---|---|---|
| 2026-07-03 | 玩家链路(连接 ②)确认**全程 TLS 无明文**;内网东西向**保持明文 h2c** | 内网加密下沉基础设施层是主流实践,应用零改动、无证书运维 |
| 2026-07-03 | 生产 online overlay 加 **分层 NetworkPolicy**(default-deny-ingress + 业务层 mesh + 存储层按端口收敛 + 网关放行) | K8s 生产最低门槛,收敛「跨 ns」与「存储层」横向;业务服之间暂全通待 mesh;dev 不挂,避免挡宿主 Envoy 联调 |
| 2026-07-03 | 仅做 **Ingress default-deny,Egress 暂全通** | 避免误伤 DNS/基础设施/Agones API;出站收紧列为阶段二 |
| 2026-07-03 | 内网加密未来走 **mesh sidecar mTLS**,不在应用层手写 TLS | 业务零改动;`bUseTls` 开关保留作无 mesh 环境后路 |
| 2026-07-12 | DS `:8444` 明文升级为 Model B 生产启用阻断项，须先落地 mesh STRICT mTLS 或等价双向 TLS | Bearer、玩家票据与 GM 控制响应均属敏感流量；NetworkPolicy/token/ACK 不能替代机密性与服务身份 |
