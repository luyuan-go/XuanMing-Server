# data_service

> 玩家数据统一读写网关:MySQL 强 schema 列(`pandora_player.player_data`)是事实源,Redis pb 二进制作旁路缓存,
> 走 cache-aside(读 miss 回填 / 写后删缓存)+ 乐观锁版本写(`UPDATE ... WHERE version=?`)。**不接 kafka**
> (避免与 `player.update` 事件语义重复)。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-data-service-schema.md`](../../../docs/design/decision-revisit-data-service-schema.md)
> (PlayerData 从 blob 升级为强类型列的拍板)、[`read-cache-strategy.md`](../../../docs/design/read-cache-strategy.md)
> (何时该挂 Redis 旁路缓存);跨服务要约见 [`go-services.md §2.3`](../../../docs/design/go-services.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:玩家数据(`PlayerData`)的统一读写网关。读走 cache-aside,写走 MySQL 乐观锁 CAS + 写后删缓存。
- **事实源**:MySQL `pandora_player.player_data`(每个标量字段一列,强 schema,可查询)。Redis 仅**弱一致旁路缓存**
  ——缓存读写失败不阻断业务,失效靠写后删 + TTL 自愈。
- **上线状态**:截至设计文档记录(`go-services.md §2.3`),本服务**仅用于本地开发 / minikube 验证,未正式上线**,
  当前**无外部调用方**,也无需保留的旧协议 / 缓存 / `player_data` 表有效数据。
- **不做的事**:
  - **不接 kafka**——缓存失效靠写后删缓存 + 主动 `InvalidateCache`,不发 `player.update`(那是 player 服务的活)。
  - **不做第二份玩家档案表**——它是 cache-aside 一致性网关,不是 player 服务 `players` 表的镜像
    (二者互补,见 `decision-revisit-data-service-schema.md §1`)。
  - **不选举 leader / 无后台撮合或扫描循环**——纯请求驱动;唯一的"后台"接线是 nil-safe 的 cell 路由器观测。
  - **不算派生数值**(MMR / 经验换算等仍在各业务服)。

## 端口(`docs/design/infra.md §6`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20003` | 内网服务-to-服务 RPC(**不经 Envoy,不直接暴露给玩家**) |
| HTTP | `:21003` | 仅 `/metrics`(`data_service.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口默认值来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr=:20003` / `Server.Http.Addr=:21003`);
登记见 [`infra.md`](../../../docs/design/infra.md) 端口表。

## 对外接口

代码入口:`internal/service/data.go`(gRPC service 层,实现 `datav1.DataServiceServer`,做 proto Request/Response ↔
biz 入参/出参互转 + `errcode.Code → commonv1.ErrCode` 1:1 映射)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `ReadPlayer(player_id)` | 内网服务(规划:player / trade / battle_result) | cache-aside 读;命中直返,miss 读 MySQL 回填;无数据 → `ERR_NOT_FOUND` | 内网专用 |
| `WritePlayer(data, update_mask)` | 内网服务 | 乐观锁版本写,返回 `new_version`;更新(`version>0`)**必须带非空 `update_mask`**;版本不匹配 → `ERR_DATA_VERSION_MISMATCH` | 内网专用 |
| `InvalidateCache(player_id)` | 内网服务(外部直写 DB 后强制失效) | 主动删缓存;无缓存时幂等 OK | 内网专用 |

> **鉴权模型(与 matchmaker / login 不同,须如实理解)**:本服务**三个 RPC 全是内网专用**,
> **不挂 `AuthRequired` middleware,也不从 JWT 取 `player_id`**——`player_id` 一律取请求体字段
> (`service/data.go` 顶注 + `server/grpc.go:20` 注释)。它**不经 Envoy**,由内网 RPC 黑白名单 / 网络边界限制调用方。
> 因此**没有** matchmaker 那种"内部 RPC 拒绝玩家 JWT(callerID 必须 =0)"的运行时校验——不存在的机制不写。
> 校验只有请求体级:`player_id==0` / `data==nil` → `ERR_INVALID_ARG`。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/data_service/main.go        启动入口(MySQL 强依赖 + Redis 弱依赖 Ping 降级 + store/uc/svc 装配 + cell router 接线)
etc/data_service-dev.yaml       开发期配置(MySQL :3307 / Redis :6380 / cache_ttl 5m / enable_reflection)
etc/data_service-prod.yaml.example  生产配置模板(密码占位,不提交真实密码)
internal/
  conf/conf.go                  配置结构(嵌 pkg/config.Base + DataConf{CacheTTL})+ Defaults()
  service/
    data.go                     RPC 入口(实现 datav1.DataServiceServer;errcode→proto enum 映射)
  biz/
    data.go                     DataUsecase 核心(ReadPlayer/WritePlayer/InvalidateCache 的 cache-aside 编排)
    data_sharding.go            玩家数据 owner cell 锚定的纯逻辑(PlayerDataShardKey + 落点观测,nil-safe)
  data/
    store.go                    MySQLPlayerStore(proto2mysql 建表/同步 + 乐观锁 CAS 读写)
    cache.go                    RedisPlayerCache(pb 二进制 + 字段号位图头,cache-aside 缓存投毒防护)
  server/
    grpc.go                     gRPC server 注册(不挂 AuthRequired)
    http.go                     HTTP server 注册(仅 /metrics)
```

> proto/schema 唯一来源是 `proto/pandora/data_service/v1/data_service.proto`(`PlayerData` message 兼作
> MySQL 表 schema、Redis 缓存值与内网 RPC 载荷)。

## 核心调用链

三条链全部**同步、请求驱动**,无后台 worker、无 saga、无 kafka。锚点以函数名为准。

### 1. ReadPlayer —— cache-aside 读

`service/data.go:ReadPlayer` →(校验 `player_id!=0`)→ `biz/data.go:ReadPlayer`(`biz/data.go:64`):

```
ReadPlayer(player_id)
├─ cache != nil ? cache.Get(player_id)          data/cache.go:Get
│    ├─ 命中且字段号位图校验通过 ──► 直返
│    └─ miss / 反序列化失败 / 位图不满足 ──► 当未命中,继续回落
├─ store.Read(player_id)                         data/store.go:Read(FindOneByPK)
│    └─ ErrNoRowsFound ──► (nil,false,nil) ──► service 转 ERR_NOT_FOUND
└─ fillCache(pd)                                 回填缓存(失败只告警)
```

- 缓存读失败(Redis 故障)只 `Warnw`,继续读 MySQL——旁路缓存不阻断读正确性(`biz/data.go:70`)。
- 回填经 `data/cache.go:Set`,TTL = `cfg.CacheTTL`(默认 5m)。

### 2. WritePlayer —— 乐观锁 CAS + 写后删缓存

`service/data.go:WritePlayer` →(校验 `data!=nil && player_id!=0`)→ `biz/data.go:WritePlayer`(`biz/data.go:102`):

```
WritePlayer(data, update_mask)
├─ version>0(更新)? 校验 update_mask 非空 + 每个 path 是可更新业务列   biz/data.go:107
│                    (空掩码 / 含 player_id·version·未知列 → ERR_INVALID_ARG)
├─ store.Write(pd, updateFields)                                       data/store.go:Write
│    ├─ version==0 ──► INSERT 起始版本 1;主键冲突 → ERR_DATA_VERSION_MISMATCH
│    └─ version >0 ──► UpdateFieldsIfVersion(WHERE version=? SET 掩码列,version+1)
│                       受影响行 0 → ERR_DATA_VERSION_MISMATCH
├─ cache.Del(player_id)                          写后删缓存(失败只告警,TTL 自愈)  biz/data.go:127
└─ logPlayerDataPlacement(...)                   owner cell 落点观测(router==nil 时 no-op)  biz/data_sharding.go:62
```

- **为什么更新必须带非空 `update_mask`**:旧副本读 MySQL 时只读得到"自己认得的列",全量覆盖会把新副本刚加的
  新列**清零**,破坏零停机滚动升级(CLAUDE.md §9 不变量 17)。故只 `SET` 掩码内的列;空掩码在 biz(`biz/data.go:108`)
  与 store(`data/store.go:180`)**双层拒绝**。
- 写 MySQL 前用 `proto.Clone` 克隆入参再改 `version`,不修改调用方 `pd`(CLAUDE.md §5.10 proto 禁止值拷贝,`store.go:161`)。

### 3. InvalidateCache —— 主动删缓存

`service/data.go:InvalidateCache` → `biz/data.go:InvalidateCache`(`biz/data.go:138`)→ `cache.Del`;
`cache==nil`(未配缓存)时直接返回 OK(幂等)。

### 启动装配(`cmd/data_service/main.go`)

```
main
├─ plog.Setup                                    全局 zap logger
├─ 加载 -conf yaml → cfg.Defaults()              填端口 / cache_ttl 默认
├─ MySQL(强依赖):DSN 空 → 直接 os.Exit(1)       main.go:85
│    └─ mysqlx.MustNewClient(pandora_player)
├─ Redis(弱依赖):host/addrs 有配才建;Ping 3s   main.go:96
│    ├─ Ping OK   ──► cache = NewRedisPlayerCache
│    └─ Ping 失败 ──► Warnw 降级为直连 MySQL(cache=nil)
├─ NewMySQLPlayerStore(db)                        proto2mysql RegisterAllTables + SyncAllTables 建表/同步
├─ NewDataUsecase(store, cache, cfg.Data, logger)
├─ etcdtable.WireRouter(cfg.CellRoute, uc.SetCellRouter)  cell 路由器接线(单 Cell/dev 为 nil,nil-safe)  main.go:121
└─ kratos.New(grpcSrv, httpSrv).Run()             阻塞
```

- **依赖策略**:MySQL 强依赖(事实源,连不上直接退出);Redis 弱依赖(Ping 失败降级,不退出)。
- `NewMySQLPlayerStore` 启动时经 proto2mysql `RegisterAllTables` 自动扫描声明了
  `(proto2mysql.table_name)` 的 message(即 `PlayerData`)并 `SyncAllTables` 建表 / 补缺列 / 对齐类型
  ——**schema 唯一来源是 pb**,不手写 DDL(`data/store.go:107`)。

## 存储布局

### MySQL(事实源)

| 表 | 库 | 主键 | 乐观锁列 | 建表方式 |
|---|---|---|---|---|
| `player_data` | `pandora_player` | `player_id`(uint64) | `version`(uint32,每次写 +1) | 启动时按 `PlayerData` proto 经 proto2mysql `SyncAllTables` 自动建 / 同步 |

`PlayerData` 列(即 proto 字段,`data_service.proto`):`player_id` / `version` / `nickname` / `level` / `mmr` /
`avatar` / `created_at_ms` / `last_seen_ms` / `total_battles` / `total_wins`。可更新业务列(update_mask 合法路径)=
全字段去掉主键 `player_id` 与乐观锁列 `version`,由 `data/store.go:buildPlayerDataUpdateFields` 从 proto 描述符**动态推导**
(新增 proto 字段自动纳入,不手工维护)。

### Redis(旁路缓存)

| Key 模板 | 值 | TTL |
|---|---|---|
| `pandora:data:player:<player_id>` | 格式头 + `PlayerData` protobuf bytes | `cache_ttl`(默认 5m) |

**缓存值格式头(滚动升级缓存投毒防护,`data/cache.go` 顶注)**:`4 字节魔数 PDC\x02` + `4 字节位图长度(BE uint32)`
+ `本副本字段号位图` + `PlayerData pb`。读取时(`data/cache.go:Get`):

- 魔数不符(旧裸 pb / 旧 `PDC\x01` / 脏字节)→ 当**未命中**回落 MySQL;
- **写入方字段号位图 ⊇ 本副本位图**(`writerHasAllReaderFields`,`cache.go:84`)才信任——否则条目可能缺本副本认得的新列
  (旧副本读库丢新列后写缓存会投毒新副本),当未命中;
- 反序列化出的 `player_id` 必须与 key 一致(串号防御纵深)。

> 用"字段号位图 + 超集判定"而非"最大字段编号"当版本:后者会漏"编号空洞里加字段"与"reserved 删最高编号"
> 两类合法演进(`cache.go` 顶注三审 P1-7)。

## 分片落点观测(nil-safe,非权威)

`internal/biz/data_sharding.go` 只落**服务内纯逻辑 + 可观测日志**,不改 cache-aside 路径:

- `PlayerDataShardKey(playerID)` = `player_id` 十进制串,是玩家数据存储分片键的**唯一口径**
  ——**不取 version / data 内容 / 任何配置 ID**(与落点无关),保证 PlayerData 与档案 / 背包 / 好友等同源 owner 数据
  落同一 owner cell(`scale-cellular-20m.md §4.2` owner 不变量)。
- `logPlayerDataPlacement`(`data_sharding.go:62`)在 `WritePlayer` 成功后经确定性 `cellroute.Router` 解析
  owner `(region, cell)` 打一条 `Debugw` 观测日志。
- **router 为 nil**(单 Cell / dev / 分片阶段 1~2,未经 `SetCellRouter` 注入)→ 整条 no-op,行为与历史一致。
  真正的"MySQL 按 owner cell 分库 / 缓存按 cell 分区"属基础设施,由 Codex / 人接,本服务只暴露观测口径。

## 配置项

| 键 | 默认 | 说明 |
|---|---|---|
| `data.cache_ttl` | `5m` | Redis 缓存条目 TTL(读 miss 回填按此 TTL);来自 `conf.go:DataConf.CacheTTL` |
| `server.grpc.addr` | `:20003` | gRPC 监听(`conf.go:Defaults()` 兜底) |
| `server.http.addr` | `:21003` | HTTP `/metrics` 监听(`conf.go:Defaults()` 兜底) |
| `server.grpc.enable_reflection` | `false`(dev yaml 开 `true`) | gRPC reflection,便于 grpcurl 联调,生产不开 |
| `server.grpc.max_conn_age` | 见 yaml(dev `15m`) | 达龄 GOAWAY 重拨,滚动更新流量滚到新副本 |
| `node.mysql_client.dsn` | 无(**必填**) | `pandora_player` 库 DSN;为空启动失败(`main.go:85`) |
| `node.redis_client.host` / `.addrs` | 无(选填) | 单实例填 `host`,Cluster/Sentinel 填 `addrs`;两者皆空 → 无缓存直连 MySQL |

> `data.cache_ttl` 是本服务**唯一私有配置字段**(`conf.go:DataConf`);其余键来自 `pkg/config.Base`
> (`server` / `node` / `cell_route`)。

## 本地启动

```powershell
# 1. 基础设施(MySQL :3307 + Redis :6380;player_data 表由服务启动时按 pb 自动建,不走 mysql-init DDL)
pwsh tools/scripts/dev_up.ps1

# 2. 启 data_service(dev 配置,开 gRPC reflection 便于 grpcurl)
cd F:\work\XuanMing-Server
go run ./services/data/data_service/cmd/data_service -conf services/data/data_service/etc/data_service-dev.yaml
```

> dev 切换前(schema 从旧 blob 迁到强类型列)如库里残留旧 `data BLOB` 列或旧结构 Redis 缓存,
> 直接清空即可:`DROP TABLE IF EXISTS pandora_player.player_data;`(服务启动按新 pb 重建)+
> `DEL pandora:data:player:*`(`data/store.go` / `data/cache.go` 顶注)。仅适用于**未上线、无有效历史数据**的 dev 环境。

## 关联文档

- [`go-services.md §2.3`](../../../docs/design/go-services.md) — data_service 要约(职责 / RPC / 为什么单独抽)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口登记(20003 / 21003)与 `pandora.player.update` topic
- [`decision-revisit-data-service-schema.md`](../../../docs/design/decision-revisit-data-service-schema.md) — PlayerData 从 blob 升级为 pb 驱动强 schema 列的拍板与实施记录
- [`read-cache-strategy.md`](../../../docs/design/read-cache-strategy.md) — MySQL 服务何时该挂 Redis 旁路缓存(cache-aside + 写后删)
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2 — 玩家 owner 数据必落同一 cell(分片落点口径)
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — 不变量 16/17:Redis pb 双向兼容 + update_mask 防清零新列
