# Pandora 配置与服务发现:为什么线上不手填 IP

> **状态**:规范说明(2026-06-30 记录)
> **本文档地位**:回答「这么多 yaml,线上 k8s 每个都要改自己 IP 吗 / 中间件是集群怎么办」。配置寻址的统一口径。
> **关联**:`infra.md`(资源命名)、`scale-cellular-20m.md`(Redis Cluster / MySQL 分库)、`pandora-arch.md §11`(决策行)、`deploy/k8s/overlays/online/`(线上 overlay)。
> **一句话**:**线上不是「每个 yaml 填自己 IP」,而是「写服务名 + 靠 service discovery」**;Pod 是不是集群、有几个副本,对配置完全透明。

## §1 核心结论

- **服务间 / 中间件寻址一律写稳定的 DNS 名,绝不写裸 IP。** k8s Service 把名字解析到当前 Pod IP;Pod 重启 IP 变,名字不变。
- **dev 和 cluster 是两套配置**:`*-dev.yaml` 写 `127.0.0.1` 仅本机自测;线上走 `run/cluster/etc/*.yaml`,全是 DNS 名(`redis:6379` / `kafka:9092` / `mysql:3306` / `player:20002`)。线上**不碰** dev 那套。
- **真实外部地址只在一处注入**(ExternalName Service / Secret),20 个 Deployment 配置不动。改 1 处,不是改 20 处 —— 这是防手抖的关键。

## §2 两类目标,机制不同

### §2.1 无状态 go 服务(login / player / friend …)→ 完全透明

- k8s Service = 负载均衡入口。配置写 `player:20002`,请求自动轮询到 player 任一健康 Pod。
- 副本 1 → N **配置一字不改**,扩缩容随便加(副本数在 `deploy/k8s/overlays/online/kustomization.yaml` 的 `replicas` 调)。
- 前提:服务无状态(状态在 Redis/MySQL,不在 Pod 内存)。本项目 go 服务均为 headless gRPC,符合。

### §2.2 有状态中间件(redis / mysql / kafka)→ 集群也没事,但「名字」语义不同

集群版中间件自带专门接入名,客户端连那个名字,客户端库处理分片/主从:

| 中间件 | 集群形态 | 配置里写什么 | 本项目落点 |
|---|---|---|---|
| **Redis** | Cluster | 连任一/多节点名,客户端按 slot 自动路由 | `redisx.NewUniversalClient` 同时支持单机/Cluster,**代码不改**(决策 2026-06-19) |
| **MySQL** | 主从 / TiDB | 写连主名(`mysql-primary`),读连 `mysql-read`;TiDB 直连其 SQL 入口名,对客户端像一台 MySQL | `mysqlx.ShardSet` 已铺路;社交库走 TiDB |
| **Kafka** | 多 broker | `brokers` 写一两个 broker 名当 **bootstrap**,客户端自动发现其余 broker | `kafka:9092` 当 bootstrap |

要点:**集群中间件不需要把每个节点 IP 列进配置**,给一个稳定接入名,客户端库负责拓扑发现和路由。

## §3 防错护栏

1. **单一事实源**:服务配置只写 DNS 名,绝不写裸 IP;外部地址集中到 ExternalName / Secret 一处。
2. **apply 前 diff**:`kubectl --context <expected> diff -k deploy/k8s/overlays/online` 只用于结构预览；
   仓库内镜像仍是 `registry.example.com/latest` 占位，真实 registry/tag 由 `start.ps1 -Mode online` 临时注入。
3. **kube-context 确认**:`start.ps1 -Mode online` 强制把 test/prod 映射到各自允许的 context，且所有
   变更命令显式 `--context`，不依赖可被其它终端切换的 current-context。
4. **配置 schema 校验**:服务启动对配置做校验,缺字段/格式错直接启动失败,不带病运行。

## §4 单机 → 集群切换要注意的坑

- **Redis 单机 → Cluster**:跨 key 的多键操作 / 事务 / Lua 多 key,Cluster 要求 key 同 slot(用 hash tag `{}`)。切前扫一遍 `redislock` 等多 key 操作。
- **MySQL 读写分离**:引入从库后「写走主、读走从」,不能把写打到只读名上。现单 MySQL 无此问题,扩时再处理。

## §5 一句话总结

从单机长到集群,基本是「**换地址名 + 调副本数**」,不是「重写一堆 IP」。`redisx.NewUniversalClient` / `mysqlx.ShardSet` / Kafka bootstrap 都按这个思路铺,Pod 集群化对「写服务名不写 IP」这套完全透明。
