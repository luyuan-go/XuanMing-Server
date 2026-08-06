# Pandora 告警通知链路设计(alerting)

> 状态:本地 compose 链路已落地(2026-07-10)。「错误 / 警告超水位 → 飞书 / 企微群 / 手机推送」的统一出口。

## 1. 背景与问题

服务运行中会产生「超水位」信号:战斗 DS 并发接近容量上限、Redis / Kafka 依赖异常、RPC 错误率飙升等。
之前这些信号只落在日志 / Prometheus 指标里,需要人主动去看 Grafana 才发现。要的是**主动推送**到
运维随时能看到的地方(飞书群 / 企业微信群 / 手机)。

早期设想让业务服务(ds_allocator 等)直接 HTTP POST 通知端点(如 ntfy),被否决,理由:
- 14 个服务各自接通知 = 14 处去重 / 限流 / 重试 / secret 管理,重复且易漂移;
- 通知渠道换绑(飞书→企微→PagerDuty)要改所有服务代码;
- 与既有 Prometheus + Grafana + Loki 可观测栈职责重叠。

## 2. 决策:告警出口唯一,业务服务不感知通知渠道

```
业务服务 ──暴露指标 / 打结构化日志──► Prometheus / Loki
                                          │
                                   Grafana 统一告警(规则 + 路由 + 去重 + 静默)
                                          │
                    ┌─────────────────────┼──────────────────────┐
                 企业微信群              飞书群                 ntfy(手机/桌面)
                 (原生 wecom)      (webhook→已验证 relay)       个人推送
```

- **业务服务只做两件事**:① 暴露 `pandora_<svc>_*` 指标;② 打 snake_case 结构化日志事件
  (如 `ds_fleet_capacity_near_limit`)。**绝不在服务内直连飞书 / 企微 / ntfy**。
- **告警出口唯一 = Grafana Unified Alerting**。规则、通知路由、去重、静默全集中在 Grafana,
  换渠道零改业务代码。
- 复用现有 Grafana(不引入 Alertmanager):Grafana 告警能同时消费 Prometheus 指标**和** Loki 日志,
  一套搞定;Alertmanager 只能消费 Prometheus 指标,日志告警仍要 Grafana,反而两套。

## 3. 群通知 vs 个人推送(为什么两者都要)

| 渠道 | 用途 | Grafana 集成 | 备注 |
|---|---|---|---|
| 企业微信群 (wecom) | 团队群通知主通道 | 原生 contact point | 填完整群机器人 webhook URL |
| 飞书群 (feishu) | 飞书团队群通道 | webhook + 转换 relay | Grafana 11.3 无原生 Lark 类型,见 §4 |
| ntfy | 值班手机 / 桌面个人推送 | webhook 直连 | v2.11 用 URL inline template 提取 Grafana JSON 的 title/message |

- **群通知**优先用 Grafana 原生 wecom(企业微信)。飞书需转换器(§4)。
- **个人推送**用 [ntfy](https://github.com/binwiederhier/ntfy):critical 级告警除了进群,再推一份到
  值班手机,确保深夜也看得到。ntfy 擅长个人推送,不擅长群消息格式,故不拿它发飞书 / 企微群。

## 4. 飞书群为什么要「转换器」

飞书自定义机器人要求 body 形如 `{"msg_type":"text","content":{"text":"..."}}`,与 Grafana 原生
webhook 的 alert JSON 不符。需部署一个**已做实际消息验证**的 Grafana→飞书 relay，或自建
轻量 relay(收 Grafana webhook → 转飞书 body → POST 飞书机器人)。

`prometheus-webhook-dingtalk` 只生成钉钉格式，**不是飞书转换器**，不准用它填这个地址。

在转换器部署前,飞书群留空不发;企微也未配时，所有告警回退 ntfy(不阻断)。

## 5. secret 纪律(CLAUDE.md §3 / AGENTS.md)

所有 webhook URL / 群机器人 key 属 secret,**不入 git**:
- provisioning 保留 `$__env{VAR}` 占位;
- 真实值只写被 git 忽略的 `deploy/env/dev.env` 或部署环境;
- 受跟踪的 `deploy/env/dev.env.example` 只放空占位与非 secret 默认值;
- `deploy/grafana/entrypoint.sh` 仅在群渠道 env 非空时才生成 receiver。Grafana 不接受空 webhook URL，
  不能用「空 URL 静默失败」作为禁用方式。

ntfy URL 需保留 `?tpl=1&t=%7B%7B.title%7D%7D&m=%7B%7B.message%7D%7D`，否则 v2.11 会把整段
Grafana webhook JSON 当成通知正文。

涉及的环境变量:`PANDORA_ALERT_WECOM_URL` / `PANDORA_ALERT_FEISHU_WEBHOOK` /
`PANDORA_ALERT_NTFY_URL` / `PANDORA_NTFY_BASE_URL` / `PANDORA_NTFY_BIND_HOST` /
`PANDORA_NTFY_BIND_PORT`。

`PANDORA_NTFY_BIND_HOST` 默认 `127.0.0.1`。手机在局域网直连时才改为 `0.0.0.0`，并把
`PANDORA_NTFY_BASE_URL` 改为开发机局域网地址;它与 `PANDORA_OBSERVABILITY_BIND_HOST` 分离，
避免为了手机推送同时暴露 Grafana/Prometheus/Loki。

`PANDORA_NTFY_BIND_PORT` 默认 `8090`，**不是 ntfy 官方的 8080**:8080 是 Jenkins 默认端口，
而本仓库自带 `pandora-jenkins`(`deploy/docker-compose.devops.yml`)，两者同时起必然抢端口;
抢不到时 compose 在建网络阶段就失败，ntfy 的 `required:false` 根本来不及生效，会连带整套
基础设施启动中止(2026-08-05 实测)。容器内仍监听 80，Grafana 走内网 `http://ntfy:80` 不受影响，
这层映射只给人在浏览器里开 ntfy 页面用。改端口须同时改 `PANDORA_NTFY_BASE_URL` 的端口。

## 6. 文件清单

| 文件 | 作用 |
|---|---|
| `deploy/grafana/provisioning/alerting/contact-points.yaml` | 始终有效的 ntfy 基础联系点 |
| `deploy/grafana/provisioning/alerting/templates.yaml` | 统一通知文案模板 |
| `deploy/grafana/provisioning/alerting/notification-policies.yaml` | 无群渠道时的 ntfy 安全默认路由 |
| `deploy/grafana/provisioning/alerting/rules.yaml` | 告警规则(首批:DS 容量接近上限 / 打满) |
| `deploy/grafana/entrypoint.sh` | 按非空 env 生成群 receiver 与分级路由 |
| `deploy/docker-compose.dev.yml` | Grafana 注入告警 env + 新增 ntfy 服务 |
| `deploy/env/dev.env.example` | 可提交的环境变量样例;`dev.env` 仅本机保留 |

## 7. 首批告警规则(消费 ds_allocator 容量巡检指标)

ds_allocator 在 mode=agones 下定期巡检 Agones Fleet 容量(`internal/biz/capacity.go`),暴露:
- `pandora_ds_allocator_fleet_replicas` / `_ready` / `_allocated` / `_usage_ratio`(label=fleet)。

规则:
- **warning** `pandora-ds-fleet-near-limit`:`usage_ratio ≥ 0.8` 持续 5m → 已配置群渠道;无群时回退 ntfy。
- **critical** `pandora-ds-fleet-exhausted`:`ready == 0` 持续 2m → 已配置群渠道 + ntfy;无群时发 ntfy。

两条容量规则显式 `noDataState: OK`:local/mock 模式或 ds_allocator 未运行时不会把「没有
Agones 容量指标」误报成容量告警。服务/抓取不可用应另建 `up` / `absent_over_time`
可用性规则，不与容量告警混用。若修改服务的 `capacity_warn_ratio`，必须同步本规则的 0.8。

业务侧同时打日志 `ds_fleet_capacity_near_limit` / `ds_fleet_capacity_exhausted`(已做状态变化降噪),
Loki 里也能查 / 告警。

## 8. 扩展方式(新增告警照抄)

1. 服务暴露 `pandora_<svc>_<metric>` 指标(或打可被 LogQL 匹配的 snake_case 日志事件);
2. 在 `rules.yaml` 追加一条规则(一个查询 refId A + 阈值 refId C),打 `severity` + `service` label;
3. 通知路由 / 联系点 / 文案已通用,通常无需改;需要新可选渠道时同步修改
   `entrypoint.sh`、`dev.env.example` 与本文，不要向基础 `contact-points.yaml` 写空 URL。

## 9. 待办 / 未决

- [ ] 飞书群转换器(§4):部署已实测的 relay 后,填 `PANDORA_ALERT_FEISHU_WEBHOOK`。
- [ ] k8s 侧:本文件目前只接 compose 观测栈;k8s Grafana(若独立部署)需同步这套 provisioning。
- [ ] ntfy 公网鉴权:公网暴露 ntfy 必须开 auth 或用 ntfy.sh 私密 topic(dev 默认回环无鉴权)。
- [ ] Grafana 安全升级:当前仍固定 11.3.1;DingDing 联系点因
  [CVE-2025-3415](https://grafana.com/security/security-advisories/cve-2025-3415/) 未启用。经人授权
  升到 `11.3.7+security-01` 或更高安全版后再评估恢复。
- [ ] 扩展更多规则:Redis/Kafka 依赖异常、RPC 错误率、心跳超时率等。
