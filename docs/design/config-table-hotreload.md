# Pandora 配置表热更流水线

> **状态**:核心链路已落地(2026-07-21;2026-07-25 增 `item` + `talent`,现有四表,§10 有落点清单),方向 2026-06-30 拍板
> **本文档地位**:策划配置表(`Table`)→ JSON → 服务端热加载 的契约与目录约定。
> **关联规范**:`CLAUDE.md §5`(proto 优先 / 配置表 ID `uint32`)、`infra.md`(资源命名)、`pandora-arch.md §11`(决策行)。
> **一句话**:这是**配置发布 / 热更流水线**,不是分布式配置中心;Apollo / Nacos **现在不接**,以后只可能当"发布通知 / 版本号",不当大量表 JSON 的主存储。

## §0 核心做法(最重要,别忘)

**版本号 + checksum + staging 目录 + reload 接口 + 加载成功才切换 + 失败保留旧配置 —— 这套就是游戏配置表热更的标准做法,直接照做。**

| 环节 | 作用 | 本项目落点 |
|---|---|---|
| **版本号** | 单调递增,防回退/重放,可追溯回滚 | manifest `version`(§5) |
| **checksum** | 逐表内容哈希,防拷贝截断/篡改 | manifest 每表 `checksum`(§5) |
| **staging 目录** | 新批次先落地、不碰线上 | `staging/` → 校验通过才进 `active/`(§4) |
| **reload 接口** | 受控触发加载,不对客户端开放 | etcd 版本键 watch(多机)/ gRPC RPC(单机)(§6) |
| **加载成功才切换** | 新表加载+校验全过,才原子替换内存指针 | `atomic.Pointer` / `AtomicTable`(§3.1) |
| **失败保留旧配置** | 任一步失败不切换,线上不受影响 | 失败回退 + 告警(§3.1) |

**发布通知用 etcd**(复用现有 `pkg/cellroute/etcdtable`、`pkg/snowflake/etcdnode`),etcd 只存 `version` 键不存表体;单机/dev 连 etcd 都不必,直接调 reload RPC。**不引入 Apollo/Nacos。**

## §1 场景与定位

```
F:\work\Pandora-Client-SVN\Table        # 策划源表(Excel / CSV / 自定义表)
        │  ① 工具生成 + 校验
        ▼
        *.json                            # proto 字段对齐的 JSON 产物
        │  ② 发布(版本号 + checksum + staging)
        ▼
F:\work\XuanMing-Server  服务端           # ③ 热加载:先加载新表 → 校验 → 原子切换 → 失败保留旧表
```

本质是**游戏/业务配置表热更新流水线**,不是「服务一启动就读一次」的静态配置,也不是分布式动态配置中心。

## §2 要不要接 Apollo / Nacos —— 现在不接

**结论:现阶段不上 Apollo / Nacos / Consul 当核心。** 理由:

- 本场景核心是「大量表 JSON 的生成、校验、发布、热加载」,Apollo / Nacos 擅长的是「少量键值配置 + 实时推送 + 权限审批」,不匹配。
- 重型平台引入运维成本(部署、依赖、SDK 接入),当前单机 / dev 阶段是负收益。
- 开源库**用在局部**是合理的(读 Excel/CSV、JSON schema 校验、文件监听、diff、HTTP reload 客户端),这不等于「不用开源库」。

### §2.1 什么时候才考虑接(触发条件)

满足下列**任意一条**且确有痛点时,再评估引入,且只让它管「发布通知 / 当前版本号 / 多机统一刷新」,**不让它存大量表 JSON**:

1. 多环境配置分治需要平台化管理(dev / test / prod 不只是目录区分)。
2. 需要多人权限、发布审批、操作审计。
3. 需要 Web 控制台改配置 + 一键回滚 + 灰度发布。
4. 多台服务器要「一次发布、统一收到刷新通知」,且 §3 的自研通知方式扛不住。

> 注:多机统一刷新这一条,本项目已有 `etcd watch`(见 `pkg/cellroute/etcdtable`、`pkg/snowflake/etcdnode`)可复用作"发布通知 / 版本号广播",**优先用现有 etcd,而不是新引入 Apollo / Nacos**。

## §3 流水线设计(自研轻量版)

1. **生成**:工具读 `Table` 源表 → 输出 proto 字段对齐的 `*.json`。
2. **校验**:生成阶段严格校验(字段名、类型、枚举、引用完整性、未知字段一律报错),不通过不产出。
3. **版本**:产出 `version`(单调递增 / 内容哈希)+ 每个文件 `checksum`,写一个 manifest。
4. **发布**:产物拷贝到服务端 **staging 目录**(不直接覆盖线上目录)。
5. **通知**:调服务端 `reload` 接口 / 写 etcd 版本号触发 watch。
6. **热加载**:服务端先把新表加载进**临时结构**并校验通过,再**原子切换**内存指针(参考 `pkg/cellroute` 的 `AtomicTable` 整表替换思路)。
7. **失败回退**:加载 / 校验任一步失败,保留旧表,不影响线上,记日志告警。

### §3.1 不变量

- **加载成功才切换**:任何一张表加载或校验失败,整批不切换(或按表粒度回退),线上始终是一份完整自洽的配置。
- **原子整表替换**:运行时读配置走原子指针,不做"边读边改",避免读到半截表。
- **版本可追溯**:每次生效记录 version + checksum,便于回滚定位。

## §4 目录约定

源表在 UE 客户端仓库,产物落后端仓库。各阶段目录职责单一,**不允许 ad-hoc 路径**(对齐 `infra.md` 命名总则)。

```
# ① 源表(UE 客户端仓库,SVN)
F:\work\Pandora-Client-SVN\Table\
    hero.xlsx                       # 策划维护的源表(Excel / CSV / 自定义)
    skill.xlsx
    item.xlsx
    ...

# ② 生成产物(后端仓库,git 跟踪;proto 字段对齐的 JSON + manifest)
F:\work\XuanMing-Server\configtable\dist\
    manifest.json                   # 本批次清单(version + 每表 checksum),见 §5
    hero.json
    skill.json
    item.json
    ...

# ③ 服务端运行态目录(git 不跟踪,发布时落地)
<deploy-root>\configtable\
    staging\                        # 新批次先落这里,未生效
        manifest.json
        *.json
    active\                         # 当前生效批次(热加载从这里读 / 切换后指向这里)
        manifest.json
        *.json
    history\                        # 旧批次留档,按 version 命名,供回滚
        v<version>\
```

约定:

- **源表只在 SVN 仓库**,后端仓库**不放 Excel 源表**,只放生成出来的 JSON(可审 diff)。
- **文件名 = 表名(snake_case)**,一表一文件,文件名与 proto message / 加载注册键一一对应。
- `staging` / `active` / `history` 三段分离:发布只写 `staging`,生效才原子切到 `active`,旧批次进 `history`。

## §5 manifest 契约

每批产物带一个 `manifest.json`,是发布与热加载的**唯一权威清单**(服务端以它为准决定加载哪些表、校验是否完整):

```jsonc
{
  "version": 20260630001,           // 单调递增版本号 = 日期*1000+当日序号(生成器自动;JSON 数字不能带下划线)
  "generated_at_ms": 1751270400000, // 生成时间(毫秒)
  "generator": "configtable-gen@1.0.0",
  "source_rev": "svn-r12345",       // 源表 SVN 版本,便于追溯
  "tables": [
    {
      "name": "hero",               // 表名 = 文件名(去 .json)= 注册键
      "file": "hero.json",
      "proto": "pandora.config.HeroTable", // 对应 proto message 全名
      "checksum": "sha256:ab12...",  // 文件内容哈希
      "rows": 128                    // 行数,加载后断言一致
    }
    // ... 其余表
  ]
}
```

不变量:

- 服务端**只加载 manifest 列出的表**;`active` 目录里有 manifest 之外的文件 = 视为脏数据,告警。
- 加载前**逐表校验 checksum**,不匹配整批拒绝(防止拷贝过程被截断)。
- `version` **单调递增**;收到的 version **<** 当前 active version 时拒绝(防回退 / 重放),
  除非显式回滚指令。version **==** 当前 active 是幂等确认(reload 返回 code=OK +
  activeVersion,发布脚本崩溃续跑 / 服务重启后内存落后磁盘靠它收敛),不算拒绝。

## §6 reload 接口契约

服务端暴露一个**受控的** reload 入口(运维 / 调试用,**不对客户端 / Envoy 开放**,对齐 `CLAUDE.md §5.11` 例外条款:鉴权 + 不经 Envoy)。

- **触发方式二选一(可并存)**:
  1. **etcd 版本键**:发布方写 `pandora/configtable/version` = 新 version,服务端 watch 到变更后自行从 `staging` 拉取加载。**多机统一刷新优先用这个**(复用现有 `pkg/cellroute/etcdtable` 模式)。
  2. **gRPC reload RPC**:`ReloadConfigTable(version)` → 服务端校验 + 加载 + 原子切换。单机 / dev 直接调。
- **语义**:
  - **幂等**:同一 version 重复 reload 不产生副作用(已生效则直接返回当前状态)。
  - **同步返回加载结果**:成功返回生效 version;失败返回错误原因(哪张表、哪行、何种校验失败),**且不切换**(保留旧表)。
  - **原子切换**:加载进临时结构 → 全表校验通过 → 原子替换内存指针(`atomic.Pointer` / 参考 `pkg/cellroute` 的 `AtomicTable`)。

**分布式边界**:`atomic.Pointer` 只保证一个服务进程内的批次原子性。当前
`configtable_publish.ps1 -ReloadAddr` 只面向单实例/dev;逐个调用多个副本会出现部分成功窗口,
不能宣称全 fleet 原子热更。生产多副本一致切换仍需版本通知、全副本确认与业务写入口屏障;
在此之前,会改变不可逆结算结果的表(如玩家升级曲线)必须先关入口再滚动收敛。
- **响应/请求结构**:按 `CLAUDE.md §5` 用 proto message 定义(`ReloadConfigTableRequest` / `ReloadConfigTableResponse`),不手写并行 struct。

## §7 校验清单(生成阶段严格执行)

生成器在产出 JSON 前**全部通过才落盘**,任一失败则整批不产出并报错定位到「表 + 行 + 列」:

1. **字段名对齐**:列名 → proto 字段名映射完整,无拼写错;`protojson` 未知字段一律报错(见 §8.3)。
2. **类型合规**:数值 / 布尔 / 字符串类型匹配 proto;配置表 ID 用 `uint32`(`CLAUDE.md §5.6`)。
3. **枚举合法**:枚举列取值在 proto enum 定义内,统一用名字或数字一种。
4. **主键唯一**:每表主键(如 `hero_id`)无重复。
5. **引用完整性(外键)**:跨表引用的 ID 必须在被引用表中存在(如 `hero.skill_id` 必须在 `skill` 表里)。
6. **非空 / 范围**:必填列不为空;有范围约束的数值在合法区间。
7. **行数一致**:产出行数写入 manifest,服务端加载后断言一致。

## §8 用 proto 读 JSON 的三个硬约束

方向正确(契合 `CLAUDE.md §5.8`「新增结构优先 proto、不写并行 struct」):给每张表定义 proto message,JSON 用 `protojson.Unmarshal` 读入 proto 结构。但 `protojson` ≠ 任意 JSON,生成器必须钉死下列三件事:

1. **字段名对齐**:`protojson` 认 `json_name`(默认 camelCase)与 proto 原名;生成 JSON 的 key **必须与 proto 字段名 / `json_name` 完全一致**,团队统一一种(建议 proto 原名 snake_case),否则字段静默丢失或报错。
2. **64 位整数是字符串**:proto3 JSON 规范规定 `uint64` / `int64` 序列化为 **string**(如 `"123"`)。配置表 ID 按 `CLAUDE.md §5.6` 默认 `uint32`,**不踩这个坑**;若某表确需 `uint64`,JSON 必须写成字符串。
3. **未知字段策略**:`protojson` 默认遇到多余字段**报错**——这对生成阶段校验是好事(能抓出列名写错)。**生成 / 发布阶段严格(不容忍未知字段);运行时加载如需向前兼容,显式 `DiscardUnknown: true`**。

补充:枚举可用名字或数字,团队统一一种;repeated / 嵌套 message 适合带子结构的表。

## §9 决策小结

- 配置表热更 = **自研轻量流水线**(生成→校验→版本→staging→通知→原子热加载→失败回退)。
- Apollo / Nacos **不作为核心**;未来若需「发布通知 / 版本号 / 多机刷新」,**优先复用现有 etcd watch**,Apollo / Nacos 仅在 §2.1 触发条件成立时再评估,且只管元数据不存表体。
- proto 读 JSON 落地前,生成器钉死「字段名对齐 / 64 位整数字符串 / 未知字段策略」三件事。

## §10 落地任务清单(2026-07-21 核心链路已落地,落点如下)

> 实现归属按 `AGENTS.md §11.1`:业务/生成器/加载器逻辑由 Claude 实现+验证,环境/SVN/git 收尾由 Codex/人。
> 移植说明:运行时访问模式移植自旧项目 mmorpg 的 `go/shared/generated/table`(TableManager),
> 三处标准化改造:全批快照 + `atomic.Pointer` 原子切换(旧为普通指针赋值)、失败返回 error 保留旧表
> (旧为 `log.Fatalf`)、manifest 驱动整批 all-or-nothing(旧为逐表独立加载)。

1. [x] 各表 proto message:`proto/pandora/config/v1/` 下 `level.proto`(关卡表)、
       `player_level_exp.proto`(玩家等级经验表,源 `角色/j_玩家等级经验.xlsx`)、
       `item.proto`(道具表,源 `道具/d_道具.xlsx`)、`talent.proto`(专精表,源 `角色/z_专精.xlsx`)。
       后两张 2026-07-25 接入,专供 player 服务做出战养成的权威校验(见「已接线服务」)。
       ⚠️ 专精源表目前是可用脚手架(8 个节点、每级消耗均为 1),数值待策划出终稿后整表替换;
       proto 契约与服务端校验逻辑不随数值变动。
2. [x] `configtable-gen` 生成器:`tools/configtable-gen`(Go,独立 module;stdlib 自实现 xlsx 最小读取器,
       无 Python 依赖)。§7 校验齐;产物确定性序列化(protojson Compact→Indent);version 自动单调
       (YYYYMMDD*1000+seq);同内容幂等不写盘。当前批次 `configtable/dist` 为
       v20260725001 / `svn-r1412+local-uncommitted`(`item` 100 行、`level` 9 行、
       `player_level_exp` 15 行、`talent` 8 行)。
       ⚠️ 该批次的 `source_rev` 带 `+local-uncommitted`:生成时 `d_道具.xlsx` 处于 SVN 修改态、
       `z_专精.xlsx` 尚未纳管,产物无法从 r1412 复现。**发布前必须先提交两张源表,再用真实 rev 重跑生成器**。
       注意生成器只拒空白 / `unknown` 占位,不会替你发现"源表没提交",这道门靠人。
3. [x] 服务端加载器:`pkg/configtable`(manifest 校验 + checksum + 行数断言 + 运行时 `DiscardUnknown` +
       version 单调防回退 + 全批成功才 `atomic.Pointer` 切换 + 未知新表跳过告警/脏文件告警)。
4. [~] reload 入口:gRPC `ConfigTableAdminService.ReloadConfigTable` 已落(matchmaker/player/
       **ds_allocator**(2026-08-04,:50020)内部 gRPC 端口,幂等/expect_version/失败保留旧表);
       **etcd 版本键 watch + fleet 确认(多机)待排期**,单机/dev 用 RPC 已够。
5. [x] staging → active 切换 + history 留档:`tools/scripts/configtable_publish.ps1`(文件面;回滚 =
       重新生成更高版本发布,脚本拒绝**低版本**覆盖;**同版本**是文件面 no-op 但仍触发
       幂等 reload——崩溃续跑 / 服务重启后内存落后磁盘靠它收敛,不算拒绝)。
6. [x] 发布脚本同上(可选 `-ReloadAddr` 用 grpcurl 触发 reload)。

**已接线服务**:

- matchmaker:`config_table.dir` 开关,空=不启用;启用后启动强依赖 fail-closed,StartMatch 校验
  map_id ∈ 关卡表且 category=战斗,否则 `ERR_MATCH_INVALID_MAP`。当前集群模板仍未默认打开该开关。
- player:配置表为启动强依赖;从 `player_level_exp` 生成本次经验事务的曲线副本,加载/热更均执行
  整表不变量校验,并拒绝降低最高等级。**2026-07-25 起同时强依赖 `item` + `talent` 两表**:
  加载校验器要求两表存在且 `talent` 整树校验(前置存在 / 前置等级不超上限 / 依赖无环)通过,
  缺表或坏树整批不切换。运行期 `SetEquipment` 用 `item.MatchesSlot` 做 isEquip + slotMatch,
  `SetTalents` 用 `talent.ValidateAllocation` 做等级上限 + 前置 + 总消耗(Σ 等级 × 每级消耗);
  两条写路径在表不可用时一律 fail-closed 拒绝,不退化成放行。Compose 只读挂 `configtable/dist`;K8s 由
  `pandora-configtable` ConfigMap 整目录挂到 `/app/configtable/active`。YAML `exp_curve` 已删除。
  `start.ps1` 发布该 ConfigMap 时先冻结并校验完整候选,再以 version 单调、同版本表内容精确一致、
  `resourceVersion` CAS 和 UID 回读门禁前向切换;同版本只允许在表运行语义不变时同步
  `source_rev/generator/generated_at_ms` manifest 溯源纠正。Player Store 不接受降版,所以 rollout 失败保留
  新批次并由同版本重跑继续收敛,禁止只回滚文件造成进程内快照与挂载文件分裂。

**生成器是 protogen 式的(2026-07-21 定稿,做法对齐旧项目 tools/proto_generator/protogen)**:
proto 即单一事实源——表与列的导表元信息全部标注在 `proto/pandora/config/v1/excel.proto`
定义的自定义 option 上(`(excel_file)` 标容器 = 一张表;`(excel_data_start_row)` 可覆盖默认
第 5 行数据起点;`(excel_col)/(excel_required)/(excel_default)/(excel_prefix)` 标行字段 = 一列),
`tools/configtable-gen` 经 protoreflect
描述符自动发现全部配置表(`internal/tablegen/discover.go`),**零手写登记代码**:
- 数据侧:通用行构建器(`builder.go`)按注解做 §7 校验(表头精确对齐 / 类型 / 枚举拒 0 /
  必填 / 默认值 / 前缀 / 主键唯一),xlsx → 容器 message → dist JSON;
- 代码侧:`internal/gogen` 用独立模板文件(`template/*.tmpl`,go:embed,对应旧
  go_config.go.j2 / go_all_table.go.j2)生成 `pkg/configtable/<name>_table.gen.go`
  (视图 + All/ByID/Exists/Count/ByIDs/RandOne/Where/First)与 `tables.gen.go`
  (Tables 快照 + specByName 注册,即旧 all_table.go 的替代);
- 伴生文件 `<name>.go`(`validate<Name>Row` 业务校验钩子 + 域方法,如 level.go 的
  `IsBattleLevel`)缺失时由生成器创建一次空桩,此后归人维护不覆盖(同 protogen 的
  instance 文件模式)。
生成代码与 proto 注解的同步由 `gogen.TestGeneratedFilesUpToDate` 守护(改注解 / 模板不重跑
生成器、手改 gen 文件 → 测试红;bitindex 产物与状态 / dist 数据的一致性同样纳入守护)。

**二级索引 / 外键 / 位序(2026-07-21 补齐,移植旧项目 key / multi / fk / bit_index 四类列标记)**:
- `(excel_key)` 唯一二级键 → 生成 `By<Field>` 单行查询;生成与加载两阶段查重(旧列标记 `key`);
- `(excel_multi_key)` 非唯一索引 → 生成 `ListBy<Field>` 多行查询(旧列标记 `multi`);
- `(excel_fk) = "<目标表名>"` 外键(列类型必须 uint32,引用目标表 id;旧列标记 `fk:Table`):
  生成阶段批内引用完整性校验(§7.5,失败整批不产出)+ 加载阶段生成的 `validateCrossTables`
  fail-closed 兜底(store.go 在整批切换前调用);代码侧生成正查(`Tables.<Src><Field>Row` /
  `...RowByID`)与反查(`ListBy<Field>`)。非必填列 0 = 无引用;必填列 0 也非法。
  暂不支持 `fk:Table.column` 与 gfk(无用例,需时再加);
- `(excel_bit_index)`(容器注解)稳定「ID → 位序」映射 → 生成 `<name>_bitindex.gen.go`
  (`<Name>BitIndex(id)` / `<Name>BitCount`),供进度 / 解锁位图存储(旧 mission/reward 用途)。
  **稳定性权威 = `configtable/bitindex_state/<name>.json`(git 跟踪,严禁手改 / 丢弃,
  丢失 = 已落库位图全部错位作废;生成器对缺失状态 fail-closed,仅 `-bitindex-bootstrap`
  可显式初始化从未发布过的表)**:新 ID 追加分配,删除的 ID 保留占位永不复用。
  关卡表已启用(关卡解锁 / 通关进度位图)。
  ⚠️ **位序映射是编译期 Go map,不随 JSON 热更**:bit_index 表新增 ID 的发布必须
  「重生代码 + 发新二进制」与 JSON 批次**同步上线**(先二进制后表体),仅热更 JSON
  会让新 ID 查不到位序;发布清单必须把这一步列为硬依赖。
这四类注解的端到端测试由 `proto/pandora/configtest/v1` 夹具包承担(角色对齐旧项目
Test.xlsx / TestMultiKey.xlsx;独立包,生产 Discover 扫不到、不进 dist)。
未移植 comp(ECS 组件)模板:Go 侧无 ECS 消费方,出现真实需求再加(§15.3)。

**加新表三步**:① 写 proto(行 message 打 `(excel_col)` 等注解,容器打 `(excel_file)`)
→ ② `pwsh tools/scripts/proto_gen.ps1` 重生 pb → ③ `go run ./tools/configtable-gen
-tables <Table根> -source-rev svn-r<N>`(数据 + Go 代码 + 伴生桩一次产出;桩里按需补
业务校验与域方法。`-source-rev` 必填且拒 unknown/空白——发布产物必须可追溯到源表版本)。

