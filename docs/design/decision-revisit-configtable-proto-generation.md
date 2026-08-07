# decision-revisit:配置表 proto 是否改为工具生成

- 状态:**方案 B 已落地(2026-08-07)，方案 A 仍待人拍板**(2026-08-07 提出，由用户指令触发)
  - 方案 B 是**加法**:不推翻「proto 即单一事实源」旧决策，不改策划表格式 / 客户端 / 产物格式，
    故不受 `AGENTS.md §7` 的拍板门禁约束，已直接实现(见 §9 落地记录)。
  - 方案 A 会推翻旧决策且跨两仓、跨策划 / 客户端 / 服务端三方，**仍需人拍板后才能动**。
- 触发:策划把 `技能/j_技能_方位类型_圆形.xlsx` 的 D 列「圆形集合」改名为「范围内圆形集合」并新增
  E 列「范围外圆形集合」,一键导表整批失败,须由程序手工改 `skill_circle.proto` 的
  `(excel_col)` 注解并新增字段。用户判断「不应该手写,应该用工具生成这些表的 proto」,
  参照旧项目 `D:\luyuan\mmorpg\tools\data_table_exporter`。
- 影响面:`proto/pandora/config/v1/*.proto`(23 张表)、`tools/configtable-gen`、
  `pkg/configtable`、`configtable/dist`、跨仓 UE cpp pb、`docs/design/config-table-hotreload.md`。
- 规范依据:`AGENTS.md §7`(推翻既有设计决策须先写本文件并等人拍板)、`CLAUDE.md §5.4`
  (字段编号上线后不复用)、`CLAUDE.md §15`(简单标准优先)、`CLAUDE.md §9.15`(热更流水线)。

## 1. 旧问题与旧决策

2026-07-21 定稿「protogen 式生成器」(`docs/design/config-table-hotreload.md`):**proto 即单一
事实源**——表与列的导表元信息以 `excel.proto` 自定义 option 标注在**人手写的** proto 上,
`tools/configtable-gen` 用 protoreflect 自动发现全部配置表,零手写登记代码,一次产出
dist JSON + `pkg/configtable` 访问代码 + 伴生校验桩。

当时解决的问题是「表清单和列映射不要手写登记代码」,**没有解决**「proto 本身要手写」。
本次暴露的正是后者:策划每次改列名 / 加列,程序都得手改 proto,且必须记得重建
`configtable-gen.exe`(一键导表默认用预编译 exe,不重建则仍带旧描述符,报同样的错)。

## 2. 参照对象:旧项目 data_table_exporter 为什么能生成 proto

旧项目的 xlsx **自带 schema 元数据行**(`readme.md` / `core/schema.py`):

| 行 | 内容 |
|---|---|
| 1 | 列名 |
| 2 | **数据类型**(`int32` / `string` / `map<K,V>` / `repeated ...`) |
| 3 | map 角色(map_key / set) |
| 4 | owner(server / client / common) |
| 5 | multi 标记 |
| 6 | table_key / bit_index |
| 9 / 10 | **`fk:Table` / `gfk:Table` 外键** |
| 20+ | 数据 |

也就是说旧项目的「proto 由工具生成」成立的前提是:**类型、主键、索引、外键这些信息本来就写在
xlsx 里**,工具只是换个格式输出。生成器不需要发明任何信息。

## 3. 本项目的阻塞事实(实测,非推测)

Pandora 的策划 xlsx **没有任何元数据行**。以 `j_技能_方位类型_圆形.xlsx` 实测:

```
行1: A=ID | B=备注 | C=位置选项 | D=范围内圆形集合 | E=范围外圆形集合
行2: C=1:固定在自身          ← 是取值说明文本,不是类型行
行3: C=2:跟随鼠标指针
行4-6: 整行空
行7+: 数据
```

于是「工具生成 proto」缺的信息必须另找来源。逐一核查后,**四条阻塞**:

### 3.1 没有任何一处有全部列的类型

唯一像 schema 的东西是客户端列登记 `Pandora-Client-SVN/Tool/Table/Cs/Proto/*.json`
(含 name / type / colName / defaultValue / isOptCol)。但它**是服务端所需列的真子集**:

| 表 | 客户端登记列数 | 服务端 proto 字段数(= xlsx 表头列数,`checkHeaders` 位置精确对齐) |
|---|---|---|
| `role_level`(角色/j_角色等级.xlsx) | 10 | 12 |
| `role`(角色/j_角色配置表.xlsx) | 18 | 19 |
| `skill`(技能/j_技能.xlsx) | 21 | 24 |
| `skill_circle` | 4 | 5 |
| `talent`(角色/z_专精.xlsx) | **0(客户端根本没登记这张表)** | 7 |

`role_level` 缺的正是 `kill_exp`——击杀经验换算的唯一权威(`CLAUDE.md §9.6`),客户端不需要
所以没登记。`talent` 是纯服务端表,`Tool/Table/Cs/Proto/` 下没有 `z_专精.json`。
**结论:客户端登记不能当 schema 源。**

剩下的可能来源只有「从数据行猜类型」。这条已被现有 proto 注释明确否掉过:`skill` 的
`damage_rate`、`correction_rate` 等列今天全是整数、明天策划填 `1.5`——按数据猜会生成 `uint32`,
下次导表整批失败,且失败原因是工具自己猜错,不是策划填错。

### 3.2 字段编号必须稳定,而列会被插入 / 删除

`CLAUDE.md §5.4`:字段编号上线后不复用。按列序自动分配编号(旧项目做法)在「策划在中间插一列」
时会把后面所有字段编号整体后移——dist JSON 是 protojson(按字段名)可能扛得住,但
`proto/gen/cpp` 同步给 UE、以及任何按编号解释的路径会静默错位。要保稳定就得再引入一份
「列名 → 字段编号」的 append-only 状态文件(类似现有 `configtable/bitindex_state/`),
这是新增的一套必须 git 跟踪、丢了就出事的权威状态。

### 3.3 服务端决策不在任何输入里

现有 23 张表 proto 上的注解统计:`(excel_required)` 74 处、`(excel_default)` 66 处、
`(excel_prefix)` 28 处、`(excel_fk)` 8 处、`(excel_bit_index)` 1 处;另有 4 个业务 enum
(`LevelCategory` / `LevelEntryMode` / `LevelExpShareMode` / `ItemType`)。这些**没有一个**
能从 xlsx 或客户端登记推出来,而且有的地方服务端**故意与客户端不一致**:

- `role_level.kill_exp` 服务端标 `required`——留空会静默落 0,而 battle_result 对 `exp==0`
  直接跳过出箱且不打日志,新增怪物忘填经验将完全无声。宁可生成期整批失败。
- `skill_circle.circles` 服务端**不标** `required`,因为客户端登记 `defaultValue=""` 允许留空,
  标了会让同一张表在两仓一边导得出一边导不出。
- `level.team_size` 有 `MaxLevelTeamSize=50` 上限,理由是 matchmaker 按 `need=2*teamSize`
  预分配切片,热更一个超大值即可打爆(`CLAUDE.md §16.5`)。

要保留这些,生成器就得读一份「服务端注解 sidecar」——而 sidecar 要登记的正是列名 + 上述注解,
**等于把 proto 换个文件格式重写一遍**,总信息量不减,却多一套格式、多一层生成、多一个漂移面
(违反 `CLAUDE.md §15.2/15.3`)。

### 3.4 proto 注释是当前唯一的决策存档

23 个文件注释占比约 60~70%,承载的是「为什么这列是这个类型 / 为什么必填 / 上限为什么是这个数」。
生成器覆盖写会全部抹掉;要保留就得再发明「保留区」机制。

## 4. 候选方案

### 方案 A:全量生成 proto(用户原始提议,直接照搬旧项目)

前置:**必须让策划 xlsx 补类型等元数据行**(旧项目格式),两仓同步——客户端 C# 导表器
(`Tool/Table/Cs/Exe/Pandora.Excel.Json.exe`,闭源 dll)也要能容忍新增的元数据行,
否则客户端导表全挂。

- 优点:一次到位,`§3.1` 阻塞消失,列名 / 类型改动零程序介入。
- 代价:改动全部策划表格式(37+ 张 xlsx),依赖一个我们没有源码的客户端导表器行为,
  仍需解决 `§3.2` 编号状态、`§3.3` 服务端注解 sidecar、`§3.4` 注释保留。
- 判断:**这是一个跨仓产品决策,不是程序能单方面做的**。

### 方案 B:漂移同步工具 `configtable-sync`(推荐)

保留 proto 为事实源,新增一个命令做「机械那一半」:比对 xlsx 表头与 proto 的 `(excel_col)`,
输出精确差异,并可自动改写 proto 文本:

- 列改名 → 就地替换该字段的 `(excel_col)` 字符串(字段名 / 编号 / 注释全不动);
- 新增列 → 在 message 末尾追加字段,取下一个未用编号,类型由客户端登记(有则用)或
  命令行显式指定,不做数据猜测;
- 列删除 / 挪位 → 只报告不自动改(涉及 `§5.4` 编号语义,必须人看);
- 顺带修掉本次踩的坑:检测到 `configtable-gen.exe` 比 proto 产物旧就自动重建(有 Go 时)
  或明确提示,不再出现「改完 proto 还是报同样的错」。

本次这个 case 用方案 B 的操作是:跑一条命令 → 它报「D 列改名、E 列新增」→ 确认后自动改
proto → 自动重建 exe → 自动重跑导表。程序仍然要看一眼,但不用手写。

- 优点:不动策划表格式、不动客户端、不新增编号状态文件、注释与服务端决策全保留;
  增量小,失败面小。
- 代价:新增列的类型 / required / prefix 仍需人确认一次(但这正是 `§3.3` 说的服务端决策)。

### 方案 C:维持现状(纯手写)

不推荐——本次已证明这条路的日常成本和「忘了重建 exe」的坑。

## 5. 风险

| 方案 | 主要风险 |
|---|---|
| A | 客户端导表器对新元数据行的行为未知(闭源 dll),验证不通过则两仓同时卡死;策划表迁移期两仓格式不一致会产生「同一张表两种解释」 |
| A | 按列序自动编号 + 中途插列 → cpp pb 字段编号错位,属 `§5.4` 红线 |
| A | 覆盖式生成抹掉决策注释,后续 AI / 人失去「为什么」的唯一存档 |
| B | 自动改写 proto 文本若匹配错字段会改错列——须限定只替换 `(excel_col)` 字面量且改后立即跑一次导表 + `gogen.TestGeneratedFilesUpToDate` 验证 |
| B | 「改名」与「删一列 + 加一列」在位置上不可区分,须由人确认分类,不能默认当改名 |

## 6. 迁移成本

- 方案 A:策划表 37+ 张改格式 + 客户端导表器验证 + 生成器重写 + 编号状态文件 + sidecar 格式设计
  + 23 张 proto 的注释迁移。跨两仓、跨策划 / 客户端 / 服务端三方。
- 方案 B:一个新子命令(复用现有 `xlsxlite` 读表 + `tablegen.Discover` 拿描述符)+ 一段
  proto 文本改写 + 一致性验证;不动既有产物与流水线。

## 7. 验收标准

无论选哪个方案,必须满足:

1. 策划改列名后,程序侧不需要手写 proto(方案 B 允许一次确认,但不允许手打注解)。
2. 已上线字段的编号不因任何列变动而改变(`§5.4`);cpp pb 与 go pb 一致。
3. 服务端独有的 `required` / `prefix` / `fk` / `bit_index` / enum / 上限校验在迁移后一条不丢,
   并有测试证明(至少 `role_level.kill_exp` required、`level.team_size ≤ 50`、
   `item` 装备 `max_stack_size == 1` 三条不变)。
4. 全批原子语义不变:任一张表校验不过整批不产出,旧表保持可用。
5. `gogen.TestGeneratedFilesUpToDate` 与现有 configtable 测试全绿。
6. `docs/design/config-table-hotreload.md` 同步更新,不留两份互相矛盾的说法。

## 8. 待拍板

方案 B 已落地(§9)，它把日常成本降到「跑一条命令 + 看一眼」。仍留给人决的是:

**要不要再进一步上方案 A(策划表补元数据行、proto 全量生成)?**
若选 A，需一并决定:谁去改 37+ 张策划表的格式、客户端导表器(闭源 dll)能否吃下元数据行、
以及是否接受 §3.2 编号状态文件、§3.3 服务端注解 sidecar 和 §3.4 注释存档迁移的成本。
个人建议:**先跑一阵 B**，若实际运行中「人确认一次」这一步真的成为瓶颈，再评估 A。

## 9. 方案 B 落地记录(2026-08-07)

用户当时不在线，按「自主决策」授权实现了 B。落点:

| 文件 | 作用 |
|---|---|
| `tools/configtable-gen/internal/tablegen/view.go` | 向 protosync 导出最小只读视图(列头 / 字段名 / 编号 / proto 文件路径 / 下一个可用编号)，刻意不暴露描述符本身 |
| `tools/configtable-gen/internal/protosync/diff.go` | 位置对齐比对，产出 Renames / Adds / Removes / Blocked |
| `tools/configtable-gen/internal/protosync/registry.go` | 读客户端列登记，**仅用于新增列的命名 / 类型**，不当 schema 权威 |
| `tools/configtable-gen/internal/protosync/apply.go` | 类型推断 + proto 文本改写(改名就地替换、新增追加到 message 末尾) |
| `tools/configtable-gen/sync.go` | `-sync` / `-sync-write` / `-client-registry` / `-sync-col` 接线，走在生成锁之前且不产出 dist |
| `tools/scripts/configtable_sync.ps1` | 程序入口:报差异 →(`-Write`)改 proto → 重生 pb → 重建 exe → 重跑导表 |
| `tools/scripts/configtable_gen.ps1` | exe 比源码 / pb 旧时自动重建;表头改名报错不再误导到「Excel 打开了」 |

守住的红线(对应 §5 风险行):

- 改名只替换 `(excel_col)` 字面量，且要求全文件**恰好命中一次**，否则拒绝改写;
- 新名已登记在别的列 / 新增列与已登记列重名 / 改名与删列同时出现 → 整表转只报告
  (「改名」与「挪位」从表头看不出区别，猜错会让 `(excel_col)` 指向错误字段、整批数据错列);
- 字段编号取「已用编号 + reserved 上界」之后的下一个，**不回填空洞**(`§5.4`);
- 不自动写 `required` / `default`(除非客户端登记明确给了 defaultValue)/ `prefix` / `fk` /
  `bit_index` / enum，只在追加处写注释提醒人补。

验证(对应 §7 验收标准):

1. 单测 `tools/configtable-gen/internal/protosync`:纯改名 / 改名+新增(本次真实 case)/
   挪位阻塞 / 删列只报告 / 重名阻塞 / 中间空列名阻塞 / 同名注解多处拒写 / 列号边界 /
   命名转换 / 登记合并与冲突作废，全绿。
2. 端到端:把 `skill_circle.circles` 的 `(excel_col)` 改回旧列名制造一次真实漂移 →
   `configtable_sync.ps1 -Write` 自动改回 → 重生 pb → 重建 exe → 重跑导表，
   23 张表全过且**批次号未变**(v20260807001，内容完全相同)——证明改写语义等价。
3. `go test ./tools/configtable-gen/...` 与 `go test ./configtable/...`(pkg module)全绿，
   含 `gogen.TestGeneratedFilesUpToDate`。
4. 全批原子语义未动:sync 只改 `.proto`，不碰 dist / Go 代码 / 位序状态，也不与生成共用锁。

待验证(剩余风险):本次未遇到真实的「新增列」漂移，追加字段路径只有单测覆盖，
下次策划真加列时需人确认一次追加位置与注释格式是否如意。
