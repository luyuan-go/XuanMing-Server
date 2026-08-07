# 玩家昵称校验规范(设计,尚未实现)

> 状态:**设计稿**。2026-08-06 记录业界通行做法与本仓库现状差距,实现前不得声称已达标。
> 关联:[go-services.md §2.2 player](./go-services.md)、`CLAUDE.md` §9.6 / §9.15 / §9.22 / §9.24 / §17.3。

## 1. 现状基线(改动前必须先确认这些仍成立)

| 项 | 现状 | 位置 |
|---|---|---|
| 校验逻辑 | 仅三条:`TrimSpace` → 非空 → rune 数 ≤ `max_nickname_len`(默认 32) | `services/account/player/internal/biz/player.go` `UpdateNickname` |
| 唯一性 | MySQL `uk_nickname` 唯一键 | `deploy/mysql-init/04-player-tables.sql` |
| 列容量 | `VARCHAR(64)` utf8mb4 / `utf8mb4_0900_ai_ci` | 同上 |
| 默认昵称 | `default_nickname_prefix`(默认 `Player_`)+ player_id | `internal/conf/conf.go` |
| 错误码 | `ErrPlayerNicknameTaken = 2003`;其余走 `ErrInvalidArg` | `pkg/errcode/errcode.go` |

**已知缺口**:下述第 2~7 节除长度外全部缺失,且玩家可自取 `Player_` 前缀冒充他人默认名。

## 2. 归一化必须在校验之前

顺序不可颠倒:**NFKC 归一化 → trim → 折叠连续空格 → 校验 → 入库**。

- 用 `golang.org/x/text/unicode/norm` 做 NFKC,把全角 `Ａ` 打平成 `A`、兼容字符合并。跳过这步,
  白名单与敏感词均可被全角/兼容字符绕过。
- **校验对象与入库对象必须是同一个归一化后的串**。校验归一化串、存原始串 = 校验等于没做。

## 3. 长度:rune 上限 + 字节上限,两条都要

- 业务上限按 rune 计;若要严格防"一个字占十几个码点"的组合序列,按 grapheme cluster 计。
- **同时**必须有字节上限兜住 DB 列:`VARCHAR(64)` utf8mb4 = 最多 64 字符 / 256 字节。
  这条对应 `CLAUDE.md §9.24` 的写入侧上限纪律。
- `sql_mode` 严格时超长报错(`pkg/dbguard.AssertStrictMode` 已在启动断言);非严格模式会
  **静默截断**,昵称被无声砍断且无错误可观测。

## 4. 字符集用白名单,不用黑名单

黑名单永远漏。建议白名单:`\p{L}`(含 CJK)+ `\p{N}` + 少量符号(`_`、词间单个空格)。
必须显式拒绝的类别:

| 类别 | 拒绝理由 |
|---|---|
| `\p{Cc}` 控制字符 | 换行 / `\0` 污染日志与协议 |
| `\p{Cf}` 格式字符 | 零宽空格、ZWJ、RTL override(U+202E)可伪造显示顺序 |
| `\p{Co}` 私用区 | 各端渲染成豆腐块或他人图标,跨端不一致 |
| 连续组合符号 > N | Zalgo 文本撑爆 UI |
| 变体选择符 / 未分配码点 | 跨端渲染不一致 |
| emoji | 需策划显式决策,不能默认放行也不能默认禁止 |

## 5. 唯一性:防仿冒,不只是防重复

- 额外存一列 `nickname_normalized` 并把唯一键建在它上面:casefold + NFKC + confusables 折叠
  (`0/O`、`1/l/I`、全角半角、西里尔 `а` vs 拉丁 `a`)。原始 `nickname` 只用于展示。
- 现有 `utf8mb4_0900_ai_ci` 只做到大小写 / 重音不敏感(副作用:`José` 与 `Jose` 会撞唯一键),
  **挡不住西里尔同形字冒充**。
- **唯一性判定只能靠唯一键冲突**(捕 MySQL 1062 → `ErrPlayerNicknameTaken`)。
  "先查是否存在再写"是 `CLAUDE.md §9.22` 点名的 TOCTOU,一律拒。

## 6. 保留前缀 / 保留词

- 玩家自取昵称**必须禁止** `default_nickname_prefix`(当前 `Player_`),否则可冒充他人默认名。
  该校验必须读同一份配置,不得硬编码字面量,防止改配置后规则漂移。
- 同类保留词:`GM` / `系统` / `官方` / `客服` / `管理员`。

## 7. 敏感词是独立一层,且可热更

- 不与字符校验混在一起,单独一层,便于换词库与灰度。
- 匹配前先去掉分隔符与重复字符(否则 `傻*逼`、`傻傻逼` 可绕过)。
- 词库走 `CLAUDE.md §9.15` 的配置表热更流水线(版本号 + checksum + staging + 加载成功才切换),
  **不得硬编码进二进制**。

## 8. 权威与错误码

- 服务端是唯一判定权威(`§9.6`)。客户端可拉取同一份规则做即时灰化与提示,但
  **不得实现第二份判定逻辑**,更不得因本地判定通过而跳过服务端校验(`§17.3`)。
- 规则查询失败 = `UNKNOWN`,按 `§9.22` fail-closed,不得默认放行。
- 错误码必须分开,前端才能给准确提示:太长 / 非法字符 / 命中保留词 / 命中敏感词 / 已被占用。
  新增码走 `pkg/errcode` player 段(2xxx),按 `§9.21` 只增不改。
- 改名必须有 CD / 次数限制 + 审计流水,否则是刷屏与洗白工具;改名成功需考虑对好友、组队、
  公会等展示侧昵称的刷新(当前组队域无昵称写入方,见 `proto/pandora/team/v1/team.proto` 注释)。

## 9. 实现落点建议(未实施)

1. 新建 `pkg/namecheck`:归一化 + 白名单 + 长度双上限 + 保留前缀,纯函数无依赖,可被 player /
   guild(公会名)/ 未来的宠物名等复用。
2. player 库迁移(`tools/migrate`)加 `nickname_normalized` 列与唯一键;存量行按同一算法回填,
   冲突行进人工处理清单,**不得自动改玩家昵称**。
3. `UpdateNickname` 接 `pkg/namecheck`,1062 映射到 `ErrPlayerNicknameTaken`。
4. 敏感词层先留配置表接口(`configtable`),词库后补。
5. 按 `CLAUDE.md §14` 一次接到可上线版本,不留 TODO 占位。

## 10. 验收要求

至少覆盖:全角绕过、零宽 / RTL override、Zalgo、私用区、超长 rune 但未超字节、超字节但未超 rune、
`Player_` 前缀冒充、西里尔同形字冒充、并发同名(必须靠唯一键判定且只有一方成功)、
归一化后为空串。未覆盖前不得声称昵称校验完成。
