# 敏感词库上游快照

`source/` 下是**逐字节固化的上游快照**,不是本仓原创内容。改词请走 `source/local/`,
不要直接改 `source/houbb/` 或 `source/konsheng/` —— 那两份必须与上游 commit 保持一致,
否则下次同步快照时冲突且无法核对。

## 来源与许可

| 目录 | 上游 | commit | 许可 | 内容 |
|---|---|---|---|---|
| `source/houbb/` | [houbb/sensitive-word-data](https://github.com/houbb/sensitive-word-data) | `fe6fc292` | Apache-2.0 | `sensitive_word_dict.txt` 主词典 + `sensitive_word_allow.txt` 白名单 |
| `source/konsheng/` | [konsheng/Sensitive-lexicon](https://github.com/konsheng/Sensitive-lexicon) | `5a8da94c` | MIT | `Vocabulary/*.txt` 17 个分类词表 |
| `source/local/` | 本仓 | — | — | `deny.txt` 自有屏蔽词 / `allow.txt` 误杀白名单 |

许可证正文随快照一并保留(`source/houbb/LICENSE.txt`、`source/konsheng/LICENSE`)。

**为什么是这两个**:`houbb/sensitive-word` 是 GitHub 上收藏最多的敏感词框架(5,985⭐),但它是
Java,Go 后端要用只能起独立服务(见 `docs/design/player-name-validation.md`);其词库已被作者
拆到 `sensitive-word-data` 独立仓库,可以单独取用。`konsheng/Sensitive-lexicon` 是收藏最多且
仍在维护的纯词库(3,963⭐),且按分类分文件,分类信息白送。匹配引擎在 `pkg/namecheck` 自研。

## 产出

```bash
go run ./tools/lexicon-import -source-rev "houbb@fe6fc292 konsheng@5a8da94c"
```

产物 `dist/{lexicon.json,manifest.json}`,热更纪律同 `CLAUDE.md §9.15`(version 单调 +
sha256 + 加载成功才切换 + 失败保留旧批次)。**不走 configtable-gen**:那条流水线是 xlsx
驱动、每行要求 uint32 主键,而词库是十万量级的上游快照、无稳定主键,塞进策划「一键导表」
既拖慢导表又要人工维持词条 id。

## 当前批次实测(source_rev `houbb@fe6fc292 konsheng@5a8da94c`)

| 项 | 值 |
|---|---|
| 上游原始行 | 151,463 |
| 去重后 | **106,759**(重复 43,733 / 短于 2 rune 969 / 白名单 2) |
| 索引 | 463,491 节点 / 463,490 边 |
| 常驻堆 | **13.2 MB** |
| 加载耗时 | ~211 ms(启动一次) |
| 昵称校验 | ~1.6 µs/次 |
| 聊天打星 | ~3.5 µs/条 |

两库重叠 43,733 条(约 29%),证明合并去重是必要的而不是形式。
`konsheng/非法网址` 去重后为 0:实测它 14,594 条里 14,588 条已被 `零时-Tencent` 完全包含。
空分类**仍然登记**,否则匹配档引用不到会加载失败,也会掩盖"本轮全是重复"这个事实。

## ⚠️ 两个匹配档,不能混用

拿 135 个**正常游戏用词**(职业 / 装备 / 称号 / 常见人名)探测全量词库,误杀 14 条(10.4%),
含「希望」「头盔」「盾牌」「匕首」「女王」「骑士」。按分类拆开,误杀 **100%** 来自四个
大而全的通用审核表:

| 分类 | 词条 | 误杀 |
|---|---|---|
| `houbb/dict` | 56,175 | 6 |
| `konsheng/网易前端过滤敏感词库` | 7,322 | 4 |
| `konsheng/GFW补充词库` | 6,157 | 3 |
| `konsheng/零时-Tencent` | 33,836 | 2 |
| 其余 13 个精编定向分类合计 | 3,269 | **0** |

原因是前四者本质是电商与通用内容审核词表,收了大量"语境敏感但本身正常"的词。因此:

- **昵称(拒绝语义)** → `namecheck.RejectProfile()`,只用精编分类(3,269 条,实测 0 误杀)。
  误拒会直接挡住玩家创角,精确率优先。
- **聊天(打星语义)** → **当前也用 `RejectProfile()`**。

聊天用全量档看似"误打星只是难看",但那 14 个误杀词里有「头盔」「盾牌」「匕首」「骑士」
「公主」—— 全是玩家在游戏聊天里天天说的词。把它们打成星号,聊天功能等于坏了。
**四个通用审核表在做完误杀治理之前,不具备直接上线的条件**,这是本次接入最重要的结论:
GitHub 上收藏最高的词库,拿来即用会毁掉功能,不是补几条白名单能解决的量级。

全量档保留在产物里、随时可用,但启用前必须:①按分类逐个跑误杀探测;②用真实聊天语料
(而不是 135 词样本)量化误杀率;③把治理结果沉淀进 `source/local/allow.txt`。
在此之前 `LoadLexicon()`(全量)只应用于离线分析,不接线上路径。

匹配档对分类漂移 **fail-closed**:上游增删词库文件时,`RejectProfile()` 的 Include/Exclude
不再与产物闭合,**导入器直接失败**并提示归档。静默排除会让昵称档漏放违禁词,静默纳入会让
聊天档突然大面积误杀,两种漂移都不会自己暴露。

## 换上游快照的流程

1. 下载新快照覆盖 `source/<上游>/`(保持与上游逐字节一致)。
2. 更新本文件的 commit 与许可表。
3. `go run ./tools/lexicon-import -source-rev "houbb@<新> konsheng@<新>"`。
   分类有增删会在这一步失败 —— 按提示改 `namecheck.RejectProfile()`。
4. `go test ./pkg/namecheck/ -run TestRealLexicon` —— 误杀探测必须仍为 0。
5. 误杀探测红了:把正常词加进 `source/local/allow.txt`,回到第 3 步。
