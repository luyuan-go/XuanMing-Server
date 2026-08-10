# Domain Docs

工程 skills 探索代码库时，按本文约定读取本仓库的领域文档。

## 探索前读取

- 仓库根目录的 **`CONTEXT.md`**；或者
- 如果根目录存在 **`CONTEXT-MAP.md`**，由它指向各上下文的 `CONTEXT.md`，只读取与当前任务有关的上下文；
- **`docs/adr/`** 中与当前工作区域有关的 ADR。多上下文仓库还要检查 `src/<context>/docs/adr/` 下的上下文级决策。

如果这些文件尚不存在，**静默继续**，不要报告缺失，也不要预先建议创建。`/domain-modeling` skill（可由 `/grill-with-docs` 和 `/improve-codebase-architecture` 进入）会在术语或决策真正确定后按需创建。

## 文件结构

单上下文仓库（大多数仓库）：

```
/
├── CONTEXT.md
├── docs/adr/
│   ├── 0001-event-sourced-orders.md
│   └── 0002-postgres-for-write-model.md
└── src/
```

多上下文仓库（根目录存在 `CONTEXT-MAP.md`）：

```
/
├── CONTEXT-MAP.md
├── docs/adr/                          ← 系统级决策
└── src/
    ├── ordering/
    │   ├── CONTEXT.md
    │   └── docs/adr/                  ← 上下文级决策
    └── billing/
        ├── CONTEXT.md
        └── docs/adr/
```

## 使用词汇表中的术语

输出中出现领域概念时（例如 Issue 标题、重构建议、假设或测试名称），使用 `CONTEXT.md` 定义的术语，不要改用词汇表明确排除的同义词。

如果词汇表还没有需要的概念，应先判断这是项目并未使用的自造术语，还是确实存在的领域缺口；真实缺口交给 `/domain-modeling` 处理。

## 标出 ADR 冲突

如果输出与现有 ADR 冲突，必须显式说明，不能静默覆盖：

> _与 ADR-0007（事件溯源订单）冲突，但由于……值得重新讨论。_
