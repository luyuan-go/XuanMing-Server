# Issue tracker: GitHub

本仓库的问题与规格记录在 GitHub Issues 中。所有操作都使用 `gh` CLI。

## 约定

- **创建问题**：`gh issue create --title "..." --body "..."`。多行正文使用 heredoc。
- **读取问题**：`gh issue view <number> --comments`，使用 `jq` 筛选评论并同时获取标签。
- **列出问题**：`gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`，并按需添加 `--label` 与 `--state` 筛选。
- **评论问题**：`gh issue comment <number> --body "..."`
- **添加或移除标签**：`gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **关闭问题**：`gh issue close <number> --comment "..."`

仓库从 `git remote -v` 推断；在克隆目录内运行时，`gh` 会自动完成此操作。

## 是否把 Pull Request 作为分流入口

**PRs as a request surface: no.** _（如果本仓库把外部 PR 当作功能请求，可改为 `yes`；`/triage` 会读取此标志。）_

设为 `yes` 后，PR 使用与 Issue 相同的标签和状态，并改用对应的 `gh pr` 命令：

- **读取 PR**：`gh pr view <number> --comments`，并用 `gh pr diff <number>` 获取差异。
- **列出待分流的外部 PR**：`gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments`，只保留 `authorAssociation` 为 `CONTRIBUTOR`、`FIRST_TIME_CONTRIBUTOR` 或 `NONE` 的条目，排除 `OWNER`、`MEMBER` 与 `COLLABORATOR`。
- **评论、标记或关闭**：`gh pr comment`、`gh pr edit --add-label` / `--remove-label`、`gh pr close`。

GitHub 的 Issue 与 PR 共用编号空间，因此单独的 `#42` 可能是任意一种；先执行 `gh pr view 42`，失败后再执行 `gh issue view 42`。

## Skill 要求“发布到问题跟踪器”时

创建一个 GitHub Issue。

## Skill 要求“获取相关工单”时

执行 `gh issue view <number> --comments`。

## Wayfinding 操作

供 `/wayfinder` 使用。**地图**是一个 Issue，**子工单**也是 Issue。

- **地图**：带有 `wayfinder:map` 标签的单个 Issue，正文保存 Notes、Decisions-so-far 与 Fog。使用 `gh issue create --label wayfinder:map` 创建。
- **子工单**：通过 GitHub sub-issue API（`gh api`）链接到地图。如果仓库没有启用 sub-issues，就把子工单加入地图正文的任务列表，并在子工单开头写 `Part of #<map>`。标签使用 `wayfinder:<type>`，类型为 `research`、`prototype`、`grilling` 或 `task`。认领后把工单分配给当前开发者。
- **阻塞关系**：优先使用 GitHub 原生 Issue dependencies。通过 `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>` 添加边；`<blocker-db-id>` 必须是阻塞 Issue 的数字数据库 ID（用 `gh api repos/<owner>/<repo>/issues/<n> --jq .id` 获取），不能使用 `#number` 或 `node_id`。`issue_dependencies_summary.blocked_by` 表示当前未关闭的阻塞项。如果依赖功能不可用，则在子工单顶部写 `Blocked by: #<n>, #<n>`。全部阻塞项关闭后，工单才解除阻塞。
- **前沿查询**：列出地图的未关闭子工单，排除仍有开放阻塞项或已有 assignee 的工单，按地图顺序选择第一个。
- **认领**：`gh issue edit <n> --add-assignee @me`，这是会话的第一次写操作。
- **解决**：执行 `gh issue comment <n> --body "<answer>"`，再执行 `gh issue close <n>`，最后把上下文指针（gist 与链接）追加到地图的 Decisions-so-far。
