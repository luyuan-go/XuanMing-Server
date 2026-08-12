# 决策复议：后端 Jenkins 权威源码切回 GitHub

> 状态：**2026-08-11 人工已拍板，实施中**。
>
> 范围：只调整 `backend-dev` 的源码与触发链；`client-dev` 继续使用 SVN，
> `artifacts-sync` 本轮仍从 SVN Server 路径读取流水线定义。

## 1. 旧决策与实际问题

2026-07-27 曾把后端团队权威源改为 SVN `^/trunk/Server`，并让 `backend-dev`
轮询该路径。当前实际开发与提交已经回到公开仓库
`https://github.com/luyuan-go/XuanMing-Server.git` 的 `main`；开发者推送 Git commit 后，
Jenkins 仍只轮询 SVN，因此持续报告 `No changes`，不会启动 Go 构建。

这也与发布线原有契约“客户端 SVN + 后端 git、后端 snapshot 使用 `g<sha>`”冲突。

## 2. 新决策

- `backend-dev` 使用 Jenkins GitSCM 跟踪公开仓库 `main`，无需 SCM 凭据。
- 保留 `H/5 * * * *` 的 `pollSCM` 自动触发；迁移后先手动构建一次建立 Git baseline。
- 后端离线镜像包以 clean Git commit 命名，`build-info.json` 使用 `vcs=git`、
  `source_rev=g<12位sha>`、`dirty=false`。
- `client-dev` 继续以 SVN `^/trunk/Client` 为准，客户端和 DS 版本仍为 `r<rev>`。
- `artifacts-sync` 的 SCM 不在本轮迁移范围，仍保留 `SVN_SERVER_URL`；它只负责分发已发布制品，
  不决定 Go 构建使用哪份源码。

## 3. 风险与代价

- Jenkins controller 与 Windows agent 必须能通过 HTTPS 访问 GitHub；断网时轮询/检出会失败。
- CI 只需要当前 `main` 快照，因此 checkout 使用单分支、depth 1、no-tags；避免约 472 MiB
  全历史传输在不稳定 HTTPS 链路上出现 `curl 56` / `early EOF`，release tag 构建不复用此 job。
- 首次切换时旧 `backend-dev` 工作区是损坏且锁定的 SVN working copy，必须只清理
  `F:\jenkins-agent\workspace\backend-dev` 后再进行 Git checkout。
- GitHub `main` 与旧 SVN Server 不再共享一个 revision；跨端发布应分别记录后端 `g<sha>`
  与客户端/DS `r<rev>`，不能再假设单一 SVN revision 同时定位两端。
- 回滚时可把 live job 和模板恢复为 SubversionSCM；已有制品目录不可覆盖，只能发布新版本。

## 4. 迁移步骤

1. 更新 `backend-dev.xml`、`.env.example` 与 `setup-jenkins.ps1`，并对 Git 插件 fail-closed 检查。
2. 用单 job Jenkins CLI 更新 live `backend-dev`，确认 GitSCM URL、`*/main`、无凭据。
3. 精确清理旧 backend job 工作区，手动触发首轮构建。
4. 验证 Checkout、Build & Test、Publish Offline Images 三阶段及新制品 provenance。
5. 后续推送用 `pollSCM` 自动触发；另行决策是否把 `artifacts-sync` 也迁到 Git。

## 5. 验收标准

- live `backend-dev` 不再包含 `SubversionSCM`，Git remote 为上述 GitHub URL，branch 为 `*/main`。
- 首轮构建 checkout 的 commit 等于触发时远端 `main`。
- `ci_backend.ps1` 全模块 build/test 成功，不能把 SKIP 当 PASS。
- `Publish Offline Images` 成功，`latest.json` 指向新的 `g<sha>` 目录。
- tar 的 SHA-256 与 `sha256sums.txt` 一致，`images-manifest.json` 包含预期业务镜像。

## 6. 2026-08-11 迁移实测

- live job 已改为 GitSCM `main` 并启用 depth-1 / no-tags / main-only refspec；旧 SVN 工作区已备份。
- `#15` 全历史 clone 因 GitHub HTTPS `curl 56` / `early EOF` 失败；`#16` 已采用浅克隆，
  但同一外部链路再次瞬断。Windows agent 上独立浅克隆成功，Git 元数据仅约 8.9 MiB，
  以该 clean shallow repo 建立首次 baseline 后，`#17` 的两次 checkout 均成功并锁定
  `e0a107867af11821fdff576f550a2d99d17ba96a`。
- `#17` 的 33 个 Go module 全部 build/test 通过；6 个 PowerShell 契约测试通过，
  `gen_cluster_prod_account_contract_test.ps1` 因 Windows Jenkins OEM code page 把子 `pwsh`
  的中文错误文本转成 `?` 而假失败，镜像发布按门禁正确跳过。
- 本地修复已把该测试改为匹配稳定 ASCII 错误码，并在普通终端与显式 `chcp 437`
  两种环境下通过。该修复进入 GitHub `main` 后，需由下一次 Jenkins 构建完成最终制品验收。
