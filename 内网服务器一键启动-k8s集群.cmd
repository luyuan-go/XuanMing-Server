@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora 后端 内网服务器一键启动(k8s 集群)
rem  双击运行
rem ------------------------------------------------------------
rem  在本机通过 minikube(docker driver 起节点) + Agones 启动真实 Kubernetes
rem  开发集群:基础设施 + 21 个业务 Deployment 运行,Battle DS
rem  走真实 Linux Agones Fleet。
rem
rem  包装命令:
rem    tools/scripts/start.ps1 -Mode k8s -BuildMode host
rem
rem  【首次在一台新机器上创建集群时的拓扑(2026-07-28 起,已是脚本默认值,
rem   不需要导任何环境变量;已存在的 profile 不受影响,拓扑只在创建时生效)】
rem    - 容器运行时 containerd(与线上同构;节点内不再用 Docker 跑 Pod)
rem    - CNI calico(可强制 NetworkPolicy)、K8s 版本钉 v1.35.1
rem    - 节点底图走阿里云镜像(墙内可达;gcr.io 会卡在 Pulling base image)
rem    - 发布宿主 127.0.0.1:8443 → 节点 NodePort 31443,即集群内边缘 Envoy
rem      的客户端入口(客户端仍连 127.0.0.1:8443,不再需要 21 条 port-forward)
rem    - 节点规格随本机自适应:内存 min(宿主上限x0.85, 40G)、CPU min(逻辑核, 16)。
rem      **内存低于 16G 会直接报错退出**——battle DS 单副本 limits 就是 14Gi,
rem      内存不够时 DS 永远调度不上,与其卡到超时不如立刻讲清楚。
rem    - 逐项覆盖用 PANDORA_MINIKUBE_DRIVER/CPUS/MEMORY/RUNTIME/CNI/K8S_VERSION/
rem      BASE_IMAGE/IMAGE_REPOSITORY/PORTS(创建后再改需 -Reset 重建才生效)。
rem    - 内网其它机器要连本机业务面时:先设 PANDORA_MINIKUBE_PORTS=0.0.0.0:8443:31443
rem      再重建集群(默认只发布到回环,别的机器连不到 8443)。
rem
rem  前置要求:Go / Docker Desktop / kubectl / minikube / helm 已安装且可用;
rem  客户端面 TLS 证书(deploy/envoy/cert.pem+key.pem)不入库,缺失时脚本会调
rem  tools/scripts/envoy_cert.ps1 自动重签,该步骤需要 mkcert 可用。
rem  本脚本默认不安装工具,避免未经授权修改本机环境。若确需安装缺失 CLI,
rem  请人工确认后在 PowerShell 7 中显式运行:
rem    pwsh tools/scripts/start.ps1 -Mode k8s -BuildMode host -Install
rem
rem  镜像构建使用宿主 Go 交叉编译 linux/amd64 静态二进制,再封装成
rem  pandora/<svc>:dev scratch 镜像并加载到 minikube。该路径不是离线
rem  docker load 导入包路径。
rem
rem  UE 战斗/大厅 DS(Linux)镜像会自动构建:脚本从【同级客户端仓库】的
rem  Packages\Server_Linux_Development\LinuxServer 取 UE Linux 打包产物
rem  (不写死路径,优先匹配同级 Pandora-Client* 仓库),同步进 deploy/ds/stage
rem  后构建 pandora/battle-ds:dev / pandora/hub-ds:dev 到 minikube。
rem  DS 起来后看 UE 日志: kubectl get gameservers; kubectl logs -f <pod>。
rem  想手动指定 DS 包路径,先设环境变量 PANDORA_DS_LINUX_PKG 再双击本脚本。
rem
rem  注意:minikube docker driver 下 Pod IP 默认不能被其它内网机器直接访问;
rem  本入口用于在这台机器上验证真实 k8s + Agones DS 链路。面向内网/生产
rem  客户端的公开集群仍走 online 模式,由人负责 k8s 侧操作。
rem
rem  停止:双击 内网服务器一键停止-k8s集群.cmd
rem ============================================================
setlocal
cd /d "%~dp0"

rem 本项目要求 PowerShell 7(pwsh)。若缺失则明确报错,不回退到 Windows PowerShell 5.1。
where pwsh >nul 2>nul
if errorlevel 1 (
  echo.
  echo  [ERR] 未找到 PowerShell 7 pwsh。本项目脚本要求 PowerShell 7。
  echo        安装地址: https://aka.ms/powershell 或 winget install Microsoft.PowerShell
  echo.
  pause
  exit /b 1
)
set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode k8s -BuildMode host
set "RC=%ERRORLEVEL%"

pause
exit /b %RC%
