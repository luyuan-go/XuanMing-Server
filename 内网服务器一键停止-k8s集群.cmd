@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora 后端 内网服务器一键停止(k8s 集群)
rem  双击运行
rem ------------------------------------------------------------
rem  删除 内网服务器一键启动-k8s集群.cmd 部署的 k8s 业务服务和基础设施。
rem  minikube 集群本身保持运行;如需完全停止,人工执行:
rem    minikube stop
rem
rem  持久卷默认保留,下次启动时数据仍在。
rem ============================================================
setlocal
cd /d "%~dp0"

where pwsh >nul 2>nul
if errorlevel 1 (
  echo.
  echo  [ERR] 未找到 PowerShell 7 pwsh。本项目脚本要求 PowerShell 7。
  echo        安装地址: https://aka.ms/powershell
  echo.
  pause
  exit /b 1
)
set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode k8s -Down
set "RC=%ERRORLEVEL%"

rem 双击运行时保留窗口；web 后台调用会设 PANDORA_NONINTERACTIVE=1，输出改在网页上看。
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
