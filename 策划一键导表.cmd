@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora 后端 策划一键导表(双击运行)
rem ------------------------------------------------------------
rem  干什么:
rem    把客户端 SVN 里的策划 xlsx(Pandora-Client-SVN\Table)生成为
rem    服务端配置表 configtable\dist\*.json + manifest.json。
rem
rem  什么时候用:
rem    改完策划表(道具/关卡/技能/掉落/刷怪点…)并 svn commit 之后。
rem    **不导表服务端就还是旧数据** —— 一键启动只读 dist,不会自己生成。
rem
rem  包装命令:
rem    tools\scripts\configtable_gen.ps1
rem    (自动找 Table 目录、自动取 SVN 版本号、没装 Go 也能跑)
rem
rem  导完之后:
rem    双击 策划一键启动-改资源即时生效.cmd(或已经在跑就重启后端)即可生效。
rem
rem  Table 目录不在默认位置时,先设环境变量再双击:
rem    set PANDORA_CLIENT_TABLE_ROOT=D:\work\Pandora-Client-SVN\Table
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\configtable_gen.ps1" %*
set "RC=%ERRORLEVEL%"

rem 双击运行时保留窗口；web 后台调用会设 PANDORA_NONINTERACTIVE=1，输出改在网页上看。
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
