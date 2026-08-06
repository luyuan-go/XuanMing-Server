@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora 后端 策划一键导表(双击运行)
rem ------------------------------------------------------------
rem  干什么:
rem    把客户端 SVN 里的策划 xlsx(Pandora-Client-SVN\Table)生成为
rem    服务端配置表 configtable\dist\*.json + manifest.json。
rem
rem  什么时候用(2026-08-05 起多数情况下用不着了):
rem    两个策划一键入口(启动 / 重启DS)**已经会自己先导表**,日常改完表直接双击那两个
rem    就行,不用再点这个。
rem    本入口留给「只想导表、暂时不起服务」的场景:比如只是想验证一下自己的表能不能导过、
rem    或者要把 configtable\dist 这批产物提交给别人。
rem
rem  包装命令:
rem    tools\scripts\configtable_gen.ps1
rem    (自动找 Table 目录、自动取 SVN 版本号、没装 Go 也能跑)
rem
rem  导完之后:
rem    后端已经在跑 → 双击 策划一键重启DS-读最新资源.cmd(它会重启读表的服务让新表生效);
rem    后端没在跑   → 双击 策划一键启动-改资源即时生效.cmd。
rem    两者都会再导一次表(内容没变就是空跑,不会重复产出),所以先点这个也不冲突。
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
