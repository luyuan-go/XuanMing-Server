@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend - planner one-click STOP for the live-asset stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Pairs with 策划一键启动-改资源即时生效.cmd:
rem    start.ps1 -Mode local -Down
rem  Stops the 21 host Go services and the Docker infra started by local mode.
rem  Data volumes (MySQL/Redis etc.) are kept, so your data survives.
rem
rem  If you ever ran the retired with-battle stack on this machine, use
rem  策划一键停止.cmd to clean that one up instead.
rem ============================================================
setlocal
cd /d "%~dp0"

where pwsh >nul 2>nul
if errorlevel 1 (
  echo.
  echo  [ERR] PowerShell 7 pwsh not found. This script requires PowerShell 7.
  echo        Install: https://aka.ms/powershell  or  winget install Microsoft.PowerShell
  echo.
  pause
  exit /b 1
)
set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -Down
set "RC=%ERRORLEVEL%"

rem Interactive (double-click) runs keep the window; the web admin sets
rem PANDORA_NONINTERACTIVE=1 and shows the output on the page instead.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
