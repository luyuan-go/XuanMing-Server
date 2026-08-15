@echo off
rem ASCII-ONLY FILE - see the start entry for why (cmd.exe re-reads batch files
rem using the current console code page; non-ASCII bytes corrupt execution).
rem ============================================================
rem  Pandora backend - planner one-click STOP, NO DOCKER (TEST BUILD)
rem ------------------------------------------------------------
rem  Stops the 22 Go services and the native infrastructure processes
rem  (MySQL / Redis / Kafka / Envoy) started by the no-Docker entry.
rem  Data under run\localinfra\data is KEPT.
rem
rem  To wipe the local databases as well:
rem    pwsh tools\scripts\local_infra.ps1 -Action reset
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

pwsh -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -NoDocker -Down
set "RC=%ERRORLEVEL%"

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
