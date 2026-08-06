@echo off
rem !! DO NOT add `chcp` to this file !! It contains non-ASCII (Chinese) rem
rem lines; running chcp mid-file shifts cmd.exe's byte offset into the batch and
rem makes it execute comment fragments. start.ps1 already sets the console to
rem UTF-8 itself. Keep every `echo` here ASCII-only.
rem ============================================================
rem  Pandora backend - planner one-click STOP for the live-asset stack
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Pairs with the planner one-click START (live-asset) entry:
rem    start.ps1 -Mode local -Down
rem  Stops the 21 host Go services and the Docker infra started by local mode.
rem  Data volumes (MySQL/Redis etc.) are kept, so your data survives.
rem
rem  If you ever ran the retired with-battle stack on this machine, use the
rem  plain planner one-click STOP entry to clean that one up instead.
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
