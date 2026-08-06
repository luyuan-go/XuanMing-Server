@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend - planner one-click: restart the local editor-form DS
rem  so it re-reads your latest saved assets. (double-click to run)
rem ------------------------------------------------------------
rem  Use this for the daily loop: you updated ONLY the client repo (assets, or
rem  a rebuilt editor DLL) and the Go backend did not change.
rem
rem  Wraps:
rem    start.ps1 -Mode local -DsLauncher editor -DsOnly
rem
rem  What it does NOT touch: Docker infra, TiDB, DB migrations and the 21 Go
rem  services keep running exactly as they are. None of them is affected by an
rem  asset change, and they are the slow part of a full start.
rem
rem  What it does: kills the running local DS and restarts the two allocators
rem  (hub_allocator / ds_allocator), then WAITS until the new Hub DS has loaded
rem  the level and is listening - so when this window says OK, you can log in.
rem
rem  Why the DS has to restart at all: the editor-form DS reads the uncooked
rem  Content/ at PROCESS START. The already-running one still holds the old
rem  assets in memory, so saving in the editor alone changes nothing for it.
rem  Why the allocators restart too: the resident Hub DS is spawned lazily once
rem  per hub_allocator process (sync.Once), so killing the DS on its own would
rem  leave nobody to spawn it again.
rem
rem  Typical timing on a warm machine (measured): allocators back up ~10s,
rem  Hub DS process spawned ~18s, listening ~44s. First entry to a brand-new
rem  map is slower (the DS builds mesh/texture DDC on the spot).
rem
rem  Note: any battle currently running on this machine is terminated too (its
rem  DS is a child of ds_allocator). Local debugging only, so just re-enter.
rem
rem  If the backend is NOT running yet, this script notices and automatically
rem  falls back to the full start, so you can always double-click this one.
rem
rem  Go backend changed / new run\artifacts binaries?
rem    -> use 策划一键启动-改资源即时生效.cmd instead (full start).
rem  Stop everything: 策划一键停止-改资源即时生效.cmd
rem ============================================================
setlocal
cd /d "%~dp0"

rem This project requires PowerShell 7 (pwsh). If missing, error out clearly; do
rem not fall back to Windows PowerShell 5.1.
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -DsLauncher editor -DsOnly
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there,
rem so the misleading "Press any key" line must not be printed.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
