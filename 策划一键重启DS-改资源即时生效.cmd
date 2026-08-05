@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend - planner one-click FAST RESTART of the local DS
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Use this one for the daily loop: you updated ONLY the client repo
rem  (assets, or a rebuilt editor DLL) and the Go backend did not change.
rem
rem  What this does:
rem    start.ps1 -Mode local -DsLauncher editor -DsOnly
rem
rem    * Docker infra, TiDB, DB migrations and the 21 Go services are left
rem      running EXACTLY as they are - none of them is stopped, rebuilt or
rem      re-migrated. That is the whole point: those steps are the slow part
rem      of a full start and none of them is affected by an asset change.
rem    * The running local DS is killed and the two allocators
rem      (hub_allocator / ds_allocator) are restarted, so the next hub entry
rem      spawns a FRESH editor-form DS that reads your updated uncooked
rem      Content/ from disk.
rem      Why the allocators have to restart too: the resident Hub DS is
rem      lazily spawned once per hub_allocator process (sync.Once), so
rem      killing the DS alone would leave nobody to spawn it again.
rem
rem  Note: any battle currently running on this machine is terminated as
rem  well (its DS is a child of ds_allocator). Local co-op debugging only,
rem  so that is fine - just re-enter.
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
