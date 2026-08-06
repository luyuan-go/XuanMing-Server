@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click: restart the local editor-form DS
rem  so it re-reads your latest saved assets. (double-click to run)
rem ------------------------------------------------------------
rem  Use this for the daily loop: you updated ONLY the client repo (assets, or
rem  a rebuilt editor DLL) and the Go backend did not change.
rem
rem  Wraps:
rem    start.ps1 -Mode local -DsLauncher editor -GenTables -DsOnly
rem
rem  You do NOT need to stop anything first, and you do NOT need to run the
rem  table-export script by hand - both are handled here.
rem
rem  What it does NOT touch: Docker infra, TiDB and DB migrations are left alone,
rem  and so are the Go services that have nothing to do with your change. Those
rem  are the slow part of a full start.
rem
rem  What it does, in order:
rem    1. Regenerates the server config tables from the planner xlsx
rem       (configtable/dist). Planners tweak tables precisely to test them, so
rem       this must not be a separate step someone can forget. The batch is
rem       all-or-nothing: if any table fails validation nothing is written, the
rem       old tables stay usable, and the script stops instead of starting up
rem       with stale tables and letting you think your edit did not work.
rem    2. Kills the running local DS and restarts hub_allocator / ds_allocator.
rem    3. If (and only if) the table batch actually changed, also restarts the
rem       Go services that read tables - player / battle_result / matchmaker /
rem       matchmaker_pve - because they load configtable/dist at PROCESS START.
rem    4. WAITS until the new Hub DS has loaded the level and is listening, so
rem       when this window says OK you can log in right away.
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
rem    -> use the planner one-click START (live-asset) entry instead (full start).
rem  Stop everything: the planner one-click STOP (live-asset) entry
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -DsLauncher editor -GenTables -DsOnly
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there,
rem so the misleading "Press any key" line must not be printed.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
