@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click: restart the local editor-form DS
rem  so it re-reads your latest saved assets. NO DOCKER (TEST BUILD).
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Same as the regular "restart DS" entry, except it knows the
rem  infrastructure is running as native Windows processes rather than
rem  Docker containers:
rem
rem    start.ps1 -Mode local -NoDocker -DsLauncher editor -GenTables -DsOnly
rem
rem  THIS IS THE ONE YOU RUN ALL DAY. The infrastructure (MySQL / Redis /
rem  Kafka / Envoy) is started ONCE - normally the first time you start the
rem  backend after booting the PC - and then just keeps running. Nothing
rem  here stops it or restarts it.
rem
rem  What it does, in order:
rem    1. Regenerates the server config tables from the planner xlsx.
rem    2. Kills the running local DS and restarts hub_allocator / ds_allocator.
rem    3. If (and only if) the table batch actually changed, also restarts the
rem       Go services that read tables (they load tables at PROCESS START).
rem    4. WAITS until the new Hub DS has loaded the level and is listening.
rem
rem  Why -NoDocker matters here even though this entry never touches the
rem  infrastructure: the fast path first checks that the backend is actually
rem  up. The Docker stack also runs etcd on 127.0.0.1:2380, the no-Docker
rem  stack deliberately does not. Without this flag the check would never
rem  pass on a planner machine and every run would silently fall back to the
rem  FULL start - the exact opposite of what this entry is for.
rem
rem  If the backend is NOT running yet (e.g. you just booted the PC), this
rem  script notices and automatically falls back to the full no-Docker start,
rem  so you can always double-click this one.
rem
rem  Go backend changed / new run\artifacts binaries?
rem    -> use the no-Docker one-click START entry instead (full start).
rem  Stop everything: the no-Docker one-click STOP entry
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -NoDocker -DsLauncher editor -GenTables -DsOnly
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there,
rem so the misleading "Press any key" line must not be printed.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
