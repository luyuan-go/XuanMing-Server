@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; start.ps1 switches the console to UTF-8, which
rem shifts cmd's saved offset by one byte per multi-byte character and makes cmd
rem execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - planner one-click start with LIVE ASSET DS
rem  (double-click to run)
rem ------------------------------------------------------------
rem  What this does:
rem    start.ps1 -Mode local -DsLauncher editor -GenTables
rem
rem    * Tables: the planner xlsx are regenerated into the server config tables
rem      (configtable/dist) FIRST, because the Go services that read tables load
rem      that directory at process start. No need to run the table-export script
rem      by hand. The batch is all-or-nothing - if a table fails validation
rem      nothing is written, the old tables stay usable, and startup stops rather
rem      than coming up with stale tables behind your back.
rem
rem    * Backend: infra in Docker + 21 Go services as host processes.
rem      If Go is NOT installed on this machine, the prebuilt binaries under
rem      run\artifacts\windows\bin are used automatically (no Go needed).
rem    * DS: launched from the engine's UnrealEditor.exe with -server instead of
rem      the packaged PandoraServer.exe. NetMode is still NM_DedicatedServer, so
rem      login / hub / matchmaking are EXACTLY the same code path as always.
rem      The difference is that the DS reads UNCOOKED Content/ straight from the
rem      project, so saving an asset in the editor takes effect on the next
rem      hub entry / match - no packaging, no compiling.
rem
rem  Engine and project paths are auto-detected (uproject EngineAssociation ->
rem  registry), so it works no matter which drive/folder UE is installed in.
rem
rem  Note: the editor-form DS starts slowly (it loads a large set of editor
rem  modules and reads loose UNCOOKED assets; the first entry to a new map may
rem  also build mesh/texture DDC). One or two minutes on first entry to a map is
rem  normal; the backend already widens ready/heartbeat timeouts for this mode.
rem  It does NOT compile shaders: under -server the engine skips both global and
rem  material shader compilation (AllowGlobalShaderLoad / FApp::CanEverRender
rem  both test !IsRunningDedicatedServer). Shader compiling is what a listen
rem  server or PIE would do, because those actually render.
rem
rem  Daily loop: once the backend is up, you do NOT need this script again for
rem  an asset-only change. Double-click the planner one-click RESTART-DS entry
rem  instead - it restarts just the DS and leaves infra + the 21 Go services
rem  untouched, which is much faster. Come back here when the Go backend itself
rem  changed.
rem
rem  Stop: the planner one-click STOP (live-asset) entry
rem        (or: pwsh tools\scripts\start.ps1 -Mode local -Down)
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -DsLauncher editor -GenTables
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there,
rem so the misleading "Press any key" line must not be printed.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
