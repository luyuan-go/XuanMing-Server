@echo off
chcp 65001 >nul
rem ============================================================
rem  Pandora backend - planner one-click start with LIVE ASSET DS
rem  (double-click to run)
rem ------------------------------------------------------------
rem  What this does:
rem    start.ps1 -Mode local -DsLauncher editor
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
rem  Stop: 策划一键停止.cmd  (or: pwsh tools\scripts\start.ps1 -Mode local -Down)
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -DsLauncher editor
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there,
rem so the misleading "Press any key" line must not be printed.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
