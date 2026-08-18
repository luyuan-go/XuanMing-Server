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

rem This project requires PowerShell 7 (pwsh) and does NOT run on Windows
rem PowerShell 5.1. If the machine has no pwsh, bootstrap_pwsh.cmd unpacks the
rem official portable build under run\localinfra - no installer, no admin, no
rem change to the machine. Read that file for why it is not the .msi.
call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
if errorlevel 1 (
  rem The web admin runs this headless; pausing there would hang it forever.
  if not defined PANDORA_NONINTERACTIVE pause
  exit /b 1
)
rem Quote it: with the portable build this is a full path, which can contain spaces.
set "PS=%PANDORA_PWSH%"

"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode local -NoDocker -Down
set "RC=%ERRORLEVEL%"

if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
