@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; the wrapped PowerShell scripts switch the console
rem to UTF-8, which shifts cmd's saved offset by one byte per multi-byte
rem character and makes cmd execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora backend - programmer one-click PLANNER PACKAGE
rem  (double-click to run)
rem ------------------------------------------------------------
rem  Produces everything a planner machine needs so that it has to
rem  install NOTHING except PowerShell 7:
rem
rem    [1/2] run\artifacts\windows\bin\*.exe
rem          The 22 Go services + configtable-gen, cross-compiled here.
rem          run_services.ps1 uses these when the machine has no Go, so
rem          planners never install the Go toolchain and start-up drops
rem          from "compile for minutes" to "copy for seconds".
rem          Wraps: tools\scripts\build_release_binaries.ps1
rem
rem    [2/2] run\localinfra\cache\*
rem          The portable infrastructure archives (MySQL, Redis, Kafka,
rem          JRE, Envoy, mkcert) used by the no-Docker start entry.
rem          Downloading these is ~400 MB PER MACHINE, so fetch them
rem          once here and hand the folder over instead.
rem          Wraps: tools\scripts\local_infra.ps1 -Action provision
rem
rem  Who runs it:
rem    Programmers. Step 1 needs Go on this machine. Step 2 only needs
rem    network access.
rem
rem  How to hand it over:
rem    * copy run\artifacts        -> the planner's repo, same path
rem    * copy run\localinfra\cache -> any share, then on the planner
rem      machines set  PANDORA_LOCALINFRA_MIRROR=\\share\pandora-infra
rem      (a machine-level env var, or put it in the start .cmd)
rem    Both are pure data drops - no installer, no service, no admin.
rem
rem  Advanced use - any argument is forwarded to the binary build and
rem  step 2 is skipped, e.g. to rebuild one service only:
rem    this.cmd -Service matchmaker
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

set "BUILD=%~dp0tools\scripts\build_release_binaries.ps1"
set "INFRA=%~dp0tools\scripts\local_infra.ps1"

rem Explicit arguments mean the caller knows what they want - forward and stop.
if not "%~1"=="" goto :forward

echo.
echo  [1/2] Building the Windows binaries planners run without Go.
echo.
pwsh -NoProfile -ExecutionPolicy Bypass -File "%BUILD%"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :done

echo.
echo  [2/2] Fetching the portable infrastructure archives (no-Docker mode).
echo        Already-downloaded archives are kept; this is safe to re-run.
echo.
pwsh -NoProfile -ExecutionPolicy Bypass -File "%INFRA%" -Action provision
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :done

echo.
echo  Done. Hand over these two folders:
echo    %~dp0run\artifacts          -^> planner repo, same path
echo    %~dp0run\localinfra\cache   -^> a share, then set PANDORA_LOCALINFRA_MIRROR to it
echo.
goto :done

:forward
pwsh -NoProfile -ExecutionPolicy Bypass -File "%BUILD%" %*
set "RC=%ERRORLEVEL%"

:done
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
