@echo off
rem ============================================================
rem  Pandora backend - intranet one-click STOP (k8s cluster)
rem  (double-click to run)
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and
rem  do NOT add `chcp`. cmd.exe re-reads the batch file after every line using
rem  the CURRENT console code page; start.ps1 switches the console to UTF-8,
rem  which shifts cmd's saved offset by one byte per multi-byte character and
rem  makes cmd execute fragments of comment lines (2026-08-06 bug).
rem  Chinese docs: deploy\k8s\agones\README.md (intranet one-click k8s entry).
rem
rem  Deletes the k8s services and infra deployed by the intranet one-click
rem  start (k8s) entry. The minikube cluster itself keeps running; to stop it
rem  completely, run `minikube stop` by hand.
rem
rem  Persistent volumes are kept, so your data is still there next start.
rem ============================================================
setlocal
cd /d "%~dp0"

where pwsh >nul 2>nul
if errorlevel 1 (
  echo.
  echo  [ERR] PowerShell 7 pwsh not found. This script requires PowerShell 7.
  echo        Install: https://aka.ms/powershell
  echo.
  pause
  exit /b 1
)
set "PS=pwsh"

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode k8s -Down
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
