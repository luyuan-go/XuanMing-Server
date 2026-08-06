@echo off
rem ============================================================
rem  Pandora backend - intranet one-click START (k8s cluster)
rem  (double-click to run)
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and
rem  do NOT add `chcp`. cmd.exe re-reads the batch file after every line using
rem  the CURRENT console code page; start.ps1 switches the console to UTF-8,
rem  which shifts cmd's saved offset by one byte per multi-byte character and
rem  makes cmd execute fragments of comment lines (2026-08-06 bug).
rem  The Chinese documentation for this entry point lives in
rem  deploy\k8s\agones\README.md (section "intranet one-click k8s entry").
rem
rem  What it does:
rem    Brings up a real Kubernetes dev cluster on this machine via minikube
rem    (docker driver) + Agones: infra + 21 service Deployments, with the
rem    Battle DS running on a real Linux Agones Fleet.
rem
rem  Wraps:
rem    tools/scripts/start.ps1 -Mode k8s -BuildMode host
rem
rem  Read deploy\k8s\agones\README.md before the first run on a new machine:
rem  it covers the cluster topology defaults, the PANDORA_MINIKUBE_* overrides,
rem  the mutual exclusion with local mode (both bind host 8443), the required
rem  CLIs (Go / Docker Desktop / kubectl / minikube / helm), how the UE Linux
rem  DS images are located and built, and the known limits of the docker
rem  driver on an intranet.
rem
rem  Stop: double-click the intranet one-click stop (k8s) entry.
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\start.ps1" -Mode k8s -BuildMode host
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
