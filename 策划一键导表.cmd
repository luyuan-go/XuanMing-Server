@echo off
rem ============================================================
rem  Pandora backend - planner one-click CONFIG TABLE EXPORT
rem  (double-click to run)
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and
rem  do NOT add `chcp`. cmd.exe re-reads the batch file after every line using
rem  the CURRENT console code page; the wrapped PowerShell script switches the
rem  console to UTF-8, which shifts cmd's saved offset by one byte per
rem  multi-byte character and makes cmd execute fragments of comment lines.
rem  That is the bug fixed on 2026-08-06: double-clicking this file printed a
rem  pile of "is not recognized" errors and never ran the exporter at all.
rem  The Chinese documentation for this entry point lives in the header of the
rem  wrapped script: tools\scripts\configtable_gen.ps1.
rem
rem  What it does:
rem    Turns the planner xlsx from the client SVN (Pandora-Client-SVN\Table)
rem    into the server config tables configtable\dist\*.json + manifest.json.
rem
rem  When you need it (rarely, since 2026-08-05):
rem    Both planner one-click entries (start / restart-DS) export the tables
rem    themselves, so after editing a table just double-click one of those.
rem    Use this one only when you want the export WITHOUT starting anything -
rem    e.g. to check that your tables pass validation, or to hand the
rem    configtable\dist batch over to someone else.
rem
rem  Wraps:
rem    tools\scripts\configtable_gen.ps1
rem    (finds the Table dir, reads the SVN revision, works without Go installed)
rem
rem  After exporting:
rem    backend already running -> double-click the planner restart-DS entry
rem    backend not running     -> double-click the planner start entry
rem    Both re-export the tables (a no-op when nothing changed), so running
rem    this one first does not conflict with them.
rem
rem  If the Table dir is not in the default place, set this before running:
rem    set PANDORA_CLIENT_TABLE_ROOT=D:\work\Pandora-Client-SVN\Table
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

%PS% -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\scripts\configtable_gen.ps1" %*
set "RC=%ERRORLEVEL%"

rem Keep the window open only for interactive (double-click) runs. The web admin
rem runs this headless with PANDORA_NONINTERACTIVE=1 and shows the output there.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
