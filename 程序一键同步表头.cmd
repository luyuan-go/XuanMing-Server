@echo off
rem ============================================================
rem  Pandora backend - programmer one-click TABLE HEADER SYNC
rem  double-click to run
rem ------------------------------------------------------------
rem  ASCII-ONLY FILE - do NOT put Chinese or any non-ASCII text in here, and
rem  do NOT add `chcp`. cmd.exe re-reads the batch file after every line using
rem  the CURRENT console code page; the wrapped PowerShell script switches the
rem  console to UTF-8, which shifts cmd's saved offset by one byte per
rem  multi-byte character and makes cmd execute fragments of comment lines.
rem  Same 2026-08-06 fix note as the planner export entry point.
rem  The Chinese documentation for this entry point lives in the header of the
rem  wrapped script: tools\scripts\configtable_sync.ps1.
rem
rem  What it does:
rem    The planner export fails with a "header column X: expected A, got B" or
rem    an "unregistered header column X" error when the planner renamed or
rem    appended a column in the xlsx while the server .proto still carries the
rem    old excel_col annotation. This entry point fixes exactly that, in two
rem    passes:
rem      pass 1 - report the drift only, nothing is written;
rem      pass 2 - after you confirm, rewrite the .proto, regenerate the pb,
rem               rebuild configtable-gen.exe and re-run the export.
rem    Rename and append-at-end are handled automatically. Deleting a column,
rem    moving one, or a duplicate name is only reported - those need a human.
rem
rem  Who runs it:
rem    Programmers. It rewrites .proto, so it needs go + buf on this machine.
rem    Planner machines have neither - there, hand the export error over to a
rem    programmer instead.
rem
rem  Wraps:
rem    tools\scripts\configtable_sync.ps1
rem
rem  Advanced use - any argument you pass is forwarded as-is and the two-pass
rem  prompt is skipped, e.g. to name a newly added column yourself:
rem    this.cmd -Write -SyncCol "skill.<column name>=damage_display:uint32"
rem
rem  If the Table dir is not in the default place, set this before running:
rem    set PANDORA_CLIENT_TABLE_ROOT=D:\work\Pandora-Client-SVN\Table
rem ============================================================
setlocal
cd /d "%~dp0"

rem This project requires PowerShell 7 pwsh. If missing, error out clearly; do
rem not fall back to Windows PowerShell 5.1.
where pwsh >nul 2>nul
if errorlevel 1 goto :nopwsh
set "PS=pwsh"
set "SYNC=%~dp0tools\scripts\configtable_sync.ps1"

rem Explicit arguments mean the caller knows what they want - forward and stop.
if not "%~1"=="" goto :forward

rem Pass 1: report only. Exit code 0 means the .proto already matches the xlsx.
echo.
echo  [1/2] Reporting header drift. Nothing is written in this pass.
echo.
%PS% -NoProfile -ExecutionPolicy Bypass -File "%SYNC%"
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" goto :done

rem Non-interactive callers - web admin, CI - stop at the report.
if defined PANDORA_NONINTERACTIVE goto :done

echo.
echo  [2/2] Apply the changes listed above?
echo        Yes  = rewrite .proto, regenerate pb, rebuild the exporter, re-run the export.
echo        No   = nothing has been written so far; run again after a manual fix.
echo        Whatever was reported as needing a human is not auto-fixed either way.
echo.
set "ANS="
set /p "ANS=Type Y then Enter to apply, anything else to quit: "
if /i not "%ANS%"=="Y" goto :quit

echo.
%PS% -NoProfile -ExecutionPolicy Bypass -File "%SYNC%" -Write
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto :done

echo.
echo  [OK] Header sync done. Review the .proto diff before committing:
echo       a new field carries no excel_required / excel_prefix / excel_fk, and a
echo       col_^<n^> field name is a placeholder that still needs a real name.
echo       Tag the commit message with [proto] - see CLAUDE.md section 4.
goto :done

:forward
%PS% -NoProfile -ExecutionPolicy Bypass -File "%SYNC%" %*
set "RC=%ERRORLEVEL%"
goto :done

:quit
echo.
echo  [..] Quit. No file was changed.
set "RC=1"
goto :done

:nopwsh
echo.
echo  [ERR] PowerShell 7 pwsh not found. This script requires PowerShell 7.
echo        Install: https://aka.ms/powershell  or  winget install Microsoft.PowerShell
echo.
pause
exit /b 1

:done
rem Keep the window open only for interactive double-click runs.
if not defined PANDORA_NONINTERACTIVE pause
exit /b %RC%
