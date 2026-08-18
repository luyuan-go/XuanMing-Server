@echo off
rem ASCII-ONLY FILE - do NOT put Chinese (or any non-ASCII) text in here, and do
rem NOT add `chcp`. cmd.exe re-reads the batch file after every line using the
rem CURRENT console code page; a multi-byte character shifts cmd's saved offset
rem and makes cmd execute fragments of comment lines (2026-08-06 bug).
rem ============================================================
rem  Pandora - make a PowerShell 7 interpreter available WITHOUT installing
rem  anything into Windows.
rem ------------------------------------------------------------
rem  Usage from a one-click entry, after its own `setlocal`:
rem
rem      call "%~dp0tools\scripts\bootstrap_pwsh.cmd"
rem      if errorlevel 1 ( ... bail out ... )
rem      "%PANDORA_PWSH%" -NoProfile -ExecutionPolicy Bypass -File ...
rem
rem  On success PANDORA_PWSH holds the interpreter to run - either the bare name
rem  `pwsh`, or the full path of the portable copy. In the portable case the
rem  folder is also prepended to the PATH of THIS process tree, because several
rem  of the project's .ps1 files re-enter themselves with a bare `& pwsh` and
rem  would otherwise die half-way through. The machine PATH is never touched.
rem
rem  Resolution order:
rem    1. `pwsh` already on PATH             -> use it, download nothing.
rem    2. run\localinfra\dist\pwsh\pwsh.exe  -> unpacked by an earlier run.
rem    3. Official portable ZIP -> cache -> verify SHA256 -> unpack into (2).
rem       Sources for (3), in order: run\localinfra\cache, the offline share
rem       %PANDORA_LOCALINFRA_MIRROR%, then github.com. Same three paths, same
rem       pinned-hash rule that local_infra.ps1 already applies to MySQL /
rem       Redis / Kafka / JRE / Envoy / mkcert.
rem
rem  Why the portable ZIP and not the .msi: the .msi is a per-machine install -
rem  it needs local admin, raises UAC, and would hang the headless web-admin
rem  runs (PANDORA_NONINTERACTIVE). The ZIP is the same official build: no
rem  admin, no registry, no service, no machine PATH change, and uninstall is
rem  "delete run\localinfra". Anyone who prefers a real install can still do it
rem  the normal way - step 1 then wins and nothing in here ever runs.
rem
rem  We deliberately do NOT fall back to Windows PowerShell 5.1. The project's
rem  scripts do not run on it, and a 5.1 fallback would turn a clear failure
rem  here into a confusing one several minutes later.
rem
rem  Pinned version / SHA256 / URL live in lib\pwsh_bootstrap.pin, shared with
rem  local_infra.ps1 so that `-Action provision` pre-stages the very same file.
rem ============================================================

set "PANDORA_PWSH="

rem ---- 1. already installed -------------------------------------------------
where pwsh >nul 2>nul
if not errorlevel 1 (
  set "PANDORA_PWSH=pwsh"
  goto :eof
)

rem %%~fI collapses the ..\.. so PATH and the messages stay readable.
for %%I in ("%~dp0..\..") do set "_PB_ROOT=%%~fI"
set "_PB_CACHE=%_PB_ROOT%\run\localinfra\cache"
set "_PB_DIST=%_PB_ROOT%\run\localinfra\dist\pwsh"

rem ---- 2. portable copy from an earlier run ---------------------------------
if exist "%_PB_DIST%\pwsh.exe" goto :use_portable

rem ---- 3. fetch, verify, unpack ---------------------------------------------
set "_PB_PIN=%~dp0lib\pwsh_bootstrap.pin"
if not exist "%_PB_PIN%" goto :err_pin
for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%_PB_PIN%") do set "%%A=%%B"
if not defined PWSH_FILE goto :err_pin
if not defined PWSH_SHA256 goto :err_pin
if not defined PWSH_URL goto :err_pin

rem The whole no-Docker stack (MySQL / Redis / Kafka / JRE / Envoy) is x64-only,
rem so there is nothing to gain from unpacking an arm64 / x86 pwsh here.
set "_PB_ARCH=%PROCESSOR_ARCHITECTURE%"
if defined PROCESSOR_ARCHITEW6432 set "_PB_ARCH=%PROCESSOR_ARCHITEW6432%"
if /i not "%_PB_ARCH%"=="AMD64" goto :err_arch

rem tar.exe and certutil.exe ship with Windows 10 1803+ / 11.
where tar.exe >nul 2>nul
if errorlevel 1 goto :err_tools
where certutil.exe >nul 2>nul
if errorlevel 1 goto :err_tools

if not exist "%_PB_CACHE%" mkdir "%_PB_CACHE%" >nul 2>nul
set "_PB_ZIP=%_PB_CACHE%\%PWSH_FILE%"

set "_PB_SRC=cache"
if exist "%_PB_ZIP%" goto :verify

rem Offline share. A file that is there but does not match the pinned hash is a
rem hard stop, NOT a silent fall-back to the internet: a wrong file on the share
rem is something a human has to look at, and quietly routing 100 machines around
rem it would hide the problem and defeat the point of having the share.
if not defined PANDORA_LOCALINFRA_MIRROR goto :fetch_net
if not exist "%PANDORA_LOCALINFRA_MIRROR%\%PWSH_FILE%" goto :fetch_net
set "_PB_SRC=mirror"
echo   [pwsh] copying %PWSH_FILE% from %PANDORA_LOCALINFRA_MIRROR%
copy /y "%PANDORA_LOCALINFRA_MIRROR%\%PWSH_FILE%" "%_PB_ZIP%" >nul
if errorlevel 1 goto :err_copy
goto :verify

:fetch_net
set "_PB_SRC=net"
where curl.exe >nul 2>nul
if errorlevel 1 goto :err_tools
echo.
echo   [pwsh] PowerShell 7 is not installed on this machine.
echo   [pwsh] Fetching the official portable build once (about 100 MB):
echo   [pwsh]   %PWSH_URL%
echo   [pwsh] Nothing gets installed into Windows - it is unpacked into
echo   [pwsh]   run\localinfra\dist\pwsh   (delete that folder to undo).
echo   [pwsh] No network here? Ask the backend team for the offline share and
echo   [pwsh] set PANDORA_LOCALINFRA_MIRROR to it.
echo.
rem Download to .part first: an interrupted download must never be left behind
rem under the real name, or the next run would treat half a file as the cache.
curl.exe -fSL --retry 3 --retry-delay 2 -o "%_PB_ZIP%.part" "%PWSH_URL%"
if errorlevel 1 goto :err_download
move /y "%_PB_ZIP%.part" "%_PB_ZIP%" >nul
if errorlevel 1 goto :err_download

:verify
rem Verify every source the same way - cache and the share are ordinary writable
rem folders with no HTTPS protecting them, and this archive gets EXECUTED.
rem findstr picks the pure-hex line so the header/footer lines cannot shift it.
set "_PB_GOT="
for /f "delims=" %%H in ('certutil -hashfile "%_PB_ZIP%" SHA256 ^| findstr /r /i "^[0-9a-f][0-9a-f]*$"') do if not defined _PB_GOT set "_PB_GOT=%%H"
set "_PB_GOT=%_PB_GOT: =%"
if /i not "%_PB_GOT%"=="%PWSH_SHA256%" goto :err_hash

rem Unpack into .tmp and rename, so a half-unpacked folder is never mistaken for
rem a good one by the next run.
if exist "%_PB_DIST%.tmp" rd /s /q "%_PB_DIST%.tmp"
if exist "%_PB_DIST%" rd /s /q "%_PB_DIST%"
mkdir "%_PB_DIST%.tmp" >nul 2>nul
if errorlevel 1 goto :err_unpack
echo   [pwsh] unpacking PowerShell %PWSH_VERSION% into run\localinfra\dist\pwsh
tar.exe -x -f "%_PB_ZIP%" -C "%_PB_DIST%.tmp"
if errorlevel 1 goto :err_unpack
if not exist "%_PB_DIST%.tmp\pwsh.exe" goto :err_layout
move "%_PB_DIST%.tmp" "%_PB_DIST%" >nul
if errorlevel 1 goto :err_unpack
echo   [pwsh] ready.

:use_portable
set "PANDORA_PWSH=%_PB_DIST%\pwsh.exe"
set "PATH=%_PB_DIST%;%PATH%"
goto :done

rem ---- failures -------------------------------------------------------------
:err_pin
echo.
echo  [ERR] missing or unreadable %_PB_PIN%
echo        The working copy is incomplete - update the repository.
goto :fail

:err_arch
echo.
echo  [ERR] this machine reports %_PB_ARCH%, the no-Docker stack is x64 only.
echo        Install PowerShell 7 yourself: https://aka.ms/powershell
goto :fail

:err_tools
echo.
echo  [ERR] need curl.exe / tar.exe / certutil.exe (Windows 10 1803+ ships all
echo        three). Update Windows, or install PowerShell 7 yourself:
echo        https://aka.ms/powershell
goto :fail

:err_copy
echo.
echo  [ERR] could not copy %PWSH_FILE% from %PANDORA_LOCALINFRA_MIRROR%
goto :fail

:err_download
echo.
echo  [ERR] could not download %PWSH_FILE%
echo        %PWSH_URL%
echo        Check the network / proxy, or ask the backend team for the offline
echo        share and set PANDORA_LOCALINFRA_MIRROR to it.
del /f /q "%_PB_ZIP%.part" >nul 2>nul
goto :fail

:err_hash
echo.
echo  [ERR] SHA256 mismatch for %_PB_ZIP%   (source: %_PB_SRC%)
echo          expected %PWSH_SHA256%
echo          actual   %_PB_GOT%
if /i "%_PB_SRC%"=="mirror" echo        The file on %PANDORA_LOCALINFRA_MIRROR% is wrong. Ask the backend
if /i "%_PB_SRC%"=="mirror" echo        team to check it - do not work around this.
del /f /q "%_PB_ZIP%" >nul 2>nul
goto :fail

:err_unpack
echo.
echo  [ERR] could not unpack %PWSH_FILE% into %_PB_DIST%
echo        Check free disk space (needs about 250 MB) and that no antivirus is
echo        holding the folder.
rd /s /q "%_PB_DIST%.tmp" >nul 2>nul
goto :fail

:err_layout
echo.
echo  [ERR] %PWSH_FILE% unpacked without a pwsh.exe at its root - the upstream
echo        archive layout changed. Tell the backend team; do not bypass this.
rd /s /q "%_PB_DIST%.tmp" >nul 2>nul
goto :fail

:fail
set "PANDORA_PWSH="
call :cleanup
exit /b 1

:done
call :cleanup
goto :eof

:cleanup
set "_PB_ROOT="
set "_PB_CACHE="
set "_PB_DIST="
set "_PB_PIN="
set "_PB_ARCH="
set "_PB_ZIP="
set "_PB_SRC="
set "_PB_GOT="
set "PWSH_VERSION="
set "PWSH_FILE="
set "PWSH_SHA256="
set "PWSH_URL="
goto :eof
