@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "HOMESTOCK_ROOT=%%~fI"

powershell -NoProfile -ExecutionPolicy Bypass -Command "$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent()); if ($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { exit 0 } exit 1"
if errorlevel 1 (
    echo [homestock] administrator privilege required; requesting UAC elevation...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $q=[char]34; $cmdArgs='/k ' + $q + '%~f0' + $q; Start-Process -FilePath 'cmd.exe' -ArgumentList $cmdArgs -Verb RunAs -WorkingDirectory '%HOMESTOCK_ROOT%'; exit 0 } catch { Write-Host ('[homestock] elevation failed: ' + $_.Exception.Message); exit 1 }"
    exit /b
)

whoami /groups | findstr /c:"S-1-16-12288" /c:"S-1-16-16384" >nul
if errorlevel 1 (
    echo [homestock] High Mandatory Level is required but was not detected.
    whoami /groups | findstr /i "Mandatory Level"
    exit /b 1
)

cd /d "%HOMESTOCK_ROOT%"

echo [homestock] ============================================================
echo [homestock] scripts\start_homestock_mcp.cmd entered
echo [homestock] date=%DATE% time=%TIME%
echo [homestock] script=%~f0
echo [homestock] root=%HOMESTOCK_ROOT%
echo [homestock] administrator=true
echo [homestock] high_integrity=true

if not defined INDI_BACKEND set "INDI_BACKEND=real"
if not defined ALLOW_LIVE_ORDERS set "ALLOW_LIVE_ORDERS=false"
if not defined HOMESTOCK_HOST set "HOMESTOCK_HOST=0.0.0.0"
if not defined HOMESTOCK_PORT set "HOMESTOCK_PORT=8000"

set "PYTHON_EXE=%HOMESTOCK_ROOT%\.venv_x86\Scripts\python.exe"
set "LOG_DIR=%HOMESTOCK_ROOT%\logs"

if not exist "%PYTHON_EXE%" (
    echo [homestock] 32-bit Python virtualenv not found: "%PYTHON_EXE%"
    echo [homestock] Expected runtime layout with .venv_x86\Scripts\python.exe under the homestock root
    exit /b 1
)
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [homestock] Starting MCP server...
echo [homestock] cwd=%cd%
echo [homestock] host=%HOMESTOCK_HOST%
echo [homestock] port=%HOMESTOCK_PORT%
echo [homestock] backend=%INDI_BACKEND%
echo [homestock] live_orders=%ALLOW_LIVE_ORDERS%
if defined HOMESTOCK_ACCOUNT_PASSWORD (
    echo [homestock] account_password=set
) else (
    echo [homestock] account_password=missing
)
echo [homestock] log_dir=%LOG_DIR%
echo [homestock] mcp=http://%HOMESTOCK_HOST%:%HOMESTOCK_PORT%/mcp
if exist "%HOMESTOCK_ROOT%\homestock\server.py" (
    echo [homestock] server_module=found "%HOMESTOCK_ROOT%\homestock\server.py"
) else (
    echo [homestock] server_module=missing "%HOMESTOCK_ROOT%\homestock\server.py"
)
if exist "%HOMESTOCK_ROOT%\.runtime" (
    echo [homestock] runtime_dir=found "%HOMESTOCK_ROOT%\.runtime"
) else (
    echo [homestock] runtime_dir=missing "%HOMESTOCK_ROOT%\.runtime"
)
echo [homestock] launching: "%PYTHON_EXE%" -m homestock.server

powershell -NoProfile -ExecutionPolicy Bypass -Command "$port = [int]'%HOMESTOCK_PORT%'; $listener = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1; if (-not $listener) { exit 0 }; $proc = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $listener.OwningProcess) -ErrorAction SilentlyContinue; if ($proc -and $proc.CommandLine -like '*-m homestock.server*') { Write-Host ('[homestock] server already running on port ' + $port + ' pid=' + $listener.OwningProcess); exit 2 }; Write-Host ('[homestock] port ' + $port + ' is already in use by pid=' + $listener.OwningProcess); exit 1"
if errorlevel 2 exit /b 0
if errorlevel 1 exit /b 1

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$logFile = Join-Path '%LOG_DIR%' ('homestock_server_{0:yyyyMMdd_HHmmss}_{1}.log' -f (Get-Date), $PID); Write-Host ('[homestock] log_file=' + $logFile); $env:INDI_BACKEND='%INDI_BACKEND%'; $env:ALLOW_LIVE_ORDERS='%ALLOW_LIVE_ORDERS%'; $env:HOMESTOCK_HOST='%HOMESTOCK_HOST%'; $env:HOMESTOCK_PORT='%HOMESTOCK_PORT%'; & '%PYTHON_EXE%' -m homestock.server 2>&1 | Tee-Object -FilePath $logFile -Append; exit $LASTEXITCODE"

set "EXIT_CODE=%ERRORLEVEL%"
echo [homestock] server exited with code %EXIT_CODE%
exit /b %EXIT_CODE%
