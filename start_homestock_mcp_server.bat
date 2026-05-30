@echo off
setlocal EnableExtensions

set "START_SCRIPT=%~dp0scripts\start_homestock_mcp.cmd"

if not exist "%START_SCRIPT%" (
    echo [homestock] canonical start script missing: "%START_SCRIPT%"
    exit /b 1
)

call "%START_SCRIPT%" %*
exit /b %ERRORLEVEL%
