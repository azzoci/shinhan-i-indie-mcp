@echo off
setlocal

cd /d "%~dp0"

set "TARGET_VENV=%~dp0.venv_x86"

echo [homestock] Preparing 32-bit Python virtualenv...
echo [homestock] cwd=%cd%

where py >nul 2>&1
if errorlevel 1 (
    echo [homestock] Python launcher (py) not found.
    echo [homestock] Please install a 32-bit Python runtime first.
    exit /b 1
)

echo [homestock] Detecting 32-bit Python via py -0p...
py -0p

echo [homestock] Creating virtualenv at "%TARGET_VENV%"...
py -3.10-32 -m venv "%TARGET_VENV%"
if errorlevel 1 (
    echo [homestock] Failed to create 32-bit virtualenv with py -3.10-32.
    echo [homestock] Confirm that Python 3.10 32-bit is installed on this machine.
    exit /b 1
)

set "PYTHON_EXE=%TARGET_VENV%\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
    echo [homestock] Virtualenv python not found after creation: "%PYTHON_EXE%"
    exit /b 1
)

echo [homestock] Installing homestock real backend dependencies...
"%PYTHON_EXE%" -m pip install --upgrade pip
if errorlevel 1 (
    echo [homestock] Failed while upgrading pip.
    exit /b 1
)

"%PYTHON_EXE%" -m pip install -e .[real]
if errorlevel 1 (
    echo [homestock] Failed while installing project dependencies.
    exit /b 1
)

echo [homestock] Done.
echo [homestock] Use this Python for the homestock runtime:
echo [homestock]   %PYTHON_EXE%

