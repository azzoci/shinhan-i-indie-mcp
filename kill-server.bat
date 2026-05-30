@echo off
setlocal EnableExtensions

echo [homestock] ============================================================
echo [homestock] kill-server.bat entered
echo [homestock] date=%DATE% time=%TIME%
echo [homestock] script=%~f0
echo [homestock] target=python.exe/pythonw.exe with command line containing "-m homestock.server"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop'; $procs = Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -like '*-m homestock.server*' }; if (-not $procs) { Write-Host '[homestock] no homestock.server process found'; exit 0 }; foreach ($proc in $procs) { Write-Host ('[homestock] stopping pid={0} session={1}' -f $proc.ProcessId, $proc.SessionId); Write-Host ('[homestock] command={0}' -f $proc.CommandLine); Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop }; Start-Sleep -Milliseconds 300; $remaining = Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -like '*-m homestock.server*' }; if ($remaining) { Write-Host '[homestock] some homestock.server processes are still running'; foreach ($proc in $remaining) { Write-Host ('[homestock] remaining pid={0} session={1} command={2}' -f $proc.ProcessId, $proc.SessionId, $proc.CommandLine) }; exit 1 }; Write-Host '[homestock] homestock.server stopped'; exit 0"

set "EXIT_CODE=%ERRORLEVEL%"
echo [homestock] kill-server exited with code %EXIT_CODE%
exit /b %EXIT_CODE%
