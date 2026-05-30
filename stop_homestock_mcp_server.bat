@echo off
setlocal EnableExtensions

cd /d "%~dp0"

echo [homestock] ============================================================
echo [homestock] stop_homestock_mcp_server.bat entered
echo [homestock] date=%DATE% time=%TIME%
echo [homestock] script=%~f0
echo [homestock] script_dir=%~dp0
echo [homestock] target=python.exe/pythonw.exe with command line containing "-m homestock.server"
echo [homestock] scheduled_task=HomestockMcpServer

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop'; $taskName='HomestockMcpServer'; $matchServer = { ($_.Name -eq 'python.exe' -or $_.Name -eq 'pythonw.exe') -and $_.CommandLine -like '*-m homestock.server*' }; $procs = Get-CimInstance Win32_Process | Where-Object $matchServer; if (-not $procs) { Write-Host '[homestock] no homestock.server process found' } else { foreach ($proc in $procs) { Write-Host ('[homestock] stopping pid={0} session={1}' -f $proc.ProcessId, $proc.SessionId); Write-Host ('[homestock] command={0}' -f $proc.CommandLine); Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop } }; Start-Sleep -Milliseconds 300; $remaining = Get-CimInstance Win32_Process | Where-Object $matchServer; if ($remaining) { Write-Host '[homestock] some homestock.server processes are still running'; foreach ($proc in $remaining) { Write-Host ('[homestock] remaining pid={0} session={1} command={2}' -f $proc.ProcessId, $proc.SessionId, $proc.CommandLine) }; exit 1 }; try { $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop; if ($task.State -eq 'Running') { Write-Host ('[homestock] ending scheduled task {0}' -f $taskName); schtasks /End /TN $taskName | Out-Host; if ($LASTEXITCODE -ne 0) { Write-Host ('[homestock] scheduled task end returned code {0}' -f $LASTEXITCODE); Start-Sleep -Milliseconds 500; $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop; if ($task.State -eq 'Running') { Write-Host ('[homestock] scheduled task {0} is still running' -f $taskName); exit 1 } } } else { Write-Host ('[homestock] scheduled task {0} state={1}' -f $taskName, $task.State) } } catch { Write-Host ('[homestock] scheduled task {0} not found or unavailable: {1}' -f $taskName, $_.Exception.Message) }; Write-Host '[homestock] homestock.server stopped'; exit 0"

set "EXIT_CODE=%ERRORLEVEL%"
echo [homestock] stop_homestock_mcp_server exited with code %EXIT_CODE%
exit /b %EXIT_CODE%
