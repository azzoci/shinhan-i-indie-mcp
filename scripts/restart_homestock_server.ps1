param(
    [int]$WarmupSeconds = 20
)

$ErrorActionPreference = "Stop"

$serverProcs = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq "python.exe" -and $_.CommandLine -like "*-m homestock.server*" }

foreach ($proc in $serverProcs) {
    try {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
        Write-Host "stopped pid=$($proc.ProcessId)"
    } catch {
        Write-Host "failed_to_stop pid=$($proc.ProcessId): $($_.Exception.Message)"
    }
}

Start-Sleep -Seconds 2

schtasks /Run /TN HomestockMcpServer | Out-Host
if ($LASTEXITCODE -ne 0) {
    Write-Host "failed_to_start_task code=$LASTEXITCODE"
    exit 1
}

Start-Sleep -Seconds $WarmupSeconds

$running = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq "python.exe" -and $_.CommandLine -like "*-m homestock.server*" } |
    Select-Object ProcessId, SessionId, CommandLine

if (-not $running) {
    Write-Host "homestock.server process not found after restart"
    exit 1
}

$running | Format-List
