# Registers two scheduled tasks for the current user (no admin rights needed):
#   dbdelay-collector  starts run-collector.ps1 at logon and keeps it running
#   dbdelay-health     runs `dbdelay health --hours 1` every hour, appending to data\logs\health.log
# Run once from the project folder:  powershell -ExecutionPolicy Bypass -File deploy\windows\install-tasks.ps1
# Stop the collector:               powershell -ExecutionPolicy Bypass -File deploy\windows\install-tasks.ps1 -Stop
# Remove both tasks (and stop):     powershell -ExecutionPolicy Bypass -File deploy\windows\install-tasks.ps1 -Uninstall

param([switch]$Uninstall, [switch]$Stop)

$ErrorActionPreference = 'Stop'
$names = 'dbdelay-collector', 'dbdelay-health'

function Stop-Collector {
    # Stopping the task ends the wrapper; the uv/python children have to be ended too,
    # or they keep the lock and the next start exits with "already running".
    if (Get-ScheduledTask -TaskName 'dbdelay-collector' -ErrorAction SilentlyContinue) {
        Stop-ScheduledTask -TaskName 'dbdelay-collector'
    }
    Get-CimInstance Win32_Process |
        Where-Object { $_.CommandLine -match 'dbdelay(\.exe)?"?\s+collect|run-collector\.ps1' } |
        ForEach-Object {
            try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch {}
            Write-Host "stopped process $($_.ProcessId)"
        }
}

if ($Stop) {
    Stop-Collector
    exit 0
}

if ($Uninstall) {
    Stop-Collector
    foreach ($n in $names) {
        if (Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $n -Confirm:$false
            Write-Host "removed $n"
        }
    }
    exit 0
}

$project = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$uv = (Get-Command uv -ErrorAction Stop).Source
if (-not (Test-Path (Join-Path $project '.env'))) { throw "No .env in $project; create it first." }
$logDir = Join-Path $project 'data\logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

$runner = Join-Path $PSScriptRoot 'run-collector.ps1'
$collect = New-ScheduledTaskAction -Execute 'powershell.exe' -WorkingDirectory $project `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runner`" -ProjectDir `"$project`" -Uv `"$uv`""
Register-ScheduledTask -TaskName 'dbdelay-collector' -Force `
    -Action $collect -Trigger (New-ScheduledTaskTrigger -AtLogOn -User $user) `
    -Settings $settings -Principal $principal `
    -Description 'DB Timetables collector (restarts on exit)' | Out-Null

$healthLog = Join-Path $logDir 'health.log'
$healthCmd = "Set-Location -LiteralPath '$project'; " +
    "('--- ' + (Get-Date -Format s)), (& '$uv' run dbdelay health --hours 1) | " +
    "Out-File -Append -Encoding utf8 -FilePath '$healthLog'"
$health = New-ScheduledTaskAction -Execute 'powershell.exe' -WorkingDirectory $project `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command `"$healthCmd`""
$hourly = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddHours((Get-Date).Hour + 1) `
    -RepetitionInterval (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName 'dbdelay-health' -Force `
    -Action $health -Trigger $hourly -Settings $settings -Principal $principal `
    -Description 'Hourly DB collector health check' | Out-Null

Write-Host "Registered dbdelay-collector (at logon) and dbdelay-health (hourly) for $user."
Write-Host "Start the collector now with:  Start-ScheduledTask -TaskName dbdelay-collector"
Write-Host "Logs: $logDir"
