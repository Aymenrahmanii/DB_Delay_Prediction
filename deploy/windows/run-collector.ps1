# Keeps `dbdelay collect` running on a Windows machine: restarts it 10 s after any exit,
# except exit code 2 (another collector already holds the lock), which ends the loop.
# Output goes to data\logs\collector-YYYY-MM-DD.log. Started at logon by install-tasks.ps1.

param(
    [Parameter(Mandatory = $true)][string]$ProjectDir,
    [Parameter(Mandatory = $true)][string]$Uv
)

$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath $ProjectDir
$logDir = Join-Path $ProjectDir 'data\logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-Log([string]$Message) {
    $log = Join-Path $logDir ("collector-{0:yyyy-MM-dd}.log" -f (Get-Date))
    "{0:yyyy-MM-dd HH:mm:ss} {1}" -f (Get-Date), $Message | Out-File -FilePath $log -Append -Encoding utf8
}

while ($true) {
    $log = Join-Path $logDir ("collector-{0:yyyy-MM-dd}.log" -f (Get-Date))
    Write-Log 'starting dbdelay collect'
    # Python writes its own log; nothing is redirected through PowerShell.
    & $Uv run dbdelay --log-file $log collect
    $code = $LASTEXITCODE
    if ($code -eq 2) {
        Write-Log 'another collector is already running; not starting a second one'
        exit 2
    }
    Write-Log "collector exited with code $code; restarting in 10 s"
    Start-Sleep -Seconds 10
}
