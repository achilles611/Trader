param(
    [string]$Python = 'C:\Users\atlas\Documents\Trader\.venv312\Scripts\python.exe',
    [string]$Corpus = 'H:\PongMeasurement\20260914\representative-signed-17h44-v2',
    [string]$OutputRoot = 'H:\PongPeriodic\20260918\paced'
)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $OutputRoot) { throw 'Fresh evidence directory required' }
New-Item -ItemType Directory -Path $OutputRoot | Out-Null
$candidateRoot = Split-Path -Parent $PSScriptRoot
$runs = @(
    @{Name='baseline-1';Interval=0},
    @{Name='10ms';Interval=10},
    @{Name='20ms-1';Interval=20},
    @{Name='25ms';Interval=25},
    @{Name='baseline-2';Interval=0},
    @{Name='20ms-2';Interval=20},
    @{Name='20ms-half-phase';Interval=20;Phase=10}
)
Push-Location -LiteralPath $candidateRoot
try {
    foreach ($run in $runs) {
        $hostSample = [ordered]@{
            at=(Get-Date).ToUniversalTime().ToString('o')
            cpu=@(Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor | Where-Object Name -eq '_Total' | Select-Object PercentProcessorTime)
            memory=@(Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory | Select-Object AvailableMBytes,PagesPersec)
            disk=@(Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk | Select-Object Name,PercentDiskTime,CurrentDiskQueueLength,DiskBytesPersec)
            relevantProcesses=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -match 'python|NinjaTrader|BeezConsole'} | Select-Object ProcessId,ParentProcessId,Name,ExecutablePath)
        }
        $hostSample | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $OutputRoot ($run.Name+'-host-before.json'))
        if (@($hostSample.relevantProcesses | Where-Object {$_.Name -match 'NinjaTrader|BeezConsole'}).Count) {
            throw 'ACTIVE_RUNTIME_PRESENT: preserve it; defer offline timing instead of introducing benchmark contention'
        }
        Write-Output ('Starting natural '+$run.Name)
        $phase = if ($run.ContainsKey('Phase')) { $run.Phase } else { 0 }
        & $Python 'tools\pong_periodic_paced.py' --corpus $Corpus --output (Join-Path $OutputRoot $run.Name) --interval-ms $run.Interval --phase-ms $phase --limit-seconds 900
        if ($LASTEXITCODE -ne 0) { throw ('Replay integrity failed: '+$run.Name+' exit '+$LASTEXITCODE) }
    }
} finally {
    Pop-Location
}
