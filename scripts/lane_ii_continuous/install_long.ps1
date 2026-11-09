param([Parameter(Mandatory = $true)][string]$RuntimeRoot)

$ErrorActionPreference = 'Stop'
$rootPath = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$config = Get-Content -LiteralPath (Join-Path $rootPath 'CONFIG.json') -Raw | ConvertFrom-Json
if ($config.root -ne $rootPath -or $config.schema -ne 'LANE_II_LONG_QUALIFICATION_V1' -or !$config.paper_only) {
    throw 'Exact frozen paper runtime required'
}
$names = @('Trader-LaneII-Long-Qualification', 'Trader-LaneII-Long-Checkpoint', 'Trader-LaneII-Long-Dashboard')
foreach ($name in $names) {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) { throw "Task name already registered $name" }
}
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero)
$settings.Hidden = $true
$program = Join-Path $config.app 'scripts\lane_ii_continuous\long_qualification.py'
$pythonPath = $config.interpreter_windows.path
$commands = @('run', 'checkpoint', 'dashboard')
$utcNow = [DateTime]::UtcNow
$quarter = $utcNow.Date.AddMinutes((1 + [math]::Floor($utcNow.TimeOfDay.TotalMinutes / 15)) * 15)
$triggers = @(
    (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)),
    (New-ScheduledTaskTrigger -Once -At $quarter.AddSeconds(5).ToLocalTime() -RepetitionInterval (New-TimeSpan -Minutes 15)),
    (New-ScheduledTaskTrigger -Once -At $quarter.AddSeconds(65).ToLocalTime() -RepetitionInterval (New-TimeSpan -Minutes 15))
)
$exportDir = Join-Path $rootPath 'installation'
New-Item -ItemType Directory -Path $exportDir | Out-Null
$exports = @()
$registered = @()
try {
    for ($i = 0; $i -lt $commands.Count; $i++) {
        $arguments = '-I -B "' + $program + '" ' + $commands[$i] + ' --root "' + $rootPath + '"'
        $action = New-ScheduledTaskAction -Execute $pythonPath -Argument $arguments -WorkingDirectory $config.app
        $task = New-ScheduledTask -Action $action -Trigger $triggers[$i] -Settings $settings -Principal $principal
        Register-ScheduledTask -TaskName $names[$i] -InputObject $task | Out-Null
        $registered += $names[$i]
        $path = Join-Path $exportDir ($names[$i] + '.xml')
        Export-ScheduledTask -TaskName $names[$i] | Set-Content -LiteralPath $path -Encoding Unicode
        $exports += @{ task = $names[$i]; path = $path; sha256 = (Get-FileHash -LiteralPath $path).Hash.ToLowerInvariant() }
    }
    @{ installed_utc = [DateTime]::UtcNow.ToString('o'); logon = 'InteractiveToken'; exports = $exports;
       checkpoint_minutes = 15; dashboard_offset_minutes = 1; exact_window_is_bound_by_coordinator = $true } |
        ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $exportDir 'TASKS.json') -Encoding UTF8
    Start-ScheduledTask -TaskName $names[0]
} catch {
    foreach ($name in $registered) {
        Disable-ScheduledTask -TaskName $name -ErrorAction Continue | Out-Null
    }
    throw
}
