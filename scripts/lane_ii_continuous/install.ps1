param([Parameter(Mandatory=$true)][string]$RuntimeRoot,[switch]$InteractiveFallback)
$ErrorActionPreference = 'Stop'
$resolvedRoot = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$config = Get-Content -LiteralPath (Join-Path $resolvedRoot 'CONFIG.json') -Raw | ConvertFrom-Json
if ($config.root -ne $resolvedRoot) { throw 'Runtime root mismatch' }
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType S4U -RunLevel Limited
if ($InteractiveFallback) { $principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited }
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero)
$settings.Hidden = $true
$pythonPath = $config.interpreter_windows.path
$cliPath = $config.cli
$arguments = '-I -B "' + $cliPath + '" run --root "' + $resolvedRoot + '"'
$action = New-ScheduledTaskAction -Execute $pythonPath -Argument $arguments -WorkingDirectory $config.app
$boot = New-ScheduledTaskTrigger -AtStartup
$login = New-ScheduledTaskTrigger -AtLogOn -User $identity
$triggers = @($boot,$login)
if ($InteractiveFallback) { $triggers = @($login) }
$task = New-ScheduledTask -Action $action -Trigger $triggers -Settings $settings -Principal $principal -Description 'Lane II paper/research coordinator; bounded admissions and application lock.'
Register-ScheduledTask -TaskName $config.coordinator_task -InputObject $task -Force -ErrorAction Stop | Out-Null
$watchAction = New-ScheduledTaskAction -Execute $pythonPath -Argument ('-I -B "' + $cliPath + '" watchdog --root "' + $resolvedRoot + '"') -WorkingDirectory $config.app
$watchTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
$watchTask = New-ScheduledTask -Action $watchAction -Trigger $watchTrigger -Settings $settings -Principal $principal -Description 'Lane II independent ownership and heartbeat watchdog; no forced unrelated process termination.'
Register-ScheduledTask -TaskName $config.watchdog_task -InputObject $watchTask -Force -ErrorAction Stop | Out-Null
$exports = @()
foreach ($name in @($config.coordinator_task,$config.watchdog_task)) {
    $path = Join-Path $resolvedRoot ('installation/' + $name + '.xml')
    Export-ScheduledTask -TaskName $name -ErrorAction Stop | Set-Content -LiteralPath $path -Encoding Unicode
    $exports += @{task=$name;path=$path;sha256=(Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()}
}
@{utc=[DateTime]::UtcNow.ToString('o');identity=$identity;logon_type= $(if($InteractiveFallback){'InteractiveToken'}else{'S4U'});startup_trigger_registered=(!$InteractiveFallback);requires_user_logon=$InteractiveFallback.IsPresent;installed=$true;scheduled_launch_observed=$false;reboot_exercised=$false;exports=$exports} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $resolvedRoot 'installation/INSTALLATION.json') -Encoding UTF8
Start-ScheduledTask -TaskName $config.coordinator_task
