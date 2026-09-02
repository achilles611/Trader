param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('probe', 'start', 'close-gracefully', 'submit-login', 'connect-lucid')]
    [string] $Action
)

$ErrorActionPreference = 'Stop'
$targetConnection = 'LucidFlex25k'
$credentialRoot = Join-Path $env:USERPROFILE 'Documents\NinjaTrader 8'
$usernamePath = Join-Path $credentialRoot 'beelzebub-login.username'
$secretPath = Join-Path $credentialRoot 'beelzebub-login.secret'

Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes

function Write-SanitizedResult {
    param([hashtable] $Payload)
    [Console]::Out.WriteLine(($Payload | ConvertTo-Json -Compress -Depth 5))
}

# Resolve only the fixed NinjaTrader executable name from administrator or
# machine configuration.  No HTTP/request value reaches this helper.
function Resolve-NinjaExecutable {
    $candidates = @()
    if (-not [String]::IsNullOrWhiteSpace($env:NINJATRADER_EXECUTABLE)) {
        $candidates += $env:NINJATRADER_EXECUTABLE
    }
    foreach ($registryPath in @(
        'Registry::HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\App Paths\NinjaTrader.exe',
        'Registry::HKEY_LOCAL_MACHINE\Software\Microsoft\Windows\CurrentVersion\App Paths\NinjaTrader.exe'
    )) {
        try {
            $configured = (Get-ItemProperty -LiteralPath $registryPath -ErrorAction Stop).'(default)'
            if (-not [String]::IsNullOrWhiteSpace($configured)) { $candidates += $configured }
        }
        catch { }
    }
    if (-not [String]::IsNullOrWhiteSpace($env:ProgramFiles)) {
        $candidates += (Join-Path $env:ProgramFiles 'NinjaTrader 8\bin\NinjaTrader.exe')
    }
    foreach ($candidate in $candidates) {
        try {
            $resolved = [System.IO.Path]::GetFullPath([Environment]::ExpandEnvironmentVariables([string] $candidate))
            if ([System.IO.Path]::GetFileName($resolved) -eq 'NinjaTrader.exe' -and [System.IO.File]::Exists($resolved)) {
                return $resolved
            }
        }
        catch { }
    }
    return $null
}

function Request-GracefulNinjaShutdown {
    param(
        [System.Diagnostics.Process] $Process,
        [System.Windows.Automation.AutomationElement] $ControlCenter
    )
    if ($null -eq $Process) { return $true }
    try {
        if ($Process.CloseMainWindow()) { return $true }
    }
    catch { }
    if ($null -eq $ControlCenter) { return $false }
    try {
        $current = $ControlCenter
        while ($null -ne $current -and $current.Current.ControlType -ne [System.Windows.Automation.ControlType]::Window) {
            $current = [System.Windows.Automation.TreeWalker]::ControlViewWalker.GetParent($current)
        }
        if ($null -eq $current) { return $false }
        $windowPattern = $current.GetCurrentPattern([System.Windows.Automation.WindowPattern]::Pattern)
        $windowPattern.Close()
        return $true
    }
    catch { return $false }
}

function Find-Descendants {
    param(
        [System.Windows.Automation.AutomationElement] $Root,
        [System.Windows.Automation.Condition] $Condition
    )
    return @($Root.FindAll([System.Windows.Automation.TreeScope]::Descendants, $Condition))
}

function Get-NinjaContext {
    $processes = @(Get-Process -Name 'NinjaTrader' -ErrorAction SilentlyContinue)
    if ($processes.Count -gt 1) {
        return @{ Failure = 'MULTIPLE_NINJATRADER_PROCESSES'; Process = $null; Windows = @() }
    }
    if ($processes.Count -eq 0) {
        return @{ Failure = $null; Process = $null; Windows = @() }
    }
    $process = $processes[0]
    $processCondition = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ProcessIdProperty,
        $process.Id
    )
    $windowCondition = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
        [System.Windows.Automation.ControlType]::Window
    )
    $windows = @([System.Windows.Automation.AutomationElement]::RootElement.FindAll(
        [System.Windows.Automation.TreeScope]::Descendants,
        (New-Object System.Windows.Automation.AndCondition($processCondition, $windowCondition))
    ))
    return @{ Failure = $null; Process = $process; Windows = $windows }
}

function Get-ControlCenter {
    param([System.Diagnostics.Process] $Process)
    if ($null -eq $Process) { return $null }
    $idCondition = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
        'ControlCenter'
    )
    $processCondition = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ProcessIdProperty,
        $Process.Id
    )
    $identified = @([System.Windows.Automation.AutomationElement]::RootElement.FindAll(
        [System.Windows.Automation.TreeScope]::Descendants,
        (New-Object System.Windows.Automation.AndCondition($processCondition, $idCondition))
    ))
    if ($identified.Count -eq 1 -and $identified[0].Current.ControlType -eq [System.Windows.Automation.ControlType]::Custom) {
        return $identified[0]
    }
    return $null
}

function Get-LoginCandidate {
    param([object[]] $Windows)
    $candidates = @()
    $welcomeWindows = @($Windows | Where-Object { $_.Current.Name -in @('Welcome', 'NinjaTrader Login') })
    foreach ($window in $welcomeWindows) {
        $usernameId = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
            'tbUserName'
        )
        $passwordId = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
            'passwordBox'
        )
        $buttonId = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
            'btnLogin'
        )
        $usernames = @(Find-Descendants -Root $window -Condition $usernameId | Where-Object {
            $_.Current.IsEnabled -and -not $_.Current.IsOffscreen -and
            $_.Current.ControlType -eq [System.Windows.Automation.ControlType]::Edit -and
            -not $_.Current.IsPassword
        })
        $passwords = @(Find-Descendants -Root $window -Condition $passwordId | Where-Object {
            $_.Current.IsEnabled -and -not $_.Current.IsOffscreen -and
            $_.Current.ControlType -eq [System.Windows.Automation.ControlType]::Edit -and
            $_.Current.IsPassword
        })
        $buttons = @(Find-Descendants -Root $window -Condition $buttonId | Where-Object {
            $_.Current.IsEnabled -and -not $_.Current.IsOffscreen -and
            $_.Current.ControlType -eq [System.Windows.Automation.ControlType]::Button -and
            $_.Current.Name -in @('Log In', 'Login')
        })
        if ($passwords.Count -eq 1 -and $usernames.Count -eq 1 -and $buttons.Count -eq 1) {
            $candidates += @{
                Window = $window
                Username = $usernames[0]
                Password = $passwords[0]
                Button = $buttons[0]
            }
        }
    }
    if ($candidates.Count -gt 1) { return @{ Failure = 'AMBIGUOUS_LOGIN_WINDOW'; Candidate = $null } }
    if ($candidates.Count -eq 1) { return @{ Failure = $null; Candidate = $candidates[0] } }
    if ($welcomeWindows.Count -gt 0) { return @{ Failure = 'UNEXPECTED_LOGIN_UI'; Candidate = $null } }
    return @{ Failure = $null; Candidate = $null }
}

function Get-UiFailure {
    param([object[]] $Windows)
    $names = @()
    $trueCondition = [System.Windows.Automation.Condition]::TrueCondition
    foreach ($window in $Windows) {
        $names += $window.Current.Name
        $names += @(Find-Descendants -Root $window -Condition $trueCondition | ForEach-Object { $_.Current.Name })
    }
    $joined = ($names -join ' ').ToLowerInvariant()
    if ($joined -match 'multi-factor|multifactor|verification code|authenticator|security challenge|one-time code|mfa') {
        return 'MFA_OR_CHALLENGE_PRESENT'
    }
    if ($joined -match 'invalid credentials|invalid username|incorrect password|login failed|log in failed') {
        return 'INVALID_CREDENTIALS'
    }
    return $null
}

function Get-CellValue {
    param([System.Windows.Automation.AutomationElement] $Element)
    try {
        $pattern = $Element.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
        return [string] $pattern.Current.Value
    }
    catch {
        return [string] $Element.Current.Name
    }
}

function Get-LucidConnectionState {
    param([System.Windows.Automation.AutomationElement] $ControlCenter)
    if ($null -eq $ControlCenter) { return 'UNKNOWN' }
    $gridId = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
        'AccountsGrid'
    )
    $grids = @(Find-Descendants -Root $ControlCenter -Condition $gridId)
    if ($grids.Count -ne 1) { return 'UNKNOWN' }
    $cells = @(Find-Descendants -Root $grids[0] -Condition ([System.Windows.Automation.Condition]::TrueCondition))
    $connectionCells = @($cells | Where-Object { $_.Current.AutomationId -match '^RecordRow(?<Row>\d+)_Connection$' })
    $found = $false
    foreach ($connectionCell in $connectionCells) {
        $rowNumber = [regex]::Match([string] $connectionCell.Current.AutomationId, '^RecordRow(\d+)_Connection$').Groups[1].Value
        $connection = Get-CellValue $connectionCell
        if ($connection -eq $targetConnection) {
            $found = $true
            $stateCellId = "RecordRow${rowNumber}_"
            $stateCells = @($cells | Where-Object { $_.Current.AutomationId -eq $stateCellId })
            $state = if ($stateCells.Count -eq 1) { Get-CellValue $stateCells[0] } else { $null }
            if ($state -eq 'Connected') { return 'CONNECTED' }
        }
    }
    if ($found) { return 'DISCONNECTED' }
    return 'UNKNOWN'
}

function Get-Probe {
    $context = Get-NinjaContext
    if ($null -ne $context.Failure) {
        return @{
            ok = $false; process_detected = $false; login_window_detected = $false
            control_center_detected = $false; lucid_connection_state = 'UNKNOWN'
            failure_category = $context.Failure
        }
    }
    if ($null -eq $context.Process) {
        return @{
            ok = $true; process_detected = $false; login_window_detected = $false
            control_center_detected = $false; lucid_connection_state = 'UNKNOWN'; failure_category = $null
        }
    }
    $uiFailure = Get-UiFailure -Windows $context.Windows
    $login = Get-LoginCandidate -Windows $context.Windows
    if ($null -eq $uiFailure) { $uiFailure = $login.Failure }
    $controlCenter = Get-ControlCenter -Process $context.Process
    return @{
        ok = ($null -eq $uiFailure)
        process_detected = $true
        login_window_detected = ($null -ne $login.Candidate)
        control_center_detected = ($null -ne $controlCenter)
        lucid_connection_state = Get-LucidConnectionState -ControlCenter $controlCenter
        failure_category = $uiFailure
    }
}

try {
    if ($Action -eq 'probe') {
        Write-SanitizedResult (Get-Probe)
        exit 0
    }

    if ($Action -eq 'start') {
        $context = Get-NinjaContext
        if ($null -ne $context.Failure) {
            Write-SanitizedResult @{ ok = $false; failure_category = $context.Failure }
            exit 0
        }
        if ($null -ne $context.Process) {
            Write-SanitizedResult @{ ok = $true; result = 'ALREADY_RUNNING'; failure_category = $null }
            exit 0
        }
        $ninjaExecutable = Resolve-NinjaExecutable
        if ($null -eq $ninjaExecutable) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'NINJATRADER_START_FAILED' }
            exit 0
        }
        Start-Process -FilePath $ninjaExecutable -WorkingDirectory (Split-Path $ninjaExecutable -Parent) | Out-Null
        Write-SanitizedResult @{ ok = $true; result = 'STARTED'; failure_category = $null }
        exit 0
    }

    if ($Action -eq 'close-gracefully') {
        $context = Get-NinjaContext
        if ($null -ne $context.Failure) {
            Write-SanitizedResult @{ ok = $false; failure_category = $context.Failure }
            exit 0
        }
        if ($null -eq $context.Process) {
            Write-SanitizedResult @{ ok = $true; result = 'ALREADY_STOPPED'; failure_category = $null }
            exit 0
        }
        $controlCenter = Get-ControlCenter -Process $context.Process
        if (-not (Request-GracefulNinjaShutdown -Process $context.Process -ControlCenter $controlCenter)) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'GRACEFUL_SHUTDOWN_REFUSED' }
            exit 0
        }
        Write-SanitizedResult @{ ok = $true; result = 'GRACEFUL_CLOSE_REQUESTED'; failure_category = $null }
        exit 0
    }

    if ($Action -eq 'submit-login') {
        $context = Get-NinjaContext
        if ($null -ne $context.Failure -or $null -eq $context.Process) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'LOGIN_WINDOW_NOT_IDENTIFIED' }
            exit 0
        }
        $uiFailure = Get-UiFailure -Windows $context.Windows
        if ($null -ne $uiFailure) {
            Write-SanitizedResult @{ ok = $false; failure_category = $uiFailure }
            exit 0
        }
        $login = Get-LoginCandidate -Windows $context.Windows
        if ($null -ne $login.Failure -or $null -eq $login.Candidate) {
            $failure = if ($null -ne $login.Failure) { $login.Failure } else { 'LOGIN_WINDOW_NOT_IDENTIFIED' }
            Write-SanitizedResult @{ ok = $false; failure_category = $failure }
            exit 0
        }
        if (-not [System.IO.File]::Exists($usernamePath) -or -not [System.IO.File]::Exists($secretPath)) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'MISSING_LOCAL_SECRET' }
            exit 0
        }
        $username = [System.IO.File]::ReadAllText($usernamePath).Trim([char]0xFEFF, [char]0x0D, [char]0x0A)
        if ([string]::IsNullOrWhiteSpace($username) -or $username.Length -gt 256) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'MISSING_LOCAL_SECRET' }
            exit 0
        }
        $securePassword = $null
        $passwordPointer = [IntPtr]::Zero
        $plainPassword = $null
        try {
            $encrypted = [System.IO.File]::ReadAllText($secretPath).Trim()
            $securePassword = ConvertTo-SecureString -String $encrypted -ErrorAction Stop
        }
        catch {
            Write-SanitizedResult @{ ok = $false; failure_category = 'CORRUPT_DPAPI_SECRET' }
            exit 0
        }
        try {
            $usernamePattern = $login.Candidate.Username.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
            $passwordPattern = $login.Candidate.Password.GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern)
            $invokePattern = $login.Candidate.Button.GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
            $usernamePattern.SetValue($username)
            $passwordPointer = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
            $plainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($passwordPointer)
            $passwordPattern.SetValue($plainPassword)
            $invokePattern.Invoke()
        }
        catch {
            Write-SanitizedResult @{ ok = $false; failure_category = 'UNEXPECTED_LOGIN_UI' }
            exit 0
        }
        finally {
            if ($passwordPointer -ne [IntPtr]::Zero) {
                [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($passwordPointer)
            }
            $plainPassword = $null
            $username = $null
            if ($null -ne $securePassword) { $securePassword.Dispose() }
        }
        Write-SanitizedResult @{ ok = $true; result = 'SUBMITTED'; failure_category = $null }
        exit 0
    }

    if ($Action -eq 'connect-lucid') {
        $context = Get-NinjaContext
        if ($null -ne $context.Failure -or $null -eq $context.Process) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'CONTROL_CENTER_NOT_IDENTIFIED' }
            exit 0
        }
        $controlCenter = Get-ControlCenter -Process $context.Process
        if ($null -eq $controlCenter) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'CONTROL_CENTER_NOT_IDENTIFIED' }
            exit 0
        }
        if ((Get-LucidConnectionState -ControlCenter $controlCenter) -eq 'CONNECTED') {
            Write-SanitizedResult @{ ok = $true; result = 'ALREADY_CONNECTED'; failure_category = $null }
            exit 0
        }
        $connectionsId = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::AutomationIdProperty,
            'ControlCenterMenuItemConnections'
        )
        $menus = @(Find-Descendants -Root $controlCenter -Condition $connectionsId)
        if ($menus.Count -ne 1) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'CONTROL_CENTER_NOT_IDENTIFIED' }
            exit 0
        }
        $expand = $menus[0].GetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern)
        $expand.Expand()
        Start-Sleep -Milliseconds 400
        $processCondition = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ProcessIdProperty,
            $context.Process.Id
        )
        $nameCondition = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::NameProperty,
            $targetConnection
        )
        $typeCondition = New-Object System.Windows.Automation.PropertyCondition(
            [System.Windows.Automation.AutomationElement]::ControlTypeProperty,
            [System.Windows.Automation.ControlType]::MenuItem
        )
        $rawCandidates = @([System.Windows.Automation.AutomationElement]::RootElement.FindAll(
            [System.Windows.Automation.TreeScope]::Descendants,
            (New-Object System.Windows.Automation.AndCondition($processCondition, $nameCondition, $typeCondition))
        ) | Where-Object { $_.Current.IsEnabled -and -not $_.Current.IsOffscreen })
        $uniqueCandidates = @{}
        foreach ($candidate in $rawCandidates) {
            $runtimeId = $candidate.GetRuntimeId() -join '.'
            if (-not $uniqueCandidates.ContainsKey($runtimeId)) { $uniqueCandidates[$runtimeId] = $candidate }
        }
        $candidates = @($uniqueCandidates.Values)
        if ($candidates.Count -ne 1) {
            Write-SanitizedResult @{ ok = $false; failure_category = 'UNEXPECTED_LOGIN_UI' }
            exit 0
        }
        $invoke = $candidates[0].GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern)
        $invoke.Invoke()
        Write-SanitizedResult @{ ok = $true; result = 'CONNECT_REQUESTED'; failure_category = $null }
        exit 0
    }
}
catch {
    Write-SanitizedResult @{ ok = $false; failure_category = 'AUTOMATION_PROCESS_FAILED' }
    exit 0
}
