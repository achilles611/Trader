$ErrorActionPreference = 'Stop'
$stage = 'LOAD_WINDOWS_SECURITY_MODULE'
$username = $null
$password = $null
$encrypted = $null
$dpapiConverter = $null
$completed = $false
try {
    Import-Module Microsoft.PowerShell.Security -ErrorAction Stop
    $dpapiConverter = Get-Command -Name ConvertFrom-SecureString -Module Microsoft.PowerShell.Security -CommandType Cmdlet -ErrorAction Stop
    $stage = 'INPUT'
    $root = Join-Path $env:USERPROFILE 'Documents\NinjaTrader 8'
    New-Item -ItemType Directory -Force -Path $root | Out-Null

    $username = Read-Host 'NinjaTrader username'
    $password = Read-Host 'NinjaTrader password (typing is masked)' -AsSecureString
    if ([string]::IsNullOrWhiteSpace($username) -or $password.Length -eq 0) {
        throw 'INCOMPLETE_INPUT'
    }

    $usernamePath = Join-Path $root 'beelzebub-login.username'
    $secretPath = Join-Path $root 'beelzebub-login.secret'
    $stage = 'WRITE_USERNAME'
    [System.IO.File]::WriteAllText($usernamePath, $username, (New-Object System.Text.UTF8Encoding($false)))
    $stage = 'DPAPI_ENCRYPT'
    $encrypted = & $dpapiConverter -SecureString $password
    $stage = 'WRITE_DPAPI_SECRET'
    [System.IO.File]::WriteAllText($secretPath, $encrypted, [System.Text.Encoding]::ASCII)

    $stage = 'RESTRICT_FILE_ACL'
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    foreach ($path in @($usernamePath, $secretPath)) {
        $security = New-Object System.Security.AccessControl.FileSecurity
        $security.SetAccessRuleProtection($true, $false)
        $security.SetOwner((New-Object System.Security.Principal.NTAccount($identity)))
        $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
            $identity,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        $security.AddAccessRule($rule)
        Set-Acl -LiteralPath $path -AclObject $security
    }
    $completed = $true
    [Console]::Out.WriteLine('Success: NinjaTrader login secret is protected for this Windows user.')
}
catch {
    $errorType = $_.Exception.GetType().Name
    $errorId = [string] $_.FullyQualifiedErrorId
    [Console]::Out.WriteLine("The credential seed did not complete during $stage ($errorType / $errorId). No password value was printed or logged.")
}
finally {
    $username = $null
    $encrypted = $null
    $dpapiConverter = $null
    if ($null -ne $password) { $password.Dispose() }
}
[void] (Read-Host 'Press Enter to close this window')
if (-not $completed) { exit 1 }
