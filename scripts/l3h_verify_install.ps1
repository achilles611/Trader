[CmdletBinding()]
param(
    [string]$NinjaTraderDocumentsRoot = (Join-Path ([Environment]::GetFolderPath("MyDocuments")) "NinjaTrader 8")
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$source = Join-Path $repo "ninjatrader\NinjaScript\AddOns\BeelzebubLiveExecutionAddOn.cs"
$installed = Join-Path $NinjaTraderDocumentsRoot "bin\Custom\AddOns\BeelzebubLiveExecutionAddOn.cs"
$dllPath = Join-Path $NinjaTraderDocumentsRoot "bin\Custom\NinjaTrader.Custom.dll"
$dll = if (Test-Path -LiteralPath $dllPath) { Get-Item -LiteralPath $dllPath } else { $null }
$sourceHash = if (Test-Path -LiteralPath $source) { (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant() } else { $null }
$installedHash = if (Test-Path -LiteralPath $installed) { (Get-FileHash -Algorithm SHA256 -LiteralPath $installed).Hash.ToLowerInvariant() } else { $null }
$utf8 = [Text.UTF8Encoding]::new($false)
# Windows PowerShell 5.1 treats BOM-less UTF-8 as the active ANSI code page.
# Read explicitly as UTF-8 so provenance hashes are identical in powershell.exe
# and pwsh.exe; source comments contain non-ASCII punctuation.
$sourceNormalized = if (Test-Path -LiteralPath $source) { [regex]::Replace([IO.File]::ReadAllText($source, $utf8), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";') } else { $null }
$installedNormalized = if (Test-Path -LiteralPath $installed) { [regex]::Replace([IO.File]::ReadAllText($installed, $utf8), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";') } else { $null }
$sourceFingerprint = if ($sourceNormalized) { (([Security.Cryptography.SHA256]::Create().ComputeHash($utf8.GetBytes($sourceNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '') } else { $null }
$installedFingerprint = if ($installedNormalized) { (([Security.Cryptography.SHA256]::Create().ComputeHash($utf8.GetBytes($installedNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '') } else { $null }
[pscustomobject]@{
    repository_addon_hash = $sourceHash; installed_addon_hash = $installedHash; repository_addon_fingerprint = $sourceFingerprint; installed_addon_fingerprint = $installedFingerprint
    compiled_dll = if ($dll) { $dll.FullName } else { $null }
    compiled_dll_hash = if ($dll) { (Get-FileHash -Algorithm SHA256 -LiteralPath $dll.FullName).Hash.ToLowerInvariant() } else { $null }
    addon_provenance = if ($sourceFingerprint -and $sourceFingerprint -eq $installedFingerprint -and $dll) { "SOURCE_MATCH_COMPILE_RUNTIME_HELLO_REQUIRED" } else { "BLOCKED_ADDON_PROVENANCE" }
    live_armed = $false
} | ConvertTo-Json -Depth 3
