[CmdletBinding()]
param(
    [string]$NinjaTraderDocumentsRoot = (Join-Path ([Environment]::GetFolderPath("MyDocuments")) "NinjaTrader 8")
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$source = Join-Path $repo "ninjatrader\NinjaScript\AddOns\BeelzebubLiveExecutionAddOn.cs"
$installed = Join-Path $NinjaTraderDocumentsRoot "bin\Custom\AddOns\BeelzebubLiveExecutionAddOn.cs"
$dll = Get-ChildItem -LiteralPath (Join-Path $NinjaTraderDocumentsRoot "bin\Custom") -Recurse -File -Filter '*Beelzebub*Live*' -ErrorAction SilentlyContinue | Select-Object -First 1
$sourceHash = if (Test-Path -LiteralPath $source) { (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant() } else { $null }
$installedHash = if (Test-Path -LiteralPath $installed) { (Get-FileHash -Algorithm SHA256 -LiteralPath $installed).Hash.ToLowerInvariant() } else { $null }
$sourceNormalized = if (Test-Path -LiteralPath $source) { [regex]::Replace((Get-Content -Raw -LiteralPath $source), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";') } else { $null }
$installedNormalized = if (Test-Path -LiteralPath $installed) { [regex]::Replace((Get-Content -Raw -LiteralPath $installed), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";') } else { $null }
$sourceFingerprint = if ($sourceNormalized) { (([Security.Cryptography.SHA256]::Create().ComputeHash([Text.UTF8Encoding]::new($false).GetBytes($sourceNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '') } else { $null }
$installedFingerprint = if ($installedNormalized) { (([Security.Cryptography.SHA256]::Create().ComputeHash([Text.UTF8Encoding]::new($false).GetBytes($installedNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '') } else { $null }
[pscustomobject]@{
    repository_addon_hash = $sourceHash; installed_addon_hash = $installedHash; repository_addon_fingerprint = $sourceFingerprint; installed_addon_fingerprint = $installedFingerprint
    compiled_dll = if ($dll) { $dll.FullName } else { $null }
    compiled_dll_hash = if ($dll) { (Get-FileHash -Algorithm SHA256 -LiteralPath $dll.FullName).Hash.ToLowerInvariant() } else { $null }
    addon_provenance = if ($sourceFingerprint -and $sourceFingerprint -eq $installedFingerprint -and $dll) { "SOURCE_MATCH_COMPILE_RUNTIME_HELLO_REQUIRED" } else { "BLOCKED_ADDON_PROVENANCE" }
    live_armed = $false
} | ConvertTo-Json -Depth 3
