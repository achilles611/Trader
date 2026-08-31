[CmdletBinding()]
param(
    [string]$NinjaTraderDocumentsRoot = (Join-Path ([Environment]::GetFolderPath("MyDocuments")) "NinjaTrader 8"),
    [switch]$InstallSource
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$source = Join-Path $repo "ninjatrader\NinjaScript\AddOns\BeelzebubLiveExecutionAddOn.cs"
$target = Join-Path $NinjaTraderDocumentsRoot "bin\Custom\AddOns\BeelzebubLiveExecutionAddOn.cs"
if (-not (Test-Path -LiteralPath $source)) { throw "L3H AddOn source is missing: $source" }
if ($InstallSource) {
    $targetDirectory = Split-Path -Parent $target
    New-Item -ItemType Directory -Force -Path $targetDirectory | Out-Null
    if (Test-Path -LiteralPath $target) {
        $backup = "$target.l3h-before-install-$(Get-Date -Format 'yyyyMMddTHHmmssZ').bak"
        Copy-Item -LiteralPath $target -Destination $backup -ErrorAction Stop
    }
    Copy-Item -LiteralPath $source -Destination $target -Force -ErrorAction Stop
    # The runtime reports a source fingerprint whose value is excluded from the
    # fingerprint input itself. This prevents a self-referential hash while
    # preserving repository/installed parity under a controlled substitution.
    $template = Get-Content -Raw -LiteralPath $source
    $normalized = [regex]::Replace($template, 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";')
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($normalized)
    $fingerprint = ([Security.Cryptography.SHA256]::Create().ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ''
    $installed = [regex]::Replace((Get-Content -Raw -LiteralPath $target), 'private const string SourceFingerprint = "[^"]+";', "private const string SourceFingerprint = `"$fingerprint`";")
    [IO.File]::WriteAllText($target, $installed, [Text.UTF8Encoding]::new($false))
}
$sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
$sourceNormalized = [regex]::Replace((Get-Content -Raw -LiteralPath $source), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";')
$sourceFingerprint = (([Security.Cryptography.SHA256]::Create().ComputeHash([Text.UTF8Encoding]::new($false).GetBytes($sourceNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '')
$installedHash = if (Test-Path -LiteralPath $target) { (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash.ToLowerInvariant() } else { $null }
$installedNormalized = if (Test-Path -LiteralPath $target) { [regex]::Replace((Get-Content -Raw -LiteralPath $target), 'private const string SourceFingerprint = "[^"]+";', 'private const string SourceFingerprint = "SOURCE_FINGERPRINT_PLACEHOLDER";') } else { $null }
$installedFingerprint = if ($installedNormalized) { (([Security.Cryptography.SHA256]::Create().ComputeHash([Text.UTF8Encoding]::new($false).GetBytes($installedNormalized)) | ForEach-Object { $_.ToString("x2") }) -join '') } else { $null }
[pscustomobject]@{
    source = $source; source_sha256 = $sourceHash; source_fingerprint = $sourceFingerprint; installed_source = $target; installed_sha256 = $installedHash
    parity = if ($sourceFingerprint -eq $installedFingerprint) { "MATCH" } else { "MISMATCH_OR_NOT_INSTALLED" }
    compile_status = "NINJATRADER_VISIBLE_COMPILE_REQUIRED"
    live_armed = $false
    next_action = "Open NinjaTrader's NinjaScript editor and compile visibly. Do not arm or submit an order."
} | ConvertTo-Json -Depth 3
