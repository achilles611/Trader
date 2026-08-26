[CmdletBinding()]
param(
    [string]$SourceAuditRoot = "C:\Users\atlas\Documents\Trader\runtime\audit",
    [string]$DestinationAuditRoot = "N:\Beelzebub\runtime\audit"
)

$ErrorActionPreference = "Stop"
# Maintenance only. This is copy-and-verify; it never removes or rewrites the
# C: source evidence. Stop the backend first so no verification is in progress.
$source = (Resolve-Path -LiteralPath $SourceAuditRoot -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath (Join-Path $source "ledger-verification-latest.json"))) {
    throw "Source audit root has no terminal verifier artifact: $source"
}
$destination = [IO.Path]::GetFullPath($DestinationAuditRoot)
New-Item -ItemType Directory -Force -Path $destination | Out-Null

$copied = 0
foreach ($file in Get-ChildItem -LiteralPath $source -File -Recurse) {
    $relative = $file.FullName.Substring($source.Length).TrimStart('\', '/')
    $target = Join-Path $destination $relative
    $targetDirectory = Split-Path -Parent $target
    New-Item -ItemType Directory -Force -Path $targetDirectory | Out-Null
    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash
    if (Test-Path -LiteralPath $target) {
        $targetHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash
        if ($targetHash -ne $sourceHash) { throw "Refusing to overwrite differing audit artifact: $target" }
    } else {
        Copy-Item -LiteralPath $file.FullName -Destination $target -ErrorAction Stop
        $targetHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash
        if ($targetHash -ne $sourceHash) { throw "Copied artifact hash mismatch: $target" }
        $copied++
    }
}
Write-Output "Audit migration verified. Copied $copied artifact(s); source remains untouched."
