[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$TargetEpoch,
    [Parameter(Mandatory = $true)][string]$OperatorId,
    [string]$LedgerPath = "N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3",
    [string]$AuditRoot = "N:\Beelzebub\runtime\audit"
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $root ".venv312\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) { throw "Missing required project Python: $python" }
Write-Warning "Maintenance only: adoption changes only legacy metadata, never ledger rows. Stop BeezConsole and verify a maintenance window before continuing."
$confirm = Read-Host "Type ADOPT to confirm the maintenance window"
if ($confirm -ne "ADOPT") { throw "Legacy epoch adoption was not confirmed." }
& $python -c "from src.l3g_paper.ledger import adopt_legacy_epoch; import json; print(json.dumps(adopt_legacy_epoch(r'$LedgerPath', r'$AuditRoot', target_epoch=r'$TargetEpoch', operator_id=r'$OperatorId', maintenance_window_confirmed=True), indent=2))"
if ($LASTEXITCODE -ne 0) { throw "Legacy epoch adoption failed." }
