[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $root ".venv312\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) { throw "Missing project Python: $python" }
& $python -m compileall -q (Join-Path $root "src") (Join-Path $root "main.py") (Join-Path $root "beez_console.py")
& $python -m unittest tests.test_l3h_authority tests.test_l3h_control_center tests.test_l3h_transport tests.test_l3h_lifecycle tests.test_l3h_bootstrap tests.test_l3h_reconciliation tests.test_l3h_storage tests.test_l3h_ninjascript_source tests.test_l3h_ops_scripts -v
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& (Join-Path $PSScriptRoot "l3h_verify_install.ps1")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& (Join-Path $PSScriptRoot "l3h_status.ps1")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
exit 0
