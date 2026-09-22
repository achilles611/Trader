"""Execute production PowerShell guards with no native, socket or ledger access."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
COMMISSION = ROOT / "tools" / "commission_ninjatrader_observer.ps1"
PWSH = shutil.which("pwsh")
pytestmark = pytest.mark.skipif(PWSH is None, reason="PowerShell 7 required")


def run_guards(body: str) -> dict:
    path = str(COMMISSION).replace("'", "''")
    script = r"""
$ErrorActionPreference = 'Stop'
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile('__PATH__', [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw "Parser errors: $errors" }
foreach ($name in @('Get-ReceiverReadiness', 'Assert-FrozenCommissioningSource', 'Assert-ReceiverFirstBoundary', 'Assert-SingleSourceLiveInstall')) {
    $fn = $ast.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name}, $true)
    . ([scriptblock]::Create($fn.Extent.Text))
}
$repositoryRoot = 'TEST_ONLY'
$frozenGitSha = 'a' * 40; $frozenGitTree = 'b' * 40
$frozenSourceHashes = @{'TEST_SOURCE' = 'HASH'}
$ExpectedInstrument = 'MNQ DEC26'
$Port = 8090
$script:gitSha = $frozenGitSha; $script:gitTree = $frozenGitTree
$script:hash = 'HASH'; $script:runtimeSha = $frozenGitSha
$script:changeDuringRead = $false
$script:nativeRunning = $false
$script:dirtySource = $false
function git {
    $global:LASTEXITCODE = 0
    if ($args -contains 'status') { if ($script:dirtySource) { return ' M TEST_SOURCE' }; return }
    if ($args -contains 'show') { return $script:gitTree }
    return $script:gitSha
}
function Get-FileHash { return @{Hash=$script:hash} }
function Get-Process { if ($script:nativeRunning) { return @{Id=999} } }
function Invoke-RestMethod { return @{git_sha=$script:runtimeSha} }
$script:paper = @{
    market_instrument='MNQ DEC26'
    market_observer=@{state='LISTENING'; host='127.0.0.1'; port=48135; error=$null}
    pong_collection=@{
        state='COLLECTING'; error=$null; instrument=@{native='MNQ DEC26'}
        collector=@{collector_id='test'; thread_alive=$true; expected_source_fingerprint=('c'*64)}
    }
}
function Get-PaperStatus {
    if ($script:changeDuringRead) { $script:gitSha = 'd'*40 }
    return $script:paper
}
""".replace("__PATH__", path) + "\n" + body
    result = subprocess.run(
        [PWSH, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True, capture_output=True, check=False, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_receiver_first_positive():
    result = run_guards("Assert-ReceiverFirstBoundary ('c'*64) | ConvertTo-Json")
    assert result["ready"] is True


@pytest.mark.parametrize("fault,reason", [
    ("$script:paper.market_observer.state='FAILED'", "Listener and independent collector"),
    ("$script:paper.market_observer.host='0.0.0.0'", "Listener and independent collector"),
    ("$script:paper.market_observer.port=48136", "Listener and independent collector"),
    ("$script:paper.market_observer.error='fault'", "Listener and independent collector"),
    ("$script:paper.pong_collection.error='fault'", "Listener and independent collector"),
    ("$script:paper.pong_collection.collector.thread_alive=$false", "Listener and independent collector"),
    ("$script:paper.pong_collection.collector.collector_id=''", "Listener and independent collector"),
    ("$script:paper.pong_collection.collector.expected_source_fingerprint='wrong'", "Listener and independent collector"),
    ("$script:paper.pong_collection.instrument.native='MNQ SEP26'", "Listener and independent collector"),
    ("$script:paper.market_instrument='MNQ SEP26'", "Listener and independent collector"),
    ("$script:gitSha='d'*40", "COMMISSIONING_CHECKOUT_CHANGED"),
    ("$script:gitTree='d'*40", "COMMISSIONING_TREE_CHANGED"),
    ("$script:hash='CHANGED'", "COMMISSIONING_SOURCE_CHANGED"),
    ("$script:dirtySource=$true", "COMMISSIONING_SOURCE_DIFFERS_FROM_HEAD"),
    ("$script:runtimeSha='d'*40", "COMMISSIONING_RUNTIME_GIT_MISMATCH"),
    ("$script:changeDuringRead=$true", "COMMISSIONING_CHECKOUT_CHANGED"),
])
def test_receiver_first_fault_refuses_before_side_effect(fault, reason):
    result = run_guards(fault + r"""
$writes=0
try {
    $null=Assert-ReceiverFirstBoundary ('c'*64)
    $writes++
    throw 'UNEXPECTED_ACCEPTANCE'
} catch { @{writes=$writes; reason=$_.Exception.Message}|ConvertTo-Json }
""")
    assert result["writes"] == 0
    assert reason in result["reason"]


def test_multi_source_install_refuses_running_native():
    result = run_guards(r"""
$script:nativeRunning=$true
try { Assert-SingleSourceLiveInstall 2; throw 'UNEXPECTED_ACCEPTANCE' }
catch { @{reason=$_.Exception.Message}|ConvertTo-Json }
""")
    assert "COMMISSIONING_MULTI_SOURCE_LIVE_INSTALL_REFUSED" in result["reason"]


def test_prepare_source_only_refuses_before_native_file_access(tmp_path):
    nonexistent_native = tmp_path / "native-must-not-be-created"
    result = subprocess.run(
        [PWSH, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(COMMISSION),
         "-PrepareSourceOnly", "-InstallSource", "-NinjaTraderDocumentsRoot", str(nonexistent_native)],
        text=True, capture_output=True, timeout=30,
    )
    assert result.returncode != 0
    assert "PREPARE_SOURCE_ONLY_REFUSED_RECEIVER_FIRST_REQUIRED" in result.stderr
    assert not nonexistent_native.exists()


def test_every_actual_install_is_guarded_and_indicator_precedes_addon():
    # Check the executable call sites too: function-only tests cannot prove the
    # installer invokes the guard. No mocked test may write installed sources.
    source = COMMISSION.read_text(encoding="utf-8")
    calls = [line.strip() for line in source.splitlines()]
    indices = [i for i, line in enumerate(calls) if line.startswith("Install-ObserverSource -Source")]
    assert len(indices) == 2
    for index in indices:
        assert calls[index-2:index] == [
            "$receiverReadiness = Assert-ReceiverFirstBoundary $expectedFingerprint",
            "Assert-SingleSourceLiveInstall $changedFiles",
        ]
    assert "$repositoryIndicator" in calls[indices[0]]
    assert "$repositoryAddOn" in calls[indices[1]]
    assert source.index("if ($InstallSource -or $RequireDistinctProducer)") < source.index("$validationText =")
