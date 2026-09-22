from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
START_SCRIPT = ROOT / "scripts" / "start_beezconsole.ps1"
PWSH = shutil.which("pwsh")


class BeezConsoleFrozenLaunchTests(unittest.TestCase):
    maxDiff = None

    def run_helpers(self, body: str) -> subprocess.CompletedProcess[str]:
        if PWSH is None:
            self.skipTest("pwsh is required for executable PowerShell helper tests")
        environment = dict(os.environ)
        environment["BEELZEBUB_START_SCRIPT_UNDER_TEST"] = str(START_SCRIPT)
        bootstrap = r"""
$ErrorActionPreference = 'Stop'
$source = [IO.File]::ReadAllText($env:BEELZEBUB_START_SCRIPT_UNDER_TEST)
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseInput(
    $source,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -ne 0) {
    throw ($parseErrors | ForEach-Object { $_.Message } | Out-String)
}
$helperNames = @(
    'Get-CheckoutIdentity',
    'Assert-CheckoutIdentity',
    'Assert-RuntimeSourcesMatchHead',
    'Get-LaunchSourceIdentity',
    'Assert-LaunchSourceIdentity'
)
$helpers = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and
        $helperNames -contains $node.Name
}, $true))
if ($helpers.Count -ne $helperNames.Count) {
    throw "Expected $($helperNames.Count) guard helpers; found $($helpers.Count)."
}
foreach ($helper in $helpers) {
    . ([scriptblock]::Create($helper.Extent.Text))
}
"""
        command = base64.b64encode((bootstrap + "\n" + body).encode("utf-16le")).decode("ascii")
        return subprocess.run(
            [PWSH, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", command],
            text=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            env=environment,
            check=False,
        )

    def test_start_script_parses_and_exposes_guard_helpers(self) -> None:
        result = self.run_helpers("Write-Output 'PARSED'\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PARSED", result.stdout)

    def test_checkout_identity_helper_uses_exact_head_and_tree(self) -> None:
        result = self.run_helpers(
            r"""
function global:git {
    if ($args[-1] -eq 'HEAD^{commit}') { $global:LASTEXITCODE = 0; return ('a' * 40) }
    if ($args[-1] -eq 'HEAD^{tree}') { $global:LASTEXITCODE = 0; return ('b' * 40) }
    $global:LASTEXITCODE = 9
}
$identity = Get-CheckoutIdentity -RepositoryRoot 'C:\mock-repository'
$identity | ConvertTo-Json -Compress
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(payload, {"head": "a" * 40, "tree": "b" * 40})

    def test_checkout_identity_guard_rejects_head_or_tree_change(self) -> None:
        for field in ("head", "tree"):
            with self.subTest(field=field):
                actual_head = "c" * 40 if field == "head" else "a" * 40
                actual_tree = "d" * 40 if field == "tree" else "b" * 40
                result = self.run_helpers(
                    f"""
$expected = [pscustomobject]@{{ head = ('a' * 40); tree = ('b' * 40) }}
$actual = [pscustomobject]@{{ head = '{actual_head}'; tree = '{actual_tree}' }}
Assert-CheckoutIdentity -Actual $actual -Expected $expected -Phase 'test boundary'
"""
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("checkout Git identity mismatch", result.stderr)
                self.assertIn("No automatic stop or restart was attempted", result.stderr)

    def test_launch_source_guard_rejects_a_changed_candidate_hash(self) -> None:
        result = self.run_helpers(
            r"""
$expected = [pscustomobject]@{
    beez_console_sha256 = ('1' * 64)
    main_sha256 = ('2' * 64)
}
$actual = [pscustomobject]@{
    beez_console_sha256 = ('1' * 64)
    main_sha256 = ('3' * 64)
}
Assert-LaunchSourceIdentity -Actual $actual -Expected $expected -Phase 'test boundary'
"""
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("launch source changed", result.stderr)
        self.assertIn("No automatic stop or restart was attempted", result.stderr)

    def test_guard_order_freezes_before_launch_and_rechecks_after_binding(self) -> None:
        script = START_SCRIPT.read_text(encoding="utf-8")
        before = script.index("$beforeLaunchIdentity = Get-CheckoutIdentity")
        source_before = script.index("$frozenLaunchSourceIdentity = Get-LaunchSourceIdentity")
        launch = script.index("$process = Start-Process")
        after_launcher = script.index("$afterLauncherIdentity = Get-CheckoutIdentity")
        binding = script.index("$binding = Invoke-RestMethod")
        after_binding = script.index("$afterBindingIdentity = Get-CheckoutIdentity")
        binding_match = script.index("$binding.git_sha")
        self.assertLess(before, source_before)
        self.assertLess(source_before, launch)
        self.assertLess(launch, after_launcher)
        self.assertLess(after_launcher, binding)
        self.assertLess(binding, after_binding)
        self.assertLess(after_binding, binding_match)
        self.assertIn("Expected launch identity requires -ExpectedGitSha and -ExpectedGitTree together.", script)
        self.assertIn("status --porcelain=v1 --untracked-files=all", script)
        self.assertIn("beez_console.py main.py src", script)
        self.assertIn("-NotePropertyName launch_guard", script)


if __name__ == "__main__":
    unittest.main()
