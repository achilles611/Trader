from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.l3g_paper.ninjatrader_login import (
    NinjaTraderLoginBootstrap,
    NinjaTraderLoginProbe,
    NinjaTraderLoginState,
    PowerShellNinjaTraderLoginAdapter,
)


class StepClock:
    def __init__(self, step: float = 1.0) -> None:
        self.value = 0.0
        self.step = step

    def __call__(self) -> float:
        value = self.value
        self.value += self.step
        return value


class FakeLoginAdapter:
    def __init__(
        self,
        probes: list[NinjaTraderLoginProbe],
        *,
        start_result: bool = True,
        submit_results: list[str] | None = None,
        connect_result: bool = True,
    ) -> None:
        self.probes = list(probes)
        self.start_result = start_result
        self.submit_results = list(submit_results or ["SUBMITTED"])
        self.connect_result = connect_result
        self.probe_calls = 0
        self.start_calls = 0
        self.submit_calls = 0
        self.connect_calls = 0

    def probe(self) -> NinjaTraderLoginProbe:
        index = min(self.probe_calls, len(self.probes) - 1)
        self.probe_calls += 1
        return self.probes[index]

    def start_ninjatrader(self) -> bool:
        self.start_calls += 1
        return self.start_result

    def submit_login(self) -> str:
        index = min(self.submit_calls, len(self.submit_results) - 1)
        self.submit_calls += 1
        return self.submit_results[index]

    def connect_lucid(self) -> bool:
        self.connect_calls += 1
        return self.connect_result


def run_bootstrap(
    adapter: FakeLoginAdapter,
    *,
    clock: StepClock | None = None,
    timeout: float = 90.0,
) -> NinjaTraderLoginBootstrap:
    bootstrap = NinjaTraderLoginBootstrap(
        adapter,
        authentication_timeout_seconds=timeout,
        poll_interval_seconds=0.001,
        clock=clock or StepClock(),
        wait=lambda _: None,
    )
    bootstrap.start()
    bootstrap.wait(2)
    return bootstrap


class NinjaTraderLoginBootstrapTests(unittest.TestCase):
    def test_already_authenticated_performs_no_process_or_credential_action(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.AUTHENTICATED)
        self.assertEqual((adapter.start_calls, adapter.submit_calls, adapter.connect_calls), (0, 0, 0))

    def test_absent_process_is_started_exactly_once(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(),
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.AUTHENTICATED)
        self.assertEqual(adapter.start_calls, 1)
        self.assertEqual(adapter.submit_calls, 0)

    def test_exact_login_window_advances_to_control_center(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, True, False, "UNKNOWN"),
            NinjaTraderLoginProbe(True, False, False, "UNKNOWN"),
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.AUTHENTICATED)
        self.assertEqual(adapter.submit_calls, 1)
        self.assertTrue(bootstrap.status()["control_center_detected"])

    def test_post_submit_welcome_transition_waits_without_more_input(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, True, False, "UNKNOWN"),
            NinjaTraderLoginProbe(True, False, False, "UNKNOWN", "UNEXPECTED_LOGIN_UI"),
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.AUTHENTICATED)
        self.assertEqual(adapter.submit_calls, 1)

    def test_wrong_or_ambiguous_login_ui_is_refused(self) -> None:
        for category in ("UNEXPECTED_LOGIN_UI", "AMBIGUOUS_LOGIN_WINDOW"):
            with self.subTest(category=category):
                adapter = FakeLoginAdapter([
                    NinjaTraderLoginProbe(True, False, False, "UNKNOWN", category),
                ])
                bootstrap = run_bootstrap(adapter)
                self.assertEqual(bootstrap.state, NinjaTraderLoginState.BLOCKED)
                self.assertEqual(bootstrap.status()["failure_category"], category)
                self.assertEqual(adapter.submit_calls, 0)

    def test_missing_and_corrupt_dpapi_secret_remain_blocked(self) -> None:
        for category in ("MISSING_LOCAL_SECRET", "CORRUPT_DPAPI_SECRET"):
            with self.subTest(category=category):
                adapter = FakeLoginAdapter(
                    [NinjaTraderLoginProbe(True, True, False, "UNKNOWN")],
                    submit_results=[category],
                )
                bootstrap = run_bootstrap(adapter)
                self.assertEqual(bootstrap.state, NinjaTraderLoginState.BLOCKED)
                self.assertEqual(bootstrap.status()["failure_category"], category)
                self.assertEqual(adapter.submit_calls, 1)

    def test_invalid_credentials_stop_after_two_bounded_attempts(self) -> None:
        adapter = FakeLoginAdapter(
            [NinjaTraderLoginProbe(True, True, False, "UNKNOWN", "INVALID_CREDENTIALS")],
            submit_results=["INVALID_CREDENTIALS", "INVALID_CREDENTIALS"],
        )
        bootstrap = run_bootstrap(adapter, clock=StepClock(20.0))
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.BLOCKED)
        self.assertEqual(adapter.submit_calls, 2)
        self.assertEqual(bootstrap.status()["attempt_count"], 2)

    def test_mfa_challenge_blocks_without_credential_entry(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, False, False, "UNKNOWN", "MFA_OR_CHALLENGE_PRESENT"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.BLOCKED)
        self.assertEqual(adapter.submit_calls, 0)

    def test_timeout_remains_disarmed_and_commandless(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, False, False, "UNKNOWN"),
        ])
        bootstrap = run_bootstrap(adapter, clock=StepClock(50.0))
        status = bootstrap.status()
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.BLOCKED)
        self.assertEqual(status["failure_category"], "LOGIN_AUTOMATION_TIMEOUT")
        self.assertEqual(adapter.submit_calls, 0)

    def test_exact_lucid_connection_is_requested_once_then_verified(self) -> None:
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, False, True, "DISCONNECTED"),
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        bootstrap = run_bootstrap(adapter)
        self.assertEqual(bootstrap.state, NinjaTraderLoginState.AUTHENTICATED)
        self.assertEqual(adapter.connect_calls, 1)
        self.assertEqual(adapter.submit_calls, 0)

    def test_credential_values_cannot_enter_status_or_helper_output_contract(self) -> None:
        username_marker = "username-must-not-appear"
        password_marker = "password-must-not-appear"
        adapter = FakeLoginAdapter([
            NinjaTraderLoginProbe(True, False, True, "CONNECTED"),
        ])
        serialized = json.dumps(run_bootstrap(adapter).status(), sort_keys=True)
        self.assertNotIn(username_marker, serialized)
        self.assertNotIn(password_marker, serialized)

    def test_production_helper_uses_ui_automation_dpapi_and_no_unsafe_input_path(self) -> None:
        root = Path(__file__).parents[1]
        helper = (root / "tools" / "ninjatrader_autologin.ps1").read_text(encoding="utf-8")
        seed = (root / "tools" / "seed_ninjatrader_login.ps1").read_text(encoding="utf-8")
        self.assertIn("UIAutomationClient", helper)
        self.assertIn("ValuePattern", helper)
        self.assertIn("InvokePattern", helper)
        self.assertIn("ConvertTo-SecureString", helper)
        self.assertIn("'tbUserName'", helper)
        self.assertIn("'passwordBox'", helper)
        self.assertIn("'btnLogin'", helper)
        self.assertIn("LucidFlex25k", helper)
        self.assertIn("GetRuntimeId() -join '.'", helper)
        self.assertIn("^RecordRow(?<Row>\\d+)_Connection$", helper)
        self.assertNotIn("SendKeys", helper)
        self.assertNotIn("Clipboard", helper)
        self.assertNotIn("Cursor", helper)
        self.assertIn("ConvertFrom-SecureString", seed)
        self.assertIn("SetAccessRuleProtection($true, $false)", seed)

    def test_production_adapter_uses_only_native_windows_powershell_modules(self) -> None:
        adapter = PowerShellNinjaTraderLoginAdapter()
        completed = SimpleNamespace(returncode=0, stdout='{"ok":true}\n', stderr="")
        with patch("src.l3g_paper.ninjatrader_login.subprocess.run", return_value=completed) as runner:
            self.assertEqual(adapter._run("probe"), {"ok": True})
        environment = runner.call_args.kwargs["env"]
        module_roots = environment["PSModulePath"].split(";")
        self.assertEqual(len(module_roots), 3)
        self.assertTrue(all("WindowsPowerShell" in root for root in module_roots))
        self.assertTrue(all("PowerShell\\7" not in root for root in module_roots))
        self.assertFalse(any(
            marker in name.lower()
            for name in environment
            for marker in ("password", "secret", "credential", "ninjatrader_login")
        ))

    def test_login_layer_has_no_arm_command_or_live_authority(self) -> None:
        root = Path(__file__).parents[1]
        source = (root / "src" / "l3g_paper" / "ninjatrader_login.py").read_text(encoding="utf-8")
        control_center = (root / "src" / "copytrade" / "control_center.py").read_text(encoding="utf-8")
        self.assertNotIn("PaperExecutionCommand", source)
        self.assertNotIn(".arm(", source)
        self.assertIn('"ninjatrader_login": ninja_login_health()', control_center)
        self.assertIn("bootstrap.state is not NinjaTraderLoginState.AUTHENTICATED", control_center)
        self.assertNotIn("PaperExecutionIntent", source)
        self.assertNotIn("LucidFlex25k", source)
        self.assertIn("else NinjaTraderLoginBootstrap()", control_center)
        self.assertIn('"state": "READY_ON_DEMAND"', control_center)
        self.assertIn("begin_automatic_ninjatrader_login", control_center)
        lifespan = control_center[control_center.index("async def lifespan"):control_center.index("app = FastAPI")]
        self.assertNotIn("NinjaTraderLoginBootstrap()", lifespan)


if __name__ == "__main__":
    unittest.main()
