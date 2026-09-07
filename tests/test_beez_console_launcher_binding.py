from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import beez_console


GIT_SHA = "a" * 40
LEDGER_IDENTITY = "l3g-ledger-" + "b" * 32


def project(root: Path) -> tuple[Path, Path]:
    python = root / ".venv312" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.touch()
    entrypoint = root / "main.py"
    entrypoint.touch()
    return python, entrypoint


def binding(root: Path, *, operation_id: str | None = None) -> beez_console.LaunchBinding:
    profile = beez_console.resolve_paper_profile("BEELZEBUB_FIVE_MINUTE_BIAS_V1")
    return beez_console.LaunchBinding(
        runtime_root=(root / "runtime").resolve(),
        ledger_path=(root / "run" / "hot" / "lane_iii_paper.sqlite3").resolve(),
        audit_root=(root / "run" / "audit").resolve(),
        ledger_epoch="L3G-PAPER-EPOCH-FIVE-MINUTE-TEST",
        profile=profile.selection_key,
        git_sha=GIT_SHA,
        python=(root / ".venv312" / "Scripts" / "python.exe").resolve(),
        entry_profile=profile.policy.entry_profile,
        entry_profile_version=profile.policy.entry_profile_version,
        paper_policy_hash=profile.policy.configuration_hash,
        risk_profile_hash=profile.risk.configuration_hash,
        source="REMEMBERED_ESTABLISHED_RUN",
        ledger_identity=LEDGER_IDENTITY,
        operation_id=operation_id or "profile-switch-" + "c" * 32,
    )


def runtime_binding(expected: beez_console.LaunchBinding) -> dict[str, object]:
    return {
        **expected.expected_runtime_binding(),
        "pid": expected.expected_pid if expected.expected_pid is not None else 1234,
    }


def write_operation(
    expected: beez_console.LaunchBinding,
    operation_id: str,
    *,
    git_sha: str,
    stage: str,
) -> tuple[dict[str, object], dict[str, object]]:
    target = beez_console.resolve_paper_profile("BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2")
    operation_root = expected.runtime_root / "profile-switch" / "operations" / operation_id
    operation_root.mkdir(parents=True)
    manifest_path = operation_root / "manifest.json"
    manifest: dict[str, object] = {
        "schema": beez_console.PROFILE_SWITCH_SCHEMA,
        "operation_id": operation_id,
        "request_id": "request-prior-terminal",
        "created_at": "2026-09-07T00:00:00Z",
        "parent_pid": 1234,
        "project_root": str(expected.python.parents[2]),
        "python_executable": str(expected.python),
        "git_sha": git_sha,
        "current_profile": expected.profile,
        "target_profile": target.selection_key,
        "paper_policy_hash": target.policy.configuration_hash,
        "risk_profile_hash": target.risk.configuration_hash,
        "ledger_path": str(operation_root / "target" / "hot" / "lane_iii_paper.sqlite3"),
        "ledger_epoch": "L3G-PAPER-EPOCH-PRIOR-TERMINAL",
        "audit_root": str(operation_root / "target" / "audit"),
        "runtime_root": str(expected.runtime_root),
        "paper_only": True,
        "live_capital": "DENIED",
        "perpetual_startup_seed_required": True,
        "perpetual_startup_seed_path": str(operation_root / "perpetual-startup-seed.json"),
        "perpetual_startup_seed_proof_path": str(operation_root / "perpetual-startup-seed-proof.json"),
        "source_ledger_path": str(expected.ledger_path),
        "source_audit_root": str(expected.audit_root),
        "source_ledger_identity": expected.ledger_identity or "l3g-ledger-" + "a" * 32,
        "source_ledger_epoch": expected.ledger_epoch,
    }
    manifest["manifest_sha256"] = sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    state = {
        "schema": beez_console.PROFILE_SWITCH_SCHEMA,
        "operation_id": operation_id,
        "current_profile": expected.profile,
        "target_profile": target.selection_key,
        "manifest_path": str(manifest_path),
        "stage": stage,
        "target_pid": 4321,
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (operation_root / "state.json").write_text(json.dumps(state), encoding="utf-8")
    return manifest, state


class BeezConsoleLauncherBindingTests(unittest.TestCase):
    def test_direct_launch_uses_validated_remembered_selection_and_discards_stale_environment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            runtime_root = (root / "runtime").resolve()
            established = {
                "profile": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                "operation_id": "profile-switch-" + "c" * 32,
                "ledger_path": str((root / "five-minute" / "hot" / "lane_iii_paper.sqlite3").resolve()),
                "ledger_identity": LEDGER_IDENTITY,
                "ledger_epoch": "L3G-PAPER-EPOCH-FIVE-MINUTE-REMEMBERED",
                "audit_root": str((root / "five-minute" / "audit").resolve()),
            }
            stale = {
                "BEELZEBUB_PROFILE_SWITCH_ROOT": str(runtime_root),
                "BEELZEBUB_L3G_PAPER_LEDGER": "stale-ledger",
                "beelzebub_l3g_paper_profile": "BEELZEBUB_SCALPER_V2",
                "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "stale-epoch",
                "BEELZEBUB_LEDGER_AUDIT_ROOT": "stale-audit",
                "BEELZEBUB_PROFILE_SWITCH_OPERATION": "stale-operation",
                "BEELZEBUB_RISK_CONTINUITY_PATH": "stale-continuity",
                "BEELZEBUB_GIT_SHA": "d" * 40,
                "BEELZEBUB_HOME": "stale-home",
                "COPYTRADE_MODE": "live",
                "COPYTRADE_LIVE_ENABLED": "true",
                "COPYTRADE_CONFIG": "stale-config",
                "KEEP_ME": "yes",
            }
            with (
                patch("beez_console.checkout_git_sha", return_value=GIT_SHA),
                patch("beez_console._remembered_selection_without_ledger", return_value=established),
                patch("beez_console.remembered_profile_selection", return_value=established) as remembered,
            ):
                resolved = beez_console.resolve_launch_binding(root, environment=stale)

            remembered.assert_called_once_with(runtime_root, git_sha=GIT_SHA)
            self.assertEqual(resolved.profile, "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
            self.assertEqual(resolved.ledger_path, Path(established["ledger_path"]))
            child = beez_console.server_environment(resolved, stale)
            self.assertEqual(child["KEEP_ME"], "yes")
            self.assertEqual(child["BEELZEBUB_PROFILE_SWITCH_ROOT"], str(runtime_root))
            self.assertEqual(child["BEELZEBUB_GIT_SHA"], GIT_SHA)
            self.assertFalse(any(key.upper().startswith("BEELZEBUB_L3G_PAPER_") for key in child))
            self.assertNotIn("BEELZEBUB_LEDGER_AUDIT_ROOT", child)
            self.assertNotIn("BEELZEBUB_PROFILE_SWITCH_OPERATION", child)
            self.assertNotIn("BEELZEBUB_RISK_CONTINUITY_PATH", child)
            self.assertNotIn("BEELZEBUB_HOME", child)
            self.assertNotIn("COPYTRADE_MODE", child)
            self.assertNotIn("COPYTRADE_LIVE_ENABLED", child)
            self.assertNotIn("COPYTRADE_CONFIG", child)

    def test_direct_launch_has_no_scalper_fallback_without_established_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            with (
                patch("beez_console.checkout_git_sha", return_value=GIT_SHA),
                patch("beez_console._remembered_selection_without_ledger", return_value=None),
            ):
                with self.assertRaisesRegex(RuntimeError, "no validated established"):
                    beez_console.resolve_launch_binding(
                        root,
                        environment={"BEELZEBUB_PROFILE_SWITCH_ROOT": str(root / "runtime")},
                    )

    def test_invalid_remembered_selection_is_not_downgraded_to_a_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            with (
                patch("beez_console.checkout_git_sha", return_value=GIT_SHA),
                patch("beez_console._remembered_selection_without_ledger", return_value={
                    "profile": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                }),
                patch(
                    "beez_console.remembered_profile_selection",
                    side_effect=RuntimeError("PROFILE_SELECTION_ESTABLISHED_LEDGER_IDENTITY_MISMATCH"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "LEDGER_IDENTITY_MISMATCH"):
                    beez_console.resolve_launch_binding(
                        root,
                        environment={"BEELZEBUB_PROFILE_SWITCH_ROOT": str(root / "runtime")},
                    )

    def test_complete_explicit_maintenance_binding_is_preserved_without_stale_inheritance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            ledger = root / "explicit" / "hot" / "lane_iii_paper.sqlite3"
            audit = root / "explicit" / "audit"
            argv = (
                "--maintenance-ledger-path", str(ledger),
                "--maintenance-audit-root", str(audit),
                "--maintenance-ledger-epoch", "L3G-PAPER-EPOCH-EXPLICIT-TEST",
                "--maintenance-paper-profile", "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            )
            with (
                patch("beez_console.checkout_git_sha", return_value=GIT_SHA),
                patch("beez_console.remembered_profile_selection") as remembered,
            ):
                resolved = beez_console.resolve_launch_binding(
                    root,
                    argv=argv,
                    environment={"BEELZEBUB_L3G_PAPER_PROFILE": "BEELZEBUB_SCALPER_V2"},
                )
            remembered.assert_not_called()
            child = beez_console.server_environment(
                resolved,
                {"BEELZEBUB_L3G_PAPER_PROFILE": "BEELZEBUB_SCALPER_V2"},
            )
            self.assertEqual(resolved.source, "EXPLICIT_MAINTENANCE")
            self.assertEqual(child["BEELZEBUB_L3G_PAPER_LEDGER"], str(ledger.resolve()))
            self.assertEqual(child["BEELZEBUB_LEDGER_AUDIT_ROOT"], str(audit.resolve()))
            self.assertEqual(child["BEELZEBUB_L3G_PAPER_PROFILE"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
            self.assertEqual(child["BEELZEBUB_L3G_PAPER_LEDGER_EPOCH"], "L3G-PAPER-EPOCH-EXPLICIT-TEST")

    def test_partial_explicit_maintenance_binding_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            with patch("beez_console.checkout_git_sha", return_value=GIT_SHA):
                with self.assertRaisesRegex(RuntimeError, "requires ledger path, audit root"):
                    beez_console.resolve_launch_binding(
                        root,
                        argv=("--maintenance-paper-profile", "BEELZEBUB_FIVE_MINUTE_BIAS_V1"),
                    )

    def test_relative_explicit_maintenance_paths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            with patch("beez_console.checkout_git_sha", return_value=GIT_SHA):
                with self.assertRaisesRegex(RuntimeError, "must be absolute"):
                    beez_console.resolve_launch_binding(
                        root,
                        argv=(
                            "--maintenance-ledger-path", "relative-ledger.sqlite3",
                            "--maintenance-audit-root", "relative-audit",
                            "--maintenance-ledger-epoch", "L3G-PAPER-EPOCH-RELATIVE-TEST",
                            "--maintenance-paper-profile", "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                        ),
                    )

    def test_existing_listener_resolution_does_not_open_remembered_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            expected = binding(root)
            with (
                patch("beez_console.project_root", return_value=root),
                patch("beez_console.resolve_launch_binding", return_value=expected) as resolve_binding,
                patch("beez_console.port_is_open", return_value=True),
                patch("beez_console.assert_remembered_launch_is_not_competing"),
                patch("beez_console.fetch_runtime_binding", return_value=runtime_binding(expected)),
                patch("beez_console.wait_for_server"),
                patch("beez_console.brave_path", return_value=root / "brave.exe"),
                patch("beez_console.open_brave"),
                patch("beez_console.remembered_profile_selection") as ledger_validation,
            ):
                self.assertEqual(beez_console.main(), 0)
            ledger_validation.assert_not_called()
            self.assertFalse(resolve_binding.call_args.kwargs["validate_remembered_evidence"])

    def test_runtime_binding_requires_exact_identity_not_generic_http_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = binding(Path(directory))
            actual = runtime_binding(expected)
            beez_console.validate_runtime_binding(actual, expected)
            actual["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
            with self.assertRaisesRegex(RuntimeError, "entry_profile_version"):
                beez_console.validate_runtime_binding(actual, expected)

    def test_runtime_binding_requires_a_real_process_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = binding(Path(directory))
            actual = runtime_binding(expected)
            actual["pid"] = True
            with self.assertRaisesRegex(RuntimeError, "pid"):
                beez_console.validate_runtime_binding(actual, expected)

    def test_runtime_binding_requires_remembered_selection_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = binding(Path(directory))
            actual = runtime_binding(expected)
            actual["profile_selection_source"] = "PROFILE_SWITCH_MANIFEST"
            beez_console.validate_runtime_binding(actual, expected)
            actual["profile_selection_source"] = "EXPLICIT_OR_DEFAULT"
            with self.assertRaisesRegex(RuntimeError, "profile_selection_source"):
                beez_console.validate_runtime_binding(actual, expected)

    def test_paper_authority_is_exact_sim101_one_contract_and_live_denied(self) -> None:
        paper = dict(beez_console._PAPER_AUTHORITY)
        beez_console.validate_paper_authority(paper)
        paper["maximum_quantity"] = 2
        with self.assertRaisesRegex(RuntimeError, "maximum_quantity"):
            beez_console.validate_paper_authority(paper)
        paper["maximum_quantity"] = True
        with self.assertRaisesRegex(RuntimeError, "maximum_quantity"):
            beez_console.validate_paper_authority(paper)

    def test_wait_rejects_a_mismatched_existing_listener_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = binding(Path(directory))
            wrong = runtime_binding(expected)
            wrong["ledger_epoch"] = "L3G-PAPER-EPOCH-WRONG"
            with (
                patch("beez_console.fetch_runtime_binding", return_value=wrong),
                patch("beez_console.fetch_paper_status", return_value=dict(beez_console._PAPER_AUTHORITY)),
            ):
                with self.assertRaisesRegex(RuntimeError, "ledger_epoch"):
                    beez_console.wait_for_server(None, expected, timeout_seconds=1)

    def test_wait_rejects_pid_replacement_after_paper_authority_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = replace(binding(Path(directory)), expected_pid=1234)
            matching = runtime_binding(expected)
            replacement = {**matching, "pid": 4322}
            with (
                patch(
                    "beez_console.fetch_runtime_binding",
                    side_effect=[matching, replacement],
                ),
                patch(
                    "beez_console.fetch_paper_status",
                    return_value=dict(beez_console._PAPER_AUTHORITY),
                ) as paper_status,
            ):
                with self.assertRaisesRegex(RuntimeError, "pid"):
                    beez_console.wait_for_server(None, expected, timeout_seconds=1)
            paper_status.assert_called_once_with()

    def test_wait_pins_first_ledger_identity_for_explicit_launch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = replace(
                binding(Path(directory)),
                ledger_identity=None,
                expected_pid=1234,
            )
            first = {
                **runtime_binding(expected),
                "ledger_identity": "l3g-ledger-" + "1" * 32,
            }
            replacement = {
                **first,
                "ledger_identity": "l3g-ledger-" + "2" * 32,
            }
            with (
                patch(
                    "beez_console.fetch_runtime_binding",
                    side_effect=[first, replacement],
                ),
                patch(
                    "beez_console.fetch_paper_status",
                    return_value=dict(beez_console._PAPER_AUTHORITY),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "ledger_identity"):
                    beez_console.wait_for_server(None, expected, timeout_seconds=1)

    def test_active_new_handoff_blocks_a_competing_direct_launch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = binding(root)
            new_operation = "profile-switch-" + "d" * 32
            established = {
                "profile": expected.profile,
                "operation_id": expected.operation_id,
                "ledger_path": str(expected.ledger_path),
                "ledger_identity": expected.ledger_identity,
                "ledger_epoch": expected.ledger_epoch,
                "audit_root": str(expected.audit_root),
                "git_sha": expected.git_sha,
            }
            selection = {
                "established": established,
                "requested": {
                    "profile": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
                    "request_id": "request-new-target",
                    "operation_id": new_operation,
                    "requested_at": "2026-09-07T00:00:00Z",
                },
            }
            established_manifest = {
                "operation_id": expected.operation_id,
                "target_profile": expected.profile,
                "ledger_path": str(expected.ledger_path),
                "audit_root": str(expected.audit_root),
                "ledger_epoch": expected.ledger_epoch,
            }
            requested_manifest = {
                "operation_id": new_operation,
                "request_id": "request-new-target",
                "target_profile": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
                "created_at": "2026-09-07T00:00:00Z",
            }
            with (
                patch("beez_console.validated_profile_selection", return_value=selection),
                patch(
                    "beez_console._validated_operation",
                    side_effect=[
                        (established_manifest, {"stage": "RUNNING"}),
                        (requested_manifest, {"stage": "STARTING_TARGET"}),
                    ],
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "will not compete"):
                    beez_console.assert_remembered_launch_is_not_competing(expected)

    def test_exact_existing_requested_target_is_adopted_without_competing_launch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            established = binding(root)
            operation_id = "profile-switch-" + "d" * 32
            manifest, state = write_operation(
                established, operation_id, git_sha=GIT_SHA,
                stage="TARGET_ACTIVE_FLAT_BLOCKED",
            )
            requested = {
                "profile": manifest["target_profile"],
                "request_id": manifest["request_id"],
                "operation_id": operation_id,
                "requested_at": manifest["created_at"],
            }
            profile = beez_console.resolve_paper_profile(str(manifest["target_profile"]))
            actual = {
                "ledger": manifest["ledger_path"],
                "audit": manifest["audit_root"],
                "control_center": "127.0.0.1:8090",
                "python": str(established.python),
                "git_sha": GIT_SHA,
                "entry_profile": profile.policy.entry_profile,
                "entry_profile_version": profile.policy.entry_profile_version,
                "paper_policy_hash": profile.policy.configuration_hash,
                "risk_profile_hash": profile.risk.configuration_hash,
                "ledger_epoch": manifest["ledger_epoch"],
                "ledger_identity": "l3g-ledger-" + "e" * 32,
                "profile_selection_source": "PROFILE_SWITCH_MANIFEST",
                "pid": 4321,
            }
            selection = {
                "established": {"profile": established.profile},
                "requested": requested,
            }
            with (
                patch("beez_console.validated_profile_selection", return_value=selection),
                patch("beez_console._validated_operation", return_value=(manifest, state)),
                patch("beez_console.fetch_runtime_binding", return_value=actual),
                patch("beez_console.fetch_paper_status", return_value=dict(beez_console._PAPER_AUTHORITY)),
            ):
                adopted = beez_console.binding_for_existing_listener(root, established)

            self.assertEqual(adopted.source, "REQUESTED_TARGET_EXISTING_LISTENER")
            self.assertEqual(adopted.profile, profile.selection_key)
            self.assertEqual(adopted.operation_id, operation_id)
            self.assertEqual(adopted.ledger_identity, actual["ledger_identity"])
            self.assertEqual(adopted.expected_pid, 4321)

            replacement = {**actual, "pid": 4322}
            with (
                patch("beez_console.fetch_runtime_binding", return_value=replacement),
                patch("beez_console.fetch_paper_status") as paper_status,
            ):
                with self.assertRaisesRegex(RuntimeError, "pid"):
                    beez_console.wait_for_server(None, adopted, timeout_seconds=1)
            paper_status.assert_not_called()

            with (
                patch("beez_console.validated_profile_selection", return_value=selection),
                patch("beez_console._validated_operation", return_value=(manifest, state)),
                patch("beez_console.fetch_paper_status", return_value=dict(beez_console._PAPER_AUTHORITY)),
            ):
                with self.assertRaisesRegex(RuntimeError, "target process identity"):
                    beez_console.adopt_exact_requested_target_listener(
                        root, established, {**actual, "pid": 4322},
                    )

    def test_runtime_binding_enforces_typed_expected_pid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = replace(binding(Path(directory)), expected_pid=4321)
            actual = runtime_binding(expected)
            beez_console.validate_runtime_binding(actual, expected)
            for invalid in (4322, True, 0, None):
                with self.subTest(invalid=invalid):
                    candidate = dict(actual)
                    if invalid is None:
                        candidate.pop("pid")
                    else:
                        candidate["pid"] = invalid
                    with self.assertRaisesRegex(RuntimeError, "pid"):
                        beez_console.validate_runtime_binding(candidate, expected)

    def test_blocked_safe_requested_target_listener_is_adopted_when_established_binding_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            established = binding(root)
            operation_id = "profile-switch-" + "d" * 32
            manifest, state = write_operation(
                established, operation_id, git_sha=GIT_SHA,
                stage="BLOCKED_SAFE",
            )
            requested = {
                "profile": manifest["target_profile"],
                "request_id": manifest["request_id"],
                "operation_id": operation_id,
                "requested_at": manifest["created_at"],
            }
            profile = beez_console.resolve_paper_profile(str(manifest["target_profile"]))
            actual = {
                "ledger": manifest["ledger_path"],
                "audit": manifest["audit_root"],
                "control_center": "127.0.0.1:8090",
                "python": str(established.python),
                "git_sha": GIT_SHA,
                "entry_profile": profile.policy.entry_profile,
                "entry_profile_version": profile.policy.entry_profile_version,
                "paper_policy_hash": profile.policy.configuration_hash,
                "risk_profile_hash": profile.risk.configuration_hash,
                "ledger_epoch": manifest["ledger_epoch"],
                "ledger_identity": "l3g-ledger-" + "e" * 32,
                "profile_selection_source": "PROFILE_SWITCH_MANIFEST",
                "pid": 4321,
            }
            selection = {
                "established": {
                    "profile": established.profile,
                    "operation_id": established.operation_id,
                    "ledger_path": str(established.ledger_path),
                    "ledger_identity": established.ledger_identity,
                    "ledger_epoch": established.ledger_epoch,
                    "audit_root": str(established.audit_root),
                    "git_sha": established.git_sha,
                },
                "requested": requested,
            }
            established_manifest = {
                "target_profile": established.profile,
                "ledger_path": str(established.ledger_path),
                "audit_root": str(established.audit_root),
                "ledger_epoch": established.ledger_epoch,
            }
            with (
                patch("beez_console.validated_profile_selection", return_value=selection),
                patch(
                    "beez_console._validated_operation",
                    side_effect=[
                        (established_manifest, {"stage": "RUNNING"}),
                        (manifest, state),
                        (manifest, state),
                    ],
                ),
                patch("beez_console.fetch_runtime_binding", return_value=actual),
                patch("beez_console.fetch_paper_status", return_value=dict(beez_console._PAPER_AUTHORITY)),
            ):
                adopted = beez_console.binding_for_existing_listener(root, established)

            self.assertEqual(adopted.source, "REQUESTED_TARGET_EXISTING_LISTENER")
            self.assertEqual(adopted.profile, profile.selection_key)
            self.assertEqual(adopted.operation_id, operation_id)
            self.assertEqual(adopted.ledger_identity, actual["ledger_identity"])

    def test_blocked_new_handoff_allows_validated_established_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = binding(Path(directory))
            new_operation = "profile-switch-" + "d" * 32
            selection = {
                "established": {
                    "profile": expected.profile,
                    "operation_id": expected.operation_id,
                    "ledger_path": str(expected.ledger_path),
                    "ledger_identity": expected.ledger_identity,
                    "ledger_epoch": expected.ledger_epoch,
                    "audit_root": str(expected.audit_root),
                    "git_sha": expected.git_sha,
                },
                    "requested": {
                        "profile": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
                        "request_id": "request-blocked-target",
                        "operation_id": new_operation,
                        "requested_at": "2026-09-07T00:00:00Z",
                    },
            }
            with (
                patch("beez_console.validated_profile_selection", return_value=selection),
                patch(
                    "beez_console._validated_operation",
                    side_effect=[
                        ({
                            "target_profile": expected.profile,
                            "ledger_path": str(expected.ledger_path),
                            "audit_root": str(expected.audit_root),
                            "ledger_epoch": expected.ledger_epoch,
                        }, {"stage": "RUNNING"}),
                        ({
                            "request_id": "request-blocked-target",
                            "target_profile": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
                            "created_at": "2026-09-07T00:00:00Z",
                        }, {"stage": "BLOCKED_SAFE"}),
                    ],
                ),
            ):
                beez_console.assert_remembered_launch_is_not_competing(expected)

    def test_explicit_maintenance_allows_terminal_prior_commit_but_not_active_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            expected = replace(
                binding(root),
                source="EXPLICIT_MAINTENANCE",
                ledger_identity=None,
                operation_id=None,
                git_sha="e" * 40,
            )
            operation_id = "profile-switch-" + "d" * 32
            prior_sha = "f" * 40
            manifest, _ = write_operation(
                expected, operation_id, git_sha=prior_sha, stage="BLOCKED_SAFE",
            )
            selection = {
                "established": None,
                "requested": {
                    "profile": manifest["target_profile"],
                    "request_id": manifest["request_id"],
                    "operation_id": operation_id,
                    "requested_at": manifest["created_at"],
                },
            }
            with patch("beez_console.validated_profile_selection", return_value=selection):
                beez_console.assert_remembered_launch_is_not_competing(expected)

            state_path = expected.runtime_root / "profile-switch" / "operations" / operation_id / "state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["stage"] = "STARTING_TARGET"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            with patch("beez_console.validated_profile_selection", return_value=selection):
                with self.assertRaisesRegex(RuntimeError, "will not compete"):
                    beez_console.assert_remembered_launch_is_not_competing(expected)

    def test_explicit_maintenance_prior_commit_still_requires_exact_requested_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            expected = replace(
                binding(root),
                source="EXPLICIT_MAINTENANCE",
                ledger_identity=None,
                operation_id=None,
                git_sha="e" * 40,
            )
            operation_id = "profile-switch-" + "d" * 32
            manifest, _ = write_operation(
                expected, operation_id, git_sha="f" * 40, stage="BLOCKED_SAFE",
            )
            selection = {
                "established": None,
                "requested": {
                    "profile": manifest["target_profile"],
                    "request_id": "request-does-not-match",
                    "operation_id": operation_id,
                    "requested_at": manifest["created_at"],
                },
            }
            with patch("beez_console.validated_profile_selection", return_value=selection):
                with self.assertRaisesRegex(RuntimeError, "requested profile handoff"):
                    beez_console.assert_remembered_launch_is_not_competing(expected)

    def test_start_script_routes_explicit_binding_through_launcher_arguments(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "scripts" / "start_beezconsole.ps1").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("$env:BEELZEBUB_L3G_PAPER_", script)
        self.assertNotIn("/api/health", script)
        self.assertIn("--maintenance-paper-profile", script)
        self.assertIn("BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2", script)
        self.assertIn("/api/runtime-binding", script)
        self.assertIn("legacyRestartDefaults", script)
        self.assertIn("IsPathFullyQualified", script)
        self.assertIn("beez_console.py", script)
        self.assertNotIn("Start-Process -FilePath $launcher", script)

    def test_restart_script_accepts_the_perpetual_profile(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "scripts" / "restart_beezconsole.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2", script)

    def test_frozen_child_environment_removes_bundle_anchored_path_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            bundle = root / "pyinstaller-bundle"
            outside = root / "system-bin"
            environment = {
                "Path": os.pathsep.join((str(bundle), str(bundle / "nested"), str(outside))),
                "KEEP_ME": "yes",
            }
            child = beez_console._sanitized_frozen_child_environment(
                environment, bundle_root=bundle,
            )
            self.assertEqual(child["Path"], str(outside))
            self.assertEqual(child["KEEP_ME"], "yes")

    def test_frozen_delegate_resets_dll_directory_before_external_python(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            (root / "beez_console.py").touch()
            process = Mock()
            events: list[str] = []
            with (
                patch(
                    "beez_console._reset_frozen_windows_dll_directory",
                    side_effect=lambda: events.append("reset-dll-directory"),
                ) as reset_dlls,
                patch(
                    "beez_console._sanitized_frozen_child_environment",
                    side_effect=lambda: events.append("sanitize-environment") or {"SAFE": "yes"},
                ),
                patch(
                    "beez_console.subprocess.Popen",
                    side_effect=lambda *args, **kwargs: events.append("popen") or process,
                ) as popen,
            ):
                actual = beez_console.delegate_frozen_launcher(root, ("--example",))
            self.assertIs(actual, process)
            reset_dlls.assert_called_once_with()
            self.assertEqual(events, ["reset-dll-directory", "sanitize-environment", "popen"])
            self.assertEqual(popen.call_args.kwargs["env"], {"SAFE": "yes"})

    def test_frozen_wrapper_delegates_to_authoritative_checkout_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project(root)
            (root / "beez_console.py").touch()
            with (
                patch("beez_console.project_root", return_value=root),
                patch.object(beez_console.sys, "frozen", True, create=True),
                patch("beez_console.delegate_frozen_launcher") as delegate,
                patch("beez_console.resolve_launch_binding") as resolve_binding,
            ):
                self.assertEqual(beez_console.main(("--example",)), 0)
            delegate.assert_called_once_with(root, ("--example",))
            resolve_binding.assert_not_called()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
