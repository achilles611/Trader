"""Deliberately explicit command line for the non-authoritative f4 lab."""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

from .anvil_backend import PINNED_ANVIL_VERSION, AnvilBackend, anvil_version, installed_anvil
from .anvil_state import ANVIL_EXECUTION_STATE_SCHEMA, PINNED_WINDOWS_ARCHIVE_SHA256
from .contracts import BackendType, ScenarioValidationError, canonical_json
from .coordinator import CounterfactualCoordinator
from .evidence import default_artifact_root, persist_result
from .rpc import assert_loopback_endpoint
from .scenario import load_scenario, scenario_from_dict


def _doctor() -> dict[str, object]:
    loopback = False
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            loopback = True
    except OSError:
        loopback = False
    path = installed_anvil()
    version = anvil_version(path)
    binary_identity = AnvilBackend(chain_id=31337, binary=path).binary_identity if path is not None else {}
    return {
        "authority_domain": "COUNTERFACTUAL_ONLY",
        "execution_authority": False,
        "trading_authority": False,
        "local_state_mutation": True,
        "anvil_available": path is not None and version is not None,
        "anvil_version": version,
        "anvil_binary_identity": binary_identity,
        "anvil_archive_sha256": PINNED_WINDOWS_ARCHIVE_SHA256,
        "pinned_anvil_version": PINNED_ANVIL_VERSION,
        "semantic_fingerprint_schema": ANVIL_EXECUTION_STATE_SCHEMA,
        "loopback_binding_capable": loopback,
        "artifact_root": str(default_artifact_root()),
        "supported_backends": [item.value for item in BackendType],
        "no_secret_state": True,
        "external_evm_smoke_available": bool(version and PINNED_ANVIL_VERSION in version and loopback),
    }


def _run(path: str, *, replay: bool = False) -> int:
    if replay:
        try:
            manifest = json.loads(Path(path).read_text(encoding="utf-8"))
            scenario = scenario_from_dict(manifest["scenario"])
        except (OSError, KeyError, json.JSONDecodeError, ScenarioValidationError) as exc:
            raise ScenarioValidationError("Replay manifest lacks a valid embedded scenario.") from exc
    else:
        scenario = load_scenario(path)
    result = CounterfactualCoordinator().run(scenario)
    target = persist_result(result, scenario=scenario)
    print(canonical_json({
        "run_id": result.identity.run_id, "scenario_hash": scenario.scenario_hash,
        "status": result.evidence.run_status.value, "restoration_verified": result.evidence.restoration_verified,
        "artifact": str(target), "provenance": "COUNTERFACTUAL_ONLY",
    }))
    return 0 if result.evidence.run_status.value == "SUCCEEDED" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.lane_ii.lab.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate a data-only JSON scenario")
    validate.add_argument("scenario")
    run = commands.add_parser("run", help="run one disposable counterfactual universe")
    run.add_argument("scenario")
    replay = commands.add_parser("replay", help="rerun the embedded scenario from a lab manifest")
    replay.add_argument("run_manifest")
    commands.add_parser("doctor", help="report f4 isolation and Anvil readiness")
    args = parser.parse_args(argv)
    try:
        if args.command == "doctor":
            print(canonical_json(_doctor()))
            return 0
        if args.command == "validate":
            scenario = load_scenario(args.scenario)
            print(canonical_json({"scenario_hash": scenario.scenario_hash, "backend": scenario.backend.value, "provenance": scenario.provenance}))
            return 0
        return _run(args.run_manifest, replay=True) if args.command == "replay" else _run(args.scenario)
    except (ScenarioValidationError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
