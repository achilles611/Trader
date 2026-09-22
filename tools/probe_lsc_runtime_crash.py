#!/usr/bin/env python3
"""Externally kill an LSC writer and verify fail-closed supported recovery.

The probe owns only the child process it starts and an explicitly new output
root. It never touches NinjaTrader, the operational runtime, or PaperLedger.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Mapping

from src.l3f_provider.ninjatrader_observation import (
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    NinjaTraderObservation,
)
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
from src.lsc.segment_store import SegmentLimits
from src.lsc.shadow_runtime import LSCShadowConfig, LSCShadowRuntime


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _write_new(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _config(root: Path) -> LSCShadowConfig:
    return LSCShadowConfig(
        root=root,
        journal_id="external-crash-probe",
        profile_id="beelzebub_pong_discernment_v1",
        native_contract_id="MNQ DEC26",
        canonical_contract_id="MNQZ6",
        segment_limits=SegmentLimits(max_records=1),
    )


def _observations(source: Path, count: int = 2) -> list[NinjaTraderObservation]:
    key = LocalPaperSecretProvider().load_key()
    result: list[NinjaTraderObservation] = []
    with source.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            raw = value.get("observation")
            if not isinstance(raw, dict) or value.get("signature_verified") is not True:
                raise ValueError("source collector record is not authenticated observation evidence")
            observation = NinjaTraderObservation.from_wire(
                _canonical_json(raw).decode("utf-8")
            ).verify_attestation(
                key,
                expected_source_fingerprint=EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
            )
            if observation.wire_hash() != value.get("wire_hash"):
                raise ValueError("source collector wire hash mismatch")
            result.append(observation)
            if len(result) == count:
                return result
    raise ValueError("source collector journal contains too few observations")


def child(root: Path, source: Path, ready: Path) -> int:
    runtime = LSCShadowRuntime(_config(root))
    runtime.start()
    for observation in _observations(source):
        if not runtime.submit(observation):
            raise RuntimeError("external crash child submission refused")
    deadline = time.monotonic() + 10.0
    while runtime.status()["processed"] != 2:
        if time.monotonic() >= deadline:
            raise RuntimeError("external crash child did not process source prefix")
        time.sleep(0.01)
    _write_new(ready, {
        "schema": "beelzebub-lsc-external-crash-ready-v1",
        "pid": os.getpid(),
        "processed": 2,
        "active_marker": str((root / "runtime-state" / "active.json").resolve()),
    })
    while True:
        time.sleep(1.0)


def driver(root: Path, source: Path, output: Path) -> int:
    if root.exists():
        raise ValueError("external crash probe root must not already exist")
    ready = root.parent / f"{root.name}-child-ready.json"
    if ready.exists() or output.exists():
        raise ValueError("external crash probe evidence path already exists")
    command = [
        sys.executable,
        "-m",
        "tools.probe_lsc_runtime_crash",
        "child",
        "--root",
        str(root),
        "--source",
        str(source),
        "--ready",
        str(ready),
    ]
    process = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[1])
    killed = False
    try:
        deadline = time.monotonic() + 15.0
        while not ready.is_file():
            if process.poll() is not None:
                raise RuntimeError(f"external crash child exited early: {process.returncode}")
            if time.monotonic() >= deadline:
                raise RuntimeError("external crash child readiness timeout")
            time.sleep(0.02)
        ready_value = json.loads(ready.read_text(encoding="utf-8"))
        process.kill()
        killed = True
        child_exit = process.wait(timeout=10.0)
        recovery = LSCShadowRuntime.recover_unclean(_config(root))
        continued = LSCShadowRuntime(_config(root))
        start = continued.start()
        terminal = continued.close()
        result = {
            "schema": "beelzebub-lsc-external-crash-probe-v1",
            "result": "PASS" if (
                child_exit != 0
                and recovery.get("proof_valid") is False
                and recovery.get("possible_unsealed_tail_loss") is True
                and recovery.get("sealed_segments") == 1
                and recovery.get("committed_segments_after") == 1
                and terminal.get("proof_valid") is True
            ) else "FAIL",
            "source": str(source.resolve()),
            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "root": str(root.resolve()),
            "child": {**ready_value, "exit_code": child_exit},
            "recovery": recovery,
            "continuation_start": start,
            "continuation_terminal": terminal,
            "qualification_authority": False,
        }
        _write_new(output, result)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result["result"] == "PASS" else 2
    finally:
        if not killed and process.poll() is None:
            process.kill()
            process.wait(timeout=10.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    child_parser = subparsers.add_parser("child")
    child_parser.add_argument("--root", type=Path, required=True)
    child_parser.add_argument("--source", type=Path, required=True)
    child_parser.add_argument("--ready", type=Path, required=True)
    driver_parser = subparsers.add_parser("driver")
    driver_parser.add_argument("--root", type=Path, required=True)
    driver_parser.add_argument("--source", type=Path, required=True)
    driver_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.mode == "child":
            return child(args.root, args.source, args.ready)
        return driver(args.root, args.source, args.output)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(json.dumps({
            "result": "ERROR",
            "error": type(error).__name__,
            "detail": str(error),
        }, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
