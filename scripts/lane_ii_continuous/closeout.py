"""Read-only reconciliation of retained paper endpoints, including failed runs.

An accounting pass never changes a study's execution result.  Unsealed SQLite
inputs are opened only after a stable copy of the DB and all WAL sidecars.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import uuid
from typing import Callable

SCHEMA = "LANE_II_RETAINED_CLOSEOUT_V1"
EPSILON = 1e-5
MIB = 1024 * 1024


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def stamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def load(path: Path, maximum: int = 8 * MIB) -> dict:
    if path.stat().st_size > maximum:
        raise ValueError("DOCUMENT_SIZE_LIMIT:" + path.name)
    return json.loads(path.read_bytes(), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_JSON")))


def bind(path: Path, maximum: int | None = None) -> dict:
    path = path.resolve()
    size = path.stat().st_size
    if maximum is not None and size > maximum:
        raise ValueError("INPUT_SIZE_LIMIT:" + path.name)
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"path": str(path), "bytes": size, "sha256": digest}


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def number(value) -> bool:
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)


def near(actual, expected, reason: str) -> None:
    require(number(actual) and number(expected) and abs(actual - expected) <= EPSILON, reason)


def atomic_json(path: Path, value: dict) -> None:
    temp = path.with_name(path.name + ".tmp." + str(os.getpid()))
    with temp.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def owner_observation(operator: dict) -> dict:
    """Inspect one retained PID and creation time; never stop a process."""
    result = {"observed_utc": utc(), "pid": operator.get("pid"), "expected_creation_utc": operator.get("creation_utc")}
    if type(operator.get("pid")) is not int or not operator.get("creation_utc"):
        return {**result, "state": "UNKNOWN", "reason": "OWNER_IDENTITY_INCOMPLETE"}
    try:
        import psutil
        process = psutil.Process(operator["pid"])
        created = process.create_time()
        result["observed_creation_epoch"] = created
        if abs(created - stamp(operator["creation_utc"])) > .01:
            return {**result, "state": "PID_REUSED", "owned_process_alive": False}
        result["executable"] = process.exe()
        return {**result, "state": "ALIVE", "owned_process_alive": True}
    except ImportError:
        return {**result, "state": "UNKNOWN", "reason": "PROCESS_INSPECTION_UNAVAILABLE"}
    except psutil.NoSuchProcess:
        return {**result, "state": "ABSENT", "owned_process_alive": False}
    except (psutil.AccessDenied, OSError):
        return {**result, "state": "UNKNOWN", "reason": "PROCESS_INSPECTION_DENIED"}


def reconcile_profiles(capture: Path, receipt: dict, max_ledger_bytes: int = 64 * MIB) -> dict:
    """Recompute all retained profile financial events without opening raw tape."""
    rows = receipt.get("profiles")
    require(isinstance(rows, list) and rows, "PROFILE_RECEIPT_MISSING")
    ids = [row.get("id") for row in rows]
    require(len(set(ids)) == len(ids) and all(isinstance(i, str) for i in ids), "DUPLICATE_PROFILE_ID")
    expected = receipt.get("ledger_sha256", {})
    require(set(expected) == set(ids), "LEDGER_ROSTER_MISMATCH")
    total_bytes = 0
    accounts, bindings, event_ids = [], [], set()
    for row in rows:
        profile_id = row["id"]
        path = (capture / (profile_id + "-ledger.jsonl")).resolve()
        require(path.is_relative_to(capture.resolve()), "LEDGER_PATH_ESCAPE")
        total_bytes += path.stat().st_size
        require(total_bytes <= max_ledger_bytes, "LEDGER_BATCH_SIZE_LIMIT")
        before = bind(path, max_ledger_bytes)
        require(before["sha256"] == expected[profile_id], "LEDGER_RECEIPT_HASH_MISMATCH:" + profile_id)
        positions, counts, sides = {}, Counter(), Counter()
        realized = fees = 0.
        with path.open("rb") as stream:
            while raw := stream.readline(8 * MIB + 1):
                require(len(raw) <= 8 * MIB, "LEDGER_LINE_SIZE_LIMIT:" + profile_id)
                event = json.loads(raw)
                kind = event.get("kind")
                if kind not in ("PAPER_ENTRY", "PAPER_EXIT", "PAPER_SETTLEMENT"):
                    continue
                seq = event.get("event_seq")
                require(type(seq) is int and seq not in event_ids, "DUPLICATE_OR_MISSING_FINANCIAL_EVENT")
                event_ids.add(seq)
                data = event["data"]
                require(data.get("profile") == profile_id, "LEDGER_PROFILE_IDENTITY")
                market = data["market"]
                counts[kind] += 1
                if kind == "PAPER_ENTRY":
                    require(market not in positions, "ENTRY_REPLACES_OPEN_POSITION")
                    require(data.get("side") in ("Up", "Down"), "UNKNOWN_ENTRY_SIDE")
                    require(all(number(data.get(k)) and data[k] >= 0 for k in ("quantity", "basis", "entry_fee")), "ENTRY_VALUES_UNKNOWN")
                    require(data["quantity"] > 0, "NONPOSITIVE_QUANTITY")
                    if "authorized_entry_budget" in data:
                        require(number(data["authorized_entry_budget"]) and data["basis"] <= data["authorized_entry_budget"] + EPSILON, "ENTRY_BUDGET_EXCEEDED")
                    if data.get("fills"):
                        near(sum(f["quantity"] for f in data["fills"]), data["quantity"], "ENTRY_QUANTITY_MISMATCH")
                        near(sum(f["quantity"] * f["price"] for f in data["fills"]) + data["entry_fee"], data["basis"], "ENTRY_FILL_ACCOUNTING")
                    positions[market] = {k: data[k] for k in ("side", "quantity", "basis")}
                    fees += data["entry_fee"]
                    sides[data["side"]] += 1
                else:
                    require(market in positions and data.get("side") == positions[market]["side"], "EXIT_WITHOUT_MATCHING_POSITION")
                    position = positions.pop(market)
                    require(number(data.get("net")), "EXIT_NET_UNKNOWN")
                    if kind == "PAPER_EXIT":
                        require(all(number(data.get(k)) for k in ("proceeds", "exit_fee", "basis")), "EXIT_VALUES_UNKNOWN")
                        near(data["basis"], position["basis"], "EXIT_BASIS_MISMATCH")
                        near(data["net"], data["proceeds"] - position["basis"], "EXIT_NET_MISMATCH")
                        if data.get("fills"):
                            near(sum(f["quantity"] for f in data["fills"]), position["quantity"], "EXIT_QUANTITY_MISMATCH")
                            near(sum(f["quantity"] * f["price"] for f in data["fills"]) - data["exit_fee"], data["proceeds"], "EXIT_FILL_ACCOUNTING")
                        fees += data["exit_fee"]
                    else:
                        require(data.get("payout") in (0, 1), "UNVERIFIED_PAYOUT_VALUE")
                        near(data["net"], position["quantity"] * data["payout"] - position["basis"], "SETTLEMENT_NET_MISMATCH")
                    realized += data["net"]
        require(bind(path) == before, "LEDGER_CHANGED_DURING_READ")
        bindings.append(before)
        quantities = {"entries": counts["PAPER_ENTRY"], "cash_exits": counts["PAPER_EXIT"], "settlements": counts["PAPER_SETTLEMENT"], "unresolved_positions": len(positions)}
        require(all(row.get(k) == value for k, value in quantities.items()), "PROFILE_LEDGER_COUNTS_MISMATCH:" + profile_id)
        basis = sum(p["basis"] for p in positions.values())
        for key, value in (("realized_pnl", realized), ("modeled_fees", fees), ("unresolved_basis", basis)):
            near(row.get(key), value, "PROFILE_LEDGER_FINANCIAL_MISMATCH:" + profile_id + ":" + key)
        near(row.get("virtual_cash") + basis, row.get("virtual_initial_cash") + realized, "CASH_PLUS_BASIS_IDENTITY")
        accounts.append({"id": profile_id, **quantities, "entry_sides": dict(sides), "realized_pnl": realized, "modeled_fees": fees, "unresolved_basis": basis, "virtual_cash": row["virtual_cash"], "open_holdings": positions})
    return {"status": "PASS_RETAINED_PROFILE_ACCOUNTING", "accounts": accounts, "bindings": bindings,
            "counts": {k: sum(r[k] for r in accounts) for k in ("entries", "cash_exits", "settlements", "unresolved_positions")},
            "counterfactual_accounts_nonadditive": True, "raw_capture_integrity": "NOT_RUN", "causal_replay": "NOT_RUN"}


def stable_database_copy(source: Path, target: Path, max_bytes: int = 64 * MIB) -> list[dict]:
    members = [source / name for name in ("experiment.sqlite", "experiment.sqlite-wal", "experiment.sqlite-shm") if (source / name).exists()]
    require(any(p.name == "experiment.sqlite" for p in members), "DATABASE_MISSING")
    require(sum(p.stat().st_size for p in members) <= max_bytes, "DATABASE_COPY_SIZE_LIMIT")
    before = [bind(p, max_bytes) for p in members]
    target.mkdir(parents=True, exist_ok=False)
    for p in members:
        shutil.copyfile(p, target / p.name)
    after_names = [name for name in ("experiment.sqlite", "experiment.sqlite-wal", "experiment.sqlite-shm") if (source / name).exists()]
    require(after_names == [p.name for p in members] and before == [bind(p, max_bytes) for p in members], "SOURCE_DATABASE_CHANGED_DURING_COPY")
    require(all(bind(target / p.name)["sha256"] == b["sha256"] for p, b in zip(members, before)), "DATABASE_COPY_HASH_MISMATCH")
    return before


def reconcile_cohort_copy(database: Path, initial_cash: float) -> dict:
    require(number(initial_cash) and initial_cash >= 0, "INITIAL_CASH_UNKNOWN")
    con = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    con.row_factory = sqlite3.Row
    operations = [0]
    def progress():
        operations[0] += 1
        return int(operations[0] > 100000)
    con.set_progress_handler(progress, 1000)
    try:
        con.execute("PRAGMA query_only=ON")
        require([r[0] for r in con.execute("PRAGMA quick_check")] == ["ok"], "SQLITE_QUICK_CHECK_FAILED")
        accounts = [dict(r) for r in con.execute("SELECT wallet,sleeve,cash,realized_pnl FROM accounts ORDER BY sleeve,wallet")]
        require(len(accounts) <= 10000, "ACCOUNT_COUNT_LIMIT")
        rows = []
        for account in accounts:
            key = (account["wallet"], account["sleeve"])
            fills = [dict(r) for r in con.execute("SELECT id,side,quantity,price,fee,cash,realized_pnl FROM paper_fills WHERE wallet=? AND sleeve=? ORDER BY id", key)]
            inventory = [dict(r) for r in con.execute("SELECT token,condition_id,quantity,basis FROM inventory WHERE wallet=? AND sleeve=? AND quantity>0.00000001 ORDER BY token", key)]
            change = realized = fees = 0.
            counts = Counter()
            for fill in fills:
                require(fill["side"] in ("BUY", "SELL", "SETTLEMENT"), "UNKNOWN_COHORT_FILL_SIDE")
                require(all(number(fill[k]) for k in ("quantity", "price", "fee", "cash", "realized_pnl")), "COHORT_FILL_VALUE_UNKNOWN")
                require(fill["quantity"] >= 0 and fill["fee"] >= 0, "NEGATIVE_COHORT_FILL")
                counts[fill["side"]] += 1
                change += (-1 if fill["side"] == "BUY" else 1) * fill["cash"]
                realized += fill["realized_pnl"]
                fees += fill["fee"]
                if fill["side"] in ("BUY", "SELL"):
                    near(fill["cash"], fill["quantity"] * fill["price"] + (1 if fill["side"] == "BUY" else -1) * fill["fee"], "COHORT_FILL_CASH_MISMATCH")
                else:
                    require(fill["price"] in (0, 1), "COHORT_SETTLEMENT_PAYOUT_UNKNOWN")
                    near(fill["cash"], fill["quantity"] * fill["price"], "COHORT_SETTLEMENT_CASH_MISMATCH")
            basis = sum(p["basis"] for p in inventory)
            near(account["cash"], initial_cash + change, "COHORT_CASH_LEDGER_MISMATCH")
            near(account["realized_pnl"], realized, "COHORT_REALIZED_LEDGER_MISMATCH")
            near(account["cash"] + basis, initial_cash + realized, "COHORT_CASH_PLUS_BASIS_IDENTITY")
            rows.append({"wallet": account["wallet"], "policy": account["sleeve"], "entries": counts["BUY"], "cash_exits": counts["SELL"], "settlements": counts["SETTLEMENT"], "virtual_cash": account["cash"], "realized_pnl": realized, "modeled_fees": fees, "unresolved_positions": len(inventory), "unresolved_basis": basis, "open_holdings": inventory})
        checkpoint_row = con.execute("SELECT value_json FROM runtime_state WHERE key='checkpoint'").fetchone()
        checkpoint = json.loads(checkpoint_row[0]) if checkpoint_row else {}
        return {"status": "PASS_RETAINED_COHORT_ACCOUNTING", "accounts": rows, "counts": {k: sum(r[k] for r in rows) for k in ("entries", "cash_exits", "settlements", "unresolved_positions")}, "checkpoint": {k: checkpoint.get(k) for k in ("at_utc", "state", "terminal", "reason")}, "sqlite_quick_check": "ok", "counterfactual_accounts_nonadditive": True, "outcome_raw_proof_audit": "NOT_RUN", "causal_replay": "NOT_RUN"}
    finally:
        con.close()


def closeout_followup(root: Path, output: Path, *, process_probe: Callable[[dict], dict] = owner_observation, max_ledger_bytes: int = 64 * MIB) -> dict:
    root, output = Path(root).resolve(), Path(output).resolve()
    require(not output.is_relative_to(root) and not root.is_relative_to(output), "OUTPUT_MUST_BE_OUTSIDE_RETAINED_RUN")
    if (output / "ARTIFACT_MANIFEST.json").exists():
        manifest = load(output / "ARTIFACT_MANIFEST.json")
        for item in manifest["artifacts"]:
            require(bind(Path(item["path"])) == item, "PUBLISHED_OUTPUT_CHANGED")
        result = load(output / "CLOSEOUT.json")
        require(result["source_root"] == str(root), "OUTPUT_SOURCE_IDENTITY_MISMATCH")
        for item in result["input_bindings"]:
            require(bind(Path(item["path"])) == item, "SEALED_INPUT_CHANGED")
        return result
    output.mkdir(parents=True, exist_ok=True)
    window_path = root / "WINDOW.json"
    window = load(window_path)
    bindings = [bind(window_path)]
    report = {"schema": SCHEMA, "as_of_utc": utc(), "source_root": str(root), "original_artifacts_modified": False,
              "network_requests": 0, "model_api_calls": 0, "real_orders": 0, "components": {}, "blockers": [], "input_bindings": bindings}
    for kind in ("study", "cohort"):
        scope = root / "segments" / "segment-001" / kind
        operator_path = scope / "operator.json"
        operator = load(operator_path)
        bindings.append(bind(operator_path))
        owner = process_probe(operator)
        component = {"owner": owner, "execution": {"state": "UNKNOWN"}, "coverage": {"planned_seconds": window["paper_seconds"], "designation": "PARTIAL", "observed_seconds": None}, "closure": {"capture_sealed": False, "integrity_audited": False, "accounting": "BLOCKED", "analysis_published": False}}
        report["components"][kind] = component
        try:
            require(owner.get("state") in ("ABSENT", "PID_REUSED"), "OWNED_PROCESS_NOT_VERIFIED_ABSENT")
            if kind == "study":
                receipt_path = scope / "capture" / "receipt.json"
                receipt = load(receipt_path)
                bindings.append(bind(receipt_path))
                require(receipt.get("source_manifest_sha256") == operator.get("source_manifest_sha256") and receipt.get("pid") == operator.get("pid"), "STUDY_SOURCE_OR_OWNER_MISMATCH")
                require(receipt.get("real_orders") == receipt.get("model_api_calls") == receipt.get("authenticated_account_calls") == 0, "PAPER_BOUNDARY_MISMATCH")
                exit_path = scope / "exit.json"
                exit_verified = False
                if exit_path.exists():
                    exited = load(exit_path); bindings.append(bind(exit_path))
                    exit_verified = exited.get("pid") == operator["pid"] and exited.get("retained_handle_exit_observed") is True and exited.get("forced_closure") is False
                component["execution"] = {"state": str(receipt["state"]).lower(), "reason": receipt.get("reason"), "source": "ORIGINAL_RECEIPT", "actual_end_utc": receipt.get("actual_end_utc"), "supervisor_exit_verified": exit_verified}
                component["coverage"]["observed_seconds"] = max(0, stamp(receipt["actual_end_utc"]) - stamp(receipt["paper_start_utc"]))
                marker_path = scope / "capture" / "COLLECTION_CLOSED.json"
                if marker_path.exists():
                    marker = load(marker_path); bindings.append(bind(marker_path))
                    component["closure"]["capture_sealed"] = bool(marker.get("collection_closed") and marker.get("network_closed") and marker.get("snapshot_publisher_closed") and marker.get("worker_pid") == operator["pid"] and marker.get("source_manifest_sha256") == receipt["source_manifest_sha256"])
                account = reconcile_profiles(scope / "capture", receipt, max_ledger_bytes)
                bindings.extend(account.pop("bindings"))
                component["accounting"] = account
                component["closure"].update(accounting="RECONCILED_RETAINED_LEDGERS", integrity_scope="PROFILE_LEDGER_HASHES_ONLY")
                report["blockers"].append("STUDY_FULL_RAW_CAPTURE_AUDIT_AND_CAUSAL_REPLAY_PENDING")
                if not component["execution"]["supervisor_exit_verified"]:
                    report["blockers"].append("STUDY_GENUINE_SUPERVISOR_EXIT_RECEIPT_MISSING")
            else:
                legacy = scope / "capture" / "legacy"
                config_path, status_path = legacy / "run_config.json", legacy / "status.json"
                config, status = load(config_path), load(status_path)
                bindings.extend((bind(config_path), bind(status_path)))
                require(config.get("live_orders") == config.get("model_api_calls") == config.get("authenticated_requests") == 0, "COHORT_PAPER_BOUNDARY_MISMATCH")
                component["execution"] = {"state": "failed", "reason": "INTERRUPTED_WITHOUT_TERMINAL_RECEIPT", "source": "OWNER_ABSENCE_AND_UNSEALED_RETAINED_STATE", "last_reported_state": status.get("state"), "last_reported_utc": status.get("updated_at_utc"), "inferred_interruption": True}
                copy_dir = output / ("cohort-recovery-copy-" + uuid.uuid4().hex)
                db_bindings = stable_database_copy(legacy, copy_dir, max_ledger_bytes)
                bindings.extend(db_bindings)
                component["accounting"] = reconcile_cohort_copy(copy_dir / "experiment.sqlite", config["initial_cash_per_wallet_per_sleeve"])
                component["recovery_copy"] = {"path": str(copy_dir), "source_members": db_bindings, "sqlite_opened_only_in_disposable_copy": True}
                last = component["accounting"]["checkpoint"].get("at_utc") or status.get("updated_at_utc")
                if last:
                    component["coverage"]["observed_seconds"] = max(0, stamp(last) - stamp(config["started_at_utc"]))
                component["closure"].update(accounting="RECONCILED_RETAINED_DATABASE", integrity_scope="STABLE_DB_WAL_COPY_AND_SQLITE_QUICK_CHECK")
                report["blockers"].append("COHORT_UNSEALED_MISSING_TERMINAL_RECEIPT_OUTCOME_PROOF_AUDIT_AND_REPLAY")
            component["closure"]["analysis_published"] = True
        except (ValueError, KeyError, OSError, sqlite3.Error, TypeError) as error:
            component["closure"]["accounting"] = "BLOCKED"
            component["error"] = type(error).__name__ + ":" + str(error)
            report["blockers"].append(kind.upper() + ":" + str(error))
    require(all(bind(Path(item["path"])) == item for item in bindings), "SOURCE_CHANGED_DURING_CLOSEOUT")
    report["status"] = "PARTIAL_ACCOUNTING_RECONCILED" if all(c["closure"]["accounting"].startswith("RECONCILED_") for c in report["components"].values()) else "PARTIAL_ANALYSIS_BLOCKED"
    report["full_duration_qualified"] = report["strategy_edge_qualified"] = False
    atomic_json(output / "CLOSEOUT.json", report)
    artifacts = [bind(output / "CLOSEOUT.json")]
    for component in report["components"].values():
        if component.get("recovery_copy"):
            artifacts.extend(bind(p) for p in Path(component["recovery_copy"]["path"]).iterdir() if p.is_file())
    atomic_json(output / "ARTIFACT_MANIFEST.json", {"schema": "LANE_II_CLOSEOUT_ARTIFACTS_V1", "published_utc": utc(), "artifacts": artifacts, "original_execution_states_preserved": True})
    return report


def closeout_capture(run_dir, output_dir, source_dir=None, *, run_raw_audit=False,
                     process_probe=owner_observation, max_ledger_bytes=64 * MIB) -> dict:
    """Idempotent bounded terminal adapter for the shared forward study.

    It requires the coordinator's operator.json and reads capture/receipt.json.
    Raw audit is an explicit optional heavy job; default accounting is bounded.
    """
    from scripts.lane_ii_continuous.capture import read_terminal
    root, output = Path(run_dir).resolve(), Path(output_dir).resolve()
    require(not output.is_relative_to(root), "CLOSEOUT_OUTPUT_INSIDE_ORIGINAL_RUN")
    if (output / "ARTIFACT_MANIFEST.json").exists():
        manifest = load(output / "ARTIFACT_MANIFEST.json")
        require(all(bind(Path(item["path"])) == item for item in manifest["artifacts"]), "PUBLISHED_CLOSEOUT_CHANGED")
        prior = load(output / "CLOSEOUT.json")
        require(prior["run_root"] == str(root), "CLOSEOUT_INPUT_IDENTITY_CHANGED")
        require(all(bind(Path(item["path"])) == item for item in prior["input_bindings"]), "RETAINED_CLOSEOUT_INPUT_CHANGED")
        return prior
    terminal = read_terminal(root)
    require(terminal is not None, "GENUINE_TERMINAL_RECEIPT_REQUIRED")
    receipt = terminal.pop("receipt")
    capture = root / "capture"
    operator = load(root / "operator.json")
    owner = process_probe(operator)
    require(owner.get("state") in ("ABSENT", "PID_REUSED"), "RETAINED_OWNER_NOT_PROVEN_ABSENT")
    require(receipt.get("pid") == operator.get("pid"), "TERMINAL_OWNER_IDENTITY_MISMATCH")
    if operator.get("source_manifest_sha256"):
        require(operator["source_manifest_sha256"] == receipt.get("source_manifest_sha256"), "TERMINAL_SOURCE_IDENTITY_MISMATCH")
    require(receipt.get("real_orders") == receipt.get("model_api_calls") == receipt.get("authenticated_account_calls") == 0, "PAPER_BOUNDARY_MISMATCH")
    accounting = reconcile_profiles(capture, receipt, max_ledger_bytes)
    bindings = [bind(root / "operator.json"), bind(capture / "receipt.json")]
    bindings.extend(accounting["bindings"])
    if (capture / "COLLECTION_CLOSED.json").exists():
        bindings.append(bind(capture / "COLLECTION_CLOSED.json"))
    if source_dir is not None:
        source = Path(source_dir).resolve()
        source_binding = bind(source / "SOURCE_MANIFEST.json", 8 * MIB)
        require(source_binding["sha256"] == receipt["source_manifest_sha256"], "FROZEN_SOURCE_MANIFEST_MISMATCH")
        bindings.append(source_binding)
    output.mkdir(parents=True, exist_ok=True)
    closure = terminal["closure"]
    closure.update(accounting="RECONCILED_PROFILE_LEDGERS", accounting_reconciled=True, analysis_published=True,
                   integrity_scope="BOUNDED_TERMINAL_LEDGER_HASH_AND_ACCOUNTING_ONLY", causal_replay="NOT_RUN")
    if (root / "exit.json").exists():
        exited = load(root / "exit.json")
        bindings.append(bind(root / "exit.json"))
        closure["supervisor_exit_verified"] = exited.get("pid") == operator["pid"] and exited.get("retained_handle_exit_observed") is True and exited.get("forced_closure") is False
    blockers = ["FULL_RAW_CAPTURE_AUDIT_AND_CAUSAL_REPLAY_PENDING"]
    raw_audit = None
    if run_raw_audit:
        require(source_dir is not None, "RAW_AUDIT_REQUIRES_FROZEN_SOURCE")
        raw_audit = audit_capture_readonly(capture, source_dir, output / "raw-audit", process_probe=process_probe)
        bindings.append(bind(output / "raw-audit/RAW_CAPTURE_AUDIT.json"))
        closure["integrity_audited"] = raw_audit["status"] == "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY"
        closure["integrity_scope"] = "CHAIN_RAW_SOURCE_LEDGER_BINDING" if closure["integrity_audited"] else "RAW_AUDIT_FAILED"
        blockers = ["CAUSAL_REPLAY_PENDING"] if closure["integrity_audited"] else ["RAW_CAPTURE_AUDIT_FAILED", "CAUSAL_REPLAY_PENDING"]
    if not closure["capture_sealed"]:
        blockers.append("GENUINE_COLLECTION_CLOSURE_MISSING_OR_MISMATCHED")
    if not closure["supervisor_exit_verified"]:
        blockers.append("GENUINE_SUPERVISOR_EXIT_RECEIPT_MISSING_OR_FORCED")
    component = {"owner": owner, "execution": terminal["execution"], "coverage": terminal["coverage"],
                 "closure": closure, "accounting": accounting, "cash_control": receipt.get("cash_control")}
    report = {"schema": SCHEMA, "as_of_utc": utc(), "run_root": str(root), "source_root": str(root), "status": "PARTIAL_ACCOUNTING_RECONCILED",
              "components": {"study": component}, "input_bindings": bindings, "blockers": blockers,
              "original_artifacts_modified": False, "network_requests": 0, "model_api_calls": 0, "real_orders": 0,
              "full_duration_qualified": False, "strategy_edge_qualified": False}
    if raw_audit is not None:
        report["raw_audit"] = {"path": str(output / "raw-audit/RAW_CAPTURE_AUDIT.json"), "status": raw_audit["status"]}
    atomic_json(output / "CLOSEOUT.json", report)
    atomic_json(output / "ARTIFACT_MANIFEST.json", {"schema": "LANE_II_CLOSEOUT_ARTIFACTS_V1", "published_utc": utc(),
                                                   "artifacts": [bind(output / "CLOSEOUT.json")], "original_execution_states_preserved": True})
    return report


def audit_capture_readonly(capture_dir, source_dir, output_dir, *, process_probe=owner_observation,
                           maximum_line_bytes=8 * MIB, maximum_raw_files=250000,
                           maximum_ledger_bytes=64 * MIB) -> dict:
    """One streaming pass per tape; never import or write retained runtime code.

    The caller schedules this as an admitted heavy local job.  Event memory is
    bounded by one line; diagnostic examples are bounded independently of errors.
    Financial ledgers are matched to their exact rows in the chained event tape.
    This proves retained integrity, not completed execution or a strategy edge.
    """
    capture, source, output = map(lambda p: Path(p).resolve(), (capture_dir, source_dir, output_dir))
    require(not output.is_relative_to(capture) and not output.is_relative_to(source), "AUDIT_OUTPUT_INSIDE_RETAINED_INPUT")
    require(0 < maximum_line_bytes <= 16 * MIB and 0 < maximum_raw_files <= 1000000
            and 0 < maximum_ledger_bytes <= 24 * 64 * MIB, "AUDIT_RESOURCE_BOUNDS")
    output.mkdir(parents=True, exist_ok=True)
    receipt = load(capture / "receipt.json")
    operator = load(capture.parent / "operator.json")
    owner = process_probe(operator)
    require(owner.get("state") in ("ABSENT", "PID_REUSED"), "RETAINED_OWNER_NOT_PROVEN_ABSENT")
    marker = load(capture / "COLLECTION_CLOSED.json")
    require(marker.get("collection_closed") is True and marker.get("network_closed") is True and marker.get("worker_pid") == operator["pid"] == receipt["pid"] and marker.get("source_manifest_sha256") == receipt["source_manifest_sha256"], "GENUINE_COLLECTION_CLOSURE_REQUIRED")
    ledgers = {key: (capture / (key + "-ledger.jsonl")).resolve() for key in receipt["ledger_sha256"]}
    for path in ledgers.values():
        require(path.parent == capture, "LEDGER_PATH_ESCAPE")
        require(path.stat().st_size <= maximum_ledger_bytes, "INPUT_SIZE_LIMIT:" + path.name)
    problems = Counter()
    examples, inputs = [], []
    def problem(reason, detail=None):
        problems[reason] += 1
        if len(examples) < 40:
            examples.append({"reason": reason, "detail": detail})
    def exact_file(path, expected=None, maximum=None):
        before = path.stat()
        binding = bind(path, maximum)
        after = path.stat()
        require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "INPUT_CHANGED_DURING_AUDIT:" + path.name)
        if expected is not None and binding["sha256"] != expected:
            problem("FILE_HASH", path.name)
        return binding
    for name in ("receipt.json", "COLLECTION_CLOSED.json"):
        inputs.append(exact_file(capture / name, maximum=8 * MIB))
    manifest_binding = exact_file(source / "SOURCE_MANIFEST.json", receipt["source_manifest_sha256"], 8 * MIB)
    inputs.append(manifest_binding)
    manifest = load(source / "SOURCE_MANIFEST.json")
    require(isinstance(manifest["files"], dict) and len(manifest["files"]) <= 2000, "SOURCE_FILE_BOUND")
    for name, expected in manifest["files"].items():
        path = (source / name).resolve()
        require(path.is_relative_to(source), "SOURCE_PATH_ESCAPE")
        inputs.append(exact_file(path, expected if isinstance(expected, str) else expected["sha256"], 16 * MIB))
    for name, key in (("preregistration.json", "preregistration_sha256"), ("market-catalog.json", "market_catalog_sha256")):
        if key in receipt:
            inputs.append(exact_file(capture / name, receipt[key], 16 * MIB))
    def canonical(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    def lines(path, expected):
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while True:
                line = stream.readline(maximum_line_bytes + 1)
                if not line:
                    break
                require(len(line) <= maximum_line_bytes and line.endswith(b"\n"), "JSONL_LINE_BOUND_OR_TRUNCATION:" + path.name)
                digest.update(line)
                yield json.loads(line, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_JSON")))
        after = path.stat()
        require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "TAPE_CHANGED_DURING_AUDIT")
        value = {"path": str(path), "bytes": before.st_size, "sha256": digest.hexdigest()}
        inputs.append(value)
        if value["sha256"] != expected:
            problem("FILE_HASH", path.name)
    previous, count, raw_count, ws_count = "0" * 64, 0, 0, 0
    last_mono = -1
    raw_names = set()
    ledger_hashes = {key: hashlib.sha256() for key in receipt["ledger_sha256"]}
    require(len(ledger_hashes) <= 1000, "LEDGER_ROSTER_BOUND")
    cutoff = stamp(receipt["cutoff_utc"])
    start_row, end_row = None, None
    for row in lines(capture / "events.jsonl", receipt["events_sha256"]):
        expected = row.pop("sha256")
        count += 1
        if row["seq"] != count or row["previous_sha256"] != previous or expected != hashlib.sha256(canonical(row)).hexdigest():
            problem("EVENT_CHAIN", count)
        previous = expected
        mono = row.get("monotonic_ns")
        if not number(mono) or mono < last_mono:
            problem("EVENT_MONOTONIC_ORDER", count)
        last_mono = mono if number(mono) else last_mono
        kind, data = row["kind"], row["data"]
        if kind == "START":
            if start_row is not None:
                problem("DUPLICATE_START")
            start_row = data
        if kind == "END":
            if end_row is not None:
                problem("DUPLICATE_END")
            end_row = data
        if data.get("profile") in ledger_hashes:
            ledger_hashes[data["profile"]].update(canonical({"event_seq": row["seq"], "kind": kind, "data": data}) + b"\n")
        if kind == "HTTP_RESPONSE":
            ref = data["raw_ref"]
            path = (capture / ref["file"]).resolve()
            require(path.parent == capture and path.name.startswith("http-") and path.suffix == ".bin", "HTTP_RAW_PATH_ESCAPE")
            if path.name in raw_names:
                problem("DUPLICATE_HTTP_RAW", path.name)
            raw_names.add(path.name)
            raw_count += 1
            require(raw_count <= maximum_raw_files, "HTTP_RAW_FILE_BOUND")
            exact_file(path, ref["sha256"], 2 * MIB)
            if data.get("transport_started") and data["entry_wall"] >= cutoff:
                problem("LATE_HTTP_ENTRY", count)
            if number(data.get("entry_mono")) and number(data.get("end_mono")) and data["end_mono"] < data["entry_mono"]:
                problem("HTTP_MONOTONIC_ORDER", count)
        if kind == "WS_CONNECT_BEGIN" and data["entry_wall"] >= cutoff:
            problem("LATE_WS_CONNECT", count)
        if kind in ("PAPER_ENTRY", "PAPER_EXIT", "PAPER_SETTLEMENT") and stamp(row["utc"]) >= cutoff:
            problem("LATE_DERIVED_PAPER_CONSUMPTION", count)
        if kind == "MODEL_AND_PROFILE_TICK":
            if data["feature_available_wall"] >= cutoff or data["feature_available_monotonic_ns"] < data["construction_start_monotonic_ns"]:
                problem("TICK_CLOCK_ORDER", count)
            if data["mark"].get("available_utc_epoch", 0) > data["second"]:
                problem("FUTURE_RECEIPT_MARK", count)
    if previous != receipt["chain_sha256"] or count != receipt["events"]:
        problem("RECEIPT_CHAIN_BINDING")
    if not start_row or start_row.get("manifest_sha256") != receipt["source_manifest_sha256"]:
        problem("START_SOURCE_BINDING")
    if not end_row or end_row.get("state") != receipt["state"] or end_row.get("reason") != receipt["reason"]:
        problem("END_EXECUTION_BINDING")
    for row in lines(capture / "websocket-frames.jsonl", receipt["websocket_sha256"]):
        ws_count += 1
        if row["seq"] != ws_count or hashlib.sha256(base64.b64decode(row["raw_b64"], validate=True)).hexdigest() != row["sha256"]:
            problem("WS_RAW_HASH_OR_SEQUENCE", ws_count)
    if receipt.get("ws_frames") != ws_count:
        problem("RECEIPT_WS_COUNT")
    for key, expected in receipt["ledger_sha256"].items():
        inputs.append(exact_file(ledgers[key], expected, maximum_ledger_bytes))
        if ledger_hashes[key].hexdigest() != expected:
            problem("CHAIN_TO_LEDGER_BINDING", key)
    # Recheck small control/receipt bytes; large immutable tapes already had a
    # complete streaming digest and unchanged size/mtime around that pass.
    require(bind(capture / "receipt.json") == inputs[0] and bind(capture / "COLLECTION_CLOSED.json") == inputs[1], "TERMINAL_METADATA_CHANGED")
    report = {"schema": "LANE_II_READONLY_CAPTURE_AUDIT_V1", "utc": utc(),
              "status": "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY" if not problems else "FAIL_RETAINED_CAPTURE_INTEGRITY",
              "capture_dir": str(capture), "source_dir": str(source), "owner": owner,
              "execution": {"state": receipt["state"], "reason": receipt["reason"], "source": "ORIGINAL_GENUINE_RECEIPT"},
              "events_checked": count, "http_raw_files_checked": raw_count, "ws_frames_checked": ws_count,
              "problem_counts": dict(problems), "problem_examples": examples, "input_bindings": inputs,
              "maximum_line_bytes": maximum_line_bytes, "original_files_modified": False,
              "supervisor_exit_proved": False, "causal_strategy_replay": "NOT_RUN",
              "full_duration_qualified": False, "strategy_edge_qualified": False}
    atomic_json(output / "RAW_CAPTURE_AUDIT.json", report)
    atomic_json(output / "RAW_CAPTURE_AUDIT_MANIFEST.json", {"schema": "LANE_II_CAPTURE_AUDIT_ARTIFACTS_V1", "artifacts": [bind(output / "RAW_CAPTURE_AUDIT.json")]})
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--audit-capture", type=Path)
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.audit_capture:
        if not args.source_dir or args.run_root:
            parser.error("--audit-capture requires --source-dir and cannot use --run-root")
        report = audit_capture_readonly(args.audit_capture, args.source_dir, args.out)
    else:
        if not args.run_root:
            parser.error("--run-root or --audit-capture is required")
        report = closeout_followup(args.run_root, args.out)
    print(json.dumps({"status": report["status"], "blockers": report.get("blockers", report.get("problem_counts", {})), "output": str(args.out)}, indent=2))


if __name__ == "__main__":
    main()
