"""Strict offline replay for prospectively instrumented Lane II paper captures.

Historical replay inputs and contracts are unchanged. This adapter requires
genuine supervision, exact prospective decision admissions and frozen risk rules.
"""
from __future__ import annotations

import argparse
import ast
import base64
from collections import Counter, deque
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.overnight_paper import replay_study as retained

MIB = 1024 ** 2
EXPECTED_IDS = {"5m-P13", "5m-P2"}
DECISION_SCHEMA = "LANE_II_DECISION_CAPTURE_V1"
RISK_SCHEMA = "LANE_II_SHARED_PAPER_RISK_V1"
_OFFLINE_ACTIVE = 0
_OFFLINE_INSTALLED = False


def require(value, reason):
    if not value:
        raise ValueError(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def stamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "TIMEZONE_REQUIRED")
    return parsed.timestamp()


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def binding(path, maximum=None):
    path = Path(path).resolve()
    size = path.stat().st_size
    require(maximum is None or size <= maximum, "REPLAY_INPUT_SIZE_BOUND")
    return {"path": str(path), "bytes": size, "sha256": sha(path)}


def load(path, maximum=8 * MIB):
    path = Path(path)
    require(path.stat().st_size <= maximum, "REPLAY_DOCUMENT_SIZE_BOUND")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "REPLAY_DOCUMENT_SIZE_BOUND")
    result = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_REPLAY_JSON")))
    require(isinstance(result, dict), "REPLAY_DOCUMENT_REQUIRED")
    return result


def _audit_boundary(event, args):
    if _OFFLINE_ACTIVE and event in {"socket.connect", "socket.getaddrinfo", "socket.gethostbyname",
                                     "subprocess.Popen", "os.system", "os.posix_spawn"}:
        raise PermissionError("PROSPECTIVE_REPLAY_NETWORK_AND_DESCENDANTS_DENIED")


@contextmanager
def offline_boundary():
    global _OFFLINE_ACTIVE, _OFFLINE_INSTALLED
    if not _OFFLINE_INSTALLED:
        sys.addaudithook(_audit_boundary)
        _OFFLINE_INSTALLED = True
    _OFFLINE_ACTIVE += 1
    try:
        yield
    finally:
        _OFFLINE_ACTIVE -= 1


def verify_candidate(candidate, expected):
    candidate = Path(candidate).resolve()
    manifest_path = candidate / "SOURCE_MANIFEST.json"
    require(sha(manifest_path) == expected, "EXACT_PROSPECTIVE_SOURCE_MANIFEST_REQUIRED")
    manifest = load(manifest_path)
    require(manifest.get("schema") == "LANE_II_CONTINUOUS_FROZEN_CAPTURE_V1", "PROSPECTIVE_FROZEN_SOURCE_REQUIRED")
    config = manifest.get("config", {})
    require(config.get("decision_capture_schema") == DECISION_SCHEMA
            and config.get("paper_risk_schema") == RISK_SCHEMA,
            "PROSPECTIVE_DECISION_AND_RISK_INSTRUMENTATION_REQUIRED")
    files = manifest.get("files", {})
    require(isinstance(files, dict) and 1 <= len(files) <= 256
            and {"continuous_decision_capture.py", "continuous_paper_risk.py", "slim_roster.py", "MINIMAL_ROSTER.json"} <= set(files),
            "BOUND_PROSPECTIVE_HELPERS_REQUIRED")
    total = 0
    for name, digest in files.items():
        path = (candidate / name).resolve()
        require(path.is_relative_to(candidate) and not path.is_symlink(), "SOURCE_PATH_ESCAPE")
        total += path.stat().st_size
        require(total <= 32 * MIB and sha(path) == digest, "PROSPECTIVE_SOURCE_CHANGED_OR_OVERSIZED")
    return manifest


def preflight(candidate, run_dir, audit_path, supervisor_path, output, *, expected_manifest_sha256,
              maximum_input_bytes=4 * 1024 ** 3, maximum_events=2_000_000):
    """Validate real sealed artifacts without importing or executing a candidate."""
    candidate, run_dir, audit_path, supervisor_path, output = map(Path, (candidate, run_dir, audit_path, supervisor_path, output))
    candidate, run_dir, audit_path, supervisor_path, output = (path.resolve() for path in (candidate, run_dir, audit_path, supervisor_path, output))
    require(type(maximum_input_bytes) is int and 1 <= maximum_input_bytes <= 64 * 1024 ** 3
            and type(maximum_events) is int and 1 <= maximum_events <= 10_000_000, "EXPLICIT_BOUNDED_REPLAY_LIMITS_REQUIRED")
    require(not output.exists() and not output.is_relative_to(run_dir) and not output.is_relative_to(candidate)
            and not run_dir.is_relative_to(output) and not candidate.is_relative_to(output), "NEW_EXTERNAL_REPLAY_OUTPUT_REQUIRED")
    manifest = verify_candidate(candidate, expected_manifest_sha256)
    capture = run_dir / "capture"
    paths = {"receipt": capture / "receipt.json", "preregistration": capture / "preregistration.json",
             "collection_closed": capture / "COLLECTION_CLOSED.json", "worker_exit": run_dir / "exit.json",
             "worker_identity": run_dir / "operator.json", "raw_audit": audit_path,
             "source_manifest": candidate / "SOURCE_MANIFEST.json", "supervisor_result": supervisor_path}
    sources = {key: binding(path, 8 * MIB) for key, path in paths.items()}
    docs = {key: load(path) for key, path in paths.items()}
    receipt, prereg, marker, exited, operator, audit, supervisor = (
        docs[key] for key in ("receipt", "preregistration", "collection_closed", "worker_exit", "worker_identity", "raw_audit", "supervisor_result"))
    require((receipt.get("state"), receipt.get("reason")) in {("COMPLETE", "DEADLINE"), ("STOPPED", "USER_STOP")},
            "GENUINE_COMPLETE_OR_USER_STOP_ENDPOINT_REQUIRED_LEGACY_FAILURES_INELIGIBLE")
    require(supervisor.get("schema") == "LANE_II_PROSPECTIVE_SUPERVISOR_RESULT_V1"
            and supervisor.get("receipt") == receipt, "GENUINE_PROSPECTIVE_SUPERVISOR_REQUIRED")
    require(supervisor.get("bindings") == {key: sources[key] for key in
            ("receipt", "preregistration", "collection_closed", "worker_exit", "worker_identity", "raw_audit", "source_manifest")},
            "SUPERVISOR_EXACT_ARTIFACT_BINDINGS_REQUIRED")
    require(exited.get("exit_code") == supervisor.get("worker_exit_code") == 0
            and exited.get("forced_closure") is supervisor.get("forced_closure") is False
            and exited.get("retained_handle_exit_observed") is True and exited.get("owned_job_closed") is True
            and exited.get("owned_children_at_parent_exit") == [], "GENUINE_UNFORCED_RETAINED_HANDLE_EXIT_REQUIRED")
    require(type(operator.get("pid")) is int and operator["pid"] == exited.get("pid") == receipt.get("pid") == marker.get("worker_pid")
            and type(operator.get("creation_epoch")) in {int, float} and operator["creation_epoch"] > 0
            and exited.get("creation_epoch") == operator["creation_epoch"]
            and type(operator.get("boot_epoch")) in {int, float} and operator["boot_epoch"] > 0
            and exited.get("boot_epoch") == operator.get("boot_epoch")
            and operator.get("owned_job_before_startup_gate") is True, "EXACT_OWNED_PROCESS_CREATION_IDENTITY_REQUIRED")
    require(marker.get("schema") == "BTC_NEXT12_COLLECTION_CLOSED_V1"
            and all(marker.get(key) is True for key in ("collection_closed", "network_closed", "producers_exited",
                                                        "snapshot_publisher_closed", "finalization_only_no_more_http"))
            and marker.get("state_at_collection_close") == receipt["state"], "GENUINE_PRODUCER_CLOSURE_REQUIRED")
    require(all(doc.get("source_manifest_sha256") == expected_manifest_sha256 for doc in (receipt, prereg, marker, exited, operator))
            and receipt.get("preregistration_sha256") == sources["preregistration"]["sha256"], "CAPTURE_SOURCE_AND_PREREGISTRATION_BINDING")
    start, cutoff, end = (stamp(receipt[key]) for key in ("paper_start_utc", "cutoff_utc", "actual_end_utc"))
    require(start < cutoff and end >= start and stamp(marker["fixed_cutoff_utc"]) == cutoff
            and stamp(prereg["paper_start_utc"]) == start and stamp(prereg["cutoff_utc"]) == cutoff,
            "FIXED_CAPTURE_INTERVAL_BINDING_REQUIRED")
    require(receipt["state"] != "COMPLETE" or end >= cutoff, "COMPLETE_BEFORE_FIXED_DEADLINE")
    command = operator.get("command", [])
    require(len(command) == 13 and Path(command[0]).resolve() == Path(sys.executable).resolve()
            and command[1:4] == ["-I", "-S", "-B"] and Path(command[4]).resolve() == candidate / "study.py"
            and command[5:7] == ["worker", "--run-dir"] and Path(command[7]).resolve() == capture
            and command[8] == "--paper-start-utc" and stamp(command[9]) == start
            and command[10] == "--cutoff-utc" and stamp(command[11]) == cutoff and command[12] == "--startup-gate",
            "EXACT_CAPTURE_INTERPRETER_AND_COMMAND_REQUIRED")
    for document in (receipt, prereg.get("policy", {})):
        require(all(type(document.get(key)) is int and document[key] == 0
                    for key in ("real_orders", "model_api_calls", "authenticated_account_calls")), "EXACT_PAPER_ONLY_COUNTERS_REQUIRED")
    require(prereg.get("policy", {}).get("virtual_cash_per_profile") == 3000, "FROZEN_INDEPENDENT_PAPER_ACCOUNTS_REQUIRED")
    rows = receipt.get("profiles", [])
    require(len(rows) == 2 and {row.get("id") for row in rows} == EXPECTED_IDS
            and set(receipt.get("ledger_sha256", {})) == EXPECTED_IDS
            and set(receipt.get("terminal_censoring", {})) == EXPECTED_IDS, "EXACT_P13_P2_TERMINAL_MEMBERSHIP_REQUIRED")
    cold = supervisor.get("capture_audit", {})
    require(cold.get("status") == "PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY" and cold.get("problems") == []
            and cold.get("events_checked") == receipt.get("events"), "BOUND_FROZEN_COLD_AUDIT_PASS_REQUIRED")
    require(audit.get("schema") == "LANE_II_READONLY_CAPTURE_AUDIT_V1"
            and audit.get("status") == "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY" and audit.get("problem_counts") == {}
            and audit.get("events_checked") == receipt.get("events")
            and audit.get("execution", {}).get("state") == receipt["state"], "BOUND_RAW_AUDIT_PASS_REQUIRED")
    require(sources["receipt"] in audit.get("input_bindings", [])
            and sources["collection_closed"] in audit.get("input_bindings", []), "RAW_AUDIT_EXACT_ENDPOINT_BINDING_REQUIRED")
    require(type(receipt.get("events")) is int and 1 <= receipt["events"] <= maximum_events, "REPLAY_EVENT_COUNT_BOUND")
    total = sum(source["bytes"] for source in sources.values())
    total += sum((candidate / name).stat().st_size for name in manifest["files"])
    for name in ("events.jsonl", "websocket-frames.jsonl", "5m-P13-ledger.jsonl", "5m-P2-ledger.jsonl"):
        total += (capture / name).stat().st_size
    require(total <= maximum_input_bytes, "REPLAY_INPUT_BYTE_BUDGET_EXCEEDED")
    decision_bound = cutoff
    if receipt["state"] == "STOPPED":
        sources["stop_request"] = binding(capture / "STOP", MIB)
        stop = load(capture / "STOP", MIB)
        require(stop.get("schema") == "LANE_II_COORDINATOR_STOP_V1", "GENUINE_COORDINATOR_STOP_REQUIRED")
        decision_bound = stamp(stop["utc"])
        require(start < decision_bound <= min(cutoff, end), "EXACT_STOP_BOUND_REQUIRED")
    return {"candidate": candidate, "capture": capture, "run_dir": run_dir, "output": output,
            "receipt": receipt, "preregistration": prereg, "manifest": manifest, "sources": sources,
            "start": start, "cutoff": cutoff, "observed_end": stamp(receipt["utc"]), "decision_bound": decision_bound,
            "maximum_input_bytes": maximum_input_bytes, "base_input_bytes": total, "maximum_events": maximum_events}


class RawProofs:
    """Bound only retained raw objects, with no fallback or external labels."""

    def __init__(self, capture, maximum_bytes):
        self.capture = Path(capture).resolve()
        self.maximum_bytes = maximum_bytes
        self.bytes = 0
        self.bindings = {}

    def json(self, root, reference):
        require(Path(root).resolve() == self.capture, "NO_EXTERNAL_REPLAY_PROOFS")
        path = (self.capture / reference["file"]).resolve()
        require(path.is_relative_to(self.capture) and not path.is_symlink(), "RAW_PROOF_PATH_ESCAPE")
        require(path.stat().st_size <= 8 * MIB, "RAW_PROOF_SIZE_BOUND")
        with path.open("rb") as stream:
            raw = stream.read(8 * MIB + 1)
        require(len(raw) <= 8 * MIB and hashlib.sha256(raw).hexdigest() == reference["sha256"], "RAW_PROOF_HASH_OR_SIZE")
        old = self.bindings.get(str(path))
        current = {"path": str(path), "bytes": len(raw), "sha256": reference["sha256"]}
        require(old is None or old == current, "RAW_REFERENCE_CHANGED")
        if old is None:
            self.bytes += len(raw)
            require(self.bytes <= self.maximum_bytes, "RAW_PROOF_TOTAL_BYTE_BOUND")
            self.bindings[str(path)] = current
        return json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_RAW_JSON")))

    @contextmanager
    def bind_retained_reader(self):
        """Scope the retained adapters' raw-reader dependency to this capture."""
        base = sys.modules[retained.Replay.__module__]
        prior_study, prior_base = retained.raw_json, base.raw_json
        retained.raw_json = base.raw_json = self.json
        try:
            yield
        finally:
            retained.raw_json, base.raw_json = prior_study, prior_base

    def verify_unchanged(self):
        require(all(binding(row["path"], 8 * MIB) == row for row in self.bindings.values()), "RAW_PROOF_CHANGED_DURING_REPLAY")


class WebsocketTape:
    def __init__(self, path, expected_hash, expected_count, maximum_bytes):
        self.path = Path(path)
        require(self.path.stat().st_size <= maximum_bytes, "WEBSOCKET_TAPE_BYTE_BOUND")
        self.stream = self.path.open("rb")
        self.expected_hash, self.expected_count = expected_hash, expected_count
        self.digest, self.count = hashlib.sha256(), 0

    def next(self, event):
        raw = self.stream.readline(64 * 1024 + 1)
        require(raw and len(raw) <= 64 * 1024 and raw.endswith(b"\n"), "BOUNDED_WEBSOCKET_ROW_REQUIRED")
        self.digest.update(raw)
        row = json.loads(raw)
        self.count += 1
        data = event["data"]
        require(row.get("seq") == data.get("raw_sequence") == self.count
                and row.get("sha256") == data.get("sha256")
                and row.get("receipt_wall") == data.get("receipt_wall"), "WEBSOCKET_JOURNAL_RAW_JOIN")
        decoded = base64.b64decode(row["raw_b64"], validate=True)
        require(len(decoded) <= 16384 and hashlib.sha256(decoded).hexdigest() == row["sha256"], "WEBSOCKET_RAW_PAYLOAD_BINDING")
        return row, decoded

    def finish(self):
        require(not self.stream.read(1) and self.count == self.expected_count
                and self.digest.hexdigest() == self.expected_hash, "EXACT_WEBSOCKET_TAPE_BINDING")

    def close(self):
        self.stream.close()


def journal_rows(path, receipt, maximum_events, maximum_bytes):
    path = Path(path)
    require(path.stat().st_size <= maximum_bytes, "JOURNAL_BYTE_BOUND")
    previous, sequence, monotonic = "0" * 64, 0, 0
    digest = hashlib.sha256()
    last = None
    with path.open("rb") as stream:
        while True:
            raw = stream.readline(8 * MIB + 1)
            if not raw:
                break
            require(len(raw) <= 8 * MIB and raw.endswith(b"\n"), "BOUNDED_COMPLETE_JOURNAL_LINE_REQUIRED")
            sequence += 1
            require(sequence <= maximum_events, "REPLAY_EVENT_COUNT_BOUND")
            digest.update(raw)
            row = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_JOURNAL_JSON")))
            require(row.get("seq") == sequence and row.get("previous_sha256") == previous
                    and type(row.get("monotonic_ns")) is int and row["monotonic_ns"] >= monotonic,
                    "PROSPECTIVE_JOURNAL_CHAIN_OR_MONOTONIC_ORDER")
            require(row.get("sha256") == hashlib.sha256(canonical({k: v for k, v in row.items() if k != "sha256"})).hexdigest(),
                    "PROSPECTIVE_JOURNAL_ROW_HASH")
            previous, monotonic = row["sha256"], row["monotonic_ns"]
            if sequence == 1:
                require(row.get("kind") == "START" and row.get("data", {}).get("manifest_sha256") == receipt["source_manifest_sha256"]
                        and row["data"].get("preregistration_sha256") == receipt["preregistration_sha256"], "PROSPECTIVE_START_BINDING")
            last = row
            row["_verified_raw_bytes"] = len(raw)
            yield row
    require(sequence == receipt["events"] and previous == receipt["chain_sha256"]
            and digest.hexdigest() == receipt["events_sha256"] and last is not None and last.get("kind") == "END"
            and last["data"].get("state") == receipt["state"] and last["data"].get("reason") == receipt["reason"],
            "PROSPECTIVE_END_AND_TAPE_BINDING")


class ReproducibilityHold(RuntimeError):
    """Unobserved runtime behavior must not become a guessed replay outcome."""


def scheduled_diagnostic_witnesses(events):
    """Index direct post-tick diagnostics, never END hashes or parity targets.

    Only the last completed evaluation of an arm in its own scheduled frame
    can be witnessed by that frame's profile row. A later callback invalidates
    that attribution. Inputs themselves are reconstructed later by the engine.
    """
    witnesses, frame, active = {}, None, None
    for event in events:
        kind, data = event["kind"], event["data"]
        if kind == "CAUSAL_MODEL_INPUT":
            frame = {"second": data["second"], "wall": data["construction_start_wall"], "profiles": {}}
        elif kind == "DECISION_BEGIN":
            active = None
            if frame is not None:
                frame["profiles"][data["profile"]] = None
                if (data.get("operation") == "tick" and data.get("trigger") == "SCHEDULED_TICK"
                        and data.get("evaluation_wall") == frame["wall"]):
                    active = {"evaluation_id": data["evaluation_id"], "profile": data["profile"],
                              "market": data["market"], "evaluation_wall": data["evaluation_wall"],
                              "begin_seq": event["seq"], "end_seq": None}
                    frame["profiles"][data["profile"]] = active
        elif kind == "DECISION_END" and active is not None:
            if data.get("evaluation_id") == active["evaluation_id"] and data.get("profile") == active["profile"]:
                active["end_seq"] = event["seq"]
            active = None
        elif kind == "MODEL_AND_PROFILE_TICK":
            if (frame is not None and data.get("second") == frame["second"]
                    and data.get("construction_start_wall") == frame["wall"]):
                rows = data.get("profile_rows", [])
                if len({row.get("id") for row in rows}) == len(rows):
                    for row in rows:
                        match = frame["profiles"].get(row.get("id"))
                        if match is not None and match["end_seq"] is not None and match["end_seq"] < event["seq"]:
                            witnesses[match["evaluation_id"]] = {**match, "result_seq": event["seq"],
                                "result_row_sha256": event["sha256"], "last_problem": row.get("last_problem")}
            frame, active = None, None
    return witnesses


class MissingFeatureDiagnostics:
    """Recover only unordered missing-key presentation from direct evidence.

    The frozen P2 sampler iterates a set of feature names. If several are
    absent, its history clearing and aborted sample are deterministic, but the
    displayed KeyError is process-hash-order dependent. An attributable native
    profile row can establish a partial order among genuinely missing keys.
    Unwitnessed ambiguity remains a HOLD, even if this process happens to emit
    the same key as the captured process. No state hash selects an alternative.
    """
    def __init__(self, witnesses):
        self.witnesses, self.keys, self.edges = witnesses, None, {}
        self.ambiguous, self.direct, self.inferred = 0, 0, 0
        self.missing_sets = Counter()

    def precedes(self, left, right, seen=None):
        seen = set() if seen is None else seen
        if left in seen:
            return False
        seen.add(left)
        return any(a == left and (b == right or self.precedes(b, right, seen)) for a, b in self.edges)

    def select(self, weights, values, begin, *, directly_attributable):
        keys = tuple(weights)
        if self.keys is None:
            self.keys = keys
        if keys != self.keys or set(keys) != {"book_momentum", "depth", "prior_hour", "spot30"}:
            raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_ROSTER_CHANGED")
        missing = set(keys) - values.keys()
        if not missing or any(type(values[key]) not in (int, float) or not math.isfinite(values[key])
                              for key in keys if key in values):
            raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_NONFINITE_OR_UNSUPPORTED_INPUT")
        if len(missing) == 1:
            return next(iter(missing))
        witness = self.witnesses.get(begin["evaluation_id"]) if directly_attributable else None
        if witness is not None:
            if any(witness.get(key) != begin.get(key) for key in ("evaluation_id", "profile", "market", "evaluation_wall")):
                raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_WITNESS_IDENTITY")
            observed = [key for key in missing if witness["last_problem"] == "INPUT:" + str(KeyError(key))]
            if len(observed) != 1:
                raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_WITNESS_NOT_GENUINELY_MISSING")
            selected = observed[0]
            for other in sorted(missing - {selected}):
                if self.precedes(other, selected):
                    raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_ORDER_CONTRADICTION")
                self.edges.setdefault((selected, other), dict(witness))
            self.direct += 1
        else:
            self.inferred += 1
        choices = [key for key in missing if all(key == other or self.precedes(key, other) for other in missing)]
        if len(choices) != 1:
            raise ReproducibilityHold("UNRESOLVED_MISSING_FEATURE_DIAGNOSTIC_ORDER")
        self.ambiguous += 1
        self.missing_sets[tuple(sorted(missing))] += 1
        return choices[0]

    def attach(self, profile, current_begin, candidate):
        original = profile.sample
        function = original.__func__
        comprehensions = {code for code in function.__code__.co_consts
                          if isinstance(code, types.CodeType) and code.co_name == "<dictcomp>"}
        # Python 3.12 inlines comprehensions into the owning function. Bind the
        # exact source expression so a prior/clock KeyError cannot be relabeled.
        lookup = ast.dump(ast.parse("{key: finite(features['values'][key]) for key in keys}", mode="eval").body)
        set_call = ast.dump(ast.parse("set(self.model['score_weights'])", mode="eval").body)
        tree = ast.parse(Path(function.__code__.co_filename).read_text(encoding="utf-8-sig"))
        owner = next((node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                      and node.name == function.__name__ and node.lineno == function.__code__.co_firstlineno), None)
        lookup_lines = set()
        if owner is not None and any(isinstance(node, ast.Call) and ast.dump(node) == set_call for node in ast.walk(owner)):
            for node in ast.walk(owner):
                if isinstance(node, ast.DictComp) and ast.dump(node) == lookup:
                    lookup_lines.update(range(node.lineno, node.end_lineno + 1))

        def sample(market, features, now):
            try:
                return original(market, features, now)
            except (KeyError, ValueError, TypeError, OverflowError) as exc:
                trace, from_comprehension = exc.__traceback__, False
                while trace is not None:
                    from_comprehension |= (trace.tb_frame.f_code in comprehensions or
                                           trace.tb_frame.f_code is function.__code__ and trace.tb_lineno in lookup_lines)
                    trace = trace.tb_next
                values = features.get("values")
                weights = profile.model["score_weights"]
                if not from_comprehension or not isinstance(values, dict) or not set(weights) - values.keys():
                    raise
                if (candidate is None or Path(function.__code__.co_filename).resolve() != Path(candidate).resolve() / "optimized_controls.py"
                        or profile.model.get("id") != "P2" or not isinstance(exc, KeyError)
                        or len(exc.args) != 1 or exc.args[0] not in set(weights) - values.keys()
                        or profile.histories[market["id"]]):
                    raise ReproducibilityHold("MISSING_FEATURE_DIAGNOSTIC_SOURCE_OR_ABORT_NOT_VERIFIED") from exc
                begin = current_begin()
                window = profile.windows.get(market["id"], {})
                direct = (begin["operation"] == "tick" and begin["trigger"] == "SCHEDULED_TICK"
                          and market["id"] not in profile.positions and not window.get("pending"))
                selected = self.select(weights, values, begin, directly_attributable=direct)
                # The original frozen sampler already cleared history and raised.
                # Re-raise an independently absent key; never return a sample.
                raise KeyError(selected) from exc

        profile.sample = sample

    def result(self):
        return {"schema": "LANE_II_MISSING_FEATURE_DIAGNOSTIC_REPLAY_V1", "ambiguous_contexts": self.ambiguous,
                "direct_observations": self.direct, "contexts_using_previously_witnessed_order": self.inferred,
                "missing_sets": [{"keys": list(keys), "contexts": count} for keys, count in sorted(self.missing_sets.items())],
                "order_witnesses": [{"before": left, "after": right, "witness": witness}
                                    for (left, right), witness in sorted(self.edges.items())],
                "unresolved_ambiguities": 0, "end_hash_or_parity_target_used_to_select": False,
                "economic_decision_or_risk_rules_changed": False}


class HardGateChecks:
    """Recompute each actual lifecycle check; never trust an admission Boolean."""

    def __init__(self, evaluation_id, checks, cutoff, deadline_mono):
        self.evaluation_id, self.checks = evaluation_id, deque(checks)
        self.cutoff, self.deadline_mono, self.count = cutoff, deadline_mono, 0
        self.last_mono = None

    def __call__(self):
        return self.raw_check()["hard_allowed"]

    def raw_check(self):
        require(bool(self.checks), "UNRECORDED_REPLAY_HARD_GATE_CHECK")
        row = self.checks.popleft()
        self.count += 1
        require(row.get("evaluation_id") == self.evaluation_id and row.get("index") == self.count,
                "EXACT_ORDERED_HARD_GATE_CHECK_REQUIRED")
        wall, mono = row.get("wall"), row.get("monotonic")
        require(type(wall) in {int, float} and math.isfinite(wall) and type(mono) in {int, float} and math.isfinite(mono)
                and (self.last_mono is None or mono >= self.last_mono)
                and type(row.get("stop_requested")) is bool and type(row.get("stop_event_set")) is bool,
                "HARD_GATE_CLOCK_AND_STOP_FACTS_REQUIRED")
        self.last_mono = mono
        allowed = not row["stop_requested"] and not row["stop_event_set"] and wall < self.cutoff and mono < self.deadline_mono
        require(row.get("hard_allowed") is allowed, "HARD_GATE_RESULT_MISMATCH")
        return {key: row[key] for key in ("wall", "host_wall", "monotonic", "stop_event_set", "stop_requested", "hard_allowed") if key in row}

    def finish(self, expected_count):
        require(not self.checks and self.count == expected_count, "HARD_GATE_CALL_COUNT_MISMATCH")


class MemoryRiskLedger:
    """The same pure guard, driven by independently reconstructed profile calls."""

    def __init__(self, module, initial_state):
        self.module, self.state = module, deepcopy(initial_state)
        self.segment_id = initial_state["segment_id"]
        require(self.state.get("schema") == RISK_SCHEMA and self.state.get("phase") == "ACTIVE"
                and self.state.get("policy_sha256") == module.digest(module.POLICY), "FROZEN_ACTIVE_RISK_STATE_REQUIRED")
        self.actions, self.seen = [], {}

    def apply(self, event_id, action, payload):
        prior = self.seen.get(event_id)
        if prior is not None:
            require(prior["action"] == action and prior["payload"] == payload, "REPLAY_RISK_IDEMPOTENCY_MISMATCH")
            return deepcopy(prior["result"])
        require(self.state["phase"] == "ACTIVE" and self.state["segment_id"] == self.segment_id, "REPLAY_RISK_SEGMENT_CHANGED")
        self.state, result = self.module.transition(self.state, action, payload)
        record = {"event_id": event_id, "action": action, "payload": deepcopy(payload), "result": deepcopy(result)}
        self.actions.append(record)
        self.seen[event_id] = record
        return result


class Coverage:
    def __init__(self, start, cutoff):
        self.start, self.cutoff = start, cutoff
        # Integer-second marks themselves must belong to the paper interval.
        self.expected_seconds = set(range(math.ceil(start), math.ceil(cutoff)))
        self.expected_windows = set(range(int(start) // 300 * 300, math.ceil(cutoff), 300))
        self.seconds = set()
        self.arm_windows = {identity: set() for identity in EXPECTED_IDS}
        self.fresh_bundles = {}
        self.entries = {identity: set() for identity in EXPECTED_IDS}

    def tick(self, second):
        require(type(second) is int and second not in self.seconds, "DUPLICATE_SCHEDULED_TICK_SECOND")
        if second in self.expected_seconds:
            self.seconds.add(second)

    def evaluation(self, begin, market, quotes, mark):
        now = begin["evaluation_wall"]
        identity = begin["profile"]
        if self.start <= now < self.cutoff and begin.get("operation") == "tick":
            self.arm_windows[identity].add(market["start"])
            fresh = (begin.get("enabled") is True and begin.get("entry_allowed") is True
                     and type(mark) in {int, float} and math.isfinite(mark)
                     and market.get("accepting") is True and market.get("closed") is False
                     and 0 <= now - market["metadata_received"] <= 30
                     and market["start"] <= now < market["end"]
                     and all(side in quotes and quotes[side].get("bid") is not None
                             and quotes[side].get("ask") is not None for side in ("Up", "Down")))
            if fresh:
                key = (begin["bundle_id"], market["start"])
                self.fresh_bundles.setdefault(key, set()).add(identity)

    def result(self):
        missing = sorted(self.expected_seconds - self.seconds)
        paired = {window for (_, window), ids in self.fresh_bundles.items() if ids == EXPECTED_IDS}
        return {"schema": "LANE_II_PROSPECTIVE_REPLAY_COVERAGE_V1",
            "integer_second_convention": "ceil(paper_start) <= mark.second < ceil(cutoff); partial boundary seconds not interpolated",
            "expected_scheduled_tick_seconds": len(self.expected_seconds), "distinct_paper_scheduled_tick_seconds": len(self.seconds),
            "missing_scheduled_tick_seconds": len(missing), "missing_second_examples": missing[:128],
            "missing_seconds_sha256": hashlib.sha256(canonical(missing)).hexdigest(),
            "expected_5m_windows": len(self.expected_windows), "expected_5m_window_starts": sorted(self.expected_windows),
            "shared_fresh_eligible_5m_windows": len(paired), "shared_fresh_eligible_window_starts": sorted(paired),
            "per_profile": {identity: {"observed_evaluated_5m_windows": len(self.arm_windows[identity]),
                "observed_window_starts": sorted(self.arm_windows[identity]),
                "true_abstention_5m_windows": len(paired - self.entries[identity]),
                "true_abstention_window_starts": sorted(paired - self.entries[identity])}
                for identity in sorted(EXPECTED_IDS)},
            "trade_quota_used": False, "missing_data_treated_as_abstention": False,
            "full_window_completion_inferred": False}


class ProspectiveReplay(retained.StudyReplay):
    def __init__(self, context, specs, registry, helpers, market_types, model, marks_module,
                 decision_module, risk_module, websocket, event_sink):
        self.context, self.decision_module, self.risk_module = context, decision_module, risk_module
        self.websocket = websocket
        self.feed, self.proxy = model.FeedHistory(), marks_module.MarkHistory()
        self.risk = MemoryRiskLedger(risk_module, context["preregistration"]["paper_risk_initial_state"])
        self.buffer, self.context_events = None, None
        self.buffer_bytes = 0
        self.current_begin, self.checks = None, None
        self.controller = None
        self.bundle = None
        self.bundle_counter = 0
        self.book_admission = None
        self.last_model_input = None
        self.scheduled_results = 0
        self.feed_rejections, self.source_risk_actions = [], []
        self.admissions, self.gate_checks = 0, 0
        self.coverage_evidence = Coverage(context["start"], context["cutoff"])
        self.missing_feature_diagnostics = MissingFeatureDiagnostics(context.get("diagnostic_witnesses", {}))
        super().__init__(context["capture"], context["start"], context["cutoff"], context["decision_bound"],
                         specs, registry, helpers, market_types, event_sink, None, None)
        # The captured controller is source-bound but only receives replay state.
        # It cannot open its SQLite ledger or poll live clocks in this mode.
        self.controller = decision_module.DecisionCapture(self, self.risk, emit=self._context_emit,
            hard_check=lambda: self.checks.raw_check(),
            clock_status=lambda: deepcopy(self.current_begin["clock_control"]),
            transport_status=lambda: deepcopy(self.current_begin["transport_control"]))
        for profile in self.profiles.values():
            self.controller.attach_profile(profile)
            self.missing_feature_diagnostics.attach(profile, lambda: self.current_begin, context.get("candidate"))

    def _context_emit(self, kind, data):
        require(self.context_events is not None, "UNOWNED_REPLAY_CONTEXT_EVENT")
        self.context_events.append({"kind": kind, "data": deepcopy(data)})

    def emit(self, identity, kind, data):
        require(self.context_events is not None, "PROFILE_EVENT_OUTSIDE_PROSPECTIVE_CONTEXT")
        self._context_emit(kind, data)
        super().emit(identity, kind, deepcopy(data))
        if kind == "PAPER_ENTRY":
            self.coverage_evidence.entries[identity].add(self.markets[data["market"]]["start"])
        if self.controller is not None:
            self.controller.financial_event(kind, deepcopy(data))

    def evaluate(self, market, callback=False):
        # BOOK_ADMITTED supplies evidence only. The subsequent prospective BEGIN
        # owns the exact callback clock and whether each profile actually ran.
        return

    def admit_book(self, data):
        super().admit_book(data)
        self.book_admission = {"market": data["market"], "sequence": self.sequence, "bundle_id": None}

    def tick(self, data, event_wall):
        require(self.last_model_input is not None, "MODEL_RESULT_WITHOUT_CAUSAL_INPUT")
        for key in ("second", "mark", "forecasts", "construction_start_wall", "construction_start_monotonic_ns"):
            require(data.get(key) == self.last_model_input.get(key), "SCHEDULED_MODEL_INPUT_RESULT_MISMATCH:" + key)
        require(data["construction_start_wall"] <= data["feature_available_wall"] <= event_wall + 1e-6
                and data["construction_start_monotonic_ns"] <= data["feature_available_monotonic_ns"], "SCHEDULED_RESULT_CLOCK_ORDER")
        self.scheduled_results += 1
        self.counts["model_ticks"] += 1
        self.last_model_input = None

    def _model_input(self, event):
        data = event["data"]
        require(self.last_model_input is None, "UNFINISHED_CAUSAL_MODEL_INPUT")
        second, begin = data["second"], data["construction_start_wall"]
        require(type(second) is int and second <= begin <= stamp(event["utc"]) + 1e-6
                and begin < self.decision_bound and begin <= data["available_wall"] <= stamp(event["utc"]) + 1e-6
                and data["construction_start_monotonic_ns"] <= data["available_monotonic_ns"], "CAUSAL_MODEL_CONSTRUCTION_CLOCK")
        mark = self.feed.sample(second)
        require(mark == data["mark"], "INDEPENDENT_RAW_SPOT_MARK_MISMATCH")
        forecasts, labels = self.proxy.tick(second, mark)
        require(forecasts == data["forecasts"] == {} and labels == [], "FROZEN_SLIM_MARK_HISTORY_REQUIRED")
        self.marks, self.forecasts = self.proxy.marks, forecasts
        self.last_model_second = second
        self.last_model_input = deepcopy(data)
        self.coverage_evidence.tick(second)
        self.counts["independently_reconstructed_marks"] += 1

    def _websocket(self, event):
        record, decoded = self.websocket.next(event)
        if event["data"]["raw_only"]:
            return
        require(record["receipt_wall"] < self.decision_bound, "LATE_CAUSAL_WEBSOCKET_FRAME")
        try:
            if record["binary"]:
                raise ValueError("BINARY_FRAME")
            body = json.loads(decoded)
            typ = body.get("type")
            if typ in ("ticker", "heartbeat"):
                if body.get("product_id") != "BTC-USD":
                    raise ValueError("EXACT_BTC_USD")
                source = stamp(body["time"])
                if typ == "ticker":
                    self.feed.ticker(body["price"], body["trade_id"], source, record["receipt_wall"], record["seq"])
                else:
                    self.feed.heartbeat(body["last_trade_id"], source, record["receipt_wall"], record["seq"])
            elif typ != "subscriptions":
                raise ValueError("UNEXPECTED_WS_FRAME")
        except (ValueError, KeyError, TypeError) as exc:
            self.feed_rejections.append("WS_REJECTED:" + str(exc))

    def _controls(self, begin):
        clock, transport = begin["clock_control"], begin["transport_control"]
        require(clock.get("state") in {"accepted", "held"} and transport.get("state") in {"healthy", "held"},
                "RECORDED_OPERATIONAL_CONFIDENCE_REQUIRED")
        if clock["state"] == "accepted":
            anchor = clock.get("anchor", {})
            require(type(clock.get("anchor_age_seconds")) in {int, float} and 0 <= clock["anchor_age_seconds"] <= 600
                    and clock.get("reason") is None and clock.get("host_clock_modified") is False
                    and all(type(anchor.get(key)) in {int, float} and math.isfinite(anchor[key])
                            for key in ("monotonic", "utc_epoch", "host_epoch", "correction_seconds"))
                    and abs(anchor["correction_seconds"]) <= 60
                    and abs(anchor["utc_epoch"] - anchor["host_epoch"] - anchor["correction_seconds"]) <= 1e-6,
                    "ACCEPTED_CLOCK_FACTS_NOT_VERIFIED")
        require(transport.get("maximum_attempts_per_request") in {1, 2, 3}
                and 0 < transport.get("total_request_seconds", 0) <= 30
                and transport.get("minimum_entry_spacing_seconds", 0) >= .501,
                "FROZEN_TRANSPORT_BOUNDS_REQUIRED")

    def _inputs(self, begin):
        market = self.markets[begin["market"]]
        if begin["operation"] != "tick":
            return market, {}, {}, {}
        now = begin["evaluation_wall"]
        key = begin["bundle_id"]
        if self.bundle is None or key != self.bundle["id"]:
            require(type(key) is int and key > self.bundle_counter, "MONOTONIC_SHARED_INPUT_BUNDLE_REQUIRED")
            self.bundle_counter = key
            prior = self.prior.direction(market["start"], market["end"] - market["start"], self.marks, now)
            quotes = self.registry.qualified_quotes(self.books.get(market["id"], {}), now)
            forecast = self.forecasts.get(str(market["end"] - market["start"]))
            features = self.helpers.live_features(market, quotes, self.book_history.setdefault(market["id"], []),
                                                 self.marks, forecast, prior, now)
            features["forecast"] = forecast
            self.bundle = {"id": key, "market": market["id"], "wall": now,
                           "quotes": quotes, "features": features, "prior": prior, "profiles": set()}
        bundle = self.bundle
        require(bundle["market"] == market["id"] and bundle["wall"] == now
                and begin["profile"] not in bundle["profiles"], "SHARED_CAUSAL_BUNDLE_IDENTITY")
        bundle["profiles"].add(begin["profile"])
        return market, bundle["quotes"], bundle["features"], bundle["prior"]

    def _context(self, rows):
        begin, end = rows[0]["data"], rows[-1]["data"]
        require(begin.get("schema") == end.get("schema") == DECISION_SCHEMA
                and begin["evaluation_id"] == end["evaluation_id"] and begin["profile"] == end["profile"]
                and begin["market"] == end["market"] and begin["profile"] in EXPECTED_IDS,
                "EXACT_PROSPECTIVE_DECISION_CONTEXT_REQUIRED")
        self._controls(begin)
        operation, now = begin["operation"], begin["evaluation_wall"]
        require(operation in {"tick", "settlement", "finalize_window", "terminal_cancel"}
                and type(now) in {int, float} and math.isfinite(now)
                and now <= stamp(rows[0]["utc"]) + 1e-6, "PROSPECTIVE_DECISION_CLOCK_OR_OPERATION")
        if operation in {"tick", "settlement"}:
            require(now < self.decision_bound, "DECISION_AFTER_CAPTURE_BOUND")
        if operation == "tick":
            if begin["trigger"] == "SCHEDULED_TICK":
                require(self.last_model_input is not None and now == self.last_model_input["construction_start_wall"],
                        "SCHEDULED_DECISION_REQUIRES_EXACT_CAUSAL_MODEL_INPUT")
            elif begin["trigger"] == "BOOK_CALLBACK":
                require(self.book_admission is not None and self.book_admission["market"] == begin["market"]
                        and self.book_admission["bundle_id"] in (None, begin["bundle_id"]), "CALLBACK_REQUIRES_NEW_BOOK_ADMISSION")
                self.book_admission["bundle_id"] = begin["bundle_id"]
            else:
                raise ValueError("UNREGISTERED_TICK_TRIGGER")
        market, quotes, features, prior = self._inputs(begin)
        require(begin["input_hashes"] == {key: self.risk_module.digest(value) for key, value in
                {"market": market, "quotes": quotes, "features": features, "prior": prior}.items()},
                "INDEPENDENT_CAUSAL_INPUT_HASH_MISMATCH")
        checks = [row["data"] for row in rows if row["kind"] == "DECISION_GATE_CHECK"]
        self.checks = HardGateChecks(begin["evaluation_id"], checks, self.cutoff,
                                     self.context["preregistration"]["deadline_mono"])
        self.current_begin, self.context_events = begin, []
        self.clock, self.sequence = now, rows[0]["seq"]
        profile = self.profiles[begin["profile"]]
        if operation == "tick":
            self.coverage_evidence.evaluation(begin, market, quotes, self.marks.get(int(now)))
            self.controller.invoke_tick(profile, market, deepcopy(quotes), deepcopy(features), deepcopy(prior), now,
                self.cutoff, begin["enabled"], trigger=begin["trigger"], bundle_id=begin["bundle_id"])
        elif operation == "settlement":
            refs, winner = begin["raw_refs"], begin["winner"]
            responses = [self.response(ref) for ref in refs]
            require(len(responses) == 2 and [row["job"]["kind"] for row in responses] == ["LABEL_GAMMA", "LABEL_CLOB"]
                    and all(row["job"]["data"]["id"] == market["id"] for row in responses)
                    and max(market["end"], *(row["received_wall"] for row in responses)) <= begin["available"] <= now,
                    "SETTLEMENT_CAUSAL_DUAL_PROOF_JOIN")
            side = next(side for side, token in market["tokens"].items() if token == winner)
            self.verified_label(market, side, self.capture, refs, "PROSPECTIVE_DUAL_PROOF")
            self.controller.invoke_settlement(profile, market["id"], winner, market, begin["available"], refs,
                                              bundle_id=begin["bundle_id"])
        else:
            self.controller.invoke_finalize(profile, market["id"], market, now=now, bundle_id=begin["bundle_id"],
                                            terminal=operation == "terminal_cancel")
        self.checks.finish(end["hard_check_count"])
        observed = [{"kind": row["kind"], "data": row["data"]} for row in rows]
        require(canonical(observed) == canonical(self.context_events), "EXACT_DECISION_RISK_AND_STATE_PARITY_MISMATCH")
        self.source_risk_actions.extend(row["data"] for row in rows if row["kind"] == "PAPER_RISK_ACTION")
        self.admissions += 1
        self.gate_checks += len(checks)
        self.context_events, self.current_begin, self.checks = None, None, None
        self.sequence, self.clock = rows[-1]["seq"], stamp(rows[-1]["utc"])
        self.last_event_wall = self.clock

    def consume(self, event):
        kind, data = event["kind"], event["data"]
        if self.buffer is not None:
            require(kind != "DECISION_BEGIN", "NESTED_SOURCE_DECISION_CONTEXT")
            self.buffer_bytes += event.get("_verified_raw_bytes", len(canonical(event)))
            require(self.buffer_bytes <= 16 * MIB, "DECISION_CONTEXT_MEMORY_BOUND")
            self.buffer.append(event)
            require(len(self.buffer) <= 4096, "DECISION_CONTEXT_EVENT_BOUND")
            if kind == "DECISION_END":
                rows, self.buffer = self.buffer, None
                self._context(rows)
            return
        if kind == "DECISION_BEGIN":
            self.buffer_bytes = event.get("_verified_raw_bytes", len(canonical(event)))
            require(self.buffer_bytes <= 16 * MIB, "DECISION_CONTEXT_MEMORY_BOUND")
            self.buffer = [event]
            return
        require(kind not in {"DECISION_END", "DECISION_GATE_CHECK", "PAPER_RISK_ACTION", *retained.FINANCIAL_FIELDS, *retained.DECISIONS},
                "UNOWNED_PROSPECTIVE_DECISION_OR_RISK_EVENT")
        if kind == "CAUSAL_MODEL_INPUT":
            self._model_input(event)
        elif kind == "WS_RAW":
            self._websocket(event)
        elif kind in {"WS_CONNECTED", "WS_DISCONNECTED"} and stamp(event["utc"]) < self.decision_bound:
            self.feed.connection(data["receipt_wall"], kind == "WS_CONNECTED")
        elif kind == "DATA_GAP" and str(data.get("reason", "")).startswith("WS_REJECTED:"):
            require(self.feed_rejections and self.feed_rejections.pop(0) == data["reason"], "RAW_FEED_REJECTION_PARITY")
        elif kind == "VENUE_LABEL_ADMITTED":
            require(data["market"] in self.labels and self.labels[data["market"]]["winning_token"] == data["winner"],
                    "LABEL_ADMISSION_WITHOUT_PROSPECTIVE_SETTLEMENT_CONTEXT")
            # The exact profile settlement already ran in its recorded context.
            event = {**event, "kind": "PROSPECTIVE_LABEL_VERIFIED"}
        super().consume(event)

    def terminal_results(self, observed_wall):
        require(self.buffer is None and self.last_model_input is None and not self.feed_rejections,
                "UNFINISHED_PROSPECTIVE_CAUSAL_CONTEXT")
        result = {}
        for identity, profile in self.profiles.items():
            row = self.profile_snapshot(identity, profile, observed_wall)
            result[identity] = {**row, "id": identity, "economics": self.economics[identity],
                "coverage": dict(self.coverage[identity]), "unresolved_markets": sorted(profile.positions),
                "unresolved_inventory": retained.inventory(profile.positions.values()), "financial_event_count": len(self.financial[identity])}
        return result


def verify_initial_risk(context, risk_module, *, require_terminal=True):
    prereg, sources = context["preregistration"], context["sources"]
    initial = prereg["paper_risk_initial_state"]
    anchor, anchor_sha = prereg["paper_risk_anchor"], prereg["paper_risk_anchor_sha256"]
    require(risk_module.digest(anchor) == anchor_sha and anchor["policy"] == risk_module.POLICY
            and anchor["policy_sha256"] == risk_module.digest(risk_module.POLICY)
            and all(initial[key] == anchor[key] for key in ("database_id", "campaign_id", "roster_sha256", "maximum_loss", "policy_sha256")),
            "INITIAL_RISK_ANCHOR_BINDING_REQUIRED")
    require(initial["phase"] == "ACTIVE" and not initial["reservations"] and not initial["holds"]
            and prereg["initial_account_carryover"] == initial["carry_accounts"], "INITIAL_RISK_CONTINUITY_OR_OPEN_LIABILITY")
    expected = deepcopy(initial)
    if initial["last_terminal"] is None:
        expected.update(phase="GENESIS", segment_id=None)
        require(risk_module.digest(expected) == anchor["genesis_state_sha256"], "RISK_GENESIS_OR_FRESH_ACCOUNT_RESET")
    else:
        endpoint = initial["last_terminal"]
        crashed = endpoint.get("kind") == "CRASH_HANDOFF"
        previous = Path(endpoint["handoff_path"] if crashed else endpoint["receipt_path"]).resolve()
        source = binding(previous, 8 * MIB)
        require(source["sha256"] == endpoint["handoff_sha256" if crashed else "receipt_sha256"], "PREDECESSOR_RISK_RECEIPT_CHANGED")
        prior = load(previous)
        if crashed:
            helper_path = context["candidate"] / "continuous_capture_checkpoint.py"
            require("continuous_capture_checkpoint.py" in context["manifest"]["files"], "FROZEN_CRASH_VALIDATOR_REQUIRED")
            helper = sys.modules["next12_imports"].bound_module("continuous_capture_checkpoint", helper_path)
            validated = helper.validate_crash_handoff(previous, anchor_sha)
            require(validated["valid"] is True and validated["qualified"] is False and validated["flat"] is True
                    and validated["closed_state"]["last_terminal"] == endpoint
                    and context["start"] >= validated["restart_not_before_epoch"]
                    and context["cutoff"] == validated["fixed_cutoff_epoch"], "CRASH_PREDECESSOR_FIXED_CONTINUITY")
            expected = deepcopy(validated["closed_state"])
            expected.update(phase="ACTIVE", segment_id=initial["segment_id"])
            require(prereg.get("paper_risk_initial_action_cursor") == prior["paper_risk_action_cursor"], "CRASH_PREDECESSOR_ACTION_CURSOR")
        else:
            expected = deepcopy(prior["paper_risk_terminal_state"])
            expected.update(phase="ACTIVE", segment_id=initial["segment_id"], last_terminal=endpoint,
                            carry_accounts=prior["account_carryover"])
        require(expected == initial, "PREDECESSOR_RISK_AND_ACCOUNT_CONTINUITY_MISMATCH")
        sources["previous_crash_handoff" if crashed else "previous_risk_receipt"] = source
    if not require_terminal:
        return None
    closure_path = context["run_dir"] / "RISK_CLOSURE.json"
    sources["risk_closure"] = binding(closure_path, 8 * MIB)
    closure = risk_module.validate_closure(context["run_dir"], anchor_sha)
    require(closure["valid"] is True, "GENUINE_RISK_CLOSURE_REQUIRED")
    return closure


def risk_result(engine, receipt, closure):
    risk = engine.risk_module
    require(engine.risk.state == receipt["paper_risk_terminal_state"]
            and risk.snapshot(engine.risk.state) == receipt["paper_risk"], "TERMINAL_INDEPENDENT_RISK_STATE_MISMATCH")
    require(len(engine.source_risk_actions) == len(engine.risk.actions)
            and all(canonical(left) == canonical(right) for left, right in zip(engine.source_risk_actions, engine.risk.actions)),
            "COMPLETE_RISK_ACTION_PARITY_REQUIRED")
    carry = engine.decision_module.account_carryover(list(engine.profiles.values()), engine.controller.initial_accounts)
    require(carry == receipt["account_carryover"], "TERMINAL_ACCOUNT_CARRYOVER_PARITY_REQUIRED")
    reservations = engine.risk.state["reservations"]
    unowned = []
    for identity, profile in engine.profiles.items():
        for market_id, position in profile.positions.items():
            matches = [(key, row) for key, row in reservations.items()
                       if row["profile"] == identity and row["market"] == market_id and row["state"] == "OPEN"]
            if len(matches) != 1:
                unowned.append({"profile": identity, "market": market_id, "reason": "ONE_OPEN_RESERVATION_REQUIRED"})
                continue
            _, row = matches[0]
            if (row["side"] != position["side"] or any(abs(risk.decimal(row[key]) - risk.decimal(position[key])) > risk.EPS
                                                       for key in ("quantity", "basis", "entry_fee"))):
                unowned.append({"profile": identity, "market": market_id, "reason": "RESERVATION_POSITION_ECONOMICS_MISMATCH"})
    for key, row in reservations.items():
        if row["state"] == "OPEN" and row["market"] not in engine.profiles[row["profile"]].positions:
            unowned.append({"reservation_id": key, "reason": "OPEN_RESERVATION_WITHOUT_POSITION"})
    require(not unowned, "UNOWNED_TERMINAL_PAPER_LIABILITY")
    snapshot = risk.snapshot(engine.risk.state)
    return {"pass": True, "actions_compared": len(engine.risk.actions), "initial_state_and_predecessor_verified": True,
            "terminal_state_verified": True, "terminal_snapshot": snapshot, "immutable_closure": closure,
            "account_carryover_verified": True, "flat": closure["flat"], "unowned_liabilities": unowned,
            "open_reservation_ids": sorted(key for key, row in reservations.items() if row["state"] == "OPEN"),
            "unknown_reservation_ids": sorted(key for key, row in reservations.items() if row["state"] == "UNKNOWN"),
            "working_reservation_ids": sorted(key for key, row in reservations.items() if row["state"] == "RESERVED"),
            "profits_refill_risk_cap": False, "account_reset_performed": False}


def write_new(path, value):
    raw = canonical(value) + b"\n"
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        import os
        os.fsync(stream.fileno())


def run(candidate, run_dir, audit_path, supervisor_path, output, *, expected_manifest_sha256,
        maximum_input_bytes=4 * 1024 ** 3, maximum_events=2_000_000):
    with offline_boundary():
        context = preflight(candidate, run_dir, audit_path, supervisor_path, output,
                            expected_manifest_sha256=expected_manifest_sha256,
                            maximum_input_bytes=maximum_input_bytes, maximum_events=maximum_events)
        candidate, capture, output = context["candidate"], context["capture"], context["output"]
        receipt, prereg, sources = context["receipt"], context["preregistration"], context["sources"]
        sys.dont_write_bytecode = True
        sys.path.insert(0, str(candidate))
        shim, helpers, market_types = retained.load_runtime(candidate)
        registry = shim.bound_module("prospective_replay_roster", candidate / "roster.py")
        decision_module = shim.bound_module("continuous_decision_capture", candidate / "continuous_decision_capture.py")
        risk_module = shim.bound_module("continuous_paper_risk", candidate / "continuous_paper_risk.py")
        marks_module = shim.bound_module("prospective_replay_marks", candidate / "lean_marks.py")
        model = sys.modules["model"]
        specs = registry.profile_specs()
        require(specs == prereg["profiles"] and len(specs) == 2 and {row["id"] for row in specs} == EXPECTED_IDS
                and prereg.get("decision_capture_schema") == decision_module.SCHEMA == DECISION_SCHEMA
                and risk_module.SCHEMA == RISK_SCHEMA, "EXACT_FROZEN_PROSPECTIVE_PROFILE_RULES_REQUIRED")
        closure = verify_initial_risk(context, risk_module)
        context["diagnostic_witnesses"] = scheduled_diagnostic_witnesses(
            journal_rows(capture / "events.jsonl", receipt, maximum_events, maximum_input_bytes))
        observed = {}
        for identity in sorted(EXPECTED_IDS):
            path = capture / (identity + "-ledger.jsonl")
            sources["ledger:" + identity] = binding(path, 64 * MIB)
            observed[identity] = retained.capture_ledger(path, receipt["ledger_sha256"][identity])
        sources["python_executable"] = binding(sys.executable, 32 * MIB)
        drivers = [binding(__file__, 2 * MIB), binding(retained.__file__, 2 * MIB),
                   binding(sys.modules[retained.Replay.__module__].__file__, 2 * MIB)]
        output.mkdir(parents=True, exist_ok=False)
        write_new(output / "INPUTS.json", {"schema": "LANE_II_PROSPECTIVE_REPLAY_INPUTS_V1", "sources": sources,
            "drivers": drivers, "profiles": specs, "source_state": receipt["state"],
            "maximum_input_bytes": maximum_input_bytes, "maximum_events": maximum_events,
            "network_allowed": False, "supplemental_labels_allowed": False, "new_strategy_variants": []})
        raw = RawProofs(capture, maximum_input_bytes - context["base_input_bytes"])
        websocket = WebsocketTape(capture / "websocket-frames.jsonl", receipt["websocket_sha256"], receipt["ws_frames"], maximum_input_bytes)
        output_bytes = 0
        try:
            with (output / "REPLAY_EVENTS.jsonl").open("xb") as stream, raw.bind_retained_reader():
                def emit(row):
                    nonlocal output_bytes
                    encoded = canonical(row) + b"\n"
                    output_bytes += len(encoded)
                    require(output_bytes <= maximum_input_bytes, "REPLAY_OUTPUT_BYTE_BOUND")
                    stream.write(encoded)
                engine = ProspectiveReplay(context, specs, registry, helpers, market_types, model, marks_module,
                                           decision_module, risk_module, websocket, emit)
                count = 0
                for event in journal_rows(capture / "events.jsonl", receipt, maximum_events, maximum_input_bytes):
                    engine.consume(event)
                    count += 1
                websocket.finish()
                replayed = engine.terminal_results(context["observed_end"])
        finally:
            websocket.close()
        observed_rows = {row["id"]: row for row in receipt["profiles"]}
        parity, decisions = {}, {}
        for identity in sorted(EXPECTED_IDS):
            row = retained.compare_parity(observed[identity]["financial"], engine.financial[identity], observed_rows[identity], replayed[identity])
            retained_inventory = retained.inventory(receipt["terminal_censoring"][identity])
            row["terminal_inventory_matches"] = retained_inventory == replayed[identity]["unresolved_inventory"]
            row["pass"] = row["pass"] and row["terminal_inventory_matches"]
            parity[identity] = row
            decisions[identity] = retained.compare_events(observed[identity]["decisions"], engine.decisions[identity])
        risk_parity = risk_result(engine, receipt, closure)
        require(all(binding(row["path"]) == row for row in [*sources.values(), *drivers]), "REPLAY_BOUND_METADATA_CHANGED")
        raw.verify_unchanged()
        verify_candidate(candidate, expected_manifest_sha256)
        passed = all(row["pass"] for row in [*parity.values(), *decisions.values()])
        result = {"schema": "LANE_II_PROSPECTIVE_REPLAY_RESULT_V1", "pass": passed,
            "source_state": receipt["state"], "source_reason": receipt["reason"], "source_events": count,
            "source_events_sha256": receipt["events_sha256"], "source_profile_count": 2, "replayed_original_profile_count": 2,
            "source_profiles_with_financial_parity": sum(row["pass"] for row in parity.values()),
            "source_profiles_with_recorded_decision_parity": sum(row["pass"] for row in decisions.values()),
            "parity": parity, "recorded_decision_parity": decisions, "profiles": replayed,
            "admission_parity": {"pass": True, "contexts": engine.admissions, "hard_gate_checks": engine.gate_checks,
                                 "exact_input_and_terminal_state_hashes": True, "historical_callback_approximation_used": False},
            "raw_feed_reconstruction": {"pass": True, "websocket_frames": websocket.count,
                "independently_reconstructed_marks": engine.counts["independently_reconstructed_marks"],
                "completed_model_ticks": engine.scheduled_results, "external_or_interpolated_marks": False},
            "risk_parity": risk_parity, "coverage": engine.coverage_evidence.result(),
            "missing_feature_diagnostics": engine.missing_feature_diagnostics.result(),
            "input_manifest": binding(output / "INPUTS.json"), "replay_events": binding(output / "REPLAY_EVENTS.jsonl"),
            "raw_proofs_read": len(raw.bindings), "raw_proof_bytes_read": raw.bytes,
            "accounts_nonadditive": True, "network_requests": 0, "model_api_calls": 0, "real_orders": 0,
            "full_duration_qualified": False, "strategy_edge_qualified": False, "funded_authority": False,
            "limits": ["Parity is prospective execution evidence; source coverage and statistical support remain separate gates.",
                       "Public request scheduling is fixed to the observed run; replay does not infer counterfactual venue capacity.",
                       "No missing mark, unobserved label, open basis or lost tick is filled in."]}
        write_new(output / "RESULT.json", result)
        write_new(output / "ARTIFACT_MANIFEST.json", {"schema": "LANE_II_PROSPECTIVE_REPLAY_ARTIFACTS_V1",
            "artifacts": [binding(output / name) for name in ("INPUTS.json", "REPLAY_EVENTS.jsonl", "RESULT.json")]})
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--raw-audit", type=Path, required=True)
    parser.add_argument("--supervisor-result", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-input-bytes", type=int, default=4 * 1024 ** 3)
    parser.add_argument("--maximum-events", type=int, default=2_000_000)
    args = parser.parse_args()
    result = run(args.candidate, args.run_dir, args.raw_audit, args.supervisor_result, args.output,
                 expected_manifest_sha256=args.manifest_sha256, maximum_input_bytes=args.maximum_input_bytes,
                 maximum_events=args.maximum_events)
    print(json.dumps({key: result[key] for key in ("schema", "pass", "source_state", "source_events",
          "source_profiles_with_financial_parity", "source_profiles_with_recorded_decision_parity", "coverage")}, sort_keys=True))
    return 0 if result["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
