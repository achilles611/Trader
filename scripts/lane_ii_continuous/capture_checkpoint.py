"""Opt-in producer durability barriers and conservative paper crash handoffs.

The producer writes only small metadata at an already quiescent loop boundary.
Consumers validate/hash the bounded prefixes separately. No checkpoint invents
a terminal receipt or promotes an interrupted attempt to complete coverage.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time

try:
    import continuous_paper_risk as risk
    from continuous_decision_capture import account_carryover
except ImportError:
    from . import paper_risk as risk
    from .decision_capture import account_carryover

SCHEMA = "LANE_II_CAPTURE_CHECKPOINT_V1"
CRASH_SCHEMA = "LANE_II_CRASH_HANDOFF_V1"
STREAMS = ("events.jsonl", "websocket-frames.jsonl", "5m-P13-ledger.jsonl", "5m-P2-ledger.jsonl")
MAX_MARKER_BYTES = 2 * 1024**2
MAX_TAIL_BYTES = 32 * 1024**2
MAX_LINE_BYTES = 1024**2
ZERO_HASH = "0" * 64


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def read(path, limit=MAX_MARKER_BYTES):
    path = Path(path)
    risk.require(path.stat().st_size <= limit, "CHECKPOINT_DOCUMENT_BOUND")
    raw = path.read_bytes()
    return json.loads(raw), {"path": str(path.resolve()), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def publish(path, value, *, replace=False):
    """Publish fsynced bytes atomically; immutable names can never be replaced."""
    path = Path(path)
    raw = risk.canonical(value)
    risk.require(len(raw) <= MAX_MARKER_BYTES, "CHECKPOINT_DOCUMENT_BOUND")
    temporary = path.with_name("." + path.name + ".tmp")
    with temporary.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    try:
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"path": str(path.resolve()), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


class CheckpointWriter:
    def __init__(self, study, *, interval=0, recovery_sync=False, origin_epoch=None):
        risk.require(interval in (0, 900) and (not recovery_sync or interval == 900), "CHECKPOINT_POLICY")
        origin = study.paper_start if origin_epoch is None else origin_epoch
        risk.require(isinstance(origin, (int, float)) and math.isfinite(origin) and origin <= study.paper_start
                     and (origin_epoch is None or origin % 900 == 0), "CHECKPOINT_GRID_ORIGIN")
        self.study, self.interval, self.recovery_sync = study, interval, recovery_sync
        self.policy = {"schema": SCHEMA, "enabled": bool(interval), "interval_seconds": interval,
                       "recovery_at_existing_sync": bool(recovery_sync), "cadence_origin_epoch": origin,
                       "fixed_cutoff_epoch": study.cutoff, "partial_contexts_permitted": False}
        self.directory = study.root / "checkpoints"
        self.next_epoch = origin + (math.floor((study.paper_start - origin) / interval) + 1) * interval if interval else None
        self.sequence = self.durability_sequence = 0
        self.previous = ZERO_HASH
        self.model_pending = None
        self.finished = False
        self.final_binding = None
        if interval:
            self.directory.mkdir(exist_ok=False)

    @classmethod
    def from_study(cls, study):
        value = os.environ.get("LANE_II_CAPTURE_CHECKPOINT_SECONDS", "0")
        recovery = os.environ.get("LANE_II_CAPTURE_RECOVERY_SYNC", "0")
        origin = os.environ.get("LANE_II_CAPTURE_CHECKPOINT_ORIGIN_EPOCH")
        risk.require(value in ("0", "900") and recovery in ("0", "1"), "CHECKPOINT_POLICY")
        return cls(study, interval=int(value), recovery_sync=recovery == "1", origin_epoch=None if origin is None else int(origin))

    def model_started(self, second):
        if self.interval:
            risk.require(self.model_pending is None, "CHECKPOINT_NESTED_MODEL_INPUT")
            self.model_pending = second

    def model_finished(self, second):
        if self.interval:
            risk.require(self.model_pending == second, "CHECKPOINT_MODEL_INPUT_PAIR")
            self.model_pending = None

    def _snapshot(self, kind, scheduled_epoch, now, *, producers_exited=None):
        study = self.study
        risk.require(not self.finished and study.decision_capture.active is None and self.model_pending is None,
                     "CHECKPOINT_PARTIAL_CONTEXT")
        handles = {"events.jsonl": study.log.handle, "websocket-frames.jsonl": study.ws_file,
                   **{identity + "-ledger.jsonl": handle for identity, handle in study.ledgers.items()}}
        risk.require(set(handles) == set(STREAMS), "CHECKPOINT_STREAM_SET")
        # All four streams have one main-loop writer. Receiver threads enqueue
        # observations but never write these handles or mutate paper accounts.
        for handle in handles.values():
            handle.flush(); os.fsync(handle.fileno())
        streams = {}
        for name, handle in handles.items():
            info = os.fstat(handle.fileno())
            risk.require(info.st_size == handle.tell(), "CHECKPOINT_UNFLUSHED_OFFSET")
            streams[name] = {"offset": info.st_size, "device": info.st_dev, "inode": info.st_ino}
        ledger = study.decision_capture.ledger
        risk.require(ledger._read() == ledger.state, "CHECKPOINT_RISK_CHANGED_EXTERNALLY")
        self.durability_sequence += 1
        return {"schema": SCHEMA, "kind": kind, "scheduled_epoch": scheduled_epoch,
                "captured_utc": utc(now), "captured_epoch": now, "captured_monotonic": time.monotonic(),
                "checkpoint_sequence": None if kind == "RECOVERY" else self.sequence + 1,
                "durability_sequence": self.durability_sequence, "previous_checkpoint_sha256": self.previous,
                "segment_id": ledger.state["segment_id"], "source_manifest_sha256": study.manifest,
                "paper_start_epoch": study.paper_start, "fixed_cutoff_epoch": study.cutoff,
                "cadence_origin_epoch": self.policy["cadence_origin_epoch"],
                "streams": streams, "event_seq": study.log.seq, "chain_sha256": study.log.chain,
                "ws_count": study.ws_count, "paper_risk_state": deepcopy(ledger.state),
                "paper_risk_state_sha256": risk.digest(ledger.state), "paper_risk_action_cursor": ledger.action_cursor(),
                "paper_risk_anchor_sha256": hashlib.sha256(ledger.manifest_path.read_bytes()).hexdigest(),
                "account_carryover": account_carryover(study.profiles, study.decision_capture.initial_accounts),
                "pending_intents": {p.id: sorted(key for key, row in p.windows.items() if row.get("pending") is not None) for p in study.profiles},
                "safe_loop_boundary": True, "producers_exited": producers_exited,
                "terminal_state": study.state if kind == "FINAL" else None,
                "terminal_reason": study.reason if kind == "FINAL" else None}

    def boundary(self, *, recovery=False, now=None):
        if not self.interval or self.finished:
            return None
        now = time.time() if now is None else now
        due = now >= self.next_epoch and self.next_epoch < self.study.cutoff
        if not due and not (recovery and self.recovery_sync):
            return None
        if due:
            # A delayed barrier retains its real capture time. Never manufacture
            # several apparently independent windows at the same byte offset.
            risk.require(now < self.next_epoch + self.interval, "CHECKPOINT_CADENCE_GAP")
            packet = self._snapshot("SCHEDULED", self.next_epoch, now)
            binding = publish(self.directory / (str(int(self.next_epoch)) + ".json"), packet)
            self.sequence += 1; self.previous = binding["sha256"]; self.next_epoch += self.interval
            return binding
        packet = self._snapshot("RECOVERY", None, now)
        return publish(self.directory / "RECOVERY.json", packet, replace=True)

    def finalize(self, *, producers_exited, now=None):
        if not self.interval:
            return None
        now = time.time() if now is None else now
        if self.study.decision_capture.active is not None or self.model_pending is not None:
            # Preserve a failed/incomplete final tail without asserting that it
            # is a usable checkpoint. Normal failure closeout may still finish.
            handles = {"events.jsonl": self.study.log.handle, "websocket-frames.jsonl": self.study.ws_file,
                       **{key + "-ledger.jsonl": value for key, value in self.study.ledgers.items()}}
            streams = {}
            for name, handle in handles.items():
                handle.flush(); os.fsync(handle.fileno())
                info = os.fstat(handle.fileno())
                streams[name] = {"offset": info.st_size, "device": info.st_dev, "inode": info.st_ino}
            self.final_binding = publish(self.directory / "FINAL_HOLD.json", {
                "schema": "LANE_II_CAPTURE_FINAL_TAIL_HOLD_V1", "reason": "CHECKPOINT_PARTIAL_CONTEXT",
                "captured_utc": utc(now), "source_manifest_sha256": self.study.manifest,
                "segment_id": self.study.decision_capture.ledger.state["segment_id"],
                "event_seq": self.study.log.seq, "chain_sha256": self.study.log.chain, "streams": streams,
                "model_input_pending": self.model_pending, "decision_context_open": self.study.decision_capture.active is not None,
                "safe_loop_boundary": False, "qualified": False})
            self.finished = True
            return self.final_binding
        packet = self._snapshot("FINAL", self.study.cutoff, now, producers_exited=bool(producers_exited))
        # Called immediately after journal END and before any stream closes.
        self.final_binding = publish(self.directory / "FINAL.json", packet)
        self.sequence += 1; self.previous = self.final_binding["sha256"]; self.finished = True
        return self.final_binding


def validate_checkpoint(packet, *, source_manifest_sha256=None, segment_id=None):
    risk.require(packet["schema"] == SCHEMA and packet["kind"] in ("SCHEDULED", "RECOVERY", "FINAL")
                 and packet["safe_loop_boundary"] is True and set(packet["streams"]) == set(STREAMS), "CHECKPOINT_SCHEMA")
    risk.require(source_manifest_sha256 is None or packet["source_manifest_sha256"] == source_manifest_sha256, "CHECKPOINT_SOURCE")
    risk.require(segment_id is None or packet["segment_id"] == segment_id, "CHECKPOINT_SEGMENT")
    risk.require(packet["paper_risk_state_sha256"] == risk.digest(packet["paper_risk_state"])
                 and packet["paper_risk_state"]["segment_id"] == packet["segment_id"], "CHECKPOINT_RISK_STATE")
    risk.require(set(packet["account_carryover"]) == set(risk.POLICY["profile_budgets"])
                 and set(packet["pending_intents"]) == set(risk.POLICY["profile_budgets"]), "CHECKPOINT_ACCOUNTS")
    for value in packet["streams"].values():
        risk.require(isinstance(value["offset"], int) and value["offset"] >= 0, "CHECKPOINT_STREAM_OFFSET")
    return packet


def _tail(path, start, expected_identity):
    info = path.stat()
    risk.require({"device": info.st_dev, "inode": info.st_ino} == expected_identity, "CRASH_STREAM_REPLACED")
    risk.require(start <= info.st_size <= start + MAX_TAIL_BYTES, "CRASH_TAIL_SIZE_BOUND")
    return info.st_size


def inspect_crash_tail(capture, checkpoint):
    """Retain and bind complete observation/diagnostic tails; never discard bytes."""
    capture = Path(capture)
    tails, events = {}, {}
    seq, chain = checkpoint["event_seq"], checkpoint["chain_sha256"]
    latest_wall = checkpoint["captured_epoch"]
    monetary = ("PAPER_", "RISK_")
    allowed_events = {"WS_RAW", "WS_CONNECT_BEGIN", "WS_CONNECTED", "WS_DISCONNECTED", "WS_ERROR", "WS_CLOSED", "HTTP_RESPONSE", "HTTP_FAILED",
                      "DATA_GAP", "HISTORICAL_MINUTES_ADMITTED", "METADATA_ADMITTED", "BOOK_ADMITTED", "VENUE_LABEL_ADMITTED",
                      "CAUSAL_MODEL_INPUT", "MODEL_AND_PROFILE_TICK", "PROXY_LABEL", "MISSED_TICKS", "LATE_TICK_DISCARDED",
                      "POST_DEADLINE_TICK_RESULT_NOT_PUBLISHED", "STATUS_PUBLICATION_GAP", "FAILURE", "END",
                      "DECISION_BEGIN", "DECISION_GATE_CHECK", "DECISION_END", "DECISION_SKIPPED", "DECISION_ADMISSION_HOLD",
                      "REPLACEMENT_SELECTIVE_WINDOW_CREATED", "REPLACEMENT_SELECTIVE_WINDOW_ENDED"}
    allowed_profile = {"DECISION_BEGIN", "DECISION_GATE_CHECK", "DECISION_END", "DECISION_SKIPPED", "DECISION_ADMISSION_HOLD",
                       "REPLACEMENT_SELECTIVE_WINDOW_CREATED", "REPLACEMENT_SELECTIVE_WINDOW_ENDED"}
    for name in STREAMS:
        binding = checkpoint["streams"][name]
        path = capture / name
        end = _tail(path, binding["offset"], {key: binding[key] for key in ("device", "inode")})
        digest, count = hashlib.sha256(), 0
        with path.open("rb") as stream:
            if name == "events.jsonl":
                if binding["offset"]:
                    stream.seek(max(0, binding["offset"] - MAX_LINE_BYTES))
                    prefix_end = stream.read(binding["offset"] - stream.tell())
                    risk.require(prefix_end.endswith(b"\n"), "CRASH_PREFIX_TORN")
                    last = json.loads(prefix_end.rstrip(b"\n").rsplit(b"\n", 1)[-1])
                    last_hash = last.pop("sha256")
                    risk.require(last["seq"] == seq and last_hash == chain and risk.digest(last) == last_hash, "CRASH_PREFIX_ENDPOINT_CHANGED")
                else:
                    risk.require(seq == 0 and chain == ZERO_HASH, "CRASH_PREFIX_EMPTY_BINDING")
            stream.seek(binding["offset"])
            while stream.tell() < end:
                raw = stream.readline(MAX_LINE_BYTES + 1)
                risk.require(raw.endswith(b"\n") and len(raw) <= MAX_LINE_BYTES and stream.tell() <= end, "CRASH_TORN_OR_OVERSIZE_TAIL")
                digest.update(raw); count += 1
                row = json.loads(raw)
                if name == "events.jsonl":
                    actual = row.pop("sha256")
                    risk.require(row["seq"] == seq + 1 and row["previous_sha256"] == chain and risk.digest(row) == actual, "CRASH_TAIL_CHAIN")
                    kind = row["kind"]
                    risk.require(kind in allowed_events and not kind.startswith(monetary), "CRASH_MONETARY_OR_UNKNOWN_TAIL")
                    seq, chain = row["seq"], actual
                    latest_wall = max(latest_wall, stamp(row["utc"]))
                    if "profile" in row["data"]:
                        events[seq] = {"event_seq": seq, "kind": kind, "data": row["data"]}
                elif name.endswith("-ledger.jsonl"):
                    risk.require(row["kind"] in allowed_profile and row == events.get(row["event_seq"]), "CRASH_FINANCIAL_OR_UNBOUND_LEDGER_TAIL")
                    risk.require(row["data"]["profile"] + "-ledger.jsonl" == name, "CRASH_LEDGER_PROFILE")
                else:
                    risk.require(isinstance(row, dict), "CRASH_WS_TAIL_SCHEMA")
        after = path.stat()
        risk.require(after.st_size == end and after.st_dev == binding["device"] and after.st_ino == binding["inode"], "CRASH_TAIL_STILL_CHANGING")
        tails[name] = {"path": str(path.resolve()), "offset_start": binding["offset"], "offset_end": end,
                       "bytes": end - binding["offset"], "sha256": digest.hexdigest(), "rows": count,
                       "excluded_from_qualified_prefix": True, "original_bytes_preserved": True}
    return {"streams": tails, "last_event_seq": seq, "last_chain_sha256": chain,
            "last_observed_epoch": latest_wall, "financial_rows": 0, "qualified": False}


def validate_crash_handoff(path, expected_anchor_sha256=None):
    """Verify immutable crash evidence and derive CLOSED state without a DB."""
    path = Path(path).resolve(); run = path.parent
    handoff, binding = read(path)
    risk.require(handoff["schema"] == CRASH_SCHEMA and handoff["normal_terminal_receipt"] is False
                 and handoff["qualified"] is False and handoff["no_budget_or_cash_refill"] is True, "CRASH_HANDOFF_SCHEMA")
    risk.require(expected_anchor_sha256 is None or handoff["anchor_sha256"] == expected_anchor_sha256, "CRASH_HANDOFF_ANCHOR")
    documents = {}
    for role, expected in (("operator", run / "operator.json"), ("exit", run / "exit.json"),
                           ("preregistration", run / "capture/preregistration.json"), ("checkpoint", None)):
        reference = handoff[role]
        source = Path(reference["path"]).resolve()
        risk.require(source == expected if expected is not None else source.parent == run / "capture/checkpoints", "CRASH_HANDOFF_PATH")
        document, actual = read(source)
        risk.require(actual == reference, "CRASH_HANDOFF_EVIDENCE_CHANGED")
        documents[role] = document
    checkpoint = validate_checkpoint(documents["checkpoint"], source_manifest_sha256=handoff["source_manifest_sha256"], segment_id=handoff["segment_id"])
    operator, exited, prereg = documents["operator"], documents["exit"], documents["preregistration"]
    risk.require(all(operator[key] == exited[key] for key in ("pid", "creation_epoch", "boot_epoch", "executable_sha256", "source_manifest_sha256"))
                 and operator["source_manifest_sha256"] == handoff["source_manifest_sha256"]
                 and operator["owned_job_before_startup_gate"] is True and exited["retained_handle_exit_observed"] is True
                 and exited["owned_job_closed"] is True and exited["owned_children_at_parent_exit"] == [], "CRASH_HANDOFF_OWNER_PROOF")
    risk.require(checkpoint["paper_risk_anchor_sha256"] == handoff["anchor_sha256"]
                 and prereg["source_manifest_sha256"] == handoff["source_manifest_sha256"]
                 and stamp(prereg["cutoff_utc"]) == checkpoint["fixed_cutoff_epoch"] == handoff["fixed_cutoff_epoch"], "CRASH_HANDOFF_FIXED_SCOPE")
    state = handoff["paper_risk_terminal_state"]
    risk.require(state == checkpoint["paper_risk_state"] and handoff["paper_risk_action_cursor"] == checkpoint["paper_risk_action_cursor"]
                 and handoff["account_carryover"] == checkpoint["account_carryover"], "CRASH_HANDOFF_STATE_BINDING")
    risk.require(state["phase"] == "ACTIVE" and not state["reservations"] and not risk.snapshot(state)["holds"]
                 and not any(checkpoint["pending_intents"].values())
                 and not any(value["positions"] for value in handoff["account_carryover"].values()), "CRASH_HANDOFF_NOT_FLAT")
    tail = inspect_crash_tail(run / "capture", checkpoint)
    risk.require(tail == handoff["excluded_tail"], "CRASH_HANDOFF_TAIL_CHANGED")
    earliest = math.ceil(max(tail["last_observed_epoch"], checkpoint["paper_start_epoch"], stamp(exited["observed_exit_utc"])) / 300) * 300
    risk.require(earliest == handoff["restart_not_before_epoch"], "CRASH_HANDOFF_RESTART_WINDOW")
    terminal = {"kind": "CRASH_HANDOFF", "handoff_path": str(path), "handoff_sha256": binding["sha256"],
                "source_manifest_sha256": handoff["source_manifest_sha256"], "state": "FAILED_CRASH_HANDOFF",
                "fixed_cutoff_epoch": handoff["fixed_cutoff_epoch"], "restart_not_before_epoch": earliest}
    closed = deepcopy(state)
    closed.update(phase="CLOSED", last_terminal=terminal, carry_accounts=deepcopy(handoff["account_carryover"]))
    return {"valid": True, "flat": True, "qualified": False, "handoff": binding, "checkpoint": checkpoint,
            "closed_state": closed, "predecessor_token": "CRASH:" + binding["sha256"],
            "fixed_cutoff_epoch": handoff["fixed_cutoff_epoch"], "restart_not_before_epoch": earliest}


def recover_flat_attempt(run_dir, manifest_binding, expected_segment_id, *, expected_checkpoint_sha256=None):
    """Parent-only explicit recovery. Never called by collector startup.

    A manager crash without retained process/Job exit proof remains a HOLD.
    Observation-only tails remain in the failed attempt and are not replay
    qualification evidence. A new segment must present CRASH:<handoff hash>.
    """
    from .common import same_process
    run = Path(run_dir).resolve(); capture = run / "capture"
    risk.require(not (capture / "receipt.json").exists(), "CRASH_RECOVERY_REQUIRES_MISSING_NORMAL_RECEIPT")
    operator, operator_binding = read(run / "operator.json")
    exit_record, exit_binding = read(run / "exit.json")
    keys = ("pid", "creation_epoch", "boot_epoch", "executable_sha256", "source_manifest_sha256")
    risk.require(all(operator[key] == exit_record[key] for key in keys) and operator["owned_job_before_startup_gate"] is True,
                 "CRASH_OWNER_IDENTITY")
    risk.require(same_process(operator) is False and exit_record["retained_handle_exit_observed"] is True
                 and exit_record["owned_job_closed"] is True and exit_record["owned_children_at_parent_exit"] == [], "CRASH_OWNER_NOT_CLOSED")
    prereg, prereg_binding = read(capture / "preregistration.json")
    candidates = []
    for path in (capture / "checkpoints").glob("*.json"):
        if path.stem not in ("RECOVERY", "FINAL") and not path.stem.isdigit():
            continue
        value, item = read(path)
        validate_checkpoint(value, source_manifest_sha256=operator["source_manifest_sha256"], segment_id=expected_segment_id)
        candidates.append((value["durability_sequence"], value, item))
    risk.require(candidates, "CRASH_DURABLE_CHECKPOINT_MISSING")
    _, checkpoint, checkpoint_binding = max(candidates, key=lambda item: item[0])
    risk.require(expected_checkpoint_sha256 is None or checkpoint_binding["sha256"] == expected_checkpoint_sha256, "CRASH_CHECKPOINT_CHANGED")
    risk.require(checkpoint["paper_risk_anchor_sha256"] == manifest_binding["sha256"]
                 and checkpoint["source_manifest_sha256"] == prereg["source_manifest_sha256"]
                 and checkpoint["fixed_cutoff_epoch"] == stamp(prereg["cutoff_utc"]), "CRASH_PREREGISTRATION_BINDING")
    state = checkpoint["paper_risk_state"]
    risk.require(state["phase"] == "ACTIVE" and not state["reservations"] and not risk.snapshot(state)["holds"]
                 and not any(checkpoint["pending_intents"].values())
                 and not any(row["positions"] for row in checkpoint["account_carryover"].values()), "CRASH_ACCOUNT_NOT_PROVEN_FLAT")
    tail = inspect_crash_tail(capture, checkpoint)
    handoff = {"schema": CRASH_SCHEMA, "state": "FAILED_CRASH_HANDOFF", "normal_terminal_receipt": False,
               "segment_id": expected_segment_id, "source_manifest_sha256": checkpoint["source_manifest_sha256"],
               "anchor_sha256": manifest_binding["sha256"], "checkpoint": checkpoint_binding,
               "operator": operator_binding, "exit": exit_binding, "preregistration": prereg_binding,
               "paper_risk_terminal_state": state, "paper_risk_action_cursor": checkpoint["paper_risk_action_cursor"],
               "account_carryover": checkpoint["account_carryover"], "excluded_tail": tail,
               "fixed_cutoff_epoch": checkpoint["fixed_cutoff_epoch"],
               "restart_not_before_epoch": math.ceil(max(tail["last_observed_epoch"], checkpoint["paper_start_epoch"], stamp(exit_record["observed_exit_utc"])) / 300) * 300,
               "qualified": False, "no_budget_or_cash_refill": True}
    ledger = risk.RiskLedger(manifest_binding["path"], manifest_binding["sha256"], roster_sha256=state["roster_sha256"])
    try:
        path = run / "CRASH_HANDOFF.json"
        if path.exists():
            existing, handoff_binding = read(path)
            risk.require(existing == handoff, "CRASH_HANDOFF_CONTENT_CHANGED")
        else:
            risk.require(ledger.state == state and ledger.action_cursor() == checkpoint["paper_risk_action_cursor"], "CRASH_UNACCOUNTED_RISK_TRANSACTIONS")
            handoff_binding = publish(path, handoff)
        ledger.finish_crashed_segment(path)
        return {"schema": CRASH_SCHEMA, "recovered": True, "handoff": handoff_binding,
                "predecessor_token": "CRASH:" + handoff_binding["sha256"], "state": deepcopy(ledger.state),
                "fixed_cutoff_epoch": handoff["fixed_cutoff_epoch"], "restart_not_before_epoch": handoff["restart_not_before_epoch"],
                "qualified": False}
    finally:
        ledger.close()
