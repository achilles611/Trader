"""Owned offline, incremental causal replay of fsynced producer checkpoints.

Only new stream ranges are read during normal operation. Source-bound strategy
state lives in one process; large observational collections spill to this
consumer's private SQLite file. Restart never treats an offset as model state.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from collections.abc import MutableMapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.lane_ii_continuous import qualification_replay as strict
from scripts.lane_ii_continuous.common import atomic_json

MIB = 1024 ** 2
ZERO = "0" * 64
STREAMS = ("events.jsonl", "websocket-frames.jsonl", "5m-P13-ledger.jsonl", "5m-P2-ledger.jsonl")
IDS = sorted(strict.EXPECTED_IDS)
CHECKPOINT_SCHEMA = "LANE_II_CAPTURE_CHECKPOINT_V1"


class Hold(RuntimeError):
    pass


def check(value, reason):
    if not value:
        raise Hold(reason)


def digest(value):
    return hashlib.sha256(strict.canonical(value)).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Limits:
    range_bytes: int = 256 * MIB
    input_bytes: int = 64 * 1024 ** 3
    events: int = 10_000_000
    rss_bytes: int = 256 * MIB
    output_bytes: int = 4 * 1024 ** 3
    checkpoints: int = 100

    def validate(self):
        check(0 < self.range_bytes <= 512 * MIB and self.range_bytes <= self.input_bytes <= 64 * 1024 ** 3,
              "INCREMENTAL_BYTE_LIMITS")
        check(0 < self.events <= 10_000_000 and 32 * MIB <= self.rss_bytes <= 1024 ** 3
              and 16 * MIB <= self.output_bytes <= 64 * 1024 ** 3 and 1 <= self.checkpoints <= 128,
              "INCREMENTAL_RESOURCE_LIMITS")


def resident_bytes():
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in ("peak", "rss", "peak_paged", "paged", "peak_nonpaged", "nonpaged", "pagefile", "peak_pagefile")]
        row = Counters(); row.cb = ctypes.sizeof(row)
        function = ctypes.WinDLL("psapi", use_last_error=True).GetProcessMemoryInfo
        function.argtypes = (wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD)
        check(function(wintypes.HANDLE(-1), ctypes.byref(row), row.cb), "OWNED_REPLAY_MEMORY_UNKNOWN")
        return row.rss
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)


class Store:
    """Disposable analysis state; never opens the campaign risk database."""
    def __init__(self, path, maximum_bytes):
        self.path, self.maximum = Path(path), maximum_bytes
        check(not self.path.exists(), "NEW_ANALYSIS_STORE_REQUIRED")
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA cache_size=-8192")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("CREATE TABLE objects(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key))")
        self.connection.execute("CREATE TABLE records(namespace TEXT,position INTEGER,value TEXT,PRIMARY KEY(namespace,position))")

    def bounded(self):
        size = self.connection.execute("PRAGMA page_count").fetchone()[0] * self.connection.execute("PRAGMA page_size").fetchone()[0]
        check(size <= self.maximum, "ANALYSIS_STORE_SIZE_BOUND")
        return size

    def commit(self):
        self.bounded(); self.connection.commit()

    def close(self):
        self.connection.close()


class DiskMap(MutableMapping):
    def __init__(self, store, name):
        self.store, self.name, self.count = store, name, 0

    def __getitem__(self, key):
        row = self.store.connection.execute("SELECT value FROM objects WHERE namespace=? AND key=?", (self.name, strict.canonical(key).decode())).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def __setitem__(self, key, value):
        raw = strict.canonical(value)
        check(len(raw) <= 8 * MIB, "SPILLED_OBJECT_BOUND")
        encoded = strict.canonical(key).decode()
        prior = self.store.connection.execute("SELECT 1 FROM objects WHERE namespace=? AND key=?", (self.name, encoded)).fetchone()
        self.store.connection.execute("INSERT OR REPLACE INTO objects VALUES(?,?,?)", (self.name, encoded, raw.decode()))
        self.count += prior is None
        check(self.count <= 250000, "SPILLED_OBJECT_COUNT_BOUND")

    def __delitem__(self, key):
        cursor = self.store.connection.execute("DELETE FROM objects WHERE namespace=? AND key=?", (self.name, strict.canonical(key).decode()))
        if not cursor.rowcount:
            raise KeyError(key)
        self.count -= 1

    def __iter__(self):
        for row in self.store.connection.execute("SELECT key FROM objects WHERE namespace=? ORDER BY key", (self.name,)):
            yield json.loads(row[0])

    def __len__(self):
        return self.count


class DiskSequence(Sequence):
    def __init__(self, store, name):
        self.store, self.name, self.count = store, name, 0

    def append(self, value):
        raw = strict.canonical(value)
        check(len(raw) <= 16 * MIB and self.count < 1_000_000, "SPILLED_SEQUENCE_BOUND")
        self.store.connection.execute("INSERT INTO records VALUES(?,?,?)", (self.name, self.count, raw.decode()))
        self.count += 1

    def extend(self, values):
        for value in values:
            self.append(value)

    def __len__(self):
        return self.count

    def __getitem__(self, index):
        if index < 0:
            index += self.count
        row = self.store.connection.execute("SELECT value FROM records WHERE namespace=? AND position=?", (self.name, index)).fetchone()
        if row is None:
            raise IndexError(index)
        return json.loads(row[0])

    def __iter__(self):
        for row in self.store.connection.execute("SELECT value FROM records WHERE namespace=? ORDER BY position", (self.name,)):
            yield json.loads(row[0])


class Cursor:
    def __init__(self, path, line_bytes=8 * MIB):
        self.path, self.line_bytes = Path(path), line_bytes
        self.offset, self.rows = 0, 0
        self.identity, self.stream = None, None
        self.digest = hashlib.sha256()

    def begin(self, endpoint):
        info = self.path.stat()
        identity = {"device": info.st_dev, "inode": info.st_ino}
        check(identity == {key: endpoint[key] for key in identity}, "STREAM_IDENTITY_CHANGED:" + self.path.name)
        check(self.identity is None or self.identity == identity, "STREAM_REPLACED:" + self.path.name)
        check(type(endpoint["offset"]) is int and self.offset <= endpoint["offset"] <= info.st_size, "STREAM_TRUNCATED_OR_OFFSET_REGRESSION")
        self.identity, self.end, self.start = identity, endpoint["offset"], self.offset
        self.range_digest, self.range_rows = hashlib.sha256(), 0
        self.stream = self.path.open("rb"); self.stream.seek(self.offset)

    def read(self):
        if self.offset == self.end:
            return None
        raw = self.stream.readline(min(self.line_bytes + 1, self.end - self.offset + 1))
        check(raw and raw.endswith(b"\n") and len(raw) <= self.line_bytes and self.offset + len(raw) <= self.end,
              "INCOMPLETE_OR_OVERSIZED_BARRIER_LINE:" + self.path.name)
        self.offset += len(raw); self.rows += 1; self.range_rows += 1
        self.digest.update(raw); self.range_digest.update(raw)
        return raw

    def finish(self):
        check(self.offset == self.end, "UNCONSUMED_BARRIER_PREFIX:" + self.path.name)
        info = self.path.stat()
        check({"device": info.st_dev, "inode": info.st_ino} == self.identity and info.st_size >= self.end, "STREAM_CHANGED_DURING_RANGE")
        self.stream.close(); self.stream = None
        return {"path": str(self.path.resolve()), **self.identity, "start_offset": self.start, "end_offset": self.end,
                "bytes": self.end - self.start, "rows": self.range_rows, "sha256": self.range_digest.hexdigest(),
                "prefix_sha256": self.digest.hexdigest()}

    def close(self):
        if self.stream is not None:
            self.stream.close(); self.stream = None


class WebsocketCursor:
    def __init__(self, cursor):
        self.cursor, self.count = cursor, 0

    def next(self, event):
        raw = self.cursor.read()
        check(raw is not None, "WEBSOCKET_RANGE_MISSING_PROOF")
        row = json.loads(raw); self.count += 1
        data = event["data"]
        check(row.get("seq") == data.get("raw_sequence") == self.count and row.get("sha256") == data.get("sha256")
              and row.get("receipt_wall") == data.get("receipt_wall"), "WEBSOCKET_RANGE_CAUSAL_JOIN")
        decoded = base64.b64decode(row["raw_b64"], validate=True)
        check(len(decoded) <= 16384 and hashlib.sha256(decoded).hexdigest() == row["sha256"], "WEBSOCKET_RANGE_RAW_HASH")
        return row, decoded


class Proofs(strict.RawProofs):
    def verify(self, reference):
        path = (self.capture / reference["file"]).resolve()
        check(path.is_relative_to(self.capture) and not path.is_symlink(), "RAW_PROOF_PATH_ESCAPE")
        current = strict.binding(path, 8 * MIB)
        check(current["sha256"] == reference["sha256"], "RAW_RESPONSE_HASH")
        old = self.bindings.get(str(path))
        check(old is None or old == current, "RAW_RESPONSE_CHANGED")
        if old is None:
            self.bytes += current["bytes"]; self.bindings[str(path)] = current
        check(self.bytes <= self.maximum_bytes, "RAW_RESPONSE_CUMULATIVE_BOUND")


class CompactCoverage(strict.Coverage):
    def evaluation(self, begin, market, quotes, mark):
        super().evaluation(begin, market, quotes, mark)
        current = (begin["bundle_id"], market["start"])
        pairs = {window for (_, window), ids in self.fresh_bundles.items() if ids == strict.EXPECTED_IDS}
        pending = self.fresh_bundles.get(current)
        self.fresh_bundles = {(-1, window): set(strict.EXPECTED_IDS) for window in pairs}
        if pending is not None and pending != strict.EXPECTED_IDS:
            self.fresh_bundles[current] = pending


class Accounting:
    def __init__(self, initial_cash):
        self.initial_cash, self.positions = initial_cash, {}
        self.counts, self.realized, self.fees = Counter(), 0., 0.

    def consume(self, kind, data):
        if kind not in strict.retained.FINANCIAL_FIELDS:
            return
        from scripts.lane_ii_continuous.closeout import near, number
        key = data["market"]; self.counts[kind] += 1
        if kind == "PAPER_ENTRY":
            check(key not in self.positions and data["side"] in ("Up", "Down"), "ACCOUNTING_DUPLICATE_ENTRY")
            check(all(number(data.get(k)) and data[k] >= 0 for k in ("quantity", "basis", "entry_fee")) and data["quantity"] > 0,
                  "ACCOUNTING_ENTRY_VALUES")
            if data.get("fills"):
                near(sum(f["quantity"] for f in data["fills"]), data["quantity"], "ACCOUNTING_ENTRY_QUANTITY")
                near(sum(f["quantity"] * f["price"] for f in data["fills"]) + data["entry_fee"], data["basis"], "ACCOUNTING_ENTRY_FILLS")
            self.positions[key] = {k: data[k] for k in ("side", "quantity", "basis")}
            self.fees += data["entry_fee"]
        else:
            check(key in self.positions and data["side"] == self.positions[key]["side"], "ACCOUNTING_EXIT_WITHOUT_POSITION")
            prior = self.positions.pop(key)
            if kind == "PAPER_EXIT":
                near(data["basis"], prior["basis"], "ACCOUNTING_EXIT_BASIS")
                near(data["net"], data["proceeds"] - prior["basis"], "ACCOUNTING_EXIT_NET")
                if data.get("fills"):
                    near(sum(f["quantity"] for f in data["fills"]), prior["quantity"], "ACCOUNTING_EXIT_QUANTITY")
                    near(sum(f["quantity"] * f["price"] for f in data["fills"]) - data["exit_fee"], data["proceeds"], "ACCOUNTING_EXIT_FILLS")
                self.fees += data["exit_fee"]
            else:
                check(data["payout"] in (0, 1), "ACCOUNTING_SETTLEMENT_PAYOUT")
                near(data["net"], prior["quantity"] * data["payout"] - prior["basis"], "ACCOUNTING_SETTLEMENT_NET")
            self.realized += data["net"]

    def verify(self, profile):
        from scripts.lane_ii_continuous.closeout import near
        check((profile.entries, profile.exits, profile.settlements) == tuple(self.counts[k] for k in
              ("PAPER_ENTRY", "PAPER_EXIT", "PAPER_SETTLEMENT")), "ACCOUNTING_PROFILE_COUNTS")
        check({key: {k: row[k] for k in ("side", "quantity", "basis")} for key, row in profile.positions.items()} == self.positions,
              "ACCOUNTING_OPEN_INVENTORY")
        near(profile.realized, self.realized, "ACCOUNTING_REALIZED"); near(profile.fees, self.fees, "ACCOUNTING_FEES")
        basis = sum(row["basis"] for row in self.positions.values())
        return {"entries": profile.entries, "cash_exits": profile.exits, "settlements": profile.settlements,
                "realized_pnl": self.realized, "modeled_fees": self.fees, "unresolved_basis": basis,
                "unresolved_positions": len(self.positions), "virtual_cash": self.initial_cash + self.realized - basis,
                "open_holdings": deepcopy(self.positions)}


class Consumer:
    def __init__(self, candidate, run_dir, output, manifest_sha256, *, limits=Limits(), restart_policy="hold", sealed=False):
        limits.validate()
        self.candidate, self.run_dir, self.output = (Path(path).resolve() for path in (candidate, run_dir, output))
        self.capture, self.manifest_sha, self.limits = self.run_dir / "capture", manifest_sha256, limits
        check(not self.output.is_relative_to(self.run_dir) and not self.output.is_relative_to(self.candidate)
              and not self.run_dir.is_relative_to(self.output) and not self.candidate.is_relative_to(self.output), "EXTERNAL_ANALYSIS_OUTPUT_REQUIRED")
        prior = list((self.output / "ranges").glob("*.json")) if self.output.exists() else []
        check(not (prior or (self.output / "INPUTS.json").exists()) or restart_policy == "rebuild", "REPLAY_RESTART_REQUIRES_VERIFIED_REBUILD")
        self.output.mkdir(parents=True, exist_ok=True); (self.output / "ranges").mkdir(exist_ok=True); (self.output / "hours").mkdir(exist_ok=True)
        self.ownership = (self.output / "OWNER.lock").open("a+b")
        if self.ownership.tell() == 0:
            self.ownership.write(b"0"); self.ownership.flush()
        self.ownership.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.ownership.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.ownership.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.ownership.close()
            raise Hold("OUTPUT_ALREADY_OWNED") from exc
        self.store = Store(self.output / ("analysis-" + uuid.uuid4().hex + ".sqlite"), limits.output_bytes)
        manifest = strict.verify_candidate(self.candidate, manifest_sha256)
        prereg_path, operator_path = self.capture / "preregistration.json", self.run_dir / "operator.json"
        prereg, operator = strict.load(prereg_path), strict.load(operator_path)
        self.start, self.cutoff = strict.stamp(prereg["paper_start_utc"]), strict.stamp(prereg["cutoff_utc"])
        check(0 < self.cutoff - self.start <= 86400 and prereg["source_manifest_sha256"] == operator["source_manifest_sha256"] == manifest_sha256,
              "PREFIX_SOURCE_AND_INTERVAL")
        check(operator.get("owned_job_before_startup_gate") is True, "PREFIX_OWNED_CAPTURE_REQUIRED")
        check(all(type(prereg["policy"].get(k)) is int and prereg["policy"][k] == 0 for k in ("real_orders", "model_api_calls", "authenticated_account_calls")), "PREFIX_PAPER_ONLY")
        command = operator.get("command", [])
        check(type(operator.get("pid")) is int and operator["pid"] > 0 and operator.get("creation_epoch", 0) > 0
              and operator.get("boot_epoch", 0) > 0 and len(command) == 13
              and Path(command[0]).resolve() == Path(sys.executable).resolve() and command[1:4] == ["-I", "-S", "-B"]
              and Path(command[4]).resolve() == self.candidate / "study.py" and command[5:7] == ["worker", "--run-dir"]
              and Path(command[7]).resolve() == self.capture and command[8] == "--paper-start-utc"
              and strict.stamp(command[9]) == self.start and command[10] == "--cutoff-utc"
              and strict.stamp(command[11]) == self.cutoff and command[12] == "--startup-gate", "PREFIX_OWNED_COMMAND_IDENTITY")
        self.sealed = sealed
        self.origin = prereg.get("capture_checkpoint_policy", {}).get("cadence_origin_epoch", self.start)
        self.first_scheduled = self.origin + 900 * (math.floor((self.start - self.origin) / 900) + 1)
        if not sealed:
            policy = prereg.get("capture_checkpoint_policy", {})
            check(policy.get("schema") == CHECKPOINT_SCHEMA and policy.get("enabled") is True
                  and policy.get("interval_seconds") == 900 and self.origin <= self.start
                  and policy.get("fixed_cutoff_epoch") == self.cutoff and policy.get("partial_contexts_permitted") is False,
                  "PREREGISTERED_PRODUCER_BARRIERS_REQUIRED")
        self.sources = {"preregistration": strict.binding(prereg_path), "operator": strict.binding(operator_path),
                        "source_manifest": strict.binding(self.candidate / "SOURCE_MANIFEST.json"), "driver": strict.binding(__file__),
                        "strict_driver": strict.binding(strict.__file__)}
        self.context = {"candidate": self.candidate, "capture": self.capture, "run_dir": self.run_dir, "output": self.output,
                        "start": self.start, "cutoff": self.cutoff, "decision_bound": self.cutoff, "preregistration": prereg,
                        "sources": self.sources, "manifest": manifest, "diagnostic_witnesses": {}}
        sys.dont_write_bytecode = True; sys.path.insert(0, str(self.candidate))
        shim, helpers, market_types = strict.retained.load_runtime(self.candidate)
        registry = shim.bound_module("hourly_replay_roster", self.candidate / "roster.py")
        decision = shim.bound_module("continuous_decision_capture", self.candidate / "continuous_decision_capture.py")
        risk = shim.bound_module("continuous_paper_risk", self.candidate / "continuous_paper_risk.py")
        marks = shim.bound_module("hourly_replay_marks", self.candidate / "lean_marks.py")
        specs = registry.profile_specs()
        check(specs == prereg["profiles"] and {p["id"] for p in specs} == set(IDS) and len(specs) == 2, "PREFIX_FROZEN_ROSTER")
        strict.verify_initial_risk(self.context, risk, require_terminal=False)
        inputs = {"schema": "LANE_II_HOURLY_REPLAY_INPUTS_V1", "sources": self.sources, "limits": asdict(limits),
                  "source_manifest_sha256": manifest_sha256, "sealed_retrospective_validation_only": sealed}
        if (self.output / "INPUTS.json").exists():
            check(strict.load(self.output / "INPUTS.json") == inputs, "RESTART_SOURCE_OR_LIMITS_CHANGED")
        else:
            strict.write_new(self.output / "INPUTS.json", inputs)
        self.cursors = {name: Cursor(self.capture / name, 65536 if name == "websocket-frames.jsonl" else 8 * MIB) for name in STREAMS}
        self.websocket = WebsocketCursor(self.cursors["websocket-frames.jsonl"])
        self.engine = strict.ProspectiveReplay(self.context, specs, registry, helpers, market_types, sys.modules["model"], marks,
                                               decision, risk, self.websocket, lambda event: None)
        self.engine.http = DiskMap(self.store, "http")
        self.engine.risk.seen = DiskMap(self.store, "risk_seen")
        self.engine.risk.actions = DiskSequence(self.store, "replay_risk")
        self.engine.source_risk_actions = DiskSequence(self.store, "source_risk")
        self.engine.coverage_evidence = CompactCoverage(self.start, self.cutoff)
        self.observed, self.accounting = {}, {}
        for identity in IDS:
            self.engine.financial[identity] = DiskSequence(self.store, "replay_financial:" + identity)
            self.engine.decisions[identity] = DiskSequence(self.store, "replay_decisions:" + identity)
            self.observed[identity] = {kind: DiskSequence(self.store, "source_" + kind + ":" + identity) for kind in ("financial", "decisions")}
            self.accounting[identity] = Accounting(self.engine.profiles[identity].initial_cash)
        self.raw = Proofs(self.capture, limits.input_bytes); self.raw.bindings = DiskMap(self.store, "raw_bindings")
        self.sequence, self.chain, self.monotonic = 0, ZERO, 0
        self.checkpoint_sequence, self.previous_checkpoint, self.previous_receipt = 0, ZERO, ZERO
        self.scheduled_epochs, self.hour_bindings = [], []
        self.final_packet, self.last_event, self.max_decision, self.max_admission = None, None, -math.inf, -math.inf
        self.peak_rss, self.account_rows = 0, {}
        self.initial_cursor = prereg.get("paper_risk_initial_action_cursor")
        self.status("REBUILDING" if prior else "WAITING_FOR_CHECKPOINT")
        for path in sorted(prior):
            retained = strict.load(path)
            packet_binding = retained["checkpoint"]
            check(strict.binding(packet_binding["path"]) == packet_binding, "REBUILD_CHECKPOINT_CHANGED")
            self.consume_checkpoint(packet_binding["path"], expected_receipt=retained)

    def status(self, state, **extra):
        atomic_json(self.output / "STATUS.json", {"schema": "LANE_II_HOURLY_REPLAY_STATUS_V1", "state": state,
            "utc": utc(), "pid": os.getpid(), "checkpoint_sequence": getattr(self, "checkpoint_sequence", 0),
            "event_seq": getattr(self, "sequence", 0), "offsets": {name: cursor.offset for name, cursor in getattr(self, "cursors", {}).items()},
            "hourly_epochs": [row["epoch"] for row in getattr(self, "hour_bindings", [])], **extra})

    def resource_check(self):
        self.peak_rss = max(self.peak_rss, resident_bytes())
        check(self.peak_rss <= self.limits.rss_bytes, "INCREMENTAL_RSS_BOUND")
        check(sum(cursor.offset for cursor in self.cursors.values()) + self.raw.bytes <= self.limits.input_bytes, "INCREMENTAL_TOTAL_INPUT_BOUND")
        check(self.sequence <= self.limits.events and len(self.engine.markets) <= 512, "INCREMENTAL_EVENT_OR_MARKET_BOUND")
        self.store.bounded()

    def storage_check(self):
        check(sum(path.stat().st_size for path in self.output.rglob("*") if path.is_file()) <= self.limits.output_bytes,
              "TOTAL_ANALYSIS_OUTPUT_BOUND")

    def journal(self, cursor, *, sequence=None, chain=None, monotonic=None):
        sequence = self.sequence if sequence is None else sequence
        chain = self.chain if chain is None else chain
        monotonic = self.monotonic if monotonic is None else monotonic
        while (raw := cursor.read()) is not None:
            event = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(Hold("NONFINITE_JOURNAL")))
            check(event["seq"] == sequence + 1 and event["previous_sha256"] == chain and event["monotonic_ns"] >= monotonic,
                  "INCREMENTAL_JOURNAL_CHAIN")
            check(event["sha256"] == digest({k: v for k, v in event.items() if k != "sha256"}), "INCREMENTAL_JOURNAL_HASH")
            if sequence == 0:
                check(event["kind"] == "START" and event["data"]["manifest_sha256"] == self.manifest_sha
                      and event["data"]["preregistration_sha256"] == self.sources["preregistration"]["sha256"], "INCREMENTAL_START_BINDING")
            sequence, chain, monotonic = event["seq"], event["sha256"], event["monotonic_ns"]
            check(sequence <= self.limits.events, "INCREMENTAL_EVENT_BOUND")
            event["_verified_raw_bytes"] = len(raw)
            yield event

    def join_ledger(self, event):
        data = event["data"]
        if "profile" not in data:
            return
        identity = data["profile"]; check(identity in IDS, "UNREGISTERED_LEDGER_PROFILE")
        raw = self.cursors[identity + "-ledger.jsonl"].read()
        check(raw is not None, "MISSING_PROFILE_LEDGER_ROW")
        row = json.loads(raw)
        check(row == {"event_seq": event["seq"], "kind": event["kind"], "data": data}, "EXACT_JOURNAL_LEDGER_JOIN")
        kind = event["kind"]
        if kind in strict.retained.FINANCIAL_FIELDS:
            self.observed[identity]["financial"].append(strict.retained.projection(kind, data))
        elif kind in strict.retained.DECISIONS:
            self.observed[identity]["decisions"].append(strict.retained.decision_projection(kind, data))
        self.accounting[identity].consume(kind, data)

    def validate_packet(self, packet):
        check(packet["schema"] == CHECKPOINT_SCHEMA and packet["kind"] in ("SCHEDULED", "FINAL")
              and packet["safe_loop_boundary"] is True, "PRODUCER_CHECKPOINT_REQUIRED")
        check(packet["source_manifest_sha256"] == self.manifest_sha
              and packet["segment_id"] == self.engine.risk.state["segment_id"]
              and packet["paper_start_epoch"] == self.start and packet["fixed_cutoff_epoch"] == self.cutoff,
              "CHECKPOINT_RUN_SCOPE")
        check(packet["checkpoint_sequence"] == self.checkpoint_sequence + 1
              and packet["previous_checkpoint_sha256"] == self.previous_checkpoint, "CHECKPOINT_SEQUENCE_OR_PREDECESSOR")
        check(set(packet["streams"]) == set(STREAMS), "CHECKPOINT_STREAM_SET")
        check(packet["kind"] != "SCHEDULED" or packet["scheduled_epoch"] == self.first_scheduled + 900 * len(self.scheduled_epochs),
              "MISSING_FIFTEEN_MINUTE_BARRIER")
        check(packet["kind"] != "FINAL" or packet["scheduled_epoch"] == self.cutoff, "FINAL_CUTOFF_CHANGED")
        check(abs(strict.stamp(packet["captured_utc"]) - packet["captured_epoch"]) <= 1e-6
              and math.isfinite(packet["captured_epoch"]), "CHECKPOINT_CAPTURE_CLOCK")
        if packet["kind"] == "SCHEDULED":
            check(packet["scheduled_epoch"] < self.cutoff and packet["scheduled_epoch"] <= packet["captured_epoch"] < packet["scheduled_epoch"] + 900,
                  "CHECKPOINT_CADENCE_GAP_OR_FUTURE")
        check(self.checkpoint_sequence < self.limits.checkpoints, "CHECKPOINT_COUNT_BOUND")
        check(sum(packet["streams"][name]["offset"] - self.cursors[name].offset for name in STREAMS) <= self.limits.range_bytes,
              "NEW_RANGE_BYTE_BOUND")

    def consume_checkpoint(self, path, *, expected_receipt=None):
        path = Path(path).resolve()
        check(path.parent == self.capture / "checkpoints" and not path.is_symlink(), "PRODUCER_CHECKPOINT_PATH")
        packet, checkpoint = strict.load(path, 2 * MIB), strict.binding(path, 2 * MIB)
        self.validate_packet(packet)
        self.status("PROCESSING")
        ranges = self.consume_ranges(packet["streams"])
        engine = self.engine; risk = engine.risk_module
        check(self.sequence == packet["event_seq"] and self.chain == packet["chain_sha256"]
              and self.websocket.count == packet["ws_count"], "CHECKPOINT_STREAM_COUNTERS")
        check(engine.buffer is None and engine.last_model_input is None and engine.controller.active is None, "CHECKPOINT_INCOMPLETE_CAUSAL_CONTEXT")
        check(risk.digest(engine.risk.state) == packet["paper_risk_state_sha256"] and engine.risk.state == packet["paper_risk_state"],
              "CHECKPOINT_INDEPENDENT_RISK_PARITY")
        check(packet["paper_risk_anchor_sha256"] == self.context["preregistration"]["paper_risk_anchor_sha256"], "CHECKPOINT_RISK_ANCHOR")
        check(engine.decision_module.account_carryover(list(engine.profiles.values()), engine.controller.initial_accounts) == packet["account_carryover"],
              "CHECKPOINT_ACCOUNT_CARRYOVER")
        check({identity: sorted(key for key, row in profile.windows.items() if row.get("pending") is not None)
               for identity, profile in engine.profiles.items()} == packet["pending_intents"], "CHECKPOINT_PENDING_INTENTS")
        self.verify_action_cursor(packet["paper_risk_action_cursor"])
        check(strict.binding(path) == checkpoint, "CHECKPOINT_CHANGED_DURING_READ")
        self.checkpoint_sequence += 1
        record = {"schema": "LANE_II_INCREMENTAL_REPLAY_RANGE_V1", "checkpoint": checkpoint,
                  "checkpoint_sequence": self.checkpoint_sequence, "previous_receipt_sha256": self.previous_receipt,
                  "streams": ranges, "event_seq": self.sequence, "chain_sha256": self.chain,
                  "accounting": self.account_rows, "paper_risk_state_sha256": risk.digest(engine.risk.state),
                  "coverage": self.prefix_coverage(packet["scheduled_epoch"]), "contexts": engine.admissions,
                  "source_execution_complete": False, "semantic_prefix_pass": True, "accounts_nonadditive": True}
        target = self.output / "ranges" / (f"{self.checkpoint_sequence:06d}.json")
        if expected_receipt is None:
            strict.write_new(target, record)
        else:
            check(record == expected_receipt and strict.load(target) == record, "REBUILD_RANGE_OR_STATE_CHANGED")
        self.previous_receipt, self.previous_checkpoint = strict.sha(target), checkpoint["sha256"]
        if packet["kind"] == "SCHEDULED":
            self.scheduled_epochs.append(packet["scheduled_epoch"])
            if ((packet["scheduled_epoch"] - self.origin) % 3600 == 0 and
                    self.scheduled_epochs[-4:] == [packet["scheduled_epoch"] - 900 * i for i in (3, 2, 1, 0)]):
                self.publish_hour(packet["scheduled_epoch"], expected_receipt is not None)
        else:
            check(self.last_event["kind"] == "END", "FINAL_BARRIER_WITHOUT_END")
            self.final_packet = packet
        self.store.commit(); self.resource_check(); self.storage_check()
        self.status("AWAITING_FINAL_INPUTS" if self.final_packet else "WAITING_FOR_CHECKPOINT")
        return record

    def consume_ranges(self, endpoints):
        scan = Cursor(self.capture / "events.jsonl"); scan.offset = self.cursors["events.jsonl"].offset
        scan.begin(endpoints["events.jsonl"])
        try:
            witnesses = strict.scheduled_diagnostic_witnesses(self.journal(scan))
            check(len(witnesses) <= 10000, "RANGE_DIAGNOSTIC_WITNESS_BOUND")
            scanned = scan.finish()
        finally:
            scan.close()
        self.engine.missing_feature_diagnostics.witnesses = witnesses
        for name, cursor in self.cursors.items():
            cursor.begin(endpoints[name])
        try:
            with self.raw.bind_retained_reader():
                for event in self.journal(self.cursors["events.jsonl"]):
                    self.join_ledger(event)
                    if event["kind"] == "HTTP_RESPONSE":
                        self.raw.verify(event["data"]["raw_ref"])
                    if event["kind"] == "DECISION_BEGIN" and event["data"]["operation"] in ("tick", "settlement"):
                        self.max_decision = max(self.max_decision, event["data"]["evaluation_wall"])
                    if event["kind"] in ("METADATA_ADMITTED", "BOOK_ADMITTED", "HISTORICAL_MINUTES_ADMITTED", "VENUE_LABEL_ADMITTED", "MODEL_AND_PROFILE_TICK"):
                        self.max_admission = max(self.max_admission, strict.stamp(event["utc"]))
                    self.engine.consume(event)
                    self.sequence, self.chain, self.monotonic = event["seq"], event["sha256"], event["monotonic_ns"]
                    self.last_event = event
                    if self.sequence % 128 == 0:
                        self.resource_check()
            ranges = {name: cursor.finish() for name, cursor in self.cursors.items()}
            check(ranges["events.jsonl"]["sha256"] == scanned["sha256"], "JOURNAL_RANGE_CHANGED_BETWEEN_PASSES")
            self.account_rows = {identity: self.accounting[identity].verify(self.engine.profiles[identity]) for identity in IDS}
            for identity in IDS:
                for kind, actual in self.observed[identity].items():
                    simulated = self.engine.financial[identity] if kind == "financial" else self.engine.decisions[identity]
                    check(strict.retained.compare_events(actual, simulated)["pass"], "RANGE_" + kind.upper() + "_PARITY")
            self.resource_check()
            return ranges
        finally:
            for cursor in self.cursors.values():
                cursor.close()

    def verify_action_cursor(self, expected):
        check(self.initial_cursor is not None, "PREREGISTERED_INITIAL_RISK_ACTION_CURSOR_REQUIRED")
        count = len(self.engine.risk.actions)
        if not count:
            actual = self.initial_cursor
        else:
            action = self.engine.risk.actions[-1]
            sequence = self.initial_cursor["sequence"] + count
            actual = {"sequence": sequence, "event_id": action["event_id"], "sha256": digest([
                sequence, action["event_id"], action["action"], strict.canonical(action["payload"]).decode(), strict.canonical(action["result"]).decode()])}
        check(actual == expected, "CHECKPOINT_RISK_ACTION_CURSOR")

    def prefix_coverage(self, through):
        evidence = self.engine.coverage_evidence
        expected = set(range(math.ceil(self.start), math.ceil(min(through, self.cutoff))))
        observed = evidence.seconds & expected
        return {"planned_through_epoch": through, "expected_seconds": len(expected), "observed_seconds": len(observed),
                "missing_seconds": len(expected - observed), "missing_examples": sorted(expected - observed)[:128],
                "full_duration_qualified": False}

    def publish_hour(self, epoch, rebuilding=False):
        if epoch - 3600 < self.start:
            return
        path = self.output / "hours" / (str(int(epoch)) + ".json")
        record = {"schema": "LANE_II_HOURLY_REPLAY_HOUR_V1", "epoch": epoch, "quarters": 4,
                  "range_receipts": [strict.binding(self.output / "ranges" / f"{index:06d}.json")
                                     for index in range(self.checkpoint_sequence - 3, self.checkpoint_sequence + 1)],
                  "coverage": self.prefix_coverage(epoch), "accounting": self.account_rows,
                  "paper_risk": self.engine.risk_module.snapshot(self.engine.risk.state),
                  "accounts_nonadditive": True, "semantic_prefix_pass": True, "full_duration_qualified": False}
        if path.exists():
            check(strict.load(path) == record, "REBUILD_HOURLY_RESULT_CHANGED")
        else:
            strict.write_new(path, record)
        self.hour_bindings.append({"epoch": epoch, "binding": strict.binding(path)})

    def finish(self, final_inputs):
        check(self.final_packet is not None or self.sealed, "FINAL_PRODUCER_BARRIER_REQUIRED")
        handoff_binding = strict.binding(final_inputs, 2 * MIB)
        handoff = strict.load(final_inputs)
        check(handoff["schema"] == "LANE_II_HOURLY_REPLAY_FINAL_INPUTS_V1" and set(handoff["bindings"]) == {"raw_audit", "supervisor_result"},
              "FINAL_INPUTS_SCHEMA")
        for value in handoff["bindings"].values():
            check(strict.binding(value["path"]) == value, "FINAL_INPUTS_BINDING")
        context = strict.preflight(self.candidate, self.run_dir, handoff["bindings"]["raw_audit"]["path"],
            handoff["bindings"]["supervisor_result"]["path"], self.output / "terminal-validation-not-created",
            expected_manifest_sha256=self.manifest_sha, maximum_input_bytes=self.limits.input_bytes, maximum_events=self.limits.events)
        receipt = context["receipt"]
        check(self.max_decision < context["decision_bound"] and self.max_admission < context["decision_bound"], "FINAL_STOP_BOUND_REJECTS_PREFIX")
        for name, cursor in self.cursors.items():
            check(cursor.offset == cursor.path.stat().st_size, "UNCONSUMED_FINAL_TAIL:" + name)
            expected = receipt["events_sha256"] if name == "events.jsonl" else receipt["websocket_sha256"] if name == "websocket-frames.jsonl" else receipt["ledger_sha256"][name[:-13]]
            check(cursor.digest.hexdigest() == expected, "FINAL_STREAM_RECEIPT_HASH:" + name)
        check(self.sequence == receipt["events"] and self.chain == receipt["chain_sha256"] and self.websocket.count == receipt["ws_frames"]
              and self.last_event["kind"] == "END" and self.last_event["data"]["state"] == receipt["state"]
              and self.last_event["data"]["reason"] == receipt["reason"], "FINAL_EVENT_RECEIPT_BINDING")
        closure = strict.verify_initial_risk(context, self.engine.risk_module)
        replayed = self.engine.terminal_results(context["observed_end"])
        source_rows = {row["id"]: row for row in receipt["profiles"]}
        parity, decisions = {}, {}
        for identity in IDS:
            parity[identity] = strict.retained.compare_parity(self.observed[identity]["financial"], self.engine.financial[identity], source_rows[identity], replayed[identity])
            parity[identity]["terminal_inventory_matches"] = strict.retained.inventory(receipt["terminal_censoring"][identity]) == replayed[identity]["unresolved_inventory"]
            parity[identity]["pass"] &= parity[identity]["terminal_inventory_matches"]
            decisions[identity] = strict.retained.compare_events(self.observed[identity]["decisions"], self.engine.decisions[identity])
        risk = strict.risk_result(self.engine, receipt, closure)
        check(all(row["pass"] for row in [*parity.values(), *decisions.values()]), "FINAL_FINANCIAL_OR_DECISION_PARITY")
        self.raw.verify_unchanged()
        check(all(strict.binding(row["path"]) == row for row in self.sources.values()), "PREFIX_METADATA_CHANGED")
        strict.verify_candidate(self.candidate, self.manifest_sha)
        if not self.sealed:
            through = min(self.cutoff, context["decision_bound"])
            check(self.scheduled_epochs == list(range(int(self.first_scheduled), math.ceil(through), 900)), "FINAL_MISSING_QUARTER")
            if (receipt["state"] == "COMPLETE" and (self.cutoff - self.origin) % 3600 == 0 and
                    self.scheduled_epochs[-3:] == [self.cutoff - 900 * i for i in (3, 2, 1)]):
                self.publish_hour(self.cutoff)
        check(strict.binding(final_inputs) == handoff_binding, "FINAL_HANDOFF_CHANGED")
        result = {"schema": "LANE_II_HOURLY_REPLAY_RESULT_V1", "pass": True, "source_state": receipt["state"],
            "source_reason": receipt["reason"], "source_events": self.sequence, "source_events_sha256": receipt["events_sha256"],
            "source_profile_count": 2, "replayed_original_profile_count": 2, "source_profiles_with_financial_parity": 2,
            "source_profiles_with_recorded_decision_parity": 2, "parity": parity, "recorded_decision_parity": decisions,
            "profiles": replayed, "risk_parity": risk, "coverage": self.engine.coverage_evidence.result(),
            "admission_parity": {"pass": True, "contexts": self.engine.admissions, "hard_gate_checks": self.engine.gate_checks,
                                  "exact_input_and_terminal_state_hashes": True, "historical_callback_approximation_used": False},
            "raw_feed_reconstruction": {"pass": True, "websocket_frames": self.websocket.count,
                "independently_reconstructed_marks": self.engine.counts["independently_reconstructed_marks"],
                "completed_model_ticks": self.engine.scheduled_results, "external_or_interpolated_marks": False},
            "hourly_epochs": [row["epoch"] for row in self.hour_bindings], "hourly_bindings": self.hour_bindings,
            "checkpoint_count": self.checkpoint_sequence, "final_inputs": strict.binding(final_inputs),
            "accounting": self.account_rows, "accounts_nonadditive": True, "network_requests": 0, "real_orders": 0,
            "model_api_calls": 0, "full_duration_qualified": False, "strategy_edge_qualified": False, "funded_authority": False,
            "sealed_retrospective_validation_only": self.sealed, "peak_sampled_rss_bytes": self.peak_rss,
            "missing_feature_diagnostics": self.engine.missing_feature_diagnostics.result()}
        if (self.output / "RESULT.json").exists():
            previous = strict.load(self.output / "RESULT.json")
            check(type(previous.get("peak_sampled_rss_bytes")) is int and previous["peak_sampled_rss_bytes"] <= self.limits.rss_bytes,
                  "PREVIOUS_RESULT_RESOURCE_BOUND")
            comparison = {**result, "peak_sampled_rss_bytes": previous["peak_sampled_rss_bytes"]}
            check(comparison == previous, "REBUILT_FINAL_RESULT_CHANGED")
            result = previous
        else:
            strict.write_new(self.output / "RESULT.json", result)
        artifacts = {"schema": "LANE_II_HOURLY_REPLAY_ARTIFACTS_V1",
            "artifacts": [strict.binding(self.output / "RESULT.json"), *[row["binding"] for row in self.hour_bindings]],
            "range_receipts": [strict.binding(path) for path in sorted((self.output / "ranges").glob("*.json"))]}
        if (self.output / "ARTIFACT_MANIFEST.json").exists():
            check(strict.load(self.output / "ARTIFACT_MANIFEST.json") == artifacts, "REBUILT_ARTIFACT_MANIFEST_CHANGED")
        else:
            strict.write_new(self.output / "ARTIFACT_MANIFEST.json", artifacts)
        self.status("COMPLETE", result=strict.binding(self.output / "RESULT.json"))
        return result

    def close(self):
        for cursor in self.cursors.values():
            cursor.close()
        self.store.close()
        self.ownership.close()


def sealed_range_plan(context, split_seconds):
    """Read-only test plan for already genuinely sealed captures, never barriers."""
    check(1 <= split_seconds <= 86400, "SEALED_SPLIT_BOUND")
    capture = context["capture"]
    cursors = {name: Cursor(capture / name, 65536 if name == "websocket-frames.jsonl" else 8 * MIB) for name in STREAMS}
    endpoints = {}
    for name, cursor in cursors.items():
        info = cursor.path.stat()
        endpoints[name] = {"offset": info.st_size, "device": info.st_dev, "inode": info.st_ino}
        cursor.begin(endpoints[name])
    plan, next_epoch = [], context["start"] + split_seconds
    try:
        for event in strict.journal_rows(capture / "events.jsonl", context["receipt"], context["maximum_events"], context["maximum_input_bytes"]):
            check(cursors["events.jsonl"].read() is not None, "SEALED_PLAN_JOURNAL_EOF")
            if event["kind"] == "WS_RAW":
                check(cursors["websocket-frames.jsonl"].read() is not None, "SEALED_PLAN_WS_EOF")
            if "profile" in event["data"]:
                check(cursors[event["data"]["profile"] + "-ledger.jsonl"].read() is not None, "SEALED_PLAN_LEDGER_EOF")
            if event["kind"] == "END" or (event["kind"] == "MODEL_AND_PROFILE_TICK" and event["data"]["second"] + 1 >= next_epoch):
                plan.append({"schema": "LANE_II_RETAINED_SEALED_RANGE_TEST_V1", "producer_checkpoint": False,
                    "event_seq": event["seq"], "chain_sha256": event["sha256"],
                    "streams": {name: {**cursor.identity, "offset": cursor.offset} for name, cursor in cursors.items()}})
                next_epoch += split_seconds
        for cursor in cursors.values():
            cursor.finish()
        return plan
    finally:
        for cursor in cursors.values():
            cursor.close()


def verify_sealed(candidate, run_dir, audit_path, supervisor_path, output, manifest_sha256, *, split_seconds=300, limits=Limits()):
    """Exercise the real engine and range I/O on sealed evidence, without promotion."""
    with strict.offline_boundary():
        context = strict.preflight(candidate, run_dir, audit_path, supervisor_path, output,
            expected_manifest_sha256=manifest_sha256, maximum_input_bytes=limits.input_bytes, maximum_events=limits.events)
        plan = sealed_range_plan(context, split_seconds)
        consumer = Consumer(candidate, run_dir, output, manifest_sha256, limits=limits, sealed=True)
        try:
            strict.write_new(consumer.output / "SEALED_RANGE_TEST_PLAN.json", {"schema": "LANE_II_SEALED_RANGE_TEST_PLAN_V1",
                "source_receipt": context["sources"]["receipt"], "producer_checkpoints_invented": False, "ranges": plan})
            for index, packet in enumerate(plan, 1):
                check(sum(packet["streams"][name]["offset"] - consumer.cursors[name].offset for name in STREAMS) <= limits.range_bytes,
                      "SEALED_TEST_RANGE_BYTE_BOUND")
                ranges = consumer.consume_ranges(packet["streams"])
                check(consumer.sequence == packet["event_seq"] and consumer.chain == packet["chain_sha256"], "SEALED_TEST_RANGE_ENDPOINT")
                check(consumer.engine.buffer is None and consumer.engine.last_model_input is None, "SEALED_TEST_INCOMPLETE_FRAME")
                strict.write_new(consumer.output / f"SEALED_RANGE_TEST_{index:04d}.json", {"range": packet, "verified_streams": ranges,
                    "accounting": consumer.account_rows, "risk_state_sha256": digest(consumer.engine.risk.state)})
                consumer.store.commit()
            final_path = consumer.output / "SEALED_FINAL_INPUTS.json"
            strict.write_new(final_path, {"schema": "LANE_II_HOURLY_REPLAY_FINAL_INPUTS_V1", "bindings": {
                "raw_audit": strict.binding(audit_path), "supervisor_result": strict.binding(supervisor_path)}})
            return consumer.finish(final_path)
        finally:
            consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "run-dir", "output", "manifest-sha256"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--final-inputs")
    parser.add_argument("--poll-seconds", type=float, default=2.)
    parser.add_argument("--maximum-seconds", type=float, required=True)
    parser.add_argument("--maximum-range-bytes", type=int, default=256 * MIB)
    parser.add_argument("--maximum-input-bytes", type=int, default=64 * 1024 ** 3)
    parser.add_argument("--maximum-events", type=int, default=10_000_000)
    parser.add_argument("--maximum-rss-bytes", type=int, default=256 * MIB)
    parser.add_argument("--maximum-output-bytes", type=int, default=4 * 1024 ** 3)
    parser.add_argument("--restart-policy", choices=("hold", "rebuild"), default="hold")
    args = parser.parse_args(); started = time.monotonic(); consumer = None
    check(.1 <= args.poll_seconds <= 30 and 0 < args.maximum_seconds <= 100000, "OWNED_CONSUMER_LIFETIME_BOUND")
    limits = Limits(args.maximum_range_bytes, args.maximum_input_bytes, args.maximum_events, args.maximum_rss_bytes, args.maximum_output_bytes)
    with strict.offline_boundary():
        try:
            consumer = Consumer(args.candidate, args.run_dir, args.output, args.manifest_sha256, limits=limits, restart_policy=args.restart_policy)
            final_inputs = Path(args.final_inputs) if args.final_inputs else Path(args.output).resolve().parent / "FINAL_INPUTS.json"
            while time.monotonic() - started < args.maximum_seconds:
                if (consumer.capture / "checkpoints/FINAL_HOLD.json").exists():
                    raise Hold("PRODUCER_FINAL_TAIL_UNSEALED")
                if consumer.final_packet is not None:
                    if final_inputs.exists():
                        consumer.finish(final_inputs); return 0
                    consumer.status("AWAITING_FINAL_INPUTS")
                else:
                    paths = list((consumer.capture / "checkpoints").glob("*.json"))
                    packets = [(strict.load(path, 2 * MIB).get("checkpoint_sequence"), path) for path in paths if path.name != "RECOVERY.json"]
                    ready = sorted((seq, path) for seq, path in packets if type(seq) is int and seq > consumer.checkpoint_sequence)
                    if ready:
                        consumer.consume_checkpoint(ready[0][1]); continue
                    consumer.status("WAITING_FOR_CHECKPOINT")
                time.sleep(args.poll_seconds)
            raise Hold("OWNED_CONSUMER_LIFETIME_EXPIRED")
        except (Hold, strict.ReproducibilityHold, ValueError, OSError, KeyError, TypeError) as exc:
            if consumer is not None:
                consumer.status("HELD", reason=type(exc).__name__ + ":" + str(exc)[:200])
            else:
                if str(exc) != "OUTPUT_ALREADY_OWNED":
                    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
                    atomic_json(output / "STATUS.json", {"schema": "LANE_II_HOURLY_REPLAY_STATUS_V1", "state": "HELD", "utc": utc(), "reason": type(exc).__name__ + ":" + str(exc)[:200]})
            print(type(exc).__name__ + ":" + str(exc)[:200]); return 2
        finally:
            if consumer is not None:
                consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
