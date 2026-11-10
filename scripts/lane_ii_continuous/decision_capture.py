"""Observable admission contexts around unchanged frozen profile decisions.

Every model call has immutable input hashes, captured operational confidence,
and each actual hard-deadline check. Replay supplies the same hooks to a pure
risk state machine, independently reconstructing the inputs from raw evidence.
"""
from __future__ import annotations

from collections import deque
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import time
import types

try:
    import continuous_paper_risk as risk
    import continuous_clock as measured_clock
except ImportError:
    from . import paper_risk as risk
    from . import clock as measured_clock

SCHEMA = "LANE_II_DECISION_CAPTURE_V1"
FINANCIAL = frozenset(("PAPER_ENTRY", "PAPER_EXIT", "PAPER_SETTLEMENT"))
CARRY_FIELDS = ("initial_cash", "realized", "entries", "exits", "settlements", "fees")


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, deque)):
        return [plain(item) for item in value]
    return value


def profile_state(profile, market_id):
    return {"account": {key: getattr(profile, key) for key in CARRY_FIELDS},
            "positions": deepcopy(profile.positions), "window": deepcopy(profile.windows.get(market_id)),
            "history": plain(getattr(profile, "histories", {}).get(market_id, [])),
            "phase": profile.phase, "last_problem": profile.last_problem}


def account_carryover(profiles, previous=None):
    result = {}
    for profile in profiles:
        prior = (previous or {}).get(profile.id, {})
        segment = {key: getattr(profile, key) for key in CARRY_FIELDS}
        basis = sum(position["basis"] for position in profile.positions.values())
        result[profile.id] = {"schema": "LANE_II_PAPER_ACCOUNT_CARRY_V1", "closing_cash": profile.initial_cash + profile.realized - basis,
                              "positions": deepcopy(profile.positions), "last_segment": segment,
                              "original_virtual_initial_cash": prior.get("original_virtual_initial_cash", 3000)}
        for key in ("realized", "entries", "exits", "settlements", "fees"):
            result[profile.id]["cumulative_" + key] = prior.get("cumulative_" + key, 0) + getattr(profile, key)
    return result


def restore_account(profile, carry):
    if not carry:
        return
    risk.require(not carry["positions"], "OPEN_ACCOUNT_CONTINUATION_REQUIRES_SEPARATE_VERIFIED_ADAPTER")
    risk.require(carry["schema"] == "LANE_II_PAPER_ACCOUNT_CARRY_V1" and carry["original_virtual_initial_cash"] == 3000,
                 "INITIAL_VIRTUAL_ACCOUNT_CHANGED")
    value = carry["closing_cash"]
    risk.require(risk.decimal(value).is_finite() and value >= 0, "INVALID_ACCOUNT_CARRYOVER")
    profile.initial_cash = value
    # Current receipt and ledger stay segment-local; the bound carry artifact
    # separately retains all cumulative counters without cash topups.
    for key in ("realized", "entries", "exits", "settlements", "fees"):
        setattr(profile, key, 0)


class DecisionCapture:
    def __init__(self, study, ledger, *, emit=None, hard_check=None, clock_status=None, transport_status=None):
        self.study, self.ledger = study, ledger
        self.emit = emit or (lambda kind, data: study.profile_event(kind, data) if "profile" in data else study.log.emit(kind, data))
        self.hard_check_source = hard_check
        self.clock_status = clock_status or measured_clock.status
        self.transport_status = transport_status or study.transport_control.status
        self.counter = self.bundle_counter = self.action_counter = 0
        self.active = None
        self.initial_state = deepcopy(ledger.state)
        self.initial_accounts = deepcopy(ledger.state["carry_accounts"])

    @classmethod
    def from_study(cls, study, roster_sha256):
        path = os.environ.get("LANE_II_PAPER_RISK_MANIFEST")
        expected = os.environ.get("LANE_II_PAPER_RISK_MANIFEST_SHA256")
        segment = os.environ.get("LANE_II_PAPER_SEGMENT_ID")
        risk.require(path and expected and segment, "EXTERNALLY_ANCHORED_PAPER_CAMPAIGN_REQUIRED")
        ledger = risk.RiskLedger(path, expected, roster_sha256=roster_sha256)
        ledger.begin_segment(segment, os.environ.get("LANE_II_PAPER_PREDECESSOR_RECEIPT_SHA256", "GENESIS"),
                             paper_start_epoch=study.paper_start, cutoff_epoch=study.cutoff)
        return cls(study, ledger)

    def attach_profile(self, profile):
        restore_account(profile, self.initial_accounts.get(profile.id))
        profile.live_check = self.live_check
        profile.before_intent = types.MethodType(lambda p, m, q, f, s, n, b: self.before_intent(p, m, q, f, s, n, b), profile)
        profile.before_entry = types.MethodType(lambda p, m, q, f, s, n, b: self.before_entry(p, m, q, f, s, n, b), profile)

    def new_bundle(self):
        self.bundle_counter += 1
        return self.bundle_counter

    def _begin(self, profile, market, *, operation, trigger, bundle_id, now, enabled=True, quotes=None, features=None, prior=None, **extra):
        risk.require(self.active is None, "NESTED_DECISION_CONTEXT")
        self.counter += 1
        clock, transport, before = self.clock_status(), self.transport_status(), risk.snapshot(self.ledger.state)
        holds = []
        if clock.get("state") != "accepted":holds.append("CLOCK_CONFIDENCE_HOLD")
        if transport.get("state") != "healthy":holds.append("TRANSPORT_CONFIDENCE_HOLD")
        if before["holds"]:holds.append("PAPER_RISK_CONTINUITY_HOLD")
        if risk.decimal(before["remaining"]) <= 0:holds.append("SHARED_PAPER_RISK_CAP")
        inputs = {"market": market, "quotes": quotes or {}, "features": features or {}, "prior": prior or {}}
        data = {"schema": SCHEMA, "evaluation_id": self.counter, "bundle_id": bundle_id,
                "profile": profile.id, "market": market["id"], "operation": operation, "trigger": trigger,
                "evaluation_wall": now, "enabled": bool(enabled), "input_hashes": {key: risk.digest(value) for key, value in inputs.items()},
                "quote_refs": {key: value["raw_ref"] for key, value in (quotes or {}).items()},
                "clock_control": clock, "transport_control": transport, "risk_before": before,
                "entry_allowed": not holds, "entry_hold_reasons": holds, **extra}
        self.active = {"record": data, "profile": profile, "checks": 0}
        self.emit("DECISION_BEGIN", data)
        return data

    def _end(self):
        active = self.active
        data = active["record"]
        self.emit("DECISION_END", {"schema": SCHEMA, "evaluation_id": data["evaluation_id"], "profile": data["profile"],
                                  "market": data["market"], "hard_check_count": active["checks"],
                                  "state_sha256": risk.digest(profile_state(active["profile"], data["market"])),
                                  "risk_after_sha256": risk.digest(self.ledger.state)})
        self.active = None

    def live_check(self):
        risk.require(self.active is not None, "HARD_CHECK_OUTSIDE_DECISION_CONTEXT")
        self.active["checks"] += 1
        if self.hard_check_source:
            observed = self.hard_check_source()
        else:
            wall, mono = time.time(), time.monotonic()
            event_set = self.study.stop.is_set()
            requested = (self.study.root / "STOP").exists()
            observed = {"wall": wall, "host_wall": measured_clock.RAW_TIME(), "monotonic": mono,
                        "stop_event_set": event_set, "stop_requested": requested,
                        "hard_allowed": not event_set and not requested and wall < self.study.cutoff and mono < self.study.deadline}
        data = {"schema": SCHEMA, "evaluation_id": self.active["record"]["evaluation_id"],
                "profile": self.active["record"]["profile"], "index": self.active["checks"], **observed}
        self.emit("DECISION_GATE_CHECK", data)
        return observed["hard_allowed"]

    def invoke_tick(self, profile, market, quotes, features, prior, now, cutoff, enabled, *, trigger, bundle_id):
        data = self._begin(profile, market, operation="tick", trigger=trigger, bundle_id=bundle_id, now=now,
                           enabled=enabled, quotes=quotes, features=features, prior=prior)
        try:
            if not data["entry_allowed"] and market["id"] not in profile.positions:
                window = profile.windows.get(market["id"])
                reason = "ADMISSION:" + ",".join(data["entry_hold_reasons"])
                if window and window.get("pending"):
                    profile.cancel(window, reason, now)
                profile.phase = "NEW_EXPOSURE_HELD"
                profile.last_problem = reason
                self.emit("DECISION_SKIPPED", {"evaluation_id": data["evaluation_id"], "profile": profile.id,
                                               "market": market["id"], "reason": reason})
            else:
                profile.tick(market, quotes, features, prior, now, cutoff, enabled)
        finally:
            self._end()

    def invoke_settlement(self, profile, key, winner, market, available, raw_refs, *, bundle_id):
        self._begin(profile, market, operation="settlement", trigger="VENUE_DUAL_PROOF", bundle_id=bundle_id,
                    now=available, winner=winner, available=available, raw_refs=raw_refs)
        try:
            profile.settlement(key, winner, market, available, raw_refs)
        finally:
            self._end()

    def invoke_finalize(self, profile, key, market, *, now, bundle_id, terminal=False):
        window = profile.windows.get(key)
        # P13/P2's frozen finalize method is a no-op for already ended/filled
        # windows. Avoid producing quadratic historical no-op context volume.
        if not window or window["initial_entry"] != "SELECTIVE_WAIT":
            return
        self._begin(profile, market, operation="terminal_cancel" if terminal else "finalize_window",
                    trigger="TERMINAL" if terminal else "SCHEDULED_FINALIZE", bundle_id=bundle_id, now=now)
        try:
            profile.finalize_window(key)
        finally:
            self._end()

    def _reservation_id(self, profile, market):
        pending = profile.windows.get(market["id"], {}).get("pending")
        signal_wall = pending["signal_wall"] if pending else self.active["record"]["evaluation_wall"]
        return risk.digest({"campaign_id": self.ledger.state["campaign_id"], "segment_id": self.ledger.state["segment_id"],
                            "profile": profile.id, "market": market["id"], "signal_wall": signal_wall})

    def _action(self, action, payload):
        self.action_counter += 1
        event_id = self.ledger.state["segment_id"] + ":" + str(self.action_counter)
        result = self.ledger.apply(event_id, action, payload)
        self.emit("PAPER_RISK_ACTION", {"event_id": event_id, "action": action, "payload": payload, "result": result})
        return result

    def _payload(self, profile, market, quote, fill, signal, now, budget, *, intent=False):
        pending = None if intent else profile.windows[market["id"]].get("pending")
        return {"reservation_id": self._reservation_id(profile, market), "profile": profile.id, "market": market["id"],
                "side": signal["side"], "signal_wall": pending["signal_wall"] if pending else now,
                "quantity": fill["quantity"], "fee_rate": market["fee_rate"], "maximum_allin": budget,
                "basis": fill["basis"], "entry_fee": fill["entry_fee"], "quote_reference": quote["raw_ref"]}

    def before_intent(self, profile, market, quote, fill, signal, now, budget):
        if not self.active["record"]["entry_allowed"]:
            return False
        payload = self._payload(profile, market, quote, fill, signal, now, budget, intent=True)
        result = self._action("RESERVE_INTENT", payload)
        if not result["accepted"]:
            profile.phase, profile.last_problem = "SHARED_RISK_HOLD", result["reason"]
            return False
        if not self.live_check():
            self._action("CANCEL_INTENT", {"reservation_id": payload["reservation_id"], "reason": "HARD_CUTOFF_BEFORE_INTENT"})
            return False
        return True

    def before_entry(self, profile, market, quote, fill, signal, now, budget):
        if not self.active["record"]["entry_allowed"]:
            return False
        payload = self._payload(profile, market, quote, fill, signal, now, budget)
        result = self._action("PREPARE_ENTRY", payload)
        if not result["accepted"]:
            profile.cancel(profile.windows[market["id"]], result["reason"], now)
            profile.phase, profile.last_problem = "SHARED_RISK_HOLD", result["reason"]
            return False
        if not self.live_check():
            self._action("ABORT_PREPARED_ENTRY", {"reservation_id": payload["reservation_id"], "hard_check_denied_before_profile_mutation": True})
            return False
        return True

    def financial_event(self, kind, data):
        if kind not in FINANCIAL and kind != "PAPER_ENTRY_INTENT_CANCELLED":
            return
        matches = [key for key, row in self.ledger.state["reservations"].items() if row["profile"] == data["profile"] and row["market"] == data["market"]]
        if kind == "PAPER_ENTRY_INTENT_CANCELLED" and not matches:
            return
        risk.require(len(matches) == 1, "FINANCIAL_FACT_REQUIRES_ONE_SHARED_RESERVATION")
        identity = matches[0]
        if kind == "PAPER_ENTRY_INTENT_CANCELLED":
            self._action("CANCEL_INTENT", {"reservation_id": identity, "reason": data["reason"]})
        else:
            action = {"PAPER_ENTRY": "COMMIT_ENTRY", "PAPER_EXIT": "EXIT", "PAPER_SETTLEMENT": "SETTLEMENT"}[kind]
            self._action(action, {"reservation_id": identity, "financial": data})

    def finish(self, receipt_path, profiles):
        carry = account_carryover(profiles, self.initial_accounts)
        self.ledger.finish_segment(receipt_path, carry)
        closure = {"schema": "LANE_II_PAPER_RISK_CLOSURE_V1", "receipt": self.ledger.state["last_terminal"],
                   "state": deepcopy(self.ledger.state), "snapshot": risk.snapshot(self.ledger.state),
                   "anchor_sha256": hashlib.sha256(self.ledger.manifest_path.read_bytes()).hexdigest()}
        path = Path(receipt_path).resolve().parent.parent / "RISK_CLOSURE.json"
        with path.open("xb") as stream:
            stream.write(risk.canonical(closure));stream.flush();os.fsync(stream.fileno())
        self.ledger.close()
