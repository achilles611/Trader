"""One durable international-paper risk campaign, distinct from native Phase D.

This is a simulation-only Decimal controller.  Source-native fills/fees remain
unchanged; it reserves risk before admitting an intent or a changed fill plan.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import closing
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import uuid

SCHEMA = "LANE_II_SHARED_PAPER_RISK_V1"
DOMAIN = "INTERNATIONAL_P13_P2_PAPER"
POLICY = {"schema": SCHEMA, "domain": DOMAIN, "maximum_loss": "25.00",
          "profile_budgets": {"5m-P13": "5.00", "5m-P2": "6.00"},
          "admission_order": ["5m-P13", "5m-P2"],
          "fee_formula": "quantity * observed_rate * price * (1-price), half-up 5dp",
          "exit_fee_reserve": "ceil_5dp(quantity * observed_rate / 4)",
          "profits_refill_consumed_losses_or_fees": False, "separate_native_phase_d_kernel": True}
ZERO = Decimal(0)
EPS = Decimal(".0000001")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def decimal(value):
    if isinstance(value, bool):
        raise ValueError("RISK_FINITE_DECIMAL_REQUIRED")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("RISK_FINITE_DECIMAL_REQUIRED")
    return result


def require(value, reason):
    if not value:
        raise ValueError(reason)


def snapshot(state):
    principal = working = reserve = ZERO
    unknown = []
    for identity, row in state["reservations"].items():
        if row["state"] == "OPEN":
            principal += decimal(row["principal"])
        else:
            working += decimal(row["maximum_allin"])
        reserve += decimal(row["exit_fee_reserve"])
        if row["state"] == "UNKNOWN":
            unknown.append(identity)
    gross, fees = decimal(state["gross_losses"]), decimal(state["paid_fees"])
    total = gross + fees + principal + working + reserve
    holds = list(state.get("holds", []))
    if unknown:
        holds.append("UNKNOWN_ENTRY_REQUIRES_RECONCILIATION")
    if total > decimal(state["maximum_loss"]):
        holds.append("AGGREGATE_PAPER_CAP_BREACH")
    return {"schema": SCHEMA, "campaign_id": state["campaign_id"], "domain": DOMAIN,
            "gross_losses": str(gross), "paid_fees": str(fees), "open_principal": str(principal),
            "working_and_unknown_allin": str(working), "exit_fee_reserve": str(reserve),
            "consumed_or_reserved": str(total), "maximum_loss": state["maximum_loss"],
            "remaining": str(max(ZERO, decimal(state["maximum_loss"]) - total)),
            "unknown_reservations": sorted(unknown), "holds": sorted(set(holds)),
            "profits_refill_consumed_losses_or_fees": False, "state_sha256": digest(state)}


def _proposal(payload):
    profile = payload["profile"]
    require(profile in POLICY["profile_budgets"], "RISK_PROFILE_NOT_BOUND")
    quantity, rate, maximum = decimal(payload["quantity"]), decimal(payload["fee_rate"]), decimal(payload["maximum_allin"])
    basis, entry_fee = decimal(payload["basis"]), decimal(payload["entry_fee"])
    require(quantity == quantity.to_integral_value() and 0 < quantity <= 200 and ZERO <= rate <= 1,
            "RISK_QUANTITY_OR_FEE_RATE")
    require(ZERO <= entry_fee <= basis <= maximum + EPS and ZERO < maximum <= decimal(POLICY["profile_budgets"][profile]),
            "RISK_EXACT_ENTRY_BUDGET")
    return {"profile": profile, "market": payload["market"], "side": payload["side"],
            "maximum_allin": str(maximum), "quantity": str(quantity), "fee_rate": str(rate),
            "basis": str(basis), "entry_fee": str(entry_fee),
            "exit_fee_reserve": str((quantity * rate / 4).quantize(Decimal(".00001"), rounding=ROUND_CEILING)),
            "quote_reference": payload["quote_reference"], "signal_wall": payload["signal_wall"]}


def transition(state, action, payload):
    """Pure transition used independently by capture and strict replay."""
    require(state["schema"] == SCHEMA and state["domain"] == DOMAIN, "RISK_STATE_SCHEMA")
    before, changed = digest(state), deepcopy(state)
    identity = payload.get("reservation_id")
    row = changed["reservations"].get(identity)
    accepted, reason = True, None
    if action in ("RESERVE_INTENT", "PREPARE_ENTRY"):
        if snapshot(state)["holds"]:
            accepted, reason = False, "PAPER_RISK_CONTINUITY_HOLD"
        else:
            proposal = _proposal(payload)
            require(proposal["side"] in ("Up", "Down") and isinstance(identity, str) and identity, "RISK_RESERVATION_IDENTITY")
            if action == "RESERVE_INTENT":
                require(row is None and not any(r["profile"] == proposal["profile"] and r["market"] == proposal["market"] for r in changed["reservations"].values()), "DUPLICATE_RISK_INTENT")
            else:
                require(row is not None and row["state"] == "RESERVED" and all(row[k] == proposal[k] for k in ("profile", "market", "side", "signal_wall")), "RISK_ENTRY_RESERVATION_BINDING")
            changed["reservations"][identity] = {**proposal, "state": "RESERVED" if action == "RESERVE_INTENT" else "UNKNOWN"}
            if decimal(snapshot(changed)["consumed_or_reserved"]) > decimal(changed["maximum_loss"]):
                changed = deepcopy(state)
                accepted, reason = False, "SHARED_PAPER_RISK_CAP"
    elif action in ("CANCEL_INTENT", "ABORT_PREPARED_ENTRY"):
        require(row is not None, "CANCEL_UNKNOWN_RESERVATION")
        require(row["state"] == ("UNKNOWN" if action == "ABORT_PREPARED_ENTRY" else "RESERVED"), "AMBIGUOUS_OR_FILLED_ENTRY_CANNOT_BE_CANCELLED")
        if action == "ABORT_PREPARED_ENTRY":
            require(payload.get("hard_check_denied_before_profile_mutation") is True, "PREPARED_ABORT_NEEDS_LOCAL_PREMUTATION_PROOF")
        del changed["reservations"][identity]
    elif action == "COMMIT_ENTRY":
        require(row is not None and row["state"] == "UNKNOWN", "ENTRY_REQUIRES_DURABLE_UNKNOWN_RESERVATION")
        data = payload["financial"]
        for key in ("market", "side", "quote_reference"):
            require(data[key] == row[key], "ENTRY_FACT_IDENTITY")
        for key in ("quantity", "basis", "entry_fee"):
            require(abs(decimal(data[key]) - decimal(row[key])) <= EPS, "ENTRY_FACT_ECONOMICS")
        require(data["profile"] == row["profile"], "ENTRY_FACT_PROFILE")
        row.update(state="OPEN", principal=str(decimal(row["basis"]) - decimal(row["entry_fee"])))
        changed["paid_fees"] = str(decimal(changed["paid_fees"]) + decimal(row["entry_fee"]))
    elif action in ("EXIT", "SETTLEMENT"):
        require(row is not None and row["state"] == "OPEN", "PROVEN_OWNED_OPEN_POSITION_REQUIRED")
        data = payload["financial"]
        require(data["profile"] == row["profile"] and data["market"] == row["market"] and data["side"] == row["side"], "CLOSE_FACT_IDENTITY")
        if action == "EXIT":
            fee = decimal(data["exit_fee"])
            require(ZERO <= fee <= decimal(row["exit_fee_reserve"]) + EPS, "EXIT_FEE_EXCEEDS_RESERVED_BOUND")
            require(sum(decimal(fill["quantity"]) for fill in data["fills"]) == decimal(row["quantity"]), "EXIT_QUANTITY_MISMATCH")
            proceeds = decimal(data["proceeds"])
            require(abs(proceeds - decimal(row["basis"]) - decimal(data["net"])) <= EPS, "EXIT_NET_IDENTITY")
            gross_proceeds = proceeds + fee
        else:
            require(data["payout"] in (0, 1) and len(data["raw_references"]) == 2, "SETTLEMENT_DUAL_PROOF_REQUIRED")
            fee, gross_proceeds = ZERO, decimal(row["quantity"]) * decimal(data["payout"])
            require(abs(gross_proceeds - decimal(row["basis"]) - decimal(data["net"])) <= EPS, "SETTLEMENT_NET_IDENTITY")
        changed["gross_losses"] = str(decimal(changed["gross_losses"]) + max(ZERO, decimal(row["principal"]) - gross_proceeds))
        changed["paid_fees"] = str(decimal(changed["paid_fees"]) + fee)
        del changed["reservations"][identity]
    else:
        raise ValueError("UNKNOWN_PAPER_RISK_ACTION:" + action)
    result = {"accepted": accepted, "reason": reason, "reservation_id": identity,
              "before_sha256": before, "after_sha256": digest(changed), "snapshot": snapshot(changed)}
    require(decimal(result["snapshot"]["consumed_or_reserved"]) <= decimal(changed["maximum_loss"]), "FACTUAL_PAPER_RISK_CAP_BREACH")
    return changed, result


def initialize_campaign(db_path, manifest_path, *, campaign_id, roster_sha256, maximum_loss="25.00"):
    """Parent-only explicit genesis; startup never calls this initializer."""
    db, manifest = Path(db_path).resolve(), Path(manifest_path).resolve()
    require(ZERO < decimal(maximum_loss) <= 25 and isinstance(campaign_id, str) and campaign_id, "PAPER_CAMPAIGN_CAP")
    require(not db.exists() and not manifest.exists(), "PAPER_CAMPAIGN_ALREADY_EXISTS")
    db.parent.mkdir(parents=True, exist_ok=True)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with db.open("xb"):
        pass
    identity = uuid.uuid4().hex
    state = {"schema": SCHEMA, "domain": DOMAIN, "campaign_id": campaign_id, "database_id": identity,
             "roster_sha256": roster_sha256, "maximum_loss": str(decimal(maximum_loss)), "policy_sha256": digest(POLICY),
             "gross_losses": "0", "paid_fees": "0", "reservations": {}, "holds": [],
             "phase": "GENESIS", "segment_id": None, "last_terminal": None, "carry_accounts": {}}
    with closing(sqlite3.connect(db)) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.executescript("CREATE TABLE state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),value_json TEXT NOT NULL);"
                                 "CREATE TABLE actions(sequence INTEGER PRIMARY KEY,event_id TEXT UNIQUE NOT NULL,action TEXT NOT NULL,payload_json TEXT NOT NULL,result_json TEXT NOT NULL);"
                                 "CREATE TABLE segments(segment_id TEXT PRIMARY KEY,previous_receipt_sha256 TEXT,state TEXT NOT NULL,terminal_json TEXT);")
        connection.execute("INSERT INTO state VALUES(1,?)", (canonical(state).decode(),))
        connection.commit()
    info = db.stat()
    value = {"schema": "LANE_II_PAPER_RISK_ANCHOR_V1", "db_path": str(db), "database_id": identity,
             "db_file_identity": {"device": info.st_dev, "inode": info.st_ino}, "campaign_id": campaign_id,
             "roster_sha256": roster_sha256, "maximum_loss": state["maximum_loss"], "policy": POLICY,
             "policy_sha256": digest(POLICY), "genesis_state_sha256": digest(state)}
    with manifest.open("xb") as stream:
        stream.write(canonical(value));stream.flush();os.fsync(stream.fileno())
    return {"path": str(manifest), "bytes": manifest.stat().st_size, "sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}


def validate_closure(run_dir, expected_anchor_sha256=None):
    """Read immutable endpoint evidence; never open the mutable campaign DB."""
    root = Path(run_dir).resolve()
    closure_path, receipt_path = root / "RISK_CLOSURE.json", root / "capture/receipt.json"
    require(closure_path.stat().st_size <= 8 * 1024**2 and receipt_path.stat().st_size <= 8 * 1024**2, "RISK_ENDPOINT_SIZE_BOUND")
    raw, receipt_raw = closure_path.read_bytes(), receipt_path.read_bytes()
    closure, receipt = json.loads(raw), json.loads(receipt_raw)
    require(closure["schema"] == "LANE_II_PAPER_RISK_CLOSURE_V1", "RISK_CLOSURE_SCHEMA")
    endpoint = closure["receipt"]
    require(Path(endpoint["receipt_path"]).resolve() == receipt_path and endpoint["receipt_sha256"] == hashlib.sha256(receipt_raw).hexdigest()
            and endpoint["source_manifest_sha256"] == receipt["source_manifest_sha256"] and endpoint["state"] == receipt["state"], "RISK_CLOSURE_RECEIPT_BINDING")
    require(expected_anchor_sha256 is None or closure["anchor_sha256"] == expected_anchor_sha256, "RISK_CLOSURE_ANCHOR_BINDING")
    expected = deepcopy(receipt["paper_risk_terminal_state"])
    require(receipt["paper_risk"] == snapshot(expected), "RISK_RECEIPT_SNAPSHOT")
    expected.update(phase="CLOSED", last_terminal=endpoint, carry_accounts=receipt["account_carryover"])
    require(expected == closure["state"] and snapshot(expected) == closure["snapshot"], "RISK_CLOSURE_STATE_BINDING")
    require(decimal(closure["snapshot"]["consumed_or_reserved"]) <= decimal(expected["maximum_loss"]), "RISK_CLOSURE_CAP")
    return {"schema": "LANE_II_PAPER_RISK_CLOSURE_VALIDATION_V1", "valid": True,
            "flat": not expected["reservations"] and not any(a["positions"] for a in receipt["account_carryover"].values()),
            "campaign_id": expected["campaign_id"], "segment_id": expected["segment_id"],
            "receipt_sha256": endpoint["receipt_sha256"], "closure_sha256": hashlib.sha256(raw).hexdigest(),
            "anchor_sha256": closure["anchor_sha256"], "snapshot": closure["snapshot"]}


class RiskLedger:
    def __init__(self, manifest_path, expected_sha256, *, roster_sha256):
        self.manifest_path = Path(manifest_path).resolve()
        raw = self.manifest_path.read_bytes()
        require(hashlib.sha256(raw).hexdigest() == expected_sha256, "PAPER_RISK_ANCHOR_CHANGED")
        anchor = self.anchor = json.loads(raw)
        require(anchor["schema"] == "LANE_II_PAPER_RISK_ANCHOR_V1" and anchor["roster_sha256"] == roster_sha256
                and anchor["policy_sha256"] == digest(POLICY) and anchor["policy"] == POLICY, "PAPER_RISK_ANCHOR_SCOPE")
        self.path = Path(anchor["db_path"]).resolve()
        info = self.path.stat()  # Missing expected state must never create a DB.
        require({"device": info.st_dev, "inode": info.st_ino} == anchor["db_file_identity"], "PAPER_RISK_DATABASE_REPLACED")
        self.connection = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=1.5, isolation_level=None)
        self.connection.execute("PRAGMA synchronous=FULL")
        require(self.connection.execute("PRAGMA quick_check").fetchone()[0] == "ok", "PAPER_RISK_DATABASE_CORRUPT")
        self.state = self._read()
        require(self.state["database_id"] == anchor["database_id"] and self.state["campaign_id"] == anchor["campaign_id"]
                and self.state["maximum_loss"] == anchor["maximum_loss"] and self.state["policy_sha256"] == digest(POLICY), "PAPER_RISK_STATE_BINDING")
        if self.state["phase"] == "GENESIS":
            require(digest(self.state) == anchor["genesis_state_sha256"], "PAPER_RISK_GENESIS_CHANGED")
        elif self.state["phase"] == "CLOSED":
            endpoint = self.state["last_terminal"]
            crashed = endpoint.get("kind") == "CRASH_HANDOFF"
            previous_raw = Path(endpoint["handoff_path"] if crashed else endpoint["receipt_path"]).read_bytes()
            require(hashlib.sha256(previous_raw).hexdigest() == endpoint["handoff_sha256" if crashed else "receipt_sha256"], "PAPER_RISK_PREDECESSOR_CHANGED")
            previous = json.loads(previous_raw)
            if crashed:
                require(previous["schema"] == "LANE_II_CRASH_HANDOFF_V1" and previous["normal_terminal_receipt"] is False
                        and previous["anchor_sha256"] == expected_sha256, "PAPER_RISK_CRASH_HANDOFF_BINDING")
                try:
                    from continuous_capture_checkpoint import validate_crash_handoff
                except ImportError:
                    from .capture_checkpoint import validate_crash_handoff
                require(validate_crash_handoff(endpoint["handoff_path"], expected_sha256)["closed_state"] == self.state,
                        "PAPER_RISK_CRASH_CLOSURE_CHANGED")
            expected = deepcopy(previous["paper_risk_terminal_state"])
            expected.update(phase="CLOSED", last_terminal=endpoint, carry_accounts=previous["account_carryover"])
            require(expected == self.state, "PAPER_RISK_TERMINAL_STATE_CHANGED")
        self.segment_id = None

    def _read(self):
        return json.loads(self.connection.execute("SELECT value_json FROM state WHERE singleton=1").fetchone()[0])

    def _write(self, state):
        self.connection.execute("UPDATE state SET value_json=? WHERE singleton=1", (canonical(state).decode(),))

    def action_cursor(self):
        row = self.connection.execute("SELECT sequence,event_id,action,payload_json,result_json FROM actions ORDER BY sequence DESC LIMIT 1").fetchone()
        return {"sequence": 0 if row is None else row[0], "event_id": None if row is None else row[1],
                "sha256": None if row is None else digest(list(row))}

    def begin_segment(self, segment_id, previous_receipt_sha256="GENESIS", *, paper_start_epoch=None, cutoff_epoch=None):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            state = self._read()
            require(state["phase"] in ("GENESIS", "CLOSED") and not state["reservations"] and not state["holds"], "PAPER_ACCOUNT_CONTINUITY_UNKNOWN_OR_OPEN")
            endpoint = state["last_terminal"]
            crashed = state["phase"] == "CLOSED" and endpoint.get("kind") == "CRASH_HANDOFF"
            expected = "GENESIS" if state["phase"] == "GENESIS" else "CRASH:" + endpoint["handoff_sha256"] if crashed else endpoint["receipt_sha256"]
            require(previous_receipt_sha256 == expected, "PAPER_PREDECESSOR_RECEIPT_REQUIRED")
            if crashed:
                require(cutoff_epoch == endpoint["fixed_cutoff_epoch"] and paper_start_epoch is not None
                        and endpoint["restart_not_before_epoch"] <= paper_start_epoch < cutoff_epoch
                        and paper_start_epoch % 300 == 0, "PAPER_CRASH_FIXED_CUTOFF_OR_RESTART_WINDOW")
            require(self.connection.execute("SELECT 1 FROM segments WHERE segment_id=?", (segment_id,)).fetchone() is None, "PAPER_SEGMENT_CANNOT_RESTART")
            self.connection.execute("INSERT INTO segments VALUES(?,?,?,NULL)", (segment_id, previous_receipt_sha256, "ACTIVE"))
            state.update(phase="ACTIVE", segment_id=segment_id)
            self._write(state)
            self.connection.execute("COMMIT")
            self.state = state
            self.segment_id = segment_id
            return deepcopy(state)
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def apply(self, event_id, action, payload):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            state = self._read()
            require(state == self.state, "RISK_STATE_CHANGED_OUTSIDE_OWNED_TRANSITION")
            previous = self.connection.execute("SELECT action,payload_json,result_json FROM actions WHERE event_id=?", (event_id,)).fetchone()
            if previous:
                require(previous[0] == action and previous[1] == canonical(payload).decode(), "RISK_IDEMPOTENCY_CONTENT_CHANGED")
                self.connection.execute("COMMIT")
                self.state = self._read()
                return json.loads(previous[2])
            require(state["phase"] == "ACTIVE" and state["segment_id"] == self.segment_id, "RISK_SEGMENT_OWNER_CHANGED")
            changed, result = transition(state, action, payload)
            self.connection.execute("INSERT INTO actions(event_id,action,payload_json,result_json) VALUES(?,?,?,?)",
                                    (event_id, action, canonical(payload).decode(), canonical(result).decode()))
            self._write(changed)
            self.connection.execute("COMMIT")
            self.state = changed
            return result
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def finish_segment(self, receipt_path, carry_accounts):
        path = Path(receipt_path).resolve()
        raw = path.read_bytes();receipt = json.loads(raw)
        terminal = {"receipt_path": str(path), "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_manifest_sha256": receipt["source_manifest_sha256"], "state": receipt["state"],
                    "actual_end_utc": receipt["actual_end_utc"], "cutoff_utc": receipt["cutoff_utc"]}
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            state = self._read()
            require(state["phase"] == "ACTIVE" and state["segment_id"] == self.segment_id, "RISK_SEGMENT_OWNER_CHANGED")
            require(receipt["paper_risk_terminal_state"] == state and receipt["account_carryover"] == carry_accounts, "RISK_TERMINAL_RECEIPT_BINDING")
            require(not any(r["state"] in ("RESERVED", "UNKNOWN") for r in state["reservations"].values()), "UNRESOLVED_ENTRY_RESERVATION_AT_CLOSE")
            state.update(phase="CLOSED", last_terminal=terminal, carry_accounts=deepcopy(carry_accounts))
            self._write(state)
            self.connection.execute("UPDATE segments SET state='CLOSED',terminal_json=? WHERE segment_id=?", (canonical(terminal).decode(), self.segment_id))
            self.connection.execute("COMMIT")
            self.state = state
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def finish_crashed_segment(self, handoff_path):
        """Bind a typed, externally verified crash handoff without resetting risk."""
        path = Path(handoff_path).resolve()
        try:
            from continuous_capture_checkpoint import validate_crash_handoff
        except ImportError:
            from .capture_checkpoint import validate_crash_handoff
        validated = validate_crash_handoff(path, hashlib.sha256(self.manifest_path.read_bytes()).hexdigest())
        require(path.stat().st_size <= 2 * 1024**2, "PAPER_CRASH_HANDOFF_SIZE")
        raw = path.read_bytes(); handoff = json.loads(raw)
        require(handoff["schema"] == "LANE_II_CRASH_HANDOFF_V1" and handoff["normal_terminal_receipt"] is False
                and handoff["anchor_sha256"] == hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()
                and handoff["excluded_tail"]["financial_rows"] == 0, "PAPER_CRASH_HANDOFF_BINDING")
        before = handoff["paper_risk_terminal_state"]
        require(before["phase"] == "ACTIVE" and before["segment_id"] == handoff["segment_id"]
                and not before["reservations"] and not snapshot(before)["holds"]
                and not any(row["positions"] for row in handoff["account_carryover"].values()), "PAPER_CRASH_ACCOUNT_NOT_FLAT")
        terminal = {"kind": "CRASH_HANDOFF", "handoff_path": str(path), "handoff_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_manifest_sha256": handoff["source_manifest_sha256"], "state": "FAILED_CRASH_HANDOFF",
                    "fixed_cutoff_epoch": handoff["fixed_cutoff_epoch"], "restart_not_before_epoch": handoff["restart_not_before_epoch"]}
        after = deepcopy(before)
        after.update(phase="CLOSED", last_terminal=terminal, carry_accounts=deepcopy(handoff["account_carryover"]))
        require(validated["closed_state"] == after, "PAPER_CRASH_CLOSURE_BINDING")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            state = self._read()
            require(self.action_cursor() == handoff["paper_risk_action_cursor"], "CRASH_UNACCOUNTED_RISK_TRANSACTIONS")
            if state != after:
                require(state == before, "CRASH_UNACCOUNTED_RISK_TRANSACTIONS")
                require(self.connection.execute("SELECT state FROM segments WHERE segment_id=?", (before["segment_id"],)).fetchone() == ("ACTIVE",), "PAPER_CRASH_SEGMENT_BINDING")
                self._write(after)
                self.connection.execute("UPDATE segments SET state='FAILED_CRASH_HANDOFF',terminal_json=? WHERE segment_id=?",
                                        (canonical(terminal).decode(), before["segment_id"]))
            self.connection.execute("COMMIT")
            self.state = after
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def close(self):
        self.connection.close()
