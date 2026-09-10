"""One-click, backend-owned startup for persistent Sim101 paper operation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from enum import StrEnum
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Callable, Iterator, Mapping
from uuid import uuid4

from .contracts import ACTIVE_PROTECTIVE_ORDER_STATES, POLICY


PAPER_AUTOSTART_SCHEMA = "lane-iii-paper-autostart-v1"
PAPER_AUTOSTART_ACTION_HEADER = "X-Beelzebub-Paper-Autostart-Action"
PAPER_AUTOSTART_ACTION_VALUE = "sim101-paper-autostart-v1"
PAPER_AUTOSTART_TOKEN_HEADER = "X-Beelzebub-Paper-Autostart-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")
_PERPETUAL_PROFILE = "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"
_SCALPER_PROFILE = "BEELZEBUB_SCALPER_V2"
_SCALPER_WARMUP_FAMILIES = frozenset({
    "STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY",
})
_SCALPER_COVERAGE_HORIZON_SECONDS = float(max(
    POLICY.structural_evidence_lifetime_seconds,
    POLICY.flow_evidence_lifetime_seconds,
    POLICY.liquidity_evidence_lifetime_seconds,
    POLICY.hypothesis_idle_lifetime_seconds,
    POLICY.decision_ttl_seconds,
))
_SCALPER_STARTUP_SLACK_SECONDS = 30.0
SCALPER_READINESS_TIMEOUT_SECONDS = (
    _SCALPER_COVERAGE_HORIZON_SECONDS + _SCALPER_STARTUP_SLACK_SECONDS
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PaperAutoStartStage(StrEnum):
    IDLE = "IDLE"
    ENSURING_NINJATRADER = "ENSURING_NINJATRADER"
    VERIFYING_FULL_LEDGER = "VERIFYING_FULL_LEDGER"
    WAITING_FOR_EVIDENCE = "WAITING_FOR_EVIDENCE"
    STARTING_PAPER = "STARTING_PAPER"
    RUNNING = "RUNNING"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_ACTIVE = frozenset({
    PaperAutoStartStage.ENSURING_NINJATRADER,
    PaperAutoStartStage.VERIFYING_FULL_LEDGER,
    PaperAutoStartStage.WAITING_FOR_EVIDENCE,
    PaperAutoStartStage.STARTING_PAPER,
})


class PaperAutoStartService:
    """Compose existing guarded startup operations without browser authority.

    The service never restarts NinjaTrader. It may launch an absent desktop via
    the normal maintenance startup mode, waits for authenticated observer and
    Sim101 reconciliation, obtains a current Full ledger proof, and only then
    invokes the canonical persistent operational-start method.
    """

    def __init__(
        self,
        *,
        paper_status: Callable[[], Mapping[str, object]],
        ensure_ninjatrader: Callable[[str], Mapping[str, object]],
        ninjatrader_status: Callable[[], Mapping[str, object]],
        start_full_verification: Callable[[], Mapping[str, object]],
        ledger_status: Callable[[], Mapping[str, object]],
        operational_readiness: Callable[[], Mapping[str, object]],
        start_operational_paper: Callable[[str], Mapping[str, object]],
        begin_startup_observation_pause: Callable[[], Mapping[str, object]],
        end_startup_observation_pause: Callable[[], Mapping[str, object]],
        stop_operational_paper: Callable[[], Mapping[str, object]],
        audit_path: str | Path,
        startup_timeout_seconds: float = 360.0,
        ledger_timeout_seconds: float = 180.0,
        readiness_timeout_seconds: float = SCALPER_READINESS_TIMEOUT_SECONDS,
        poll_seconds: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        wait: Callable[[float], bool] | None = None,
    ) -> None:
        if min(
            startup_timeout_seconds, ledger_timeout_seconds,
            readiness_timeout_seconds, poll_seconds,
        ) <= 0:
            raise ValueError("Paper auto-start timeouts must be positive.")
        self._paper_status = paper_status
        self._ensure_ninjatrader = ensure_ninjatrader
        self._ninjatrader_status = ninjatrader_status
        self._start_full_verification = start_full_verification
        self._ledger_status = ledger_status
        self._operational_readiness = operational_readiness
        self._start_operational_paper = start_operational_paper
        self._begin_startup_observation_pause = begin_startup_observation_pause
        self._end_startup_observation_pause = end_startup_observation_pause
        self._stop_operational_paper = stop_operational_paper
        self._audit_path = Path(audit_path).resolve()
        self._startup_timeout_seconds = startup_timeout_seconds
        self._ledger_timeout_seconds = ledger_timeout_seconds
        self._readiness_timeout_seconds = readiness_timeout_seconds
        self._poll_seconds = poll_seconds
        self._clock = clock
        self._stop = threading.Event()
        self._custom_wait = wait
        self._wait = wait or self._stop.wait
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._operation_id: str | None = None
        self._request_id: str | None = None
        self._stage = PaperAutoStartStage.IDLE
        self._stage_started_at = _utc_now()
        self._started_at: str | None = None
        self._blockers: list[str] = []
        self._maintenance: dict[str, object] = {}
        self._ledger: dict[str, object] = {}
        self._readiness: dict[str, object] = {}
        self._warmup_started_monotonic: float | None = None
        self._warmup_progress: dict[str, object] = {}
        self._result: dict[str, object] = {}
        self._diagnostics: list[dict[str, str]] = []
        self._audit_healthy = True
        self._action_token = secrets.token_urlsafe(32)

    @property
    def action_token(self) -> str:
        return self._action_token

    @staticmethod
    def _operational_active(paper: Mapping[str, object]) -> bool:
        session = paper.get("operational_paper_session")
        return isinstance(session, Mapping) and session.get("active") is True

    @staticmethod
    def _requires_position_proof(paper: Mapping[str, object]) -> bool:
        return paper.get("entry_profile_version") == _PERPETUAL_PROFILE

    @staticmethod
    def _requires_scalper_warmup(paper: Mapping[str, object]) -> bool:
        return paper.get("entry_profile_version") == _SCALPER_PROFILE

    @classmethod
    def _perpetual_position_proven(cls, paper: Mapping[str, object]) -> bool:
        if not cls._requires_position_proof(paper):
            return cls._operational_active(paper)
        position = paper.get("current_position")
        requirement = paper.get("position_requirement")
        source = requirement.get("source_signal") if isinstance(requirement, Mapping) else None
        reasons = requirement.get("blocking_reasons") if isinstance(requirement, Mapping) else None
        expected_state = position if position in {"LONG", "SHORT"} else None
        return bool(
            cls._operational_active(paper)
            and expected_state is not None
            and paper.get("state") == expected_state
            and paper.get("paper_execution") == "POSITIONED"
            and type(paper.get("current_quantity")) is int
            and paper.get("current_quantity") == 1
            and type(paper.get("current_position_quantity")) is int
            and paper.get("current_position_quantity") == 1
            and paper.get("broker_snapshot_position") == expected_state
            and type(paper.get("broker_snapshot_position_quantity")) is int
            and paper.get("broker_snapshot_position_quantity") == 1
            and type(paper.get("working_owned_orders")) is int
            and paper.get("working_owned_orders") == 1
            and type(paper.get("working_entry_orders")) is int
            and paper.get("working_entry_orders") == 0
            and paper.get("protective_stop_state") in ACTIVE_PROTECTIVE_ORDER_STATES
            and paper.get("foreign_activity") is False
            and paper.get("position_snapshot_complete") is True
            and paper.get("order_snapshot_complete") is True
            and paper.get("reconciliation_current") is True
            and paper.get("unresolved_command") is False
            and paper.get("unresolved_native_order") is False
            and paper.get("unresolved_execution") is False
            and isinstance(requirement, Mapping)
            and requirement.get("required") is True
            and requirement.get("state") == "POSITIONED"
            and requirement.get("actual_position") == expected_state
            and type(requirement.get("actual_quantity")) is int
            and requirement.get("actual_quantity") == 1
            and requirement.get("desired_position") == expected_state
            and requirement.get("primary_blocker") is None
            and isinstance(reasons, (list, tuple))
            and not reasons
            and isinstance(source, Mapping)
            and source.get("direction") == expected_state
            and isinstance(source.get("candle_close_utc"), str)
            and bool(str(source.get("candle_close_utc")).strip())
            and isinstance(source.get("signal_hash"), str)
            and bool(str(source.get("signal_hash")).strip())
            and type(source.get("ledger_sequence")) is int
            and source.get("ledger_sequence", 0) > 0
            and isinstance(source.get("record_hash"), str)
            and bool(str(source.get("record_hash")).strip())
            and source.get("ledger_verified") is True
        )

    @staticmethod
    def _position_blocker(paper: Mapping[str, object]) -> str:
        requirement = paper.get("position_requirement")
        if isinstance(requirement, Mapping):
            blocker = requirement.get("primary_blocker")
            if isinstance(blocker, str) and blocker:
                return blocker
        reason = paper.get("lockout_or_fault_reason")
        if isinstance(reason, str) and reason:
            return reason
        return "PERPETUAL_POSITION_NOT_PROVEN"

    @staticmethod
    def _base_start_failures(paper: Mapping[str, object]) -> list[str]:
        checks = (
            (paper.get("paper_execution") == "DISARMED", "PAPER_RUNTIME_NOT_DISARMED"),
            (paper.get("session_armed_state") == "DISARMED", "SESSION_AUTHORITY_NOT_DISARMED"),
            (paper.get("live_capital") == "DENIED", "LIVE_CAPITAL_NOT_DENIED"),
            (paper.get("paper_account") == "Sim101", "PAPER_ACCOUNT_NOT_SIM101"),
            (paper.get("account_class") == "LOCAL_SIMULATION", "ACCOUNT_NOT_LOCAL_SIMULATION"),
            (paper.get("market_instrument") == "MNQ SEP26", "INSTRUMENT_NOT_MNQ_SEP26"),
            (paper.get("maximum_quantity") == 1, "MAXIMUM_QUANTITY_NOT_ONE"),
            (paper.get("current_position") == "FLAT", "POSITION_NOT_FLAT"),
            (paper.get("current_quantity") == 0, "POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("working_owned_orders") == 0, "WORKING_OWNED_ORDERS_PRESENT"),
            (paper.get("working_entry_orders") == 0, "WORKING_ENTRY_ORDERS_PRESENT"),
            (paper.get("unresolved_command") is False, "UNRESOLVED_COMMAND_STATE"),
            (paper.get("unresolved_native_order") is False, "UNRESOLVED_NATIVE_ORDER_STATE"),
            (paper.get("unresolved_execution") is False, "UNRESOLVED_EXECUTION_STATE"),
            (paper.get("entry_owner") == "NONE", "UNRESOLVED_OWNERSHIP_STATE"),
        )
        return [code for passed, code in checks if not passed]

    @classmethod
    def _ready_failures(cls, paper: Mapping[str, object], maintenance: Mapping[str, object]) -> list[str]:
        failures = cls._base_start_failures(paper)
        checks = (
            (paper.get("state") == "READY_DISARMED", "RUNTIME_NOT_READY_DISARMED"),
            (paper.get("broker_snapshot_position") == "FLAT", "BROKER_POSITION_NOT_FLAT"),
            (paper.get("broker_snapshot_position_quantity") == 0, "BROKER_POSITION_QUANTITY_NOT_ZERO"),
            (paper.get("position_snapshot_complete") is True, "POSITION_SNAPSHOT_INCOMPLETE"),
            (paper.get("order_snapshot_complete") is True, "ORDER_SNAPSHOT_INCOMPLETE"),
            (paper.get("reconciliation_current") is True, "RECONCILIATION_NOT_CURRENT"),
            (maintenance.get("stage") == "READY", "NINJATRADER_NOT_READY"),
        )
        failures.extend(code for passed, code in checks if not passed)
        return list(dict.fromkeys(failures))

    @staticmethod
    def _reason_codes(readiness: Mapping[str, object]) -> list[str]:
        values = readiness.get("blocking_reasons")
        if not isinstance(values, (list, tuple)):
            return []
        return list(dict.fromkeys(
            str(value) for value in values
            if isinstance(value, str) and value
        ))

    @classmethod
    def _readiness_disposition(
        cls, readiness: Mapping[str, object], paper: Mapping[str, object],
    ) -> tuple[str, list[str], list[str], dict[str, bool]]:
        """Classify only a proven current-session evidence rewarm as waitable."""

        reasons = cls._reason_codes(readiness)
        if readiness.get("result") == "READY" and not reasons:
            ready_progress: dict[str, bool] = {}
            warmup = readiness.get("commissioning_warmup")
            strategy_evidence = readiness.get("strategy_evidence")
            raw_families = warmup.get("required_families") if isinstance(warmup, Mapping) else None
            if isinstance(raw_families, Mapping) and set(raw_families) == _SCALPER_WARMUP_FAMILIES:
                for family in sorted(_SCALPER_WARMUP_FAMILIES):
                    detail = raw_families.get(family)
                    if isinstance(detail, Mapping) and type(detail.get("seen")) is bool:
                        ready_progress[family] = detail.get("seen") is True
            warmup_proven = (
                isinstance(warmup, Mapping) and warmup.get("status") == "WARMED"
            )
            strategy_evidence_proven = (
                isinstance(strategy_evidence, Mapping)
                and strategy_evidence.get("status") == "ACTIVE"
            )
            if not bool(
                readiness.get("schema") == "lane-iii-phase-g-operational-paper-readiness-v1"
                and warmup_proven
                and strategy_evidence_proven
                and ready_progress
                and all(ready_progress.values())
            ):
                return (
                    "HARD_BLOCK", ["SCALPER_READY_EVIDENCE_PROOF_INVALID"],
                    [family for family, seen in ready_progress.items() if not seen],
                    ready_progress,
                )
            return (
                "READY", [],
                [family for family, seen in ready_progress.items() if not seen],
                ready_progress,
            )
        transient = {
            "COMMISSIONING_SESSION_NOT_WARMED", "PAPER_EVIDENCE_NOT_WARMED",
            "PAPER_CONTINUITY_UNUSABLE",
        }
        if not reasons or not set(reasons).issubset(transient):
            return "HARD_BLOCK", reasons or ["OPERATIONAL_READINESS_UNAVAILABLE"], [], {}

        warmup = readiness.get("commissioning_warmup")
        session = readiness.get("session")
        observer = readiness.get("observer")
        freshness = readiness.get("market_freshness")
        continuity = readiness.get("continuity")
        if not all(isinstance(value, Mapping) for value in (
            warmup, session, observer, freshness, continuity,
        )):
            return "HARD_BLOCK", reasons + ["OPERATIONAL_READINESS_DETAIL_UNAVAILABLE"], [], {}
        assert isinstance(warmup, Mapping)
        assert isinstance(session, Mapping)
        assert isinstance(observer, Mapping)
        assert isinstance(freshness, Mapping)
        assert isinstance(continuity, Mapping)

        raw_families = warmup.get("required_families")
        if not isinstance(raw_families, Mapping) or set(raw_families) != _SCALPER_WARMUP_FAMILIES:
            return "HARD_BLOCK", reasons + ["SCALPER_WARMUP_COVERAGE_UNAVAILABLE"], [], {}
        family_progress: dict[str, bool] = {}
        for family in sorted(_SCALPER_WARMUP_FAMILIES):
            detail = raw_families.get(family)
            if not isinstance(detail, Mapping) or type(detail.get("seen")) is not bool:
                return "HARD_BLOCK", reasons + ["SCALPER_WARMUP_COVERAGE_UNAVAILABLE"], [], {}
            family_progress[family] = detail.get("seen") is True
        missing = [family for family, seen in family_progress.items() if not seen]

        freshness_proven = all(
            isinstance(freshness.get(name), Mapping)
            and freshness[name].get("fresh") is True  # type: ignore[index]
            for name in ("quote", "classified_trade", "depth_mutation")
        )
        common_rewarm_proven = bool(
            readiness.get("schema") == "lane-iii-phase-g-operational-paper-readiness-v1"
            and warmup.get("status") == "NOT_WARMED"
            and missing
            and session.get("current") is True
            and session.get("session_kind") not in {None, "OFF_SESSION"}
            and observer.get("status") == "ACTIVE"
            and observer.get("local_bridge_healthy") is True
            and observer.get("market_price_connected") is True
            and freshness_proven
            and paper.get("state") == "READY_DISARMED"
            and paper.get("paper_execution") == "DISARMED"
            and paper.get("live_capital") == "DENIED"
        )
        if not common_rewarm_proven:
            return "HARD_BLOCK", reasons + ["SCALPER_EVIDENCE_REWARM_UNPROVEN"], missing, family_progress

        if "COMMISSIONING_SESSION_NOT_WARMED" not in reasons:
            return "HARD_BLOCK", reasons + ["SCALPER_EVIDENCE_REWARM_UNPROVEN"], missing, family_progress
        if "PAPER_CONTINUITY_UNUSABLE" in reasons:
            recovery_source = bool(
                continuity.get("recovery_condition") == "FRESH_POLICY_EVIDENCE_REWARM_REQUIRED"
                and (
                    continuity.get("local_sequence_gap") is True
                    or continuity.get("depth_reset_recovery") is True
                )
            )
            if observer.get("continuity_healthy") is not False or not recovery_source:
                return "HARD_BLOCK", reasons + ["SCALPER_CONTINUITY_RECOVERY_UNPROVEN"], missing, family_progress
        elif observer.get("continuity_healthy") is not True:
            return "HARD_BLOCK", reasons + ["SCALPER_CONTINUITY_STATE_INCONSISTENT"], missing, family_progress
        return "WAIT", reasons, missing, family_progress

    def _capture_readiness(
        self, readiness: Mapping[str, object], *, missing: list[str],
        family_progress: Mapping[str, bool],
    ) -> None:
        now = self._clock()
        with self._lock:
            self._readiness = dict(readiness)
            started = self._warmup_started_monotonic
            elapsed = 0.0 if started is None else max(0.0, now - started)
            self._warmup_progress = {
                "coverage_horizon_seconds": _SCALPER_COVERAGE_HORIZON_SECONDS,
                "startup_slack_seconds": _SCALPER_STARTUP_SLACK_SECONDS,
                "timeout_seconds": self._readiness_timeout_seconds,
                "elapsed_seconds": round(elapsed, 3),
                "required_families": sorted(_SCALPER_WARMUP_FAMILIES),
                "family_progress": dict(family_progress),
                "covered_family_count": sum(1 for seen in family_progress.values() if seen),
                "missing_families": list(missing),
                "readiness_blockers": self._reason_codes(readiness),
            }
            if self._stage is PaperAutoStartStage.WAITING_FOR_EVIDENCE:
                self._blockers = self._detailed_readiness_blockers(readiness, missing)

    @classmethod
    def _detailed_readiness_blockers(
        cls, readiness: Mapping[str, object], missing: list[str],
    ) -> list[str]:
        blockers = cls._reason_codes(readiness)
        blockers.extend(f"SCALPER_EVIDENCE_FAMILY_MISSING_{family}" for family in missing)
        continuity = readiness.get("continuity")
        if isinstance(continuity, Mapping):
            if continuity.get("local_sequence_gap") is True:
                blockers.append("LOCAL_SEQUENCE_GAP_REWARM_REQUIRED")
            if continuity.get("depth_reset_recovery") is True:
                blockers.append("DEPTH_RESET_RECOVERY_REWARM_REQUIRED")
        return list(dict.fromkeys(blockers))

    def _button(self, paper: Mapping[str, object]) -> dict[str, object]:
        if self._perpetual_position_proven(paper):
            return {"label": "Paper Trading Running", "enabled": False, "tone": "ready"}
        if self._operational_active(paper) and self._requires_position_proof(paper):
            blocker = self._position_blocker(paper)
            requirement = paper.get("position_requirement")
            requirement_position = (
                requirement.get("actual_position")
                if isinstance(requirement, Mapping) else None
            )
            flat = paper.get("current_position") == "FLAT" or requirement_position == "FLAT"
            return {
                "label": ("FLAT" if flat else "POSITION UNPROVEN") + " — BLOCKED: " + blocker,
                "enabled": False,
                "tone": "blocked",
            }
        if self._stage in _ACTIVE:
            labels = {
                PaperAutoStartStage.ENSURING_NINJATRADER: "Starting NinjaTrader…",
                PaperAutoStartStage.VERIFYING_FULL_LEDGER: "Verifying ledger…",
                PaperAutoStartStage.WAITING_FOR_EVIDENCE: "Warming Scalper evidence…",
                PaperAutoStartStage.STARTING_PAPER: "Starting paper trading…",
            }
            return {"label": labels[self._stage], "enabled": False, "tone": "progress"}
        failures = self._base_start_failures(paper)
        return {"label": "Start Paper Trading", "enabled": not failures, "tone": "primary" if not failures else "blocked"}

    def status(self) -> dict[str, object]:
        try:
            paper = self._paper_status()
            paper = paper if isinstance(paper, Mapping) else {}
        except Exception:
            paper = {}
        with self._lock:
            if self._perpetual_position_proven(paper) and self._stage not in _ACTIVE:
                stage = PaperAutoStartStage.RUNNING
            elif self._stage == PaperAutoStartStage.RUNNING:
                stage = PaperAutoStartStage.IDLE
            else:
                stage = self._stage
            return {
                "schema": PAPER_AUTOSTART_SCHEMA,
                "action_token": self._action_token,
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": stage.value,
                "stage_started_at": self._stage_started_at,
                "started_at": self._started_at,
                "in_progress": stage in _ACTIVE,
                "button": self._button(paper),
                "blockers": list(self._blockers),
                "maintenance": dict(self._maintenance),
                "ledger": dict(self._ledger),
                "readiness": dict(self._readiness),
                "warmup": dict(self._warmup_progress),
                "result": dict(self._result),
                "diagnostics": list(self._diagnostics[-20:]),
                "audit": {"schema": PAPER_AUTOSTART_SCHEMA, "durable": self._audit_healthy},
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
            }

    def _audit(self, event: str) -> None:
        with self._lock:
            record = {
                "schema": PAPER_AUTOSTART_SCHEMA,
                "event": event,
                "recorded_at": _utc_now(),
                "operation_id": self._operation_id,
                "request_id": self._request_id,
                "stage": self._stage.value,
                "blockers": list(self._blockers),
                "maintenance_stage": self._maintenance.get("stage"),
                "ledger_verification_id": self._ledger.get("verification_id"),
                "ledger_verified_through_sequence": self._ledger.get("verified_through_sequence"),
                "readiness_blockers": self._reason_codes(self._readiness),
                "warmup_missing_families": list(self._warmup_progress.get("missing_families", [])),
                "warmup_elapsed_seconds": self._warmup_progress.get("elapsed_seconds"),
                "operational_started": self._result.get("started") is True,
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
            }
            path = self._audit_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            encoded = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                os.write(descriptor, encoded.encode("utf-8"))
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            with self._lock:
                self._audit_healthy = False
            raise RuntimeError("PAPER_AUTOSTART_AUDIT_UNAVAILABLE") from error

    def _transition(self, stage: PaperAutoStartStage, blockers: list[str] | None = None) -> None:
        with self._lock:
            self._stage = stage
            self._stage_started_at = _utc_now()
            self._blockers = list(blockers or [])
        self._audit("STAGE_TRANSITION")

    def start(self, request_id: str) -> dict[str, object]:
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("Invalid paper auto-start request ID.")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            if self._request_id == request_id and self._operation_id is not None:
                return self.status()
            self._stop = threading.Event()
            self._wait = self._custom_wait or self._stop.wait
            self._operation_id = f"paper-auto-{uuid4().hex}"
            self._request_id = request_id
            self._started_at = _utc_now()
            self._stage = PaperAutoStartStage.ENSURING_NINJATRADER
            self._stage_started_at = self._started_at
            self._blockers = []
            self._maintenance = {}
            self._ledger = {}
            self._readiness = {}
            self._warmup_started_monotonic = None
            self._warmup_progress = {}
            self._result = {}
            self._diagnostics = []
            self._audit_healthy = True
            try:
                self._audit("OPERATION_STARTED")
            except RuntimeError:
                self._stage = PaperAutoStartStage.BLOCKED
                self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
                return self.status()
            try:
                paper = self._paper_status()
                failures = self._base_start_failures(paper if isinstance(paper, Mapping) else {})
            except Exception:
                failures = ["PAPER_RUNTIME_STATUS_UNAVAILABLE"]
            if failures:
                self._stage = PaperAutoStartStage.BLOCKED
                self._stage_started_at = _utc_now()
                self._blockers = failures
                try:
                    self._audit("STAGE_TRANSITION")
                except RuntimeError:
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
                return self.status()
            self._thread = threading.Thread(target=self._run, name="PaperAutoStart", daemon=True)
            self._thread.start()
        return self.status()

    def _wait_for(self, predicate: Callable[[], bool], timeout: float) -> bool:
        deadline = self._clock() + timeout
        while not self._stop.is_set() and self._clock() < deadline:
            if predicate():
                return True
            self._wait(min(self._poll_seconds, max(0.001, deadline - self._clock())))
        return False

    @staticmethod
    def _paper_ledger_tip(paper: Mapping[str, object]) -> int | None:
        ledger = paper.get("ledger")
        if not isinstance(ledger, Mapping):
            return None
        operational = ledger.get("operational_ledger")
        if isinstance(operational, Mapping):
            tip = operational.get("tail_tip_sequence")
            if type(tip) is int:
                return tip
        tip = ledger.get("highest_sequence")
        return tip if type(tip) is int else None

    @contextmanager
    def _fixed_startup_ledger_boundary(self) -> Iterator[None]:
        """Hold the established observation pause through proof and decision."""
        result = self._begin_startup_observation_pause()
        paused = isinstance(result, Mapping) and result.get("paused") is True
        drained = isinstance(result, Mapping) and result.get("drained") is True
        if not paused or not drained:
            if paused:
                try:
                    self._end_startup_observation_pause()
                except Exception:
                    pass
            raise RuntimeError("SCALPER_STARTUP_LEDGER_BOUNDARY_UNAVAILABLE")
        try:
            yield
        finally:
            try:
                resumed = self._end_startup_observation_pause()
                if not isinstance(resumed, Mapping) or resumed.get("paused") is not False:
                    raise RuntimeError("STARTUP_LEDGER_OBSERVATION_RESUME_FAILED")
            except Exception as error:
                # A successful start must never remain operational behind a
                # stuck market-observation pause. The normal runtime stop path
                # preserves all evidence and proves flatness when possible.
                if self._result.get("started") is True:
                    try:
                        self._stop_operational_paper()
                    except Exception:
                        raise RuntimeError(
                            "STARTUP_LEDGER_RESUME_FAILED_ABORT_UNPROVEN"
                        ) from error
                    paper_value = self._paper_status()
                    paper = paper_value if isinstance(paper_value, Mapping) else {}
                    if not (
                        not self._operational_active(paper)
                        and paper.get("current_position") == "FLAT"
                        and paper.get("current_quantity") == 0
                        and paper.get("working_owned_orders") == 0
                    ):
                        raise RuntimeError(
                            "STARTUP_LEDGER_RESUME_FAILED_ABORT_UNPROVEN"
                        ) from error
                raise RuntimeError("STARTUP_LEDGER_OBSERVATION_RESUME_FAILED") from error

    def _verify_full_ledger(
        self, *, require_current_stable_tip: bool,
    ) -> tuple[dict[str, object], list[str]]:
        started = self._start_full_verification()
        verification_id = started.get("verification_id") if isinstance(started, Mapping) else None

        def ledger_done() -> bool:
            value = self._ledger_status()
            with self._lock:
                self._ledger = dict(value) if isinstance(value, Mapping) else {}
            if verification_id and self._ledger.get("verification_id") != verification_id:
                return False
            return self._ledger.get("status") != "IN_PROGRESS"

        if not self._wait_for(ledger_done, self._ledger_timeout_seconds):
            return {}, ["FULL_LEDGER_VERIFICATION_TIMEOUT"]
        # The verifier controller retains an already-running scan. If it was
        # incremental, let it finish and then start the required Full scan.
        # Never cancel or relabel the pre-existing verification.
        if self._ledger.get("verification_mode") != "full":
            started = self._start_full_verification()
            verification_id = started.get("verification_id") if isinstance(started, Mapping) else None
            if not self._wait_for(ledger_done, self._ledger_timeout_seconds):
                return {}, ["FULL_LEDGER_VERIFICATION_TIMEOUT"]

        paper_value = self._paper_status()
        paper = dict(paper_value) if isinstance(paper_value, Mapping) else {}
        failures: list[str] = []
        if self._ledger.get("status") != "PASS" or self._ledger.get("verification_mode") != "full":
            failures.append("FULL_LEDGER_VERIFICATION_NOT_PASS")
        if self._ledger.get("chain_valid") is not True or self._ledger.get("checkpoint_valid") is not True:
            failures.append("LEDGER_CHAIN_OR_CHECKPOINT_INVALID")
        captured_tip = self._ledger.get("captured_tip_sequence")
        if (
            type(captured_tip) is not int
            or self._ledger.get("verified_through_sequence") != captured_tip
        ):
            failures.append("FULL_LEDGER_CAPTURED_TIP_NOT_VERIFIED")
        failures.extend(self._ready_failures(paper, self._maintenance))

        if require_current_stable_tip and not failures:
            first_tip = self._paper_ledger_tip(paper)
            if type(first_tip) is not int or first_tip != captured_tip:
                failures.append("FULL_LEDGER_VERIFICATION_NOT_CURRENT")
        return paper, list(dict.fromkeys(failures))

    def wait(self, timeout_seconds: float | None = None) -> PaperAutoStartStage:
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            return self._stage

    def _run(self) -> None:
        try:
            assert self._request_id is not None
            self._transition(PaperAutoStartStage.ENSURING_NINJATRADER)
            self._ensure_ninjatrader(self._request_id)

            def maintenance_done() -> bool:
                value = self._ninjatrader_status()
                with self._lock:
                    self._maintenance = dict(value) if isinstance(value, Mapping) else {}
                return self._maintenance.get("in_progress") is not True

            if not self._wait_for(maintenance_done, self._startup_timeout_seconds):
                if self._stop.is_set():
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                    return
                self._transition(PaperAutoStartStage.BLOCKED, ["NINJATRADER_STARTUP_TIMEOUT"])
                return
            if self._maintenance.get("stage") != "READY":
                blockers = self._maintenance.get("blockers")
                failures = [str(value) for value in blockers] if isinstance(blockers, list) else []
                self._transition(PaperAutoStartStage.BLOCKED, failures or ["NINJATRADER_STARTUP_BLOCKED"])
                return
            paper = self._paper_status()
            failures = self._ready_failures(paper, self._maintenance)
            if failures:
                self._transition(PaperAutoStartStage.BLOCKED, failures)
                return

            self._transition(PaperAutoStartStage.VERIFYING_FULL_LEDGER)
            paper, failures = self._verify_full_ledger(
                require_current_stable_tip=False,
            )
            if self._stop.is_set():
                self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                return
            if failures:
                self._transition(PaperAutoStartStage.BLOCKED, list(dict.fromkeys(failures)))
                return

            if self._requires_scalper_warmup(paper):
                self._transition(PaperAutoStartStage.WAITING_FOR_EVIDENCE)
                with self._lock:
                    self._warmup_started_monotonic = self._clock()
                readiness_deadline = self._clock() + self._readiness_timeout_seconds
                while not self._stop.is_set():
                    readiness_value = self._operational_readiness()
                    readiness = dict(readiness_value) if isinstance(readiness_value, Mapping) else {}
                    paper_value = self._paper_status()
                    paper = dict(paper_value) if isinstance(paper_value, Mapping) else {}
                    if not self._requires_scalper_warmup(paper):
                        self._transition(
                            PaperAutoStartStage.BLOCKED,
                            ["SCALPER_PROFILE_CHANGED_DURING_WARMUP"],
                        )
                        return
                    disposition, reasons, missing, family_progress = self._readiness_disposition(
                        readiness, paper,
                    )
                    self._capture_readiness(
                        readiness, missing=missing, family_progress=family_progress,
                    )
                    if disposition == "READY":
                        break
                    if disposition == "HARD_BLOCK":
                        self._transition(
                            PaperAutoStartStage.BLOCKED,
                            list(dict.fromkeys(
                                reasons + self._detailed_readiness_blockers(readiness, missing)
                            )),
                        )
                        return
                    now = self._clock()
                    if now >= readiness_deadline:
                        self._transition(
                            PaperAutoStartStage.BLOCKED,
                            list(dict.fromkeys(
                                reasons
                                + self._detailed_readiness_blockers(readiness, missing)
                                + ["SCALPER_EVIDENCE_WARMUP_TIMEOUT"]
                            )),
                        )
                        return
                    self._wait(min(self._poll_seconds, max(0.001, readiness_deadline - now)))
                if self._stop.is_set():
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                    return

                with self._fixed_startup_ledger_boundary():
                    # Warmup can append durable evidence/reset attestations.
                    # Freeze new paper observations, drain the writer, and run
                    # a new Full proof over that fixed current tip.
                    self._transition(PaperAutoStartStage.VERIFYING_FULL_LEDGER)
                    paper, failures = self._verify_full_ledger(
                        require_current_stable_tip=True,
                    )
                    if self._stop.is_set():
                        self._transition(
                            PaperAutoStartStage.CANCELLED,
                            ["CONTROL_CENTER_SHUTDOWN"],
                        )
                        return
                    if failures:
                        self._transition(PaperAutoStartStage.BLOCKED, failures)
                        return
                    if not self._requires_scalper_warmup(paper):
                        self._transition(
                            PaperAutoStartStage.BLOCKED,
                            ["SCALPER_PROFILE_CHANGED_DURING_WARMUP"],
                        )
                        return

                    # Re-evaluate immediately next to the authority mutation.
                    # The canonical start repeats these checks atomically; this
                    # pass prevents a visible regression from causing a call.
                    final_value = self._operational_readiness()
                    final_readiness = dict(final_value) if isinstance(final_value, Mapping) else {}
                    final_paper_value = self._paper_status()
                    final_paper = dict(final_paper_value) if isinstance(final_paper_value, Mapping) else {}
                    final_disposition, final_reasons, missing, family_progress = self._readiness_disposition(
                        final_readiness, final_paper,
                    )
                    self._capture_readiness(
                        final_readiness, missing=missing, family_progress=family_progress,
                    )
                    if final_disposition != "READY":
                        self._transition(
                            PaperAutoStartStage.BLOCKED,
                            list(dict.fromkeys(
                                final_reasons
                                + self._detailed_readiness_blockers(final_readiness, missing)
                                + ["OPERATIONAL_READINESS_REGRESSED"]
                            )),
                        )
                        return
                    if self._stop.is_set():
                        self._transition(
                            PaperAutoStartStage.CANCELLED,
                            ["CONTROL_CENTER_SHUTDOWN"],
                        )
                        return
                    self._transition(PaperAutoStartStage.STARTING_PAPER)
                    result = self._start_operational_paper(self._request_id)
                    with self._lock:
                        self._result = dict(result) if isinstance(result, Mapping) else {}
            else:
                if self._stop.is_set():
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                    return
                self._transition(PaperAutoStartStage.STARTING_PAPER)
                result = self._start_operational_paper(self._request_id)
                with self._lock:
                    self._result = dict(result) if isinstance(result, Mapping) else {}
            paper = self._paper_status()
            if self._result.get("started") is not True and not self._operational_active(paper):
                reasons = self._result.get("reason_codes")
                failures = (
                    [str(value) for value in reasons if isinstance(value, str) and value]
                    if isinstance(reasons, (list, tuple)) else []
                )
                result_readiness = self._result.get("readiness")
                if isinstance(result_readiness, Mapping):
                    failures.extend(self._reason_codes(result_readiness))
                failures.append("OPERATIONAL_PAPER_START_REFUSED")
                self._transition(
                    PaperAutoStartStage.BLOCKED,
                    list(dict.fromkeys(failures)),
                )
                return
            if self._requires_position_proof(paper):
                terminal: list[str] = []

                def positioned_or_terminal() -> bool:
                    nonlocal paper, terminal
                    value = self._paper_status()
                    paper = value if isinstance(value, Mapping) else {}
                    if self._perpetual_position_proven(paper):
                        return True
                    if (
                        not self._operational_active(paper)
                        or paper.get("state") in {
                            "LOCKED_OUT", "FAULTED", "STOPPING", "STOPPED",
                        }
                    ):
                        terminal = [self._position_blocker(paper)]
                        return True
                    return False

                reached = self._wait_for(
                    positioned_or_terminal, self._startup_timeout_seconds,
                )
                if self._stop.is_set():
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                    return
                if not reached or terminal or not self._perpetual_position_proven(paper):
                    self._transition(
                        PaperAutoStartStage.BLOCKED,
                        terminal or [self._position_blocker(paper)],
                    )
                    return
            self._transition(PaperAutoStartStage.RUNNING)
        except RuntimeError as error:
            blocker = str(error) if str(error).isupper() else "PAPER_AUTOSTART_INTERNAL_FAILURE"
            try:
                self._transition(PaperAutoStartStage.BLOCKED, [blocker])
            except RuntimeError:
                with self._lock:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
        except Exception as error:
            with self._lock:
                self._diagnostics.append({"at": _utc_now(), "message": type(error).__name__})
            try:
                self._transition(PaperAutoStartStage.FAILED, ["PAPER_AUTOSTART_INTERNAL_FAILURE"])
            except RuntimeError:
                with self._lock:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            if self._stage in _ACTIVE:
                try:
                    self._transition(PaperAutoStartStage.CANCELLED, ["CONTROL_CENTER_SHUTDOWN"])
                except RuntimeError:
                    self._stage = PaperAutoStartStage.BLOCKED
                    self._blockers = ["PAPER_AUTOSTART_AUDIT_UNAVAILABLE"]
