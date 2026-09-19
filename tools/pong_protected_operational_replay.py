"""Test-only protected-path replay through the hosted Pong production seam.

This is deliberately distinct from :mod:`pong_periodic` and its detached
``OfflinePeriodicPong`` adapter.  It starts an isolated ``PongProductionHost``
and the existing append-only ``PongAuthenticHourCollector``.  Its only native
dispatcher is an in-memory test adapter; no listener, NinjaTrader process,
paper runtime, or external transport is opened.

The fixture has two complementary scopes:

* a natural-paced authenticated SEP26 corpus exercises source reception,
  producer validation, the authoritative owner, QPC deadline wakeups, the
  existing collector's journal/fsync acknowledgement, and controller state
  persistence; and
* a small explicitly synthetic state fixture exercises entry-pending,
  cancellation, fill/protection, held-position exit, and invalid-evidence
  branches that the historical receipt cannot legitimately claim to seed.

Neither scope establishes fresh-market readiness, native/broker execution, or
paper authority.  Every receipt emitted by this tool says so explicitly.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, deque
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness
from src.l3f_provider.ninjatrader_observation import (
    NinjaTraderObservation,
    expected_observer_source_fingerprint,
)
from src.l3g_paper.daily_bias import (
    BiasInputProvenance,
    BiasUpdateReason,
    CoreComponentEvidence,
    CoreComponentKind,
    DailyBiasDirection,
    DailyBiasEngine,
    DailyBiasResult,
    HourlyBiasFrame,
    OptionsDayClass,
    OptionsOverlayEvidence,
    OptionsOverlayOutcome,
)
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.discernment import (
    PONG_RESEARCH_EVIDENCE_MAPPING,
    PONG_RESEARCH_POLICY,
    DiscernmentDirection,
    DiscernmentEngine,
    DiscernmentEvidence,
    DiscernmentPolicy,
    DiscernmentSnapshot,
    DiscernmentSwitch,
    DiscernmentUpdate,
    VolumeRegime,
)
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
from src.l3g_paper.pong import PongDirection, PongState, PongStateMachine
from src.l3g_paper.pong_authorization import PongLaunchAuthorization
from src.l3g_paper.pong_collection import PongAuthenticHourCollector
from src.l3g_paper.pong_controller import PongControllerPhase
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_quantity import PongQuantityPolicy
from src.l3g_paper.pong_runtime import PongLoadedRuntimeIdentity, PongRuntimeBridge
from src.l3g_paper.pong_scheduler import PongSchedulingConfig, PongSchedulingMode
from src.l3g_paper.pong_volume_history import PongVolumeReferenceHistory
from src.lane_iii.contracts import canonical_hash
from tools.pong_periodic_support import (
    BenchmarkLease,
    RELEASE_ABORTED,
    RELEASE_CAPACITY_EXHAUSTED,
    RELEASE_COMPLETE,
    RELEASE_FAILED,
    independent_release_sender,
    process_identity,
    release_progress_guard,
    system_cpu_times,
)


MARKET_TYPES = frozenset(("DEPTH", "QUOTE", "TRADE"))
TEST_ONLY_SCHEMA = "pong-protected-operational-fixture-v1"
TEST_ONLY_DISPATCHER = "ISOLATED_IN_MEMORY_NATIVE_ADAPTER_V1"


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def wire(at: datetime) -> str:
    return at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def native_timestamp_ns(value: str) -> int:
    """Keep the native receipt's 100 ns fraction for paced scheduling."""
    at = utc(value)
    match = re.search(r"\.(\d+)(?:Z|[+-]\d\d:\d\d)$", value)
    nanos = int((match.group(1) + "000000000")[:9]) if match else 0
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = int((at.replace(microsecond=0) - epoch).total_seconds())
    return seconds * 1_000_000_000 + nanos


def distribution(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": None if not ordered else ordered[len(ordered) // 2],
        "p95": None if not ordered else ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        "p99": None if not ordered else ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))],
        "maximum": None if not ordered else ordered[-1],
    }


def write_json(path: Path, value: object) -> None:
    path.write_bytes(json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n")


def source_manifest() -> dict[str, str]:
    result: dict[str, str] = {}
    for module in tuple(sys.modules.values()):
        file = getattr(module, "__file__", None)
        if not file:
            continue
        path = Path(file)
        if path.suffix != ".py":
            continue
        try:
            relative = path.resolve().relative_to(ROOT)
        except ValueError:
            continue
        result[str(relative)] = file_digest(path)
    return dict(sorted(result.items()))


def verify_corpus(corpus: Path, key: bytes) -> tuple[dict[str, object], str, datetime]:
    manifest = json.loads((corpus / "manifest.json").read_bytes())
    frames = corpus / "frames.jsonl"
    if file_digest(frames) != manifest.get("frames_sha256"):
        raise ValueError("CORPUS_DIGEST_CHANGED")
    sources = manifest.get("source_fingerprints")
    if not isinstance(sources, dict) or len(sources) != 1:
        raise ValueError("CORPUS_SOURCE_AMBIGUOUS")
    source = next(iter(sources))
    receipt = manifest.get("history_receipt")
    if not isinstance(receipt, dict) or not isinstance(receipt.get("path"), str):
        raise ValueError("HISTORY_RECEIPT_MISSING")
    history_path = corpus / receipt["path"]
    if file_digest(history_path) != receipt.get("sha256"):
        raise ValueError("HISTORY_RECEIPT_BYTES_CHANGED")
    history = PongVolumeReferenceHistory(history_path, root_key=key, expected_source=source)
    if history.error or history.receipt_hash != receipt.get("receipt_hash"):
        raise ValueError("HISTORY_RECEIPT_AUTHENTICATION")
    dependencies_path = corpus / "source-dependencies.jsonl"
    dependencies = dependencies_path.read_bytes()
    if file_digest(dependencies_path) != manifest.get("source_dependencies_sha256"):
        raise ValueError("HISTORY_DEPENDENCY_BYTES_CHANGED")
    value = json.loads(history_path.read_bytes())
    if value.get("records") != [json.loads(line)["observation"] for line in dependencies.splitlines()]:
        raise ValueError("HISTORY_RECEIPT_DEPENDENCY_MISMATCH")
    if value.get("received_at") != manifest.get("history_admission_at"):
        raise ValueError("HISTORY_RECEIPT_AVAILABILITY_MISMATCH")
    return manifest, source, utc(str(value["received_at"]))


def strong_policy() -> DiscernmentPolicy:
    """Explicit synthetic-only policy state; never a market calibration claim."""
    return replace(
        PONG_RESEARCH_POLICY,
        reliability=1.0,
        model_version="pong-protected-operational-fixture-strong-v1",
        dependency_group_bounds=(
            ("FLOW_AND_INTERACTION_SHARED", 4.0),
            ("WEAK_STRUCTURE", 1.0),
        ),
        evidence_mapping_hash=canonical_hash({
            "fixture": "pong-protected-operational-fixture-strong-v1",
            "dependency_group_bounds": [
                ["FLOW_AND_INTERACTION_SHARED", 4.0],
                ["WEAK_STRUCTURE", 1.0],
            ],
        }),
    )


def fixture_provenance(name: str, at: datetime) -> BiasInputProvenance:
    start = at - timedelta(hours=1)
    return BiasInputProvenance(
        source="ISOLATED_TEST_FIXTURE",
        calculation_id="fixture-" + name,
        calculation_version="1",
        calculation_hash=canonical_hash({"fixture_component": name, "version": 1}),
        window_start=wire(start),
        window_end=wire(at),
        latest_event_at=wire(at - timedelta(seconds=1)),
        observed_at=wire(at),
        available_at=wire(at),
        valid_until=wire(at + timedelta(hours=2)),
        input_ids=("fixture-" + name + "-input",),
        input_hashes=(canonical_hash({"fixture_input": name}),),
    )


def fixture_bias(direction: DailyBiasDirection, at: datetime) -> DailyBiasResult:
    end = at.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    components = tuple(
        CoreComponentEvidence(
            component,
            direction,
            True,
            True,
            False,
            ("ISOLATED_TEST_FIXTURE",),
            fixture_provenance(component.value.lower(), end),
        )
        for component in CoreComponentKind
    )
    options = OptionsOverlayEvidence(
        OptionsDayClass.ORDINARY,
        OptionsOverlayOutcome.NOT_APPLICABLE,
        None,
        False,
        ("ISOLATED_TEST_FIXTURE_ORDINARY_DAY",),
        fixture_provenance("options", end),
    )
    return DailyBiasEngine().update(
        HourlyBiasFrame(
            "MNQ SEP26",
            wire(end - timedelta(hours=1)),
            wire(end),
            wire(end),
            components,
            options,
            True,
            None,
        ),
        BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
    ).result


def fixture_snapshot(
    policy: DiscernmentPolicy,
    *,
    score: float | None,
    at: datetime,
    valid: bool = True,
    ordinal: int,
    domain: str = "ISOLATED_TEST_FIXTURE_CONTINUITY",
) -> DiscernmentSnapshot:
    """Create a verified synthetic snapshot using the real frozen scorer."""
    engine = DiscernmentEngine(policy)
    if not valid:
        update = engine.on_expiry(wire(at))
        return update.snapshot
    if score is None:
        raise ValueError("VALID_FIXTURE_SCORE_REQUIRED")
    raw_target = 0.5 + (score - 0.5) / policy.reliability
    if not 0 < raw_target < 1:
        raise ValueError("FIXTURE_SCORE_UNATTAINABLE")
    remaining = math.log(raw_target / (1 - raw_target)) - math.log(
        policy.prior_long_score / (1 - policy.prior_long_score),
    )
    group_effects: dict[str, float] = {}
    for group, bound in policy.dependency_group_bounds:
        contribution = max(-bound, min(bound, remaining))
        group_effects[group] = contribution
        remaining -= contribution
    if not math.isclose(remaining, 0.0, rel_tol=0, abs_tol=1e-12):
        raise ValueError("FIXTURE_SCORE_EXCEEDS_POLICY_BOUND")
    groups = {
        (item.component_id, item.component_version): item.dependency_group
        for item in PONG_RESEARCH_EVIDENCE_MAPPING.effects
    }
    required = sorted(policy.required_components)
    specifications: list[list[object]] = [
        [component_id, version, groups.get((component_id, version), policy.dependency_group_bounds[0][0]), 0.0]
        for component_id, version in required
    ]
    for group, effect in group_effects.items():
        match = next((item for item in specifications if item[2] == group), None)
        if match is None:
            specifications.append(["FIXTURE_" + group, "1", group, effect])
        else:
            match[3] = effect
    update: DiscernmentUpdate | None = None
    timestamp = wire(at)
    for index, (component, version, group, effect) in enumerate(sorted(specifications), start=1):
        source_id = f"fixture-{ordinal}-{component}-{version}"
        update = engine.on_evidence(
            DiscernmentEvidence(
                event_id=f"fixture-event-{ordinal}-{component}-{version}",
                component_id=str(component),
                component_version=str(version),
                dependency_group=str(group),
                log_odds_effect=float(effect),
                required=(str(component), str(version)) in policy.required_components,
                source_event_at=timestamp,
                received_at=timestamp,
                available_at=timestamp,
                valid_until=wire(at + timedelta(seconds=30)),
                evidence_revision=ordinal * 100 + index,
                continuity_domain=domain,
                continuity_valid=True,
                source_ids=(source_id,),
                source_hashes=(canonical_hash({"fixture_source": source_id}),),
            ),
            volume_regime=VolumeRegime.NORMAL,
        )
    if update is None or not update.snapshot.valid:
        raise AssertionError("FIXTURE_SNAPSHOT_NOT_VALID")
    if update.snapshot.verification_error(policy, at=timestamp) is not None:
        raise AssertionError("FIXTURE_SNAPSHOT_NOT_VERIFIED")
    if not math.isclose(update.snapshot.effective_long_score or 0.0, score, rel_tol=0, abs_tol=1e-12):
        raise AssertionError("FIXTURE_SNAPSHOT_SCORE_MISMATCH")
    return update.snapshot


def fixture_switch(
    policy: DiscernmentPolicy,
    *,
    previous: PongDirection,
    direction: PongDirection,
    at: datetime,
    ordinal: int,
    prior: str | None,
) -> DiscernmentSwitch:
    # ``ordinal`` distinguishes the synthetic evidence revisions from the
    # switch cursor.  A seeded stream has no switch yet, so its first actual
    # transition must be sequence 1 (not evidence ordinal 2).
    sequence = ordinal - 1
    if sequence <= 0:
        raise ValueError("FIXTURE_SWITCH_SEQUENCE_REQUIRED")
    score = 0.9 if direction is PongDirection.LONG else 0.1
    snapshot = fixture_snapshot(policy, score=score, at=at, ordinal=ordinal)
    return DiscernmentSwitch.create(
        policy=policy,
        sequence=sequence,
        prior_switch_id=prior,
        previous_direction=DiscernmentDirection(previous.value),
        direction=DiscernmentDirection(direction.value),
        snapshot=snapshot,
    )


@dataclass
class _RuntimeState:
    source_fingerprint: str
    build_fingerprint: str
    execution_session_id: str
    position_quantity: int = 0
    working_owned_orders: int = 0
    working_entry_orders: int = 0
    unresolved_execution: bool = False
    entry_fence_active: bool = False
    protection_current: bool = True
    entries_paused: bool = False
    runtime_active: bool = True
    safety_lockout: bool = False
    evidence_sealed: bool = False

    def snapshot(self) -> dict[str, object]:
        return {
            "observed_at": wire(datetime.now(timezone.utc)),
            "entry_profile_version": PONG_ENTRY_PROFILE_VERSION,
            "runtime_state": "TEST_ONLY_ISOLATED_FIXTURE",
            "runtime_active": self.runtime_active,
            "entry_fence_active": self.entry_fence_active,
            "evidence_sealed": self.evidence_sealed,
            "entries_paused": self.entries_paused,
            "position_quantity": self.position_quantity,
            "working_owned_orders": self.working_owned_orders,
            "working_entry_orders": self.working_entry_orders,
            "unresolved_execution": self.unresolved_execution,
            "reconciliation_current": True,
            "protection_current": self.protection_current,
            "safety_lockout": self.safety_lockout,
            "lockout_reason": None,
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "maximum_quantity": 1,
            "available_loss_allowance_dollars": "200",
            "live_authority": "DISARMED",
            "execution_session_id": self.execution_session_id,
            "addon_protocol_version": "l3g-paper-addon-provenance-v1",
            "addon_source_fingerprint": self.source_fingerprint,
            "addon_build_fingerprint": self.build_fingerprint,
            "addon_provenance_valid": True,
        }


class InstrumentedHost(PongProductionHost):
    """Production host with measurement-only timing hooks for the isolated run."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.measure_lock = threading.Lock()
        self.enqueued_at: dict[str, float] = {}
        self.pending_market: deque[str] = deque()
        self.admission_latency_seconds: list[float] = []
        self.result_latency_seconds: list[float] = []
        self.evaluation_wall_seconds: list[float] = []
        self.evaluation_cpu_seconds: list[float] = []
        self.observation_queue_peak = 0
        self.observation_queue_ages_seconds: list[float] = []
        self.evaluation_count = 0

    def on_observation(self, record: NinjaTraderObservation) -> None:
        submitted = time.perf_counter()
        with self.measure_lock:
            self.enqueued_at[record.observation_id] = submitted
        super().on_observation(record)
        with self.measure_lock:
            self.observation_queue_peak = max(self.observation_queue_peak, self._observations.qsize())

    def _consume_observation(self, record: NinjaTraderObservation) -> None:
        started = time.perf_counter()
        super()._consume_observation(record)
        completed = time.perf_counter()
        with self.measure_lock:
            submitted = self.enqueued_at.pop(record.observation_id, None)
            if submitted is not None:
                age = completed - submitted
                self.admission_latency_seconds.append(age)
                self.observation_queue_ages_seconds.append(age)
            if record.observation_type in MARKET_TYPES:
                self.pending_market.append(record.observation_id)

    def _evaluate_microstructure(self, at: datetime) -> None:
        started_wall = time.perf_counter()
        started_cpu = time.thread_time()
        super()._evaluate_microstructure(at)
        completed_wall = time.perf_counter()
        with self.measure_lock:
            self.evaluation_count += 1
            self.evaluation_wall_seconds.append(completed_wall - started_wall)
            self.evaluation_cpu_seconds.append(time.thread_time() - started_cpu)
            while self.pending_market:
                self.pending_market.popleft()
                self.result_latency_seconds.append(completed_wall - started_wall)


class ObservedQueue(queue.Queue):
    """Collector instrumentation only; preserves the collector's queue contract."""

    def __init__(self, maxsize: int) -> None:
        super().__init__(maxsize)
        self.peak = 0

    def _put(self, item: object) -> None:
        super()._put(item)
        self.peak = max(self.peak, self._qsize())


class ProtectedFixture:
    """Creates an explicitly isolated test-only host/bridge/controller stack."""

    def __init__(self, root: Path, *, source: str, build: str) -> None:
        now = datetime.now(timezone.utc)
        denver_day = now.astimezone(ZoneInfo("America/Denver")).date()
        self.root = root
        self.policy = strong_policy()
        self.runtime = _RuntimeState(source, build, "l3g-es-" + "7" * 32)
        self.native_dispatches: list[dict[str, object]] = []
        self.controller_dispatches: list[dict[str, object]] = []
        self.authorization = PongLaunchAuthorization(
            "pong-test-only-protected-fixture-" + denver_day.strftime("%Y%m%d"),
            denver_day,
            False,
            "f" * 64,
            "e" * 64,
        )

        def controller_dispatch(kind: str, reason: str) -> bool:
            self.controller_dispatches.append({"kind": kind, "reason": reason, "at": wire(datetime.now(timezone.utc))})
            return True

        def native_dispatch(directive: object, evidence: object) -> dict[str, object]:
            directive_id = str(getattr(directive, "directive_id", ""))
            command_id = "fixture-command-" + directive_id.rsplit("-", 1)[-1]
            self.native_dispatches.append({
                "dispatcher": TEST_ONLY_DISPATCHER,
                "directive_id": directive_id,
                "kind": str(getattr(directive, "kind", "")),
                "command_id": command_id,
                "evidence": dict(evidence) if isinstance(evidence, dict) else None,
            })
            return {"accepted": True, "command_id": command_id, "directive_id": directive_id}

        self.host = InstrumentedHost(
            root / "host",
            selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=source,
            runtime_snapshot=self.runtime.snapshot,
            collector_status=lambda: {
                "state": "TEST_ONLY_FIXTURE",
                "durability": {"enqueued_records": 0, "written_records": 0, "durable_records": 0},
            },
            controller_dispatch=controller_dispatch,
            native_dispatch=native_dispatch,
            activation_authorized=True,
            launch_authorization=self.authorization,
            scheduling=PongSchedulingConfig(
                mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                interval_ms=20,
            ),
        )
        self.host._discernment = DiscernmentEngine(self.policy)
        self.host._daily_bias = fixture_bias(DailyBiasDirection.LONG, now)
        self.host._microstructure_blockers = ()
        self.host._certificate = {
            "producer": {"source_fingerprint": source},
            "fixture_only": True,
            "certificate_hash": "c" * 64,
            "required_interval": {"fixture_only": True},
            "daily_bias_result_hash": self.host._daily_bias.result_hash,
        }
        self.host._retained_hour_seed = True
        self.host._producer = ("fixture-producer", source, build)
        identity = PongLoadedRuntimeIdentity(
            execution_session_id=self.runtime.execution_session_id,
            addon_protocol_version="l3g-paper-addon-provenance-v1",
            addon_source_fingerprint=source,
            addon_build_fingerprint=build,
            verified_at=wire(now),
            attestation_verified=True,
        )
        policy = PongQuantityPolicy(
            native_maximum_quantity=1,
            native_capability_status="TEST_ONLY_ISOLATED_ONE_MNQ_CAPABILITY",
            reconciliation_source_fingerprint=source,
            reconciliation_build_fingerprint=build,
        )
        machine = PongStateMachine(quantity_policy=policy, discernment_policy=self.policy)
        machine.on_bias_update(self.host._daily_bias)
        self.host._bridge = PongRuntimeBridge.create(
            machine,
            root / "host" / "bridges" / "test-only.json",
            runtime_identity=identity,
        )
        self.host._runtime = self.runtime.snapshot()

    def start(self) -> None:
        self.host.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = self.host.status()
            controller = status.get("controller")
            if isinstance(controller, dict) and controller.get("phase") in {
                PongControllerPhase.RUNNING_PRE_BLACKOUT.value,
                PongControllerPhase.RUNNING_POST_BLACKOUT.value,
            }:
                return
            time.sleep(0.01)
        raise RuntimeError("TEST_ONLY_CONTROLLER_DID_NOT_ACTIVATE")

    def stop(self) -> None:
        self.host.stop()

    def wait_dispatch(self, kind: str) -> dict[str, object]:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            for item in self.native_dispatches:
                if item["kind"] == kind:
                    return item
            time.sleep(0.01)
        raise RuntimeError("TEST_ONLY_DISPATCH_TIMEOUT:" + kind)

    def send(self, message: dict[str, object]) -> None:
        message.setdefault("timestamp", wire(datetime.now(timezone.utc)))
        self.host.on_execution_message(message)


def scenario_matrix(root: Path) -> dict[str, object]:
    """Exercise production host callbacks with synthetic-only state adapters."""
    source = expected_observer_source_fingerprint()
    build = "b" * 64
    events: list[dict[str, object]] = []

    def admitted_long(label: str) -> tuple[
        ProtectedFixture,
        datetime,
        DiscernmentSwitch,
        dict[str, object],
    ]:
        """Enter one isolated host through its actual admission callback."""
        fixture = ProtectedFixture(root / label, source=source, build=build)
        fixture.start()
        at = datetime.now(timezone.utc)
        seed = fixture_snapshot(fixture.policy, score=0.1, at=at, ordinal=1)
        fixture.host._discernment._latched_direction = DiscernmentDirection.SHORT
        fixture.host._apply_discernment_update(
            DiscernmentUpdate(seed, None, True, "TEST_ONLY_SEED"),
        )
        entry_switch = fixture_switch(
            fixture.policy,
            previous=PongDirection.SHORT,
            direction=PongDirection.LONG,
            at=at + timedelta(milliseconds=1),
            ordinal=2,
            prior=None,
        )
        fixture.host._apply_discernment_update(
            DiscernmentUpdate(
                entry_switch.snapshot, entry_switch, True, "TEST_ONLY_ENTRY_SWITCH",
            ),
        )
        return fixture, at, entry_switch, fixture.wait_dispatch("ENTER_LONG")

    # The timer has its own flat-only scheduling path, which must remain alive
    # while no event needs protection.
    quiet_fixture = ProtectedFixture(root / "quiet", source=source, build=build)
    try:
        quiet_fixture.start()
        status = quiet_fixture.host.status()
        events.append({
            "scenario": "flat_periodic_quiet_wakeup",
            "passed": status["evaluation_schedule"]["configuration"] == {
                "mode": "PERIODIC_FLAT_PROTECTED", "interval_ms": 20,
            },
            "active_path": status["evaluation_schedule"]["active_path"],
        })
        time.sleep(0.06)
        quiet = quiet_fixture.host.status()["evaluation_schedule"]
        events[-1]["periodic_evaluations_after_quiet_wait"] = quiet["periodic_flat_analytical_evaluations"]
        events[-1]["passed"] = bool(
            events[-1]["passed"]
            and quiet["deadline_metrics"]["scheduled_opportunities"] > 0,
        )
    finally:
        quiet_fixture.stop()

    # A cancelled entry which nevertheless fills cannot be normalised into a
    # position: the real bridge must send an emergency flatten directive.
    fixture, at, entry_switch, entry = admitted_long("late-fill")
    late_final_status: dict[str, object]
    try:
        protected = fixture.host.status()["evaluation_schedule"]
        events.append({
            "scenario": "entry_intent_before_submission",
            "passed": protected["active_path"] == "EVENT_DRIVEN_PROTECTION",
            "active_path": protected["active_path"],
            "protection_reason": protected["protection_reason"],
            "directive_id": entry["directive_id"],
        })

        fixture.runtime.working_owned_orders = 1
        fixture.runtime.working_entry_orders = 1
        fixture.runtime.unresolved_execution = True
        fixture.send({"message_type": "COMMAND_ACK", "order_role": "ENTRY", "command_id": entry["command_id"]})
        cancel_switch = fixture_switch(
            fixture.policy,
            previous=PongDirection.LONG,
            direction=PongDirection.SHORT,
            at=at + timedelta(milliseconds=2),
            ordinal=3,
            prior=entry_switch.event_id,
        )
        fixture.host._apply_discernment_update(
            DiscernmentUpdate(cancel_switch.snapshot, cancel_switch, True, "TEST_ONLY_CANCEL_PENDING"),
        )
        cancellation = fixture.wait_dispatch("CANCEL_PENDING_ENTRY")
        events.append({
            "scenario": "pending_entry_opposite_signal_cancellation",
            "passed": True,
            "directive_id": cancellation["directive_id"],
        })

        fixture.runtime.position_quantity = 1
        fixture.runtime.working_entry_orders = 0
        fixture.runtime.working_owned_orders = 1
        fixture.runtime.unresolved_execution = False
        fixture.send({
            "message_type": "EXECUTION_EVENT", "order_role": "ENTRY",
            "command_id": entry["command_id"], "quantity": 1,
            "execution_id": "fixture-late-entry-fill",
        })
        emergency = fixture.wait_dispatch("EMERGENCY_FLATTEN")
        fixture.send({"message_type": "COMMAND_ACK", "order_role": "EXIT", "command_id": emergency["command_id"]})
        fixture.runtime.position_quantity = 0
        fixture.runtime.working_owned_orders = 0
        fixture.send({
            "message_type": "EXECUTION_EVENT", "order_role": "EXIT",
            "command_id": emergency["command_id"], "quantity": 1,
            "execution_id": "fixture-emergency-flatten-fill",
        })
        fixture.send({
            "message_type": "RECONCILIATION", "position_quantity": 0,
            "working_order_count": 0,
        })
        events.append({
            "scenario": "late_fill_after_cancel_requires_emergency_flatten",
            "passed": (
                fixture.host._bridge is not None
                and fixture.host._bridge.machine.actual_quantity == 0
                and fixture.host._bridge.machine.state is PongState.SAFETY_BLOCKED
            ),
            "directive_id": emergency["directive_id"],
        })
        late_final_status = fixture.host.status()
    finally:
        fixture.stop()

    # A distinct clean fixture exercises the normal held-position path.  It
    # must obtain signed protection before an opposite signal can release an
    # ordinary exit after protective-stop cancellation.
    held, held_at, held_entry_switch, held_entry = admitted_long("held-exit")
    held_final_status: dict[str, object]
    try:
        held.runtime.position_quantity = 1
        held.runtime.working_owned_orders = 1
        held.send({"message_type": "COMMAND_ACK", "order_role": "ENTRY", "command_id": held_entry["command_id"]})
        held.send({
            "message_type": "EXECUTION_EVENT", "order_role": "ENTRY",
            "command_id": held_entry["command_id"], "quantity": 1,
            "execution_id": "fixture-held-entry-fill",
        })
        protection = held.wait_dispatch("VERIFY_NATIVE_PROTECTION")
        held.send({"message_type": "COMMAND_ACK", "order_role": "PROTECTIVE", "command_id": protection["command_id"]})
        held.send({
            "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE",
            "order_state": "WORKING", "command_id": protection["command_id"], "quantity": 1,
        })
        opposite = fixture_switch(
            held.policy,
            previous=PongDirection.LONG,
            direction=PongDirection.SHORT,
            at=held_at + timedelta(milliseconds=2),
            ordinal=3,
            prior=held_entry_switch.event_id,
        )
        held.host._apply_discernment_update(
            DiscernmentUpdate(opposite.snapshot, opposite, True, "TEST_ONLY_HELD_OPPOSITE"),
        )
        cancellation_stop = held.wait_dispatch("CANCEL_PROTECTIVE_STOP")
        held.send({
            "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE",
            "order_state": "CANCELLED", "command_id": cancellation_stop["command_id"],
        })
        exit_directive = held.wait_dispatch("EXIT")
        held.send({"message_type": "COMMAND_ACK", "order_role": "EXIT", "command_id": exit_directive["command_id"]})
        held.runtime.position_quantity = 0
        held.runtime.working_owned_orders = 0
        held.send({
            "message_type": "EXECUTION_EVENT", "order_role": "EXIT",
            "command_id": exit_directive["command_id"], "quantity": 1,
            "execution_id": "fixture-held-exit-fill",
        })
        held.send({
            "message_type": "RECONCILIATION", "position_quantity": 0,
            "working_order_count": 0,
        })
        events.append({
            "scenario": "held_long_transient_opposite_exit",
            "passed": (
                held.host._bridge is not None
                and held.host._bridge.machine.actual_quantity == 0
                and held.host._bridge.machine.state is PongState.FLAT_WAIT_BEAT
            ),
            "directive_id": exit_directive["directive_id"],
        })
        held_final_status = held.host.status()
    finally:
        held.stop()

    # Invalid evidence while protected must retain risk reduction authority:
    # it cancels the stop, emits emergency flatten, and remains lockout-flat.
    invalid, invalid_at, invalid_entry_switch, invalid_entry = admitted_long("invalid-evidence")
    invalid_final_status: dict[str, object]
    try:
        invalid.runtime.position_quantity = 1
        invalid.runtime.working_owned_orders = 1
        invalid.send({"message_type": "COMMAND_ACK", "order_role": "ENTRY", "command_id": invalid_entry["command_id"]})
        invalid.send({
            "message_type": "EXECUTION_EVENT", "order_role": "ENTRY",
            "command_id": invalid_entry["command_id"], "quantity": 1,
            "execution_id": "fixture-invalid-entry-fill",
        })
        invalid_protection = invalid.wait_dispatch("VERIFY_NATIVE_PROTECTION")
        invalid.send({
            "message_type": "COMMAND_ACK", "order_role": "PROTECTIVE", "command_id": invalid_protection["command_id"]})
        invalid.send({
            "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE",
            "order_state": "WORKING", "command_id": invalid_protection["command_id"], "quantity": 1,
        })
        invalid_snapshot = fixture_snapshot(
            invalid.policy,
            score=None,
            at=invalid_at + timedelta(milliseconds=2),
            valid=False,
            ordinal=5,
        )
        invalid.host._apply_discernment_update(
            DiscernmentUpdate(invalid_snapshot, None, False, "TEST_ONLY_INVALID"),
        )
        invalid_cancellation = invalid.wait_dispatch("CANCEL_PROTECTIVE_STOP")
        invalid.send({
            "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE",
            "order_state": "CANCELLED", "command_id": invalid_cancellation["command_id"],
        })
        safety = invalid.wait_dispatch("EMERGENCY_FLATTEN")
        invalid.send({"message_type": "COMMAND_ACK", "order_role": "EXIT", "command_id": safety["command_id"]})
        invalid.runtime.position_quantity = 0
        invalid.runtime.working_owned_orders = 0
        invalid.send({
            "message_type": "EXECUTION_EVENT", "order_role": "EXIT",
            "command_id": safety["command_id"], "quantity": 1,
            "execution_id": "fixture-invalid-evidence-safety-fill",
        })
        invalid.send({
            "message_type": "RECONCILIATION", "position_quantity": 0,
            "working_order_count": 0,
        })
        events.append({
            "scenario": "invalid_evidence_safety_exit",
            "passed": (
                invalid.host._bridge is not None
                and invalid.host._bridge.machine.actual_quantity == 0
                and invalid.host._bridge.machine.state is PongState.SAFETY_BLOCKED
            ),
            "directive_id": safety["directive_id"],
        })
        invalid_final_status = invalid.host.status()
    finally:
        invalid.stop()
    return {
        "schema": TEST_ONLY_SCHEMA,
        "test_only": True,
        "native_dispatcher": TEST_ONLY_DISPATCHER,
        "events": events,
        "all_passed": all(item["passed"] for item in events),
        "late_fill_final_host_status": late_final_status,
        "held_exit_final_host_status": held_final_status,
        "invalid_evidence_final_host_status": invalid_final_status,
    }


def run_paced(corpus: Path, output: Path, key: bytes, *, limit_seconds: float) -> dict[str, object]:
    """Natural-paced single candidate run through real host and collector code."""
    manifest, source, history_at = verify_corpus(corpus, key)
    raw = (corpus / "frames.jsonl").read_bytes()
    if digest(raw) != manifest["frames_sha256"]:
        raise ValueError("CORPUS_DIGEST_CHANGED")
    frames = tuple(raw.splitlines())
    del raw
    if len(frames) != manifest["records"]:
        raise ValueError("CORPUS_COUNT_CHANGED")
    rows = [json.loads(frame) for frame in frames]
    kinds = [str(row["observation_type"]) for row in rows]
    receipts = [utc(str(row["ninja_receipt_time"])) for row in rows]
    receipt_ns = [native_timestamp_ns(str(row["ninja_receipt_time"])) for row in rows]
    source_ns = [native_timestamp_ns(str(row.get("exchange_timestamp") or row.get("provider_timestamp") or row["ninja_receipt_time"])) for row in rows]
    ids = [str(row["observation_id"]) for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("CORPUS_DUPLICATE_OBSERVATION_ID")
    market_origin = next(index for index, kind in enumerate(kinds) if kind in MARKET_TYPES)
    origin_ns = receipt_ns[market_origin]
    dues = [max(0.0, (value - origin_ns) / 1_000_000_000) for value in receipt_ns]
    if history_at > receipts[market_origin]:
        raise ValueError("HISTORY_NOT_AVAILABLE_BEFORE_REPLAY_MARKET_ORIGIN")

    if not output.is_dir():
        raise ValueError("OUTPUT_DIRECTORY_MISSING")
    output.joinpath("TEST_ONLY_NO_NATIVE_OR_PAPER_EXECUTION.txt").write_text(
        "This directory is an isolated offline operational fixture. No native, broker, or paper execution occurred.\n",
        encoding="utf-8",
    )
    output.joinpath("loaded-source-manifest.json").write_bytes(canonical(source_manifest()) + b"\n")
    write_json(output / "run-plan.json", {
        "schema": TEST_ONLY_SCHEMA,
        "test_only": True,
        "execution_authority": "DENIED",
        "native_runtime_started": False,
        "corpus": str(corpus),
        "frames_sha256": manifest["frames_sha256"],
        "records": len(frames),
        "natural_schedule_seconds": dues[-1],
        "interval_ms": 20,
        "producer": "independent_release_sender",
        "source_history_available_at": wire(history_at),
    })

    context = multiprocessing.get_context("spawn")
    go, abort = context.Event(), context.Event()
    epoch = context.RawValue("d", 0.0)
    released, admitted_shared = context.RawValue("q", 0), context.RawValue("q", 0)
    sender_state = context.RawValue("i", 0)
    progress_lock = context.Lock()
    producer_cpu = context.RawValue("q", 0)
    release_started = context.RawArray("d", len(frames))
    release_completed = context.RawArray("d", len(frames))
    outstanding_peaks = context.RawArray("q", 2)
    read_pipe, write_pipe = context.Pipe(duplex=False)
    producer = context.Process(
        target=independent_release_sender,
        name="PONG_PROTECTED_OPERATIONAL_FIXTURE_PRODUCER",
        args=(str(corpus / "frames.jsonl"), manifest["frames_sha256"], dues, write_pipe, go, abort, epoch,
              release_started, release_completed, released, admitted_shared, sender_state,
              progress_lock, producer_cpu, outstanding_peaks, 262144, 536870912),
    )

    failures: list[str] = []
    samples: list[dict[str, object]] = []
    host_cpu_samples: list[dict[str, object]] = []
    accepted = 0
    current_index = -1
    sender_identity: dict[str, object] | None = None
    collector_status: dict[str, object] | None = None
    started = 0.0
    initial_cpu = system_cpu_times()
    time.sleep(0.5)
    final_cpu = system_cpu_times()
    cpu_total = final_cpu[1] - initial_cpu[1]
    preflight = {
        "aggregate_cpu_busy_percent": None if not cpu_total else 100 * (1 - (final_cpu[0] - initial_cpu[0]) / cpu_total),
        "sample_seconds": 0.5,
        "benchmark_exclusion": "GLOBAL_MUTEX",
        "disk_contention": "NOT_MEASURED_BY_RUNNER_REQUIRES_EXTERNAL_HOST_CHECK",
        "owner_process": process_identity(),
    }
    write_json(output / "preflight-host.json", preflight)

    runtime = _RuntimeState(source, str(rows[0]["producer"]["build_fingerprint"]), "l3g-es-" + "5" * 32)
    native_dispatches: list[dict[str, object]] = []
    def native_dispatch(directive: object, evidence: object) -> dict[str, object]:
        native_dispatches.append({
            "dispatcher": TEST_ONLY_DISPATCHER,
            "directive_id": getattr(directive, "directive_id", None),
            "kind": str(getattr(directive, "kind", "")),
        })
        return {"accepted": True, "command_id": "fixture-unreachable", "directive_id": getattr(directive, "directive_id", None)}

    collector = PongAuthenticHourCollector(
        output / "collector",
        expected_source_fingerprint=source,
        independent_market_collection=True,
    )
    collector._queue = ObservedQueue(collector._queue.maxsize)
    identity = PongLoadedRuntimeIdentity(
        execution_session_id=runtime.execution_session_id,
        addon_protocol_version="l3g-paper-addon-provenance-v1",
        addon_source_fingerprint=source,
        addon_build_fingerprint=runtime.build_fingerprint,
        verified_at=wire(datetime.now(timezone.utc)),
        attestation_verified=True,
    )
    host = InstrumentedHost(
        output / "host",
        selected_profile=PONG_ENTRY_PROFILE_VERSION,
        expected_market_source_fingerprint=source,
        runtime_snapshot=runtime.snapshot,
        collector_status=collector.status,
        controller_dispatch=lambda _kind, _reason: True,
        native_dispatch=native_dispatch,
        activation_authorized=False,
        history_verification_key=key,
        scheduling=PongSchedulingConfig(PongSchedulingMode.PERIODIC_FLAT_PROTECTED, 20),
    )
    host._runtime = runtime.snapshot()
    host._bridge = PongRuntimeBridge.create(
        PongStateMachine(
            quantity_policy=PongQuantityPolicy(
                native_maximum_quantity=1,
                native_capability_status="TEST_ONLY_ISOLATED_ONE_MNQ_CAPABILITY",
                reconciliation_source_fingerprint=source,
                reconciliation_build_fingerprint=runtime.build_fingerprint,
            ),
            discernment_policy=host._discernment.policy,
        ),
        output / "host" / "bridges" / "test-only-flat.json",
        runtime_identity=identity,
    )

    def admit(record: NinjaTraderObservation) -> None:
        nonlocal accepted
        if current_index < 0 or record.observation_id != ids[current_index]:
            raise ValueError("RECEIVER_ORDER_CHANGED")
        collector.ingest(record)
        host.on_observation(record)
        accepted += 1
        with release_progress_guard(progress_lock):
            admitted_shared.value = accepted

    harness = NinjaTraderCommissioningHarness(
        attestation_key=key,
        require_attestation=True,
        expected_source_fingerprint=source,
        on_observation=admit,
        on_rejection=collector.on_rejection,
        on_duplicate=collector.on_duplicate,
    )
    owner_cpu_start = time.process_time_ns()
    cpu_counters = system_cpu_times()
    next_sample = 0.0
    next_cpu_sample = 1.0
    workers_stopped = False
    source_unchanged = False
    lifecycle_seconds: float | None = None
    try:
        collector.start()
        host.start()
        producer.start()
        write_pipe.close()
        if not read_pipe.poll(30):
            raise RuntimeError("PRODUCER_PRELOAD_TIMEOUT")
        sender_identity = json.loads(read_pipe.recv_bytes())
        read_pipe.close()
        if (
            sender_identity.get("status") != "READY"
            or sender_identity.get("frames_sha256") != manifest["frames_sha256"]
            or sender_identity.get("frame_count") != len(frames)
        ):
            raise RuntimeError("PRODUCER_PRELOAD_IDENTITY_MISMATCH")
        started = time.perf_counter()
        epoch.value = started
        go.set()
        while True:
            elapsed = time.perf_counter() - started
            with release_progress_guard(progress_lock):
                published, state = released.value, sender_state.value
            if state in (RELEASE_ABORTED, RELEASE_CAPACITY_EXHAUSTED, RELEASE_FAILED):
                failures.append("PRODUCER_STATE_" + str(state))
                break
            target = min(published, current_index + 1 + 64)
            while target < published and target and receipt_ns[target] == receipt_ns[target - 1]:
                target += 1
            while current_index + 1 < target:
                current_index += 1
                harness.ingest((frames[current_index],))
                if accepted != current_index + 1:
                    failures.append("SIGNED_INPUT_REJECTED_OR_DUPLICATED")
                    break
            if failures:
                break
            host_status = host.status()
            if elapsed >= next_sample:
                durable = int(collector.status()["durability"]["durable_records"])
                samples.append({
                    "elapsed_seconds": elapsed,
                    "due": bisect_right(dues, elapsed),
                    "released": published,
                    "receiver_accepted": accepted,
                    "authoritative_admission": host_status["evaluation_schedule"]["authoritative_admission_records"],
                    "durable": durable,
                    "host_observation_queue": host_status["observation_queue_depth"],
                    "collector_queue": collector._queue.qsize(),
                    "publisher_backlog": published - accepted,
                    "durability_backlog": accepted - durable,
                })
                next_sample = elapsed + 0.1
            if elapsed >= next_cpu_sample:
                counters = system_cpu_times()
                total_delta = counters[1] - cpu_counters[1]
                idle_delta = counters[0] - cpu_counters[0]
                host_cpu_samples.append({
                    "elapsed_seconds": elapsed,
                    "aggregate_cpu_busy_percent": None if not total_delta else 100 * (1 - idle_delta / total_delta),
                })
                cpu_counters = counters
                next_cpu_sample = elapsed + 1.0
            if host_status["input_error"] or collector.status()["state"] == "FAILED":
                failures.append(str(host_status["input_error"] or "COLLECTOR_FAILED"))
                break
            admitted_count = int(host_status["evaluation_schedule"]["authoritative_admission_records"])
            durable_count = int(collector.status()["durability"]["durable_records"])
            if current_index + 1 == len(frames) and state == RELEASE_COMPLETE and admitted_count == accepted and durable_count == accepted:
                break
            if elapsed > limit_seconds:
                failures.append("DRAIN_TIMEOUT")
                break
            time.sleep(0.0005)
    except BaseException as error:
        failures.append(type(error).__name__ + ":" + str(error))
    finally:
        lifecycle_seconds = time.perf_counter() - started if started else None
        abort.set()
        if producer.pid is not None:
            producer.join(10)
        try:
            host.stop(10)
        except Exception as error:
            failures.append("HOST_STOP_" + type(error).__name__)
        try:
            collector_status = collector.stop(timeout_seconds=15)
        except Exception as error:
            failures.append("COLLECTOR_STOP_" + type(error).__name__)
        workers_stopped = not producer.is_alive() and not bool(host._thread and host._thread.is_alive()) and not bool(collector._thread and collector._thread.is_alive())
        if not workers_stopped:
            failures.append("OWNED_WORKER_SHUTDOWN_UNPROVEN")

    collector_status = collector.status() if collector_status is None else collector_status
    host_status = host.status()
    journal_ok = False
    verified_records = 0
    previous = "0" * 64
    try:
        with collector.journal_path.open("rb") as stream:
            for index, line in enumerate(stream):
                row = json.loads(line)
                claimed = row.pop("record_hash")
                if row.get("journal_sequence") != index + 1 or row.get("previous_record_hash") != previous:
                    raise ValueError("JOURNAL_SEQUENCE_OR_CHAIN")
                if digest(canonical(row)) != claimed:
                    raise ValueError("JOURNAL_RECORD_HASH")
                observation = canonical(row["observation"])
                if observation != frames[index] or digest(observation) != row.get("wire_hash"):
                    raise ValueError("JOURNAL_SIGNED_WIRE_PARITY")
                previous = claimed
                verified_records += 1
        journal_ok = (
            verified_records == accepted
            and previous == collector_status["durability"]["journal_tip_sha256"]
        )
    except Exception as error:
        failures.append("JOURNAL_VERIFICATION_" + type(error).__name__ + ":" + str(error))
    loaded = json.loads((output / "loaded-source-manifest.json").read_bytes())
    try:
        source_unchanged = all(file_digest(ROOT / path) == value for path, value in loaded.items())
    except OSError:
        source_unchanged = False
        failures.append("LOADED_SOURCE_REVALIDATION_FAILED")
    input_counts = Counter(kinds)
    expected_market = sum(input_counts[item] for item in MARKET_TYPES)
    final_admission = int(host_status["evaluation_schedule"]["authoritative_admission_records"])
    final_durable = int(collector_status["durability"]["durable_records"])
    integrity_pass = bool(
        not failures
        and accepted == len(frames) == final_admission == final_durable == verified_records
        and not harness.summary.rejected
        and not harness.summary.duplicates
        and not collector_status["durability"]["queue_overflow_records"]
        and journal_ok
        and workers_stopped
        and source_unchanged
    )
    result = {
        "schema": TEST_ONLY_SCHEMA,
        "test_only": True,
        "qualification": "NOT_QUALIFIED",
        "execution_authority": "DENIED",
        "native_runtime_started": False,
        "paper_execution_started": False,
        "dispatcher": TEST_ONLY_DISPATCHER,
        "corpus": {"path": str(corpus), "manifest": manifest, "frames_sha256": manifest["frames_sha256"]},
        "schedule": {"mode": "NATURAL_RECEIPT_PACED", "interval_ms": 20, "scheduled_seconds": dues[-1]},
        "records": {
            "source": len(frames), "source_by_type": dict(input_counts), "market": expected_market,
            "receiver_accepted": accepted, "authoritative_admission": final_admission,
            "journal_enqueued": collector_status["durability"]["enqueued_records"],
            "journal_written": collector_status["durability"]["written_records"],
            "journal_durable": final_durable,
            "verified_journal_records": verified_records,
        },
        "queues": {
            "publisher": {"limit_records": 262144, "peak_records": outstanding_peaks[0]},
            "production_observation": {
                "limit_records": host._observations.maxsize,
                "peak_records": host.observation_queue_peak,
                "final_records": host_status["observation_queue_depth"],
                "admission_age_seconds": distribution(host.observation_queue_ages_seconds),
            },
            "collector": {
                "limit_records": collector._queue.maxsize,
                "peak_records": collector._queue.peak,
                "final_records": collector._queue.qsize(),
            },
            "growth_samples": samples,
        },
        "timing": {
            "host_evaluation_wall_seconds": distribution(host.evaluation_wall_seconds),
            "host_evaluation_thread_cpu_seconds": distribution(host.evaluation_cpu_seconds),
            "callback_to_authoritative_admission_seconds": distribution(host.admission_latency_seconds),
            "first_analytical_attempt_seconds": distribution(host.result_latency_seconds),
            "source_to_receipt_seconds": distribution([
                (receipt - source_value) / 1_000_000_000
                for receipt, source_value, kind in zip(receipt_ns, source_ns, kinds)
                if kind in MARKET_TYPES
            ]),
            "decision_latency": None,
            "decision_latency_reason": "No live/current decision; this historical fixture exposes analytical attempt timing only.",
            "protective_trigger_to_request": "Measured by the synthetic protected scenario receipt, not inferred from historical market data.",
        },
        "cpu": {
            "owner_process_cpu_ns": time.process_time_ns() - owner_cpu_start,
            "producer_process_cpu_ns": producer_cpu.value,
            "host_load_samples": host_cpu_samples,
            "preflight": preflight,
        },
        "deadline": host_status["evaluation_schedule"]["deadline_metrics"],
        "host": host_status,
        "collector": collector_status,
        "native_dispatches": native_dispatches,
        "integrity": {
            "journal_signed_wire_parity": "PASS" if journal_ok else "FAIL",
            "source_unchanged_during_run": source_unchanged,
            "workers_stopped": workers_stopped,
            "receiver_rejected": harness.summary.rejected,
            "receiver_duplicates": harness.summary.duplicates,
            "complete_durable_drain": final_durable == accepted,
            "pass": integrity_pass,
        },
        "failures": failures,
        "lifecycle_seconds": lifecycle_seconds,
        "producer_exitcode": producer.exitcode,
        "sender_identity": sender_identity,
        "limits": {
            "maximum_quantity": 1,
            "live_capital": "DENIED",
            "entry_authority": "DISARMED",
            "freshness": "Historical source only; no fresh-market qualification claim.",
            "watchdog": "Host controller remains PREPARED; synthetic active-controller coverage is recorded separately.",
        },
    }
    write_json(output / "result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--limit-seconds", type=float, default=900.0)
    parser.add_argument("--scenarios-only", action="store_true")
    args = parser.parse_args()
    if args.limit_seconds <= 0:
        parser.error("--limit-seconds must be positive")
    output = args.output.resolve()
    if output.exists():
        parser.error("output must not already exist")
    output.mkdir(parents=True)
    try:
        scenarios = scenario_matrix(output)
        write_json(output / "protected-scenarios.json", scenarios)
        if args.scenarios_only:
            result = {
                "schema": TEST_ONLY_SCHEMA,
                "test_only": True,
                "qualification": "NOT_QUALIFIED",
                "protected_scenarios": scenarios,
                "status": "PASS" if scenarios["all_passed"] else "FAIL",
            }
            write_json(output / "result.json", result)
            return 0 if scenarios["all_passed"] else 1
        if args.corpus is None:
            parser.error("--corpus is required unless --scenarios-only is used")
        key = LocalPaperSecretProvider().load_key()
        # A named OS mutex makes this one machine's sole capacity measurement;
        # a concurrent Pong benchmark is a refused run, not a noisy sample.
        with BenchmarkLease(str(output)) as lease:
            result = run_paced(
                args.corpus.resolve(), output, key, limit_seconds=args.limit_seconds,
            )
        result["benchmark_lease"] = lease.metadata
        result["protected_scenarios"] = scenarios
        result["status"] = "PASS" if result["integrity"]["pass"] and scenarios["all_passed"] else "FAIL"
        write_json(output / "result.json", result)
        return 0 if result["status"] == "PASS" else 1
    except BaseException as error:
        result_path = output / "result.json"
        if not result_path.exists():
            write_json(result_path, {
                "schema": TEST_ONLY_SCHEMA,
                "test_only": True,
                "qualification": "NOT_QUALIFIED",
                "status": "INCOMPLETE",
                "error": type(error).__name__ + ":" + str(error),
            })
        raise


if __name__ == "__main__":
    raise SystemExit(main())
