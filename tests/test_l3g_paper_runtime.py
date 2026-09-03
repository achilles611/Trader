from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import AccountClass, NinjaTraderObservation
from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.ledger import (
    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS,
    HEALTH_AUTHORITY_OBSERVATION_SEMANTICS,
    PaperLedger,
    CommissioningTailCategory,
    commissioning_tail_classification,
)
from src.l3g_paper.ninjatrader_transport import ADDON_PROTOCOL_VERSION, PaperExecutionTransport, expected_addon_source_fingerprint
from src.l3g_paper.health import ledger_health_projection
from src.l3g_paper.runtime import (
    LaneIIIPaperRuntime,
    ObservationFanout,
    _CommissioningOwnership,
    _DURABILITY_UNAVAILABLE_MARKER,
)
from src.l3g_paper.contracts import PaperDirection, PaperEntryOwner, PaperRuntimeState, PaperSessionArmGrant
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.sessions import PaperSessionResolver
from tests.l3g_helpers import ObservationFactory, warmed_bullish_policy


class PaperRuntimeTests(unittest.TestCase):
    @staticmethod
    def reserve_commissioning(runtime: LaneIIIPaperRuntime, context: object, now: str) -> tuple[str, str]:
        ownership = _CommissioningOwnership(
            "l3g-commissioning-test", "l3g-commissioning-token-test", context, now,
            ledger_preflight={
                "authority_commit_checkpoint": runtime.ledger.commissioning_authority_checkpoint(),
            },
        )  # type: ignore[arg-type]
        runtime._commissioning_ownership = ownership
        runtime._entry_owner = PaperEntryOwner.COMMISSIONING
        candidate = warmed_bullish_policy()[2]
        runtime._last_qualifying_entry_decision = replace(
            candidate,
            created_at=now,
            expires_at=(
                datetime.fromisoformat(now.replace("Z", "+00:00")) + timedelta(seconds=5)
            ).isoformat().replace("+00:00", "Z"),
            session_kind=context.session_kind,
            session_id=context.session_id,
            trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash,
            session_generation=context.session_generation,
        )
        return ownership.commissioning_id, ownership.commissioning_token

    @staticmethod
    def reconcile_flat(runtime: LaneIIIPaperRuntime) -> None:
        runtime.on_execution_bridge_state("AUTHENTICATED")
        runtime.on_execution_message({
            "message_type": "RECONCILIATION", "receipt_id": "reconcile-flat", "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
            "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
            "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:00Z",
        })

    @staticmethod
    def watchdog_runtime(directory: str) -> tuple[PaperLedger, LaneIIIPaperRuntime]:
        """Create an authenticated-looking AddOn boundary without listening or sending."""
        ledger = PaperLedger(Path(directory) / "paper.sqlite3")
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(ledger, port=48174)
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-watchdog-test"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        runtime._state = PaperRuntimeState.ENTRY_PENDING
        return ledger, runtime

    @staticmethod
    def watchdog_flat_reconciliation(
        receipt_id: str,
        *,
        safety_event_id: str | None = None,
        safety_settlement_final: bool | None = None,
        safety_settlement_sequence: int | None = None,
    ) -> dict[str, object]:
        message: dict[str, object] = {
            "message_type": "RECONCILIATION",
            "receipt_id": receipt_id,
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "position_quantity": 0,
            "working_order_count": 0,
            "working_entry_count": 0,
            "position_snapshot_complete": True,
            "order_snapshot_complete": True,
            "foreign_activity": False,
            "timestamp": "2026-08-28T14:00:00Z",
        }
        if safety_event_id is not None:
            message["safety_event_id"] = safety_event_id
        if safety_settlement_final is not None:
            message["safety_settlement_final"] = safety_settlement_final
        if safety_settlement_sequence is not None:
            message["safety_settlement_sequence"] = safety_settlement_sequence
        return message

    @staticmethod
    def operational_runtime(directory: str, *, now: str = "2026-08-26T14:00:00Z") -> tuple[PaperLedger, LaneIIIPaperRuntime]:
        """Ready, authenticated Sim101 fixture without a listener or order transport."""
        ledger = PaperLedger(Path(directory) / "paper.sqlite3")
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(ledger, port=48175)
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-operational-test"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        context = PaperSessionResolver().resolve(now, generation=1).context
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._session_context = context
        runtime._snapshot = PaperRiskSnapshot(
            now, account_name="Sim101", account_class="LOCAL_SIMULATION", instrument="MNQ SEP26",
            position_snapshot_complete=True, order_snapshot_complete=True,
            reconciliation_current=True, local_bridge_healthy=True,
            market_price_connected=True, execution_bridge_healthy=True, evidence_warmed=True,
            commissioning_session_warmed=True, depth_reset_recovery=False,
            quote_observed_at=now, classified_trade_observed_at=now, depth_mutation_observed_at=now,
            session_kind=context.session_kind, session_id=context.session_id, trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
        )
        runtime._persist_and_send = lambda *_: None  # type: ignore[method-assign]
        runtime.operational_paper_readiness = lambda _preflight=None: {  # type: ignore[method-assign]
            "result": "READY", "blocking_reasons": [], "ledger": {"status": "PASS"},
        }
        return ledger, runtime

    def test_operational_paper_start_is_continuous_and_never_invokes_atomic_commissioning(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                with (
                    patch("src.l3g_paper.runtime._now", return_value=now),
                    patch.object(runtime, "commissioning_start", side_effect=AssertionError("atomic commissioning must not run")),
                ):
                    started = runtime.operational_paper_start("operational-start-001")
                    replay = runtime.operational_paper_start("operational-start-001")

                self.assertTrue(started["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertTrue(runtime.status()["operational_paper_session"]["active"])
                self.assertTrue(replay["idempotent_replay"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                self.assertEqual(
                    [record["kind"] for record in ledger.recent(30)].count("SESSION_OPERATIONAL_PAPER_STARTED"),
                    1,
                )
            finally:
                runtime.stop(); ledger.close()

    def test_operational_session_stays_running_across_idle_time_and_one_flat_trade_cycle(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    self.assertTrue(runtime.operational_paper_start("operational-start-002")["started"])
                    # Five simulated idle minutes with no signal/command are
                    # not a lifecycle boundary.
                    with patch("src.l3g_paper.runtime._now", return_value="2026-08-26T14:05:00Z"):
                        runtime.status()
                    self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)

                    for sequence in range(1):
                        runtime._transition(PaperRuntimeState.ENTRY_PENDING, f"TEST_ENTRY_{sequence}")
                        runtime._transition(PaperRuntimeState.LONG, f"TEST_FILL_{sequence}")
                        runtime._position = PaperDirection.LONG
                        runtime._position_quantity = 1
                        runtime._entry_fill_price = Decimal("100")
                        runtime._entry_fill_quantity = 1
                        runtime._entry_direction = PaperDirection.LONG
                        runtime._entry_execution = {"native_execution_id": f"entry-{sequence}", "timestamp": now}
                        runtime._entry_session_context = runtime._session_context
                        runtime._snapshot = replace(
                            runtime._snapshot, current_position=PaperDirection.LONG,
                            current_position_quantity=1, protective_stop_state="WORKING",
                        )
                        runtime._request_exit("TEST_NORMAL_EXIT")
                        self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                        runtime.on_execution_message({
                            "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101",
                            "quantity": 1, "native_execution_id": f"exit-{sequence}", "timestamp": now,
                        })
                        runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": now})
                        runtime.on_execution_message({
                            "message_type": "RECONCILIATION", "receipt_id": f"operational-flat-{sequence}",
                            "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
                            "position_quantity": 0, "working_order_count": 0, "working_entry_count": 0,
                            "position_snapshot_complete": True, "order_snapshot_complete": True,
                            "foreign_activity": False, "timestamp": now,
                        })
                        self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                        self.assertTrue(runtime.status()["operational_paper_session"]["active"])

                status = runtime.status()
                self.assertEqual(status["paper_session_pnl"]["realized"], "2")
                self.assertEqual(status["paper_session_pnl"]["unrealized"], "0")
                self.assertEqual(status["current_position"], "FLAT")
            finally:
                runtime.stop(); ledger.close()

    def test_open_paper_position_updates_unrealized_pnl_without_quote_persistence(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            factory = ObservationFactory(
                start=datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc)
            )
            try:
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                runtime._entry_fill_price = Decimal("100")
                runtime._entry_fill_quantity = 1
                runtime._entry_direction = PaperDirection.LONG
                runtime._entry_session_context = runtime._session_context
                runtime._snapshot = replace(
                    runtime._snapshot,
                    current_position=PaperDirection.LONG,
                    current_position_quantity=1,
                )
                runtime.ingest(factory.quote(101))
                status = runtime.status()
                self.assertEqual(status["paper_session_pnl"]["unrealized"], "2")
                self.assertEqual(ledger.health_status()["counts"].get("OBSERVATION", 0), 0)
            finally:
                runtime.stop(); ledger.close()

    def test_operational_stop_is_idempotent_and_requires_clean_flat_reconciliation(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    self.assertTrue(runtime.operational_paper_start("operational-start-003")["started"])
                    first = runtime.flatten_and_disarm()
                    replay = runtime.flatten_and_disarm()
                self.assertTrue(first["flat_confirmed"])
                self.assertTrue(replay["flat_confirmed"])
                self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
                self.assertIsNone(runtime.status()["operational_paper_session"])
                self.assertEqual(
                    [record["kind"] for record in ledger.recent(50)].count("SESSION_OPERATIONAL_PAPER_STOPPED"),
                    1,
                )
            finally:
                runtime.stop(); ledger.close()

    def test_operational_ledger_accepts_its_known_online_tail_but_not_an_unknown_row(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                anchor = ledger.health_status()
                verification = {
                    "status": "PASS", "chain_valid": True, "checkpoint_valid": True,
                    "verified_through_sequence": anchor["highest_sequence"],
                    "tip_hash": anchor["final_record_hash"],
                }
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    self.assertTrue(runtime.operational_paper_start("operational-start-003b")["started"])
                active = runtime.status()["operational_paper_session"]
                healthy = ledger_health_projection(ledger.health_status(), verification, operational_session=active)
                self.assertTrue(healthy["operational_ledger"]["online_append_integrity"])
                self.assertEqual(
                    healthy["operational_ledger"]["tail_state"],
                    "LEGITIMATE_AUTHORITY_MUTATION_TAIL_AWAITING_BATCH_VERIFICATION",
                )

                ledger.append("FUTURE_UNCLASSIFIED_OPERATIONAL_RECORD", {"opaque": "must-fail-closed"})
                corrupt = ledger_health_projection(ledger.health_status(), verification, operational_session=active)
                self.assertFalse(corrupt["operational_ledger"]["online_append_integrity"])
                self.assertTrue(corrupt["operational_ledger"]["unknown_tail_present"])
            finally:
                runtime.stop(); ledger.close()

    def test_operational_session_stays_green_across_repeated_observer_health_cycles(self) -> None:
        now = "2026-08-26T14:00:00Z"
        observer_health = {
            "component": "MARKET_OBSERVER_ATTACHMENT",
            "state": "NATIVE_ADDON_OBSERVER_ACTIVE",
            "configured_instrument": "MNQ SEP26",
            "instrument": "MNQ SEP26",
            "chart_found": True,
            "observer_attached": True,
            "subscription_mode": "NATIVE_ADDON",
        }
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                runtime._session_generation = 1
                anchor = ledger.health_status()
                verification = {
                    "status": "PASS", "chain_valid": True, "checkpoint_valid": True,
                    "verified_through_sequence": anchor["highest_sequence"],
                    "tip_hash": anchor["final_record_hash"],
                }
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    self.assertTrue(runtime.operational_paper_start("operational-health-cycles")["started"])

                factory = ObservationFactory(
                    start=datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc),
                )
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    for _ in range(4):
                        runtime.ingest(factory.make("HEALTH", observer_health))
                        ledger.flush_deferred()
                        projected = ledger_health_projection(
                            ledger.health_status(), verification,
                            operational_session=runtime.status()["operational_paper_session"],
                        )
                        self.assertTrue(projected["operational_ledger"]["online_append_integrity"])
                        self.assertFalse(projected["operational_ledger"]["unknown_tail_present"])
                        self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)

                latest = ledger.recent(1)[0]["payload"]
                self.assertEqual(
                    latest["observation_semantics"],
                    HEALTH_AUTHORITY_OBSERVATION_SEMANTICS,
                )
                self.assertEqual(latest["authority_effect"], "NONE")
            finally:
                runtime.stop(); ledger.close()

    def test_unattested_or_future_health_shape_remains_unknown(self) -> None:
        now = "2026-08-26T14:00:00Z"
        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                runtime._session_generation = 1
                anchor = ledger.health_status()
                verification = {
                    "status": "PASS", "chain_valid": True, "checkpoint_valid": True,
                    "verified_through_sequence": anchor["highest_sequence"],
                    "tip_hash": anchor["final_record_hash"],
                }
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    runtime.operational_paper_start("operational-unknown-health")
                factory = ObservationFactory(
                    start=datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc),
                )
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    runtime.ingest(factory.make("HEALTH", {
                        "state": "NATIVE_ADDON_OBSERVER_ACTIVE",
                        "future_authority_field": "must-not-be-accepted",
                    }))
                    ledger.flush_deferred()
                    projected = ledger_health_projection(
                        ledger.health_status(), verification,
                        operational_session=runtime.status()["operational_paper_session"],
                    )
                self.assertFalse(projected["operational_ledger"]["online_append_integrity"])
                self.assertTrue(projected["operational_ledger"]["unknown_tail_present"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
            finally:
                runtime.stop(); ledger.close()

    def test_operational_stop_seals_pending_entries_and_open_positions_until_reconciled(self) -> None:
        now = "2026-08-26T14:00:00Z"

        def reconciliation(receipt_id: str) -> dict[str, object]:
            return {
                "message_type": "RECONCILIATION", "receipt_id": receipt_id,
                "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
                "position_quantity": 0, "working_order_count": 0, "working_entry_count": 0,
                "position_snapshot_complete": True, "order_snapshot_complete": True,
                "foreign_activity": False, "timestamp": now,
            }

        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    runtime.operational_paper_start("operational-start-004")
                    runtime._last_quote = (Decimal("100"), Decimal("100.25"), now)
                    runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_PENDING_ENTRY")
                    runtime._snapshot = replace(runtime._snapshot, working_owned_orders=1, working_entry_orders=1)
                    pending_stop = runtime.flatten_and_disarm()
                self.assertTrue(pending_stop["stopping"])
                self.assertTrue(runtime.status()["operational_paper_session"]["stopping"])
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                runtime.on_execution_message(reconciliation("operational-pending-flat"))
                self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
                self.assertIsNone(runtime.status()["operational_paper_session"])
            finally:
                runtime.stop(); ledger.close()

        with TemporaryDirectory() as directory:
            ledger, runtime = self.operational_runtime(directory, now=now)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    runtime.operational_paper_start("operational-start-005")
                    runtime._last_quote = (Decimal("100"), Decimal("100.25"), now)
                    runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_OPEN_ENTRY")
                    runtime._transition(PaperRuntimeState.LONG, "TEST_OPEN_FILL")
                    runtime._position = PaperDirection.LONG
                    runtime._position_quantity = 1
                    runtime._entry_fill_price = Decimal("100")
                    runtime._entry_fill_quantity = 1
                    runtime._entry_direction = PaperDirection.LONG
                    runtime._entry_execution = {"native_execution_id": "open-entry", "timestamp": now}
                    runtime._entry_session_context = runtime._session_context
                    runtime._snapshot = replace(
                        runtime._snapshot, current_position=PaperDirection.LONG,
                        current_position_quantity=1, protective_stop_state="WORKING",
                    )
                    open_stop = runtime.flatten_and_disarm()
                self.assertTrue(open_stop["stopping"])
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101", "quantity": 1,
                    "native_execution_id": "open-exit", "timestamp": now,
                })
                runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": now})
                runtime.on_execution_message(reconciliation("operational-open-flat"))
                self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
                self.assertIsNone(runtime.status()["operational_paper_session"])
            finally:
                runtime.stop(); ledger.close()

    def test_accepted_deferred_receipt_does_not_treat_its_own_enqueue_as_capacity_failure(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            receipt = {
                "schema": "l3g-ledger-writer-capacity-v1",
                "state": "HEALTHY",
                "admission_open": True,
                "capacity_fault_latched": False,
                "wal_capacity_fault_latched": False,
                "negative_headroom_sustained": False,
                "writer_error": None,
                "queue_growth_records_per_second": 1.0,
            }
            try:
                with patch.object(ledger, "append_deferred", return_value=receipt):
                    accepted = runtime._append_deferred_or_pause_locked(
                        "OBSERVATION_ENVELOPE", {"observation_type": "QUOTE"},
                    )
                self.assertTrue(accepted)
                self.assertIsNone(runtime.status()["lockout_or_fault_reason"])
                self.assertFalse(runtime._deferred_capacity_healthy_locked(receipt))
            finally:
                runtime.stop()
                ledger.close()

    def test_restart_reconciliation_returns_ready_disarmed_and_never_auto_arms(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48159)
            runtime.bind_transport(transport); runtime.start(); runtime.on_execution_bridge_state("AUTHENTICATED")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "r", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:00Z",
            })
            self.assertEqual(runtime.state.value, "READY_DISARMED")
            self.assertFalse(runtime.arm()["armed"])
            runtime.stop(); ledger.close()

    def test_exact_read_only_account_items_are_suppressed_but_unknown_shapes_remain_durable(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)

            def account(number: int, payload: dict[str, object]) -> NinjaTraderObservation:
                at = f"2026-08-26T14:00:0{number}Z"
                return NinjaTraderObservation(
                    f"nt-account-{number}", "account-session", "ACCOUNT", at, number, payload,
                    account_alias="Sim101", account_class=AccountClass.LOCAL_SIMULATION,
                    provider_timestamp=at,
                )

            runtime.ingest(account(1, {"item": "RealizedProfitLoss", "value": "0"}))
            runtime.ingest(account(2, {"item": "RealizedProfitLoss", "value": "0", "unknown": True}))
            ledger.flush_deferred()
            records = [record for record in ledger.recent(20, domain="OBSERVATION") if record["payload"]["observation_type"] == "ACCOUNT"]
            self.assertEqual(len(records), 1)
            unmarked = records[0]["payload"]
            self.assertNotIn("authority_effect", unmarked)
            policy = ledger.health_status()["persistence_policy"]
            self.assertEqual(policy["informational_account_observations"], "DISABLED")
            self.assertEqual(policy["suppressed_records_by_domain"]["OBSERVATION"], 1)
            runtime.stop(); ledger.close()

    def test_only_exact_account_authority_observations_receive_no_effect_marker(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)

            def authority(number: int, kind: str, payload: dict[str, object]) -> NinjaTraderObservation:
                at = f"2026-08-26T14:00:{number:02d}Z"
                return NinjaTraderObservation(
                    f"nt-authority-{number}", "authority-session", kind, at, number, payload,
                    account_alias="Sim101", account_class=AccountClass.LOCAL_SIMULATION,
                    provider_timestamp=at,
                )

            runtime.ingest(authority(1, "ORDER", {
                "native_order_id": "order-1", "status": "Filled", "quantity": 1,
                "filled_quantity": 1,
            }))
            runtime.ingest(authority(2, "EXECUTION", {
                "native_execution_id": "execution-1", "price": 23000.25, "quantity": 1,
            }))
            runtime.ingest(authority(3, "POSITION", {
                "quantity": 0, "direction": "Flat", "average_price": 0.0,
            }))
            runtime.ingest(authority(4, "ORDER", {
                "native_order_id": "order-2", "status": "Filled", "quantity": 1,
                "filled_quantity": 1, "future": True,
            }))
            ledger.flush_deferred()
            records = sorted(
                (
                    record for record in ledger.recent(20, domain="OBSERVATION")
                    if record["payload"]["observation_type"] in {"ORDER", "EXECUTION", "POSITION"}
                ),
                key=lambda record: record["payload"]["local_monotonic_sequence"],
            )
            self.assertEqual(len(records), 4)
            for record in records[:3]:
                payload = record["payload"]
                self.assertEqual(
                    payload["observation_semantics"],
                    COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS,
                )
                self.assertEqual(payload["authority_effect"], "NONE")
                self.assertEqual(
                    commissioning_tail_classification(
                        "OBSERVATION", "OBSERVATION_ENVELOPE", payload,
                    ).category,
                    CommissioningTailCategory.AUTHORITY_OBSERVATION,
                )
            self.assertNotIn("authority_effect", records[3]["payload"])
            runtime.stop(); ledger.close()

    def test_default_authority_ledger_suppresses_unbounded_market_traffic(self) -> None:
        """Raw market traffic stays in memory and cannot grow the authority ledger."""
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(
                start=datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc)
            )
            try:
                for index in range(200):
                    quote = factory.quote(100 + (index % 2))
                    runtime.ingest(quote)
                    runtime.ingest(factory.trade(quote, quote.payload["ask"]))
                    runtime.ingest(factory.depth("UPDATE", 10 + (index % 2)))
                    runtime.ingest(replace(
                        factory.make("CONNECTION", {"item": "UnrealizedProfitLoss", "value": "0"}),
                        observation_type="ACCOUNT",
                        account_alias="Sim101",
                        account_class=AccountClass.LOCAL_SIMULATION,
                    ))
                ledger.flush_deferred()

                health = ledger.health_status()
                self.assertEqual(health["counts"].get("OBSERVATION", 0), 0)
                self.assertEqual(health["counts"].get("EVIDENCE", 0), 0)
                self.assertEqual(health["counts"].get("DECISION", 0), 0)
                policy = health["persistence_policy"]
                self.assertEqual(policy["raw_market_observations"], "DISABLED")
                self.assertEqual(policy["informational_account_observations"], "DISABLED")
                self.assertEqual(policy["derived_evidence"], "DISABLED")
                self.assertEqual(policy["no_effect_decisions"], "DISABLED")
                self.assertGreaterEqual(policy["suppressed_records_total"], 800)
                self.assertEqual(
                    policy["scientific_bulk_persistence"],
                    "DISABLED_UNTIL_SEPARATE_BOUNDED_STORE",
                )
            finally:
                runtime.stop(); ledger.close()

    def test_addon_provenance_denies_arm_but_not_observation_or_exit_safety(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(ledger, port=48171)
            runtime = LaneIIIPaperRuntime(ledger); runtime.bind_transport(transport)
            runtime._state = PaperRuntimeState.READY_DISARMED
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = "stale"
            self.assertEqual(runtime.arm()["reason_codes"], ("ADDON_BUILD_MISMATCH",))
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
            # The provenance gate is now clear; an off-session fence, not the
            # AddOn, is the reason this fixture cannot arm.
            self.assertEqual(runtime.arm()["reason_codes"], ("NO_CURRENT_EVENT_SESSION",))
            ledger.close()

    def test_backward_provider_timestamp_does_not_close_the_current_paper_session(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(start=datetime(2026, 8, 25, 19, 0, tzinfo=timezone.utc))
            first = factory.quote(100); runtime.ingest(first)
            first_status = runtime.status()
            crossing = factory.depth("ADD", 10)
            earlier_provider_time = (datetime.fromisoformat(first.provider_timestamp.replace("Z", "+00:00")) - timedelta(milliseconds=50)).isoformat().replace("+00:00", "Z")
            crossing = type(crossing)(
                crossing.observation_id, crossing.session_id, crossing.observation_type,
                crossing.ninja_receipt_time, crossing.local_monotonic_sequence,
                crossing.payload, provider_timestamp=earlier_provider_time,
            )
            runtime.ingest(crossing)
            second_status = runtime.status()
            self.assertEqual(second_status["current_session_id"], first_status["current_session_id"])
            self.assertEqual(second_status["session_generation"], first_status["session_generation"])
            runtime.stop(); ledger.close()

    def test_late_local_callback_is_refused_without_resetting_ny_after_evidence_domain(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(start=datetime(2026, 8, 25, 20, 10, tzinfo=timezone.utc))
            runtime.ingest(factory.quote(100))
            admitted = runtime.status()
            late = factory.depth("ADD", 10)
            late = type(late)(
                late.observation_id, late.session_id, late.observation_type,
                (datetime.fromisoformat(late.ninja_receipt_time.replace("Z", "+00:00")) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
                late.local_monotonic_sequence, late.payload, provider_timestamp=late.provider_timestamp,
            )
            runtime.ingest(late)
            refused = runtime.status()
            self.assertEqual(admitted["current_session"], "NY_AFTER")
            self.assertEqual(refused["current_session_id"], admitted["current_session_id"])
            self.assertEqual(refused["session_generation"], admitted["session_generation"])
            self.assertTrue(any(item["kind"] == "INCIDENT_STALE_CALLBACK_REFUSED" for item in ledger.recent(20, domain="INCIDENT")))
            runtime.stop(); ledger.close()

    def test_fanout_isolates_sink_failures_and_preserves_both_deliveries(self) -> None:
        calls: list[str] = []; failures: list[tuple[str, str, str]] = []
        def broken(_: object) -> None: calls.append("shadow"); raise RuntimeError()
        def paper(_: object) -> None: calls.append("paper")
        fanout = ObservationFanout(
            shadow_observation=broken, shadow_transport=broken, shadow_rejection=broken, shadow_duplicate=lambda: None,
            paper_observation=paper, paper_transport=paper, paper_rejection=paper, paper_duplicate=lambda: None,
            record_failure=lambda *args: failures.append(args),
        )
        fanout.on_transport_state(StreamHealth.HEALTHY)
        self.assertEqual(calls, ["shadow", "paper"])
        self.assertEqual(failures[0][0], "SHADOW")

    def test_controlled_execution_events_cover_entry_protection_exit_and_flat(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48158)
            runtime.bind_transport(transport); runtime.start(); self.reconcile_flat(runtime)
            runtime._transition(PaperRuntimeState.ARMED_FLAT, "TEST_ARM")
            decision = warmed_bullish_policy()[2]
            runtime._pending_intent = runtime.risk.make_intent(
                decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100.25"),
            )
            runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_ENTRY_SENT")
            runtime.on_execution_message({
                "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100.25",
                "quantity": 1, "direction": "LONG", "timestamp": "2026-08-24T14:00:01Z",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LONG)
            runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING"})
            self.assertEqual(runtime.status()["protective_stop_state"], "WORKING")
            runtime._transition(PaperRuntimeState.EXIT_PENDING, "TEST_EXIT_SENT")
            runtime.on_execution_message({"message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101", "quantity": 1})
            reconciliation_commands: list[object] = []
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            runtime._persist_and_send = lambda command, grant: reconciliation_commands.append((command, grant))  # type: ignore[method-assign]
            runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": "2026-08-24T14:00:02Z"})
            self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
            self.assertEqual(reconciliation_commands[0][0].action.value, "RECONCILE")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "post-exit-flat", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:03Z",
            })
            status = runtime.status()
            self.assertEqual(runtime.state, PaperRuntimeState.ARMED_FLAT)
            self.assertEqual(status["session_entries"], 1)
            self.assertEqual(status["daily_realized_pnl"], "1.50")
            runtime.stop(); ledger.close()

    def test_commissioning_entry_is_fixed_and_retains_all_non_strategy_markers(self) -> None:
        now = "2026-08-26T14:00:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime._session_context = context
            runtime._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now, "2026-08-26T19:30:00Z",
            )
            runtime._snapshot = PaperRiskSnapshot(
                now, position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, local_bridge_healthy=True,
                market_price_connected=True, execution_bridge_healthy=True, evidence_warmed=True,
                commissioning_session_warmed=True,
                depth_reset_recovery=False, quote_observed_at=now, classified_trade_observed_at=now,
                depth_mutation_observed_at=now, session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
            )
            submitted: list[object] = []
            runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            commissioning_id, commissioning_token = self.reserve_commissioning(runtime, context, now)
            with patch("src.l3g_paper.runtime._now", return_value=now):
                result = runtime.commission_entry(commissioning_id, commissioning_token)
            self.assertTrue(result["submitted"])
            self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
            command, grant = submitted[0]
            self.assertEqual(command.account_name, "Sim101")
            self.assertEqual(command.instrument, "MNQ SEP26")
            self.assertEqual(command.quantity, 1)
            for record in ledger.recent(10):
                if record["kind"] in {"DECISION", "INTENT", "RISK_GRANT", "COMMAND"}:
                    self.assertTrue(record["payload"]["commissioning"])
                    self.assertFalse(record["payload"]["strategy_generated"])
                    self.assertFalse(record["payload"]["scientific_evidence"])
            runtime.stop(); ledger.close()

    def test_exit_submission_cannot_reenter_before_exit_pending_is_recorded(self) -> None:
        now = "2026-08-24T22:10:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.SHORT
            runtime._position = PaperDirection.SHORT
            runtime._position_quantity = -1
            runtime._session_context = context
            runtime._snapshot = PaperRiskSnapshot(
                now, current_position=PaperDirection.SHORT, current_position_quantity=1,
                position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, execution_bridge_healthy=True,
                session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
            )
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            submitted: list[object] = []

            def send(command: object, grant: object) -> None:
                submitted.append((command, grant))
                runtime._request_exit("PROTECTIVE_STOP_REJECTED", emergency=True)

            runtime._persist_and_send = send  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=now):
                runtime._request_exit("OPPOSING_HYPOTHESIS")
            self.assertEqual(len(submitted), 1)
            self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
            runtime.stop(); ledger.close()

    def test_ledger_failure_during_stop_still_stops_heartbeat_and_reaches_stopped(self) -> None:
        """A sealed/unavailable audit writer must not trap process shutdown."""
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                # Model an active paper position without starting a transport
                # or sending a command. This forces the shutdown safety-audit
                # path.
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1

                with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")):
                    runtime.stop()

                self.assertTrue(runtime._heartbeat_stop.is_set())
                self.assertEqual(runtime.state, PaperRuntimeState.STOPPED)
                # A second request is a no-op, not a retry loop against a
                # failed ledger or a resurrection of the heartbeat.
                runtime.stop()
                self.assertEqual(runtime.state, PaperRuntimeState.STOPPED)
                self.assertTrue(runtime._heartbeat_stop.is_set())
            finally:
                ledger.close()

    def test_pending_entry_shutdown_keeps_watchdog_when_ledger_is_unavailable(self) -> None:
        """An owned pending order is active even when the position is flat."""
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.ENTRY_PENDING
                runtime._snapshot = PaperRiskSnapshot(
                    runtime._snapshot.observed_at,
                    working_owned_orders=1,
                    working_entry_orders=1,
                )
                submitted: list[object] = []
                runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]

                with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")):
                    watchdog = runtime.stop()

                self.assertEqual(runtime.state, PaperRuntimeState.STOPPED)
                self.assertTrue(runtime._heartbeat_stop.is_set())
                self.assertTrue(watchdog["required"])
                self.assertFalse(watchdog["flat_confirmed"])
                self.assertEqual(submitted, [])
            finally:
                ledger.close()

    def test_pending_entry_stop_without_transport_reports_unavailable_unconfirmed_watchdog(self) -> None:
        """ENTRY_PENDING is unresolved activity, even before an order snapshot reports it."""
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.ENTRY_PENDING
                self.assertEqual(runtime._position, PaperDirection.FLAT)
                self.assertEqual(runtime._snapshot.working_owned_orders, 0)

                watchdog = runtime.stop()

                self.assertEqual(runtime.state, PaperRuntimeState.STOPPED)
                self.assertTrue(runtime._heartbeat_stop.is_set())
                self.assertTrue(watchdog["required"])
                self.assertFalse(watchdog["flat_confirmed"])
                self.assertFalse(watchdog["watchdog_available"])
                self.assertIsNone(watchdog["durable_confirmation"])
            finally:
                ledger.close()

    def test_foreign_lockout_with_owned_pending_entry_retains_watchdog_callback_path(self) -> None:
        """Foreign activity never makes a still-working owned entry safe to tear down."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                runtime._state = PaperRuntimeState.LOCKED_OUT
                foreign_pending = self.watchdog_flat_reconciliation("foreign-owned-entry-pending")
                foreign_pending.update({
                    "working_order_count": 1,
                    "working_entry_count": 1,
                    "foreign_activity": True,
                })
                runtime.on_execution_message(foreign_pending)

                watchdog = runtime.stop()

                self.assertEqual(runtime.state, PaperRuntimeState.STOPPED)
                self.assertTrue(runtime._heartbeat_stop.is_set())
                self.assertTrue(watchdog["required"])
                self.assertFalse(watchdog["flat_confirmed"])
                self.assertIsNone(watchdog["durable_confirmation"])
                self.assertTrue(runtime._snapshot.foreign_activity)
                self.assertEqual(runtime._snapshot.working_entry_orders, 1)
            finally:
                ledger.close()

    def test_stop_arms_watchdog_before_fast_emergency_flatten_safety_event(self) -> None:
        """A synchronous AddOn SAFETY_EVENT must not race ahead of the latch."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                safety_event_id = "safety-process-stop-fast"

                def immediate_emergency_exit(reason: str, *, emergency: bool = False) -> None:
                    self.assertEqual(reason, "PROCESS_STOP_OPEN_POSITION")
                    self.assertTrue(emergency)
                    # This emulates the signed AddOn callback delivered
                    # synchronously while the command is submitted.
                    self.assertTrue(runtime.watchdog_shutdown_status()["required"])
                    runtime.on_execution_message({
                        "message_type": "SAFETY_EVENT",
                        "receipt_id": safety_event_id,
                        "safety_event_id": safety_event_id,
                        "reason_code": "EMERGENCY_FLATTEN_ACCEPTED",
                        "timestamp": "2026-08-28T14:00:01Z",
                    })

                runtime._request_exit = immediate_emergency_exit  # type: ignore[method-assign]
                watchdog = runtime.stop()

                self.assertTrue(watchdog["required"])
                self.assertEqual(watchdog["safety_event_id"], safety_event_id)
            finally:
                ledger.close()

    def test_generic_post_stop_reconciliation_cannot_clear_watchdog(self) -> None:
        """A pre-watchdog or ordinary flat snapshot is not a shutdown confirmation."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                started = runtime.stop()
                self.assertTrue(started["required"])
                self.assertTrue(started["watchdog_available"])

                runtime.on_execution_message(
                    self.watchdog_flat_reconciliation("generic-post-stop-flat"),
                )

                watchdog = runtime.watchdog_shutdown_status()
                self.assertTrue(watchdog["required"])
                self.assertFalse(watchdog["flat_confirmed"])
                self.assertIsNone(watchdog["safety_event_id"])
                self.assertIsNone(watchdog["durable_confirmation"])
            finally:
                ledger.close()

    def test_correlated_safety_event_and_full_reconciliation_clear_watchdog(self) -> None:
        """Only two increasing AddOn final snapshots release the watchdog."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                runtime.stop()
                safety_event_id = "safety-watchdog-1"
                runtime.on_execution_message({
                    "message_type": "SAFETY_EVENT",
                    "receipt_id": safety_event_id,
                    "safety_event_id": safety_event_id,
                    "reason_code": "HEARTBEAT_TIMEOUT",
                    "timestamp": "2026-08-28T14:00:01Z",
                })
                runtime.on_execution_message(self.watchdog_flat_reconciliation(
                    "safety-correlated-flat-1",
                    safety_event_id=safety_event_id,
                    safety_settlement_final=True,
                    safety_settlement_sequence=1,
                ))

                after_first = runtime.watchdog_shutdown_status()
                self.assertTrue(after_first["required"])
                self.assertFalse(after_first["flat_confirmed"])
                self.assertEqual(after_first["settled_reconciliation_count"], 1)
                runtime.on_execution_message(self.watchdog_flat_reconciliation(
                    "safety-correlated-flat-2",
                    safety_event_id=safety_event_id,
                    safety_settlement_final=True,
                    safety_settlement_sequence=2,
                ))

                watchdog = runtime.watchdog_shutdown_status()
                self.assertFalse(watchdog["required"])
                self.assertTrue(watchdog["flat_confirmed"])
                self.assertTrue(watchdog["durable_confirmation"])
                self.assertEqual(watchdog["safety_event_id"], safety_event_id)
                self.assertEqual(watchdog["settled_reconciliation_count"], 2)
            finally:
                ledger.close()

    def test_correlated_nonfinal_reconciliation_cannot_clear_watchdog(self) -> None:
        """A clean correlation is insufficient until the AddOn marks it settled."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                runtime.stop()
                safety_event_id = "safety-watchdog-nonfinal-1"
                runtime.on_execution_message({
                    "message_type": "SAFETY_EVENT",
                    "receipt_id": safety_event_id,
                    "safety_event_id": safety_event_id,
                    "reason_code": "HEARTBEAT_TIMEOUT",
                    "timestamp": "2026-08-28T14:00:01Z",
                })
                runtime.on_execution_message(self.watchdog_flat_reconciliation(
                    "safety-correlated-nonfinal",
                    safety_event_id=safety_event_id,
                    safety_settlement_final=False,
                    safety_settlement_sequence=1,
                ))

                watchdog = runtime.watchdog_shutdown_status()
                self.assertTrue(watchdog["required"])
                self.assertFalse(watchdog["flat_confirmed"])
                self.assertIsNone(watchdog["durable_confirmation"])
                self.assertEqual(watchdog["settled_reconciliation_count"], 0)
            finally:
                ledger.close()

    def test_safety_fallback_flat_confirmation_is_not_claimed_durable(self) -> None:
        """Authenticated fallback truth may clear the watchdog, never its durable proof."""
        with TemporaryDirectory() as directory:
            ledger, runtime = self.watchdog_runtime(directory)
            try:
                runtime.stop()
                safety_event_id = "safety-watchdog-fallback-1"
                runtime.on_execution_message({
                    "message_type": "SAFETY_EVENT",
                    "receipt_id": safety_event_id,
                    "safety_event_id": safety_event_id,
                    "reason_code": "HEARTBEAT_TIMEOUT",
                    "timestamp": "2026-08-28T14:00:01Z",
                    _DURABILITY_UNAVAILABLE_MARKER: True,
                })
                runtime.on_execution_message(self.watchdog_flat_reconciliation(
                    "safety-fallback-durable-flat-1",
                    safety_event_id=safety_event_id,
                    safety_settlement_final=True,
                    safety_settlement_sequence=1,
                ))
                runtime.on_execution_message(self.watchdog_flat_reconciliation(
                    "safety-fallback-durable-flat-2",
                    safety_event_id=safety_event_id,
                    safety_settlement_final=True,
                    safety_settlement_sequence=2,
                ))

                watchdog = runtime.watchdog_shutdown_status()
                self.assertFalse(watchdog["required"])
                self.assertTrue(watchdog["flat_confirmed"])
                self.assertFalse(watchdog["durable_confirmation"])
                self.assertEqual(watchdog["safety_event_id"], safety_event_id)
                self.assertTrue(watchdog["reconciliation_durable"])
            finally:
                ledger.close()

    def test_exit_ledger_failure_fails_closed_without_phantom_exit_pending(self) -> None:
        """Never send an unrecorded exit when durable exit authority is gone."""
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                submitted: list[object] = []
                runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]

                with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")):
                    runtime._request_exit("LEDGER_UNAVAILABLE_SAFETY_EXIT", emergency=True)

                self.assertEqual(runtime.state, PaperRuntimeState.FAULTED)
                self.assertNotEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertFalse(runtime._exit_submission_in_progress)
                self.assertTrue(runtime._heartbeat_stop.is_set())
                self.assertEqual(submitted, [])
                self.assertEqual(runtime._fault_reason, "EXIT_DURABLE_AUTHORITY_UNAVAILABLE:RuntimeError")
            finally:
                ledger.close()

    def test_expected_protective_cancellation_during_owned_exit_does_not_lock_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.EXIT_PENDING
            runtime._position = PaperDirection.LONG
            runtime._position_quantity = 1
            runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "CANCELLED"})
            self.assertIsNone(runtime.status()["lockout_or_fault_reason"])
            runtime.stop(); ledger.close()

    def test_commissioning_exit_requires_clean_reconciliation_and_persists_fill_pnl(self) -> None:
        now = "2026-08-26T14:00:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime._session_context = context
            runtime._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now, "2026-08-26T19:30:00Z",
            )
            runtime._snapshot = PaperRiskSnapshot(
                now, position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, local_bridge_healthy=True,
                market_price_connected=True, execution_bridge_healthy=True, evidence_warmed=True,
                commissioning_session_warmed=True,
                depth_reset_recovery=False, quote_observed_at=now, classified_trade_observed_at=now,
                depth_mutation_observed_at=now, session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
            )
            submitted: list[object] = []
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]
            runtime._last_quote = (Decimal("100"), Decimal("100.25"), now)
            commissioning_id, commissioning_token = self.reserve_commissioning(runtime, context, now)
            with patch("src.l3g_paper.runtime._now", return_value=now):
                self.assertTrue(runtime.commission_entry(commissioning_id, commissioning_token)["submitted"])
                self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100.25", "quantity": 1,
                    "direction": "LONG", "command_id": submitted[0][0].command_id, "decision_id": submitted[0][0].decision_id,
                    "native_order_id": "entry-order", "native_execution_id": "entry-execution", "timestamp": now,
                })
                runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING"})
                self.assertTrue(runtime.commission_exit()["submitted"])
                exit_command = submitted[1][0]
                self.assertTrue(exit_command.commissioning)
                self.assertFalse(exit_command.strategy_generated)
                self.assertEqual(exit_command.action.value, "EXIT")
                runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "CANCELLED"})
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101", "quantity": 1,
                    "command_id": exit_command.command_id, "native_order_id": "exit-order",
                    "native_execution_id": "exit-execution", "timestamp": now,
                })
                runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": now})
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                self.assertEqual(submitted[2][0].action.value, "RECONCILE")
                runtime.on_execution_message({
                    "message_type": "RECONCILIATION", "receipt_id": "commission-flat", "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                    "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                    "order_snapshot_complete": True, "foreign_activity": False, "timestamp": now,
                })
            closure = next(record["payload"] for record in ledger.recent(10) if record["kind"] == "COMMISSIONING_CLOSURE")
            self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(closure["entry_order_id"], "entry-order")
            self.assertEqual(closure["exit_order_id"], "exit-order")
            self.assertEqual(closure["realized_pnl"], "1.50")
            self.assertEqual(closure["final_working_order_count"], 0)
            self.assertEqual(closure["post_run_verification"]["status"], "PENDING")
            self.assertEqual(closure["final_judgment"], "COMMISSIONING_INCOMPLETE_PENDING_POST_RUN_VERIFICATION")
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.NONE.value)
            runtime.stop(); ledger.close()

    def test_protective_stop_acceptance_and_failure_matrix_is_fail_closed(self) -> None:
        now = "2026-08-26T14:00:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context

        def positioned(directory: str) -> tuple[PaperLedger, LaneIIIPaperRuntime, list[object]]:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.LONG
            runtime._position = PaperDirection.LONG
            runtime._position_quantity = 1
            runtime._entry_fill_price = Decimal("100")
            runtime._entry_fill_quantity = 1
            runtime._entry_direction = PaperDirection.LONG
            runtime._session_context = context
            runtime._snapshot = PaperRiskSnapshot(
                now, current_position=PaperDirection.LONG, current_position_quantity=1,
                position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, execution_bridge_healthy=True,
                session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
            )
            runtime._execution_session_id = lambda: "l3g-es-protective-test"  # type: ignore[method-assign]
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            return ledger, runtime, commands

        with TemporaryDirectory() as directory:
            ledger, runtime, commands = positioned(directory)
            runtime.on_execution_message({
                "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING",
                "account_name": "Sim101", "instrument": "MNQ SEP26", "quantity": 1,
                "native_order_id": "protective-1",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LONG)
            self.assertEqual(runtime.status()["protective_stop_state"], "WORKING")
            self.assertEqual(commands, [])
            runtime.stop(); ledger.close()

        failures = {
            "rejected": {"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "REJECTED"},
            "cancelled": {"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "CANCELLED"},
            "wrong quantity": {"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING", "quantity": 2},
            "wrong account": {"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING", "account_name": "Lucid25kflex01"},
            "wrong instrument": {"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING", "instrument": "NQ SEP26"},
            "missing acknowledgement": {"message_type": "COMMAND_REJECTED", "order_role": "PROTECTIVE", "reason_code": "ACKNOWLEDGEMENT_MISSING"},
        }
        for name, event in failures.items():
            with self.subTest(name=name), TemporaryDirectory() as directory:
                ledger, runtime, commands = positioned(directory)
                with patch("src.l3g_paper.runtime._now", return_value=now):
                    runtime.on_execution_message(event)
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertEqual(len(commands), 1)
                self.assertEqual(commands[0].action.value, "EMERGENCY_FLATTEN")  # type: ignore[attr-defined]
                self.assertTrue(runtime.risk.status()["locked_out"])
                runtime._request_entry(warmed_bullish_policy()[2])
                self.assertEqual(len(commands), 1)
                runtime.stop(); ledger.close()

        with TemporaryDirectory() as directory:
            ledger, runtime, commands = positioned(directory)
            first = {
                "message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING",
                "account_name": "Sim101", "instrument": "MNQ SEP26", "quantity": 1,
                "native_order_id": "protective-1",
            }
            runtime.on_execution_message(first)
            with patch("src.l3g_paper.runtime._now", return_value=now):
                runtime.on_execution_message({**first, "native_order_id": "protective-2"})
            self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
            self.assertEqual(len(commands), 1)
            self.assertEqual(runtime.status()["lockout_or_fault_reason"], "DUPLICATE_PROTECTIVE_STOP")
            runtime.stop(); ledger.close()

    def test_short_exit_pnl_duplicate_and_execution_classification_matrix(self) -> None:
        now = "2026-08-26T14:00:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.EXIT_PENDING
            runtime._position = PaperDirection.SHORT
            runtime._position_quantity = 1
            runtime._entry_fill_price = Decimal("100")
            runtime._entry_fill_quantity = 1
            runtime._entry_direction = PaperDirection.SHORT
            runtime._entry_session_context = context
            runtime._entry_execution = {
                "decision_id": "short-decision", "command_id": "short-entry-command",
                "native_order_id": "short-entry-order", "native_execution_id": "short-entry-fill",
                "price": "100", "quantity": 1, "timestamp": now,
            }
            runtime._session_context = context
            runtime._snapshot = PaperRiskSnapshot(
                now, current_position=PaperDirection.SHORT, current_position_quantity=1,
                position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, execution_bridge_healthy=True,
                session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
            )
            runtime._execution_session_id = lambda: "l3g-es-short-test"  # type: ignore[method-assign]
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            exit_fill = {
                "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "98", "quantity": 1,
                "account_name": "Sim101", "instrument": "MNQ SEP26", "command_id": "short-exit-command",
                "native_order_id": "short-exit-order", "native_execution_id": "short-exit-fill", "timestamp": now,
            }
            with patch("src.l3g_paper.runtime._now", return_value=now):
                runtime.on_execution_message(exit_fill)
                runtime.on_execution_message(exit_fill)
                runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": now})
                runtime.on_execution_message({
                    "message_type": "RECONCILIATION", "receipt_id": "short-flat", "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                    "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                    "order_snapshot_complete": True, "foreign_activity": False, "timestamp": now,
                })
            self.assertEqual(runtime.status()["daily_realized_pnl"], "4")
            self.assertEqual(runtime.status()["current_position"], "FLAT")
            self.assertEqual(runtime.status()["working_owned_orders"], 0)
            self.assertEqual(len(ledger.recent_kinds(("EXECUTION_REALIZED_PNL",))), 1)
            self.assertEqual(len(ledger.recent_kinds(("INCIDENT_DUPLICATE_EXECUTION_CALLBACK",))), 1)
            runtime.stop(); ledger.close()

        classifications = {
            "foreign account": {"account_name": "Lucid25kflex01", "instrument": "MNQ SEP26"},
            "foreign instrument": {"account_name": "Sim101", "instrument": "NQ SEP26"},
        }
        for name, identity in classifications.items():
            with self.subTest(name=name), TemporaryDirectory() as directory:
                ledger = PaperLedger(Path(directory) / "paper.sqlite3")
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.ENTRY_PENDING
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100",
                    "quantity": 1, "direction": "LONG", "native_execution_id": name,
                    **identity,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                self.assertTrue(runtime._snapshot.foreign_activity)
                runtime.stop(); ledger.close()

        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime.on_execution_message({
                "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100",
                "quantity": 1, "direction": "LONG", "native_execution_id": "out-of-order-entry",
                "account_name": "Sim101", "instrument": "MNQ SEP26",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            self.assertEqual(runtime.status()["lockout_or_fault_reason"], "UNEXPECTED_ENTRY_EXECUTION_STATE")
            runtime.stop(); ledger.close()

    def test_ambiguous_restarts_and_unexpected_fills_lock_out(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48157)
            runtime.bind_transport(transport); runtime.start(); runtime.on_execution_bridge_state("AUTHENTICATED")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "ambiguous", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 1,
                "working_order_count": 1, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": True, "timestamp": "2026-08-24T14:00:00Z",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            runtime.stop(); ledger.close()

        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48156)
            runtime.bind_transport(transport); runtime.start(); self.reconcile_flat(runtime)
            runtime._transition(PaperRuntimeState.ARMED_FLAT, "TEST_ARM")
            runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_ENTRY_SENT")
            runtime.on_execution_message({"message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100", "quantity": 1, "direction": "LONG"})
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            runtime.stop(); ledger.close()


if __name__ == "__main__":
    unittest.main()
