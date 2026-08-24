"""Closed-market checks for the L3-F3 live-shadow bridge.

All observations in this module are SYNTHETIC fixtures.  They are deliberately
not evidence of live or provider-verified commissioning.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.lane_iii.contracts import EvidenceFamily
from src.lane_iii.market_data import DataQuality
from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker
from src.l3f_provider.ninjatrader_observation import (
    LoopbackBridgeConfig,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from src.l3f_provider.shadow_runtime import (
    LANE_III_SHADOW_MODE,
    LaneIIIShadowRuntime,
    ShadowExecutionDenied,
)
from src.l3f_provider.tradovate_observation import ProviderErrorCode, StreamHealth


BASE = datetime(2026, 8, 20, 14, 30, tzinfo=timezone.utc)
CONTRACT_ID = "MNQ SEPT26"


def timestamp(offset_seconds: int) -> str:
    return (BASE + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def observation(
    kind: str,
    sequence: int,
    *,
    event_offset: int = 0,
    receipt_offset: int | None = None,
    provider_sequence: int | None = None,
    session: str = "market-session-1",
    contract_id: str = CONTRACT_ID,
    payload: dict[str, object] | None = None,
    exchange_timestamp: str | None = None,
) -> NinjaTraderObservation:
    if payload is None:
        payload = {
            "QUOTE": {"contract_id": contract_id, "bid": "20000.00", "ask": "20000.25", "bid_size": 2, "ask_size": 3},
            "TRADE": {"contract_id": contract_id, "price": "20000.25", "size": 1, "aggressor_side": "BUY"},
            "DEPTH": {
                "contract_id": contract_id,
                "bids": [{"price": "20000.00", "size": 4}],
                "asks": [{"price": "20000.25", "size": 5}],
            },
            "INSTRUMENT": {"contract_id": contract_id},
        }[kind]
    event_time = timestamp(event_offset)
    return NinjaTraderObservation(
        observation_id=f"synthetic-{session}-{kind}-{sequence}-{event_offset}",
        session_id=session,
        observation_type=kind,
        ninja_receipt_time=timestamp(event_offset if receipt_offset is None else receipt_offset),
        local_monotonic_sequence=sequence,
        payload=payload,
        provider_timestamp=event_time,
        provider_sequence=sequence if provider_sequence is None else provider_sequence,
        exchange_timestamp=exchange_timestamp,
    )


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class LaneIIILiveShadowTests(unittest.TestCase):
    def feed_healthy_triplet(self, runtime: LaneIIIShadowRuntime, *, offset: int = 0, session: str = "market-session-1") -> None:
        runtime.ingest(observation("QUOTE", 1, event_offset=offset, session=session))
        runtime.ingest(observation("DEPTH", 2, event_offset=offset + 1, session=session))
        runtime.ingest(observation("TRADE", 3, event_offset=offset + 2, session=session))

    def test_empty_stream_is_healthy_and_cannot_emit_a_shadow_decision(self) -> None:
        status = LaneIIIShadowRuntime().status()
        self.assertEqual(status["counters"]["raw_accepted_observations"], 0)
        self.assertEqual(status["counters"]["shadow_decisions_emitted"], 0)
        self.assertEqual(status["counters"]["execution_attempts"], 0)

    def test_exact_shadow_path_emits_auditable_no_trade_without_execution(self) -> None:
        runtime = LaneIIIShadowRuntime()
        self.feed_healthy_triplet(runtime)

        status = runtime.status()
        self.assertEqual(status["mode"], LANE_III_SHADOW_MODE)
        self.assertEqual(status["contract"]["canonical"]["contract_symbol"], "MNQU6")
        self.assertEqual(status["authority"]["execution"], "DENIED")
        self.assertEqual(status["authority"]["live_capital"], "DENIED")
        self.assertEqual(status["counters"]["raw_accepted_observations"], 3)
        self.assertEqual(status["counters"]["normalized_admitted_market_events"], 3)
        self.assertEqual(status["counters"]["downstream_interpretation_invocations"], 3)
        self.assertEqual(status["counters"]["shadow_decision_evaluations"], 1)
        self.assertEqual(status["counters"]["shadow_decisions_emitted"], 1)
        self.assertEqual(status["counters"]["shadow_directional_actions"], 0)
        self.assertEqual(status["counters"]["execution_attempts"], 0)
        decision = next(item for item in runtime.audit_records() if item["kind"] == "SHADOW_DECISION_EMITTED")
        for field in (
            "timestamp", "instrument", "canonical_event_id", "market_state_hash",
            "l3c_snapshot_hash", "interpreter_identity", "decision_id",
            "decision_identity", "hypothetical_action", "reason_code", "execution_authority",
        ):
            self.assertIn(field, decision)
        self.assertEqual(decision["hypothetical_action"], "NO_TRADE")
        self.assertEqual(decision["execution_authority"], "DENIED")

    def test_quote_trade_and_depth_only_are_suppressed(self) -> None:
        for kind in ("QUOTE", "TRADE", "DEPTH"):
            with self.subTest(kind=kind):
                runtime = LaneIIIShadowRuntime()
                runtime.ingest(observation(kind, 1))
                counters = runtime.status()["counters"]
                self.assertEqual(counters["shadow_decision_evaluations"], 0)
                self.assertGreaterEqual(counters["decisions_suppressed"], 1)
                self.assertEqual(counters["execution_attempts"], 0)

    def test_stale_backward_and_large_gap_events_fail_closed(self) -> None:
        runtime = LaneIIIShadowRuntime()
        runtime.ingest(observation("QUOTE", 1, event_offset=0))
        runtime.ingest(observation("QUOTE", 2, event_offset=1, receipt_offset=12))
        runtime.ingest(observation("QUOTE", 3, event_offset=-1))
        runtime.ingest(observation("QUOTE", 4, event_offset=11))

        status = runtime.status()
        self.assertGreaterEqual(status["counters"]["stale_events"], 3)
        self.assertGreaterEqual(status["counters"]["state_resets"], 1)
        reasons = {item.get("reason_code") for item in runtime.audit_records()}
        self.assertTrue({"STALE_PROVIDER_EVENT", "TIMESTAMP_MOVED_BACKWARD", "LARGE_TIMESTAMP_GAP"}.issubset(reasons))
        self.assertEqual(status["counters"]["execution_attempts"], 0)

    def test_duplicate_partial_depth_and_wrong_contract_never_become_market_state(self) -> None:
        runtime = LaneIIIShadowRuntime()
        first = observation("QUOTE", 1)
        runtime.ingest(first)
        runtime.ingest(first)
        runtime.ingest(observation("DEPTH", 2, payload={"contract_id": CONTRACT_ID, "bids": [], "asks": [{"price": "20000.25", "size": 1}]}))
        runtime.ingest(observation("QUOTE", 3, contract_id="NQ SEPT26"))
        runtime.ingest(observation("INSTRUMENT", 4, contract_id="MNQ DEC26"))

        status = runtime.status()
        self.assertEqual(status["counters"]["normalized_admitted_market_events"], 1)
        self.assertGreaterEqual(status["counters"]["duplicate_events"], 1)
        self.assertGreaterEqual(status["counters"]["malformed_events"], 1)
        reasons = {item.get("reason_code") for item in runtime.audit_records()}
        self.assertTrue({"DUPLICATE_OBSERVATION", "PARTIAL_DEPTH_SNAPSHOT", "CONTRACT_NOT_FOUND"}.issubset(reasons))

    def test_receiver_rejection_and_duplicate_are_counted_without_state(self) -> None:
        runtime = LaneIIIShadowRuntime()
        runtime.record_raw_rejection(NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "synthetic"))
        runtime.record_raw_duplicate()
        status = runtime.status()
        self.assertEqual(status["counters"]["raw_rejected_observations"], 1)
        self.assertEqual(status["counters"]["duplicate_events"], 1)
        self.assertEqual(status["counters"]["normalized_admitted_market_events"], 0)

    def test_disconnect_rebuilds_state_and_recovery_has_one_consumer(self) -> None:
        runtime = LaneIIIShadowRuntime()
        self.feed_healthy_triplet(runtime)
        first_generation = runtime.status()["state_generation"]
        runtime.on_transport_state(StreamHealth.DISCONNECTED)
        runtime.on_transport_state(StreamHealth.HEALTHY)
        self.feed_healthy_triplet(runtime, offset=10, session="market-session-2")
        status = runtime.status()
        self.assertGreater(status["state_generation"], first_generation)
        self.assertEqual(status["transport_state"], "HEALTHY")
        self.assertEqual(status["pipeline"]["events_processed"], 3)
        self.assertEqual(status["counters"]["transport_disconnects"], 1)
        self.assertEqual(status["counters"]["execution_attempts"], 0)

    def test_market_session_transition_discards_prior_state(self) -> None:
        runtime = LaneIIIShadowRuntime()
        runtime.ingest(observation("QUOTE", 1, event_offset=0, session="market-session-1"))
        runtime.ingest(observation("QUOTE", 2, event_offset=1, session="market-session-2"))
        status = runtime.status()
        self.assertEqual(status["pipeline"]["events_processed"], 1)
        self.assertGreaterEqual(status["counters"]["state_resets"], 1)
        self.assertIn("MARKET_SESSION_BOUNDARY", {item.get("reason_code") for item in runtime.audit_records()})

    def test_provider_price_feed_disconnect_discards_state_before_reconnect(self) -> None:
        runtime = LaneIIIShadowRuntime()
        self.feed_healthy_triplet(runtime)
        first_generation = runtime.status()["state_generation"]
        runtime.ingest(observation("CONNECTION", 4, event_offset=3, payload={
            "scope": "MARKET_DATA", "price_status": "ConnectionLost",
        }))
        status = runtime.status()
        self.assertGreater(status["state_generation"], first_generation)
        self.assertEqual(status["provider_price_state"], "CONNECTIONLOST")
        self.assertEqual(status["pipeline"]["events_processed"], 0)
        self.assertIn("PROVIDER_PRICE_FEED_NOT_CONNECTED", {item.get("reason_code") for item in runtime.audit_records()})
        runtime.ingest(observation("CONNECTION", 5, event_offset=4, payload={
            "scope": "MARKET_DATA", "price_status": "Connected",
        }))
        self.assertEqual(runtime.status()["provider_price_state"], "CONNECTED")
        self.assertEqual(runtime.status()["counters"]["execution_attempts"], 0)

    def test_provider_depth_reset_discards_all_downstream_state(self) -> None:
        runtime = LaneIIIShadowRuntime()
        self.feed_healthy_triplet(runtime)
        first_generation = runtime.status()["state_generation"]
        runtime.ingest(observation("DEPTH", 4, event_offset=3, payload={
            "contract_id": CONTRACT_ID,
            "bids": [],
            "asks": [],
            "operation": "Update",
            "side": "Bid",
            "mutation_price": "20000.00",
            "mutation_volume": 0,
            "mutation_position": 0,
            "is_reset": True,
        }))
        status = runtime.status()
        self.assertGreater(status["state_generation"], first_generation)
        self.assertEqual(status["pipeline"]["events_processed"], 0)
        self.assertIsNone(runtime.pipeline.latest_quote)
        self.assertIsNone(runtime.pipeline.latest_trade)
        self.assertIn("PROVIDER_DEPTH_RESET", {item.get("reason_code") for item in runtime.audit_records()})
        self.assertEqual(status["counters"]["execution_attempts"], 0)

    def test_downstream_exception_discards_state_and_suppresses(self) -> None:
        runtime = LaneIIIShadowRuntime()
        with patch.object(runtime.engine, "observe", side_effect=RuntimeError("synthetic interpreter fault")):
            runtime.ingest(observation("QUOTE", 1))
        status = runtime.status()
        self.assertEqual(status["counters"]["downstream_interpretation_failures"], 1)
        self.assertEqual(status["pipeline"]["events_processed"], 0)
        self.assertEqual(status["counters"]["execution_attempts"], 0)
        self.assertIn("DOWNSTREAM_EXCEPTION", {item.get("reason_code") for item in runtime.audit_records()})

    def test_forced_execution_handoff_is_denied_before_any_attempt(self) -> None:
        runtime = LaneIIIShadowRuntime()
        with self.assertRaises(ShadowExecutionDenied):
            runtime.execution_guard.deny({"hypothetical": "LONG"})
        counters = runtime.status()["counters"]
        self.assertEqual(counters["execution_attempts"], 0)
        self.assertEqual(counters["execution_denials"], 1)
        self.assertIn("EXECUTION_HARD_DENIAL", {item["kind"] for item in runtime.audit_records()})

    def test_exchange_timestamp_is_preserved_as_canonical_ordering_time(self) -> None:
        runtime = LaneIIIShadowRuntime()
        runtime.ingest(observation("QUOTE", 1, event_offset=0, exchange_timestamp=timestamp(1)))
        quote = runtime.pipeline.latest_quote
        assert quote is not None
        self.assertEqual(quote.header.timestamps.exchange_time, timestamp(1))
        self.assertEqual(quote.header.timestamps.ordering_time, timestamp(1))

    def test_live_shadow_counts_explicit_aggressor_provenance(self) -> None:
        runtime = LaneIIIShadowRuntime()
        runtime.ingest(observation("TRADE", 1, payload={
            "contract_id": CONTRACT_ID, "price": "20000.25", "size": 1,
            "aggressor_side": "BUY", "aggressor_source": "PROVIDER_NATIVE",
        }))
        runtime.ingest(observation("TRADE", 2, event_offset=1, payload={
            "contract_id": CONTRACT_ID, "price": "20000.25", "size": 1,
            "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
        }))
        quote = observation("QUOTE", 3, event_offset=2)
        runtime.ingest(quote)
        runtime.ingest(observation("TRADE", 4, event_offset=2, provider_sequence=3, payload={
            "contract_id": CONTRACT_ID, "price": "20000.25", "size": 1,
            "aggressor_side": "UNKNOWN", "aggressor_source": "BID_ASK_CLASSIFICATION",
            "bid_at_trade": "20000.00", "ask_at_trade": "20000.25",
            "derivation_quote_observation_id": quote.observation_id,
        }))
        counters = runtime.status()["counters"]
        self.assertEqual(counters["trade_aggressor_provider_native"], 1)
        self.assertEqual(counters["trade_aggressor_quote_derived"], 1)
        self.assertEqual(counters["trade_aggressor_unknown"], 1)
        self.assertEqual(counters["execution_attempts"], 0)

    def test_public_boundary_keeps_quote_derived_flow_and_depth_incomplete_without_provider_sequence(self) -> None:
        runtime = LaneIIIShadowRuntime()
        # Public NinjaTrader trade/depth callbacks have no authoritative
        # provider sequence.  The same-callback quote may truthfully classify
        # side, but neither stream may become directional L3-C evidence.
        runtime.ingest(replace(observation("DEPTH", 1), provider_sequence=None))
        for index in range(3):
            quote = replace(
                observation("QUOTE", index * 2 + 2, event_offset=index + 1),
                provider_sequence=None,
            )
            runtime.ingest(quote)
            runtime.ingest(replace(
                observation("TRADE", index * 2 + 3, event_offset=index + 1, payload={
                    "contract_id": CONTRACT_ID,
                    "price": "20000.25",
                    "size": 1,
                    "aggressor_side": "UNKNOWN",
                    "aggressor_source": "BID_ASK_CLASSIFICATION",
                    "bid_at_trade": "20000.00",
                    "ask_at_trade": "20000.25",
                    "derivation_quote_observation_id": quote.observation_id,
                }),
                provider_sequence=None,
            ))

        snapshot = runtime.engine.snapshot()
        families = {item.evidence.family for item in snapshot.evidence}
        status = runtime.status()
        rejected_families = {item.family for item in snapshot.rejected_observations}
        self.assertNotIn(EvidenceFamily.ORDER_FLOW, families)
        self.assertNotIn(EvidenceFamily.RESTING_LIQUIDITY, families)
        self.assertIn(EvidenceFamily.RESTING_LIQUIDITY, rejected_families)
        self.assertEqual(runtime.pipeline.latest_trade_quality, DataQuality.INCOMPLETE)
        self.assertEqual(runtime.pipeline.latest_quote_quality, DataQuality.INCOMPLETE)
        self.assertEqual(runtime.pipeline.book._state().quality, DataQuality.INCOMPLETE)
        self.assertFalse(runtime.pipeline.trade_flow.measurements().complete)
        self.assertEqual(status["counters"]["trade_aggressor_quote_derived"], 3)
        self.assertEqual(status["counters"]["shadow_decision_evaluations"], 0)
        self.assertEqual(status["counters"]["execution_attempts"], 0)

    def test_replay_is_deterministic_and_explicitly_not_live_commissioning(self) -> None:
        def replay() -> tuple[object, ...]:
            runtime = LaneIIIShadowRuntime()
            self.feed_healthy_triplet(runtime)
            decision = next(item for item in runtime.audit_records() if item["kind"] == "SHADOW_DECISION_EMITTED")
            return decision["decision_id"], decision["market_state_hash"], decision["l3c_snapshot_hash"], decision["hypothetical_action"]

        self.assertEqual(replay(), replay())

    def test_normal_fastapi_lifespan_routes_receiver_to_shadow_runtime(self) -> None:
        port = free_port()
        worker = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
        with tempfile.TemporaryDirectory() as directory:
            config = replace(CopyTradeConfig(), artifacts=replace(
                CopyTradeConfig().artifacts, database_path=Path(directory) / "copytrade.sqlite3",
            ))
            app = create_control_center_app(config, ninjatrader_listener_factory=lambda: worker)

            async def exercise() -> None:
                async with app.router.lifespan_context(app):
                    wire = LaneIIIShadowRuntime._wire_payload(observation("QUOTE", 1))
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5) as client:
                        client.sendall(json.dumps(wire).encode("utf-8") + b"\n")
                    await asyncio.sleep(0.10)
                    shadow = app.state.lane_iii_shadow
                    self.assertIsInstance(shadow, LaneIIIShadowRuntime)
                    self.assertEqual(shadow.status()["counters"]["raw_accepted_observations"], 1)
                    self.assertEqual(worker.status().start_attempts, 1)

            asyncio.run(exercise())
        self.assertEqual(worker.status().state, "STOPPED")

    def test_shadow_bridge_has_no_execution_or_lane_ii_import_path(self) -> None:
        source = (Path(__file__).parents[1] / "src" / "l3f_provider" / "shadow_runtime.py").read_text(encoding="utf-8")
        self.assertNotRegex(source, r"(?:from|import)\s+src\.lane_ii(?:\s|\.)")
        self.assertNotIn("simulated_execution", source)
        self.assertNotIn("hyperliquid", source.lower())
        self.assertNotIn("tradovate_execution", source.lower())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
