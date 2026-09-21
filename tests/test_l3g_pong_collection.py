import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.lane_iii.contracts import canonical_hash
from src.l3f_provider.ninjatrader_observation import (
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
    NinjaTraderProducerAttestation,
)
from src.l3g_paper.pong_collection import PongAuthenticHourCollector


def wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def record(
    kind: str,
    payload: dict[str, object],
    sequence: int,
    observed: datetime,
    *,
    session_id: str = "native-test-session",
) -> NinjaTraderObservation:
    producer = NinjaTraderProducerAttestation(
        protocol_version=NINJATRADER_ATTESTATION_PROTOCOL,
        source_fingerprint=EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
        build_fingerprint="b" * 64,
        build_timestamp="2026-09-11T13:30:00Z",
        source_stream={"TRADE": "MARKET_DATA_LAST", "QUOTE": "MARKET_DATA_BID_ASK", "DEPTH": "MARKET_DEPTH"}[kind],
        publication_policy=str(payload["publication_policy"]),
    )
    return NinjaTraderObservation(
        observation_id=f"native-{kind}-{sequence}",
        session_id=session_id,
        observation_type=kind,
        ninja_receipt_time=wire(observed),
        local_monotonic_sequence=sequence,
        provider_timestamp=wire(observed),
        provider_sequence=None,
        exchange_timestamp=None,
        account_alias=None,
        account_class=None,
        payload=payload,
        schema=L3F3_SCHEMA,
        payload_hash=canonical_hash(payload),
        producer=producer,
        signature="a" * 64,
        signature_verified=True,
    )


class PongAuthenticHourCollectorTests(unittest.TestCase):
    def test_test_only_generation_change_invalidates_without_fake_sequence_inversion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 9, 14, 14, 55, tzinfo=timezone.utc)
            collector = PongAuthenticHourCollector(Path(directory), now=now)
            at = now.replace(hour=15, minute=1)
            payload = {"contract_id": "MNQ SEP26", "price": 100, "size": 1,
                       "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
                       "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1"}
            with patch("src.l3g_paper.pong_collection._utc_now", return_value=at):
                collector._apply_observation_locked(record("TRADE", dict(payload, source_event_sequence=1, source_generation=1), 1, at))
                collector._apply_observation_locked(record("TRADE", dict(payload, source_event_sequence=2, source_generation=2,
                    observer_queue_overflow_frames=3, observer_transport_failed_frames=7), 2, at))
                state = collector.status()
            self.assertIn("SOURCE_GENERATION_CHANGED", state["required_interval"]["blockers"])
            self.assertEqual(state["required_interval"]["streams"]["TRADE"]["duplicate_or_out_of_order"], 0)
            self.assertEqual(state["durability"]["producer_queue_overflow_records"], 3)
            self.assertEqual(state["durability"]["producer_queue_overflow_baseline"], 3)
            self.assertEqual(state["durability"]["producer_queue_overflow_delta"], 0)
            self.assertEqual(state["durability"]["producer_transport_failed_records"], 7)
            self.assertEqual(state["durability"]["producer_transport_failed_baseline"], 7)
            self.assertEqual(state["durability"]["producer_transport_failed_delta"], 0)
            self.assertEqual(state["durability"]["queue_overflow_records"], 0)

    def test_producer_counters_use_same_session_baseline_and_fail_on_delta_or_regression(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 9, 14, 14, 55, tzinfo=timezone.utc)
            collector = PongAuthenticHourCollector(Path(directory), now=now)
            at = now.replace(hour=15, minute=1)
            base = {
                "contract_id": "MNQ SEP26",
                "price": 100,
                "size": 1,
                "aggressor_side": "UNKNOWN",
                "aggressor_source": "UNKNOWN",
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
                "source_generation": 1,
            }
            with patch("src.l3g_paper.pong_collection._utc_now", return_value=at):
                collector._apply_observation_locked(record(
                    "TRADE",
                    dict(base, source_event_sequence=1,
                         observer_queue_overflow_frames=13,
                         observer_transport_failed_frames=21),
                    1,
                    at,
                ))
                collector._apply_observation_locked(record(
                    "TRADE",
                    dict(base, source_event_sequence=2,
                         observer_queue_overflow_frames=13,
                         observer_transport_failed_frames=21),
                    2,
                    at + timedelta(milliseconds=1),
                ))
                stable = collector.status()
                self.assertNotIn(
                    "PRODUCER_TRANSPORT_FAILED",
                    stable["required_interval"]["blockers"],
                )
                self.assertEqual(
                    stable["durability"]["producer_transport_failed_delta"], 0,
                )

                collector._apply_observation_locked(record(
                    "TRADE",
                    dict(base, source_event_sequence=3,
                         observer_queue_overflow_frames=14,
                         observer_transport_failed_frames=22),
                    3,
                    at + timedelta(milliseconds=2),
                ))
                increased = collector.status()
                self.assertIn(
                    "PRODUCER_TRANSPORT_FAILED",
                    increased["required_interval"]["blockers"],
                )
                self.assertEqual(
                    increased["durability"]["producer_transport_failed_delta"], 1,
                )
                self.assertEqual(
                    increased["durability"]["producer_queue_overflow_delta"], 1,
                )

                collector._apply_observation_locked(record(
                    "TRADE",
                    dict(base, source_event_sequence=4,
                         observer_queue_overflow_frames=12,
                         observer_transport_failed_frames=22),
                    4,
                    at + timedelta(milliseconds=3),
                ))
                regressed = collector.status()
                self.assertIn(
                    "PRODUCER_COUNTER_REGRESSION",
                    regressed["required_interval"]["blockers"],
                )

                collector._apply_observation_locked(record(
                    "TRADE",
                    dict(base, source_event_sequence=1,
                         observer_queue_overflow_frames=40,
                         observer_transport_failed_frames=60),
                    5,
                    at + timedelta(milliseconds=4),
                    session_id="replacement-native-session",
                ))
                replaced = collector.status()
                self.assertIn(
                    "PRODUCER_IDENTITY_CHANGED",
                    replaced["required_interval"]["blockers"],
                )
                self.assertEqual(
                    replaced["durability"]["producer_queue_overflow_baseline"], 40,
                )
                self.assertEqual(
                    replaced["durability"]["producer_queue_overflow_records"], 40,
                )
                self.assertEqual(
                    replaced["durability"]["producer_queue_overflow_delta"], 0,
                )
                self.assertEqual(
                    replaced["durability"]["producer_transport_failed_baseline"], 60,
                )
                self.assertEqual(
                    replaced["durability"]["producer_transport_failed_records"], 60,
                )
                self.assertEqual(
                    replaced["durability"]["producer_transport_failed_delta"], 0,
                )

    def test_transient_windows_status_reader_contention_does_not_fail_collector(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = PongAuthenticHourCollector(Path(directory))
            original_replace = __import__("os").replace
            attempts = 0

            def transient_replace(source: object, destination: object) -> None:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError(5, "Access is denied", str(destination))
                original_replace(source, destination)

            with patch(
                "src.l3g_paper.pong_collection.os.replace",
                side_effect=transient_replace,
            ):
                started = collector.start()
                stopped = collector.stop()

            self.assertEqual(started["state"], "COLLECTING")
            self.assertEqual(stopped["state"], "STOPPED")
            self.assertGreaterEqual(attempts, 3)

    def test_test_only_hour_boundary_tracks_each_native_stream_independently(self) -> None:
        """Accelerated fixture only; these records are not authentic history."""
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 9, 11, 17, 55, tzinfo=timezone.utc)
            collector = PongAuthenticHourCollector(Path(directory), now=now)
            started = collector.start()
            collector.on_transport_state("HEALTHY")
            target = datetime.fromisoformat(
                started["required_interval"]["start_utc"].replace("Z", "+00:00")
            )
            payloads = {
                "QUOTE": {
                    "contract_id": "MNQ SEP26", "bid": 100, "ask": 101,
                    "bid_size": 2, "ask_size": 3,
                    "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
                },
                "TRADE": {
                    "contract_id": "MNQ SEP26", "price": 101, "size": 1,
                    "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
                    "bid_at_trade": None, "ask_at_trade": None,
                    "derivation_quote_observation_id": None,
                    "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
                },
                "DEPTH": {
                    "contract_id": "MNQ SEP26",
                    "bids": [{"price": 100, "size": 2}],
                    "asks": [{"price": 101, "size": 3}],
                    "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
                },
            }
            native_sequences = {"QUOTE": 7, "TRADE": 40, "DEPTH": 99}
            local_sequence = 0
            for kind in ("TRADE", "QUOTE", "DEPTH"):
                local_sequence += 1
                payload = dict(payloads[kind], source_event_sequence=native_sequences[kind])
                collector.ingest(record(kind, payload, local_sequence, target - timedelta(microseconds=1)))
            for kind in ("QUOTE", "TRADE", "DEPTH", "TRADE", "QUOTE", "DEPTH"):
                local_sequence += 1
                native_sequences[kind] += 1
                payload = dict(payloads[kind], source_event_sequence=native_sequences[kind])
                collector.ingest(record(kind, payload, local_sequence, target + timedelta(seconds=1)))
            deadline = time.monotonic() + 2
            while collector.status()["durability"]["written_records"] < 9 and time.monotonic() < deadline:
                time.sleep(0.02)
            status = collector.stop()["required_interval"]
            self.assertEqual(status["streams"]["TRADE"]["source_sequence_gaps"], 0)
            self.assertEqual(status["streams"]["QUOTE"]["source_sequence_gaps"], 0)
            self.assertEqual(status["streams"]["DEPTH"]["source_sequence_gaps"], 0)
            self.assertNotIn("SOURCE_SEQUENCE_GAP", status["blockers"])
            self.assertNotIn("SOURCE_SEQUENCE_DUPLICATE_OR_OUT_OF_ORDER", status["blockers"])
            self.assertEqual(status["streams"]["TRADE"]["records"], 2)
            self.assertEqual(status["streams"]["QUOTE"]["records"], 2)
            self.assertEqual(status["streams"]["DEPTH"]["records"], 2)

    def test_verified_events_are_chained_durable_and_classified_causally(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            now = datetime.now(timezone.utc)
            collector = PongAuthenticHourCollector(
                Path(directory), now=now, independent_market_collection=True,
            )
            started = collector.start()
            self.assertEqual(
                started["independence"]["market_fanout"], "PONG_ONLY",
            )
            self.assertFalse(
                started["independence"]["perpetual_operational_session_required"],
            )
            target = datetime.fromisoformat(started["required_interval"]["start_utc"].replace("Z", "+00:00"))
            event_at = target + timedelta(seconds=1)
            collector.on_transport_state("HEALTHY")
            quote = {
                "contract_id": "MNQ SEP26", "bid": 100, "ask": 101,
                "bid_size": 2, "ask_size": 3, "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
            }
            collector.ingest(record("QUOTE", quote, 1, event_at))
            trade = {
                "contract_id": "MNQ SEP26", "price": 101, "size": 2,
                "aggressor_side": "UNKNOWN", "aggressor_source": "BID_ASK_CLASSIFICATION",
                "bid_at_trade": 100, "ask_at_trade": 101,
                "derivation_quote_observation_id": "native-QUOTE-1",
                "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }
            collector.ingest(record("TRADE", trade, 2, event_at))
            depth = {
                "contract_id": "MNQ SEP26",
                "bids": [{"price": 100, "size": 2}],
                "asks": [{"price": 101, "size": 3}],
                "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
            }
            collector.ingest(record("DEPTH", depth, 3, event_at))
            deadline = time.monotonic() + 3
            while collector.status()["durability"]["durable_records"] < 3 and time.monotonic() < deadline:
                time.sleep(0.02)
            status = collector.stop()
            self.assertEqual(status["durability"]["durable_records"], 3)
            self.assertGreaterEqual(status["durability"]["queue_high_water_records"], 1)
            self.assertGreaterEqual(status["durability"]["last_queue_age_milliseconds"], 0)
            self.assertGreaterEqual(
                status["durability"]["maximum_queue_age_milliseconds"],
                status["durability"]["last_queue_age_milliseconds"],
            )
            self.assertEqual(status["required_interval"]["classified_trades"]["BUY"], 1)
            self.assertEqual(status["required_interval"]["depth"]["available"], 1)
            rows = [json.loads(line) for line in collector.journal_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["previous_record_hash"], "0" * 64)
            self.assertEqual(rows[1]["previous_record_hash"], rows[0]["record_hash"])
            self.assertEqual(rows[2]["previous_record_hash"], rows[1]["record_hash"])
            self.assertEqual(rows[1]["aggressor_classification"]["side"], "BUY")
            self.assertTrue(rows[1]["signature_verified"])
            for row in rows:
                digest = row.pop("record_hash")
                self.assertEqual(digest, canonical_hash(row))

    def test_source_sequence_gap_invalidates_only_the_affected_hour(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = PongAuthenticHourCollector(Path(directory))
            started = collector.start()
            target = datetime.fromisoformat(started["required_interval"]["start_utc"].replace("Z", "+00:00"))
            payload = {
                "contract_id": "MNQ SEP26", "price": 101, "size": 1,
                "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
                "bid_at_trade": None, "ask_at_trade": None,
                "derivation_quote_observation_id": None,
                "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }
            collector.ingest(record("TRADE", payload, 1, target + timedelta(seconds=1)))
            collector.ingest(record("TRADE", dict(payload, source_event_sequence=3), 2, target + timedelta(seconds=2)))
            deadline = time.monotonic() + 2
            while collector.status()["durability"]["written_records"] < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            status = collector.stop()
            self.assertIn("SOURCE_SEQUENCE_GAP", status["required_interval"]["blockers"])
            self.assertFalse(status["required_interval"]["continuity_valid"])


if __name__ == "__main__":
    unittest.main()
