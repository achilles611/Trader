import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import (
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
    NinjaTraderProducerAttestation,
)
from src.l3g_paper.pong_collection import PongAuthenticHourCollector
from src.lane_iii.contracts import canonical_hash
from tools.pong_receiver_comparison import (
    bounded_timing_report,
    new_bounded_timing,
    record_bounded_timing,
)


def observation(sequence: int) -> NinjaTraderObservation:
    at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload = {
        "contract_id": "MNQ SEP26",
        "bid": 100,
        "ask": 100.25,
        "bid_size": 1,
        "ask_size": 1,
        "source_event_sequence": sequence,
        "source_generation": 1,
        "observer_queue_overflow_frames": 0,
        "observer_transport_failed_frames": 0,
        "publication_policy": "EVENT_DRIVEN_EVERY_BID_ASK_CALLBACK_V1",
    }
    producer = NinjaTraderProducerAttestation(
        protocol_version=NINJATRADER_ATTESTATION_PROTOCOL,
        source_fingerprint=EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
        build_fingerprint="b" * 64,
        build_timestamp="2026-09-21T00:00:00Z",
        source_stream="MARKET_DATA_BID_ASK",
        publication_policy=str(payload["publication_policy"]),
    )
    return NinjaTraderObservation(
        observation_id=f"asia-repair-{sequence}",
        session_id="asia-repair-test",
        observation_type="QUOTE",
        ninja_receipt_time=at,
        local_monotonic_sequence=sequence,
        provider_timestamp=at,
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


class AsiaRepairReceiverInstrumentationTests(unittest.TestCase):
    def test_collector_reports_constant_memory_stage_distributions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = PongAuthenticHourCollector(Path(directory))
            with patch(
                "src.l3g_paper.pong_collection.PongAuthenticHourCollector._STATUS_INTERVAL_SECONDS",
                60.0,
            ):
                collector.start()
                for sequence in range(1, 4):
                    collector.ingest(observation(sequence))

                deadline = time.monotonic() + 3.0
                while (
                    collector.status()["durability"]["written_records"] < 3
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                status = collector.stop()

        durability = status["durability"]
        timing = durability["bounded_timing"]
        self.assertEqual(durability["enqueued_records"], 3)
        self.assertEqual(durability["written_records"], 3)
        self.assertEqual(durability["durable_records"], 3)
        for name in (
            "observation_enqueue_service",
            "record_encode_and_hash",
            "journal_buffer_write",
            "collector_state_apply",
        ):
            self.assertEqual(timing[name]["count"], 3, name)
        self.assertGreaterEqual(timing["queue_wait"]["count"], 4)
        self.assertGreaterEqual(timing["journal_flush"]["count"], 1)
        self.assertEqual(
            timing["journal_flush"]["count"],
            timing["journal_fsync"]["count"],
        )
        # Initial, STOP checkpoint and final STOPPED publication only. A fresh
        # collector must not perform a redundant status fsync after record one.
        self.assertEqual(timing["status_persist"]["count"], 3)
        for stage in timing.values():
            self.assertEqual(stage["count"], sum(stage["histogram_counts"]))
            self.assertEqual(
                len(stage["histogram_counts"]),
                len(stage["histogram_upper_bounds_milliseconds"]),
            )

    def test_sender_distribution_is_bounded_and_reports_upper_bound_quantiles(self) -> None:
        timing = new_bounded_timing()
        for elapsed in (0.00001, 0.0002, 0.004, 0.2, 7.0):
            record_bounded_timing(timing, elapsed)
        report = bounded_timing_report(timing)

        self.assertEqual(report["count"], 5)
        self.assertEqual(sum(report["histogram_counts"]), 5)
        self.assertEqual(
            len(report["histogram_counts"]),
            len(report["histogram_upper_bounds_milliseconds"]),
        )
        self.assertEqual(report["p50_milliseconds"], 5.0)
        self.assertEqual(report["p95_milliseconds"], 10_000.0)
        self.assertEqual(report["p99_milliseconds"], 10_000.0)
        self.assertEqual(report["maximum_milliseconds"], 7000.0)


if __name__ == "__main__":
    unittest.main()
