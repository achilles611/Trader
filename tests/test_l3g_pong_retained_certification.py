from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.pong_collection import PongAuthenticHourCollector
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_inputs import (
    PONG_HOURLY_WARM_START_POLICY_VERSION,
    hourly_warm_start_policy_hash,
)
from tests.test_l3g_pong_inputs import KEY, verified
from tools.certify_pong_retained_hour import certify, completed_hour


def wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class RetainedPongCertificationTests(unittest.TestCase):
    def test_accelerated_fixture_certifies_only_hash_verified_eligible_hour(self) -> None:
        """TEST ONLY: this accelerated fixture is not authentic market history."""
        with TemporaryDirectory(prefix="pong-retained-test-") as directory:
            root = Path(directory)
            start = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
            collector = PongAuthenticHourCollector(
                root / "collection",
                now=start - timedelta(minutes=5),
                topology=DEC26_TOPOLOGY,
            )
            collector.collector_root.mkdir(parents=True)
            collector._transport_state = "HEALTHY"
            records = []
            sequence = 1
            for hour_start, values in (
                (start - timedelta(hours=1), (100, 105, 95, 101, 1000)),
                (start, (101, 110, 100, 109, 1200)),
            ):
                observed = hour_start + timedelta(hours=1, seconds=1)
                payload = {
                    "start": wire(hour_start),
                    "end": wire(hour_start + timedelta(hours=1)),
                    "open": values[0], "high": values[1], "low": values[2],
                    "close": values[3], "volume": values[4],
                    "contract_id": DEC26_TOPOLOGY.native_instrument,
                    "source": "NINJATRADER_BARS_REQUEST_PROVIDER",
                    "policy_version": PONG_HOURLY_WARM_START_POLICY_VERSION,
                    "policy_hash": hourly_warm_start_policy_hash(DEC26_TOPOLOGY),
                    "publication_policy": "CAUSAL_LIVE_COMPLETED_HOUR_V1",
                }
                records.append(verified(
                    "HOURLY_BAR", payload, local_sequence=sequence,
                    observed=wire(observed),
                ))
                sequence += 1
            event_at = start + timedelta(minutes=1)
            quote = verified("QUOTE", {
                "contract_id": DEC26_TOPOLOGY.native_instrument, "bid": 100, "ask": 101,
                "bid_size": 2, "ask_size": 3, "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
            }, local_sequence=sequence, observed=wire(event_at))
            sequence += 1
            depth = verified("DEPTH", {
                "contract_id": DEC26_TOPOLOGY.native_instrument,
                "bids": [{"price": 100, "size": 2}],
                "asks": [{"price": 101, "size": 3}],
                "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
            }, local_sequence=sequence, observed=wire(event_at))
            sequence += 1
            market = [quote, depth]
            for trade_sequence in range(1, 9):
                market.append(verified("TRADE", {
                    "contract_id": DEC26_TOPOLOGY.native_instrument, "price": 101, "size": 2,
                    "aggressor_side": "BUY", "aggressor_source": "PROVIDER_NATIVE",
                    "bid_at_trade": 100, "ask_at_trade": 101,
                    "derivation_quote_observation_id": quote.observation_id,
                    "source_event_sequence": trade_sequence,
                    "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
                }, local_sequence=sequence, observed=wire(
                    event_at + timedelta(milliseconds=trade_sequence),
                )))
                sequence += 1
            records = [records[0], *market, records[1]]
            with collector.journal_path.open("wb") as handle:
                for record in records:
                    line = collector._observation_line(record, record.ninja_receipt_time)
                    handle.write(line)
                    collector._written_records += 1
                    collector._journal_bytes += len(line)
                    if record is records[-1]:
                        collector._advance_intervals_locked(
                            start + timedelta(hours=1, seconds=1),
                        )
                    collector._apply_observation_locked(record)
            collector._durable_records = collector._written_records
            collector._state = "STOPPED"
            collector._last_fsync_at = wire(start + timedelta(hours=1, seconds=2))
            collector._advance_intervals_locked(start + timedelta(hours=1, seconds=2))
            interval = collector._intervals[wire(start)]
            interval_status = collector._interval_status_locked(
                interval, start + timedelta(hours=1, seconds=2),
            )
            self.assertTrue(interval_status["eligible"], interval_status)
            status = {
                "state": "STOPPED",
                "collector": {"collector_id": collector.collector_id},
                "instrument": {
                    "native": DEC26_TOPOLOGY.native_instrument,
                    "canonical": DEC26_TOPOLOGY.canonical_contract,
                    "topology_version": DEC26_TOPOLOGY.version,
                    "topology_hash": DEC26_TOPOLOGY.binding_hash,
                    "state_namespace": DEC26_TOPOLOGY.state_namespace,
                },
                "producer": collector._identity_payload(collector._active_identity),
                "durability": {
                    "journal_path": str(collector.journal_path),
                    "written_records": collector._written_records,
                    "durable_records": collector._durable_records,
                    "journal_bytes": collector._journal_bytes,
                    "journal_tip_sha256": collector._journal_tip,
                    "queue_overflow_records": 0,
                    "rejected_records": 0,
                    "duplicate_records": 0,
                },
                "required_interval": interval_status,
                "recent_completed_intervals": [interval_status],
            }
            collector.collector_status_path.write_text(
                json.dumps(status), encoding="utf-8",
            )
            result = certify(
                collector.root, root / "state", root_key=KEY, test_only=True,
            )
            self.assertEqual(result["result"], "TEST_ONLY_CERTIFIED")
            self.assertEqual(result["journal_verified_records"], len(records))
            self.assertEqual(result["classified_trades"], 8)
            certificate = json.loads(
                Path(result["certificate_path"]).read_text(encoding="utf-8")
            )
            self.assertTrue(certificate["test_only"])
            self.assertEqual(
                certificate["daily_bias"]["instrument"],
                DEC26_TOPOLOGY.native_instrument,
            )
            self.assertEqual(
                certificate["topology"]["binding_hash"],
                DEC26_TOPOLOGY.binding_hash,
            )
            self.assertEqual(certificate["required_interval"]["start_utc"], wire(start))
            mismatched_status = dict(status)
            mismatched_status["instrument"] = {
                **status["instrument"],
                "native": "MNQ SEP26",
            }
            collector.collector_status_path.write_text(
                json.dumps(mismatched_status), encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "RETAINED_TOPOLOGY_INVALID"):
                certify(
                    collector.root, root / "mismatched-state",
                    root_key=KEY, test_only=True,
                )
            collector.collector_status_path.write_text(
                json.dumps(status), encoding="utf-8",
            )
            lines = collector.journal_path.read_text(encoding="utf-8").splitlines()
            tampered = json.loads(lines[0])
            tampered["received_by_collector_at"] = wire(start + timedelta(seconds=9))
            lines[0] = json.dumps(tampered, sort_keys=True, separators=(",", ":"))
            collector.journal_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "JOURNAL_CHAIN_INVALID"):
                certify(
                    collector.root, root / "tampered-state",
                    root_key=KEY, test_only=True,
                )

    def test_dec26_hour_refuses_the_wrong_policy_identity(self) -> None:
        start = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
        observed = start + timedelta(hours=1, seconds=1)
        payload = {
            "start": wire(start),
            "end": wire(start + timedelta(hours=1)),
            "open": 100, "high": 105, "low": 95, "close": 101,
            "volume": 1000,
            "contract_id": DEC26_TOPOLOGY.native_instrument,
            "source": "NINJATRADER_BARS_REQUEST_PROVIDER",
            "policy_version": PONG_HOURLY_WARM_START_POLICY_VERSION,
            "policy_hash": hourly_warm_start_policy_hash(DEC26_TOPOLOGY),
            "publication_policy": "CAUSAL_LIVE_COMPLETED_HOUR_V1",
        }
        for sequence, wrong_identity in enumerate((
            {"policy_hash": hourly_warm_start_policy_hash()},
            {"policy_version": "wrong-policy-version"},
        ), start=1):
            with self.subTest(wrong_identity=wrong_identity):
                record = verified(
                    "HOURLY_BAR", {**payload, **wrong_identity},
                    local_sequence=sequence, observed=wire(observed),
                )
                with self.assertRaisesRegex(RuntimeError, "HOURLY_PROVENANCE_INVALID"):
                    completed_hour(
                        json.loads(record.signed_wire_bytes()),
                        continuity_domain="NINJATRADER_HOURLY:test:source:build",
                        topology=DEC26_TOPOLOGY,
                    )


if __name__ == "__main__":
    unittest.main()
