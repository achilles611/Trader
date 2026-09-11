from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.pong_production import PongProductionHost
from tests.test_l3g_pong_inputs import verified, warm_record


def runtime_snapshot() -> dict[str, object]:
    return {
        "observed_at": "2026-09-11T18:30:00Z",
        "entry_profile_version": PONG_ENTRY_PROFILE_VERSION,
        "runtime_state": "READY_DISARMED",
        "runtime_active": True,
        "entry_fence_active": True,
        "evidence_sealed": False,
        "entries_paused": False,
        "position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "unresolved_execution": False,
        "reconciliation_current": True,
        "protection_current": True,
        "safety_lockout": False,
        "lockout_reason": None,
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "available_loss_allowance_dollars": "200",
        "live_authority": "DISARMED",
        "execution_session_id": "l3g-es-" + "1" * 32,
        "addon_protocol_version": "l3g-paper-addon-provenance-v1",
        "addon_source_fingerprint": "d" * 64,
        "addon_build_fingerprint": "b" * 64,
        "addon_provenance_valid": True,
    }


def pending_collection() -> dict[str, object]:
    return {
        "state": "COLLECTING",
        "producer": None,
        "durability": {"journal_path": "missing", "journal_tip_sha256": None},
        "required_interval": {
            "start_utc": "2026-09-11T19:00:00Z",
            "end_utc": "2026-09-11T20:00:00Z",
            "eligible": False,
            "blockers": ["HOURLY_INTERVAL_INCOMPLETE"],
        },
    }


class PongProductionHostTests(unittest.TestCase):
    def test_lifespan_host_owns_prepared_controller_without_entry_authority(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
                activation_authorized=True,
            )
            host.start()
            deadline = time.monotonic() + 3
            while host.status()["controller"] is None and time.monotonic() < deadline:
                time.sleep(0.05)
            status = host.status()
            host.stop()
            self.assertEqual(status["registration"], "PRODUCTION_REGISTERED")
            self.assertEqual(status["controller"]["phase"], "PREPARED")
            self.assertFalse(status["entry_authority_ready"])
            self.assertIn("PONG_AUTHENTIC_HOUR_NOT_QUALIFIED", status["blocking_reasons"])
            self.assertEqual(status["maximum_quantity"], 1)
            self.assertEqual(status["live_authority"], "DISARMED")

    def test_native_window_preserves_stream_domains_and_exposes_real_gap(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
            )
            host.on_observation(warm_record())
            base = {
                "contract_id": "MNQ SEP26", "price": 100, "size": 1,
                "aggressor_side": "BUY", "aggressor_source": "NATIVE_BID_ASK",
                "source_event_sequence": 100,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }
            host.on_observation(verified("TRADE", base, local_sequence=2))
            quote = {
                "contract_id": "MNQ SEP26", "bid": 99.75, "ask": 100,
                "bid_size": 1, "ask_size": 1, "source_event_sequence": 500,
                "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
            }
            host.on_observation(verified("QUOTE", quote, local_sequence=3))
            host.on_observation(verified(
                "TRADE", dict(base, source_event_sequence=102), local_sequence=4,
            ))
            status = host.status()
            self.assertEqual(status["stream_counts"]["TRADE"], 1)
            self.assertEqual(status["stream_counts"]["QUOTE"], 1)
            self.assertEqual(status["missing_events"]["TRADE"], 1)
            self.assertEqual(status["missing_events"]["QUOTE"], 0)
            self.assertEqual(status["input_error"], "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP")

    def test_non_price_bearing_depth_shift_is_retained_without_latching_input_error(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
            )
            host.on_observation(warm_record())
            depth = {
                "contract_id": "MNQ SEP26",
                "side": "Ask",
                "mutation_price": 0,
                "mutation_volume": 0,
                "is_reset": False,
                "bids": [{"price": 99.75, "size": 1}],
                "asks": [{"price": 100, "size": 1}],
                "source_event_sequence": 700,
                "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
            }
            host.on_observation(verified("DEPTH", depth, local_sequence=2))
            status = host.status()
            self.assertEqual(status["stream_counts"]["DEPTH"], 1)
            self.assertEqual(status["missing_events"]["DEPTH"], 0)
            self.assertIsNone(status["input_error"])


if __name__ == "__main__":
    unittest.main()
