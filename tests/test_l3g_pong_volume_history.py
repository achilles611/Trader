"""TEST ONLY accelerated provider-history fixtures; no authentic market data."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.pong_volume_history import PongVolumeReferenceHistory, HISTORY_TYPE, HISTORY_POLICY
from src.l3g_paper.pong_production import PongProductionHost
from tests.test_l3g_pong_inputs import KEY, verified
from tests.test_l3g_pong_production import pending_collection, runtime_snapshot

def payload(**changes):
    bars = [{"start": f"2026-09-{day:02d}T14:05:00Z", "end": f"2026-09-{day:02d}T14:10:00Z", "volume": 100 + day}
            for day in (8, 9, 10)]
    result = {"contract_id": "MNQ SEP26", "canonical_contract": "MNQU6", "status": "COMPLETE",
              "source": "NINJATRADER_BARS_REQUEST_PROVIDER", "merge_policy": "DO_NOT_MERGE",
              "trading_hours": "Default 24 x 7", "timezone": "UTC", "bar_period_minutes": 5,
              "requested_bars_back": 2016, "request_id": "TEST-ONLY-history-request",
              "requested_at": "2026-09-14T14:01:00Z", "chunk_index": 0, "chunk_count": 1,
              "total_bars": len(bars), "bars": bars, "publication_policy": HISTORY_POLICY}
    return dict(result, **changes)

def record(p=None, index=1):
    return verified(HISTORY_TYPE, payload() if p is None else p, local_sequence=index,
                    observed="2026-09-14T14:01:01Z")

class VolumeHistoryTests(unittest.TestCase):
    def test_index_rechecks_cutoff_error_and_replaced_receipt(self):
        from dataclasses import replace
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(), received_at='2026-09-14T14:01:02Z')
            at = datetime(2026, 9, 14, 14, 5, tzinfo=timezone.utc)
            self.assertEqual(len(loader.prior(at)), 3)
            self.assertEqual(loader.prior(at - timedelta(days=1)), ())
            loader.windows = tuple(replace(w, available_at='2026-09-15T00:00:00Z') for w in loader.windows)
            self.assertEqual(loader.prior(at), ())
            self.assertEqual(len(loader.prior(at + timedelta(days=1))), 3)
            loader.error = 'TEST ONLY failed new receipt'
            self.assertEqual(loader.prior(at + timedelta(days=1)), ())

    def test_production_uses_same_callback_quote_without_later_quote_lookahead(self):
        """TEST ONLY native-shaped UNKNOWN trade with an authenticated quote ref."""
        with TemporaryDirectory() as root:
            host = PongProductionHost(root, selected_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {})
            q = verified("QUOTE", {"contract_id": "MNQ SEP26", "bid": 100, "ask": 100.25,
                "bid_size": 2, "ask_size": 3, "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1"}, local_sequence=1)
            t = verified("TRADE", {"contract_id": "MNQ SEP26", "price": 100.25, "size": 1,
                "aggressor_side": "UNKNOWN", "aggressor_source": "BID_ASK_CLASSIFICATION",
                "bid_at_trade": 100, "ask_at_trade": 100.25, "derivation_quote_observation_id": q.observation_id,
                "source_event_sequence": 1, "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1"}, local_sequence=2)
            host._consume_observation(q)
            host._consume_observation(t)
            self.assertEqual(host._trades[-1].aggressor_side.value, "BUY")
            self.assertEqual(host._trades[-1].classification_provenance.value, "AUTHENTICATED_DERIVED")
            self.assertEqual(len(host._classified_hour), 1)
            # A later quote cannot alter the captured classification of this trade.
            q2 = verified("QUOTE", dict(q.payload, bid=101, ask=101.25, source_event_sequence=2), local_sequence=3)
            host._consume_observation(q2)
            self.assertEqual(host._trades[-1].aggressor_side.value, "BUY")

    def make(self, root):
        return PongVolumeReferenceHistory(Path(root) / "history.json", root_key=KEY,
                                          expected_source=expected_observer_source_fingerprint())

    def test_complete_provider_history_restores_and_is_prior_only(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(), received_at="2026-09-14T14:01:02Z")
            self.assertIsNone(loader.error)
            self.assertEqual(len(loader.windows), 3)
            self.assertEqual(loader.prior(datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc)), ())
            self.assertEqual(len(loader.prior(datetime(2026, 9, 14, 14, 5, tzinfo=timezone.utc))), 3)
            self.assertEqual(self.make(root).windows, loader.windows)
            self.assertEqual(loader.status("2026-09-14T14:05:01Z")["authentic_order_flow_authority"], "DENIED")

    def test_backdating_receipt_or_tampering_native_history_cannot_restore(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(), received_at="2026-09-14T14:01:02Z")
            value = json.loads(loader.path.read_text())
            value["received_at"] = "2026-09-10T14:01:02Z"
            loader.path.write_text(json.dumps(value))
            restored = self.make(root)
            self.assertEqual(restored.windows, ())
            self.assertIn("RECEIPT_AUTHENTICATION", restored.error)

    def test_source_incompatible_receipt_is_retained_but_never_restored_or_admitted(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(), received_at="2026-09-14T14:01:02Z")
            original = loader.path.read_bytes()
            upgraded = PongVolumeReferenceHistory(
                loader.path,
                root_key=KEY,
                expected_source="f" * 64,
            )
            self.assertEqual(upgraded.windows, ())
            self.assertIn(
                "PONG_VOLUME_HISTORY_RESTORE_REFUSED:AUTH_FAILED:"
                "ninjatrader_source_fingerprint",
                upgraded.error,
            )
            self.assertEqual(loader.path.read_bytes(), original)

            upgraded.admit(record(), received_at="2026-09-14T14:01:03Z")
            self.assertEqual(upgraded.windows, ())
            self.assertIn(
                "PONG_VOLUME_HISTORY_ADMISSION_REFUSED:AUTH_FAILED:"
                "ninjatrader_source_fingerprint",
                upgraded.error,
            )
            self.assertEqual(loader.path.read_bytes(), original)

    def test_invalid_contract_policy_volume_future_alignment_and_completeness_refused(self):
        variants = [payload(contract_id="MNQ DEC26"), payload(canonical_contract="MNQZ6"),
                    payload(merge_policy="MERGE_BACK_ADJUSTED"), payload(timezone="America/Chicago"),
                    payload(requested_bars_back=3), payload(total_bars=4), payload(status="PARTIAL"),
                    payload(chunk_index=1), payload(bar_period_minutes=1)]
        for change in ({"volume": -1}, {"volume": 1.5}, {"start": "2026-09-08T14:06:00Z"},
                       {"end": "2026-09-15T14:10:00Z"}):
            p = payload()
            p["bars"][0].update(change)
            variants.append(p)
        for p in variants:
            with self.subTest(p=p), TemporaryDirectory() as root:
                loader = self.make(root)
                loader.admit(record(p), received_at="2026-09-14T14:01:02Z")
                self.assertEqual(loader.windows, ())
                self.assertIsNotNone(loader.error)
                self.assertFalse(loader.path.exists())

    def test_missing_chunk_does_not_publish_a_partial_reference(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(payload(chunk_count=2)), received_at="2026-09-14T14:01:02Z")
            self.assertEqual(loader.windows, ())
            self.assertFalse(loader.path.exists())
            self.assertEqual(self.make(root).windows, ())

    def test_full_chunk_sequence_admits_and_duplicate_bar_refused(self):
        start = datetime(2026, 9, 8, tzinfo=timezone.utc)
        bars = [{"start": (start + timedelta(minutes=5*i)).isoformat(),
                 "end": (start + timedelta(minutes=5*(i+1))).isoformat(), "volume": i + 1} for i in range(129)]
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(payload(chunk_count=2, total_bars=129, bars=bars[:128])), received_at="2026-09-14T14:01:02Z")
            self.assertEqual(loader.windows, ())
            loader.admit(record(payload(chunk_count=2, chunk_index=1, total_bars=129, bars=bars[128:]), 2),
                         received_at="2026-09-14T14:01:03Z")
            self.assertEqual(len(loader.windows), 129)
        with TemporaryDirectory() as root:
            loader = self.make(root)
            loader.admit(record(payload(total_bars=2, bars=[bars[0], bars[0]])), received_at="2026-09-14T14:01:02Z")
            self.assertEqual(loader.windows, ())

    def test_host_wires_loader_without_qualifying_order_flow(self):
        with TemporaryDirectory() as root:
            host = PongProductionHost(root, selected_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {}, history_verification_key=KEY)
            with patch("src.l3g_paper.pong_production._now", return_value="2026-09-14T14:05:01Z"):
                host._consume_observation(record())
                status = host.status()
                self.assertEqual(status["volume_reference"]["loaded_windows"], 3)
                # Receipt time is now 14:05:01, after this five-minute slot's start.
                self.assertFalse(status["volume_reference"]["ready_for_current_slot"])
                self.assertIsNone(status["qualified_hour_certificate"])
                self.assertFalse(status["operational_start_ready"])
