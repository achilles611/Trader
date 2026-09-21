"""TEST ONLY accelerated provider-history fixtures; no authentic market data."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.pong_volume_history import (
    HISTORY_BARS_BACK,
    HISTORY_CHUNK_COUNT,
    HISTORY_CHUNK_SIZE,
    HISTORY_POLICY,
    HISTORY_TYPE,
    PongVolumeReferenceHistory,
)
from src.l3g_paper.pong_production import PongProductionHost
from tests.test_l3g_pong_inputs import KEY, verified
from tests.test_l3g_pong_production import pending_collection, runtime_snapshot

REQUESTED_AT = datetime(2026, 9, 14, 14, 1, tzinfo=timezone.utc)


def history_bars(count=HISTORY_BARS_BACK):
    start = REQUESTED_AT.replace(second=0, microsecond=0) - timedelta(
        minutes=5 * count + 1,
    )
    return [
        {
            "start": (start + timedelta(minutes=5 * index)).isoformat(),
            "end": (start + timedelta(minutes=5 * (index + 1))).isoformat(),
            "volume": index + 1,
        }
        for index in range(count)
    ]


def payload(*, bars=None, **changes):
    bars = history_bars() if bars is None else bars
    result = {"contract_id": "MNQ SEP26", "canonical_contract": "MNQU6", "status": "COMPLETE",
              "source": "NINJATRADER_BARS_REQUEST_PROVIDER", "merge_policy": "DO_NOT_MERGE",
              "trading_hours": "Default 24 x 7", "timezone": "UTC", "bar_period_minutes": 5,
              "requested_bars_back": 2016, "request_id": "TEST-ONLY-history-request",
              "requested_at": "2026-09-14T14:01:00Z", "chunk_index": 0,
              "chunk_count": HISTORY_CHUNK_COUNT,
              "total_bars": len(bars), "bars": bars, "publication_policy": HISTORY_POLICY}
    return dict(result, **changes)

def record(p=None, index=1):
    return verified(HISTORY_TYPE, payload() if p is None else p, local_sequence=index,
                    observed="2026-09-14T14:01:01Z")


def complete_payloads(*, bars=None, **changes):
    bars = history_bars() if bars is None else bars
    chunk_count = (len(bars) + HISTORY_CHUNK_SIZE - 1) // HISTORY_CHUNK_SIZE
    common = dict(changes)
    common.setdefault("total_bars", len(bars))
    common.setdefault("chunk_count", chunk_count)
    return [
        payload(
            bars=bars[index * HISTORY_CHUNK_SIZE:(index + 1) * HISTORY_CHUNK_SIZE],
            chunk_index=index,
            **common,
        )
        for index in range(chunk_count)
    ]


def admit_complete(loader, *, received_at="2026-09-14T14:01:02Z", bars=None, **changes):
    for index, item in enumerate(complete_payloads(bars=bars, **changes), 1):
        loader.admit(record(item, index), received_at=received_at)

class VolumeHistoryTests(unittest.TestCase):
    def test_index_rechecks_cutoff_error_and_replaced_receipt(self):
        from dataclasses import replace
        with TemporaryDirectory() as root:
            loader = self.make(root)
            admit_complete(loader)
            at = datetime(2026, 9, 14, 14, 5, tzinfo=timezone.utc)
            self.assertEqual(len(loader.prior(at)), 7)
            self.assertEqual(loader.prior(at - timedelta(days=1)), ())
            loader.windows = tuple(replace(w, available_at='2026-09-15T00:00:00Z') for w in loader.windows)
            self.assertEqual(loader.prior(at), ())
            self.assertEqual(len(loader.prior(at + timedelta(days=1))), 7)
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
            admit_complete(loader)
            self.assertIsNone(loader.error)
            self.assertEqual(len(loader.windows), HISTORY_BARS_BACK)
            self.assertEqual(loader.prior(datetime(2026, 9, 14, 14, 0, tzinfo=timezone.utc)), ())
            self.assertEqual(len(loader.prior(datetime(2026, 9, 14, 14, 5, tzinfo=timezone.utc))), 7)
            self.assertEqual(self.make(root).windows, loader.windows)
            self.assertEqual(loader.status("2026-09-14T14:05:01Z")["authentic_order_flow_authority"], "DENIED")

    def test_backdating_receipt_or_tampering_native_history_cannot_restore(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            admit_complete(loader)
            value = json.loads(loader.path.read_text())
            value["received_at"] = "2026-09-10T14:01:02Z"
            loader.path.write_text(json.dumps(value))
            restored = self.make(root)
            self.assertEqual(restored.windows, ())
            self.assertIn("RECEIPT_AUTHENTICATION", restored.error)

    def test_source_incompatible_receipt_is_retained_but_never_restored_or_admitted(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            admit_complete(loader)
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

            upgraded.admit(
                record(complete_payloads()[0]),
                received_at="2026-09-14T14:01:03Z",
            )
            self.assertEqual(upgraded.windows, ())
            self.assertIn(
                "PONG_VOLUME_HISTORY_ADMISSION_REFUSED:AUTH_FAILED:"
                "ninjatrader_source_fingerprint",
                upgraded.error,
            )
            self.assertEqual(loader.path.read_bytes(), original)

    def test_invalid_contract_policy_volume_future_alignment_and_completeness_refused(self):
        variants = [
            {"contract_id": "MNQ DEC26"},
            {"canonical_contract": "MNQZ6"},
            {"merge_policy": "MERGE_BACK_ADJUSTED"},
            {"timezone": "America/Chicago"},
            {"requested_bars_back": 3},
            {"total_bars": 4},
            {"status": "PARTIAL"},
            {"bar_period_minutes": 1},
        ]
        for change in ({"volume": -1}, {"volume": 1.5}, {"start": "2026-09-08T14:06:00Z"},
                       {"end": "2026-09-15T14:10:00Z"}):
            bars = history_bars()
            bars[0].update(change)
            variants.append({"bars": bars})
        for changes in variants:
            with self.subTest(changes=changes), TemporaryDirectory() as root:
                loader = self.make(root)
                bars = changes.get("bars")
                overrides = {key: value for key, value in changes.items() if key != "bars"}
                admit_complete(loader, bars=bars, **overrides)
                self.assertEqual(loader.windows, ())
                self.assertIsNotNone(loader.error)
                self.assertFalse(loader.path.exists())

    def test_missing_chunk_does_not_publish_a_partial_reference(self):
        with TemporaryDirectory() as root:
            loader = self.make(root)
            first = complete_payloads()[0]
            loader.admit(record(first), received_at="2026-09-14T14:01:02Z")
            self.assertEqual(loader.windows, ())
            self.assertFalse(loader.path.exists())
            self.assertEqual(loader.status("2026-09-14T14:05:01Z")["pending_chunks"], 1)
            self.assertEqual(self.make(root).windows, ())

    def test_exact_full_chunk_sequence_admits_and_partial_or_duplicate_refused(self):
        bars = history_bars()
        with TemporaryDirectory() as root:
            loader = self.make(root)
            admit_complete(loader, bars=bars)
            self.assertEqual(len(loader.windows), HISTORY_BARS_BACK)
        with TemporaryDirectory() as root:
            loader = self.make(root)
            admit_complete(loader, bars=bars[:-1])
            self.assertEqual(loader.windows, ())
            self.assertIn("BARS_SHAPE", loader.error)
        with TemporaryDirectory() as root:
            loader = self.make(root)
            duplicated = list(bars)
            duplicated[1] = duplicated[0]
            admit_complete(loader, bars=duplicated)
            self.assertEqual(loader.windows, ())

    def test_host_wires_loader_without_qualifying_order_flow(self):
        with TemporaryDirectory() as root:
            host = PongProductionHost(root, selected_profile="BEELZEBUB_PONG_DISCERNMENT_V1",
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {}, history_verification_key=KEY)
            with patch("src.l3g_paper.pong_production._now", return_value="2026-09-14T14:05:01Z"):
                for index, item in enumerate(complete_payloads(), 1):
                    host._consume_observation(record(item, index))
                status = host.status()
                self.assertEqual(status["volume_reference"]["loaded_windows"], HISTORY_BARS_BACK)
                # Receipt time is now 14:05:01, after this five-minute slot's start.
                self.assertFalse(status["volume_reference"]["ready_for_current_slot"])
                self.assertIsNone(status["qualified_hour_certificate"])
                self.assertFalse(status["operational_start_ready"])
