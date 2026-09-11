import hashlib
import hmac
import json
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3f_provider.ninjatrader_observation import (
    L3F3_SCHEMA, NINJATRADER_ATTESTATION_KEY_CONTEXT,
    NINJATRADER_ATTESTATION_PROTOCOL, LoopbackNinjaTraderBridge,
    expected_observer_source_fingerprint,
)
from src.l3g_paper.pong_inputs import (
    PONG_HISTORICAL_ORDER_FLOW_STATUS, PONG_HOURLY_WARM_START_POLICY_HASH,
    PONG_HOURLY_WARM_START_POLICY_VERSION, PongCausalInputAdapter,
    PongNativeInputRefused, PongWarmStart,
)


KEY = b"pong-input-test-root-key-material-32bytes!!!!"


def verified(kind, payload, *, local_sequence=1, observed="2026-09-11T12:30:00Z"):
    stream = {
        "HISTORICAL_WARM_START": "HISTORICAL_BARS", "HOURLY_BAR": "HISTORICAL_BARS",
        "TRADE": "MARKET_DATA_LAST", "QUOTE": "MARKET_DATA_BID_ASK", "DEPTH": "MARKET_DEPTH",
    }[kind]
    value = {
        "schema": L3F3_SCHEMA, "observation_id": f"native-{kind}-{local_sequence}",
        "session_id": "native-session", "observation_type": kind,
        "ninja_receipt_time": observed, "local_monotonic_sequence": local_sequence,
        "provider_timestamp": observed, "provider_sequence": None,
        "exchange_timestamp": None, "account": None, "payload": payload,
        "payload_hash": canonical_hash(payload),
        "producer": {
            "protocol_version": NINJATRADER_ATTESTATION_PROTOCOL,
            "source_fingerprint": expected_observer_source_fingerprint(),
            "build_fingerprint": "b" * 64, "build_timestamp": "2026-09-11T12:00:00Z",
            "source_stream": stream, "publication_policy": payload["publication_policy"],
        },
    }
    derived = hmac.new(KEY, NINJATRADER_ATTESTATION_KEY_CONTEXT, hashlib.sha256).digest()
    value["signature"] = hmac.new(
        derived, json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(), hashlib.sha256,
    ).hexdigest()
    bridge = LoopbackNinjaTraderBridge(attestation_key=KEY, require_attestation=True)
    return bridge.accept_observation(json.dumps(value).encode())


def warm_record():
    payload = {
        "contract_id": "MNQ SEP26", "status": "COMPLETE",
        "source": "NINJATRADER_BARS_REQUEST_PROVIDER",
        "requested_at": "2026-09-11T12:30:00Z",
        "earliest_required": "2026-09-11T10:00:00Z",
        "latest_included_timestamp": "2026-09-11T12:00:00Z",
        "expected_intervals": 2, "actual_intervals": 2, "gaps": [],
        "timezone": "UTC", "day_anchor": "2026-09-11",
        "policy_version": PONG_HOURLY_WARM_START_POLICY_VERSION,
        "policy_hash": PONG_HOURLY_WARM_START_POLICY_HASH,
        "bars": [
            {"start": "2026-09-11T10:00:00Z", "end": "2026-09-11T11:00:00Z", "open": 100, "high": 110, "low": 90, "close": 101, "volume": 10},
            {"start": "2026-09-11T11:00:00Z", "end": "2026-09-11T12:00:00Z", "open": 101, "high": 111, "low": 91, "close": 102, "volume": 11},
        ],
        "publication_policy": "CAUSAL_HISTORICAL_COMPLETED_HOUR_V1",
    }
    return verified("HISTORICAL_WARM_START", payload)


class PongInputTests(unittest.TestCase):
    def test_authenticated_completed_hours_seed_then_live_without_overlap(self):
        warm = PongWarmStart.from_observation(warm_record())
        self.assertTrue(warm.coverage.complete)
        self.assertEqual(warm.coverage.payload()["historical_order_flow"], PONG_HISTORICAL_ORDER_FLOW_STATUS)
        adapter = PongCausalInputAdapter(warm)
        payload = {
            "start": "2026-09-11T12:00:00Z", "end": "2026-09-11T13:00:00Z",
            "open": 102, "high": 112, "low": 92, "close": 103, "volume": 12,
            "contract_id": "MNQ SEP26", "source": "NINJATRADER_BARS_REQUEST_PROVIDER",
            "policy_version": PONG_HOURLY_WARM_START_POLICY_VERSION,
            "policy_hash": PONG_HOURLY_WARM_START_POLICY_HASH,
            "publication_policy": "CAUSAL_LIVE_COMPLETED_HOUR_V1",
        }
        hour = adapter.admit_live_hour(verified("HOURLY_BAR", payload, local_sequence=2, observed="2026-09-11T13:00:01Z"))
        self.assertEqual(hour.start, "2026-09-11T12:00:00Z")
        previous, current = adapter.latest_hour_pair()
        self.assertEqual(previous.end, current.start)

    def test_market_source_sequences_are_exact_and_gaps_fail_closed(self):
        adapter = PongCausalInputAdapter(PongWarmStart.from_observation(warm_record()))
        base = {
            "contract_id": "MNQ SEP26", "price": 100, "size": 1,
            "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
            "source_event_sequence": 1,
            "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
        }
        self.assertTrue(adapter.admit_market_observation(verified("TRADE", base, local_sequence=2)))
        gap = dict(base, source_event_sequence=3)
        with self.assertRaisesRegex(PongNativeInputRefused, "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP"):
            adapter.admit_market_observation(verified("TRADE", gap, local_sequence=3))

    def test_unsigned_or_incomplete_warm_start_never_admits(self):
        record = warm_record()
        object.__setattr__(record, "signature_verified", None)
        with self.assertRaisesRegex(PongNativeInputRefused, "ATTESTATION_UNVERIFIED"):
            PongWarmStart.from_observation(record)


if __name__ == "__main__":
    unittest.main()
