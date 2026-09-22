"""Offline refusal diagnostics; all frames and signing material are TEST ONLY."""

import hashlib
import hmac
import json
import unittest
from unittest.mock import Mock

from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness
from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    NINJATRADER_ATTESTATION_KEY_CONTEXT,
    LoopbackNinjaTraderBridge,
    NinjaTraderObservation,
    NinjaTraderObservationError,
    NinjaTraderSessionLedger,
    ProviderErrorCode,
)
from tests.test_l3f_ninjatrader_observation import ROOT_KEY, signed_record


SESSION_A = "a" * 32
SESSION_B = "b" * 32
PRIVATE_VALUE = "TEST_ONLY_PRIVATE_PAYLOAD_39715"
MARKER = ";authenticated_ordering="


def frame(sequence, *, session=SESSION_A, kind="TRADE"):
    value = signed_record(sequence=sequence)
    value.update(
        observation_id=f"test-only-{sequence}", session_id=session,
        observation_type=kind,
    )
    value["payload"]["test_only_value"] = PRIVATE_VALUE
    value["payload_hash"] = hashlib.sha256(json.dumps(
        value["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()).hexdigest()
    if kind == "ACCOUNT":
        value["account"] = {"alias": "Sim101", "class": "LOCAL_SIMULATION"}
    del value["signature"]
    key = hmac.new(ROOT_KEY, NINJATRADER_ATTESTATION_KEY_CONTEXT, hashlib.sha256).digest()
    value["signature"] = hmac.new(key, json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode(), hashlib.sha256).hexdigest()
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class RejectionProvenanceTests(unittest.TestCase):
    def bridge(self):
        return LoopbackNinjaTraderBridge(attestation_key=ROOT_KEY, require_attestation=True)

    def refusal(self, bridge, wire, reason="out_of_order_or_prior_session"):
        with self.assertRaises(NinjaTraderObservationError) as caught:
            bridge.accept_observation(wire)
        error = caught.exception
        self.assertIs(error.code, ProviderErrorCode.STALE_PROVIDER_STATE)
        self.assertTrue(error.detail.startswith(reason + MARKER))
        context = json.loads(error.detail.split(MARKER, 1)[1])
        self.assertLess(len(error.detail), 700)
        self.assertNotIn(PRIVATE_VALUE, error.detail)
        self.assertNotIn(json.loads(wire)["signature"], error.detail)
        self.assertNotIn("Sim101", error.detail)
        self.assertNotIn("LOCAL_SIMULATION", error.detail)
        self.assertNotIn(ROOT_KEY.decode(), error.detail)
        return context

    def test_regression_context_identifies_each_channel_without_payload_or_signature(self):
        for kind, channel in (
            ("TRADE", "MARKET_DATA"), ("ACCOUNT", "ACCOUNT_STATE"), ("HEALTH", "CONTROL"),
        ):
            with self.subTest(kind=kind):
                bridge = self.bridge()
                first = frame(10, kind=kind)
                self.assertTrue(bridge.accept_observation(first).signature_verified)
                self.assertIsNone(bridge.accept_observation(first))
                rejected = frame(9, kind=kind)
                context = self.refusal(bridge, rejected)
                self.assertEqual(context, {
                    "channel": channel,
                    "session_id": SESSION_A,
                    "global_sequence": 9,
                    "prior_session_id": SESSION_A,
                    "prior_global_sequence": 10,
                    "prior_signature_verified": True,
                    "wire_sha256": NinjaTraderObservation.from_wire(rejected.decode()).wire_hash(),
                })
                # Refusal must not overwrite the previously accepted state.
                self.assertIsNotNone(bridge.accept_observation(frame(11, kind=kind)))

    def test_retired_session_refusal_distinguishes_rejected_and_current_sessions(self):
        bridge = self.bridge()
        bridge.accept_observation(frame(10))
        bridge.accept_observation(frame(3, session=SESSION_B))
        context = self.refusal(bridge, frame(11), reason="prior_session_callback")
        self.assertEqual(context["session_id"], SESSION_A)
        self.assertEqual(context["prior_session_id"], SESSION_B)
        self.assertEqual(context["global_sequence"], 11)
        self.assertEqual(context["prior_global_sequence"], 3)
        self.assertTrue(context["prior_signature_verified"])
        self.assertIsNotNone(bridge.accept_observation(frame(4, session=SESSION_B)))

    def test_unverified_signed_records_do_not_receive_authenticated_context(self):
        ledger = NinjaTraderSessionLedger()
        ledger.admit(NinjaTraderObservation.from_wire(frame(10).decode()))
        with self.assertRaises(NinjaTraderObservationError) as caught:
            ledger.admit(NinjaTraderObservation.from_wire(frame(9).decode()))
        self.assertEqual(caught.exception.detail, "out_of_order_or_prior_session")

    def test_legacy_refusal_remains_exact_and_does_not_receive_context(self):
        bridge = LoopbackNinjaTraderBridge()
        def legacy(sequence):
            value = json.loads(frame(sequence))
            for name in ("payload_hash", "producer", "signature"):
                del value[name]
            value["schema"] = L3F2_SCHEMA
            return json.dumps(value).encode()
        bridge.accept_observation(legacy(10))
        with self.assertRaises(NinjaTraderObservationError) as caught:
            bridge.accept_observation(legacy(9))
        self.assertEqual(str(caught.exception), "STALE_PROVIDER_STATE:out_of_order_or_prior_session")

    def test_bad_signature_is_rejected_before_ledger_or_authenticated_context(self):
        bridge = self.bridge()
        bridge.ledger.admit = Mock(side_effect=AssertionError("must not admit before authentication"))
        value = json.loads(frame(9))
        value["signature"] = "0" * 64
        with self.assertRaises(NinjaTraderObservationError) as caught:
            bridge.accept_observation(json.dumps(value).encode())
        self.assertIs(caught.exception.code, ProviderErrorCode.AUTH_FAILED)
        self.assertEqual(caught.exception.detail, "ninjatrader_attestation_signature")
        bridge.ledger.admit.assert_not_called()

    def test_context_does_not_claim_prior_unverified_sequence_is_authenticated(self):
        bridge = self.bridge()
        bridge.ledger.admit(NinjaTraderObservation.from_wire(frame(10).decode()))
        context = self.refusal(bridge, frame(9))
        self.assertFalse(context["prior_signature_verified"])
        self.assertEqual(context["prior_global_sequence"], 10)

    def test_non_native_session_text_and_sequences_keep_diagnostics_bounded(self):
        bridge = self.bridge()
        session = "TEST_ONLY_PRIVATE_SESSION\n" * 100
        bridge.accept_observation(frame(10 ** 500 + 1, session=session))
        context = self.refusal(bridge, frame(10 ** 500, session=session))
        digest = "sha256:" + hashlib.sha256(session.encode()).hexdigest()
        self.assertEqual(context["session_id"], digest)
        self.assertEqual(context["prior_session_id"], digest)
        self.assertEqual(context["global_sequence"], "OUT_OF_NATIVE_INT64_RANGE")
        self.assertEqual(context["prior_global_sequence"], "OUT_OF_NATIVE_INT64_RANGE")
        self.assertNotIn("TEST_ONLY_PRIVATE_SESSION", json.dumps(context))

    def test_required_listener_error_retains_context_without_opening_socket(self):
        rejected = Mock()
        harness = NinjaTraderCommissioningHarness(
            attestation_key=ROOT_KEY, require_attestation=True,
            on_rejection=rejected, fail_on_observation_error=True,
        )
        with self.assertRaises(RuntimeError) as caught:
            harness.ingest((frame(10), frame(9), frame(11)))
        self.assertTrue(str(caught.exception).startswith(
            "NINJATRADER_REQUIRED_OBSERVATION_REJECTED:STALE_PROVIDER_STATE:"
            "out_of_order_or_prior_session" + MARKER,
        ))
        rejected.assert_called_once()
        self.assertEqual(harness.summary.accepted, 1)
        self.assertEqual(harness.summary.rejected, 1)
        self.assertIn('"prior_global_sequence":10', str(caught.exception))
        self.assertNotIn(PRIVATE_VALUE, str(caught.exception))

    def test_non_unicode_scalar_session_cannot_replace_ordering_refusal(self):
        bridge = self.bridge()
        bridge.accept_observation(frame(10, session="test-\ud800"))
        context = self.refusal(bridge, frame(9, session="test-\ud800"))
        self.assertTrue(context["session_id"].startswith("sha256:"))


if __name__ == "__main__":
    unittest.main()
