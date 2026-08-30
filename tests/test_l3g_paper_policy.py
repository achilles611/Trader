from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import PaperDecisionKind, PaperDirection
from src.l3g_paper.policy import ExperimentalPaperPolicy

from tests.l3g_helpers import ObservationFactory, warmed_bullish_policy


class PaperPolicyTests(unittest.TestCase):
    def test_non_market_observation_advances_global_local_sequence(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory()
        policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        policy.ingest(factory.quote(100))
        policy.ingest(factory.make("HEALTH", {"state": "OBSERVATION_ONLY"}))
        decision = policy.ingest(factory.quote(100))
        self.assertNotEqual(decision.reason_code, "LOCAL_SEQUENCE_GAP")
        self.assertEqual(policy.status()["counters"]["local_sequence_gaps"], 0)

    def test_exact_market_callback_recovers_price_state_after_bridge_restart(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory()
        decision = policy.ingest(factory.quote(100))
        self.assertNotEqual(decision.reason_code, "MARKET_PRICE_STATE_NOT_CONNECTED")
        self.assertTrue(policy.status()["market_price_connected"])

    def test_runtime_ingests_passive_callbacks_without_manufacturing_decisions(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest_runtime(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        quote = factory.quote(100)
        self.assertIsNone(policy.ingest_runtime(quote))
        self.assertEqual(policy.status()["counters"]["quotes"], 1)
        self.assertIsNotNone(policy.ingest_runtime(factory.trade(quote, 100)))
        self.assertIsNone(policy.ingest_runtime(factory.depth("ADD", 10)))
        self.assertIsNotNone(policy.ingest_runtime(factory.depth("UPDATE", 5)))

    def test_identical_replay_has_identical_decision_ids(self) -> None:
        first = warmed_bullish_policy()[2]
        second = warmed_bullish_policy()[2]
        self.assertEqual(first.paper_decision_id, second.paper_decision_id)
        self.assertFalse(first.scientific_eligibility)
        self.assertNotIn("provider_sequence", first.payload())

    def test_unknown_aggressor_never_votes(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        for price in (100, 99, 100):
            quote = factory.quote(price); policy.ingest(quote); policy.ingest(factory.trade(quote, price, source="UNKNOWN"))
        counters = policy.status()["counters"]
        self.assertEqual(counters["unknown_aggressor"], 3)
        self.assertEqual(counters["classified_buy"] + counters["classified_sell"], 0)

    def test_three_families_are_required_and_bullish_reversal_is_long_only(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        last = None
        for price in (100, 99, 100):
            quote = factory.quote(price); policy.ingest(quote); last = policy.ingest(factory.trade(quote, price))
        self.assertEqual(last.decision, PaperDecisionKind.NO_TRADE)
        for operation, volume in (("ADD", 10), ("UPDATE", 5), ("UPDATE", 10), ("UPDATE", 5), ("UPDATE", 11)):
            last = policy.ingest(factory.depth(operation, volume))
        self.assertEqual(last.decision, PaperDecisionKind.LONG)
        self.assertEqual(last.direction, PaperDirection.LONG)

    def test_gap_and_reset_clear_provisional_evidence(self) -> None:
        policy, factory, _ = warmed_bullish_policy()
        factory.sequence += 1
        decision = policy.ingest(factory.quote(100))
        self.assertEqual(decision.reason_code, "LOCAL_SEQUENCE_GAP")
        reset = factory.make("DEPTH", {"contract_id": "MNQ SEP26", "is_reset": True})
        self.assertEqual(policy.ingest(reset).reason_code, "DEPTH_RESET")
        self.assertEqual(policy.status()["quality"], "UNUSABLE")

    def test_legacy_depth_snapshot_never_becomes_paper_mutation_evidence(self) -> None:
        policy = ExperimentalPaperPolicy()
        factory = ObservationFactory()
        legacy = factory.make("DEPTH", {
            "contract_id": "MNQ SEP26",
            "bids": [{"price": 100, "size": 2}],
            "asks": [{"price": 100.25, "size": 2}],
            "operation": "Update",
            "side": "Bid",
        })
        decision = policy.ingest(legacy)
        self.assertEqual(decision.reason_code, "LEGACY_DEPTH_SNAPSHOT_NO_MUTATION")
        self.assertEqual(policy.status()["quality"], "UNUSABLE")
        self.assertEqual(policy.status()["paper_evidence_by_family"]["RESTING_LIQUIDITY"], 0)

    def test_opposing_hypothesis_while_positioned_exits_only(self) -> None:
        policy, factory, _ = warmed_bullish_policy()
        decision = policy.ingest(factory.depth("UPDATE", 4), current_position=PaperDirection.SHORT)
        self.assertNotEqual(decision.decision, PaperDecisionKind.LONG)
        self.assertIn(decision.decision, {PaperDecisionKind.EXIT, PaperDecisionKind.NO_TRADE})

    def test_bearish_continuation_is_the_only_short_entry(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        last = None
        for price in (100, 99, 98):
            quote = factory.quote(price); policy.ingest(quote); last = policy.ingest(factory.trade(quote, price))
        policy.ingest(factory.depth("ADD", 10))
        last = policy.ingest(factory.depth("UPDATE", 5))
        self.assertEqual(last.decision, PaperDecisionKind.SHORT)
        self.assertEqual(last.direction, PaperDirection.SHORT)

    def test_quote_reference_mismatch_crossed_quote_and_partial_depth_fail_closed(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        quote = factory.quote(100); policy.ingest(quote)
        mismatched = factory.trade(quote, 100)
        mismatched = type(mismatched)(
            mismatched.observation_id, mismatched.session_id, mismatched.observation_type,
            mismatched.ninja_receipt_time, mismatched.local_monotonic_sequence,
            {**mismatched.payload, "derivation_quote_observation_id": "missing"},
            provider_timestamp=mismatched.provider_timestamp,
        )
        policy.ingest(mismatched)
        self.assertEqual(policy.status()["counters"]["unknown_aggressor"], 1)
        crossed = factory.make("QUOTE", {"contract_id": "MNQ SEP26", "bid": 101, "ask": 100, "bid_size": 1, "ask_size": 1})
        self.assertEqual(policy.ingest(crossed).reason_code, "INVALID_OR_CROSSED_QUOTE")
        partial = factory.make("DEPTH", {
            "contract_id": "MNQ SEP26", "bids": [{"price": 100, "size": 1}], "asks": [],
            "operation": "Update", "side": "Bid", "mutation_price": 100,
            "mutation_volume": 1, "mutation_position": 0, "is_reset": False,
        })
        self.assertEqual(policy.ingest(partial).reason_code, "PARTIAL_DEPTH")

    def test_session_change_timestamp_reversal_disconnect_and_expiry_clear_evidence(self) -> None:
        policy, factory, _ = warmed_bullish_policy()
        changed = ObservationFactory(session="new-session", start=factory.start + timedelta(seconds=5))
        self.assertEqual(policy.ingest(changed.quote(100)).reason_code, "OBSERVATION_SESSION_CHANGED")
        policy.on_transport_state(StreamHealth.DISCONNECTED)
        self.assertEqual(policy.status()["quality"], "UNUSABLE")
        policy.on_transport_state(StreamHealth.HEALTHY)
        policy.ingest(changed.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        later = changed.quote(100); policy.ingest(later)
        changed.start -= timedelta(minutes=1)
        self.assertEqual(policy.ingest(changed.quote(100)).reason_code, "TIMESTAMP_MOVED_BACKWARD")
        self.assertTrue(all(value == 0 for value in policy.status()["paper_evidence_by_family"].values()))

    def test_cross_stream_provider_timestamp_crossing_uses_local_receipt_order(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        first = factory.quote(100); policy.ingest(first)
        crossing = factory.depth("ADD", 10)
        crossing = type(crossing)(
            crossing.observation_id, crossing.session_id, crossing.observation_type,
            crossing.ninja_receipt_time, crossing.local_monotonic_sequence,
            crossing.payload, provider_timestamp=first.provider_timestamp,
        )
        decision = policy.ingest(crossing)
        self.assertNotEqual(decision.reason_code, "TIMESTAMP_MOVED_BACKWARD")
        self.assertEqual(policy.status()["counters"]["resets"], 1)  # connection recovery only

    def test_backward_provider_clock_cannot_reset_local_callback_evidence(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        first = factory.quote(100); policy.ingest(first)
        crossing = factory.depth("ADD", 10)
        earlier_provider_time = (datetime.fromisoformat(first.provider_timestamp.replace("Z", "+00:00")) - timedelta(milliseconds=50)).isoformat().replace("+00:00", "Z")
        crossing = type(crossing)(
            crossing.observation_id, crossing.session_id, crossing.observation_type,
            crossing.ninja_receipt_time, crossing.local_monotonic_sequence,
            crossing.payload, provider_timestamp=earlier_provider_time,
        )
        decision = policy.ingest(crossing)
        self.assertNotEqual(decision.reason_code, "TIMESTAMP_MOVED_BACKWARD")
        self.assertEqual(policy.status()["counters"]["resets"], 1)  # connection recovery only

    def test_small_provider_clock_skew_is_bounded_but_not_rejected(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        quote = factory.quote(100)
        provider_time = (datetime.fromisoformat(quote.ninja_receipt_time.replace("Z", "+00:00")) + timedelta(milliseconds=1200)).isoformat().replace("+00:00", "Z")
        quote = type(quote)(
            quote.observation_id, quote.session_id, quote.observation_type,
            quote.ninja_receipt_time, quote.local_monotonic_sequence,
            quote.payload, provider_timestamp=provider_time,
        )
        self.assertNotEqual(policy.ingest(quote).reason_code, "FUTURE_EVENT_TIMESTAMP")

    def test_provider_event_older_than_idle_boundary_is_stale(self) -> None:
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(); policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        observation = factory.quote(100)
        stale_provider = (factory.start - timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
        observation = type(observation)(
            observation.observation_id, observation.session_id, observation.observation_type,
            observation.ninja_receipt_time, observation.local_monotonic_sequence,
            observation.payload, provider_timestamp=stale_provider,
        )
        self.assertEqual(policy.ingest(observation).reason_code, "STALE_EVENT_TIMESTAMP")


if __name__ == "__main__":
    unittest.main()
