from __future__ import annotations

import ast
import json
import unittest
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from src.lane_iii.contracts import (
    EvidenceFamily,
    EvidenceRelation,
    HypothesisDirection,
    HypothesisKind,
    LaneIIIInstrument,
    LaneIIIStrategyArtifact,
    canonical_hash,
)
from src.lane_iii.hypothesis_engine import (
    BEARISH_CONTINUATION,
    BEARISH_REVERSAL,
    BULLISH_CONTINUATION,
    BULLISH_REVERSAL,
    ConfidenceAssessment,
    EvidenceDerivation,
    EvidenceImpact,
    EvidenceKind,
    EvidenceObject,
    EvidenceState,
    EvidenceUsability,
    FamilyContribution,
    HypothesisEngineMetrics,
    HypothesisEngineSnapshot,
    HypothesisIdentity,
    HypothesisRecord,
    HypothesisState,
    SourceProvenance,
)
from src.lane_iii.market_data import DataQuality
from src.lane_iii.trader_v0 import (
    COMMISSIONED_L3C_CONFIGURATION_HASH,
    COMMISSIONED_TRADER_V0_CONFIG,
    TRADER_V0_ARTIFACT_HASH,
    TRADER_V0_ARTIFACT_PAYLOAD,
    TRADER_V0_AUTHORITY_REGISTRY,
    TRADER_V0_STRATEGY,
    DeterministicTraderReplay,
    SignalAuthorityRefused,
    SignalDecision,
    SignalDecisionType,
    SignalReason,
    TraderDataQuality,
    TraderEvaluationRefused,
    TraderV0,
    build_strategy_artifact,
)


ROOT = Path(__file__).resolve().parents[1]
BASE = datetime(2026, 8, 19, 14, 30, tzinfo=timezone.utc)


def text_time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class LaneIIIPhaseDTests(unittest.TestCase):
    def quality(self, at: datetime = BASE, state: DataQuality = DataQuality.HEALTHY) -> TraderDataQuality:
        return TraderDataQuality(
            text_time(at), canonical_hash({"market": text_time(at), "quality": state.value}),
            state, state, state, state,
        )

    def record(
        self,
        identity: HypothesisIdentity = BULLISH_REVERSAL,
        score: str = "0.65",
        *,
        at: datetime = BASE,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        source_at: datetime | None = None,
        expires_at: datetime | None = None,
        state: HypothesisState = HypothesisState.ACTIVE,
        families: tuple[EvidenceFamily, ...] = (
            EvidenceFamily.STRUCTURAL_CONTEXT,
            EvidenceFamily.ORDER_FLOW,
            EvidenceFamily.RESTING_LIQUIDITY,
        ),
        contradiction: bool = False,
        invalidates: bool = False,
        multiple_flow_supports: int = 1,
        hypothesis_id: str | None = None,
    ) -> tuple[HypothesisRecord, tuple[EvidenceState, ...]]:
        created = created_at or at - timedelta(seconds=10)
        updated = updated_at or at
        source_time = source_at or at - timedelta(seconds=2)
        expires = expires_at or at + timedelta(seconds=60)
        support_ids: list[str] = []
        contradiction_ids: list[str] = []
        invalidator_ids: list[str] = []
        contributions: list[FamilyContribution] = []
        evidence_states: list[EvidenceState] = []
        for family_index, family in enumerate(families):
            support_count = multiple_flow_supports if family is EvidenceFamily.ORDER_FLOW else 1
            family_support_ids: list[str] = []
            for support_index in range(support_count):
                token = canonical_hash({
                    "hypothesis": identity.payload(), "family": family.value, "at": text_time(at),
                    "index": support_index, "id": hypothesis_id,
                })
                evidence_id = "l3c-e-" + token[:32]
                source = SourceProvenance(
                    ("event-" + token[:20],), (token,), text_time(source_time), text_time(source_time),
                    DataQuality.HEALTHY, "l3b:fixture:" + token[:24],
                )
                kind = {
                    EvidenceFamily.STRUCTURAL_CONTEXT: EvidenceKind.RANGE_RECLAIM_UP,
                    EvidenceFamily.ORDER_FLOW: EvidenceKind.SELLING_WITHOUT_DOWNWARD_PROGRESS,
                    EvidenceFamily.RESTING_LIQUIDITY: EvidenceKind.BID_REPLENISHMENT,
                    EvidenceFamily.TIMING_SESSION_CONTEXT: EvidenceKind.SESSION_PHASE,
                    EvidenceFamily.DERIVATIVES_CONTEXT: EvidenceKind.DERIVATIVES_VINTAGE,
                }[family]
                evidence = EvidenceObject(
                    evidence_id, family, kind, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                    text_time(source_time), text_time(max(expires, at + timedelta(seconds=60))), source,
                    (EvidenceImpact(identity, EvidenceRelation.SUPPORTS, Decimal("0.50")),),
                    f"fixture:{identity.key}:{family.value}", Decimal("0.50"),
                )
                evidence_states.append(EvidenceState(evidence, EvidenceUsability.AUTHORITATIVE))
                family_support_ids.append(evidence_id)
                support_ids.append(evidence_id)
            family_contradiction_ids: tuple[str, ...] = ()
            strongest_contradiction = Decimal(0)
            if contradiction and family_index == 0:
                token = canonical_hash({"contradiction": identity.payload(), "at": text_time(at), "id": hypothesis_id})
                evidence_id = "l3c-e-" + token[:32]
                source = SourceProvenance(
                    ("event-" + token[:20],), (token,), text_time(source_time), text_time(source_time),
                    DataQuality.HEALTHY, "l3b:fixture:" + token[:24],
                )
                evidence = EvidenceObject(
                    evidence_id, family, EvidenceKind.RANGE_RECLAIM_DOWN, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                    text_time(source_time), text_time(max(expires, at + timedelta(seconds=60))), source,
                    (EvidenceImpact(identity, EvidenceRelation.CONTRADICTS, Decimal("0.20"), invalidates),),
                    f"fixture:contradiction:{identity.key}", Decimal("0.20"),
                )
                evidence_states.append(EvidenceState(evidence, EvidenceUsability.AUTHORITATIVE))
                family_contradiction_ids = (evidence_id,)
                contradiction_ids.append(evidence_id)
                strongest_contradiction = Decimal("0.20")
                if invalidates:
                    invalidator_ids.append(evidence_id)
            contributions.append(FamilyContribution(
                family, tuple(family_support_ids), family_contradiction_ids,
                Decimal("0.50"), strongest_contradiction, Decimal("0.50") - strongest_contradiction,
            ))
        key = hypothesis_id or "l3c-h-" + identity.key + "-" + canonical_hash({"created": text_time(created), "identity": identity.payload()})[:20]
        record = HypothesisRecord(
            key, identity, text_time(created), text_time(updated), text_time(expires), state,
            ConfidenceAssessment(text_time(at), Decimal(score), tuple(contributions)),
            tuple(support_ids), tuple(contradiction_ids), tuple(invalidator_ids),
            COMMISSIONED_L3C_CONFIGURATION_HASH,
        )
        return record, tuple(evidence_states)

    def snapshot(self, *cases: tuple[HypothesisRecord, tuple[EvidenceState, ...]], at: datetime = BASE) -> HypothesisEngineSnapshot:
        records = tuple(case[0] for case in cases)
        evidence = tuple(item for case in cases for item in case[1])
        return HypothesisEngineSnapshot(
            text_time(at), COMMISSIONED_L3C_CONFIGURATION_HASH, evidence, records,
            None, (), (), HypothesisEngineMetrics(0, len(evidence), 0, len(records), 0, 0, len(evidence), len(records)),
        )

    def test_exact_strategy_identity_and_artifact_are_hash_bound(self) -> None:
        self.assertEqual(TRADER_V0_ARTIFACT_HASH, canonical_hash(TRADER_V0_ARTIFACT_PAYLOAD))
        self.assertEqual(TRADER_V0_STRATEGY.strategy_artifact_hash, TRADER_V0_ARTIFACT_HASH)
        self.assertTrue(TRADER_V0_STRATEGY.strategy_identity.startswith("l3-strategy-"))
        self.assertTrue(TRADER_V0_AUTHORITY_REGISTRY.require(TRADER_V0_STRATEGY).signal_authority)
        self.assertEqual(
            TRADER_V0_ARTIFACT_PAYLOAD["signal_contract_fields"],
            [item.name for item in fields(SignalDecision)],
        )

    def test_commissioned_artifact_document_matches_runtime_payload(self) -> None:
        path = ROOT / "docs" / "commissioning" / "lane-iii-phase-d" / "trader-v0-artifact.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(document.pop("strategy_identity"), TRADER_V0_STRATEGY.strategy_identity)
        self.assertEqual(document.pop("strategy_artifact_hash"), TRADER_V0_ARTIFACT_HASH)
        self.assertEqual(document, TRADER_V0_ARTIFACT_PAYLOAD)

    def test_mutated_policy_changes_hash_and_identity_and_has_no_authority(self) -> None:
        changed = replace(COMMISSIONED_TRADER_V0_CONFIG, entry_relative_support=Decimal("0.66"))
        artifact = build_strategy_artifact(changed)
        self.assertNotEqual(artifact.strategy_artifact_hash, TRADER_V0_ARTIFACT_HASH)
        self.assertNotEqual(artifact.strategy_identity, TRADER_V0_STRATEGY.strategy_identity)
        with self.assertRaises(SignalAuthorityRefused):
            TraderV0(artifact, changed)

    def test_wrong_strategy_id_and_wrong_artifact_hash_are_rejected(self) -> None:
        wrong_id = LaneIIIStrategyArtifact("l3-other-v0", "1", TRADER_V0_ARTIFACT_HASH, LaneIIIInstrument.MNQ)
        wrong_hash = LaneIIIStrategyArtifact("l3-trader-v0", "1", "f" * 64, LaneIIIInstrument.MNQ)
        with self.assertRaises(SignalAuthorityRefused):
            TRADER_V0_AUTHORITY_REGISTRY.require(wrong_id)
        with self.assertRaises(SignalAuthorityRefused):
            TRADER_V0_AUTHORITY_REGISTRY.require(wrong_hash)

    def test_clean_bullish_reversal_emits_long_once_and_suppresses_duplicates(self) -> None:
        trader = TraderV0()
        case = self.record()
        first_snapshot = self.snapshot(case)
        first = trader.evaluate(first_snapshot, self.quality())
        duplicate = trader.evaluate(first_snapshot, self.quality())
        later = BASE + timedelta(seconds=1)
        retained_case = self.record(at=later, created_at=BASE - timedelta(seconds=10), hypothesis_id=case[0].hypothesis_id)
        retained = trader.evaluate(self.snapshot(retained_case, at=later), self.quality(later))
        self.assertIs(first.decision, SignalDecisionType.LONG)
        self.assertEqual(duplicate.decision_id, first.decision_id)
        self.assertIs(retained.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(retained.reason_code, SignalReason.ACTIVE_THESIS_RETAINED)
        self.assertEqual(trader.metrics().long_signals, 1)
        self.assertEqual(trader.metrics().duplicate_suppression, 1)

    def test_clean_bearish_continuation_emits_short(self) -> None:
        decision = TraderV0().evaluate(self.snapshot(self.record(BEARISH_CONTINUATION)), self.quality())
        self.assertIs(decision.decision, SignalDecisionType.SHORT)
        self.assertIs(decision.reason_code, SignalReason.ENTRY_BEARISH_CONTINUATION)

    def test_high_support_high_conflict_abstains(self) -> None:
        bullish = self.record(BULLISH_REVERSAL, "0.66")
        bearish = self.record(BEARISH_CONTINUATION, "0.62")
        decision = TraderV0().evaluate(self.snapshot(bullish, bearish), self.quality())
        self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(decision.reason_code, SignalReason.INSUFFICIENT_DOMINANCE)

    def test_correlated_same_family_support_cannot_satisfy_breadth(self) -> None:
        correlated = self.record(
            BULLISH_REVERSAL, "0.80", families=(EvidenceFamily.ORDER_FLOW,), multiple_flow_supports=4,
        )
        decision = TraderV0().evaluate(self.snapshot(correlated), self.quality())
        self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(decision.reason_code, SignalReason.INSUFFICIENT_FAMILY_BREADTH)
        self.assertEqual(len(decision.family_summary), 1)
        self.assertEqual(len(decision.family_summary[0].supporting_evidence), 4)

    def test_below_threshold_abstains(self) -> None:
        decision = TraderV0().evaluate(self.snapshot(self.record(score="0.64")), self.quality())
        self.assertIs(decision.reason_code, SignalReason.BELOW_ENTRY_THRESHOLD)

    def test_unauthorized_hypothesis_abstains(self) -> None:
        for identity in (BULLISH_CONTINUATION, BEARISH_REVERSAL):
            with self.subTest(identity=identity.key):
                decision = TraderV0().evaluate(self.snapshot(self.record(identity, "0.90")), self.quality())
                self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
                self.assertIs(decision.reason_code, SignalReason.UNAUTHORIZED_HYPOTHESIS)

    def test_stale_hypothesis_and_stale_evidence_abstain(self) -> None:
        stale_hypothesis = self.record(
            created_at=BASE - timedelta(seconds=30), updated_at=BASE - timedelta(seconds=16),
        )
        stale_evidence = self.record(source_at=BASE - timedelta(seconds=31))
        first = TraderV0().evaluate(self.snapshot(stale_hypothesis), self.quality())
        second = TraderV0().evaluate(self.snapshot(stale_evidence), self.quality())
        self.assertIs(first.reason_code, SignalReason.HYPOTHESIS_STALE)
        self.assertIs(second.reason_code, SignalReason.EVIDENCE_STALE)

    def test_missing_retained_evidence_abstains_instead_of_fabricating_provenance(self) -> None:
        case = self.record()
        snapshot = replace(self.snapshot(case), evidence=())
        decision = TraderV0().evaluate(snapshot, self.quality())
        self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(decision.reason_code, SignalReason.EVIDENCE_STALE)
        self.assertTrue(all(not summary.supporting_evidence for summary in decision.family_summary))

    def test_hypothesis_too_old_for_new_entry_abstains(self) -> None:
        old = self.record(created_at=BASE - timedelta(seconds=61))
        decision = TraderV0().evaluate(self.snapshot(old), self.quality())
        self.assertIs(decision.reason_code, SignalReason.HYPOTHESIS_STALE)

    def test_every_degraded_quality_state_blocks_new_entry(self) -> None:
        for state in (
            DataQuality.STALE, DataQuality.GAPPED, DataQuality.RECOVERING,
            DataQuality.INCOMPLETE, DataQuality.INVALID,
        ):
            with self.subTest(state=state.value):
                decision = TraderV0().evaluate(self.snapshot(self.record()), self.quality(state=state))
                self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
                self.assertIs(decision.reason_code, SignalReason.DATA_QUALITY_DEGRADED)

    def test_invalidated_and_contradicted_hypotheses_abstain(self) -> None:
        invalidated = self.record(state=HypothesisState.INVALIDATED, contradiction=True, invalidates=True)
        contradicted = self.record(state=HypothesisState.CONFLICTED, contradiction=True)
        first = TraderV0().evaluate(self.snapshot(invalidated), self.quality())
        second = TraderV0().evaluate(self.snapshot(contradicted), self.quality())
        self.assertIs(first.reason_code, SignalReason.HYPOTHESIS_INVALIDATED)
        self.assertIs(second.reason_code, SignalReason.BLOCKING_CONTRADICTION)

    def test_expired_hypothesis_cannot_create_new_entry(self) -> None:
        expired = self.record(
            state=HypothesisState.EXPIRED, expires_at=BASE - timedelta(seconds=1),
            created_at=BASE - timedelta(seconds=20),
        )
        decision = TraderV0().evaluate(self.snapshot(expired), self.quality())
        self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(decision.reason_code, SignalReason.HYPOTHESIS_EXPIRED)

    def test_confidence_decay_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        decayed = self.record(
            score="0.57", at=later, created_at=BASE - timedelta(seconds=10),
            families=(EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceFamily.ORDER_FLOW),
            hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(decayed, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.CONFIDENCE_DECAY)

    def test_invalidating_contradiction_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        invalidated = self.record(
            at=later, created_at=BASE - timedelta(seconds=10), state=HypothesisState.INVALIDATED,
            contradiction=True, invalidates=True, hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(invalidated, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.THESIS_INVALIDATED)

    def test_blocking_contradiction_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        conflicted = self.record(
            at=later, created_at=BASE - timedelta(seconds=10), state=HypothesisState.CONFLICTED,
            contradiction=True, hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(conflicted, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.BLOCKING_CONTRADICTION)

    def test_expired_thesis_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=61)
        expired = self.record(
            at=later, created_at=BASE - timedelta(seconds=10), state=HypothesisState.EXPIRED,
            expires_at=BASE + timedelta(seconds=60), hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(expired, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.THESIS_EXPIRED)
        self.assertEqual(trader.metrics().hypothesis_expirations_encountered, 1)

    def test_decisive_opposing_thesis_exits_without_same_event_reversal(self) -> None:
        trader = TraderV0()
        entry = self.record(score="0.65")
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        active = self.record(
            score="0.60", at=later, created_at=BASE - timedelta(seconds=10),
            hypothesis_id=entry[0].hypothesis_id,
        )
        opposing = self.record(BEARISH_CONTINUATION, "0.72", at=later)
        decision = trader.evaluate(self.snapshot(active, opposing, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.OPPOSING_HYPOTHESIS)
        self.assertEqual(decision.related_hypothesis_id, opposing[0].hypothesis_id)
        self.assertEqual(trader.metrics().short_signals, 0)

    def test_retention_hysteresis_prevents_small_threshold_churn(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        retained = self.record(
            score="0.60", at=later, created_at=BASE - timedelta(seconds=10),
            families=(EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceFamily.ORDER_FLOW),
            hypothesis_id=entry[0].hypothesis_id,
        )
        competitor = self.record(BEARISH_CONTINUATION, "0.56", at=later)
        decision = trader.evaluate(self.snapshot(retained, competitor, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.NO_TRADE)
        self.assertIs(decision.reason_code, SignalReason.ACTIVE_THESIS_RETAINED)

    def test_dominance_loss_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        active = self.record(score="0.60", at=later, created_at=BASE - timedelta(seconds=10), hypothesis_id=entry[0].hypothesis_id)
        competitor = self.record(BEARISH_CONTINUATION, "0.58", at=later)
        decision = trader.evaluate(self.snapshot(active, competitor, at=later), self.quality(later))
        self.assertIs(decision.reason_code, SignalReason.DOMINANCE_LOST)
        self.assertIs(decision.decision, SignalDecisionType.EXIT)

    def test_family_breadth_loss_after_entry_emits_exit(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        narrow = self.record(
            score="0.60", at=later, created_at=BASE - timedelta(seconds=10),
            families=(EvidenceFamily.STRUCTURAL_CONTEXT,), hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(narrow, at=later), self.quality(later))
        self.assertIs(decision.reason_code, SignalReason.FAMILY_BREADTH_LOST)
        self.assertIs(decision.decision, SignalDecisionType.EXIT)

    def test_degraded_quality_after_entry_is_strategy_exit_only(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=1)
        active = self.record(at=later, created_at=BASE - timedelta(seconds=10), hypothesis_id=entry[0].hypothesis_id)
        decision = trader.evaluate(self.snapshot(active, at=later), self.quality(later, DataQuality.GAPPED))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.DATA_QUALITY_DEGRADED)
        self.assertFalse(hasattr(decision, "flatten"))

    def test_maximum_thesis_age_is_a_signal_exit_not_broker_timeout(self) -> None:
        trader = TraderV0()
        entry = self.record()
        trader.evaluate(self.snapshot(entry), self.quality())
        later = BASE + timedelta(seconds=120)
        active = self.record(
            at=later, created_at=later - timedelta(seconds=10),
            hypothesis_id=entry[0].hypothesis_id,
        )
        decision = trader.evaluate(self.snapshot(active, at=later), self.quality(later))
        self.assertIs(decision.decision, SignalDecisionType.EXIT)
        self.assertIs(decision.reason_code, SignalReason.THESIS_MAXIMUM_AGE)

    def test_same_hypothesis_never_reenters_and_new_hypothesis_obeys_cooldown(self) -> None:
        trader = TraderV0()
        original = self.record()
        trader.evaluate(self.snapshot(original), self.quality())
        exit_at = BASE + timedelta(seconds=1)
        invalidated = self.record(
            at=exit_at, created_at=BASE - timedelta(seconds=10), state=HypothesisState.INVALIDATED,
            contradiction=True, invalidates=True, hypothesis_id=original[0].hypothesis_id,
        )
        trader.evaluate(self.snapshot(invalidated, at=exit_at), self.quality(exit_at))
        cooldown_at = BASE + timedelta(seconds=2)
        new = self.record(at=cooldown_at, hypothesis_id="l3c-h-new-bullish-reversal")
        cooldown = trader.evaluate(self.snapshot(new, at=cooldown_at), self.quality(cooldown_at))
        self.assertIs(cooldown.reason_code, SignalReason.REENTRY_COOLDOWN)
        ready_at = BASE + timedelta(seconds=32)
        same = self.record(at=ready_at, created_at=ready_at - timedelta(seconds=10), hypothesis_id=original[0].hypothesis_id)
        repeated = trader.evaluate(self.snapshot(same, at=ready_at), self.quality(ready_at))
        self.assertIs(repeated.reason_code, SignalReason.ALREADY_SIGNALED_HYPOTHESIS)
        next_at = BASE + timedelta(seconds=33)
        independent = self.record(at=next_at, hypothesis_id="l3c-h-independent-bullish-reversal")
        admitted = trader.evaluate(self.snapshot(independent, at=next_at), self.quality(next_at))
        self.assertIs(admitted.decision, SignalDecisionType.LONG)

    def test_signal_provenance_traces_family_evidence_to_l3b_events(self) -> None:
        decision = TraderV0().evaluate(self.snapshot(self.record()), self.quality())
        self.assertEqual(len(decision.family_summary), 3)
        for summary in decision.family_summary:
            self.assertTrue(summary.supporting_evidence)
            for trace in summary.supporting_evidence:
                self.assertTrue(trace.source_observation_id.startswith("l3b:"))
                self.assertEqual(len(trace.source_event_ids), len(trace.source_payload_hashes))
                self.assertEqual(len(trace.evidence_snapshot_hash), 64)
        self.assertEqual(len(decision.source_state_hash), 64)

    def test_signal_expiration_is_exactly_five_event_time_seconds(self) -> None:
        decision = TraderV0().evaluate(self.snapshot(self.record()), self.quality())
        self.assertEqual(
            datetime.fromisoformat(decision.expires_at.replace("Z", "+00:00"))
            - datetime.fromisoformat(decision.created_at.replace("Z", "+00:00")),
            timedelta(seconds=COMMISSIONED_TRADER_V0_CONFIG.signal_ttl_seconds),
        )

    def test_identical_replay_has_identical_decision_ids_and_sequence_hash(self) -> None:
        inputs = []
        first = self.record()
        inputs.append((self.snapshot(first), self.quality()))
        later = BASE + timedelta(seconds=1)
        retained = self.record(at=later, created_at=BASE - timedelta(seconds=10), hypothesis_id=first[0].hypothesis_id)
        inputs.append((self.snapshot(retained, at=later), self.quality(later)))
        one = DeterministicTraderReplay(TraderV0()).replay(inputs)
        two = DeterministicTraderReplay(TraderV0()).replay(inputs)
        self.assertEqual(one, two)
        self.assertEqual([item.decision_id for item in one.decisions], [item.decision_id for item in two.decisions])

    def test_input_time_and_l3c_configuration_mismatch_refuse(self) -> None:
        case = self.record()
        snapshot = self.snapshot(case)
        with self.assertRaises(TraderEvaluationRefused):
            TraderV0().evaluate(snapshot, self.quality(BASE + timedelta(seconds=1)))
        mutated = replace(snapshot, configuration_hash="f" * 64)
        with self.assertRaises(TraderEvaluationRefused):
            TraderV0().evaluate(mutated, self.quality())

    def test_time_cannot_move_backward(self) -> None:
        trader = TraderV0()
        trader.evaluate(self.snapshot(self.record()), self.quality())
        earlier = BASE - timedelta(seconds=1)
        with self.assertRaises(TraderEvaluationRefused):
            trader.evaluate(self.snapshot(self.record(at=earlier), at=earlier), self.quality(earlier))

    def test_signal_contract_has_no_execution_sizing_account_or_price_fields(self) -> None:
        signal_fields = {item.name for item in fields(SignalDecision)}
        forbidden = {"quantity", "account", "broker", "order", "price", "stop", "target", "intent", "position"}
        self.assertFalse(signal_fields & forbidden)
        decision = TraderV0().evaluate(self.snapshot(self.record()), self.quality())
        document = json.dumps(decision.payload(), sort_keys=True).lower()
        self.assertNotIn('"quantity"', document)
        self.assertNotIn('"account"', document)
        self.assertNotIn('"order"', document)
        self.assertNotIn('"intent"', document)

    def test_source_has_no_broker_risk_phase_e_copier_or_network_dependencies(self) -> None:
        source_path = ROOT / "src" / "lane_iii" / "trader_v0.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        denied_prefixes = (
            "src.phase_d", "src.phase_e", "src.phase_f", "src.lane_ii", "src.copytrade",
            "requests", "websockets", "boto3", "hyperliquid",
        )
        self.assertFalse([name for name in imported if name.startswith(denied_prefixes)])
        public = {name.lower() for name in dir(TraderV0) if not name.startswith("_")}
        self.assertFalse(public & {"submit", "order", "broker", "account", "position", "size", "risk", "execute", "intent", "flatten"})

    def test_confidence_remains_relative_support_not_probability_or_pnl(self) -> None:
        decision = TraderV0().evaluate(self.snapshot(self.record(score="0.70")), self.quality())
        self.assertEqual(decision.relative_support_snapshot, Decimal("0.70"))
        self.assertFalse(hasattr(decision, "probability"))
        self.assertFalse(hasattr(decision, "win_rate"))
        self.assertFalse(hasattr(decision, "pnl"))

    def test_metrics_are_deterministic_diagnostics_only(self) -> None:
        trader = TraderV0()
        trader.evaluate(self.snapshot(self.record(score="0.64")), self.quality())
        metrics = trader.metrics()
        self.assertEqual(metrics.evaluations, 1)
        self.assertEqual(metrics.no_trade_results, 1)
        self.assertEqual(metrics.long_signals, 0)
        self.assertEqual(dict(metrics.blocked_entries_by_reason)[SignalReason.BELOW_ENTRY_THRESHOLD.value], 1)
        self.assertFalse(hasattr(metrics, "profit"))

    def test_signaled_hypothesis_memory_is_bounded(self) -> None:
        trader = TraderV0()
        now = BASE
        for index in range(COMMISSIONED_TRADER_V0_CONFIG.hypothesis_history_limit + 2):
            hypothesis_id = f"l3c-h-bounded-{index}"
            entry = self.record(at=now, hypothesis_id=hypothesis_id)
            trader.evaluate(self.snapshot(entry, at=now), self.quality(now))
            exit_at = now + timedelta(seconds=1)
            invalidated = self.record(
                at=exit_at, created_at=now - timedelta(seconds=10), state=HypothesisState.INVALIDATED,
                contradiction=True, invalidates=True, hypothesis_id=hypothesis_id,
            )
            trader.evaluate(self.snapshot(invalidated, at=exit_at), self.quality(exit_at))
            now += timedelta(seconds=31)
        self.assertEqual(
            trader.metrics().retained_signaled_hypothesis_ids,
            COMMISSIONED_TRADER_V0_CONFIG.hypothesis_history_limit,
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
