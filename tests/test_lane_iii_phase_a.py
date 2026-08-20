from __future__ import annotations

import ast
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from src.lane_iii.admission import LaneIIIFuturesAdmissionKernel, StrategyAdmission, StrategyAdmissionRegistry
from src.lane_iii.contracts import (
    L3A_AUTHORITY_MANIFEST_HASH,
    L3A_MANIFEST,
    AuthorityCapability,
    AuthorityStatus,
    ConfidenceLifecycle,
    ConfidenceState,
    EvidenceFamily,
    EvidenceObservation,
    EvidenceRelation,
    FamilyAssessment,
    FuturesExecutionInstrument,
    FuturesRiskConstitution,
    FuturesSessionPhase,
    FuturesTimeContext,
    HypothesisDirection,
    HypothesisKind,
    InstrumentRefused,
    IntentPurpose,
    IntentRefused,
    KnowledgeState,
    LaneIIIInstrument,
    LaneIIIStrategyArtifact,
    MarketHypothesis,
    MarketLocation,
    OperatorCommand,
    PositionKnowledge,
    ProfitTakingKind,
    ProfitTakingSemantics,
    ProtectiveStopSemantics,
    SafetySnapshot,
    SessionRiskState,
    StopProtectionKind,
    TargetExposure,
    TemporalBehavior,
    WorkingOrderKnowledge,
    ExecutionIntent,
    canonical_hash,
)
from src.lane_iii.persistence import LaneIIISafetyStore, RecoveryRefused


ROOT = Path(__file__).parents[1]
MANIFEST_PATH = ROOT / "docs" / "commissioning" / "lane-iii-phase-a" / "l3-a-authority-manifest.json"
NOW = "2026-08-19T14:30:00Z"
LATER = "2026-08-19T14:35:00Z"


class LaneIIIPhaseATests(unittest.TestCase):
    def artifact(self) -> LaneIIIStrategyArtifact:
        return LaneIIIStrategyArtifact(
            strategy_id="l3-mnq-evidence-fusion", strategy_version="v1",
            strategy_artifact_hash="a" * 64, strategy_instrument=LaneIIIInstrument.MNQ,
        )

    def observation(self, family: EvidenceFamily = EvidenceFamily.STRUCTURAL_CONTEXT, name: str = "one") -> EvidenceObservation:
        return EvidenceObservation(
            evidence_id=f"evidence-{name}", family=family, strategy_instrument=LaneIIIInstrument.MNQ,
            observed_at=NOW, expires_at=LATER, source_payload_hash=("b" if name == "one" else "c") * 64,
            temporal_behavior=TemporalBehavior.PERSISTENT,
        )

    def hypothesis(self, artifact: LaneIIIStrategyArtifact | None = None) -> MarketHypothesis:
        artifact = artifact or self.artifact()
        observation = self.observation()
        return MarketHypothesis.create(
            kind=HypothesisKind.REVERSAL, direction=HypothesisDirection.BULLISH, created_at=NOW,
            expires_at=LATER,
            location=MarketLocation(LaneIIIInstrument.MNQ, Decimal("22000.25"), "d" * 64),
            strategy_identity=artifact.strategy_identity, strategy_artifact_hash=artifact.strategy_artifact_hash,
            supporting_evidence=(observation.reference(),),
        )

    def confidence(self, hypothesis: MarketHypothesis) -> ConfidenceState:
        return ConfidenceState(
            hypothesis_id=hypothesis.hypothesis_id, evaluated_at=NOW, expires_at=LATER,
            lifecycle=ConfidenceLifecycle.ALIGNED,
            family_assessments=(FamilyAssessment(
                EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceRelation.SUPPORTS, (self.observation().reference(),),
            ),),
        )

    def snapshot(
        self,
        *,
        market: KnowledgeState = KnowledgeState.FRESH,
        broker: KnowledgeState = KnowledgeState.FRESH,
        position: PositionKnowledge = PositionKnowledge.FLAT,
        quantity: int | None = 0,
        orders: WorkingOrderKnowledge = WorkingOrderKnowledge.CLEAR,
        daily_loss: Decimal = Decimal("0"),
        session_loss: Decimal = Decimal("0"),
    ) -> SafetySnapshot:
        if position is PositionKnowledge.UNKNOWN:
            quantity = None
        return SafetySnapshot(
            observed_at=NOW, market_data=market, broker_state=broker, position_state=position,
            position_quantity=quantity, working_orders=orders,
            session_risk=SessionRiskState("CME-2026-08-19", daily_loss, session_loss),
        )

    def intent(
        self, *, artifact: LaneIIIStrategyArtifact | None = None, hypothesis: MarketHypothesis | None = None,
        confidence: ConfidenceState | None = None, quantity: int = 1, expires_at: str = LATER,
    ) -> ExecutionIntent:
        artifact = artifact or self.artifact()
        hypothesis = hypothesis or self.hypothesis(artifact)
        confidence = confidence or self.confidence(hypothesis)
        return ExecutionIntent.create(
            strategy_identity=artifact.strategy_identity, strategy_artifact_hash=artifact.strategy_artifact_hash,
            hypothesis_id=hypothesis.hypothesis_id,
            instrument=FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "MNQU6"), purpose=IntentPurpose.ENTRY,
            target_exposure=TargetExposure.LONG, quantity=quantity, created_at=NOW, expires_at=expires_at,
            protective_stop=ProtectiveStopSemantics(StopProtectionKind.PRICE_STOP, Decimal("21990.00")),
            profit_taking=ProfitTakingSemantics(ProfitTakingKind.NONE),
            confidence_snapshot_hash=confidence.snapshot_hash,
            evidence_state_hash=canonical_hash(hypothesis.payload()),
        )

    def kernel(
        self, directory: Path, *, activated: bool = True,
    ) -> tuple[LaneIIISafetyStore, LaneIIIFuturesAdmissionKernel, LaneIIIStrategyArtifact, MarketHypothesis, ConfidenceState]:
        artifact = self.artifact()
        hypothesis = self.hypothesis(artifact)
        confidence = self.confidence(hypothesis)
        store = LaneIIISafetyStore(directory / "lane-iii.sqlite3")
        store.initialize()
        if activated:
            store.record_active_hypothesis(hypothesis, recorded_at=NOW)
            store.record_confidence(confidence, recorded_at=NOW)
        store.apply_operator_command(OperatorCommand.ARM, requested_at=NOW)
        store.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=NOW)
        kernel = LaneIIIFuturesAdmissionKernel(
            store,
            FuturesRiskConstitution(
                4, 4, Decimal("500"), Decimal("300"), FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "MNQU6"),
            ),
            StrategyAdmissionRegistry((StrategyAdmission.from_artifact(artifact),)),
            clock=lambda: datetime(2026, 8, 19, 14, 31, tzinfo=timezone.utc),
        )
        return store, kernel, artifact, hypothesis, confidence

    def test_required_authority_matrix_is_exhaustive_and_denies_capital(self) -> None:
        self.assertEqual(L3A_MANIFEST.manifest_hash, L3A_AUTHORITY_MANIFEST_HASH)
        self.assertEqual(L3A_MANIFEST.record_for(AuthorityCapability.OBSERVE_MNQ_MARKET_DATA).status, AuthorityStatus.ARCHITECTURE_ONLY)
        self.assertEqual(L3A_MANIFEST.record_for(AuthorityCapability.CONSTRUCT_MARKET_HYPOTHESES).status, AuthorityStatus.CONTRACT_ONLY)
        self.assertEqual(L3A_MANIFEST.record_for(AuthorityCapability.COMPUTE_CONFIDENCE).status, AuthorityStatus.SEMANTICS_ONLY)
        for capability in (
            AuthorityCapability.GENERATE_TRADE_SIGNALS,
            AuthorityCapability.EXECUTE_FUTURES_ORDERS,
            AuthorityCapability.ACCESS_BROKER_ACCOUNT,
            AuthorityCapability.ACCESS_PROP_ACCOUNT,
            AuthorityCapability.CHANGE_HARD_RISK_LIMITS,
            AuthorityCapability.OVERRIDE_FLATTEN,
            AuthorityCapability.CONTROL_FOLLOWER_ACCOUNTS,
            AuthorityCapability.SCIENTIFIC_AUTHORITY,
            AuthorityCapability.MODIFY_ACTIVE_STRATEGY_FROM_PNL,
            AuthorityCapability.MODIFY_PHASE_E,
            AuthorityCapability.MODIFY_LANE_II,
            AuthorityCapability.LIVE_CAPITAL_AUTHORITY,
        ):
            self.assertEqual(L3A_MANIFEST.record_for(capability).status, AuthorityStatus.DENIED)

    def test_manifest_mirror_is_replayable(self) -> None:
        document = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        self.assertEqual(document, {**L3A_MANIFEST.payload(), "manifest_hash": L3A_AUTHORITY_MANIFEST_HASH})

    def test_evidence_is_family_scoped_and_cannot_count_correlated_flow_four_times(self) -> None:
        references = tuple(self.observation(EvidenceFamily.ORDER_FLOW, str(index)).reference() for index in range(4))
        assessment = FamilyAssessment(EvidenceFamily.ORDER_FLOW, EvidenceRelation.SUPPORTS, references)
        state = ConfidenceState("l3h-" + "f" * 32, NOW, LATER, ConfidenceLifecycle.ALIGNED, (assessment,))
        self.assertEqual(state.supporting_families, (EvidenceFamily.ORDER_FLOW,))
        with self.assertRaisesRegex(ValueError, "each evidence family only once"):
            ConfidenceState(
                "l3h-" + "f" * 32, NOW, LATER, ConfidenceLifecycle.ALIGNED,
                (assessment, FamilyAssessment(EvidenceFamily.ORDER_FLOW, EvidenceRelation.INCONCLUSIVE, (references[0],))),
            )

    def test_contradiction_and_staleness_have_stateful_confidence_semantics(self) -> None:
        contradiction = FamilyAssessment(
            EvidenceFamily.DERIVATIVES_CONTEXT, EvidenceRelation.CONTRADICTS,
            (self.observation(EvidenceFamily.DERIVATIVES_CONTEXT, "derivatives").reference(),),
        )
        with self.assertRaisesRegex(ValueError, "must collapse confidence"):
            ConfidenceState("l3h-" + "f" * 32, NOW, LATER, ConfidenceLifecycle.ALIGNED, (contradiction,))
        self.assertEqual(
            ConfidenceState("l3h-" + "f" * 32, NOW, LATER, ConfidenceLifecycle.CONTRADICTED, (contradiction,)).lifecycle,
            ConfidenceLifecycle.CONTRADICTED,
        )

    def test_hypothesis_is_deterministic_state_not_free_form_strategy_authority(self) -> None:
        first = self.hypothesis()
        second = self.hypothesis()
        self.assertEqual(first, second)
        self.assertEqual(first.hypothesis_id, second.hypothesis_id)
        self.assertFalse(hasattr(first, "order"))
        self.assertFalse(hasattr(first, "signal"))

    def test_mnq_scope_rejects_other_roots_and_unspecified_expiry(self) -> None:
        with self.assertRaises(InstrumentRefused):
            FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "NQZ6")
        with self.assertRaises(InstrumentRefused):
            FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "MNQ")

    def test_time_context_canonicalizes_exchange_session_and_display_boundaries(self) -> None:
        context = FuturesTimeContext(
            observed_at_utc=NOW, exchange_timestamp_utc=NOW, trading_session_id="CME-2026-08-19",
            session_phase=FuturesSessionPhase.NY_CASH_OPEN, display_timezone="America/Denver",
            economic_event_context_hash="1" * 64, expiration_context_hash="2" * 64,
        )
        self.assertEqual(context.payload()["display_timezone"], "America/Denver")
        with self.assertRaisesRegex(ValueError, "IANA timezone"):
            FuturesTimeContext(
                observed_at_utc=NOW, exchange_timestamp_utc=NOW, trading_session_id="CME-2026-08-19",
                session_phase=FuturesSessionPhase.REGULAR, display_timezone="not/a/timezone",
                economic_event_context_hash="1" * 64, expiration_context_hash="2" * 64,
            )

    def test_unadmitted_mnq_expiry_is_refused_without_a_rollover_engine(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            base = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            different_expiry = ExecutionIntent.create(
                strategy_identity=base.strategy_identity, strategy_artifact_hash=base.strategy_artifact_hash,
                hypothesis_id=base.hypothesis_id,
                instrument=FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "MNQZ6"), purpose=base.purpose,
                target_exposure=base.target_exposure, quantity=base.quantity, created_at=base.created_at,
                expires_at=base.expires_at, protective_stop=base.protective_stop, profit_taking=base.profit_taking,
                confidence_snapshot_hash=base.confidence_snapshot_hash, evidence_state_hash=base.evidence_state_hash,
            )
            self.assertEqual(kernel.review(different_expiry, self.snapshot()).reason_code, "execution_expiry_not_admitted")

    def test_malformed_intent_and_required_protective_stop_are_refused(self) -> None:
        artifact = self.artifact()
        hypothesis = self.hypothesis(artifact)
        confidence = self.confidence(hypothesis)
        with self.assertRaisesRegex(IntentRefused, "protective stop"):
            ExecutionIntent.create(
                strategy_identity=artifact.strategy_identity, strategy_artifact_hash=artifact.strategy_artifact_hash,
                hypothesis_id=hypothesis.hypothesis_id,
                instrument=FuturesExecutionInstrument(LaneIIIInstrument.MNQ, "MNQU6"), purpose=IntentPurpose.ENTRY,
                target_exposure=TargetExposure.LONG, quantity=1, created_at=NOW, expires_at=LATER,
                protective_stop=None, profit_taking=ProfitTakingSemantics(ProfitTakingKind.NONE),
                confidence_snapshot_hash=confidence.snapshot_hash, evidence_state_hash="e" * 64,
            )

    def test_unauthorized_strategy_and_wrong_artifact_hash_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            unauthorized = LaneIIIStrategyArtifact("l3-other", "v1", "f" * 64, LaneIIIInstrument.MNQ)
            decision = kernel.review(self.intent(artifact=unauthorized, hypothesis=self.hypothesis(unauthorized), confidence=self.confidence(self.hypothesis(unauthorized))), self.snapshot())
            self.assertEqual(decision.reason_code, "unauthorized_strategy_identity")
            wrong_hash = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            wrong_hash = ExecutionIntent.create(
                strategy_identity=wrong_hash.strategy_identity, strategy_artifact_hash="f" * 64,
                hypothesis_id=wrong_hash.hypothesis_id, instrument=wrong_hash.instrument, purpose=wrong_hash.purpose,
                target_exposure=wrong_hash.target_exposure, quantity=wrong_hash.quantity, created_at=wrong_hash.created_at,
                expires_at=wrong_hash.expires_at, protective_stop=wrong_hash.protective_stop,
                profit_taking=wrong_hash.profit_taking, confidence_snapshot_hash=wrong_hash.confidence_snapshot_hash,
                evidence_state_hash=wrong_hash.evidence_state_hash,
            )
            self.assertEqual(kernel.review(wrong_hash, self.snapshot()).reason_code, "strategy_artifact_hash_mismatch")

    def test_excessive_quantity_expired_and_stale_state_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            self.assertEqual(kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence, quantity=5), self.snapshot()).reason_code, "maximum_individual_order_quantity_exceeded")
            self.assertEqual(kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence, expires_at="2026-08-19T14:30:30Z"), self.snapshot()).reason_code, "intent_expired_or_not_yet_valid")
            self.assertEqual(kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence), self.snapshot(broker=KnowledgeState.STALE)).reason_code, "stale_or_unknown_broker_state")
            self.assertEqual(kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence, quantity=2), self.snapshot(market=KnowledgeState.STALE)).reason_code, "stale_or_unknown_market_data")

    def test_unknown_is_not_flat_or_cancelled(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            intent = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            self.assertEqual(kernel.review(intent, self.snapshot(position=PositionKnowledge.UNKNOWN)).reason_code, "unknown_position_not_zero")
            second = ExecutionIntent.create(
                strategy_identity=intent.strategy_identity, strategy_artifact_hash=intent.strategy_artifact_hash,
                hypothesis_id=intent.hypothesis_id, instrument=intent.instrument, purpose=intent.purpose,
                target_exposure=intent.target_exposure, quantity=2, created_at=intent.created_at, expires_at=intent.expires_at,
                protective_stop=intent.protective_stop, profit_taking=intent.profit_taking,
                confidence_snapshot_hash=intent.confidence_snapshot_hash, evidence_state_hash=intent.evidence_state_hash,
            )
            self.assertEqual(kernel.review(second, self.snapshot(orders=WorkingOrderKnowledge.UNKNOWN)).reason_code, "unknown_order_not_cancelled")

    def test_hard_loss_and_one_position_risk_remain_above_aligned_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            self.assertEqual(
                kernel.review(
                    self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence),
                    self.snapshot(daily_loss=Decimal("500")),
                ).reason_code,
                "maximum_daily_loss_reached",
            )
            self.assertEqual(
                kernel.review(
                    self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence, quantity=2),
                    self.snapshot(session_loss=Decimal("300")),
                ).reason_code,
                "maximum_session_loss_reached",
            )
            self.assertEqual(
                kernel.review(
                    self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence, quantity=3),
                    self.snapshot(position=PositionKnowledge.LONG, quantity=1),
                ).reason_code,
                "one_position_semantics_existing_position",
            )

    def test_duplicate_intent_is_idempotent_and_cannot_duplicate_future_exposure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            intent = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            first = kernel.review(intent, self.snapshot())
            second = kernel.review(intent, self.snapshot())
            self.assertTrue(first.eligible_for_sovereign_review)
            self.assertTrue(second.replayed)
            self.assertEqual(len(store.unresolved_intents()), 1)
            self.assertFalse(first.execution_authority)

    def test_intent_must_bind_to_the_persisted_evidence_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            base = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            mismatched = ExecutionIntent.create(
                strategy_identity=base.strategy_identity, strategy_artifact_hash=base.strategy_artifact_hash,
                hypothesis_id=base.hypothesis_id, instrument=base.instrument, purpose=base.purpose,
                target_exposure=base.target_exposure, quantity=base.quantity, created_at=base.created_at,
                expires_at=base.expires_at, protective_stop=base.protective_stop, profit_taking=base.profit_taking,
                confidence_snapshot_hash=base.confidence_snapshot_hash, evidence_state_hash="e" * 64,
            )
            self.assertEqual(kernel.review(mismatched, self.snapshot()).reason_code, "evidence_state_unavailable_or_mismatched")

    def test_evidence_object_and_arbitrary_broker_command_cannot_cross_execution_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, kernel, _, _, _ = self.kernel(Path(temp))
            with self.assertRaisesRegex(Exception, "exact_execution_intent_required"):
                kernel.review(self.observation(), self.snapshot())
            with self.assertRaisesRegex(Exception, "exact_execution_intent_required"):
                kernel.review({"broker_command": "BUY", "account": "master"}, self.snapshot())

    def test_hard_limits_are_external_and_strategy_cannot_suppress_operator_flatten(self) -> None:
        self.assertNotIn("maximum_mnq_exposure", ExecutionIntent.__dataclass_fields__)
        self.assertNotIn("hard_risk_limits", ExecutionIntent.__dataclass_fields__)
        self.assertNotIn("flatten", ExecutionIntent.__dataclass_fields__)
        with tempfile.TemporaryDirectory() as temp:
            store, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            state = store.apply_operator_command(OperatorCommand.FLATTEN, requested_at=NOW)
            self.assertTrue(state.flatten_latched)
            decision = kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence), self.snapshot())
            self.assertEqual(decision.reason_code, "operator_flatten_latched")
            with self.assertRaises(RecoveryRefused):
                store.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=NOW)

    def test_no_follower_or_account_fields_exist_on_the_contract(self) -> None:
        all_fields = set(ExecutionIntent.__dataclass_fields__) | set(FuturesExecutionInstrument.__dataclass_fields__)
        self.assertFalse({"account", "account_id", "follower", "followers", "copier", "broker_command"} & all_fields)

    def test_restart_persists_hypothesis_confidence_safety_and_unresolved_intent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            store, kernel, artifact, hypothesis, confidence = self.kernel(directory)
            intent = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            kernel.review(intent, self.snapshot())
            restarted = LaneIIISafetyStore(directory / "lane-iii.sqlite3")
            restarted.initialize()
            self.assertEqual(restarted.active_hypothesis()["hypothesis_id"], hypothesis.hypothesis_id)  # type: ignore[index]
            self.assertEqual(restarted.confidence_state()["snapshot_hash"], confidence.snapshot_hash)  # type: ignore[index]
            self.assertEqual(restarted.latest_safety_snapshot(), self.snapshot())
            self.assertEqual(restarted.operator_state().armed, True)
            self.assertEqual(len(restarted.unresolved_intents()), 1)

    def test_recovery_requires_verified_flat_and_clear_orders(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            intent = self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence)
            kernel.review(intent, self.snapshot())
            with self.assertRaises(RecoveryRefused):
                store.resolve_intent_after_authoritative_recovery(
                    intent.intent_id, snapshot=self.snapshot(position=PositionKnowledge.UNKNOWN), resolved_at=LATER,
                )
            store.resolve_intent_after_authoritative_recovery(intent.intent_id, snapshot=self.snapshot(), resolved_at=LATER)
            self.assertEqual(store.unresolved_intents(), ())

    def test_operator_disarm_blocks_new_future_requests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store, kernel, artifact, hypothesis, confidence = self.kernel(Path(temp))
            store.apply_operator_command(OperatorCommand.DISARM, requested_at=NOW)
            self.assertEqual(
                kernel.review(self.intent(artifact=artifact, hypothesis=hypothesis, confidence=confidence), self.snapshot()).reason_code,
                "operator_disarmed",
            )

    def test_lane_iii_has_no_phase_e_lane_ii_or_phase_d_transport_import(self) -> None:
        forbidden: list[str] = []
        for source in (ROOT / "src" / "lane_iii").glob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    names.append(node.module)
                elif isinstance(node, ast.Import):
                    names.extend(alias.name for alias in node.names)
                forbidden.extend(
                    name for name in names
                    if name.startswith(("src.phase_e", "src.lane_ii", "src.copytrade.execution", "requests", "websockets"))
                )
        self.assertEqual(forbidden, [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
