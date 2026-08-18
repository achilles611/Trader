from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from src.lane_ii.boundary import (
    F0_AUTHORITY_MANIFEST_HASH,
    F0_MANIFEST,
    AuthorityCapability,
    AuthorityOwner,
    AuthorityRecord,
    AuthorityRefused,
    AuthorityState,
    ExecutionAuthorityRefused,
    LaneIdentity,
    OperationalInput,
    OperationalInputSource,
    OperationalStrategyArtifact,
    StrategyAuthorityRegistry,
    StrategyProvenanceRefused,
    StrategyRegistration,
    TradeDirection,
    TradeIntentRefused,
    TradeIntentRequest,
    create_trade_intent,
    evaluate_lane_ii_authority,
    request_phase_d_execution,
)
from src.phase_e.acquisition import PhaseE6Acquisition
from src.phase_e.prospective import OutcomeRecord, PhaseE5Registry, load_frozen_protocol


ROOT = Path(__file__).parents[1]
FROZEN_PROTOCOL = ROOT / "docs" / "commissioning" / "phase-e5-prospective-experiment" / "e5-protocol-v1.json"
F0_MANIFEST_PATH = ROOT / "docs" / "commissioning" / "phase-f0-lane-ii-boundary" / "f0-authority-manifest.json"


class PhaseF0LaneIIBoundaryTests(unittest.TestCase):
    def strategy(self) -> OperationalStrategyArtifact:
        return OperationalStrategyArtifact(
            strategy_id="trader-v0-boundary-test",
            strategy_version="v0.0.0",
            strategy_artifact_hash="a" * 64,
            allowed_input_sources=(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA,),
        )

    def input(self) -> OperationalInput:
        return OperationalInput(
            input_id="market-snapshot-001",
            source=OperationalInputSource.LIVE_PUBLIC_MARKET_DATA,
            observed_at="2026-08-18T00:00:00Z",
            payload_hash="b" * 64,
            source_system="public-market-feed",
        )

    def denied_registry(self, strategy: OperationalStrategyArtifact) -> StrategyAuthorityRegistry:
        return StrategyAuthorityRegistry((
            StrategyRegistration(
                strategy_identity=strategy.strategy_identity,
                strategy_version=strategy.strategy_version,
                signal_authority=AuthorityRecord(
                    AuthorityOwner.TRADER_LANE,
                    AuthorityCapability.SIGNAL,
                    AuthorityState.DENIED,
                    "F0_NO_SIGNAL_AUTHORITY",
                ),
            ),
        ))

    def intent(self) -> TradeIntentRequest:
        strategy = self.strategy()
        item = self.input()
        decision = evaluate_lane_ii_authority(strategy, (item,), registry=self.denied_registry(strategy))
        return TradeIntentRequest(
            strategy_id=strategy.strategy_id,
            strategy_version=strategy.strategy_version,
            strategy_identity=strategy.strategy_identity,
            symbol="BTC",
            direction=TradeDirection.LONG,
            requested_notional_ceiling=100.0,
            created_at="2026-08-18T00:00:00Z",
            expires_at="2026-08-18T00:05:00Z",
            authority_decision_hash=decision.decision_hash,
            input_provenance_hashes=(item.provenance_hash,),
            exit_policy_ref="exit-policy-v0",
            risk_policy_ref="risk-policy-v0",
        )

    def test_lane_ii_cannot_read_an_e5_protected_outcome_capability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            protected_capability = PhaseE5Registry(Path(temp) / "e5.sqlite3")
            decision = evaluate_lane_ii_authority(
                self.strategy(), (protected_capability,), registry=StrategyAuthorityRegistry(),
            )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY")

    def test_lane_ii_cannot_consume_an_e5_scientific_result_before_authorization(self) -> None:
        decision = evaluate_lane_ii_authority(
            self.strategy(), (OutcomeRecord("e5-observation", 0.01),), registry=StrategyAuthorityRegistry(),
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY")

    def test_lane_ii_has_no_e5_or_e6_mutation_seam(self) -> None:
        before = FROZEN_PROTOCOL.read_bytes()
        before_digest = hashlib.sha256(before).hexdigest()
        with tempfile.TemporaryDirectory() as temp:
            e6 = PhaseE6Acquisition(Path(temp) / "e6.sqlite3", FROZEN_PROTOCOL)
            baseline = e6.status()
            decision = evaluate_lane_ii_authority(
                self.strategy(), (load_frozen_protocol(FROZEN_PROTOCOL),), registry=StrategyAuthorityRegistry(),
            )
            after = e6.status()
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY")
        self.assertEqual(FROZEN_PROTOCOL.read_bytes(), before)
        self.assertEqual(hashlib.sha256(FROZEN_PROTOCOL.read_bytes()).hexdigest(), before_digest)
        self.assertEqual(baseline["block_states"], {"scheduled": 60})
        self.assertEqual(after["block_states"], {"scheduled": 60})
        self.assertEqual(after["observation_count"], 0)
        self.assertEqual(after["outcome_access"]["scientific_evaluation_reads"], 0)
        self.assertEqual(after["reserved_test_queries"], 0)
        self.assertFalse(any(after["authority"].values()))

    def test_scientific_hypothesis_cannot_gain_trading_identity_or_authority(self) -> None:
        with self.assertRaises(StrategyProvenanceRefused):
            OperationalStrategyArtifact(
                strategy_id="e5p-ae597d81614b76feba54168141de6a73",
                strategy_version="v1",
                strategy_artifact_hash="c" * 64,
                allowed_input_sources=(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA,),
            )
        self.assertFalse(F0_MANIFEST.record_for(AuthorityOwner.TRADER_LANE, AuthorityCapability.TRADING).granted)
        self.assertFalse(F0_MANIFEST.record_for(AuthorityOwner.TRADER_LANE, AuthorityCapability.SIGNAL).granted)

    def test_strategy_without_explicit_operational_authority_cannot_create_an_intent(self) -> None:
        strategy = self.strategy()
        denied = self.denied_registry(strategy)
        decision = evaluate_lane_ii_authority(strategy, (self.input(),), registry=denied)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "SIGNAL_AUTHORITY_NOT_COMMISSIONED")
        with self.assertRaisesRegex(TradeIntentRefused, "SIGNAL_AUTHORITY_NOT_COMMISSIONED"):
            create_trade_intent(
                strategy, (self.input(),), symbol="BTC", direction=TradeDirection.LONG,
                requested_notional_ceiling=100.0, created_at="2026-08-18T00:00:00Z",
                expires_at="2026-08-18T00:05:00Z", exit_policy_ref="exit-policy-v0",
                risk_policy_ref="risk-policy-v0", registry=denied,
            )

    def test_trade_intent_does_not_grant_execution_authority(self) -> None:
        intent = self.intent()
        self.assertFalse(intent.execution_authority)
        self.assertFalse(intent.live_capital_authority)
        with self.assertRaises(ExecutionAuthorityRefused):
            request_phase_d_execution(intent)

    def test_phase_d_is_the_only_execution_bound_authority(self) -> None:
        phase_d = F0_MANIFEST.record_for(AuthorityOwner.PHASE_D_EXECUTION_SOVEREIGN, AuthorityCapability.EXECUTION)
        trader = F0_MANIFEST.record_for(AuthorityOwner.TRADER_LANE, AuthorityCapability.EXECUTION)
        scientific = F0_MANIFEST.record_for(AuthorityOwner.SCIENTIFIC_LANE, AuthorityCapability.EXECUTION)
        self.assertEqual(phase_d.state, AuthorityState.PHASE_D_SOVEREIGN)
        self.assertEqual(trader.state, AuthorityState.DENIED)
        self.assertEqual(scientific.state, AuthorityState.DENIED)

    def test_missing_and_unknown_provenance_fail_closed(self) -> None:
        missing = evaluate_lane_ii_authority(None, (self.input(),), registry=StrategyAuthorityRegistry())
        unknown = evaluate_lane_ii_authority(self.strategy(), (self.input(),), registry=StrategyAuthorityRegistry())
        self.assertEqual((missing.allowed, missing.reason_code), (False, "STRATEGY_PROVENANCE_MISSING"))
        self.assertEqual((unknown.allowed, unknown.reason_code), (False, "UNKNOWN_STRATEGY_VERSION"))

    def test_replay_preserves_authority_decision_deterministically(self) -> None:
        strategy = self.strategy()
        registry = self.denied_registry(strategy)
        first = evaluate_lane_ii_authority(strategy, (self.input(),), registry=registry)
        second = evaluate_lane_ii_authority(strategy, (self.input(),), registry=registry)
        self.assertEqual(first, second)
        self.assertEqual(first.decision_hash, second.decision_hash)
        self.assertEqual(first.manifest_hash, F0_AUTHORITY_MANIFEST_HASH)

    def test_lane_identities_cannot_be_silently_substituted(self) -> None:
        with self.assertRaises(AuthorityRefused):
            F0_MANIFEST.record_for(LaneIdentity.SCIENTIFIC_LANE, AuthorityCapability.SIGNAL)  # type: ignore[arg-type]
        with self.assertRaises(StrategyProvenanceRefused):
            TradeIntentRequest(
                strategy_id="trader-v0-boundary-test", strategy_version="v0.0.0",
                strategy_identity="e5p-ae597d81614b76feba54168141de6a73", symbol="BTC",
                direction=TradeDirection.LONG, requested_notional_ceiling=100.0,
                created_at="2026-08-18T00:00:00Z", expires_at="2026-08-18T00:05:00Z",
                authority_decision_hash="d" * 64, input_provenance_hashes=("e" * 64,),
                exit_policy_ref="exit-policy-v0", risk_policy_ref="risk-policy-v0",
            )

    def test_manifest_is_replayable_and_lane_ii_cannot_import_scientific_or_execution_transport(self) -> None:
        document = json.loads(F0_MANIFEST_PATH.read_text(encoding="utf-8"))
        self.assertEqual(document, {**F0_MANIFEST.payload(), "manifest_hash": F0_AUTHORITY_MANIFEST_HASH})
        tree = ast.parse((ROOT / "src" / "lane_ii" / "boundary.py").read_text(encoding="utf-8"))
        forbidden: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("src.phase_e") or node.module.startswith("src.copytrade.execution"):
                    forbidden.append(node.module)
            if isinstance(node, ast.Import):
                forbidden.extend(
                    alias.name for alias in node.names
                    if alias.name.startswith("src.phase_e") or alias.name.startswith("src.copytrade.execution")
                )
        self.assertEqual(forbidden, [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
