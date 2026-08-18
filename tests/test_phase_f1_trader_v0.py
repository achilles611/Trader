from __future__ import annotations

import ast
import hashlib
import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.lane_ii.boundary import (
    F0_AUTHORITY_MANIFEST_HASH,
    F0_MANIFEST,
    AuthorityCapability,
    ExecutionAuthorityRefused,
    OperationalInput,
    OperationalInputSource,
    OperationalStrategyArtifact,
    StrategyProvenanceRefused,
    TradeDirection,
    TradeIntentRefused,
    request_phase_d_execution,
)
from src.lane_ii.trader_v0 import (
    F1_AUTHORITY_MANIFEST_HASH,
    F1_MANIFEST,
    F1_STRATEGY_REGISTRY,
    TRADER_V0_ARTIFACT_HASH,
    TRADER_V0_ARTIFACT_PAYLOAD,
    TRADER_V0_EXIT_POLICY_REF,
    TRADER_V0_RISK_POLICY_REF,
    TRADER_V0_STRATEGY,
    TraderV0,
    TraderV0Action,
    TraderV0DecisionInput,
    create_f1_trade_intent,
    evaluate_f1_authority,
)
from src.phase_e.acquisition import PhaseE6Acquisition
from src.phase_e.prospective import load_frozen_protocol


ROOT = Path(__file__).parents[1]
ARTIFACT_PATH = ROOT / "docs" / "commissioning" / "phase-f1-trader-v0" / "trader-v0-v1.json"
F1_MANIFEST_PATH = ROOT / "docs" / "commissioning" / "phase-f1-trader-v0" / "f1-authority-manifest.json"
F0_MANIFEST_PATH = ROOT / "docs" / "commissioning" / "phase-f0-lane-ii-boundary" / "f0-authority-manifest.json"
FROZEN_PROTOCOL = ROOT / "docs" / "commissioning" / "phase-e5-prospective-experiment" / "e5-protocol-v1.json"


class _ExplosiveProtectedCapability:
    @property
    def source(self) -> object:  # pragma: no cover - must not be invoked
        raise AssertionError("protected object was invoked")


class _ExplosiveContainer:
    def __iter__(self) -> object:  # pragma: no cover - must not be invoked
        raise AssertionError("foreign input container was invoked")


class PhaseF1TraderV0Tests(unittest.TestCase):
    now = "2026-08-18T00:00:00Z"

    def operational_input(self, source: OperationalInputSource, token: str) -> OperationalInput:
        return OperationalInput(
            input_id=f"{source.value.lower()}-{token}", source=source, observed_at=self.now,
            payload_hash=token * 64, source_system=f"{source.value.lower()}-feed",
        )

    def entry_inputs(self) -> tuple[OperationalInput, ...]:
        return (
            self.operational_input(OperationalInputSource.LIVE_PUBLIC_WALLET_ACTIVITY, "a"),
            self.operational_input(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA, "b"),
            self.operational_input(OperationalInputSource.OPERATIONAL_INDICATOR, "c"),
            self.operational_input(OperationalInputSource.CONFIGURATION_OR_RISK_POLICY, "d"),
        )

    def position_inputs(self) -> tuple[OperationalInput, ...]:
        return (self.operational_input(OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE, "e"),)

    def decision_input(self, **changes: object) -> TraderV0DecisionInput:
        payload: dict[str, object] = {
            "operational_inputs": self.entry_inputs(),
            "now": self.now,
            "symbol": "BTC",
            "direction": TradeDirection.LONG,
            "source_action_at": self.now,
            "market_observed_at": self.now,
            "indicator_ids": ("wallet-flow", "microstructure"),
            "effective_confidence": 0.60,
            "expected_gross_edge": 0.020,
            "estimated_fees": 0.001,
            "estimated_spread": 0.001,
            "estimated_slippage": 0.001,
            "estimated_market_impact": 0.001,
            "estimated_latency_cost": 0.001,
            "alpha_survival": 1.0,
            "requested_notional_ceiling": 120.0,
            "market_regime": "normal",
        }
        payload.update(changes)
        return TraderV0DecisionInput(**payload)  # type: ignore[arg-type]

    def positioned_input(self, **changes: object) -> TraderV0DecisionInput:
        payload: dict[str, object] = {
            "operational_inputs": self.position_inputs(),
            "position_open": True,
        }
        payload.update(changes)
        return self.decision_input(**payload)

    def test_exact_artifact_and_single_registered_version(self) -> None:
        document = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
        authority_document = json.loads(F1_MANIFEST_PATH.read_text(encoding="utf-8"))
        self.assertEqual(document["strategy_artifact_hash"], TRADER_V0_ARTIFACT_HASH)
        self.assertEqual({key: value for key, value in document.items() if key != "strategy_artifact_hash"}, TRADER_V0_ARTIFACT_PAYLOAD)
        self.assertEqual(authority_document, {**F1_MANIFEST.payload(), "manifest_hash": F1_AUTHORITY_MANIFEST_HASH})
        self.assertEqual(len(F1_STRATEGY_REGISTRY.registrations), 1)
        self.assertEqual(F1_STRATEGY_REGISTRY.registrations[0].strategy_identity, TRADER_V0_STRATEGY.strategy_identity)
        self.assertTrue(F1_MANIFEST.record_for(AuthorityCapability.SIGNAL).granted)
        changed = {**TRADER_V0_ARTIFACT_PAYLOAD, "risk_policy": {"maximum_requested_notional_ceiling": 999.0}}
        changed_hash = hashlib.sha256(json.dumps(changed, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        self.assertNotEqual(changed_hash, TRADER_V0_ARTIFACT_HASH)

    def test_unknown_version_changed_hash_and_scientific_identity_fail_closed(self) -> None:
        altered = OperationalStrategyArtifact(
            strategy_id="trader-v0", strategy_version="2", strategy_artifact_hash="f" * 64,
            allowed_input_sources=TRADER_V0_STRATEGY.allowed_input_sources,
        )
        decision = evaluate_f1_authority(altered, self.entry_inputs())
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "STRATEGY_IDENTITY_MISMATCH")
        with self.assertRaises(StrategyProvenanceRefused):
            OperationalStrategyArtifact(
                strategy_id="e5-hypothesis", strategy_version="1", strategy_artifact_hash="f" * 64,
                allowed_input_sources=(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA,),
            )

    def test_unknown_and_undeclared_input_capabilities_refuse_without_invocation(self) -> None:
        protected = evaluate_f1_authority(TRADER_V0_STRATEGY, (_ExplosiveProtectedCapability(),))
        undeclared = evaluate_f1_authority(
            TRADER_V0_STRATEGY,
            (self.operational_input(OperationalInputSource.PHASE_ABC_OPERATIONAL_OBSERVATION, "f"),),
        )
        self.assertEqual((protected.allowed, protected.reason_code), (False, "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY"))
        self.assertEqual((undeclared.allowed, undeclared.reason_code), (False, "INPUT_SOURCE_NOT_DECLARED_BY_STRATEGY"))
        refused = TraderV0().decide(self.decision_input(operational_inputs=_ExplosiveContainer()))
        self.assertEqual(refused.action, TraderV0Action.SKIP)
        self.assertIn("INPUT_PROVENANCE_REFUSED", refused.reason_codes)

    def test_missing_provenance_and_duplicate_provenance_fail_closed(self) -> None:
        missing = evaluate_f1_authority(TRADER_V0_STRATEGY, ())
        repeated = self.operational_input(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA, "a")
        duplicate = evaluate_f1_authority(TRADER_V0_STRATEGY, (repeated, repeated))
        self.assertEqual(missing.reason_code, "INPUT_PROVENANCE_MISSING")
        self.assertEqual(duplicate.reason_code, "DUPLICATE_INPUT_PROVENANCE")

    def test_authorized_entry_and_intent_are_bounded_and_non_executable(self) -> None:
        decision = TraderV0().decide(self.decision_input())
        intent = create_f1_trade_intent(decision)
        self.assertEqual(decision.action, TraderV0Action.LONG)
        self.assertEqual(decision.reason_codes, ("ENTRY_AUTHORIZED",))
        self.assertEqual(intent.strategy_identity, TRADER_V0_STRATEGY.strategy_identity)
        self.assertEqual(intent.exit_policy_ref, TRADER_V0_EXIT_POLICY_REF)
        self.assertEqual(intent.risk_policy_ref, TRADER_V0_RISK_POLICY_REF)
        self.assertEqual(intent.input_provenance_hashes, tuple(sorted(intent.input_provenance_hashes)))
        self.assertFalse(intent.execution_authority)
        self.assertFalse(intent.live_capital_authority)
        with self.assertRaises(ExecutionAuthorityRefused):
            request_phase_d_execution(intent)

    def test_entry_threshold_hysteresis_and_direction(self) -> None:
        engine = TraderV0()
        at_threshold = engine.decide(self.decision_input(effective_confidence=0.60))
        below_threshold = engine.decide(self.decision_input(effective_confidence=0.599999))
        short = engine.decide(self.decision_input(direction=TradeDirection.SHORT))
        invalid = engine.decide(self.decision_input(direction="SIDEWAYS"))
        self.assertEqual(at_threshold.action, TraderV0Action.LONG)
        self.assertEqual(short.action, TraderV0Action.SHORT)
        self.assertIn("LOW_EFFECTIVE_CONFIDENCE", below_threshold.reason_codes)
        self.assertIn("INVALID_DIRECTION", invalid.reason_codes)

    def test_failed_closed_entry_gates(self) -> None:
        engine = TraderV0()
        cases = {
            "non_positive": self.decision_input(expected_gross_edge=0.005),
            "nonfinite_cost": self.decision_input(estimated_fees=math.nan),
            "stale_signal": self.decision_input(source_action_at="2026-08-17T23:59:49Z"),
            "stale_market": self.decision_input(market_observed_at="2026-08-17T23:59:49Z"),
            "missing_indicator": self.decision_input(indicator_ids=()),
            "zero_alpha": self.decision_input(alpha_survival=0.0),
            "invalid_regime": self.decision_input(market_regime=""),
        }
        for name, item in cases.items():
            with self.subTest(name=name):
                self.assertEqual(engine.decide(item).action, TraderV0Action.SKIP)
        self.assertIn("NON_POSITIVE_NET_EDGE", engine.decide(cases["non_positive"]).reason_codes)
        self.assertIn("STALE_SIGNAL", engine.decide(cases["stale_signal"]).reason_codes)
        self.assertIn("MARKET_EVIDENCE_STALE", engine.decide(cases["stale_market"]).reason_codes)
        self.assertIn("MISSING_REQUIRED_INDICATOR", engine.decide(cases["missing_indicator"]).reason_codes)

    def test_notional_is_capped_and_wallet_leverage_has_no_sizing_authority(self) -> None:
        engine = TraderV0()
        high = engine.decide(self.decision_input(requested_notional_ceiling=100000.0, source_wallet_leverage=100.0))
        low = engine.decide(self.decision_input(requested_notional_ceiling=100000.0, source_wallet_leverage=1.0))
        self.assertEqual(high.requested_notional_ceiling, 1000.0)
        self.assertEqual(low.requested_notional_ceiling, 1000.0)
        self.assertEqual(high.action, low.action)

    def test_authoritative_exit_conditions_and_no_new_entry_in_hysteresis_band(self) -> None:
        engine = TraderV0()
        cases = {
            "age": self.positioned_input(position_age_seconds=600.0),
            "confidence": self.positioned_input(effective_confidence=0.519),
            "edge": self.positioned_input(expected_gross_edge=0.005),
            "risk": self.positioned_input(hard_risk_exit=True),
            "regime": self.positioned_input(regime_invalidated=True),
            "integrity": self.positioned_input(estimated_fees=math.inf),
        }
        for name, item in cases.items():
            with self.subTest(name=name):
                self.assertEqual(engine.decide(item).action, TraderV0Action.EXIT)
        self.assertEqual(engine.decide(self.positioned_input(effective_confidence=0.55)).action, TraderV0Action.SKIP)
        self.assertEqual(engine.decide(self.decision_input(effective_confidence=0.55)).action, TraderV0Action.SKIP)

    def test_replay_and_input_order_are_deterministic(self) -> None:
        engine = TraderV0()
        first = engine.decide(self.decision_input())
        second = engine.decide(self.decision_input())
        reordered = engine.decide(self.decision_input(operational_inputs=tuple(reversed(self.entry_inputs()))))
        first_intent = create_f1_trade_intent(first)
        second_intent = create_f1_trade_intent(second)
        self.assertEqual(first.decision_hash, second.decision_hash)
        self.assertEqual(first.decision_hash, reordered.decision_hash)
        self.assertEqual(first_intent.intent_id, second_intent.intent_id)

    def test_issuance_replays_and_refuses_tampered_decision_or_non_entry(self) -> None:
        decision = TraderV0().decide(self.decision_input())
        with self.assertRaisesRegex(TradeIntentRefused, "F1_DECISION_REPLAY_MISMATCH"):
            create_f1_trade_intent(replace(decision, requested_notional_ceiling=999.0))
        with self.assertRaisesRegex(TradeIntentRefused, "F1_DECISION_IS_NOT_AN_ENTRY"):
            create_f1_trade_intent(TraderV0().decide(self.positioned_input()))

    def test_f0_phase_e_and_import_boundaries_remain_frozen(self) -> None:
        expected_f0 = json.loads(F0_MANIFEST_PATH.read_text(encoding="utf-8"))
        self.assertEqual(expected_f0["manifest_hash"], F0_AUTHORITY_MANIFEST_HASH)
        self.assertEqual(F0_MANIFEST.manifest_hash, F0_AUTHORITY_MANIFEST_HASH)
        self.assertFalse(F1_MANIFEST.record_for(AuthorityCapability.EXECUTION).granted)
        self.assertFalse(F1_MANIFEST.record_for(AuthorityCapability.TRADING).granted)
        self.assertFalse(F1_MANIFEST.record_for(AuthorityCapability.LIVE_CAPITAL).granted)
        source_tree = ast.parse((ROOT / "src" / "lane_ii" / "trader_v0.py").read_text(encoding="utf-8"))
        imports = [
            node.module for node in ast.walk(source_tree)
            if isinstance(node, ast.ImportFrom) and node.module
        ]
        self.assertFalse(any(name.startswith("src.phase_e") or name.startswith("src.copytrade.execution") for name in imports))
        before = FROZEN_PROTOCOL.read_bytes()
        with tempfile.TemporaryDirectory() as temp:
            e6 = PhaseE6Acquisition(Path(temp) / "e6.sqlite3", FROZEN_PROTOCOL)
            baseline = e6.status()
            TraderV0().decide(self.decision_input())
            after = e6.status()
        self.assertEqual(FROZEN_PROTOCOL.read_bytes(), before)
        self.assertEqual(hashlib.sha256(before).hexdigest(), hashlib.sha256(FROZEN_PROTOCOL.read_bytes()).hexdigest())
        self.assertEqual(baseline["outcome_access"]["scientific_evaluation_reads"], 0)
        self.assertEqual(after["outcome_access"]["scientific_evaluation_reads"], 0)
        self.assertEqual(after["reserved_test_queries"], 0)
        self.assertEqual(after["observation_count"], 0)
        self.assertFalse(any(after["authority"].values()))
        self.assertEqual(
            load_frozen_protocol(FROZEN_PROTOCOL)["identity"]["protocol_id"],
            "e5p-ae597d81614b76feba54168141de6a73",
        )
        self.assertEqual(
            load_frozen_protocol(FROZEN_PROTOCOL)["identity"]["protocol_hash"],
            "ae597d81614b76feba54168141de6a738876107639213a56a1c1aaa21c17c27f",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
