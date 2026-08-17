from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.copytrade.alpha import AlphaDecayPoint, alpha_half_life, alpha_survival
from src.copytrade.confidence import ConfidenceEngine, ConfidenceSnapshot, ModelEvidence, effective_confidence
from src.copytrade.decision import DecisionInput, DecisionRiskPolicy, DecisionType, ScientificDecisionEngine
from src.copytrade.experiments import ForwardShadowEngine, HistoricalExperimentEngine, TemporalSplit, benjamini_hochberg, block_sign_permutation_pvalue
from src.copytrade.features import FeatureDefinition, FeatureRegistry
from src.copytrade.hypotheses import HypothesisDefinition, HypothesisRegistry, HypothesisState
from src.copytrade.indicators import IndicatorProvenance, IndicatorRegistry, IndicatorState
from src.copytrade.science_repository import ScientificRepository
from src.copytrade.science_storage import ColdArchiveSpool, StorageRoots, migrate_sqlite_to_hot
from src.copytrade.scientific_models import ModelDefinition, ScientificModelRegistry


def hypothesis(version: int = 1, **changes: object) -> HypothesisDefinition:
    payload: dict[str, object] = {
        "hypothesis_id": "H-wallet-convergence", "version": version, "title": "Wallet convergence after cost",
        "scientific_statement": "Convergent wallet observations predict positive net edge.",
        "null_hypothesis": "Net edge is zero.", "alternative_hypothesis": "Net edge is positive after cost.",
        "feature_versions": (("wallet_convergence_count", 1),), "thresholds": {"wallets": 3},
        "symbol_scope": ("BTC",), "regime_scope": ("TREND",), "prediction_horizon_seconds": 120.0,
        "entry_definition": "Three fresh observations", "outcome_definition": "Net return at 120 seconds",
        "cost_model": {"fees": "taker"}, "detection_latency_ms": 250, "fee_model": {"bps": 5},
        "slippage_model": {"bps": 4}, "minimum_sample": 10,
        "discovery_range": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-10T00:00:00Z"},
        "validation_range": {"start": "2026-01-11T00:10:00Z", "end": "2026-01-20T00:00:00Z"},
        "purge_embargo_seconds": 300.0, "success_criteria": {"net_expectancy": ">0"},
        "failure_criteria": {"net_expectancy": "<=0"}, "multiple_testing_family": "wallet-signals",
        "registered_at": "2026-01-01T00:00:00Z", "code_sha": "abc123", "data_fingerprints": {"fills": "fp-1"},
    }
    payload.update(changes)
    return HypothesisDefinition(**payload)  # type: ignore[arg-type]


class ScientificAlphaEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "hot" / "science.sqlite3"
        self.repository = ScientificRepository(self.path)
        self.repository.initialize()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_features_and_registered_hypotheses_are_semantically_immutable(self) -> None:
        feature = FeatureDefinition("wallet_convergence_count", 1, "Count unique agreeing wallets in 20 seconds", "count", ("wallet_fills",), 20, "missing=unavailable", "2026-01-01T00:00:00Z", "abc123", "wallet")
        registry = FeatureRegistry(self.repository)
        registry.register(feature)
        with self.assertRaisesRegex(ValueError, "new feature version"):
            registry.register(FeatureDefinition("wallet_convergence_count", 1, "Changed count", "count", ("wallet_fills",), 20, "missing=unavailable", "2026-01-01T00:00:00Z", "abc123", "wallet"))
        hypotheses = HypothesisRegistry(self.repository)
        hypotheses.register(hypothesis())
        with self.assertRaisesRegex(ValueError, "new version"):
            hypotheses.register(hypothesis(thresholds={"wallets": 2}))

    def test_temporal_purge_and_seeded_statistics_are_reproducible(self) -> None:
        split = TemporalSplit("2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z", "2026-01-10T00:07:00Z", "2026-01-20T00:00:00Z", 120, 300)
        train, validation = split.partition([
            {"id": "train", "timestamp": "2026-01-09T00:00:00Z"},
            {"id": "purged", "timestamp": "2026-01-10T00:03:00Z"},
            {"id": "validation", "timestamp": "2026-01-11T00:00:00Z"},
        ])
        self.assertEqual([item["id"] for item in train], ["train"])
        self.assertEqual([item["id"] for item in validation], ["validation"])
        with self.assertRaisesRegex(ValueError, "purge/embargo"):
            TemporalSplit("2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z", "2026-01-10T00:01:00Z", "2026-01-20T00:00:00Z", 120, 300)
        values = [0.01, -0.003, 0.004, 0.005] * 5
        self.assertEqual(block_sign_permutation_pvalue(values, seed=4), block_sign_permutation_pvalue(values, seed=4))
        self.assertEqual(benjamini_hochberg([0.01, 0.04, 0.03]), [0.03, 0.04, 0.04])

    def test_experiment_costs_and_graveyard_are_durable(self) -> None:
        definition = hypothesis()
        hypotheses = HypothesisRegistry(self.repository)
        hypotheses.register(definition)
        hypotheses.transition(definition, from_state=HypothesisState.REGISTERED, to_state=HypothesisState.HISTORICAL_TESTING, reason="started", event_id="event-1", created_at="2026-01-21T00:00:00Z")
        self.repository.create_experiment("exp-1", hypothesis_id=definition.hypothesis_id, hypothesis_version=1, kind="HISTORICAL", state="RUNNING", dataset_fingerprint="data-1", configuration={"split": "fixed"}, created_at="2026-01-21T00:00:00Z")
        result = HistoricalExperimentEngine(self.repository, seed=3, resamples=20).evaluate(experiment_id="exp-1", records=[{"gross_return": 0.01, "fee": 0.002, "slippage_cost": 0.001}] * 12, minimum_sample=10)
        self.assertAlmostEqual(result["net_expectancy"], 0.007)
        self.assertIn("p_value", result); self.assertIsNone(result["q_value"])
        self.repository.record_experiment_result("exp-1", result, recorded_at="2026-01-22T00:00:00Z")
        family = HistoricalExperimentEngine(self.repository, seed=3, resamples=20).evaluate_family([
            ("a", [{"gross_return": .01}] * 10, 10), ("b", [{"gross_return": -.01}] * 10, 10),
        ])
        self.assertTrue(all(item["q_value"] is not None for item in family.values()))
        hypotheses.reject(definition, experiment_id="exp-1", reason="policy rejection", result=result, recorded_at="2026-01-22T00:00:00Z", event_id="event-2")
        self.assertEqual(self.repository.list_graveyard()[0]["reason"], "policy rejection")

    def test_forward_predictions_precede_and_cannot_mutate_outcomes(self) -> None:
        definition = hypothesis()
        HypothesisRegistry(self.repository).register(definition)
        self.repository.create_experiment("forward-1", hypothesis_id=definition.hypothesis_id, hypothesis_version=1, kind="FORWARD", state="RUNNING", dataset_fingerprint="data-1", configuration={"shadow": True}, created_at="2026-01-21T00:00:00Z")
        forward = ForwardShadowEngine(self.repository)
        with self.assertRaises(KeyError):
            forward.resolve("missing", realized_at="2026-01-21T00:02:00Z", realized_net_outcome=0.01)
        forward.predict("prediction-1", experiment_id="forward-1", predicted_at="2026-01-21T00:00:00Z", market="BTC", horizon_seconds=60, features={"wallet_convergence_count@1": 3}, predicted_direction="long", predicted_net_edge=0.004, trade_confidence=0.7, model_confidence=0.6, expected_costs=0.001)
        forward.resolve("prediction-1", realized_at="2026-01-21T00:02:00Z", realized_net_outcome=0.01)
        with self.assertRaises(sqlite3.IntegrityError):
            forward.resolve("prediction-1", realized_at="2026-01-21T00:03:00Z", realized_net_outcome=-0.01)

    def test_indicator_and_model_versions_require_provenance(self) -> None:
        provenance = IndicatorProvenance("H-wallet-convergence", 1, "historical-1", (), (("wallet_convergence_count", 1),), {}, {"data": "fp"}, "abc123", ("TREND",), (), ("CHOP",), (), 120, (), {"robust": True}, {"samples": 100})
        indicators = IndicatorRegistry(self.repository)
        with self.assertRaisesRegex(ValueError, "forward-shadow"):
            indicators.register("I-002", 1, provenance, state=IndicatorState.VALIDATED, created_at="2026-01-01T00:00:00Z")
        indicators.register("I-002", 1, provenance, state=IndicatorState.EXPERIMENTAL, created_at="2026-01-01T00:00:00Z")
        model = ModelDefinition("M-001", 1, (("I-002", 1),), {"start": "a", "end": "b"}, {"start": "c", "end": "d"}, {"weight": 1}, {"method": "platt"}, "abc123", "hash", {"net": 0.1}, "2026-01-01T00:00:00Z")
        ScientificModelRegistry(self.repository).register(model)
        self.assertEqual(self.repository.list_models()[0]["model_id"], "M-001")

    def test_confidence_is_separate_dynamic_and_shrunk(self) -> None:
        evidence = ModelEvidence(.9, .8, .8, None, .8, .9, .9, .9, .8, experimental_ceiling=.65)
        engine = ConfidenceEngine()
        model = engine.model_confidence(evidence)
        self.assertLessEqual(model, .65)
        decline, _ = engine.trade_confidence(.80, evidence_updates={"confirmation_absent": -1.0})
        rise, _ = engine.trade_confidence(.80, evidence_updates={"fresh_corroboration": .7})
        self.assertLess(decline, .80); self.assertGreater(rise, .80)
        self.assertEqual(effective_confidence(.9, 0), .5)
        self.assertLess(effective_confidence(.9, .2), .9)

    def test_alpha_decay_and_decision_gates(self) -> None:
        self.assertEqual(alpha_survival(-.01, .02), 0)
        self.assertEqual(alpha_half_life([AlphaDecayPoint(0, .02), AlphaDecayPoint(5, .014), AlphaDecayPoint(9, .009)]), 9)
        confidence = ConfidenceSnapshot(.8, .8, .74, ("validated evidence",))
        policy = DecisionRiskPolicy(100, 1000, 5, 2000, entry_min_effective_confidence=.7, exit_effective_confidence=.6)
        raw_only = DecisionInput("2026-01-01T00:00:00Z", "BTC", "long", "M", 1, "ACTIVE_SIMULATION", (), {}, confidence, .01, .001, 0, 0, 0, 0, .02, None, 1, 1, "TREND", source_wallet_action={"wallet": "0xraw"}, source_wallet_leverage=50)
        engine = ScientificDecisionEngine(self.repository)
        self.assertEqual(engine.decide(raw_only, policy).decision, DecisionType.SKIP)
        gated = DecisionInput(**{**raw_only.__dict__, "active_indicator_versions": (("I-002", 1),)})
        entry = engine.decide(gated, policy)
        self.assertEqual(entry.decision, DecisionType.ENTER)
        self.assertLessEqual(entry.derived_leverage, policy.max_leverage)
        self.assertTrue(entry.payload["provenance"]["source_wallet_leverage_ignored"])
        exit_item = DecisionInput(**{**gated.__dict__, "position_open": True, "position_age_seconds": 600})
        self.assertEqual(engine.decide(exit_item, policy).decision, DecisionType.EXIT)
        with self.assertRaisesRegex(ValueError, "600"):
            DecisionRiskPolicy(1, 1, 1, 1, max_position_age_seconds=601)

    def test_hot_cold_spool_and_sqlite_migration(self) -> None:
        root = Path(self.temp.name)
        roots = StorageRoots(home=root, hot_root=root / "hot", cold_root=root / "cold")
        spool = ColdArchiveSpool(roots, max_bytes=10000, max_age_seconds=60)
        self.assertEqual(spool.flush_once()["state"], "DEGRADED_ARCHIVAL")
        spool.enqueue("decision-log", [{"decision": "SKIP", "private_key": "never archive"}])
        roots.ensure_cold()
        flushed = spool.flush_once()
        self.assertEqual(flushed["flushed"], 1)
        contents = next((roots.cold_root / "archives").rglob("*.jsonl")).read_text(encoding="utf-8")
        self.assertNotIn("never archive", contents)
        legacy, destination = root / "legacy.sqlite3", root / "hot" / "active.sqlite3"
        with closing(sqlite3.connect(legacy)) as connection:
            connection.execute("CREATE TABLE evidence(value TEXT)")
            connection.execute("INSERT INTO evidence VALUES ('kept')")
            connection.commit()
        migrated = migrate_sqlite_to_hot(source=legacy, destination=destination, roots=roots)
        self.assertEqual(migrated["state"], "MIGRATED")
        with closing(sqlite3.connect(destination)) as connection:
            self.assertEqual(connection.execute("SELECT value FROM evidence").fetchone()[0], "kept")

    def test_environment_roots_route_active_database_and_reports_cold_absence(self) -> None:
        from src.copytrade.config import CopyTradeConfig
        prior = {name: os.environ.get(name) for name in ("BEELZEBUB_HOME", "BEELZEBUB_HOT_ROOT", "BEELZEBUB_COLD_ROOT")}
        try:
            os.environ["BEELZEBUB_HOME"] = self.temp.name
            os.environ.pop("BEELZEBUB_HOT_ROOT", None)
            os.environ["BEELZEBUB_COLD_ROOT"] = str(Path(self.temp.name) / "missing-cold")
            config = CopyTradeConfig.from_yaml("config/copytrade.yaml")
            self.assertEqual(config.artifacts.database_path, Path(self.temp.name) / "runtime" / "hot" / "copytrade.sqlite3")
            self.assertEqual(config.storage.cold_root, Path(self.temp.name) / "missing-cold")
            self.assertFalse(StorageRoots.from_environment().cold_status()["cold_available"])
        finally:
            for name, value in prior.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def test_control_center_exposes_read_only_scientific_routes(self) -> None:
        from dataclasses import replace
        from src.copytrade.config import CopyTradeConfig
        from src.copytrade.control_center import create_control_center_app
        base = CopyTradeConfig()
        config = replace(base, artifacts=replace(base.artifacts, database_path=Path(self.temp.name) / "control.sqlite3"))
        paths = {route.path for route in create_control_center_app(config).routes}
        self.assertTrue({"/api/science/health", "/api/science/ecosystem", "/api/wallet-sensors", "/api/hypotheses", "/api/experiments", "/api/indicators", "/api/models", "/api/confidence", "/api/decisions", "/api/graveyard", "/api/storage"}.issubset(paths))

    def test_production_science_mode_blocks_raw_wallet_action_from_paper_execution(self) -> None:
        from src.copytrade.config import CopyTradeConfig, ScientificExecutionConfig
        from src.copytrade.models import CopySignal, PositionEvent, PositionEventType
        from src.copytrade.service import CopyTradeService
        config = replace(
            CopyTradeConfig(), artifacts=replace(CopyTradeConfig().artifacts, database_path=Path(self.temp.name) / "science-service.sqlite3"),
            scientific_execution=ScientificExecutionConfig(enabled=True),
        )
        service = CopyTradeService(config)
        now = datetime.now(timezone.utc)
        event = PositionEvent("event-1", "0x1111111111111111111111111111111111111111", "BTC", PositionEventType.OPEN, "long", 1, 0, 1, 100, 100, now, "campaign-1", ("fill-1",))
        signal = CopySignal("signal-1", event.target_wallet, event.campaign_id, event.event_id, "BTC", "open", "long", 100, 1, 100, .1, 10, now, now, target_leverage=50)
        service._execute_reconstructed_signal(service._execution_engine(), event, signal, "CONTINUOUS")
        attempt = service.database.get_execution_attempt(signal.signal_id)
        self.assertEqual(attempt.reason, "scientific_decision_required")
        self.assertEqual(attempt.status, "skipped")
        self.assertFalse(service.database.list_virtual_positions(open_only=True))
        self.assertEqual(service.science_repository.list_decisions()[0]["decision"], "SKIP")  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
