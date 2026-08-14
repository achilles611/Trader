from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from src.copytrade.analysis import CandidateAnalysisPipeline, _config_fingerprint
from src.copytrade.config import AnalysisConfig, ArtifactConfig, CandidateConfig, CopyTradeConfig, FinalistRequirementsConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.control_center import CONTROL_ENTRIES_PAUSED, CONTROL_RUNNING, CopyControlCenter
from src.copytrade.discovery import DiscoveryPipeline, HyperCoreNodeTradeDiscoveryProvider, IterableNodeTradeTransport
from src.copytrade.models import AnalysisRun, CandidateAnalysis, CandidateScore, DiscoveryObservation, RawFill, Target, utc_now
from src.copytrade.service import CopyTradeService
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x5555555555555555555555555555555555555555"
LEGACY = "0x6666666666666666666666666666666666666666"


class StaticProvider:
    source_name = "phase_c_authority_fixture"

    def __init__(self, wallets: tuple[str, ...] = (WALLET,)) -> None:
        self.wallets = wallets

    def discover(self, *, refresh: bool = False):
        now = utc_now()
        return iter(DiscoveryObservation(wallet, self.source_name, now, now, evidence_id=f"phase-c-evidence-{wallet}") for wallet in self.wallets)


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "phase-c.sqlite3", obsidian_root=root / "obsidian"),
        sizing=SizingConfig(min_history=1, max_equity_age_seconds=86_400),
        paper_execution=PaperExecutionConfig(fee_rate=0, slippage_bps=0, min_order_notional=1, random_seed=1),
        risk=RiskConfig(kill_switch_path=root / "kill", max_total_committed_fraction=1, max_capital_per_target_fraction=1,
                        max_capital_per_symbol_fraction=1, max_signal_age_seconds=86_400),
        candidates=CandidateConfig(history_days_min=0, closed_campaigns_min=0, max_drawdown_hard=1,
                                   require_positive_expectancy=False, require_positive_follower_expectancy=False,
                                   activity_max_age_days=30),
        analysis=AnalysisConfig(default_workers=1, retry_attempts=1, retry_initial_seconds=0, history_days=30,
                                min_discovery_activity=1, shadow_finalist_count=2, market_evidence_enabled=False),
        finalist_requirements=FinalistRequirementsConfig(minimum_confidence_score=0, require_copyability_evidence=False),
    )


def seed_phase_b_authority(
    service: CopyTradeService, *, recommendation_fingerprint: str | None = None,
    finalist_eligible: bool = True, reasons: tuple[str, ...] = (), run_status: str = "completed",
) -> tuple[CopyControlCenter, str]:
    database = service.database
    DiscoveryPipeline(database).run(StaticProvider(), limit=10, min_activity=1, max_activity_age=None)
    fingerprint = _config_fingerprint(service.config.research_snapshot())
    run_id = f"phase_b_{run_status}_{'eligible' if finalist_eligible else 'rejected'}"
    database.start_analysis_run(AnalysisRun(run_id=run_id, started_at=utc_now(), configuration={"config_fingerprint": fingerprint}))
    database.finish_analysis_run(
        run_id, status=run_status, wallets_considered=1, cheap_rejected=0, backfill_attempted=1,
        backfill_failed=0, reconstructed=1, scored=1, eligible=1, rejected=0, deferred=0,
    )
    database.upsert_candidate_analysis(CandidateAnalysis(
        WALLET, "qualified", run_id, started_at=utc_now(), completed_at=utc_now(),
        summary={"coverage": {"coverage_state": "PROVEN_COMPLETE"}},
    ))
    database.upsert_candidate_score(CandidateScore(
        WALLET, utc_now(), 99.0, {"quality": 99.0}, {}, True, provenance="phase_b",
        analysis_run_id=run_id, config_fingerprint=fingerprint, confidence_score=99.0,
    ))
    database.upsert_finalist_recommendations(recommendation_fingerprint or fingerprint, ({
        "analysis_run_id": run_id, "wallet": WALLET, "finalist_eligible": finalist_eligible,
        "finalist_rejection_reasons": reasons, "diversification_penalty": 0.5 if finalist_eligible else None,
        "final_selection_score": 42.5 if finalist_eligible else None,
        "selection_rank": 1 if finalist_eligible else None,
    },))
    return CopyControlCenter(service.config, database), fingerprint


def raw_fill(tid: int, side: str, position_before: float, *, wallet: str = WALLET) -> RawFill:
    now = utc_now()
    return RawFill.from_hyperliquid({
        "coin": "BTC", "px": "100", "sz": "1", "side": side, "time": int(now.timestamp() * 1000),
        "startPosition": str(position_before), "oid": tid, "tid": tid, "fee": "0", "accountValue": "1000",
    }, wallet)


class PhaseCAuthorityTests(unittest.TestCase):
    def test_generic_status_paths_cannot_activate_and_configuration_cannot_bootstrap_active(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            service.import_wallets([WALLET])
            with self.assertRaisesRegex(ValueError, "Direct Active transition"):
                service.set_status(WALLET, "active")
            self.assertEqual(service.database.get_target(WALLET).status, "pending")  # type: ignore[union-attr]
            center = CopyControlCenter(service.config, service.database)
            with self.assertRaisesRegex(ValueError, "Direct Active transition"):
                center._apply_operator_state(WALLET, "active", by="test", allow_overflow=False)
            self.assertEqual(service.database.get_target(WALLET).status, "pending")  # type: ignore[union-attr]
            for state in ("new", "queued", "approved", "shadow", "muted", "rejected", "pending"):
                service.set_status(WALLET, state)
                self.assertEqual(service.database.get_target(WALLET).status, state)  # type: ignore[union-attr]
            configured = replace(config(root / "configured"), targets=({"wallet": WALLET, "status": "active"},))
            with self.assertRaisesRegex(ValueError, "cannot enter Active"):
                CopyTradeService(configured)

    def test_deterministic_phase_a_to_b_to_c_flow_uses_versioned_evidence_and_persisted_finalists(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            observed_at = utc_now()
            hypercore_input = [
                {"user": wallet, "coin": "BTC", "side": "B", "time": int(observed_at.timestamp() * 1000),
                 "px": "100", "sz": "1", "tid": f"hypercore-{index}", "oid": index}
                for index, wallet in enumerate((WALLET, LEGACY), 1)
            ]
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport(hypercore_input))
            DiscoveryPipeline(service.database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            candidates = {row["wallet"]: row for row in service.database.list_discovery_candidates(limit=10)}
            phase_a_metadata = json.loads(candidates[WALLET]["metadata_json"])
            self.assertEqual(phase_a_metadata["evidence_schema_version"], 2)
            self.assertEqual(phase_a_metadata["cheap_stats"]["distinct_observed_events"], 1)
            # An old discovery candidate is safe to count for activity only;
            # it is explicitly held for refresh rather than penalized for
            # missing time/symbol measurements.
            with service.database._connect() as connection:  # type: ignore[attr-defined]
                connection.execute(
                    "UPDATE copy_discovery_candidates SET metadata_json=? WHERE wallet=?",
                    (json.dumps({"latest_activity_observations": 680}), LEGACY),
                )

            def backfill(wallet: str, start: object, end: object) -> dict[str, object]:
                now = utc_now() - timedelta(minutes=5)
                open_fill = RawFill.from_hyperliquid({
                    "coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": int(now.timestamp() * 1000),
                    "startPosition": "0", "oid": 100, "tid": 100, "fee": "0", "accountValue": "1000",
                }, wallet)
                close_fill = RawFill.from_hyperliquid({
                    "coin": "BTC", "px": "110", "sz": "1", "side": "A", "time": int((now + timedelta(minutes=1)).timestamp() * 1000),
                    "startPosition": "1", "oid": 101, "tid": 101, "fee": "0", "accountValue": "1000",
                }, wallet)
                service.database.insert_raw_fills((open_fill, close_fill))
                return {"new_raw_fills": 2}

            phase_b = CandidateAnalysisPipeline(service, backfill_wallet=backfill, sleep=lambda _: None)
            result = phase_b.run(limit=10, workers=1)
            self.assertEqual((result["cheap_rejected"], result["eligible"]), (1, 1))
            legacy_analysis = service.database.get_candidate_analysis(LEGACY)
            self.assertEqual(legacy_analysis.prefilter_reasons, ("phase_a_refresh_required",))  # type: ignore[union-attr]
            persisted = service.database.list_finalist_recommendations(_config_fingerprint(service.config.research_snapshot()))
            self.assertEqual([item["wallet"] for item in persisted if item["finalist_eligible"]], [WALLET])

            center = CopyControlCenter(service.config, service.database)
            self.assertEqual([item["wallet"] for item in center.shadow_finalists()], [WALLET])
            self.assertEqual(center.set_operator_state(WALLET, "active")["operator_state"], "active")

    def test_recommendation_contract_migration_is_additive_for_existing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            connection = sqlite3.connect(cfg.artifacts.database_path)
            try:
                connection.execute(
                    """CREATE TABLE copy_analysis_finalist_recommendations (
                        analysis_run_id TEXT NOT NULL, config_fingerprint TEXT NOT NULL, wallet TEXT NOT NULL,
                        finalist_eligible INTEGER NOT NULL, finalist_rejection_reasons_json TEXT NOT NULL DEFAULT '[]',
                        diversification_penalty REAL, final_selection_score REAL, selection_rank INTEGER,
                        evaluated_at TEXT NOT NULL, PRIMARY KEY(analysis_run_id, config_fingerprint, wallet))"""
                )
                connection.execute(
                    "INSERT INTO copy_analysis_finalist_recommendations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("old-run", "old-fingerprint", WALLET, 1, "[]", 0.0, 10.0, 1, "2026-01-01T00:00:00+00:00"),
                )
                connection.commit()
            finally:
                connection.close()
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            database.initialize()
            recommendation = database.get_finalist_recommendation("old-run", "old-fingerprint", WALLET)
            self.assertEqual(recommendation["recommendation_schema_version"], 1)  # type: ignore[index]
            self.assertEqual(recommendation["final_selection_score"], 10.0)  # type: ignore[index]

    def test_activation_requires_current_phase_b_finalist_authority_without_mutation_on_failure(self) -> None:
        cases = (
            ("low confidence recommendation", False, ("confidence_below_minimum",), "completed", None, False),
            ("copyability evidence missing", False, ("copyability_evidence_required",), "completed", None, False),
            ("stale recommendation", True, (), "completed", "stale-fingerprint", False),
            ("failed parent run", True, (), "failed", None, False),
            ("current persisted finalist", True, (), "completed", None, True),
            ("completed with errors finalist", True, (), "completed_with_errors", None, True),
        )
        for name, eligible, reasons, run_status, recommendation_fingerprint, succeeds in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                service = CopyTradeService(config(Path(temp)))
                center, fingerprint = seed_phase_b_authority(
                    service, recommendation_fingerprint=recommendation_fingerprint,
                    finalist_eligible=eligible, reasons=reasons, run_status=run_status,
                )
                if succeeds:
                    changed = center.set_operator_state(WALLET, "active")
                    self.assertEqual(changed["operator_state"], "active")
                    finalists = center.shadow_finalists()
                    self.assertEqual((finalists[0]["wallet"], finalists[0]["selection_score"]), (WALLET, 42.5))
                    self.assertEqual(finalists[0]["current_config_fingerprint"], fingerprint)
                else:
                    with self.assertRaises(ValueError):
                        center.set_operator_state(WALLET, "active")
                    self.assertEqual(service.database.get_target(WALLET).status, "new")  # type: ignore[union-attr]

    def test_activation_rejection_cases_leave_target_and_activity_unchanged(self) -> None:
        def mutate(name: str, service: CopyTradeService, fingerprint: str) -> None:
            with service.database._connect() as connection:  # type: ignore[attr-defined]
                if name == "non-qualified lifecycle":
                    connection.execute("UPDATE copy_candidate_analyses SET lifecycle_status='analyzed' WHERE wallet=?", (WALLET,))
                elif name == "missing score":
                    connection.execute("DELETE FROM copy_candidate_scores WHERE target_wallet=?", (WALLET,))
                elif name == "wrong score run":
                    connection.execute("UPDATE copy_candidate_scores SET analysis_run_id='other-run' WHERE target_wallet=?", (WALLET,))
                elif name == "legacy score provenance":
                    connection.execute("UPDATE copy_candidate_scores SET provenance='legacy' WHERE target_wallet=?", (WALLET,))
                elif name == "ineligible score":
                    connection.execute("UPDATE copy_candidate_scores SET eligible=0 WHERE target_wallet=?", (WALLET,))
                elif name == "stale score fingerprint":
                    connection.execute("UPDATE copy_candidate_scores SET config_fingerprint='stale' WHERE target_wallet=?", (WALLET,))
                elif name == "rejected parent run":
                    connection.execute("UPDATE copy_analysis_runs SET status='failed'")
                elif name == "missing recommendation":
                    connection.execute("DELETE FROM copy_analysis_finalist_recommendations WHERE wallet=?", (WALLET,))
                elif name == "unsupported recommendation schema":
                    connection.execute(
                        "UPDATE copy_analysis_finalist_recommendations SET recommendation_schema_version=999 WHERE wallet=?", (WALLET,)
                    )
                elif name == "not diversified selection":
                    connection.execute(
                        "UPDATE copy_analysis_finalist_recommendations SET selection_rank=NULL WHERE wallet=?", (WALLET,)
                    )
                else:
                    raise AssertionError(name)

        cases = (
            "non-qualified lifecycle", "missing score", "wrong score run", "legacy score provenance", "ineligible score",
            "stale score fingerprint", "rejected parent run", "missing recommendation", "unsupported recommendation schema",
            "not diversified selection",
        )
        for name in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                service = CopyTradeService(config(Path(temp)))
                center, fingerprint = seed_phase_b_authority(service)
                mutate(name, service, fingerprint)
                before = center.activity(limit=100, wallet=WALLET)
                with self.assertRaises(ValueError):
                    center.activate_wallet(WALLET)
                self.assertEqual(service.database.get_target(WALLET).status, "new")  # type: ignore[union-attr]
                self.assertEqual(center.activity(limit=100, wallet=WALLET), before)
                self.assertEqual(service.monitored_execution_wallets(), [])
                self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

    def test_canonical_activation_audits_only_after_authority_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            center, _ = seed_phase_b_authority(service)
            result = center.activate_wallet(WALLET, by="test-operator")
            self.assertEqual((result["operator_state"], result["previous_state"]), ("active", "new"))
            events = center.activity(limit=10, wallet=WALLET)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["category"], "operator")
            self.assertEqual(events[0]["payload"], {"from": "new", "to": "active", "by": "test-operator"})

    def test_activation_serializes_phase_b_authority_and_status_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            center, _ = seed_phase_b_authority(service)
            validate = center._validate_activation_authority

            def assert_write_lock(wallet: str, *, connection: sqlite3.Connection | None = None) -> dict[str, object]:
                self.assertIsNotNone(connection)
                with sqlite3.connect(service.config.artifacts.database_path, timeout=0) as contender:
                    with self.assertRaises(sqlite3.OperationalError):
                        contender.execute("UPDATE copy_candidate_analyses SET last_run_id='concurrent-run' WHERE wallet=?", (wallet,))
                return validate(wallet, connection=connection)

            with patch.object(center, "_validate_activation_authority", side_effect=assert_write_lock):
                self.assertEqual(center.activate_wallet(WALLET)["operator_state"], "active")

    def test_operational_rate_limit_settings_do_not_stale_phase_b_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            _, fingerprint = seed_phase_b_authority(service)
            historical_snapshot = service.config.snapshot()
            for field_name in (
                "api_weight_budget_per_minute",
                "rate_limit_backoff_initial_seconds",
                "rate_limit_backoff_max_seconds",
                "rate_limit_jitter_seconds",
            ):
                historical_snapshot["analysis"].pop(field_name)
            self.assertEqual(_config_fingerprint(historical_snapshot), fingerprint)
            changed_config = replace(
                service.config,
                analysis=replace(service.config.analysis, api_weight_budget_per_minute=700, rate_limit_jitter_seconds=0),
            )
            self.assertNotEqual(
                _config_fingerprint(service.config.snapshot()), _config_fingerprint(changed_config.snapshot()),
            )
            self.assertEqual(
                _config_fingerprint(service.config.research_snapshot()), _config_fingerprint(changed_config.research_snapshot()),
            )
            self.assertEqual(_config_fingerprint(changed_config.research_snapshot()), fingerprint)
            self.assertEqual(CopyControlCenter(changed_config, service.database).activate_wallet(WALLET)["operator_state"], "active")

    def test_activation_requires_an_existing_target_without_creating_activity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            center = CopyControlCenter(service.config, service.database)
            with self.assertRaises(KeyError):
                center.activate_wallet(WALLET)
            self.assertEqual(center.activity(limit=10, wallet=WALLET), [])

    def test_execution_entry_gates_preserve_raw_evidence_and_never_gate_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            # Fixture-only raw state: production entry uses the canonical
            # Phase-C activation test path above.
            service.database.upsert_target(Target(wallet=WALLET, status="active"))
            service.control_store.set_control_state(CONTROL_ENTRIES_PAUSED, by="test")
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(service.ingest_watched_fills(WALLET, [raw_fill(1, "B", 0)], False))
            self.assertEqual(len(service.database.list_raw_fills(WALLET)), 1)
            self.assertEqual(service.database.dashboard_snapshot()["execution_attempts"][0]["reason"], "paper_entries_paused")
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

            # A running control plus Active status can open.  Muting afterwards
            # must preserve that sleeve and still allow the later close.
            service.control_store.set_control_state(CONTROL_RUNNING, by="test")
            service.database.upsert_target(Target(wallet=WALLET, status="active"))
            fresh = CopyTradeService(config(Path(temp) / "fresh"))
            fresh.database.upsert_target(Target(wallet=WALLET, status="active"))
            asyncio.run(fresh.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(fresh.ingest_watched_fills(WALLET, [raw_fill(2, "B", 0)], False))
            self.assertEqual(len(fresh.database.list_virtual_positions(open_only=True)), 1)
            fresh.set_status(WALLET, "muted")
            self.assertIn(WALLET, fresh.monitored_execution_wallets())
            asyncio.run(fresh.ingest_watched_fills(WALLET, [raw_fill(3, "A", 1)], False))
            self.assertEqual(fresh.database.list_virtual_positions(open_only=True), [])


if __name__ == "__main__":
    unittest.main()
