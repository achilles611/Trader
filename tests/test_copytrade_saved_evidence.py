from __future__ import annotations

import hashlib
import gc
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.copytrade.analysis import _config_fingerprint
from src.copytrade.config import CopyTradeConfig
from src.copytrade.hyperliquid import BackfillCoverage
from src.copytrade.models import AnalysisRun, RawFill
from src.copytrade.saved_evidence import SavedEvidenceOnlyError, evaluate_saved_evidence
from src.copytrade.service import CopyTradeService


GOOD = "0x1111111111111111111111111111111111111111"
INCOMPLETE = "0x2222222222222222222222222222222222222222"
PENDING = "0x3333333333333333333333333333333333333333"


def _fill(wallet: str, identifier: int, *, side: str, when: datetime, price: int, position: int) -> RawFill:
    return RawFill.from_hyperliquid({
        "coin": "BTC", "px": str(price), "sz": "1", "side": side,
        "time": int(when.timestamp() * 1000), "startPosition": str(position),
        "oid": identifier, "tid": identifier, "fee": "0", "accountValue": "1000",
    }, wallet)


def _backup(source: Path, destination: Path) -> None:
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as reader, sqlite3.connect(destination) as writer:
        reader.backup(writer)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class SavedEvidenceOnlyTests(unittest.TestCase):
    def _config(self, root: Path) -> CopyTradeConfig:
        base = CopyTradeConfig.from_yaml("config/copytrade_lane_ii_100.yaml")
        return replace(
            base,
            artifacts=replace(base.artifacts, database_path=root / "active.sqlite3", obsidian_root=root / "obsidian"),
            candidates=replace(
                base.candidates,
                history_days_min=90,
                history_days_preferred=90,
                closed_campaigns_min=1,
                max_drawdown_hard=1.0,
                max_follower_drawdown_hard=1.0,
                liquidation_frequency_hard=1.0,
                minimum_copyability_hard=0.0,
                pnl_concentration_hard=1.0,
            ),
            scientific_execution=replace(base.scientific_execution, enabled=False),
            scientific_worker=replace(base.scientific_worker, enabled=False),
        )

    def _manifest(self, wallet: str, at: datetime) -> dict[str, object]:
        return {
            "wallet": wallet,
            "current_status": "new",
            "recent_activity_at": at.isoformat(),
            "metadata": {"cheap_stats": {"first_observed_activity": at.isoformat(), "last_observed_activity": at.isoformat()}},
        }

    def test_saved_only_analysis_preserves_snapshot_and_pending_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self._config(root)
            service = CopyTradeService(config)
            service.import_wallets([GOOD, INCOMPLETE, PENDING])
            end = datetime(2026, 9, 5, tzinfo=timezone.utc)
            start = end - timedelta(days=100)
            service.database.insert_raw_fills([
                _fill(GOOD, 1, side="B", when=start, price=100, position=0),
                _fill(GOOD, 2, side="A", when=end - timedelta(days=1), price=110, position=1),
                # Pending raw data proves only that rows exist; the lifecycle
                # must remain pending until its saved backfill record completes.
                _fill(PENDING, 3, side="B", when=start, price=100, position=0),
                _fill(PENDING, 4, side="A", when=end - timedelta(days=1), price=110, position=1),
            ])
            service.database.insert_backfill_coverage(GOOD, BackfillCoverage(
                requested_start=start, requested_end=end, earliest_observed_fill=start,
                latest_observed_fill=end - timedelta(days=1), source_limit_detected=False,
                coverage_complete=True, coverage_quality="fixture", coverage_state="PROVEN_COMPLETE",
            ))
            service.database.insert_backfill_coverage(PENDING, BackfillCoverage(
                requested_start=start, requested_end=end, earliest_observed_fill=start,
                latest_observed_fill=end - timedelta(days=1), source_limit_detected=True,
                coverage_complete=False, coverage_quality="fixture", coverage_state="UNPROVEN",
            ))
            service.database.insert_backfill_coverage(INCOMPLETE, BackfillCoverage(
                requested_start=start, requested_end=end, earliest_observed_fill=None,
                latest_observed_fill=None, source_limit_detected=True,
                coverage_complete=False, coverage_quality="fixture", coverage_state="KNOWN_INCOMPLETE",
            ))
            run_id = "analysis_saved_evidence_fixture"
            configuration = {
                "config_fingerprint": _config_fingerprint(config.research_snapshot()),
                "analysis_window": {"required_start": start.isoformat(), "required_end": end.isoformat()},
                "candidate_manifest": [self._manifest(wallet, end - timedelta(days=1)) for wallet in (GOOD, INCOMPLETE, PENDING)],
            }
            service.database.start_analysis_run(AnalysisRun(run_id, start, configuration))
            service.database.record_analysis_wallet(run_id, GOOD, stage="backfill", status="completed")
            service.database.record_analysis_wallet(run_id, INCOMPLETE, stage="backfill", status="started")
            service.database.record_analysis_wallet(run_id, PENDING, stage="backfill", status="started")

            snapshot = root / "recovery-snapshot.sqlite3"
            _backup(config.artifacts.database_path, snapshot)
            source_hash = _sha256(snapshot)
            report = evaluate_saved_evidence(
                config=config,
                snapshot_database=snapshot,
                output_directory=root / "saved-evidence-output",
                original_run_id=run_id,
                workspace=root,
            )

            self.assertEqual(report["network_guard"], {"enabled": True, "blocked_attempts": 0, "network_requests": 0})
            self.assertTrue(report["input_snapshot"]["unchanged"])
            self.assertEqual(source_hash, _sha256(snapshot))
            rows = {item["wallet"]: item for item in report["wallet_results"]}
            self.assertTrue(rows[GOOD]["replay_performed"])
            self.assertEqual(rows[GOOD]["canonical_score"]["score_version"], "phase_b_suitability_v3")
            self.assertIn("modeled_costs", rows[GOOD]["follower"])
            self.assertEqual(rows[INCOMPLETE]["assessment"], "QUARANTINED_KNOWN_INCOMPLETE")
            self.assertEqual(rows[PENDING]["assessment"], "PENDING_SAVED_ACQUISITION")
            self.assertEqual(rows[PENDING]["available_history"]["fill_count"], 2)
            self.assertFalse(report["authority"]["cohort_selection_completed"])

    def test_snapshot_and_output_must_stay_inside_recovery_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temp, tempfile.TemporaryDirectory() as outside:
            root, external = Path(temp), Path(outside)
            config = self._config(root)
            with self.assertRaises(SavedEvidenceOnlyError):
                evaluate_saved_evidence(
                    config=config,
                    snapshot_database=external / "outside.sqlite3",
                    output_directory=root / "out",
                    original_run_id="not-used",
                    workspace=root,
                )

    def test_unresolved_source_position_is_reported_not_scored_as_performance(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self._config(root)
            service = CopyTradeService(config)
            service.import_wallets([GOOD])
            end = datetime(2026, 9, 5, tzinfo=timezone.utc)
            start = end - timedelta(days=100)
            # Same timestamp and same source boundary have no causal ordering.
            service.database.insert_raw_fills([
                _fill(GOOD, 1, side="B", when=start, price=100, position=0),
                _fill(GOOD, 2, side="B", when=start, price=101, position=0),
            ])
            service.database.insert_backfill_coverage(GOOD, BackfillCoverage(
                requested_start=start, requested_end=end, earliest_observed_fill=start,
                latest_observed_fill=start, source_limit_detected=False,
                coverage_complete=True, coverage_quality="fixture", coverage_state="PROVEN_COMPLETE",
            ))
            run_id = "analysis_saved_evidence_unresolved_fixture"
            configuration = {
                "config_fingerprint": _config_fingerprint(config.research_snapshot()),
                "analysis_window": {"required_start": start.isoformat(), "required_end": end.isoformat()},
                "candidate_manifest": [self._manifest(GOOD, end - timedelta(days=1))],
            }
            service.database.start_analysis_run(AnalysisRun(run_id, start, configuration))
            service.database.record_analysis_wallet(run_id, GOOD, stage="backfill", status="completed")
            snapshot = root / "recovery-snapshot.sqlite3"
            _backup(config.artifacts.database_path, snapshot)
            source_hash = _sha256(snapshot)

            report = evaluate_saved_evidence(
                config=config,
                snapshot_database=snapshot,
                output_directory=root / "saved-evidence-output",
                original_run_id=run_id,
                workspace=root,
            )

            row = report["wallet_results"][0]
            self.assertEqual(row["assessment"], "INTEGRITY_UNRESOLVED_SOURCE_POSITION_CONTINUITY")
            self.assertEqual(row["assessment_reasons"], ["UNRESOLVED_SOURCE_POSITION_DUPLICATE_BOUNDARY"])
            self.assertEqual(row["campaigns"]["status"], "integrity_unresolved")
            self.assertFalse(row["replay_performed"])
            self.assertEqual(report["summary"]["integrity_unresolved_wallets"], 1)
            self.assertEqual(source_hash, _sha256(snapshot))
            # Windows keeps an SQLite handle alive until the final local
            # service reference is collected; make cleanup deterministic.
            del report
            del service
            gc.collect()
