from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"


class CopytradeStorageHardeningTests(unittest.TestCase):
    def test_fresh_database_initializes_discovery_audit_and_phase_b_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(Path(temp) / "fresh.sqlite3")
            database.initialize()
            with database._connect() as connection:  # type: ignore[attr-defined]
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                indexes = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
            self.assertTrue({"copy_discovery_rejections", "copy_candidate_score_archive"}.issubset(tables))
            self.assertIn("idx_copy_phase_b_score_authority", indexes)

    def test_pre_migration_duplicate_phase_b_scores_are_archived_before_authority_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "legacy.sqlite3"
            with sqlite3.connect(path) as connection:
                connection.execute(
                    """CREATE TABLE copy_candidate_scores (
                        target_wallet TEXT NOT NULL, calculated_at TEXT NOT NULL, total_score REAL NOT NULL,
                        component_scores_json TEXT NOT NULL, penalties_json TEXT NOT NULL, eligible INTEGER NOT NULL,
                        reasons_json TEXT NOT NULL, source_quality REAL NOT NULL, provenance TEXT NOT NULL DEFAULT 'legacy',
                        analysis_run_id TEXT, config_fingerprint TEXT, PRIMARY KEY(target_wallet, calculated_at)
                    )"""
                )
                values = (WALLET, "2026-01-01T00:00:00+00:00", 10.0, "{}", "{}", 1, "[]", 1.0,
                          "phase_b", "analysis-run", "config-a")
                connection.execute("INSERT INTO copy_candidate_scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
                connection.execute(
                    "INSERT INTO copy_candidate_scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (WALLET, "2026-01-01T00:00:01+00:00", 20.0, "{}", "{}", 1, "[]", 1.0,
                    "phase_b", "analysis-run", "config-a"),
                )
            connection.close()
            database = CopyTradeDatabase(path)
            database.initialize()
            with database._connect() as connection:  # type: ignore[attr-defined]
                active = connection.execute(
                    "SELECT total_score FROM copy_candidate_scores WHERE target_wallet=? AND analysis_run_id=?",
                    (WALLET, "analysis-run"),
                ).fetchall()
                archived = connection.execute(
                    "SELECT total_score, archive_reason FROM copy_candidate_score_archive"
                ).fetchall()
            self.assertEqual([row[0] for row in active], [20.0])
            self.assertEqual([(row[0], row[1]) for row in archived], [(10.0, "duplicate_phase_b_authority_migration")])
            # Reinitialization is idempotent: no archived evidence is lost or duplicated.
            database.initialize()
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_candidate_score_archive").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
