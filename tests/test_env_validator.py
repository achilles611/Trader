from __future__ import annotations

import os
import unittest
from pathlib import Path

from src.config.env_validator import validate_environment
from src.profile_loader import load_runtime_settings


class EnvValidatorTests(unittest.TestCase):
    def test_missing_env_keys_reported_for_dry_run(self) -> None:
        root = Path(__file__).resolve().parents[1]
        original = {
            key: os.environ.get(key)
            for key in (
                "DRY_RUN",
                "SWARM_DRY_RUN",
                "LIVE_TRADING_ENABLED",
                "PATCHING_ENABLED",
                "SCHEDULER_TIMEZONE",
                "TRADER_ARTIFACT_ROOT",
                "ARTIFACT_ROOT",
                "TRADER_DB_PATH",
                "SQLITE_DB_PATH",
                "RUN_LOCK_PATH",
                "OPENAI_MODEL_ANALYSIS",
                "OPENAI_API_KEY",
                "AI_ANALYSIS_ENABLED",
            )
        }
        try:
            for key in original:
                os.environ.pop(key, None)
            settings = load_runtime_settings(root)
            report = validate_environment(settings, root_dir=root)
            self.assertFalse(report.valid)
            self.assertEqual(report.missing_keys, ["OPENAI_API_KEY"])
            check_map = {check.key: check for check in report.checks}
            self.assertTrue(check_map["DRY_RUN"].present)
            self.assertIn(check_map["DRY_RUN"].source, {"env", "runtime"})
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_analysis_key_not_required_when_analysis_disabled(self) -> None:
        root = Path(__file__).resolve().parents[1]
        original = {key: os.environ.get(key) for key in ("AI_ANALYSIS_ENABLED", "OPENAI_API_KEY")}
        try:
            os.environ["AI_ANALYSIS_ENABLED"] = "false"
            os.environ.pop("OPENAI_API_KEY", None)
            settings = load_runtime_settings(root)
            report = validate_environment(settings, root_dir=root)
            self.assertTrue(report.valid)
            self.assertEqual(report.missing_keys, [])
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
