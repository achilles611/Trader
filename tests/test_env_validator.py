from __future__ import annotations

import os
import unittest
from pathlib import Path

from src.config.env_validator import validate_environment
from src.profile_loader import load_runtime_settings


class EnvValidatorTests(unittest.TestCase):
    def test_missing_env_keys_reported_for_dry_run(self) -> None:
        root = Path(__file__).resolve().parents[1]
        original = {key: os.environ.get(key) for key in ("DRY_RUN", "LIVE_TRADING_ENABLED", "PATCHING_ENABLED", "SCHEDULER_TIMEZONE", "TRADER_ARTIFACT_ROOT", "TRADER_DB_PATH", "RUN_LOCK_PATH", "OPENAI_MODEL_ANALYSIS", "OPENAI_API_KEY")}
        try:
            for key in original:
                os.environ.pop(key, None)
            settings = load_runtime_settings(root)
            report = validate_environment(settings, root_dir=root)
            self.assertFalse(report.valid)
            self.assertIn("DRY_RUN", report.missing_keys)
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
