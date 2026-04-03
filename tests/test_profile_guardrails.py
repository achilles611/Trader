from __future__ import annotations

import unittest
from pathlib import Path

from src.profile_loader import load_bot_definitions, load_runtime_settings
from src.safety.position_limits import validate_profile_limits


class ProfileGuardrailTests(unittest.TestCase):
    def test_default_swarm_profiles_fit_global_guardrails(self) -> None:
        root = Path(__file__).resolve().parents[1]
        settings = load_runtime_settings(root)
        bot_definitions = load_bot_definitions(settings)
        issues = validate_profile_limits(settings, bot_definitions)
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main()
