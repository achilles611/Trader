from __future__ import annotations

import unittest
from pathlib import Path

from src.profile_loader import load_bot_definitions, load_runtime_settings


class ProfileLoaderTests(unittest.TestCase):
    def test_loads_all_expected_bot_profiles(self) -> None:
        root = Path(__file__).resolve().parents[1]
        settings = load_runtime_settings(root)
        definitions = load_bot_definitions(settings)
        self.assertEqual(10, len(definitions))
        self.assertEqual({"zerk1", "zerk2", "tr1", "tr2", "tr3", "tr4", "tr5", "tr6", "tr7", "tr8"}, {item.bot_id for item in definitions})


if __name__ == "__main__":
    unittest.main()
