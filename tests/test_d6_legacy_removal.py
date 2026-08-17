from __future__ import annotations

import unittest
from pathlib import Path


class LegacyEthRemovalTests(unittest.TestCase):
    """Prevent the retired independent ETH/Coinbase bot from returning."""

    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1]

    def test_legacy_subsystem_and_coinbase_dependency_are_absent(self) -> None:
        self.assertFalse((self.root / "src" / "eth_bot").exists())
        requirements = (self.root / "requirements.txt").read_text(encoding="utf-8").lower()
        lock = (self.root / "requirements.lock").read_text(encoding="utf-8").lower()
        self.assertNotIn("coinbase", requirements)
        self.assertNotIn("coinbase", lock)

    def test_production_python_has_no_legacy_import_or_live_flag(self) -> None:
        for path in (self.root / "src").rglob("*.py"):
            text = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("src.eth_bot", text, path)
            self.assertNotIn("from .eth_bot", text, path)
            self.assertNotIn("live_trading_enabled", text, path)
            self.assertNotIn("bot_trading_enabled", text, path)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
