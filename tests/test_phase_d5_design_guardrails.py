from __future__ import annotations

import tempfile
import unittest
from dataclasses import fields
from pathlib import Path

from src.copytrade.config import CopyTradeConfig, SourceConfig
from src.copytrade.execution import ExecutionEngine
from src.copytrade.shadow import HyperliquidReadOnlyShadowAdapter
from src.copytrade.storage import CopyTradeDatabase


class _PretendLiveAdapter:
    adapter_name = "pretend_live_adapter"
    adapter_mode = "LIVE_TESTNET"


class PhaseD5DesignGuardrailTests(unittest.TestCase):
    """Permanent D.5 boundaries that remain true after live work is added elsewhere."""

    def test_frozen_execution_engine_remains_simulator_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CopyTradeDatabase(Path(directory) / "copytrade.sqlite3")
            store.initialize()
            with self.assertRaisesRegex(ValueError, "SIMULATOR_ONLY"):
                ExecutionEngine(store, _PretendLiveAdapter())  # type: ignore[arg-type]

    def test_d4_shadow_adapter_has_no_write_capability(self) -> None:
        adapter = HyperliquidReadOnlyShadowAdapter(SourceConfig())
        for name in ("submit", "cancel", "modify", "amend", "sign", "withdraw", "transfer"):
            self.assertFalse(hasattr(adapter, name), name)

    def test_copytrade_config_contains_no_signer_secret_fields(self) -> None:
        names = {item.name.lower() for item in fields(CopyTradeConfig)}
        forbidden = {
            "private_key", "secret_key", "seed", "seed_phrase",
            "mnemonic", "api_secret", "signer_secret",
        }
        self.assertTrue(names.isdisjoint(forbidden), names & forbidden)

    def test_legacy_copytrade_flags_still_cannot_enable_live_execution(self) -> None:
        config = CopyTradeConfig(mode="live", live_enabled=True)
        with self.assertRaisesRegex(ValueError, "Live copy trading is not implemented"):
            config.validate()


if __name__ == "__main__":
    unittest.main()
