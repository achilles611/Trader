from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.copytrade.cli import run_copytrade_command
from src.copytrade.config import ArtifactConfig, CopyTradeConfig, RiskConfig
from src.copytrade.discovery import (
    DiscoveryPipeline,
    DiscoveryProviderError,
    HyperCoreNodeTradeDiscoveryProvider,
    IterableNodeTradeTransport,
)
from src.copytrade.models import DiscoveryObservation, TargetStatus, utc_now
from src.copytrade.storage import CopyTradeDatabase


BUYER = "0x1111111111111111111111111111111111111111"
SELLER = "0x2222222222222222222222222222222222222222"
THIRD = "0x3333333333333333333333333333333333333333"
LOW_ACTIVITY = "0x4444444444444444444444444444444444444444"
T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copytrade.sqlite3", obsidian_root=root / "obsidian"),
        risk=RiskConfig(kill_switch_path=root / "kill"),
    )


def trade(timestamp: datetime, buyer: str = BUYER, seller: str = SELLER) -> dict[str, object]:
    return {
        "coin": "BTC", "side": "B", "time": timestamp.isoformat().replace("+00:00", "Z"), "px": "100", "sz": "2",
        "hash": f"0x{int(timestamp.timestamp()):064x}",
        "side_info": [{"user": buyer, "start_pos": "0", "oid": 1}, {"user": seller, "start_pos": "0", "oid": 2}],
    }


class StaticProvider:
    def __init__(self, observations: list[DiscoveryObservation], source_name: str = "fixture") -> None:
        self.observations = observations
        self.source_name = source_name

    def discover(self, *, refresh: bool = False):
        return iter(self.observations)


class FailingProvider:
    source_name = "failing_fixture"

    def discover(self, *, refresh: bool = False):
        raise OSError("fixture transport unavailable")


class CopytradeDiscoveryTests(unittest.TestCase):
    def test_hypercore_side_info_repeat_history_manual_status_and_no_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            database.initialize()
            pipeline = DiscoveryPipeline(database)
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([trade(T0)]))

            first = pipeline.run(provider, limit=10, min_activity=1)
            self.assertEqual((first.wallets_seen, first.new_wallets, first.existing_wallets_refreshed), (2, 2, 0))
            self.assertEqual(database.get_target(BUYER).status, TargetStatus.NEW.value)  # type: ignore[union-attr]
            original_seen = next(row for row in database.list_discovery_candidates() if row["wallet"] == BUYER)["last_seen_at"]
            self.assertTrue(database.set_target_status(BUYER, TargetStatus.APPROVED.value))

            later = utc_now() + timedelta(seconds=1)
            refreshed = StaticProvider([
                DiscoveryObservation(BUYER, "hyperliquid_hypercore_node_trades", later, later, evidence_id="later-buyer"),
                DiscoveryObservation(SELLER, "hyperliquid_hypercore_node_trades", later, later, evidence_id="later-seller"),
            ], source_name="hyperliquid_hypercore_node_trades")
            second = pipeline.run(refreshed, limit=10, min_activity=1, refresh=True)
            self.assertEqual((second.new_wallets, second.existing_wallets_refreshed), (0, 2))
            buyer = next(row for row in database.list_discovery_candidates() if row["wallet"] == BUYER)
            self.assertGreater(buyer["last_seen_at"], original_seen)
            self.assertEqual(buyer["discovery_status"], TargetStatus.APPROVED.value)
            self.assertEqual(database.get_target(BUYER).status, TargetStatus.APPROVED.value)  # type: ignore[union-attr]
            self.assertEqual(len(database.list_discovery_runs()), 2)
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_discovery_observations").fetchone()[0], 4)
                for table in ("copy_signals", "copy_execution_claims", "copy_execution_attempts", "copy_execution_fills", "copy_virtual_positions"):
                    self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_filters_invalid_and_low_activity_and_merges_independent_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            observed = T0
            provider = StaticProvider([
                DiscoveryObservation(BUYER, "source_a", observed, observed, evidence_id="a1"),
                DiscoveryObservation(BUYER, "source_a", observed, observed, evidence_id="a2"),
                DiscoveryObservation(THIRD, "source_a", observed, observed, evidence_id="b1"),
                DiscoveryObservation(THIRD, "source_b", observed, observed, evidence_id="b2"),
                DiscoveryObservation(LOW_ACTIVITY, "source_a", observed, observed, evidence_id="low"),
                DiscoveryObservation("not-a-wallet", "source_a", observed, observed, evidence_id="invalid"),
            ])
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=2)
            self.assertEqual(summary.wallets_seen, 3)
            self.assertEqual(summary.filtered_wallets, 2)
            self.assertIn("invalid_wallets_rejected:1", summary.errors)
            candidates = {row["wallet"]: row for row in database.list_discovery_candidates()}
            self.assertEqual(set(candidates), {BUYER, THIRD})
            self.assertEqual(candidates[BUYER]["source_count"], 1)
            self.assertEqual(candidates[THIRD]["source_count"], 2)
            self.assertEqual(database.list_discovery_candidates(source="source_b")[0]["wallet"], THIRD)

    def test_provider_failure_records_failed_run_without_corrupting_existing_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            pipeline = DiscoveryPipeline(database)
            pipeline.run(StaticProvider([DiscoveryObservation(BUYER, "fixture", T0, T0)]), limit=10, min_activity=1)
            with self.assertRaisesRegex(DiscoveryProviderError, "fixture transport unavailable"):
                pipeline.run(FailingProvider(), limit=10, min_activity=1)
            self.assertEqual([row["wallet"] for row in database.list_discovery_candidates()], [BUYER])
            runs = database.list_discovery_runs()
            self.assertIn("failed", {row["status"] for row in runs})

    def test_copy_discover_cli_file_source_is_repeatable_and_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = config(root)
            input_path = root / "node_trades.jsonl"
            input_path.write_text(json.dumps(trade(T0)) + "\n", encoding="utf-8")
            output_path = root / "discovery.json"
            args = argparse.Namespace(
                command="copy-discover", config="ignored.yaml", source="hypercore-file", input=[str(input_path)],
                limit=10, refresh=False, min_activity=1, output=str(output_path),
            )
            with patch("src.copytrade.cli.CopyTradeConfig.from_yaml", return_value=cfg), patch("src.copytrade.cli._print"):
                self.assertEqual(run_copytrade_command(args), 0)
                self.assertEqual(run_copytrade_command(args), 0)
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            self.assertEqual(len(database.list_discovery_candidates()), 2)
            self.assertEqual(len(database.list_discovery_runs()), 2)
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8"))["message"], "Discovery complete")


if __name__ == "__main__":
    unittest.main()
