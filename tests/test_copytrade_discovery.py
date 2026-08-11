from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from src.copytrade.cli import run_copytrade_command
from src.copytrade.config import ArtifactConfig, CopyTradeConfig, RiskConfig
from src.copytrade.discovery import (
    DiscoveryPipeline,
    DiscoveryProviderError,
    HyperCoreNodeTradeDiscoveryProvider,
    IterableNodeTradeTransport,
    LocalNodeTradeFileTransport,
    parse_activity_age,
)
from src.copytrade.models import DiscoveryObservation, TargetStatus, as_utc, utc_now
from src.copytrade.storage import CopyTradeDatabase


BUYER = "0x1111111111111111111111111111111111111111"
SELLER = "0x2222222222222222222222222222222222222222"
THIRD = "0x3333333333333333333333333333333333333333"
LOW_ACTIVITY = "0x4444444444444444444444444444444444444444"
T0 = utc_now() - timedelta(hours=2)


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copytrade.sqlite3", obsidian_root=root / "obsidian"),
        risk=RiskConfig(kill_switch_path=root / "kill"),
    )


def timestamp(value) -> str:
    return as_utc(value).isoformat().replace("+00:00", "Z")


def trade(event_time, buyer: str = BUYER, seller: str = SELLER, *, trade_id: str = "trade-1") -> dict[str, object]:
    return {
        "coin": "BTC", "side": "B", "time": timestamp(event_time), "px": "100", "sz": "2",
        "tid": trade_id,
        "side_info": [{"user": buyer, "start_pos": "0", "oid": 1}, {"user": seller, "start_pos": "0", "oid": 2}],
    }


def fill(event_time, wallet: str = BUYER, *, fill_id: str = "fill-1", include_time: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "user": wallet, "coin": "ETH", "side": "B", "px": "200", "sz": "1", "tid": fill_id, "oid": 17,
    }
    if include_time:
        payload["time"] = timestamp(event_time)
    return payload


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
    def test_node_trades_repeat_history_preserves_manual_status_and_never_executes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            pipeline = DiscoveryPipeline(database)
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([trade(T0)]))

            first = pipeline.run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((first.wallets_seen, first.eligible_wallets, first.new_wallets, first.existing_wallets_refreshed), (2, 2, 2, 0))
            self.assertEqual(database.get_target(BUYER).status, TargetStatus.NEW.value)  # type: ignore[union-attr]
            original_seen = next(row for row in database.list_discovery_candidates() if row["wallet"] == BUYER)["last_seen_at"]
            self.assertTrue(database.set_target_status(BUYER, TargetStatus.APPROVED.value))

            later = utc_now() + timedelta(seconds=1)
            refreshed = StaticProvider([
                DiscoveryObservation(BUYER, "hyperliquid_hypercore_node_trades", later, later, evidence_id="later-buyer"),
                DiscoveryObservation(SELLER, "hyperliquid_hypercore_node_trades", later, later, evidence_id="later-seller"),
            ], source_name="hyperliquid_hypercore_node_trades")
            second = pipeline.run(refreshed, limit=10, min_activity=1, refresh=True, max_activity_age=None)
            self.assertEqual((second.new_wallets, second.existing_wallets_refreshed), (0, 2))
            buyer = next(row for row in database.list_discovery_candidates() if row["wallet"] == BUYER)
            self.assertGreater(buyer["last_seen_at"], original_seen)
            self.assertEqual(buyer["discovery_status"], TargetStatus.APPROVED.value)
            self.assertEqual(database.get_target(BUYER).status, TargetStatus.APPROVED.value)  # type: ignore[union-attr]
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_discovery_observations").fetchone()[0], 4)
                for table in ("copy_signals", "copy_execution_claims", "copy_execution_attempts", "copy_execution_fills", "copy_virtual_positions"):
                    self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_node_fills_and_node_fills_by_block_normalize_wallets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([
                fill(T0, BUYER, fill_id="api-fill"),
                {
                    "local_time": timestamp(T0), "block_time": timestamp(T0), "block_number": 123,
                    "events": [fill(T0, SELLER, fill_id="block-fill", include_time=False)],
                },
            ]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.wallets_seen, summary.new_wallets), (2, 2))
            candidates = {row["wallet"] for row in database.list_discovery_candidates()}
            self.assertEqual(candidates, {BUYER, SELLER})
            with database._connect() as connection:  # type: ignore[attr-defined]
                formats = {
                    json.loads(row[0])["format"]
                    for row in connection.execute("SELECT metadata_json FROM copy_discovery_observations").fetchall()
                }
            self.assertEqual(formats, {"node_fills", "node_fills_by_block"})

    def test_unsupported_valid_schema_fails_without_creating_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([{"valid": "json", "but": "unsupported"}]))
            with self.assertRaisesRegex(DiscoveryProviderError, "Unsupported HyperCore discovery schema"):
                DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual(database.list_discovery_candidates(), [])
            failed = database.list_discovery_runs()[0]
            self.assertEqual(failed["status"], "failed")
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_discovery_observations").fetchone()[0], 0)

    def test_overlapping_file_evidence_is_deduplicated_before_min_activity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            input_path = root / "overlap.jsonl"
            input_path.write_text(json.dumps(fill(T0, BUYER, fill_id="same-fill")) + "\n", encoding="utf-8")
            database = CopyTradeDatabase(config(root).artifacts.database_path)
            database.initialize()
            provider = HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport([input_path, input_path]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=2, max_activity_age=None)
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.filtered_wallets), (1, 0, 1))
            self.assertEqual(database.list_discovery_candidates(), [])
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_discovery_observations").fetchone()[0], 1)

    def test_limit_deferred_accounting_reconciles_for_every_valid_wallet(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            provider = StaticProvider([
                DiscoveryObservation(wallet, "source_a", T0, T0, evidence_id=f"event-{index}")
                for index, wallet in enumerate((BUYER, SELLER, THIRD, LOW_ACTIVITY), 1)
            ])
            summary = DiscoveryPipeline(database).run(provider, limit=2, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.limit_deferred_wallets, summary.filtered_wallets), (4, 4, 2, 0))
            self.assertEqual(summary.eligible_wallets, summary.new_wallets + summary.existing_wallets_refreshed + summary.limit_deferred_wallets)
            self.assertEqual(summary.wallets_seen, summary.eligible_wallets + summary.filtered_wallets)
            run = database.list_discovery_runs()[0]
            self.assertEqual((run["eligible_wallets"], run["limit_deferred_wallets"]), (4, 2))
            self.assertEqual(len(database.list_discovery_candidates()), 2)

    def test_recency_filter_rejects_stale_activity_and_keeps_newest_activity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            fresh = utc_now() - timedelta(hours=1)
            stale = utc_now() - timedelta(days=31)
            provider = StaticProvider([
                DiscoveryObservation(BUYER, "source_a", fresh, fresh, evidence_id="buyer-fresh"),
                DiscoveryObservation(SELLER, "source_a", stale, stale, evidence_id="seller-stale"),
                DiscoveryObservation(THIRD, "source_a", stale, stale, evidence_id="third-stale"),
                DiscoveryObservation(THIRD, "source_a", fresh, fresh, evidence_id="third-fresh"),
            ])
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=parse_activity_age("30d"))
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.filtered_wallets), (3, 2, 1))
            candidates = {row["wallet"]: row for row in database.list_discovery_candidates()}
            self.assertEqual(set(candidates), {BUYER, THIRD})
            self.assertEqual(as_utc(candidates[THIRD]["recent_activity_at"]), as_utc(fresh))
            self.assertIsNone(parse_activity_age("none"))
            configuration = json.loads(database.list_discovery_runs()[0]["configuration_json"])
            self.assertEqual(configuration["max_activity_age_seconds"], 30 * 24 * 60 * 60)

    def test_disabled_recency_gate_keeps_observations_without_activity_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            summary = DiscoveryPipeline(database).run(
                StaticProvider([DiscoveryObservation(BUYER, "source_a", T0, evidence_id="unknown-activity")]),
                limit=10, min_activity=1, max_activity_age=parse_activity_age("none"),
            )
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.new_wallets), (1, 1, 1))
            self.assertIsNone(database.list_discovery_candidates()[0]["recent_activity_at"])

    def test_invalid_low_activity_and_independent_sources_merge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            provider = StaticProvider([
                DiscoveryObservation(BUYER, "source_a", T0, T0, evidence_id="a1"),
                DiscoveryObservation(BUYER, "source_a", T0, T0, evidence_id="a2"),
                DiscoveryObservation(THIRD, "source_a", T0, T0, evidence_id="b1"),
                DiscoveryObservation(THIRD, "source_b", T0, T0, evidence_id="b2"),
                DiscoveryObservation(LOW_ACTIVITY, "source_a", T0, T0, evidence_id="low"),
                DiscoveryObservation("not-a-wallet", "source_a", T0, T0, evidence_id="invalid"),
            ])
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=2, max_activity_age=None)
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.filtered_wallets), (3, 2, 1))
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
            pipeline.run(StaticProvider([DiscoveryObservation(BUYER, "fixture", T0, T0)]), limit=10, min_activity=1, max_activity_age=None)
            with self.assertRaisesRegex(DiscoveryProviderError, "fixture transport unavailable"):
                pipeline.run(FailingProvider(), limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual([row["wallet"] for row in database.list_discovery_candidates()], [BUYER])
            self.assertIn("failed", {row["status"] for row in database.list_discovery_runs()})

    def test_large_generated_stream_is_batched_without_materializing_observations(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()

            def records():
                for index in range(1_200):
                    yield fill(T0, f"0x{index + 1000:040x}", fill_id=f"large-{index}")

            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport(records()))
            summary = DiscoveryPipeline(database, batch_size=61).run(provider, limit=2_000, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.wallets_seen, summary.eligible_wallets, summary.new_wallets), (1_200, 1_200, 1_200))
            with database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_discovery_observations").fetchone()[0], 1_200)

    def test_copy_discover_cli_file_source_is_repeatable_and_writes_complete_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = config(root)
            input_path = root / "node_trades.jsonl"
            input_path.write_text(json.dumps(trade(T0)) + "\n", encoding="utf-8")
            output_path = root / "discovery.json"
            args = argparse.Namespace(
                command="copy-discover", config="ignored.yaml", source="hypercore-file", input=[str(input_path)],
                limit=10, refresh=False, min_activity=1, max_activity_age="30d", output=str(output_path),
            )
            with patch("src.copytrade.cli.CopyTradeConfig.from_yaml", return_value=cfg), patch("src.copytrade.cli._print"):
                self.assertEqual(run_copytrade_command(args), 0)
                self.assertEqual(run_copytrade_command(args), 0)
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            self.assertEqual(len(database.list_discovery_candidates()), 2)
            self.assertEqual(len(database.list_discovery_runs()), 2)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual((payload["message"], payload["eligible_wallets"], payload["limit_deferred_wallets"]), ("Discovery complete", 2, 0))


if __name__ == "__main__":
    unittest.main()
