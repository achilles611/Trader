from __future__ import annotations

import argparse
import json
import sys
import tempfile
from types import SimpleNamespace
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
    RequesterPaysS3NodeTradeTransport,
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

    def test_production_node_fills_by_block_wallet_fill_pairs_keep_all_valid_events(self) -> None:
        """Regression for production HyperCore events encoded as ``[wallet, fill]``."""
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            block_time = timestamp(T0)
            first = fill(T0, fill_id="paired-first", include_time=False)
            second = fill(T0 + timedelta(minutes=1), fill_id="paired-second")
            # The fills deliberately omit user. The outer event is authoritative
            # only as a user override; it never fabricates another required field.
            first.pop("user")
            second.pop("user")
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([{
                "block_number": 123, "block_time": block_time,
                "events": [[BUYER, first], [SELLER, second]],
            }]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual(summary.status, "completed")
            self.assertEqual((summary.valid_events, summary.normalized_observations), (2, 2))
            self.assertEqual({row["wallet"] for row in database.list_discovery_candidates()}, {BUYER, SELLER})
            with database._connect() as connection:  # type: ignore[attr-defined]
                rows = connection.execute(
                    "SELECT wallet, recent_activity_at, metadata_json FROM copy_discovery_observations ORDER BY wallet"
                ).fetchall()
            self.assertEqual(as_utc(rows[0]["recent_activity_at"]), as_utc(T0))
            self.assertEqual(as_utc(rows[1]["recent_activity_at"]), as_utc(T0 + timedelta(minutes=1)))
            self.assertEqual(json.loads(rows[0]["metadata_json"])["block_event_index"], 0)

    def test_malformed_block_event_is_quarantined_without_discarding_valid_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            good_first = fill(T0, fill_id="good-first")
            good_second = fill(T0, SELLER, fill_id="good-second")
            broken = fill(T0, THIRD, fill_id="bad-price")
            broken["px"] = "not-a-number"
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([{
                "block_number": 456, "block_time": timestamp(T0),
                "events": [[BUYER, good_first], [THIRD, broken], [SELLER, good_second]],
            }]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertEqual((summary.valid_events, summary.malformed_events, summary.unsupported_records), (2, 1, 0))
            self.assertEqual({row["wallet"] for row in database.list_discovery_candidates()}, {BUYER, SELLER})
            run = database.list_discovery_runs()[0]
            self.assertEqual((run["malformed_events"], run["fatal_source_errors"]), (1, 0))
            with database._connect() as connection:  # type: ignore[attr-defined]
                rejection = connection.execute(
                    "SELECT category, event_index, raw_record_json FROM copy_discovery_rejections WHERE run_id=?",
                    (summary.run_id,),
                ).fetchone()
            self.assertEqual((rejection["category"], rejection["event_index"]), ("malformed_event", 1))
            self.assertEqual(json.loads(rejection["raw_record_json"])[1]["tid"], "bad-price")

    def test_invalid_outer_wallet_is_counted_and_does_not_register_a_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            missing_user = fill(T0, fill_id="invalid-wallet")
            missing_user.pop("user")
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([{
                "block_number": 999, "block_time": timestamp(T0), "events": [["not-a-wallet", missing_user]],
            }]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.valid_events, summary.invalid_wallets, summary.wallets_seen), (1, 1, 0))
            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertEqual(database.list_discovery_candidates(), [])

    def test_unsupported_nested_event_is_counted_without_failing_its_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([{
                "block_number": 777, "block_time": timestamp(T0),
                "events": [[BUYER, fill(T0, BUYER, fill_id="supported")], "not-a-fill", [SELLER, fill(T0, SELLER, fill_id="also-supported")]],
            }]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.valid_events, summary.malformed_events, summary.unsupported_records), (2, 0, 1))
            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertEqual({row["wallet"] for row in database.list_discovery_candidates()}, {BUYER, SELLER})

    def test_duplicate_within_one_file_and_multi_fill_transaction_are_distinguished(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(config(root).artifacts.database_path)
            database.initialize()
            repeated = fill(T0, BUYER, fill_id="same-file")
            same_transaction_first = fill(T0, BUYER, fill_id="")
            same_transaction_second = fill(T0, BUYER, fill_id="")
            for payload in (same_transaction_first, same_transaction_second):
                payload.pop("tid")
                payload["hash"] = "one-transaction"
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([
                repeated, repeated,
                {"block_number": 1000, "block_time": timestamp(T0),
                 "events": [[BUYER, same_transaction_first], [BUYER, same_transaction_second]]},
            ]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.valid_events, summary.normalized_observations, summary.duplicate_events), (4, 4, 1))
            with database._connect() as connection:  # type: ignore[attr-defined]
                ids = [row[0] for row in connection.execute("SELECT evidence_id FROM copy_discovery_observations ORDER BY evidence_id").fetchall()]
            self.assertEqual(len(ids), 3)
            self.assertEqual(len(set(ids)), 3)

    def test_fallback_event_id_is_deterministic_for_overlapping_block_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            no_identifier = fill(T0, BUYER, fill_id="")
            no_identifier.pop("tid")
            record = {"block_number": 314, "block_time": timestamp(T0), "events": [[BUYER, no_identifier]]}
            provider = HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([record, record]))
            summary = DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
            self.assertEqual((summary.normalized_observations, summary.duplicate_events), (2, 1))

    def test_corrupt_empty_and_unsupported_sources_fail_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, text, message in (
                ("corrupt.jsonl", json.dumps(fill(T0)) + "\n{broken", "Malformed JSONL"),
                ("empty.jsonl", "", "contained no JSON records"),
                ("unsupported.jsonl", json.dumps({"valid": "json"}) + "\n", "Unsupported HyperCore discovery schema"),
            ):
                path = root / name
                path.write_text(text, encoding="utf-8")
                database = CopyTradeDatabase(root / f"{name}.sqlite3")
                database.initialize()
                provider = HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport([path]))
                with self.assertRaisesRegex(DiscoveryProviderError, message):
                    DiscoveryPipeline(database).run(provider, limit=10, min_activity=1, max_activity_age=None)
                run = database.list_discovery_runs()[0]
                self.assertEqual((run["status"], run["fatal_source_errors"]), ("failed", 1))
                self.assertEqual(database.list_discovery_candidates(), [])

    def test_lz4_source_is_supported_or_reports_a_safe_missing_dependency_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            compressed = root / "fills.jsonl.lz4"
            try:
                import lz4.frame
            except ImportError:
                compressed.write_bytes(b"placeholder")
                with self.assertRaisesRegex(DiscoveryProviderError, "requires the lz4 dependency"):
                    next(LocalNodeTradeFileTransport([compressed]).iter_trades())
                return
            with lz4.frame.open(compressed, "wt", encoding="utf-8") as stream:
                stream.write(json.dumps(fill(T0)) + "\n")
            database = CopyTradeDatabase(config(root).artifacts.database_path)
            database.initialize()
            summary = DiscoveryPipeline(database).run(
                HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport([compressed])),
                limit=10, min_activity=1, max_activity_age=None,
            )
            self.assertEqual(summary.wallets_seen, 1)
            with patch.dict(sys.modules, {"lz4": None, "lz4.frame": None}):
                with self.assertRaisesRegex(DiscoveryProviderError, "requires the lz4 dependency"):
                    next(LocalNodeTradeFileTransport([compressed]).iter_trades())

    def test_requester_pays_failure_does_not_expose_underlying_credential_text(self) -> None:
        class FailingClient:
            def get_object(self, **kwargs):
                self.request = kwargs
                raise RuntimeError("credential=should-not-be-reported")

        client = FailingClient()
        with patch.dict(sys.modules, {"boto3": SimpleNamespace(client=lambda _service: client)}):
            with self.assertRaises(DiscoveryProviderError) as raised:
                next(RequesterPaysS3NodeTradeTransport(["s3://bucket/private-object"]).iter_trades())
        self.assertIn("RequestPayer=requester", str(raised.exception))
        self.assertNotIn("should-not-be-reported", str(raised.exception))
        self.assertEqual(client.request["RequestPayer"], "requester")

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

    def test_stage_a_statistics_are_persisted_from_deduplicated_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(config(Path(temp)).artifacts.database_path)
            database.initialize()
            first = fill(T0, BUYER, fill_id="stats-one")
            second = fill(T0 + timedelta(days=1, hours=2), BUYER, fill_id="stats-two")
            second["coin"] = "BTC"
            summary = DiscoveryPipeline(database).run(
                HyperCoreNodeTradeDiscoveryProvider(IterableNodeTradeTransport([first, second])),
                limit=10, min_activity=2, max_activity_age=None,
            )
            self.assertEqual(summary.eligible_wallets, 1)
            candidate = database.list_discovery_candidates()[0]
            metadata = json.loads(candidate["metadata_json"])
            self.assertEqual(metadata["evidence_schema_version"], 2)
            stats = metadata["cheap_stats"]
            self.assertEqual((stats["distinct_observed_events"], stats["distinct_active_days"], stats["distinct_active_hours"]), (2, 2, 2))
            self.assertEqual((stats["distinct_symbols"], stats["symbols"], stats["independent_source_count"]), (2, ["BTC", "ETH"], 1))
            self.assertGreater(stats["approximate_observed_notional"], 0)
            self.assertGreater(stats["observation_span_hours"], 24)

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
