from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.models import RawFill, Target, TraderSnapshot, jsonable, stable_id, utc_now
from src.copytrade.service import CopyTradeService


WALLET = "0x9999999999999999999999999999999999999999"


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "incremental.sqlite3", obsidian_root=root / "obsidian"),
        sizing=SizingConfig(min_history=1),
        paper_execution=PaperExecutionConfig(
            fee_rate=0, slippage_bps=0, min_order_notional=1, random_seed=7,
            # This fixture tests reconstruction chunk equivalence.  Keep the
            # initial reference market valid across slow Windows workers;
            # stale-market policy is covered by dedicated tests.
            market_data_max_age_ms=60_000,
        ),
        risk=RiskConfig(
            kill_switch_path=root / "kill", max_total_committed_fraction=1, max_capital_per_target_fraction=1,
            max_capital_per_symbol_fraction=1, max_signal_age_seconds=86_400,
        ),
    )


def fill(
    index: int, side: str, before: float, *, symbol: str = "BTC", quantity: float = 1, order: int | None = None,
    timestamp: object | None = None,
) -> RawFill:
    at = utc_now() + timedelta(milliseconds=index) if timestamp is None else timestamp
    return RawFill.from_hyperliquid(
        {
            "coin": symbol, "px": "100", "sz": str(quantity), "side": side,
            "time": int(getattr(at, "timestamp")() * 1000),
            "startPosition": str(before), "oid": order if order is not None else index, "tid": f"trade-{index}",
            "hash": f"0x{index:064x}", "fee": "0", "accountValue": "1000",
        },
        WALLET,
        ingested_at=at,
    )


def source_state(quantity: float) -> TraderSnapshot:
    now = utc_now()
    return TraderSnapshot(
        snapshot_id=f"state-{quantity}-{int(now.timestamp() * 1000)}", target_wallet=WALLET,
        snapshot_timestamp=now, account_value=1000, withdrawable=None, total_notional_position=None,
        positions={"asset_positions": [{"position": {"szi": str(quantity)}}]}, source="hyperliquid",
        raw_payload={"assetPositions": [{"position": {"szi": str(quantity)}}]},
    )


class IncrementalReconstructionAndRecoveryTests(unittest.TestCase):
    def _service(self, root: Path) -> CopyTradeService:
        service = CopyTradeService(config(root))
        service.database.upsert_target(Target(wallet=WALLET, status="active"))
        asyncio.run(service.ingest_market_update({"mids": {"BTC": "100", "ETH": "100"}}))
        return service

    @staticmethod
    def _event_campaign_view(service: CopyTradeService) -> tuple[list[object], list[object]]:
        events = [jsonable(item) for item in service.database.list_position_events(WALLET)]
        campaigns = [jsonable(item) for item in service.database.list_campaigns(WALLET)]
        return events, campaigns

    def test_full_and_incremental_reconstruction_are_semantically_equivalent(self) -> None:
        history = [
            fill(1, "B", 0, quantity=1, order=10), fill(2, "B", 0, quantity=1, order=10),  # one partial aggregate
            fill(3, "B", 2, quantity=1, order=11), fill(4, "A", 3, quantity=1, order=12),
            fill(5, "A", 2, quantity=4, order=13), fill(6, "B", -2, quantity=1, order=14),
            fill(7, "B", -1, quantity=1, order=15), fill(8, "B", 0, symbol="ETH", order=16),
            fill(9, "A", 1, symbol="ETH", order=17),
        ]
        with tempfile.TemporaryDirectory() as first_temp, tempfile.TemporaryDirectory() as second_temp:
            complete = self._service(Path(first_temp))
            incremental = self._service(Path(second_temp))
            asyncio.run(complete.ingest_watched_fills(WALLET, history, False))
            for chunk in (history[:2], history[2:4], history[4:5], history[5:7], history[7:]):
                asyncio.run(incremental.ingest_watched_fills(WALLET, chunk, False))
            self.assertEqual(self._event_campaign_view(complete), self._event_campaign_view(incremental))
            complete_positions = [
                (item.symbol, item.direction, item.quantity, item.remaining_capital, item.realized_pnl, item.closed_at is None)
                for item in complete.database.list_virtual_positions()
            ]
            incremental_positions = [
                (item.symbol, item.direction, item.quantity, item.remaining_capital, item.realized_pnl, item.closed_at is None)
                for item in incremental.database.list_virtual_positions()
            ]
            self.assertEqual(complete_positions, incremental_positions)

    def test_distinct_order_history_matches_across_delivery_chunk_boundaries(self) -> None:
        """Source-frame chunking never changes durable reconstruction output.

        Partial fills are aggregated inside their complete ``userFills``
        delivery frame (covered above).  This independently exercises one,
        two, and unevenly batched distinct orders -- the normal reconnect and
        real-time delivery boundaries -- against one complete reconstruction.
        """
        history = [
            fill(1, "B", 0, order=1), fill(2, "B", 1, order=2),
            fill(3, "A", 2, order=3), fill(4, "A", 1, order=4),
            fill(5, "B", 0, symbol="ETH", order=5), fill(6, "A", 1, symbol="ETH", order=6),
        ]
        boundary_sets = [
            [history],
            [[item] for item in history],
            [history[:2], history[2:4], history[4:]],
            [history[:1], history[1:4], history[4:5], history[5:]],
        ]
        with tempfile.TemporaryDirectory() as reference_temp:
            reference = self._service(Path(reference_temp))
            asyncio.run(reference.ingest_watched_fills(WALLET, history, False))
            expected = self._event_campaign_view(reference)
        for chunks in boundary_sets:
            with self.subTest(chunks=[len(chunk) for chunk in chunks]), tempfile.TemporaryDirectory() as temp:
                candidate = self._service(Path(temp))
                for chunk in chunks:
                    asyncio.run(candidate.ingest_watched_fills(WALLET, chunk, False))
                self.assertEqual(expected, self._event_campaign_view(candidate))

    def test_replay_restart_and_cursor_failure_are_idempotent(self) -> None:
        history = [fill(1, "B", 0), fill(2, "B", 1), fill(3, "A", 2), fill(4, "A", 1)]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = self._service(root)
            original = service.database.persist_reconstruction_batch

            def crash(*args: object, **kwargs: object) -> object:
                raise RuntimeError("injected reconstruction crash")

            service.database.persist_reconstruction_batch = crash  # type: ignore[method-assign]
            with self.assertRaisesRegex(RuntimeError, "injected reconstruction crash"):
                asyncio.run(service.ingest_watched_fills(WALLET, history[:1], False))
            self.assertEqual(service.database.list_position_events(WALLET), [])
            self.assertEqual(service.database.reconstruction_cursor(WALLET).revision, 0)
            service.database.persist_reconstruction_batch = original  # type: ignore[method-assign]
            asyncio.run(service.ingest_watched_fills(WALLET, history[:2], False))
            before = self._event_campaign_view(service)
            sizing_before = list(service.database.sizing_history(WALLET))
            # A whole already-seen websocket snapshot must load no historical
            # fills through the cursor-driven hot path.
            asyncio.run(service.ingest_watched_fills(WALLET, history[:2], True))
            self.assertEqual(before, self._event_campaign_view(service))
            self.assertEqual(sizing_before, service.database.sizing_history(WALLET))
            self.assertEqual(service.incremental_work(WALLET)["fills_loaded"], 0)
            restarted = CopyTradeService(config(root))
            asyncio.run(restarted.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(restarted.ingest_watched_fills(WALLET, history[2:], False))
            self.assertEqual(len(restarted.database.list_position_events(WALLET)), 4)
            self.assertEqual(restarted.database.reconstruction_cursor(WALLET).pending_event_ids, ())

    def test_steady_state_one_new_fill_does_not_load_full_history(self) -> None:
        history: list[RawFill] = []
        position = 0.0
        for index in range(1, 10_001):
            side = "B" if position == 0 else "A"
            history.append(fill(index, side, position, order=index))
            position = 1.0 if side == "B" else 0.0
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            service.database.insert_raw_fills(history)
            service.reconstruct(WALLET)  # explicit historical path, not watcher hot path
            original_list = service.database.list_raw_fills

            def historical_read_is_forbidden(*args: object, **kwargs: object) -> object:
                raise AssertionError("watcher hot path must not list full raw-fill history")

            service.database.list_raw_fills = historical_read_is_forbidden  # type: ignore[method-assign]
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(10_001, "B", 0, order=10_001)], False))
            service.database.list_raw_fills = original_list  # type: ignore[method-assign]
            work = service.incremental_work(WALLET)
            self.assertEqual(work["mode"], "incremental")
            self.assertEqual(work["fills_loaded"], 1)
            self.assertEqual(work["events_produced"], 1)
            self.assertLess(int(work["fills_loaded"]), 10)

    def test_initial_history_snapshot_rebuilds_without_holding_paper_execution_queue(self) -> None:
        history: list[RawFill] = []
        position = 0.0
        for index in range(1, 1_001):
            side = "B" if position == 0 else "A"
            history.append(fill(index, side, position, order=index))
            position = 1.0 if side == "B" else 0.0
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            asyncio.run(service.ingest_watched_fills(WALLET, history, True))
            self.assertEqual(len(service.database.list_position_events(WALLET)), 1_000)
            self.assertEqual(service.database.reconstruction_cursor(WALLET).pending_event_ids, ())
            self.assertEqual(service.database.dashboard_snapshot()["execution_attempts"], [])

    def test_pre_cursor_database_is_rebuilt_before_processing_new_watcher_evidence(self) -> None:
        historical = [fill(1, "B", 0), fill(2, "A", 1)]
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            # Simulate the previous release: durable evidence exists but no
            # reconstruction boundary has ever been written.  A recovery
            # status record may already exist after watcher startup, but it
            # is not sufficient provenance to begin incremental processing.
            service.database.insert_raw_fills(historical)
            service.database.set_recovery_state(WALLET, "RECOVERING")
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(3, "B", 0)], False))
            events = service.database.list_position_events(WALLET)
            self.assertEqual([(event.event_type.value, event.before_quantity, event.after_quantity) for event in events], [
                ("OPEN", 0.0, 1.0), ("CLOSE", 1.0, 0.0), ("OPEN", 0.0, 1.0),
            ])
            self.assertEqual(service.incremental_work(WALLET)["mode"], "full_rebuild")
            self.assertTrue(service.database.has_reconstruction_cursor(WALLET))

    def test_signal_persisted_before_execution_replays_after_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            original = service._execute_reconstructed_signal

            def crash_after_signal(*args: object, **kwargs: object) -> None:
                raise RuntimeError("injected post-signal interruption")

            service._execute_reconstructed_signal = crash_after_signal  # type: ignore[method-assign]
            with self.assertRaisesRegex(RuntimeError, "post-signal"):
                asyncio.run(service.ingest_watched_fills(WALLET, [fill(1, "B", 0)], False))
            cursor = service.database.reconstruction_cursor(WALLET)
            self.assertEqual(len(cursor.pending_event_ids), 1)
            self.assertEqual(len(service.database.sizing_history(WALLET)), 1)
            # Simulate an actual process loss, not merely a new service sharing
            # the in-process PAPER authority registry.
            from src.copytrade import service as service_module
            service_module._paper_execution_authorities.pop(str(service.database.path.resolve()), None)
            restarted = self._service(Path(temp))
            asyncio.run(restarted.ingest_watched_fills(WALLET, [], False))
            self.assertEqual(restarted.database.reconstruction_cursor(WALLET).pending_event_ids, ())
            self.assertEqual(len(restarted.database.list_virtual_positions(open_only=True)), 1)

    def test_historical_snapshot_seeds_target_sizing_without_replaying_paper_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(1, "B", 0, symbol="ETH")], True))
            self.assertEqual(service.database.dashboard_snapshot()["execution_attempts"], [])
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(2, "B", 0, symbol="BTC", quantity=2)], False))
            event = service.database.list_position_events(WALLET)[-1]
            signal = service.database.get_signal(stable_id("signal", event.event_id, "open"))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.reason, "size_large")  # type: ignore[union-attr]

    def test_incremental_soak_keeps_per_message_work_flat(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            position = 0.0
            max_loaded = max_events = 0
            for index in range(1, 251):
                side = "B" if position == 0 else "A"
                asyncio.run(service.ingest_watched_fills(WALLET, [fill(index, side, position)], False))
                position = 1.0 if side == "B" else 0.0
                work = service.incremental_work(WALLET)
                max_loaded = max(max_loaded, int(work["fills_loaded"]))
                max_events = max(max_events, int(work["events_produced"]))
            self.assertEqual((max_loaded, max_events), (1, 1))
            self.assertEqual(len(service.database.list_position_events(WALLET)), 250)
            self.assertEqual(service.database.reconstruction_cursor(WALLET).pending_event_ids, ())

    def test_anchor_missing_blocks_entries_but_exit_and_state_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = self._service(root)
            opening = fill(1, "B", 0)
            asyncio.run(service.ingest_watched_fills(WALLET, [opening], False))
            # Same timestamp is insufficient: identity, not time proximity,
            # must prove continuity.
            missing_anchor_open = fill(2, "B", 0, symbol="ETH", timestamp=opening.event_timestamp)
            service.adapter.fetch_user_fills = lambda wallet: [missing_anchor_open]  # type: ignore[method-assign]
            service.adapter.fetch_clearinghouse_state = lambda wallet: source_state(2)  # type: ignore[method-assign]
            asyncio.run(service.reconcile_wallet(WALLET))
            status = service.recovery_status(WALLET)["wallets"][0]
            self.assertEqual(status["state"], "RECOVERY_INCOMPLETE")
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(3, "B", 0, symbol="SOL")], False))
            attempts = service.database.dashboard_snapshot()["execution_attempts"]
            self.assertTrue(any(item["reason"] == "source_recovery_not_continuous" for item in attempts))
            closing = fill(4, "A", 2, quantity=2)
            asyncio.run(service.ingest_watched_fills(WALLET, [closing], False))
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])
            restarted = CopyTradeService(config(root))
            self.assertEqual(restarted.recovery_status(WALLET)["wallets"][0]["state"], "RECOVERY_INCOMPLETE")

    def test_overlap_is_proven_and_flat_rebaseline_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = self._service(Path(temp))
            anchor = fill(1, "B", 0)
            asyncio.run(service.ingest_watched_fills(WALLET, [anchor], False))
            service.adapter.fetch_user_fills = lambda wallet: [anchor, fill(2, "A", 1)]  # type: ignore[method-assign]
            asyncio.run(service.reconcile_wallet(WALLET))
            self.assertEqual(service.recovery_status(WALLET)["wallets"][0]["state"], "CONTINUOUS")
            # A later missing anchor remains fail-closed even if its current
            # source state is flat; only an explicit acknowledgement resumes.
            service.adapter.fetch_user_fills = lambda wallet: []  # type: ignore[method-assign]
            service.adapter.fetch_clearinghouse_state = lambda wallet: source_state(0)  # type: ignore[method-assign]
            asyncio.run(service.reconcile_wallet(WALLET))
            self.assertEqual(service.recovery_status(WALLET)["wallets"][0]["state"], "RECOVERY_INCOMPLETE")
            result = asyncio.run(service.safe_rebaseline_recovery(WALLET))
            self.assertTrue(result["accepted"])
            self.assertEqual(service.recovery_status(WALLET)["wallets"][0]["state"], "CONTINUOUS")
            asyncio.run(service.ingest_watched_fills(WALLET, [fill(3, "B", 0)], False))
            self.assertEqual(len(service.database.list_virtual_positions(open_only=True)), 1)


if __name__ == "__main__":
    unittest.main()
