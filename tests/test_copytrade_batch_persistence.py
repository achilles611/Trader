from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src.copytrade.models import RawFill
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x8888888888888888888888888888888888888888"


def fill(tid: int) -> RawFill:
    return RawFill.from_hyperliquid({
        "coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": 1_700_000_000_000 + tid,
        "startPosition": "0", "oid": tid, "tid": tid, "fee": "0", "accountValue": "1000",
    }, WALLET)


class RawFillBatchPersistenceTests(unittest.TestCase):
    def database(self, root: Path) -> CopyTradeDatabase:
        database = CopyTradeDatabase(root / "fills.sqlite3")
        database.initialize()
        return database

    def test_empty_single_and_duplicate_batches_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            first = fill(1)
            self.assertEqual(database.insert_raw_fills(()), 0)
            self.assertEqual(database.insert_raw_fills((first,)), 1)
            self.assertEqual(database.insert_raw_fills((first, first)), 0)
            self.assertEqual(database.insert_raw_fills((fill(2), first, fill(2))), 1)
            self.assertEqual(len(database.list_raw_fills(WALLET)), 2)

    def test_thousands_of_fills_use_bounded_batches_and_keep_exact_uniqueness(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            rows = [fill(index) for index in range(10_000)]
            self.assertEqual(database.insert_raw_fills(rows, batch_size=500), 10_000)
            self.assertEqual(database.insert_raw_fills(rows, batch_size=500), 0)
            self.assertEqual(len(database.list_raw_fills(WALLET)), 10_000)

    def test_exception_rolls_back_all_batches_and_retry_is_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            rows = [fill(index) for index in range(1_000)]
            original = database._raw_fill_values
            calls = 0

            def fail_midstream(item: RawFill):
                nonlocal calls
                calls += 1
                if calls == 701:
                    raise RuntimeError("injected persistence failure")
                return original(item)

            with patch.object(database, "_raw_fill_values", side_effect=fail_midstream):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    database.insert_raw_fills(rows, batch_size=500)
            self.assertEqual(database.list_raw_fills(WALLET), [])
            self.assertEqual(database.insert_raw_fills(rows, batch_size=500), 1_000)
            self.assertEqual(len(database.list_raw_fills(WALLET)), 1_000)

    def test_controlled_concurrent_batches_share_sqlite_without_lock_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            barrier = threading.Barrier(4)
            failures: list[BaseException] = []
            inserted: list[int] = []

            def worker(offset: int) -> None:
                try:
                    barrier.wait()
                    # Each producer overlaps half its range with peers.
                    inserted.append(
                        database.insert_raw_fills(
                            (fill(index) for index in range(offset, offset + 500)), batch_size=100,
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)

            workers = [threading.Thread(target=worker, args=(offset,)) for offset in (0, 250, 500, 750)]
            for worker_thread in workers: worker_thread.start()
            for worker_thread in workers: worker_thread.join()
            self.assertEqual(failures, [])
            self.assertEqual(len(database.list_raw_fills(WALLET)), 1_250)
            self.assertEqual(sum(inserted), 1_250)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
