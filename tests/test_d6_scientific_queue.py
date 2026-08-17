from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from src.copytrade.science_repository import ScientificRepository


T0 = "2026-08-17T12:00:00Z"


class ScientificQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = ScientificRepository(Path(self.temp.name) / "science.sqlite3")
        self.repository.initialize()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _enqueue(self, identifier: str = "work-1") -> None:
        self.repository.enqueue_work(
            identifier, work_type="FEATURE_MATERIALIZATION", subject_type="observation", subject_id="obs-1",
            subject_version=1, priority=10, created_at=T0, available_at=T0, max_attempts=2,
            input_fingerprint="immutable-input",
        )

    def test_duplicate_schedule_is_idempotent_and_only_one_worker_claims(self) -> None:
        self._enqueue()
        self._enqueue()
        claimed: list[dict[str, object] | None] = []

        def claim(worker_id: str) -> None:
            claimed.append(self.repository.claim_work(worker_id=worker_id, now="2026-08-17T12:00:01Z", lease_expires_at="2026-08-17T12:00:31Z"))

        threads = [threading.Thread(target=claim, args=(f"worker-{index}",)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(item is not None for item in claimed), 1)

    def test_expired_lease_recovery_and_bounded_retry(self) -> None:
        self._enqueue()
        first = self.repository.claim_work(worker_id="first", now="2026-08-17T12:00:01Z", lease_expires_at="2026-08-17T12:00:02Z")
        self.assertIsNotNone(first)
        self.assertEqual(self.repository.recover_expired_leases(now="2026-08-17T12:00:03Z"), 1)
        second = self.repository.claim_work(worker_id="second", now="2026-08-17T12:00:04Z", lease_expires_at="2026-08-17T12:00:05Z")
        self.assertEqual(second["attempt_count"], 2)
        state = self.repository.fail_work("work-1", worker_id="second", available_at="2026-08-17T12:00:06Z", error_class="SCIENTIFIC_INVALID", message_redacted="bad frozen definition", permanent=True)
        self.assertEqual(state, "FAILED")

    def test_completed_item_cannot_be_replayed(self) -> None:
        self._enqueue()
        item = self.repository.claim_work(worker_id="worker", now="2026-08-17T12:00:01Z", lease_expires_at="2026-08-17T12:00:31Z")
        self.assertIsNotNone(item)
        self.repository.complete_work("work-1", worker_id="worker", completed_at="2026-08-17T12:00:02Z", result_reference="feature:obs-1")
        self.assertIsNone(self.repository.claim_work(worker_id="other", now="2026-08-17T12:00:03Z", lease_expires_at="2026-08-17T12:00:33Z"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
