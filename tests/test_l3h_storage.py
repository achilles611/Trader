from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3h_live.storage import DISARM_WATERMARK, evaluate_disk


class L3HStorageTests(unittest.TestCase):
    def test_disk_guard_reports_without_deleting_or_assuming_capacity(self) -> None:
        with TemporaryDirectory() as directory:
            status = evaluate_disk(Path(directory))
        self.assertGreater(status.total_bytes, 0)
        self.assertGreaterEqual(status.used_ratio, 0)
        self.assertLessEqual(status.used_ratio, 1)
        self.assertEqual(DISARM_WATERMARK, 0.85)
        self.assertIn(status.reason, {"PASS", "DISK_PRESSURE_DISARM"})


if __name__ == "__main__":
    unittest.main()
