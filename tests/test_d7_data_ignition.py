from __future__ import annotations

import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CommissioningConfig, CopyTradeConfig, ScientificWorkerConfig, StorageConfig
from src.copytrade.data_ignition import CoveragePolicy, DataCoverage, DataIgnitionCommissioner, LIVE_PUBLIC_SOURCE, PublicObservationService
from src.copytrade.science_repository import ScientificRepository
from src.copytrade.scientific_worker import ScientificWorker
from src.copytrade.source_acquisition import HyperCoreSourceAcquisition, historical_hour_slots


class _Body(io.BytesIO):
    def close(self) -> None:  # boto-style body close is harmless for a fixture stream
        pass


class FakeRequesterPaysS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.get_calls = 0

    def list_objects_v2(self, *, Bucket: str, Prefix: str, **_: object) -> dict[str, object]:
        contents = [
            {"Key": key, "Size": len(value), "ETag": f'"etag-{index}"', "LastModified": datetime(2026, 8, 1, tzinfo=timezone.utc)}
            for index, (key, value) in enumerate(self.objects.items()) if key.startswith(Prefix)
        ]
        return {"Contents": contents}

    def get_object(self, *, Bucket: str, Key: str, **_: object) -> dict[str, object]:
        self.get_calls += 1
        value = self.objects[Key]
        return {"Body": _Body(value), "ContentLength": len(value)}


def _fixture_object(hour: int) -> bytes:
    events = []
    for index in range(36):
        events.append(["0x1111111111111111111111111111111111111111", {
            "tid": f"{hour}-{index}", "time": f"2026-08-01T{hour:02d}:{(index // 4):02d}:{(index % 4) * 15:02d}Z",
            "coin": "BTC", "px": str(100 + index), "sz": "1", "side": "B" if index % 2 == 0 else "A",
        }])
    return (json.dumps({"block_number": hour, "block_time": f"2026-08-01T{hour:02d}:00:00Z", "events": events}) + "\n").encode()


class DataIgnitionTests(unittest.TestCase):
    def make_commissioner(self, temp: Path, objects: dict[str, bytes], *, start: str = "2026-08-01T00:00:00Z", end: str = "2026-08-01T01:00:00Z") -> tuple[DataIgnitionCommissioner, ScientificRepository, FakeRequesterPaysS3]:
        fake = FakeRequesterPaysS3(objects)
        config = CopyTradeConfig(
            artifacts=ArtifactConfig(database_path=temp / "hot" / "science.sqlite3"),
            storage=StorageConfig(hot_root=temp / "hot", cold_root=temp / "cold"),
            # This fixture validates replay/idempotency, not the production
            # low-disk pause.  Keep it independent of the host's free space.
            scientific_worker=ScientificWorkerConfig(minimum_hot_free_bytes=1),
            commissioning=CommissioningConfig(enabled=True, historical_start=start, historical_end=end, max_download_bytes=10_000_000,
                                               max_hours_per_run=24, min_coverage_fraction=1.0, archive_verified_sources=True),
        )
        repository = ScientificRepository(config.artifacts.database_path)
        worker = ScientificWorker(repository, config, worker_id="d7-test-worker")
        source = HyperCoreSourceAcquisition(temp / "hot" / "cache", s3_client_factory=lambda: fake,
                                            now=lambda: datetime(2026, 8, 2, tzinfo=timezone.utc))
        return DataIgnitionCommissioner(repository, worker, config, source=source), repository, fake

    def test_historical_planner_is_deterministic_and_bounded(self) -> None:
        slots = historical_hour_slots("2026-08-01T00:15:00Z", "2026-08-01T03:59:00Z", maximum_hours=4)
        self.assertEqual([(item.start, item.end) for item in slots], [
            ("2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z"),
            ("2026-08-01T01:00:00Z", "2026-08-01T02:00:00Z"),
            ("2026-08-01T02:00:00Z", "2026-08-01T03:00:00Z"),
        ])
        with self.assertRaises(ValueError):
            historical_hour_slots("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", maximum_hours=2)

    def test_complete_missing_and_corrupt_coverage_are_first_class(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = ScientificRepository(Path(directory) / "science.sqlite3")
            now = "2026-08-02T00:00:00Z"
            for start, end, state, malformed in [
                ("2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z", "INGESTED", 0),
                ("2026-08-01T01:00:00Z", "2026-08-01T02:00:00Z", "MISSING_SOURCE", 0),
            ]:
                repository.record_acquisition_hour(expected_start=start, expected_end=end,
                                                    source_name="hyperliquid_hypercore_node_fills_by_block", state=state, updated_at=now,
                                                    malformed_count=malformed)
            policy = CoveragePolicy(1.0)
            coverage = DataCoverage(repository, policy=policy).calculate("2026-08-01T00:00:00Z", "2026-08-01T02:00:00Z")
            self.assertEqual(coverage["state"], "KNOWN_GAP")
            self.assertFalse(DataCoverage(repository, policy=policy).eligible(coverage))
            repository.record_acquisition_hour(expected_start="2026-08-01T01:00:00Z", expected_end="2026-08-01T02:00:00Z",
                                                source_name="hyperliquid_hypercore_node_fills_by_block", state="INGESTED", updated_at=now,
                                                malformed_count=1)
            corrupt = DataCoverage(repository, policy=policy).calculate("2026-08-01T00:00:00Z", "2026-08-01T02:00:00Z")
            self.assertEqual(corrupt["state"], "CORRUPT")

    def test_byte_forecast_blocks_download_and_cancellation_stops_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = "node_fills_by_block/hourly/20260801/0.jsonl"
            commissioner, _, fake = self.make_commissioner(root, {key: _fixture_object(0)})
            commissioner.settings = replace(commissioner.settings, max_download_bytes=1)
            blocked = commissioner.acquire_history()
            self.assertEqual(blocked["state"], "BYTE_CAP_EXCEEDED")
            self.assertEqual(fake.get_calls, 0)
            slots = historical_hour_slots("2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z", maximum_hours=1)
            self.assertEqual(commissioner.source.resolve_historical_slots(slots, cancelled=lambda: True), {})

    def test_verified_historical_data_replays_idempotently_into_d6(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = "node_fills_by_block/hourly/20260801/0.jsonl"
            commissioner, repository, fake = self.make_commissioner(root, {key: _fixture_object(0)})
            commissioner.settings = replace(commissioner.settings, max_corpus_observations=8)
            result = commissioner.acquire_history()
            self.assertEqual(result["state"], "COMPLETED")
            self.assertEqual(result["coverage"]["state"], "PROVEN_COMPLETE")
            counts = commissioner.science_counts()
            self.assertGreater(counts["wallet_observations"], 0)
            self.assertGreater(counts["market_observations"], 0)
            self.assertEqual(repository.list_acquisition_manifest()[0]["state"], "INGESTED")
            self.assertEqual(result["selection"]["selected"], 8)
            self.assertGreaterEqual(result["selection"]["superseded_work"], 0)
            self.assertTrue((root / "cold" / "source-cache").exists())
            before = commissioner.science_counts()["observations"]
            replay = commissioner.acquire_history()
            self.assertIn(replay["state"], {"COMPLETED", "PARTIAL"})
            self.assertEqual(before, commissioner.science_counts()["observations"])
            self.assertEqual(fake.get_calls, 1)
            worker_result = commissioner.worker.run_until_idle(max_cycles=64)
            self.assertGreater(worker_result["processed"], 0)
            self.assertGreater(len(repository.list_feature_values()), 0)
            self.assertGreater(len(repository.list_outcome_labels()), 0)
            snapshot = commissioner.corpus_snapshot()
            self.assertEqual(snapshot["payload"]["coverage"]["state"], "PROVEN_COMPLETE")

    def test_live_and_historical_provenance_do_not_share_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = "node_fills_by_block/hourly/20260801/0.jsonl"
            commissioner, repository, _ = self.make_commissioner(root, {key: _fixture_object(0)})
            commissioner.acquire_history()
            observer = PublicObservationService(commissioner)
            self.assertTrue(observer.ingest_wallet_fill("0x1111111111111111111111111111111111111111", {
                "tid": "0-0", "time": "2026-08-01T00:00:00Z", "coin": "BTC", "px": "100", "sz": "1", "side": "B",
            }, received_at=datetime(2026, 8, 1, 0, 0, 1, tzinfo=timezone.utc)))
            self.assertGreater(observer.ingest_market({"mids": {"BTC": "101"}, "time": "2026-08-01T00:00:02Z"}, received_at=datetime(2026, 8, 1, 0, 0, 2, tzinfo=timezone.utc)), 0)
            sources = {row["source"] for row in repository.list_observations(limit=5000)}
            self.assertIn(LIVE_PUBLIC_SOURCE, sources)
            self.assertIn("HISTORICAL_OFFICIAL_ARCHIVE", sources)


if __name__ == "__main__":
    unittest.main()
