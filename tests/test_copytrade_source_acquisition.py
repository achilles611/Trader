from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest

from src.copytrade.source_acquisition import HyperCoreSourceAcquisition, HyperCoreSourceError, discovery_preset


class FakeS3:
    def __init__(self) -> None:
        self.list_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.objects = {
            "node_fills_by_block/hourly/20260813/11.lz4": b"eleven",
            "node_fills_by_block/hourly/20260813/10.lz4": b"ten-ten",
        }

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.list_calls.append(kwargs)
        prefix = str(kwargs["Prefix"])
        contents = [
            {"Key": key, "Size": len(value), "ETag": '"fixture"'}
            for key, value in self.objects.items() if key.startswith(prefix)
        ]
        return {"Contents": contents}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.get_calls.append(kwargs)
        content = self.objects[str(kwargs["Key"])]
        return {"Body": BytesIO(content), "ContentLength": len(content)}


class InterruptedBody(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.calls = 0

    def read(self, size: int = -1) -> bytes:
        self.calls += 1
        if self.calls > 1:
            raise OSError("connection interrupted")
        return super().read(min(size, 2))


class InterruptedS3(FakeS3):
    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.get_calls.append(kwargs)
        content = self.objects[str(kwargs["Key"])]
        return {"Body": InterruptedBody(content), "ContentLength": len(content)}


class HyperCoreSourceAcquisitionTests(unittest.TestCase):
    def test_resolves_bounded_official_hourly_objects_and_reuses_verified_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeS3()
            acquisition = HyperCoreSourceAcquisition(
                Path(temp) / "cache", s3_client_factory=lambda: fake,
                now=lambda: datetime(2026, 8, 13, 12, 34, tzinfo=timezone.utc),
            )
            self.assertEqual(discovery_preset("quick")["hourly_object_count"], 1)
            sources = acquisition.resolve_hourly_objects(2)
            self.assertEqual([source.key for source in sources], [
                "node_fills_by_block/hourly/20260813/10.lz4",
                "node_fills_by_block/hourly/20260813/11.lz4",
            ])
            self.assertTrue(all(call["RequestPayer"] == "requester" for call in fake.list_calls))
            self.assertTrue(all(str(call["Prefix"]) != "node_fills_by_block/" for call in fake.list_calls))

            plan = acquisition.preflight(sources)
            self.assertEqual((plan["objects_planned"], plan["bytes_to_download"]), (2, 13))
            path, metadata = acquisition.acquire(sources[0], protected_paths=plan["protected_paths"])
            self.assertEqual((path.read_bytes(), metadata["data_hour_start"]), (b"ten-ten", "2026-08-13T10:00:00+00:00"))
            acquisition.acquire(sources[0])
            self.assertEqual(len(fake.get_calls), 1)

            status = acquisition.source_status(test_access=True)
            self.assertEqual((status["connection_state"], status["requester_pays_access"]), ("READY", "READY"))

    def test_cache_sha256_detects_truncation_tampering_wrong_or_legacy_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fake = FakeS3()
            acquisition = HyperCoreSourceAcquisition(
                Path(temp) / "cache", s3_client_factory=lambda: fake,
                now=lambda: datetime(2026, 8, 13, 12, 34, tzinfo=timezone.utc),
            )
            source = acquisition.resolve_hourly_objects(1)[0]
            path, metadata = acquisition.acquire(source)
            metadata_path = path.with_suffix(path.suffix + ".metadata.json")
            self.assertEqual(metadata["sha256"], acquisition._file_sha256(path))
            acquisition.acquire(source)
            self.assertEqual(len(fake.get_calls), 1)  # validated cache reuse
            for corrupted_metadata, corrupted_bytes in (
                ({**metadata}, b"x"),  # truncated cache
                ({**metadata}, b"X" * len(path.read_bytes())),  # same-size tampering
                ({**metadata, "sha256": "0" * 64}, path.read_bytes()),  # incorrect digest
                ({key: value for key, value in metadata.items() if key != "sha256"}, path.read_bytes()),  # legacy metadata
            ):
                path.write_bytes(corrupted_bytes)
                metadata_path.write_text(json.dumps(corrupted_metadata), encoding="utf-8")
                restored, refreshed = acquisition.acquire(source)
                self.assertEqual(restored.read_bytes(), fake.objects[source.key])
                self.assertEqual(refreshed["sha256"], acquisition._file_sha256(restored))
            self.assertEqual(len(fake.get_calls), 5)

    def test_interrupted_download_leaves_no_accepted_file_or_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fake = InterruptedS3()
            acquisition = HyperCoreSourceAcquisition(
                Path(temp) / "cache", s3_client_factory=lambda: fake,
                now=lambda: datetime(2026, 8, 13, 12, 34, tzinfo=timezone.utc),
            )
            source = acquisition.resolve_hourly_objects(1)[0]
            path, _ = acquisition._paths_for(source)
            with self.assertRaises(HyperCoreSourceError):
                acquisition.acquire(source)
            self.assertFalse(path.exists())
            self.assertFalse(path.with_suffix(path.suffix + ".partial").exists())


if __name__ == "__main__":
    unittest.main()
