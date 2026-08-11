from __future__ import annotations

import asyncio
import io
import json
import shutil
import tempfile
import unittest
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.copytrade.control_center import CandidateDiscoveryOrchestrator, ControlCenterStore, create_control_center_app, discovery_job_configuration
from src.copytrade.models import utc_now
from src.copytrade.service import CopyTradeService
from src.copytrade.source_acquisition import (
    OFFICIAL_BUCKET,
    OFFICIAL_HOURLY_PREFIX,
    OFFICIAL_PREFIX,
    HyperCoreSourceAcquisition,
    HyperCoreSourceError,
)
from tests.test_copytrade_control_center import config


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"
NOW = datetime(2026, 8, 11, 20, 25, tzinfo=timezone.utc)


def hour_key(value: datetime, suffix: str = "") -> str:
    value = value.astimezone(timezone.utc)
    return f"{OFFICIAL_HOURLY_PREFIX}{value:%Y%m%d}/{value.hour}{suffix}"


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, last_modified: dict[str, datetime] | None = None,
                 access_error: Exception | None = None, get_error: Exception | None = None) -> None:
        self.objects = objects
        self.last_modified = last_modified or {}
        self.access_error = access_error
        self.get_error = get_error
        self.get_calls = 0
        self.requests: list[dict[str, object]] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.requests.append(kwargs)
        if self.access_error:
            raise self.access_error
        prefix, limit = str(kwargs.get("Prefix") or ""), int(kwargs.get("MaxKeys") or 1000)
        keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
        contents = [
            {"Key": key, "Size": len(self.objects[key]), "LastModified": self.last_modified.get(key, NOW), "ETag": f"etag-{index}"}
            for index, key in enumerate(keys[:limit], 1)
        ]
        return {"Contents": contents, "IsTruncated": len(keys) > limit}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.requests.append(kwargs)
        if self.access_error:
            raise self.access_error
        if self.get_error:
            raise self.get_error
        self.get_calls += 1
        value = self.objects[str(kwargs["Key"])]
        return {"Body": io.BytesIO(value), "ContentLength": len(value)}


def fill(wallet: str, tid: int) -> dict[str, object]:
    return {"user": wallet, "time": int(utc_now().timestamp() * 1000), "coin": "BTC", "px": "100", "sz": "1", "tid": tid}


class SourceAcquisitionTests(unittest.TestCase):
    def _source(self, root: Path, objects: dict[str, bytes] | None = None, **kwargs: object) -> tuple[HyperCoreSourceAcquisition, FakeS3]:
        data = objects or {hour_key(NOW - timedelta(hours=1)): b'{"user":"fixture"}\n'}
        max_cache_bytes = int(kwargs.pop("max_cache_bytes", 5 * 1024 * 1024 * 1024))
        client = FakeS3(data, **kwargs)
        return HyperCoreSourceAcquisition(root / "hypercore-cache", s3_client_factory=lambda: client, now=lambda: NOW,
                                          max_cache_bytes=max_cache_bytes), client

    @staticmethod
    def _hours(count: int, *, missing: set[int] | None = None, payload: bytes = b"x") -> dict[str, bytes]:
        missing = missing or set()
        return {hour_key(NOW - timedelta(hours=offset)): payload for offset in range(1, count + len(missing) + 1) if offset not in missing}

    def test_resolver_uses_completed_path_hours_not_current_or_last_modified(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            current_key = hour_key(NOW)
            hour_19, hour_18 = hour_key(NOW - timedelta(hours=1)), hour_key(NOW - timedelta(hours=2))
            objects = {current_key: b"current", hour_19: b"one", hour_18: b"two"}
            source, client = self._source(Path(temp), objects, last_modified={hour_18: NOW, hour_19: NOW - timedelta(days=90)})
            plan = source.resolve_hourly_objects(2)
            self.assertEqual([item.key for item in plan], [hour_18, hour_19])
            self.assertEqual([item.data_hour_start for item in plan], ["2026-08-11T18:00:00+00:00", "2026-08-11T19:00:00+00:00"])
            self.assertTrue(all(str(request["Prefix"]).startswith(OFFICIAL_HOURLY_PREFIX) for request in client.requests))
            self.assertFalse(any(request.get("Prefix") == OFFICIAL_PREFIX for request in client.requests))
            self.assertTrue(all(request.get("RequestPayer") == "requester" for request in client.requests))

    def test_quick_standard_and_deep_have_exact_hourly_object_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source, _ = self._source(Path(temp), self._hours(24))
            self.assertEqual(len(source.resolve_hourly_objects(1)), 1)
            self.assertEqual(len(source.resolve_hourly_objects(6)), 6)
            self.assertEqual(len(source.resolve_hourly_objects(24)), 24)

    def test_missing_recent_and_interior_hours_walk_back_with_a_bounded_lookback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            objects = self._hours(10, missing={1, 4})
            source, _ = self._source(Path(temp), objects)
            plan = source.resolve_hourly_objects(6, lookback_hours=10)
            self.assertEqual([item.hour for item in plan], [12, 13, 14, 15, 17, 18])
            limited, _ = self._source(Path(temp) / "limited", {hour_key(NOW - timedelta(hours=8)): b"x"})
            with self.assertRaisesRegex(HyperCoreSourceError, "only 0 were available within the bounded 3-hour"):
                limited.resolve_hourly_objects(1, lookback_hours=3)

    def test_exact_hourly_key_parser_rejects_unrelated_objects_and_accepts_actual_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            good = hour_key(NOW - timedelta(hours=1), ".lz4")
            bad = f"{OFFICIAL_HOURLY_PREFIX}20260811/19/manifest.json"
            source, _ = self._source(Path(temp), {good: b"a", bad: b"manifest"})
            plan = source.resolve_hourly_objects(1)
            self.assertEqual(plan[0].key, good)
            self.assertEqual(plan[0].hour, 19)
            self.assertEqual(plan[0].data_hour_end, "2026-08-11T20:00:00+00:00")
            with self.assertRaisesRegex(HyperCoreSourceError, "official HyperCore hourly"):
                source.acquire(plan[0].__class__(OFFICIAL_BUCKET, bad, 1, None))

    def test_preflight_counts_cache_and_prunes_only_unrelated_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            objects = {
                hour_key(NOW - timedelta(hours=1)): b"a" * 10,
                hour_key(NOW - timedelta(hours=2)): b"b" * 10,
                hour_key(NOW - timedelta(hours=3)): b"c" * 10,
            }
            source, client = self._source(root, objects, max_cache_bytes=25)
            first, second, unrelated = source.resolve_hourly_objects(3)
            source.acquire(first)
            unrelated_path, _ = source.acquire(unrelated)
            plan = source.preflight([first, second])
            first_path, _ = source._paths_for(first)
            self.assertEqual(plan["objects_cached"], 1)
            self.assertEqual(plan["bytes_to_download"], 10)
            self.assertTrue(first_path.exists())
            self.assertFalse(unrelated_path.exists())
            source.acquire(second, protected_paths=plan["protected_paths"])
            self.assertTrue(first_path.exists())
            self.assertEqual(client.get_calls, 3)

    def test_preflight_too_large_and_disk_failure_happen_before_first_get(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            objects = {hour_key(NOW - timedelta(hours=1)): b"a" * 60, hour_key(NOW - timedelta(hours=2)): b"b" * 60}
            source, client = self._source(Path(temp), objects, max_cache_bytes=100)
            with self.assertRaisesRegex(HyperCoreSourceError, "staging space"):
                source.preflight(source.resolve_hourly_objects(2))
            self.assertEqual(client.get_calls, 0)

        with tempfile.TemporaryDirectory() as temp:
            source, client = self._source(Path(temp), {hour_key(NOW - timedelta(hours=1)): b"x" * 10})
            DiskUsage = namedtuple("usage", "total used free")
            with patch("src.copytrade.source_acquisition.shutil.disk_usage", return_value=DiskUsage(100, 100, 0)):
                with self.assertRaisesRegex(HyperCoreSourceError, "Insufficient free disk"):
                    source.preflight(source.resolve_hourly_objects(1))
            self.assertEqual(client.get_calls, 0)

    def test_failed_get_leaves_no_partial_and_fully_cached_standard_makes_zero_gets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source, client = self._source(Path(temp), {hour_key(NOW - timedelta(hours=1)): b"x" * 10}, get_error=RuntimeError("network unavailable"))
            item = source.resolve_hourly_objects(1)[0]
            path, _ = source._paths_for(item)
            with self.assertRaises(HyperCoreSourceError):
                source.acquire(item)
            self.assertFalse(path.with_suffix(path.suffix + ".partial").exists())

        with tempfile.TemporaryDirectory() as temp:
            source, client = self._source(Path(temp), self._hours(6, payload=b"abcdef"))
            plan = source.resolve_hourly_objects(6)
            preflight = source.preflight(plan)
            for item in plan:
                source.acquire(item, protected_paths=preflight["protected_paths"])
            client.get_calls = 0
            cached = source.preflight(plan)
            for item in plan:
                source.acquire(item, protected_paths=cached["protected_paths"])
            self.assertEqual(cached["objects_cached"], 6)
            self.assertEqual(cached["bytes_to_download"], 0)
            self.assertEqual(client.get_calls, 0)

    def test_source_access_state_machine_is_safe_and_cheap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, _ = self._source(root)
            with patch.object(source, "credentials_detected", return_value=False):
                missing = source.source_status()
            self.assertEqual((missing["connection_state"], missing["requester_pays_access"]), ("SETUP_REQUIRED", "UNTESTED"))
            untested = source.source_status()
            self.assertEqual((untested["connection_state"], untested["requester_pays_access"]), ("UNTESTED", "UNTESTED"))
            ready = source.source_status(test_access=True)
            self.assertEqual((ready["connection_state"], ready["requester_pays_access"]), ("READY", "READY"))
            self.assertEqual(ready["probe_object_count"], 1)
            self.assertNotIn("secret", json.dumps(ready).lower())
            denied, _ = self._source(root / "denied", access_error=RuntimeError("AccessDenied requester pays"))
            failed = denied.source_status(test_access=True)
            self.assertEqual((failed["connection_state"], failed["requester_pays_access"]), ("SETUP_REQUIRED", "FAILED"))
            outage, _ = self._source(root / "outage", access_error=RuntimeError("service unavailable"))
            unavailable = outage.source_status(test_access=True)
            self.assertEqual((unavailable["connection_state"], unavailable["requester_pays_access"]), ("UNAVAILABLE", "FAILED"))

    def test_orchestration_uses_frozen_phase_a_and_preserves_operator_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            payload = "\n".join(json.dumps(item) for item in [fill(WALLET_A, 1), fill(WALLET_A, 1), fill(WALLET_A, 2), fill(WALLET_B, 3), fill(WALLET_B, 4)]).encode()
            source, _ = self._source(root, {hour_key(NOW - timedelta(hours=1)): payload})
            store = ControlCenterStore(service.config.artifacts.database_path)
            first = store.create_job(job_type="candidate_discovery", configuration=discovery_job_configuration({"preset": "quick", "candidate_limit": 1}))
            CandidateDiscoveryOrchestrator(service, store, source).run(first["job_id"])
            completed = store.get_job(first["job_id"])
            assert completed is not None
            self.assertEqual(completed["status"], "completed")
            self.assertEqual(completed["result"]["hourly_objects"], 1)
            self.assertEqual(completed["result"]["source_first_hour"], "2026-08-11T19:00:00+00:00")
            self.assertIn("data_hour_start", completed["result"]["source_metadata"][0])
            self.assertEqual(len(service.database.list_discovery_candidates()), 1)
            self.assertFalse(service.database.list_targets("active"))
            self.assertFalse(service.database.list_virtual_positions(open_only=True))
            self.assertEqual(service.monitored_execution_wallets(), [])
            with service.database._connect() as connection:
                phase_b_before = connection.execute("SELECT COUNT(*) FROM copy_analysis_runs").fetchone()[0]
            self.assertEqual(phase_b_before, 0)

            discovered = service.database.list_discovery_candidates()[0]["wallet"]
            service.database.set_target_status(discovered, "shadow")
            second = store.create_job(job_type="candidate_discovery", configuration={**first["configuration"], "candidate_limit": 2})
            CandidateDiscoveryOrchestrator(service, store, source).run(second["job_id"])
            self.assertEqual(service.database.get_target(discovered).status, "shadow")  # type: ignore[union-attr]

    def test_api_requires_successful_probe_and_background_job_persists_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            payload = "\n".join(json.dumps(item) for item in [fill(WALLET_A, 1), fill(WALLET_A, 2)]).encode()
            source, _ = self._source(root, {hour_key(NOW - timedelta(hours=1)): payload})
            app = create_control_center_app(service.config, service.database, discovery_source=source)
            start = next(route.endpoint for route in app.routes if route.path == "/api/discovery/jobs" and "POST" in route.methods)
            detail = next(route.endpoint for route in app.routes if route.path == "/api/discovery/jobs/{job_id}")

            async def exercise() -> None:
                with self.assertRaises(Exception) as rejected:
                    await start({"preset": "quick"})
                self.assertEqual(rejected.exception.status_code, 409)
                source.source_status(test_access=True)
                async with app.router.lifespan_context(app):
                    created = await start({"preset": "quick", "candidate_limit": 1})
                    for _ in range(100):
                        current = await detail(created["job_id"])
                        if current["status"] in {"completed", "completed_with_warnings", "failed"}:
                            break
                        await asyncio.sleep(0.01)
                    self.assertEqual(current["status"], "completed")
                    self.assertIn("discovery_run_id", current["result"])

            asyncio.run(exercise())
