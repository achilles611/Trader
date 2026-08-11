from __future__ import annotations

import io
import json
import tempfile
import unittest
import asyncio
from datetime import timedelta
from pathlib import Path

from src.copytrade.control_center import CandidateDiscoveryOrchestrator, ControlCenterStore, create_control_center_app, discovery_job_configuration
from src.copytrade.models import Target, utc_now
from src.copytrade.service import CopyTradeService
from src.copytrade.source_acquisition import (
    OFFICIAL_BUCKET,
    OFFICIAL_PREFIX,
    HyperCoreSourceAcquisition,
    HyperCoreSourceError,
)
from tests.test_copytrade_control_center import config


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, access_error: Exception | None = None) -> None:
        self.objects = objects
        self.access_error = access_error
        self.get_calls = 0
        self.requests: list[dict[str, object]] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.requests.append(kwargs)
        if self.access_error:
            raise self.access_error
        now = utc_now()
        return {"Contents": [
            {"Key": key, "Size": len(value), "LastModified": now, "ETag": f"etag-{index}"}
            for index, (key, value) in enumerate(sorted(self.objects.items()), 1)
        ], "IsTruncated": False}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.requests.append(kwargs)
        if self.access_error:
            raise self.access_error
        self.get_calls += 1
        value = self.objects[str(kwargs["Key"])]
        return {"Body": io.BytesIO(value), "ContentLength": len(value)}


def fill(wallet: str, tid: int) -> dict[str, object]:
    return {"user": wallet, "time": int(utc_now().timestamp() * 1000), "coin": "BTC", "px": "100", "sz": "1", "tid": tid}


class SourceAcquisitionTests(unittest.TestCase):
    def _source(self, root: Path, records: list[dict[str, object]] | None = None) -> tuple[HyperCoreSourceAcquisition, FakeS3]:
        payload = "\n".join(json.dumps(item) for item in (records or [fill(WALLET_A, 1), fill(WALLET_A, 2)])).encode("utf-8")
        client = FakeS3({f"{OFFICIAL_PREFIX}20260811/12": payload})
        return HyperCoreSourceAcquisition(root / "hypercore-cache", s3_client_factory=lambda: client), client

    def test_official_resolver_is_deterministic_and_cache_reuses_complete_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source, client = self._source(Path(temp))
            plan = source.resolve_recent(timedelta(hours=1))
            self.assertEqual(plan[0].identifier, f"s3://{OFFICIAL_BUCKET}/{OFFICIAL_PREFIX}20260811/12")
            self.assertTrue(all(request.get("RequestPayer") == "requester" for request in client.requests))
            path, metadata = source.acquire(plan[0])
            self.assertTrue(path.exists())
            self.assertEqual(metadata["source_transport"], "aws_s3_requester_pays")
            source.acquire(plan[0])
            self.assertEqual(client.get_calls, 1)
            self.assertEqual(source.cache_status()["object_count"], 1)

    def test_partial_cache_objects_are_never_accepted_and_source_errors_are_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source, client = self._source(Path(temp))
            item = source.resolve_recent(timedelta(hours=1))[0]
            path, _ = source._paths_for(item)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_suffix(path.suffix + ".partial").write_bytes(b"incomplete")
            cached, _ = source.acquire(item)
            self.assertEqual(cached, path)
            self.assertFalse(path.with_suffix(path.suffix + ".partial").exists())
            self.assertEqual(client.get_calls, 1)
            with self.assertRaisesRegex(HyperCoreSourceError, "Only the documented official"):
                source.acquire(item.__class__(bucket="other", key="anything", size=1, last_modified=None))

        with tempfile.TemporaryDirectory() as temp:
            denied = HyperCoreSourceAcquisition(Path(temp) / "cache", s3_client_factory=lambda: FakeS3({}, access_error=RuntimeError("AccessDenied requester pays")))
            status = denied.source_status(test_access=True)
            self.assertEqual(status["connection_state"], "SETUP_REQUIRED")
            self.assertIn("requester-pays authorization was denied", status["message"])
            self.assertNotIn("secret", json.dumps(status).lower())

    def test_orchestration_uses_frozen_phase_a_and_preserves_operator_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            source, _ = self._source(root, [fill(WALLET_A, 1), fill(WALLET_A, 1), fill(WALLET_A, 2), fill(WALLET_B, 3), fill(WALLET_B, 4)])
            store = ControlCenterStore(service.config.artifacts.database_path)
            store.initialize()
            configuration = discovery_job_configuration({"preset": "quick", "candidate_limit": 1, "min_activity": 2, "max_activity_age": "30d"})
            first = store.create_job(job_type="candidate_discovery", configuration=configuration)
            CandidateDiscoveryOrchestrator(service, store, source).run(first["job_id"])
            completed = store.get_job(first["job_id"])
            assert completed is not None
            self.assertEqual(completed["status"], "completed")
            self.assertIn("discovery_run_id", completed["result"])
            self.assertEqual(completed["result"]["wallets_observed"], 2)
            self.assertEqual(len(service.database.list_discovery_candidates()), 1)
            self.assertFalse(service.database.list_targets("active"))
            self.assertFalse(service.database.list_virtual_positions(open_only=True))
            self.assertEqual(service.monitored_execution_wallets(), [])
            with service.database._connect() as connection:
                phase_b_before = connection.execute("SELECT COUNT(*) FROM copy_analysis_runs").fetchone()[0]
                provenance = connection.execute("SELECT configuration_json FROM copy_discovery_runs WHERE run_id=?", (completed["result"]["discovery_run_id"],)).fetchone()[0]
            self.assertIn("official_hypercore_requester_pays_cache", provenance)
            self.assertEqual(phase_b_before, 0)

            discovered = service.database.list_discovery_candidates()[0]["wallet"]
            service.database.set_target_status(discovered, "shadow")
            second = store.create_job(job_type="candidate_discovery", configuration={**configuration, "candidate_limit": 2})
            CandidateDiscoveryOrchestrator(service, store, source).run(second["job_id"])
            self.assertEqual(service.database.get_target(discovered).status, "shadow")  # type: ignore[union-attr]
            self.assertEqual(store.get_job(second["job_id"])["status"], "completed")  # type: ignore[index]
            self.assertEqual(ControlCenterStore(service.config.artifacts.database_path).get_job(first["job_id"])["status"], "completed")  # type: ignore[index]

    def test_acquisition_failure_is_persisted_without_mutating_phase_a_or_paper_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            source = HyperCoreSourceAcquisition(root / "cache", s3_client_factory=lambda: FakeS3({}, access_error=RuntimeError("AccessDenied requester pays")))
            store = ControlCenterStore(service.config.artifacts.database_path)
            job = store.create_job(job_type="candidate_discovery", configuration=discovery_job_configuration({"preset": "quick"}))
            CandidateDiscoveryOrchestrator(service, store, source).run(job["job_id"])
            failed = store.get_job(job["job_id"])
            assert failed is not None
            self.assertEqual(failed["status"], "failed")
            self.assertIn("requester-pays authorization was denied", failed["message"])
            self.assertFalse(service.database.list_discovery_candidates())
            self.assertFalse(service.database.list_targets("active"))
            self.assertFalse(service.database.list_virtual_positions(open_only=True))

    def test_status_api_is_safe_and_rejects_generic_download_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            source, _ = self._source(root)
            app = create_control_center_app(service.config, service.database, discovery_source=source)
            status_endpoint = next(route.endpoint for route in app.routes if route.path == "/api/discovery/status")
            status = __import__("asyncio").run(status_endpoint())
            self.assertEqual(status["candidate_universe_count"], 0)
            self.assertNotIn("aws_secret_access_key", json.dumps(status).lower())
            self.assertNotIn("aws_session_token", json.dumps(status).lower())
            start_endpoint = next(route.endpoint for route in app.routes if route.path == "/api/discovery/jobs")
            with self.assertRaises(Exception) as rejected:
                __import__("asyncio").run(start_endpoint({"url": "https://example.invalid"}))
            self.assertEqual(rejected.exception.status_code, 400)
            with self.assertRaises(Exception) as rejected_path:
                __import__("asyncio").run(start_endpoint({"path": "C:\\Windows\\system.ini"}))
            self.assertEqual(rejected_path.exception.status_code, 400)

    def test_api_background_job_persists_progress_and_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            source, _ = self._source(root, [fill(WALLET_A, 1), fill(WALLET_A, 2)])
            app = create_control_center_app(service.config, service.database, discovery_source=source)

            async def exercise() -> None:
                start = next(route.endpoint for route in app.routes if route.path == "/api/discovery/jobs" and "POST" in route.methods)
                detail = next(route.endpoint for route in app.routes if route.path == "/api/discovery/jobs/{job_id}")
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
            self.assertEqual(len(service.database.list_discovery_candidates()), 1)
