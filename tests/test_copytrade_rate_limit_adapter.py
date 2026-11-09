from __future__ import annotations

import unittest
import tempfile
import threading
from pathlib import Path

import requests

from src.copytrade.config import SourceConfig
from src.copytrade.hyperliquid import HyperliquidAPIError, HyperliquidPublicAdapter
from src.copytrade.rate_limit import HyperliquidInfoRateLimiter, shared_hyperliquid_info_limiter


class FakeResponse:
    def __init__(self, status_code: int, payload: object = None, *, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.payload = payload
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self) -> object:
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response


class HyperliquidInfoAdapterRateLimitTests(unittest.TestCase):
    def adapter(self, response: FakeResponse) -> tuple[HyperliquidPublicAdapter, FakeSession, HyperliquidInfoRateLimiter]:
        limiter = HyperliquidInfoRateLimiter(operating_budget=900, jitter_seconds=0)
        session = FakeSession(response)
        return HyperliquidPublicAdapter(SourceConfig(info_url="https://example.test/info"), session=session, limiter=limiter), session, limiter

    def test_successful_json_settles_reserved_user_fills_weight(self) -> None:
        adapter, session, limiter = self.adapter(FakeResponse(200, [{}, {}, {}]))
        self.assertEqual(adapter.info({"type": "userFills", "user": "0xabc"}), [{}, {}, {}])
        self.assertEqual(len(session.calls), 1)
        telemetry = limiter.telemetry()
        self.assertEqual((telemetry["requests_last_minute"], telemetry["estimated_weight_last_minute"]), (1, 21))

    def test_429_registers_shared_cooldown_and_does_not_retry_internally(self) -> None:
        adapter, session, limiter = self.adapter(FakeResponse(429, [], headers={"Retry-After": "5"}))
        with self.assertRaises(HyperliquidAPIError):
            adapter.info({"type": "userFills", "user": "0xabc"})
        self.assertEqual(len(session.calls), 1)
        telemetry = limiter.telemetry()
        self.assertEqual((telemetry["429_count"], telemetry["retry_count"], telemetry["state"]), (1, 1, "THROTTLED"))

    def test_non_429_http_failure_has_no_retry_telemetry(self) -> None:
        adapter, session, limiter = self.adapter(FakeResponse(503, []))
        with self.assertRaises(HyperliquidAPIError):
            adapter.info({"type": "portfolio", "user": "0xabc"})
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(limiter.telemetry()["retry_count"], 0)

    def test_default_adapters_share_one_process_limiter_per_info_url(self) -> None:
        source = SourceConfig(info_url="https://adapter-sharing-test.example/info")
        self.assertIs(HyperliquidPublicAdapter(source).limiter, HyperliquidPublicAdapter(source).limiter)

    def test_independent_limiters_share_one_host_sqlite_budget_and_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            first = HyperliquidInfoRateLimiter(operating_budget=200, jitter_seconds=0, coordination_path=path)
            second = HyperliquidInfoRateLimiter(operating_budget=200, jitter_seconds=0, coordination_path=path)
            first.settle(first.acquire({"type": "userFills"}), [{}] * 20)
            second.settle(second.acquire({"type": "userFills"}), [{}] * 20)
            telemetry = second.telemetry()
            self.assertEqual((telemetry["coordination_scope"], telemetry["requests_last_minute"], telemetry["estimated_weight_last_minute"]),
                             ("host_sqlite", 2, 42))
            first.register_429("5")
            first.record_retry()
            throttled = second.telemetry()
            self.assertEqual((throttled["state"], throttled["429_count"], throttled["retry_count"]), ("THROTTLED", 1, 1))

    def test_independent_limiter_instances_coordinate_concurrent_threads(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            limiters = [HyperliquidInfoRateLimiter(operating_budget=100, jitter_seconds=0, coordination_path=path) for _ in range(4)]
            barrier = threading.Barrier(len(limiters))
            failures: list[BaseException] = []

            def worker(limiter: HyperliquidInfoRateLimiter) -> None:
                try:
                    barrier.wait()
                    limiter.settle(limiter.acquire({"type": "allMids"}), {})
                except BaseException as exc:
                    failures.append(exc)

            workers = [threading.Thread(target=worker, args=(limiter,)) for limiter in limiters]
            for worker in workers: worker.start()
            for worker in workers: worker.join()
            self.assertEqual(failures, [])
            telemetry = limiters[0].telemetry()
            self.assertEqual((telemetry["requests_last_minute"], telemetry["estimated_weight_last_minute"]), (4, 8))

    def test_smaller_budget_propagates_to_existing_host_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            first = HyperliquidInfoRateLimiter(operating_budget=900, jitter_seconds=0, coordination_path=path)
            second = HyperliquidInfoRateLimiter(operating_budget=600, jitter_seconds=0, coordination_path=path)
            self.assertEqual(second.telemetry()["operating_budget"], 600)
            # An older object must refresh its larger local value before it
            # reserves against the shared SQLite coordinator.
            self.assertEqual(first.telemetry()["operating_budget"], 600)
            self.assertEqual(first.operating_budget, 600)

    def test_larger_later_budget_cannot_relax_host_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            first = HyperliquidInfoRateLimiter(operating_budget=600, jitter_seconds=0, coordination_path=path)
            second = HyperliquidInfoRateLimiter(operating_budget=900, jitter_seconds=0, coordination_path=path)
            self.assertEqual((first.telemetry()["operating_budget"], second.telemetry()["operating_budget"]), (600, 600))

    def test_shared_limiter_lowering_updates_its_sqlite_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            url = "https://shared-budget-lowering.example/info"
            first = shared_hyperliquid_info_limiter(url, operating_budget=900, coordination_path=path, jitter_seconds=0)
            second = shared_hyperliquid_info_limiter(url, operating_budget=600, coordination_path=path, jitter_seconds=0)
            self.assertIs(first, second)
            self.assertEqual((first.operating_budget, first.telemetry()["operating_budget"]), (600, 600))

    def test_concurrent_coordinator_initialization_keeps_smallest_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shared-rate-limit.sqlite3"
            budgets = [900, 700, 600, 800]
            barrier = threading.Barrier(len(budgets))
            limiters: list[HyperliquidInfoRateLimiter] = []
            failures: list[BaseException] = []

            def worker(budget: int) -> None:
                try:
                    barrier.wait()
                    limiters.append(HyperliquidInfoRateLimiter(operating_budget=budget, jitter_seconds=0, coordination_path=path))
                except BaseException as exc:
                    failures.append(exc)

            workers = [threading.Thread(target=worker, args=(budget,)) for budget in budgets]
            for worker in workers: worker.start()
            for worker in workers: worker.join()
            self.assertEqual(failures, [])
            self.assertEqual(HyperliquidInfoRateLimiter(operating_budget=900, jitter_seconds=0, coordination_path=path).telemetry()["operating_budget"], 600)


if __name__ == "__main__":
    unittest.main()
