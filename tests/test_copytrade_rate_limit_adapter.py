from __future__ import annotations

import unittest

import requests

from src.copytrade.config import SourceConfig
from src.copytrade.hyperliquid import HyperliquidAPIError, HyperliquidPublicAdapter
from src.copytrade.rate_limit import HyperliquidInfoRateLimiter


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


if __name__ == "__main__":
    unittest.main()
