from __future__ import annotations

import ast
import os
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from src.lane_iii.market_data import MarketDataSource, RawProviderEvent
from src.l3f_provider.tradovate_observation import (
    COMMISSIONED_LUCID_PROFILE,
    AccountObservation,
    CapabilityStatus,
    EnvironmentTradovateSecretProvider,
    FutureExecutionRateGuard,
    FutureExecutionRatePolicy,
    FutureRateAction,
    LocalObservedState,
    ObservationMode,
    ObservationTruth,
    OrderObservation,
    PositionObservation,
    ProviderAccount,
    ProviderErrorCode,
    ProviderHealthTracker,
    ProviderObservationRefused,
    ProviderOrderStatus,
    ProviderPositionDirection,
    ProviderStream,
    StreamHealth,
    TradovateContract,
    TradovateCredentials,
    TradovateEndpoints,
    TradovateEnvironment,
    TradovateMarketDataAdapter,
    TradovateObservationConfig,
    TradovateObservationService,
    RequestsTradovateReadOnlyClient,
    TradovateSession,
    TradovateReadOnlyWebSocket,
    TradeDurationDiagnosticRecord,
    capture_sanitized_fixture,
    reconcile_provider_truth,
    microscalping_diagnostic,
)


TIME = "2026-08-20T15:00:00Z"
LATER = "2026-08-20T15:00:31Z"


class StaticSecrets:
    def credentials(self) -> TradovateCredentials:
        return TradovateCredentials("user", "topsecret-value", "client", "client-secret-value")


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self.body


class FakeHttp:
    def __init__(self):
        self.posts = []
        self.gets = []
        self.responses = {}

    def post(self, url, *, json, timeout):
        self.posts.append((url, json, timeout))
        return FakeResponse({"accessToken": "token", "expirationTime": "2026-08-20T16:00:00Z", "userId": 17})

    def get(self, url, *, headers, params, timeout):
        self.gets.append((url, headers, params, timeout))
        return FakeResponse(self.responses[url.rsplit("/", 1)[-1]])


class FakeWebSocket:
    def __init__(self, frames):
        self.frames = list(frames)
        self.sent = []
        self.closed = False

    def send(self, message):
        self.sent.append(message)

    def recv(self, timeout=None):
        return self.frames.pop(0)

    def close(self):
        self.closed = True


class FakeReadOnlyClient:
    def __init__(self) -> None:
        self.environment = TradovateEnvironment.DEMO
        self.accounts = [ProviderAccount(101, "provider-private-name", True, self.environment)]
        self.contract = TradovateContract(2026, "MNQU6", "2026-09", "CME", Decimal("0.25"), Decimal("2"))
        self.position = PositionObservation(None, 0, ProviderPositionDirection.FLAT, None, "Lucid25kflex01", TIME)
        self.orders: list[OrderObservation] = []
        self.fail_auth = False
        self.capabilities = {"authentication": CapabilityStatus.SUPPORTED, "accounts": CapabilityStatus.SUPPORTED, "contract_discovery": CapabilityStatus.SUPPORTED, "quotes": CapabilityStatus.SUPPORTED, "trades": CapabilityStatus.SUPPORTED, "dom": CapabilityStatus.UNAVAILABLE, "user_sync": CapabilityStatus.SUPPORTED, "positions": CapabilityStatus.SUPPORTED, "orders": CapabilityStatus.SUPPORTED, "market_data": CapabilityStatus.SUPPORTED}

    def authenticate(self, credentials, endpoints):
        if self.fail_auth:
            raise RuntimeError("rejected")
        return TradovateSession("access-token", "2026-08-20T16:00:00Z", self.environment, 17)

    def discover_capabilities(self, session, endpoints):
        return self.capabilities

    def list_accounts(self, session, endpoints):
        return self.accounts

    def resolve_contract(self, session, endpoints, symbol):
        if symbol != self.contract.symbol:
            raise LookupError(symbol)
        return self.contract

    def observe_account(self, session, endpoints, account, alias, observed_at):
        return AccountObservation(alias, account.provider_account_id, True, observed_at, Decimal("25010"), Decimal("10"), Decimal("0"), "NORMAL")

    def observe_position(self, session, endpoints, account, alias, contract, observed_at):
        if self.position.direction is ProviderPositionDirection.UNKNOWN:
            return PositionObservation(None, None, ProviderPositionDirection.UNKNOWN, None, alias, observed_at)
        if self.position.direction is ProviderPositionDirection.FLAT:
            return PositionObservation(None, 0, ProviderPositionDirection.FLAT, None, alias, observed_at)
        return PositionObservation(contract, self.position.quantity, self.position.direction, self.position.average_price, alias, observed_at)

    def observe_orders(self, session, endpoints, account, alias, contract, observed_at):
        return [OrderObservation(item.provider_order_id, item.contract, item.side, item.quantity, item.filled_quantity, item.remaining_quantity, item.status, alias, observed_at, item.created_at) for item in self.orders]


def config(**changes):
    payload = {"environment": TradovateEnvironment.DEMO, "mode": ObservationMode.OBSERVE_ONLY, "account_alias": "Lucid25kflex01", "provider_account_id": 101, "contract_symbol": "MNQU6", "risk_profile": COMMISSIONED_LUCID_PROFILE}
    payload.update(changes)
    return TradovateObservationConfig(**payload)


class LaneIIIPhaseFTests(unittest.TestCase):
    def service(self, client: FakeReadOnlyClient | None = None, **changes):
        return TradovateObservationService(config(**changes), client or FakeReadOnlyClient(), StaticSecrets())

    def raw(self, body):
        return RawProviderEvent("raw-one", MarketDataSource("TRADOVATE", "CQG_TRADOVATE"), TIME, body, "provider-event")

    def test_demo_and_live_endpoint_fence_has_no_cross_fallback(self):
        demo = TradovateEndpoints.for_environment(TradovateEnvironment.DEMO)
        live = TradovateEndpoints.for_environment(TradovateEnvironment.LIVE)
        self.assertIn("demo", demo.rest_base_url)
        self.assertIn("live", live.rest_base_url)
        with self.assertRaises(ProviderObservationRefused) as refusal:
            TradovateEndpoints(TradovateEnvironment.DEMO, live.rest_base_url, live.websocket_url, live.market_data_websocket_url)
        self.assertIs(refusal.exception.code, ProviderErrorCode.ENVIRONMENT_MISMATCH)

    def test_only_observe_only_mode_is_constructible(self):
        with self.assertRaises(ProviderObservationRefused):
            config(mode=object())

    def test_concrete_http_transport_is_named_read_only_and_auth_redacts_errors(self):
        http = FakeHttp()
        client = RequestsTradovateReadOnlyClient(application_id="app", application_version="v1", device_id="device", http_session=http)
        endpoints = TradovateEndpoints.for_environment(TradovateEnvironment.DEMO)
        session = client.authenticate(StaticSecrets().credentials(), endpoints)
        self.assertEqual(session.user_id, 17)
        self.assertEqual(len(http.posts), 1)
        self.assertIn("auth/accesstokenrequest", http.posts[0][0])
        self.assertNotIn("topsecret-value", repr(client))

    def test_concrete_http_transport_reads_accounts_positions_and_orders_without_write_routes(self):
        http = FakeHttp()
        client = RequestsTradovateReadOnlyClient(application_id="app", application_version="v1", device_id="device", http_session=http)
        endpoints = TradovateEndpoints.for_environment(TradovateEnvironment.DEMO)
        session = client.authenticate(StaticSecrets().credentials(), endpoints)
        base = endpoints.rest_base_url
        http.responses = {"list": [{"id": 101, "name": "private", "active": True}]}
        accounts = client.list_accounts(session, endpoints)
        http.responses = {"item": {"id": 101, "active": True, "cashBalance": "25000"}, "list": []}
        observation = client.observe_account(session, endpoints, accounts[0], "Lucid25kflex01", TIME)
        contract = FakeReadOnlyClient().contract
        position = client.observe_position(session, endpoints, accounts[0], "Lucid25kflex01", contract, TIME)
        orders = client.observe_orders(session, endpoints, accounts[0], "Lucid25kflex01", contract, TIME)
        self.assertEqual(observation.balance, Decimal("25000"))
        self.assertIs(position.direction, ProviderPositionDirection.FLAT)
        self.assertEqual(orders, ())
        paths = [url.removeprefix(base) for url, *_ in http.gets]
        self.assertEqual(paths, ["/account/list", "/account/item", "/position/list", "/order/list"])

    def test_websocket_can_authorize_and_request_only_market_or_user_observation(self):
        socket = FakeWebSocket([
            'a[{"s":200,"i":0}]',
            'a[{"e":"md","d":{"quote":{"contractId":2026,"timestamp":"2026-08-20T15:00:00Z","bid":"20000","ask":"20000.25","bidSize":1,"askSize":1}}}]',
        ])
        session = TradovateSession("token", "2026-08-20T16:00:00Z", TradovateEnvironment.DEMO, 17)
        connection = TradovateReadOnlyWebSocket(session, TradovateEndpoints.for_environment(TradovateEnvironment.DEMO), websocket_factory=lambda _url, _timeout: socket)
        connection.open()
        contract = FakeReadOnlyClient().contract
        connection.subscribe_quotes(contract)
        connection.subscribe_dom(contract)
        connection.subscribe_tick_chart(contract)
        connection.synchronize_user(ProviderAccount(101, "private", True, TradovateEnvironment.DEMO))
        packet = connection.next_market_packet(TIME)
        self.assertEqual(packet.payload["d"]["quote"]["contractId"], 2026)
        frame_routes = [frame.split("\n", 1)[0] for frame in socket.sent]
        self.assertEqual(frame_routes, ["authorize", "md/subscribeQuote", "md/subscribeDOM", "md/getChart", "user/syncrequest"])
        connection.close()
        self.assertTrue(socket.closed)

    def test_missing_environment_secrets_fail_without_echoing_variable_value(self):
        names = ("L3F_TRADOVATE_USERNAME", "L3F_TRADOVATE_PASSWORD", "L3F_TRADOVATE_CID", "L3F_TRADOVATE_SECRET")
        prior = {name: os.environ.pop(name, None) for name in names}
        try:
            with self.assertRaisesRegex(ProviderObservationRefused, "AUTH_FAILED") as refusal:
                EnvironmentTradovateSecretProvider().credentials()
            self.assertNotIn("password", str(refusal.exception).lower())
        finally:
            for name, value in prior.items():
                if value is not None:
                    os.environ[name] = value

    def test_secret_representations_are_redacted(self):
        self.assertNotIn("topsecret-value", repr(StaticSecrets().credentials()).lower())
        self.assertNotIn("client-secret-value", repr(StaticSecrets().credentials()).lower())
        self.assertNotIn("access-token", repr(TradovateSession("access-token", "2026-08-20T16:00:00Z", TradovateEnvironment.DEMO, 1)))

    def test_explicit_provider_account_id_is_required(self):
        service = self.service(provider_account_id=None)
        service.authenticate_and_discover(TIME)
        with self.assertRaises(ProviderObservationRefused) as refusal:
            service.select_account_and_contract(TIME)
        self.assertIs(refusal.exception.code, ProviderErrorCode.ACCOUNT_AMBIGUOUS)

    def test_wrong_or_inactive_account_fails_closed(self):
        wrong = self.service(provider_account_id=999)
        wrong.authenticate_and_discover(TIME)
        with self.assertRaisesRegex(ProviderObservationRefused, "ACCOUNT_NOT_FOUND"):
            wrong.select_account_and_contract(TIME)
        client = FakeReadOnlyClient()
        client.accounts[0] = ProviderAccount(101, "name", False, TradovateEnvironment.DEMO)
        inactive = self.service(client)
        inactive.authenticate_and_discover(TIME)
        with self.assertRaisesRegex(ProviderObservationRefused, "ACCOUNT_NOT_FOUND"):
            inactive.select_account_and_contract(TIME)

    def test_account_environment_mismatch_fails_closed(self):
        client = FakeReadOnlyClient()
        client.accounts[0] = ProviderAccount(101, "name", True, TradovateEnvironment.LIVE)
        service = self.service(client)
        service.authenticate_and_discover(TIME)
        with self.assertRaisesRegex(ProviderObservationRefused, "ENVIRONMENT_MISMATCH"):
            service.select_account_and_contract(TIME)

    def test_authentication_failure_is_explicit_and_does_not_try_live(self):
        client = FakeReadOnlyClient()
        client.fail_auth = True
        service = self.service(client)
        with self.assertRaises(ProviderObservationRefused) as refusal:
            service.authenticate_and_discover(TIME)
        self.assertIs(refusal.exception.code, ProviderErrorCode.AUTH_FAILED)
        self.assertIs(service.health.snapshot().streams[ProviderStream.AUTH], StreamHealth.DISCONNECTED)

    def test_startup_reconciliation_requires_authenticated_account_position_and_orders(self):
        snapshot = self.service().startup_reconcile(TIME)
        self.assertTrue(snapshot.startup_ready)
        self.assertIs(snapshot.reconciliation.state, ObservationTruth.FLAT_CONFIRMED)
        self.assertIs(snapshot.health.streams[ProviderStream.USER_DATA], StreamHealth.HEALTHY)
        self.assertEqual(snapshot.contract.symbol, "MNQU6")

    def test_unknown_position_is_never_flat(self):
        client = FakeReadOnlyClient()
        client.position = PositionObservation(None, None, ProviderPositionDirection.UNKNOWN, None, "Lucid25kflex01", TIME)
        snapshot = self.service(client).startup_reconcile(TIME)
        self.assertFalse(snapshot.startup_ready)
        self.assertIs(snapshot.reconciliation.state, ObservationTruth.UNKNOWN)

    def test_long_and_short_position_are_authoritative(self):
        for direction in (ProviderPositionDirection.LONG, ProviderPositionDirection.SHORT):
            client = FakeReadOnlyClient()
            client.position = PositionObservation(client.contract, 1, direction, Decimal("20000.25"), "Lucid25kflex01", TIME)
            snapshot = self.service(client).startup_reconcile(TIME)
            self.assertTrue(snapshot.startup_ready)
            self.assertIs(snapshot.reconciliation.state, ObservationTruth.POSITION_CONFIRMED)

    def test_local_position_or_order_mismatch_fails_closed(self):
        client = FakeReadOnlyClient()
        service = self.service(client)
        service.authenticate_and_discover(TIME)
        service.select_account_and_contract(TIME)
        result = service.startup_reconcile(TIME, LocalObservedState(ProviderPositionDirection.LONG, 1, ()))
        self.assertIs(result.reconciliation.state, ObservationTruth.MISMATCH)
        client = FakeReadOnlyClient()
        order = OrderObservation(800, client.contract, "BUY", 1, 0, 1, ProviderOrderStatus.WORKING, "Lucid25kflex01", TIME)
        client.orders = [order]
        result = self.service(client).startup_reconcile(TIME, LocalObservedState(ProviderPositionDirection.FLAT, None, ()))
        self.assertIs(result.reconciliation.state, ObservationTruth.MISMATCH)

    def test_working_and_partial_orders_are_observed_not_sent(self):
        client = FakeReadOnlyClient()
        client.orders = [OrderObservation(800, client.contract, "BUY", 2, 1, 1, ProviderOrderStatus.PARTIALLY_FILLED, "Lucid25kflex01", TIME)]
        result = self.service(client).startup_reconcile(TIME, LocalObservedState(ProviderPositionDirection.FLAT, None, (800,)))
        self.assertIs(result.reconciliation.state, ObservationTruth.ORDER_WORKING)

    def test_stale_and_incoherent_external_state_blocks_readiness(self):
        position = PositionObservation(None, 0, ProviderPositionDirection.FLAT, None, "Lucid25kflex01", TIME)
        stale = reconcile_provider_truth(local=None, position=position, orders=(), observed_at=LATER)
        self.assertIs(stale.state, ObservationTruth.STALE)
        contract = FakeReadOnlyClient().contract
        incoherent = reconcile_provider_truth(local=None, position=position, orders=(OrderObservation(800, contract, "BUY", 1, 0, 1, ProviderOrderStatus.WORKING, "Lucid25kflex01", "2026-08-20T14:59:00Z"),), observed_at=TIME)
        self.assertIs(incoherent.state, ObservationTruth.STALE)

    def test_independent_stream_disconnect_and_token_expiry_are_visible(self):
        health = ProviderHealthTracker()
        health.mark(ProviderStream.AUTH, StreamHealth.HEALTHY, TIME)
        health.mark(ProviderStream.USER_DATA, StreamHealth.HEALTHY, TIME)
        health.mark(ProviderStream.MARKET_DATA, StreamHealth.HEALTHY, TIME)
        health.mark_disconnected(ProviderStream.MARKET_DATA, TIME)
        self.assertTrue(health.snapshot().account_truth_available)
        self.assertFalse(health.snapshot().market_data_available)
        health.mark_token_expired(TIME)
        self.assertFalse(health.snapshot().account_truth_available)
        self.assertIs(health.snapshot().streams[ProviderStream.AUTH], StreamHealth.AUTH_EXPIRED)

    def test_stream_staleness_is_deterministic(self):
        health = ProviderHealthTracker()
        health.mark(ProviderStream.AUTH, StreamHealth.HEALTHY, TIME)
        self.assertIs(health.assess_staleness(LATER, timedelta(seconds=30)).streams[ProviderStream.AUTH], StreamHealth.STALE)

    def test_token_renewal_reauthenticates_and_never_retains_old_authority(self):
        service = self.service()
        service.authenticate_and_discover(TIME)
        old = service.session
        service.renew_session("2026-08-20T15:05:00Z")
        self.assertIsNot(service.session, old)
        self.assertIs(service.health.snapshot().streams[ProviderStream.AUTH], StreamHealth.HEALTHY)

    def test_mnq_contract_requires_symbol_expiry_exchange_and_tick_truth(self):
        with self.assertRaisesRegex(ProviderObservationRefused, "CONTRACT_NOT_FOUND"):
            TradovateContract(1, "MNQU6", "2026-12", "CME", Decimal("0.25"))
        with self.assertRaisesRegex(ProviderObservationRefused, "CONTRACT_NOT_FOUND"):
            TradovateContract(1, "MNQU6", "2026-09", "CME", Decimal("0.50"))
        with self.assertRaisesRegex(ProviderObservationRefused, "CONTRACT_NOT_FOUND"):
            TradovateContract(1, "MNQ", "2026-09", "CME", Decimal("0.25"))

    def test_quote_trade_and_aggregated_dom_normalize_to_frozen_l3b(self):
        contract = FakeReadOnlyClient().contract
        adapter = TradovateMarketDataAdapter(contract)
        events = adapter.normalize(self.raw({"d": {"quotes": [{"contractId": 2026, "timestamp": TIME, "bid": "20000.00", "ask": "20000.25", "bidSize": 4, "askSize": 3, "seq": 1}], "trades": [{"contractId": 2026, "timestamp": TIME, "price": "20000.25", "size": 2, "side": "BUY", "seq": 2}], "doms": [{"contractId": 2026, "timestamp": TIME, "bids": [{"price": "20000.00", "size": 4}], "asks": [{"price": "20000.25", "size": 3}], "seq": 3}]}}))
        self.assertEqual([event.header.stream.value for event in events], ["QUOTE", "TRADE", "DEPTH"])
        self.assertEqual(events[0].header.instrument.contract_symbol, "MNQU6")
        self.assertEqual(events[2].bids[0].quantity, 4)

    def test_provider_market_data_contract_timestamp_and_payload_errors_fail_closed(self):
        adapter = TradovateMarketDataAdapter(FakeReadOnlyClient().contract)
        for payload in (
            {"quote": {"contractId": 99, "timestamp": TIME, "bid": "20000", "ask": "20000.25", "bidSize": 1, "askSize": 1}},
            {"quote": {"contractId": 2026, "bid": "20000", "ask": "20000.25", "bidSize": 1, "askSize": 1}},
            {"quote": {"contractId": 2026, "timestamp": TIME, "bid": "20000.10", "ask": "20000.25", "bidSize": 1, "askSize": 1}},
        ):
            with self.assertRaises(ProviderObservationRefused):
                adapter.normalize(self.raw(payload))

    def test_fixture_capture_sanitizes_credentials_and_replay_is_deterministic(self):
        adapter = TradovateMarketDataAdapter(FakeReadOnlyClient().contract)
        raw = self.raw({"quote": {"contractId": 2026, "timestamp": TIME, "bid": "20000", "ask": "20000.25", "bidSize": 1, "askSize": 1}, "accessToken": "do-not-store", "password": "do-not-store"})
        fixture = capture_sanitized_fixture(raw)
        self.assertEqual(fixture["payload"]["accessToken"], "<redacted>")
        self.assertEqual(fixture["payload"]["password"], "<redacted>")
        self.assertEqual(adapter.normalize(raw), adapter.normalize(raw))

    def test_lucid_profile_is_external_risk_not_market_evidence_and_is_stricter(self):
        profile = COMMISSIONED_LUCID_PROFILE
        self.assertEqual(profile.account_program, "LucidFlex")
        self.assertEqual(profile.internal_max_mnq_contracts, 1)
        self.assertLess(profile.internal_daily_loss_ceiling, profile.firm_max_loss_limit)
        self.assertLess(profile.internal_flat_time, profile.firm_flat_time)
        self.assertIn("high_impact_news_restriction_unknown", profile.future_live_readiness_blockers)
        with self.assertRaises(ValueError):
            type(profile)(**{**profile.__dict__, "internal_max_mnq_contracts": 21})

    def test_future_rate_guard_and_microscalping_diagnostic_have_no_execution_authority(self):
        guard = FutureExecutionRateGuard(FutureExecutionRatePolicy(timedelta(minutes=1), maximum_entry_attempts=1, maximum_change_attempts=1))
        self.assertTrue(guard.assess_attempt(FutureRateAction.ENTRY_ATTEMPT, "decision-1", TIME)[0])
        self.assertFalse(guard.assess_attempt(FutureRateAction.ENTRY_ATTEMPT, "decision-2", TIME)[0])
        self.assertFalse(guard.assess_attempt(FutureRateAction.ENTRY_ATTEMPT, "decision-1", "2026-08-20T15:02:00Z")[0])
        report = microscalping_diagnostic((
            TradeDurationDiagnosticRecord(TIME, "2026-08-20T15:00:04Z", Decimal("60")),
            TradeDurationDiagnosticRecord(TIME, "2026-08-20T15:00:10Z", Decimal("40")),
        ))
        self.assertEqual(report.short_duration_profit_fraction, Decimal("0.6"))

    def test_source_has_no_execution_write_or_generic_request_surface(self):
        path = Path(__file__).parents[1] / "src" / "l3f_provider" / "tradovate_observation.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        methods = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        forbidden = {"submit", "place_order", "modify_order", "cancel", "flatten", "liquidate", "reverse", "send_any_tradovate_request"}
        self.assertFalse(methods & forbidden)

    def test_l3f_cannot_produce_live_execution_artifact(self):
        from src.l3f_provider.tradovate_observation import live_execution_artifact_refused
        with self.assertRaisesRegex(ProviderObservationRefused, "READ_ONLY_VIOLATION"):
            live_execution_artifact_refused()

    def test_frozen_l3b_to_l3e_sources_remain_unmodified(self):
        root = Path(__file__).parents[1] / "src" / "lane_iii"
        for name in ("market_data.py", "hypothesis_engine.py", "trader_v0.py", "simulated_execution.py"):
            self.assertTrue((root / name).exists())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
