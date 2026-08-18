from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.execution import AmbiguousSubmissionError, HyperliquidTestnetExecutionEngine
from src.copytrade.execution_contracts import (
    ExecutionSafetyContext,
    ExecutionState,
    ExposureEffect,
    SubmissionRequest,
    VenueOrderStatus,
)
from src.copytrade.hyperliquid_testnet import (
    HYPERLIQUID_TESTNET_API_URL,
    HYPERLIQUID_TESTNET_DOMAIN,
    EnvironmentApiWalletSecretProvider,
    HyperliquidConfigurationRefused,
    HyperliquidCredentialRefused,
    HyperliquidSdkClients,
    HyperliquidTestnetConfig,
    HyperliquidTestnetExecutionAdapter,
    HyperliquidVenueEvidenceRefused,
    derive_hyperliquid_cloid,
)
from src.copytrade.storage import CopyTradeDatabase
from src.lane_ii.boundary import OperationalInput, OperationalInputSource, TradeDirection
from src.lane_ii.phase_d_bridge import ExecutionSizingEvidence, LaneIIPhaseDBridge, VerifiedPositionTruth
from src.lane_ii.trader_v0 import TraderV0, TraderV0DecisionInput, create_f1_trade_intent


TIME = "2026-08-18T00:00:00Z"
ACCOUNT = "0x" + "a" * 40
SIGNER = "0x" + "b" * 40
DUMMY_KEY = "0x" + "1" * 64


class DummySecretProvider:
    def __init__(self, value: str | None = DUMMY_KEY) -> None:
        self.value = value

    def get_api_wallet_private_key(self) -> str:
        if self.value is None:
            raise HyperliquidCredentialRefused("missing")
        return self.value

    def __repr__(self) -> str:
        return "DummySecretProvider(<redacted>)"


class RateLimitedError(RuntimeError):
    status_code = 429


class FakeHyperliquidExchange:
    def __init__(self) -> None:
        self.base_url = HYPERLIQUID_TESTNET_API_URL
        self.modes: list[str] = []
        self.cancel_modes: list[str] = []
        self.orders: dict[str, dict[str, object]] = {}
        self.fills: list[dict[str, object]] = []
        self.positions: dict[str, float] = {}
        self.order_calls: list[dict[str, object]] = []
        self.cancel_calls: list[dict[str, object]] = []
        self.schedule_calls: list[int | None] = []
        self.expiry_values: list[int | None] = []
        self.next_oid = 100

    def set_expires_after(self, value: int | None) -> None:
        self.expiry_values.append(value)

    def order(
        self, symbol: str, is_buy: bool, size: float, price: float, order_type: object,
        *, reduce_only: bool, cloid: object,
    ) -> dict[str, object]:
        raw = cloid.to_raw()
        self.order_calls.append({
            "symbol": symbol, "is_buy": is_buy, "size": size, "price": price,
            "order_type": order_type, "reduce_only": reduce_only, "cloid": raw,
        })
        mode = self.modes.pop(0) if self.modes else "filled"
        if mode == "rate_limit":
            raise RateLimitedError("private material must never persist: " + DUMMY_KEY)
        if mode == "throw_before_record":
            raise TimeoutError("private material must never persist: " + DUMMY_KEY)
        oid = self.next_oid
        self.next_oid += 1
        remaining = size
        status = "open"
        if mode in {"filled", "timeout_after_fill"}:
            remaining, status = 0.0, "filled"
        elif mode == "partial":
            remaining = size / 2
        order = {
            "coin": symbol, "side": "B" if is_buy else "A", "limitPx": str(price),
            "sz": str(remaining), "origSz": str(size), "oid": oid,
            "timestamp": 1_776_729_600_000, "cloid": raw,
        }
        self.orders[raw] = {"order": order, "status": status, "statusTimestamp": 1_776_729_600_000}
        filled = size - remaining
        if filled:
            self._add_fill(order, filled)
        if mode == "timeout_after_fill":
            raise TimeoutError("connection closed after write")
        if mode == "malformed":
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"mystery": {}}]}}}
        if mode == "reject":
            self.orders.pop(raw, None)
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"error": "bad"}]}}}
        if mode == "err":
            self.orders.pop(raw, None)
            return {"status": "err", "response": "rejected"}
        status_payload = {"filled": {"oid": oid}} if status == "filled" else {"resting": {"oid": oid}}
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": [status_payload]}}}

    def cancel_by_cloid(self, symbol: str, cloid: object) -> dict[str, object]:
        raw = cloid.to_raw()
        self.cancel_calls.append({"symbol": symbol, "cloid": raw})
        mode = self.cancel_modes.pop(0) if self.cancel_modes else "cancel"
        wrapper = self.orders[raw]
        if mode == "fill_race":
            order = wrapper["order"]
            assert isinstance(order, dict)
            remaining = float(order["sz"])
            if remaining:
                self._add_fill(order, remaining)
                order["sz"] = "0"
            wrapper["status"] = "filled"
        else:
            wrapper["status"] = "canceled"
        if mode == "ambiguous":
            raise TimeoutError("cancel response lost")
        return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}

    def schedule_cancel(self, value: int | None) -> dict[str, str]:
        self.schedule_calls.append(value)
        return {"status": "ok"}

    def _add_fill(self, order: dict[str, object], quantity: float, *, tid: int | None = None) -> None:
        fill_id = tid if tid is not None else len(self.fills) + 1
        side = str(order["side"])
        symbol = str(order["coin"])
        self.fills.append({
            "coin": symbol, "px": order["limitPx"], "sz": str(quantity), "side": side,
            "time": 1_776_729_600_000 + fill_id, "oid": order["oid"], "tid": fill_id,
            "fee": "0.01", "hash": "0x" + f"{fill_id:064x}",
        })
        signed = quantity if side == "B" else -quantity
        self.positions[symbol] = self.positions.get(symbol, 0.0) + signed
        if abs(self.positions[symbol]) <= 1e-12:
            self.positions.pop(symbol)


class FakeHyperliquidInfo:
    def __init__(self, exchange: FakeHyperliquidExchange) -> None:
        self.base_url = HYPERLIQUID_TESTNET_API_URL
        self.exchange = exchange
        self.query_users: list[str] = []
        self.malformed_metadata = False
        self.unknown_status = False
        self.account_mismatch = False
        self.foreign_orders: list[dict[str, object]] = []

    def meta_and_asset_ctxs(self) -> object:
        if self.malformed_metadata:
            return [{"universe": [{"name": "BTC", "szDecimals": "bad"}]}, [{}]]
        return [
            {"universe": [{"name": "BTC", "szDecimals": 2}, {"name": "ETH", "szDecimals": 3}]},
            [{"midPx": "100", "markPx": "100"}, {"midPx": "2000", "markPx": "2000"}],
        ]

    def query_order_by_cloid(self, user: str, cloid: object) -> dict[str, object]:
        self.query_users.append(user)
        wrapper = self.exchange.orders.get(cloid.to_raw())
        if wrapper is None:
            return {"status": "unknownOid"}
        copied = dict(wrapper)
        if self.unknown_status:
            copied["status"] = "unrecognizedVenueState"
        response: dict[str, object] = {"status": "order", "order": copied}
        if self.account_mismatch:
            response["user"] = SIGNER
        return response

    def user_fills(self, user: str) -> list[dict[str, object]]:
        self.query_users.append(user)
        return list(self.exchange.fills)

    def open_orders(self, user: str) -> list[dict[str, object]]:
        self.query_users.append(user)
        rows = [
            dict(wrapper["order"])
            for wrapper in self.exchange.orders.values()
            if wrapper["status"] == "open"
        ]
        return [*rows, *self.foreign_orders]

    def user_state(self, user: str) -> dict[str, object]:
        self.query_users.append(user)
        state: dict[str, object] = {
            "assetPositions": [
                {"position": {"coin": symbol, "szi": str(quantity), "entryPx": "100"}}
                for symbol, quantity in sorted(self.exchange.positions.items())
            ],
            "marginSummary": {"accountValue": "1000", "totalMarginUsed": "10"},
            "withdrawable": "900",
        }
        if self.account_mismatch:
            state["user"] = SIGNER
        return state


class FakeSdkFactory:
    def __init__(self, exchange: FakeHyperliquidExchange | None = None, *, signer: str = SIGNER) -> None:
        self.exchange = exchange or FakeHyperliquidExchange()
        self.info = FakeHyperliquidInfo(self.exchange)
        self.signer = signer
        self.create_calls = 0

    def create(self, private_key: str, config: HyperliquidTestnetConfig) -> HyperliquidSdkClients:
        self.create_calls += 1
        if private_key != DUMMY_KEY:
            raise AssertionError("test factory received unexpected dummy key")
        return HyperliquidSdkClients(self.exchange, self.info, self.signer)


class PhaseF3HyperliquidTestnetTests(unittest.TestCase):
    def config(self, **changes: object) -> HyperliquidTestnetConfig:
        payload: dict[str, object] = {
            "account_address": ACCOUNT, "account_kind": "MASTER", "maximum_slippage_bps": 25.0,
            "minimum_order_notional_usd": 10.0,
        }
        payload.update(changes)
        return HyperliquidTestnetConfig(**payload)  # type: ignore[arg-type]

    def adapter(
        self, *, factory: FakeSdkFactory | None = None, config: HyperliquidTestnetConfig | None = None,
    ) -> tuple[HyperliquidTestnetExecutionAdapter, FakeSdkFactory]:
        selected = factory or FakeSdkFactory()
        adapter = HyperliquidTestnetExecutionAdapter(
            config or self.config(), DummySecretProvider(), sdk_factory=selected, clock=lambda: TIME,
        )
        return adapter, selected

    @staticmethod
    def request(**changes: object) -> SubmissionRequest:
        payload: dict[str, object] = {
            "intent_id": "intent-1", "submission_id": "submission-1", "client_order_id": "client-order-1",
            "symbol": "BTC", "side": "BUY", "quantity": 0.5,
            "exposure_effect": ExposureEffect.INCREASE, "reduce_only": False,
        }
        payload.update(changes)
        return SubmissionRequest(**payload)  # type: ignore[arg-type]

    def test_missing_secret_and_master_wallet_substitution_refuse(self) -> None:
        factory = FakeSdkFactory()
        with self.assertRaises(HyperliquidCredentialRefused):
            HyperliquidTestnetExecutionAdapter(
                self.config(), DummySecretProvider(None), sdk_factory=factory, clock=lambda: TIME,
            )
        with self.assertRaisesRegex(HyperliquidCredentialRefused, "distinct"):
            HyperliquidTestnetExecutionAdapter(
                self.config(), DummySecretProvider(), sdk_factory=FakeSdkFactory(signer=ACCOUNT), clock=lambda: TIME,
            )

    def test_environment_secret_provider_has_no_default_and_redacts_repr(self) -> None:
        name = "HYPERLIQUID_TESTNET_API_WALLET_PRIVATE_KEY_TEST_MISSING"
        os.environ.pop(name, None)
        provider = EnvironmentApiWalletSecretProvider(name)
        self.assertNotIn(DUMMY_KEY, repr(provider))
        with self.assertRaises(HyperliquidCredentialRefused):
            provider.get_api_wallet_private_key()

    def test_only_exact_testnet_host_and_explicit_account_kind_are_accepted(self) -> None:
        refused = (
            "https://api.hyperliquid.xyz",
            "https://evil.invalid",
            HYPERLIQUID_TESTNET_API_URL + "/exchange",
            HYPERLIQUID_TESTNET_API_URL + "/",
        )
        for host in refused:
            with self.subTest(host=host), self.assertRaises(HyperliquidConfigurationRefused):
                self.config(base_url=host)
        with self.assertRaises(HyperliquidConfigurationRefused):
            self.config(account_kind="AUTO")
        self.assertEqual(self.config().base_url, HYPERLIQUID_TESTNET_API_URL)

    def test_runtime_configuration_mutation_cannot_retarget_existing_adapter(self) -> None:
        config = self.config()
        adapter, _ = self.adapter(config=config)
        object.__setattr__(config, "base_url", "https://api.hyperliquid.xyz")
        with self.assertRaises(HyperliquidConfigurationRefused):
            adapter.get_positions()

    def test_deterministic_cloid_is_stable_across_restart_and_scoped(self) -> None:
        first, _ = self.adapter()
        second, _ = self.adapter()
        one = derive_hyperliquid_cloid(
            execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
            execution_account_id=first.execution_account_id,
            client_order_id="durable-client-order",
        )
        two = derive_hyperliquid_cloid(
            execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
            execution_account_id=second.execution_account_id,
            client_order_id="durable-client-order",
        )
        self.assertEqual(one, two)
        self.assertRegex(one, r"^0x[0-9a-f]{32}$")
        self.assertNotEqual(one, derive_hyperliquid_cloid(
            execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
            execution_account_id=first.execution_account_id,
            client_order_id="other-client-order",
        ))

    def test_startup_reconciliation_is_required_before_entry(self) -> None:
        adapter, factory = self.adapter()
        rejected = adapter.submit(self.request())
        self.assertEqual(rejected.status, VenueOrderStatus.REJECTED)
        self.assertIn("startup_reconciliation", rejected.reason)
        self.assertEqual(factory.exchange.order_calls, [])
        adapter.mark_startup_reconciled()
        accepted = adapter.submit(self.request())
        self.assertEqual(accepted.status, VenueOrderStatus.FILLED)

    def test_submission_uses_ioc_explicit_slippage_cloid_expiry_and_reduce_only(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("resting")
        order = adapter.submit(self.request())
        self.assertEqual(order.status, VenueOrderStatus.ACKNOWLEDGED)
        call = factory.exchange.order_calls[0]
        self.assertEqual(call["order_type"], {"limit": {"tif": "Ioc"}})
        self.assertEqual(call["price"], 100.25)
        self.assertFalse(call["reduce_only"])
        self.assertRegex(str(call["cloid"]), r"^0x[0-9a-f]{32}$")
        self.assertEqual(factory.exchange.expiry_values[-1], 1_787_011_205_000)

    def test_definitive_rejection_is_not_ambiguous(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        for mode in ("reject", "err"):
            factory.exchange.modes.append(mode)
            result = adapter.submit(self.request(client_order_id=f"reject-{mode}"))
            self.assertEqual(result.status, VenueOrderStatus.REJECTED)
            self.assertEqual(result.reason, "hyperliquid_definitive_rejection")

    def test_timeout_after_possible_transmission_is_unknown_and_reconciled_by_cloid(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("timeout_after_fill")
        request = self.request()
        with self.assertRaises(AmbiguousSubmissionError):
            adapter.submit(request)
        self.assertEqual(len(factory.exchange.order_calls), 1)
        order = adapter.get_order(request.client_order_id)
        assert order is not None
        self.assertEqual(order.status, VenueOrderStatus.FILLED)
        self.assertEqual(factory.info.query_users[-1], ACCOUNT)

    def test_malformed_post_write_response_is_ambiguous(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("malformed")
        with self.assertRaises(AmbiguousSubmissionError):
            adapter.submit(self.request())
        self.assertEqual(len(factory.exchange.order_calls), 1)
        self.assertFalse(adapter.entry_transport_health()[0])

    def test_partial_and_duplicate_fills_normalize_with_durable_identity(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("partial")
        request = self.request()
        adapter.submit(request)
        fills = adapter.list_fills(request.client_order_id)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].quantity, 0.25)
        factory.exchange.fills.append(dict(factory.exchange.fills[0]))
        replay = adapter.list_fills(request.client_order_id)
        self.assertEqual(replay[0].venue_fill_id, replay[1].venue_fill_id)

    def test_cancel_uses_cloid_and_ambiguous_cancel_reconciles(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.extend(("resting", "resting"))
        first = self.request(client_order_id="cancel-one")
        adapter.submit(first)
        cancelled = adapter.cancel(first.client_order_id)
        self.assertEqual(cancelled.status, VenueOrderStatus.CANCELLED)
        self.assertEqual(factory.exchange.cancel_calls[0]["cloid"], derive_hyperliquid_cloid(
            execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
            execution_account_id=adapter.execution_account_id,
            client_order_id=first.client_order_id,
        ))
        second = self.request(client_order_id="cancel-two")
        adapter.submit(second)
        factory.exchange.cancel_modes.append("ambiguous")
        with self.assertRaises(AmbiguousSubmissionError):
            adapter.cancel(second.client_order_id)
        reconciled = adapter.get_order(second.client_order_id)
        assert reconciled is not None
        self.assertEqual(reconciled.status, VenueOrderStatus.CANCELLED)

    def test_fill_racing_cancel_remains_filled(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("resting")
        request = self.request()
        adapter.submit(request)
        factory.exchange.cancel_modes.append("fill_race")
        result = adapter.cancel(request.client_order_id)
        self.assertEqual(result.status, VenueOrderStatus.FILLED)
        self.assertEqual(len(adapter.list_fills(request.client_order_id)), 1)

    def test_quantity_precision_minimum_and_reduce_only_bounds_fail_closed(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        precision = adapter.submit(self.request(quantity=0.001))
        self.assertEqual(precision.status, VenueOrderStatus.REJECTED)
        self.assertIn("precision", precision.reason)
        below_notional = adapter.submit(self.request(client_order_id="below-notional", quantity=0.01))
        self.assertEqual(below_notional.status, VenueOrderStatus.REJECTED)
        self.assertIn("notional", below_notional.reason)
        factory.exchange.positions["BTC"] = 0.25
        too_large = adapter.submit(self.request(
            client_order_id="reduce-large", side="SELL", quantity=0.5,
            exposure_effect=ExposureEffect.FLATTEN, reduce_only=True,
        ))
        self.assertEqual(too_large.status, VenueOrderStatus.REJECTED)
        self.assertIn("exceeds", too_large.reason)
        wrong_way = adapter.submit(self.request(
            client_order_id="reduce-wrong", side="BUY", quantity=0.25,
            exposure_effect=ExposureEffect.FLATTEN, reduce_only=True,
        ))
        self.assertEqual(wrong_way.status, VenueOrderStatus.REJECTED)
        self.assertIn("direction", wrong_way.reason)
        self.assertEqual(factory.exchange.order_calls, [])

    def test_account_reads_use_trading_account_not_api_wallet(self) -> None:
        adapter, factory = self.adapter()
        factory.exchange.positions["BTC"] = 0.5
        positions = adapter.get_positions()
        balances = adapter.get_balances()
        self.assertEqual(positions[0].signed_quantity, 0.5)
        self.assertEqual(balances["account_value"], 1000.0)
        self.assertTrue(factory.info.query_users)
        self.assertEqual(set(factory.info.query_users), {ACCOUNT})
        self.assertNotIn(SIGNER, factory.info.query_users)

    def test_malformed_metadata_unknown_status_and_account_mismatch_fail_closed(self) -> None:
        adapter, factory = self.adapter()
        factory.info.malformed_metadata = True
        with self.assertRaises(HyperliquidVenueEvidenceRefused):
            adapter.get_instrument_metadata("BTC")
        factory.info.malformed_metadata = False
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("resting")
        request = self.request()
        adapter.submit(request)
        factory.info.unknown_status = True
        with self.assertRaisesRegex(HyperliquidVenueEvidenceRefused, "status_unknown"):
            adapter.get_order(request.client_order_id)
        factory.info.unknown_status = False
        factory.info.account_mismatch = True
        with self.assertRaisesRegex(HyperliquidVenueEvidenceRefused, "account_mismatch"):
            adapter.get_positions()

    def test_foreign_open_order_is_explicit_external_activity(self) -> None:
        adapter, factory = self.adapter()
        factory.info.foreign_orders.append({
            "coin": "ETH", "side": "B", "limitPx": "2000", "sz": "0.1", "origSz": "0.1",
            "oid": 999, "timestamp": 1_776_729_600_000,
        })
        orders = adapter.list_open_orders()
        self.assertTrue(orders[0].raw_payload["external_manual_activity"])

    def test_rate_limit_creates_health_latch_and_no_retry_loop(self) -> None:
        adapter, factory = self.adapter()
        adapter.mark_startup_reconciled()
        factory.exchange.modes.append("rate_limit")
        with self.assertRaises(AmbiguousSubmissionError):
            adapter.submit(self.request())
        self.assertEqual(len(factory.exchange.order_calls), 1)
        self.assertFalse(adapter.entry_transport_health()[0])
        rejected = adapter.submit(self.request(client_order_id="after-rate-limit"))
        self.assertEqual(rejected.status, VenueOrderStatus.REJECTED)
        self.assertEqual(len(factory.exchange.order_calls), 1)

    def test_dead_man_renewal_and_clear_are_deterministic_not_per_order(self) -> None:
        adapter, factory = self.adapter()
        first = adapter.renew_dead_man_switch()
        second = adapter.renew_dead_man_switch()
        self.assertEqual(first, 1_787_011_320_000)
        self.assertEqual(second, first)
        self.assertEqual(factory.exchange.schedule_calls, [first])
        adapter.clear_dead_man_switch()
        self.assertEqual(factory.exchange.schedule_calls, [first, None])

    def test_engine_startup_reconciliation_blocks_manual_state_and_clears_flat(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(Path(temp) / "phase-d.sqlite3")
            database.initialize()
            adapter, factory = self.adapter()
            engine = HyperliquidTestnetExecutionEngine(
                database, adapter, execution_account_id=adapter.execution_account_id,
            )
            flat = engine.startup_reconcile()
            self.assertTrue(flat["entries_eligible"])
            factory.exchange.positions["BTC"] = 1.0
            second, _ = self.adapter(factory=factory)
            second_engine = HyperliquidTestnetExecutionEngine(
                database, second, execution_account_id=second.execution_account_id,
            )
            mismatch = second_engine.startup_reconcile()
            self.assertFalse(mismatch["entries_eligible"])
            self.assertFalse(second.entry_transport_health()[0])

    def test_engine_unknown_submission_reconciles_without_blind_resubmit_or_secret_leak(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(Path(temp) / "phase-d.sqlite3")
            database.initialize()
            adapter, factory = self.adapter()
            engine = HyperliquidTestnetExecutionEngine(
                database, adapter, safety_context=ExecutionSafetyContext(),
                execution_account_id=adapter.execution_account_id,
            )
            engine.startup_reconcile()
            intent = self._bridge_entry(database, adapter)
            factory.exchange.modes.append("timeout_after_fill")
            unknown = engine.resume_intent(intent.intent_id, context=ExecutionSafetyContext())
            self.assertEqual(unknown.state, ExecutionState.SUBMISSION_UNKNOWN)
            filled = engine.resume_intent(intent.intent_id, context=ExecutionSafetyContext())
            self.assertEqual(filled.state, ExecutionState.FILLED)
            self.assertEqual(len(factory.exchange.order_calls), 1)
            raw_db = (Path(temp) / "phase-d.sqlite3").read_bytes()
            self.assertNotIn(DUMMY_KEY.encode(), raw_db)
            self.assertNotIn(DUMMY_KEY, repr(adapter))

    def test_duplicate_fill_is_deduplicated_by_phase_d_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "phase-d.sqlite3")
            database.initialize()
            adapter, factory = self.adapter()
            engine = HyperliquidTestnetExecutionEngine(
                database, adapter, execution_account_id=adapter.execution_account_id,
            )
            engine.startup_reconcile()
            intent = self._bridge_entry(database, adapter)
            result = engine.resume_intent(intent.intent_id, context=ExecutionSafetyContext())
            self.assertEqual(result.state, ExecutionState.FILLED)
            factory.exchange.fills.append(dict(factory.exchange.fills[0]))
            restarted = HyperliquidTestnetExecutionEngine(
                CopyTradeDatabase(root / "phase-d.sqlite3"), adapter,
                execution_account_id=adapter.execution_account_id,
            )
            restarted.reconcile_intent(intent.intent_id)
            self.assertEqual(len(database.list_execution_fills(intent.intent_id)), 1)

    def test_complete_fake_testnet_entry_exit_lifecycle_reaches_verified_flat(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(Path(temp) / "phase-d.sqlite3")
            database.initialize()
            adapter, _ = self.adapter()
            engine = HyperliquidTestnetExecutionEngine(
                database, adapter, execution_account_id=adapter.execution_account_id,
            )
            self.assertTrue(engine.startup_reconcile()["entries_eligible"])
            entry = self._bridge_entry(database, adapter)
            self.assertEqual(
                engine.resume_intent(entry.intent_id, context=ExecutionSafetyContext()).state,
                ExecutionState.FILLED,
            )
            exit_decision = TraderV0().decide(self._decision_input(position_open=True, hard_risk_exit=True))
            bridge = LaneIIPhaseDBridge(
                database, execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
                execution_account_id=adapter.execution_account_id, phase_d_notional_limit=50.0,
                clock=lambda: TIME,
            )
            exit_intent = bridge.admit_verified_flatten(
                exit_decision,
                position=VerifiedPositionTruth(
                    symbol="BTC", signed_quantity=0.5, observed_at=TIME,
                    provenance_hash=hashlib.sha256(b"venue-position").hexdigest(), authoritative=True,
                ),
                sizing=self._sizing(),
            )
            result = engine.resume_intent(
                exit_intent.intent_id,
                context=ExecutionSafetyContext(
                    verified_positions={"BTC": 0.5}, verified_positions_current=True,
                    verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(result.state, ExecutionState.FILLED)
            self.assertEqual(engine.verify_flat()["state"], "VERIFIED_FLAT")

    def _bridge_entry(self, database: CopyTradeDatabase, adapter: HyperliquidTestnetExecutionAdapter):
        bridge = LaneIIPhaseDBridge(
            database, execution_domain=HYPERLIQUID_TESTNET_DOMAIN,
            execution_account_id=adapter.execution_account_id, phase_d_notional_limit=50.0,
            clock=lambda: TIME,
        )
        request = create_f1_trade_intent(TraderV0().decide(self._decision_input()))
        return bridge.admit_entry(request, sizing=self._sizing())

    def _operational_input(self, source: OperationalInputSource, token: str) -> OperationalInput:
        return OperationalInput(
            input_id=f"{source.value}-{token}", source=source, observed_at=TIME,
            payload_hash=token * 64, source_system="f3-test",
        )

    def _decision_input(self, **changes: object) -> TraderV0DecisionInput:
        position_open = changes.get("position_open") is True
        inputs = (
            (self._operational_input(OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE, "e"),)
            if position_open else (
                self._operational_input(OperationalInputSource.LIVE_PUBLIC_WALLET_ACTIVITY, "a"),
                self._operational_input(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA, "b"),
                self._operational_input(OperationalInputSource.OPERATIONAL_INDICATOR, "c"),
                self._operational_input(OperationalInputSource.CONFIGURATION_OR_RISK_POLICY, "d"),
            )
        )
        payload: dict[str, object] = {
            "operational_inputs": inputs, "now": TIME, "symbol": "BTC", "direction": TradeDirection.LONG,
            "source_action_at": TIME, "market_observed_at": TIME, "indicator_ids": ("one", "two"),
            "effective_confidence": 0.60, "expected_gross_edge": 0.02, "estimated_fees": 0.001,
            "estimated_spread": 0.001, "estimated_slippage": 0.001, "estimated_market_impact": 0.001,
            "estimated_latency_cost": 0.001, "alpha_survival": 1.0,
            "requested_notional_ceiling": 50.0, "market_regime": "normal",
        }
        payload.update(changes)
        return TraderV0DecisionInput(**payload)  # type: ignore[arg-type]

    @staticmethod
    def _sizing() -> ExecutionSizingEvidence:
        return ExecutionSizingEvidence(
            symbol="BTC", mark_price=100.0, price_observed_at=TIME, metadata_observed_at=TIME,
            quantity_decimals=2, minimum_quantity=0.01, source="hyperliquid-metaAndAssetCtxs",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
