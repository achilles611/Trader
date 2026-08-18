"""Authenticated, structurally testnet-only Hyperliquid Phase D adapter.

Signing is delegated to the official ``hyperliquid-python-sdk``.  This module
contains no mainnet mode, transfer, withdrawal, leverage, or strategy surface.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Callable, Protocol

from hyperliquid.utils.types import Cloid

from .execution import AmbiguousSubmissionError
from .execution_contracts import (
    ExposureEffect,
    SubmissionRequest,
    VenueFill,
    VenueOrder,
    VenueOrderStatus,
    VenuePosition,
)
from .models import as_utc, stable_id, utc_now


HYPERLIQUID_TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
HYPERLIQUID_TESTNET_DOMAIN = "HYPERLIQUID_TESTNET"
HYPERLIQUID_TESTNET_ADAPTER_VERSION = "phase-f3-hyperliquid-testnet-v1"
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,31}")


class HyperliquidConfigurationRefused(RuntimeError):
    """Configuration could enable the wrong authority, signer, or host."""


class HyperliquidCredentialRefused(HyperliquidConfigurationRefused):
    """The required API-wallet secret is absent or structurally invalid."""


class HyperliquidVenueEvidenceRefused(RuntimeError):
    """Venue evidence is stale, malformed, inconsistent, or unknown."""


class ApiWalletSecretProvider(Protocol):
    def get_api_wallet_private_key(self) -> str: ...


class HyperliquidSdkFactory(Protocol):
    def create(self, private_key: str, config: "HyperliquidTestnetConfig") -> "HyperliquidSdkClients": ...


@dataclass(frozen=True)
class EnvironmentApiWalletSecretProvider:
    """Resolve the testnet API-wallet key only at adapter construction time."""

    variable_name: str = "HYPERLIQUID_TESTNET_API_WALLET_PRIVATE_KEY"

    def get_api_wallet_private_key(self) -> str:
        value = os.environ.get(self.variable_name)
        if not value:
            raise HyperliquidCredentialRefused("hyperliquid_testnet_api_wallet_secret_missing")
        return value

    def __repr__(self) -> str:
        return f"EnvironmentApiWalletSecretProvider(variable_name={self.variable_name!r}, secret=<runtime-only>)"


@dataclass(frozen=True)
class HyperliquidTestnetConfig:
    account_address: str
    account_kind: str
    maximum_slippage_bps: float
    minimum_order_notional_usd: float
    base_url: str = HYPERLIQUID_TESTNET_API_URL
    action_expiry_ms: int = 5_000
    request_timeout_seconds: float = 10.0
    evidence_ttl_seconds: float = 30.0
    rate_limit_cooldown_seconds: float = 60.0
    dead_man_horizon_seconds: int = 120
    dead_man_renewal_seconds: int = 30

    def __post_init__(self) -> None:
        if self.base_url != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("only_the_exact_hyperliquid_testnet_host_is_permitted")
        if _ADDRESS.fullmatch(self.account_address) is None:
            raise HyperliquidConfigurationRefused("explicit_hyperliquid_trading_account_address_required")
        if self.account_kind not in {"MASTER", "SUBACCOUNT"}:
            raise HyperliquidConfigurationRefused("hyperliquid_account_kind_must_be_master_or_subaccount")
        self._finite_positive(self.maximum_slippage_bps, "maximum_slippage_bps")
        if self.maximum_slippage_bps > 1_000:
            raise HyperliquidConfigurationRefused("maximum_slippage_bps_exceeds_testnet_policy_limit")
        self._finite_positive(self.minimum_order_notional_usd, "minimum_order_notional_usd")
        if self.minimum_order_notional_usd < 10.0:
            raise HyperliquidConfigurationRefused("minimum_order_notional_below_hyperliquid_venue_minimum")
        self._finite_positive(self.request_timeout_seconds, "request_timeout_seconds")
        self._finite_positive(self.evidence_ttl_seconds, "evidence_ttl_seconds")
        self._finite_positive(self.rate_limit_cooldown_seconds, "rate_limit_cooldown_seconds")
        if isinstance(self.action_expiry_ms, bool) or not 1_000 <= self.action_expiry_ms <= 60_000:
            raise HyperliquidConfigurationRefused("action_expiry_ms_out_of_bounds")
        if (
            isinstance(self.dead_man_horizon_seconds, bool)
            or not 10 <= self.dead_man_horizon_seconds <= 3_600
            or isinstance(self.dead_man_renewal_seconds, bool)
            or not 5 <= self.dead_man_renewal_seconds < self.dead_man_horizon_seconds
        ):
            raise HyperliquidConfigurationRefused("dead_man_schedule_is_not_safely_renewable")

    @staticmethod
    def _finite_positive(value: object, field: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise HyperliquidConfigurationRefused(f"{field}_must_be_positive_and_finite")
        if not math.isfinite(float(value)) or float(value) <= 0:
            raise HyperliquidConfigurationRefused(f"{field}_must_be_positive_and_finite")

    @property
    def normalized_account_address(self) -> str:
        return self.account_address.lower()

    @property
    def account_identity_hash(self) -> str:
        return hashlib.sha256(self.normalized_account_address.encode("ascii")).hexdigest()


@dataclass(frozen=True)
class HyperliquidSdkClients:
    exchange: Any
    info: Any
    signer_address: str


class OfficialHyperliquidSdkFactory:
    """Create official SDK clients without retaining the private-key string."""

    def create(self, private_key: str, config: HyperliquidTestnetConfig) -> HyperliquidSdkClients:
        if config.base_url != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("sdk_factory_refused_non_testnet_host")
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info

            wallet = Account.from_key(private_key)
        except Exception as exc:
            raise HyperliquidCredentialRefused("hyperliquid_testnet_api_wallet_secret_invalid") from None
        info = Info(
            HYPERLIQUID_TESTNET_API_URL, skip_ws=True, timeout=config.request_timeout_seconds,
        )
        exchange = Exchange(
            wallet,
            HYPERLIQUID_TESTNET_API_URL,
            vault_address=config.account_address if config.account_kind == "SUBACCOUNT" else None,
            account_address=config.account_address,
            timeout=config.request_timeout_seconds,
        )
        if exchange.base_url != HYPERLIQUID_TESTNET_API_URL or info.base_url != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("official_sdk_client_host_mismatch")
        return HyperliquidSdkClients(exchange=exchange, info=info, signer_address=wallet.address)


def derive_hyperliquid_cloid(
    *, execution_domain: str, execution_account_id: str, client_order_id: str,
) -> str:
    """Derive the stable 128-bit venue identity from Phase D's durable ID."""
    if execution_domain != HYPERLIQUID_TESTNET_DOMAIN:
        raise HyperliquidConfigurationRefused("cloid_derivation_requires_hyperliquid_testnet_domain")
    if not execution_account_id or not client_order_id:
        raise HyperliquidConfigurationRefused("cloid_derivation_identity_missing")
    canonical = json.dumps(
        {
            "schema": "phase-f3-hyperliquid-cloid-v1",
            "execution_domain": execution_domain,
            "execution_account_id": execution_account_id,
            "client_order_id": client_order_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return "0x" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


class HyperliquidTestnetExecutionAdapter:
    """Official-SDK execution adapter with no usable mainnet configuration."""

    adapter_name = "hyperliquid_testnet_execution_adapter"
    adapter_mode = HYPERLIQUID_TESTNET_DOMAIN

    def __init__(
        self,
        config: HyperliquidTestnetConfig,
        secret_provider: ApiWalletSecretProvider,
        *,
        sdk_factory: HyperliquidSdkFactory | None = None,
        clock: Callable[[], object] = utc_now,
    ) -> None:
        if type(config) is not HyperliquidTestnetConfig:
            raise HyperliquidConfigurationRefused("exact_hyperliquid_testnet_config_required")
        if config.base_url != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("adapter_refused_non_testnet_host")
        if secret_provider is None or not callable(getattr(secret_provider, "get_api_wallet_private_key", None)):
            raise HyperliquidCredentialRefused("api_wallet_secret_provider_required")
        private_key = secret_provider.get_api_wallet_private_key()
        if not isinstance(private_key, str) or not private_key.strip():
            raise HyperliquidCredentialRefused("hyperliquid_testnet_api_wallet_secret_missing")
        clients = (sdk_factory or OfficialHyperliquidSdkFactory()).create(private_key, config)
        signer_address = str(clients.signer_address)
        if _ADDRESS.fullmatch(signer_address) is None:
            raise HyperliquidCredentialRefused("api_wallet_signer_address_invalid")
        if signer_address.lower() == config.normalized_account_address:
            raise HyperliquidCredentialRefused("api_wallet_must_be_distinct_from_trading_account")
        if getattr(clients.exchange, "base_url", HYPERLIQUID_TESTNET_API_URL) != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("exchange_client_host_mismatch")
        if getattr(clients.info, "base_url", HYPERLIQUID_TESTNET_API_URL) != HYPERLIQUID_TESTNET_API_URL:
            raise HyperliquidConfigurationRefused("info_client_host_mismatch")
        self.config = config
        self.exchange = clients.exchange
        self.info = clients.info
        self.api_wallet_address = signer_address.lower()
        self.execution_domain = HYPERLIQUID_TESTNET_DOMAIN
        self.execution_account_id = f"HYPERLIQUID_TESTNET:{config.account_identity_hash[:32]}"
        self.clock = clock
        self._signer_lock = threading.Lock()
        self._known_cloids: dict[str, str] = {}
        self._known_clients: dict[str, str] = {}
        self._symbols: dict[str, str] = {}
        self._last_positions_observed_ms: int | None = None
        self._startup_reconciled = False
        self._entry_degraded_until_ms = 0
        self._dead_man_schedule_ms: int | None = None
        self._dead_man_last_renewed_ms: int | None = None

    def __repr__(self) -> str:
        return (
            "HyperliquidTestnetExecutionAdapter("
            f"mode={self.adapter_mode!r}, account_ref={self.config.account_identity_hash[:16]!r}, "
            "api_wallet_secret=<runtime-only>)"
        )

    def entry_transport_health(self) -> tuple[bool, str]:
        if not self._startup_reconciled:
            return False, "entry_blocked_startup_reconciliation_required"
        if self._now_ms() < self._entry_degraded_until_ms:
            return False, "entry_blocked_transport_rate_limited_or_uncertain"
        return True, "hyperliquid_testnet_transport_healthy"

    def mark_startup_reconciled(self) -> None:
        self._startup_reconciled = True

    def submit(self, request: SubmissionRequest) -> VenueOrder:
        self._assert_testnet_binding()
        if type(request) is not SubmissionRequest:
            raise HyperliquidVenueEvidenceRefused("exact_submission_request_required")
        rejection = self._preflight_submission(request)
        if rejection is not None:
            return rejection
        metadata = self.get_instrument_metadata(request.symbol)
        rejection = self._validate_quantity(request, metadata)
        if rejection is not None:
            return rejection
        if request.reduce_only:
            rejection = self._validate_reduce_only(request)
            if rejection is not None:
                return rejection
        cloid_raw = derive_hyperliquid_cloid(
            execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
            client_order_id=request.client_order_id,
        )
        self._remember(request.client_order_id, cloid_raw, request.symbol)
        limit_price = self._aggressive_limit_price(
            mid_price=metadata["mid_price"],
            is_buy=request.side == "BUY",
            size_decimals=metadata["quantity_precision"],
        )
        try:
            with self._signer_lock:
                self.exchange.set_expires_after(self._now_ms() + self.config.action_expiry_ms)
                response = self.exchange.order(
                    request.symbol,
                    request.side == "BUY",
                    request.quantity,
                    limit_price,
                    order_type={"limit": {"tif": "Ioc"}},
                    reduce_only=request.reduce_only,
                    cloid=Cloid.from_str(cloid_raw),
                )
        except Exception as exc:
            self._degrade_after_write_exception(exc)
            raise AmbiguousSubmissionError("hyperliquid_testnet_submission_outcome_unknown") from None
        try:
            return self._normalize_submit_response(request, cloid_raw, response)
        except HyperliquidVenueEvidenceRefused:
            self._degrade_transport()
            raise AmbiguousSubmissionError("hyperliquid_testnet_submission_response_ambiguous") from None

    def cancel(self, client_order_id: str) -> VenueOrder:
        self._assert_testnet_binding()
        cloid_raw = self._cloid_for(client_order_id)
        lookup = self._lookup_order(client_order_id)
        if lookup is None:
            raise AmbiguousSubmissionError("hyperliquid_testnet_cancel_order_identity_unresolved")
        symbol = lookup[1]["coin"]
        self._remember(client_order_id, cloid_raw, symbol)
        try:
            with self._signer_lock:
                self.exchange.set_expires_after(self._now_ms() + self.config.action_expiry_ms)
                response = self.exchange.cancel_by_cloid(symbol, Cloid.from_str(cloid_raw))
        except Exception as exc:
            self._degrade_after_write_exception(exc)
            raise AmbiguousSubmissionError("hyperliquid_testnet_cancel_outcome_unknown") from None
        try:
            self._require_cancel_success(response)
            order = self.get_order(client_order_id)
        except Exception:
            self._degrade_transport()
            raise AmbiguousSubmissionError("hyperliquid_testnet_cancel_reconciliation_required") from None
        if order is None:
            raise AmbiguousSubmissionError("hyperliquid_testnet_cancel_order_not_yet_observable")
        return order

    def get_order(self, client_order_id: str) -> VenueOrder | None:
        self._assert_testnet_binding()
        lookup = self._lookup_order(client_order_id)
        if lookup is None:
            return None
        wrapper, order = lookup
        status_text = wrapper.get("status")
        status_map = {
            "open": VenueOrderStatus.ACKNOWLEDGED,
            "filled": VenueOrderStatus.FILLED,
            "canceled": VenueOrderStatus.CANCELLED,
            "cancelled": VenueOrderStatus.CANCELLED,
            "rejected": VenueOrderStatus.REJECTED,
        }
        if status_text not in status_map:
            raise HyperliquidVenueEvidenceRefused("hyperliquid_order_status_unknown")
        symbol = self._required_symbol(order.get("coin"))
        self._remember(client_order_id, self._cloid_for(client_order_id), symbol)
        requested = self._positive_float(order.get("origSz", order.get("sz")), "order_original_size")
        remaining = self._nonnegative_float(order.get("sz", 0.0), "order_remaining_size")
        if remaining > requested + 1e-12:
            raise HyperliquidVenueEvidenceRefused("order_remaining_size_exceeds_original")
        filled = requested - remaining
        status = status_map[status_text]
        if status is VenueOrderStatus.FILLED:
            filled = requested
        elif filled > 0 and status is VenueOrderStatus.ACKNOWLEDGED:
            status = VenueOrderStatus.PARTIALLY_FILLED
        venue_order_id = self._required_identifier(order.get("oid"), "venue_order_id")
        timestamp = self._timestamp(order.get("timestamp", wrapper.get("statusTimestamp")))
        return VenueOrder(
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            status=status,
            requested_quantity=requested,
            filled_quantity=filled,
            reason=f"hyperliquid_{status_text}",
            venue_timestamp=timestamp,
            raw_payload={
                "venue": "HYPERLIQUID_TESTNET", "cloid": self._cloid_for(client_order_id),
                "symbol": symbol, "status": status_text, "venue_order_id": venue_order_id,
            },
        )

    def list_fills(self, client_order_id: str) -> list[VenueFill]:
        self._assert_testnet_binding()
        lookup = self._lookup_order(client_order_id)
        if lookup is None:
            return []
        _, order = lookup
        venue_order_id = self._required_identifier(order.get("oid"), "venue_order_id")
        rows = self.info.user_fills(self.config.account_address)
        if not isinstance(rows, list):
            raise HyperliquidVenueEvidenceRefused("user_fills_response_malformed")
        fills: list[VenueFill] = []
        for row in rows:
            if not isinstance(row, dict):
                raise HyperliquidVenueEvidenceRefused("user_fill_malformed")
            if str(row.get("oid")) != venue_order_id:
                continue
            side_text = row.get("side")
            if side_text not in {"A", "B"}:
                raise HyperliquidVenueEvidenceRefused("user_fill_side_unknown")
            fill_id = self._required_identifier(row.get("tid"), "venue_fill_id")
            fee = self._finite_float(row.get("fee", 0.0), "venue_fill_fee")
            fills.append(VenueFill(
                venue_fill_id=f"hyperliquid-testnet:{fill_id}",
                client_order_id=client_order_id,
                quantity=self._positive_float(row.get("sz"), "venue_fill_size"),
                price=self._positive_float(row.get("px"), "venue_fill_price"),
                fee=fee,
                venue_timestamp=self._timestamp(row.get("time")),
                side="BUY" if side_text == "B" else "SELL",
                raw_payload={
                    "venue": "HYPERLIQUID_TESTNET", "venue_order_id": venue_order_id,
                    "trade_id": fill_id, "transaction_hash": self._bounded_text(row.get("hash"), 80),
                },
            ))
        return fills

    def list_open_orders(self) -> list[VenueOrder]:
        self._assert_testnet_binding()
        rows = self.info.open_orders(self.config.account_address)
        if not isinstance(rows, list):
            raise HyperliquidVenueEvidenceRefused("open_orders_response_malformed")
        result: list[VenueOrder] = []
        for row in rows:
            if not isinstance(row, dict):
                raise HyperliquidVenueEvidenceRefused("open_order_malformed")
            if "status" in row and row["status"] not in {"open", "resting"}:
                raise HyperliquidVenueEvidenceRefused("open_order_status_unknown")
            symbol = self._required_symbol(row.get("coin"))
            cloid_raw = row.get("cloid")
            known_client = self._known_clients.get(str(cloid_raw)) if cloid_raw else None
            foreign = known_client is None
            venue_order_id = self._required_identifier(row.get("oid"), "venue_order_id")
            client_order_id = known_client or stable_id(
                "hyperliquid_testnet_foreign_order", self.execution_account_id, venue_order_id,
            )
            remaining = self._positive_float(row.get("sz"), "open_order_remaining_size")
            original = self._positive_float(row.get("origSz", row.get("sz")), "open_order_original_size")
            if remaining > original + 1e-12:
                raise HyperliquidVenueEvidenceRefused("open_order_remaining_size_exceeds_original")
            result.append(VenueOrder(
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                status=VenueOrderStatus.PARTIALLY_FILLED if original > remaining else VenueOrderStatus.ACKNOWLEDGED,
                requested_quantity=original,
                filled_quantity=original - remaining,
                reason="hyperliquid_open_order",
                venue_timestamp=self._timestamp(row.get("timestamp")),
                raw_payload={
                    "venue": "HYPERLIQUID_TESTNET", "symbol": symbol,
                    "cloid": str(cloid_raw) if cloid_raw else None,
                    "venue_order_id": venue_order_id, "external_manual_activity": foreign,
                },
            ))
        return result

    def get_positions(self) -> list[VenuePosition]:
        self._assert_testnet_binding()
        state = self._user_state()
        rows = state.get("assetPositions")
        if not isinstance(rows, list):
            raise HyperliquidVenueEvidenceRefused("account_positions_response_malformed")
        observed_ms = self._now_ms()
        self._last_positions_observed_ms = observed_ms
        positions: list[VenuePosition] = []
        for row in rows:
            position = row.get("position") if isinstance(row, dict) else None
            if not isinstance(position, dict):
                raise HyperliquidVenueEvidenceRefused("account_position_malformed")
            symbol = self._required_symbol(position.get("coin"))
            signed = self._finite_float(position.get("szi"), "position_size")
            if abs(signed) <= 1e-12:
                continue
            positions.append(VenuePosition(
                symbol=symbol,
                signed_quantity=signed,
                observed_at=as_utc(observed_ms),
                raw_payload={
                    "venue": "HYPERLIQUID_TESTNET", "account_ref": self.config.account_identity_hash,
                    "entry_price": self._optional_finite(position.get("entryPx")),
                    "external_manual_activity": False,
                },
            ))
        return positions

    def positions_observation_is_fresh(self) -> bool:
        return (
            self._last_positions_observed_ms is not None
            and self._now_ms() - self._last_positions_observed_ms
            <= int(self.config.evidence_ttl_seconds * 1_000)
        )

    def get_balances(self) -> dict[str, Any]:
        self._assert_testnet_binding()
        state = self._user_state()
        summary = state.get("marginSummary")
        if not isinstance(summary, dict):
            raise HyperliquidVenueEvidenceRefused("account_balance_response_malformed")
        return {
            "venue": "HYPERLIQUID_TESTNET",
            "account_ref": self.config.account_identity_hash,
            "account_value": self._finite_float(summary.get("accountValue"), "account_value"),
            "total_margin_used": self._finite_float(summary.get("totalMarginUsed"), "total_margin_used"),
            "withdrawable": self._finite_float(state.get("withdrawable"), "withdrawable"),
            "observed_at": as_utc(self._now_ms()).isoformat(),
        }

    def get_instrument_metadata(self, symbol: str) -> dict[str, Any]:
        self._assert_testnet_binding()
        checked_symbol = self._required_symbol(symbol)
        response = self.info.meta_and_asset_ctxs()
        if not isinstance(response, list) or len(response) != 2:
            raise HyperliquidVenueEvidenceRefused("instrument_metadata_response_malformed")
        meta, contexts = response
        universe = meta.get("universe") if isinstance(meta, dict) else None
        if not isinstance(universe, list) or not isinstance(contexts, list) or len(universe) != len(contexts):
            raise HyperliquidVenueEvidenceRefused("instrument_metadata_alignment_invalid")
        for item, context in zip(universe, contexts, strict=True):
            if not isinstance(item, dict) or not isinstance(context, dict):
                raise HyperliquidVenueEvidenceRefused("instrument_metadata_item_malformed")
            if item.get("name") != checked_symbol:
                continue
            decimals = item.get("szDecimals")
            if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 18:
                raise HyperliquidVenueEvidenceRefused("instrument_size_precision_invalid")
            mid = self._positive_float(context.get("midPx"), "instrument_mid_price")
            mark = self._positive_float(context.get("markPx"), "instrument_mark_price")
            return {
                "venue": "HYPERLIQUID_TESTNET", "symbol": checked_symbol,
                "quantity_precision": decimals,
                "minimum_quantity": float(Decimal(1).scaleb(-decimals)),
                "venue_minimum_notional": self.config.minimum_order_notional_usd,
                "mid_price": mid,
                "mark_price": mark,
                "observed_at": as_utc(self._now_ms()).isoformat(),
                "source": "hyperliquid_metaAndAssetCtxs",
            }
        raise HyperliquidVenueEvidenceRefused("instrument_symbol_not_listed")

    def renew_dead_man_switch(self) -> int:
        self._assert_testnet_binding()
        now = self._now_ms()
        if (
            self._dead_man_schedule_ms is not None
            and self._dead_man_last_renewed_ms is not None
            and now - self._dead_man_last_renewed_ms < self.config.dead_man_renewal_seconds * 1_000
        ):
            return self._dead_man_schedule_ms
        schedule_ms = now + self.config.dead_man_horizon_seconds * 1_000
        try:
            with self._signer_lock:
                self.exchange.set_expires_after(now + self.config.action_expiry_ms)
                response = self.exchange.schedule_cancel(schedule_ms)
            self._require_write_ok(response, "dead_man_schedule_rejected")
        except Exception as exc:
            self._degrade_after_write_exception(exc)
            raise AmbiguousSubmissionError("hyperliquid_dead_man_schedule_outcome_unknown") from None
        self._dead_man_schedule_ms = schedule_ms
        self._dead_man_last_renewed_ms = now
        return schedule_ms

    def clear_dead_man_switch(self) -> None:
        self._assert_testnet_binding()
        try:
            with self._signer_lock:
                self.exchange.set_expires_after(self._now_ms() + self.config.action_expiry_ms)
                response = self.exchange.schedule_cancel(None)
            self._require_write_ok(response, "dead_man_clear_rejected")
        except Exception as exc:
            self._degrade_after_write_exception(exc)
            raise AmbiguousSubmissionError("hyperliquid_dead_man_clear_outcome_unknown") from None
        self._dead_man_schedule_ms = None
        self._dead_man_last_renewed_ms = None

    def _preflight_submission(self, request: SubmissionRequest) -> VenueOrder | None:
        if request.side not in {"BUY", "SELL"}:
            return self._local_rejection(request, "submission_side_invalid")
        if type(request.exposure_effect) is not ExposureEffect:
            return self._local_rejection(request, "submission_exposure_effect_invalid")
        if request.reduce_only != (request.exposure_effect in {ExposureEffect.REDUCE, ExposureEffect.FLATTEN}):
            return self._local_rejection(request, "submission_reduce_only_integrity_invalid")
        if request.exposure_effect is ExposureEffect.INCREASE:
            healthy, reason = self.entry_transport_health()
            if not healthy:
                return self._local_rejection(request, reason)
        return None

    def _validate_quantity(self, request: SubmissionRequest, metadata: dict[str, Any]) -> VenueOrder | None:
        try:
            quantity = Decimal(str(request.quantity))
            if not quantity.is_finite() or quantity <= 0:
                raise InvalidOperation
            quantum = Decimal(1).scaleb(-int(metadata["quantity_precision"]))
            if quantity.quantize(quantum, rounding=ROUND_DOWN) != quantity:
                return self._local_rejection(request, "submission_quantity_precision_invalid")
            if quantity < Decimal(str(metadata["minimum_quantity"])):
                return self._local_rejection(request, "submission_quantity_below_minimum")
            notional = quantity * Decimal(str(metadata["mark_price"]))
            if notional < Decimal(str(metadata["venue_minimum_notional"])):
                return self._local_rejection(request, "submission_notional_below_venue_minimum")
        except (InvalidOperation, ValueError, TypeError, KeyError):
            return self._local_rejection(request, "submission_quantity_invalid")
        return None

    def _validate_reduce_only(self, request: SubmissionRequest) -> VenueOrder | None:
        positions = {item.symbol: item.signed_quantity for item in self.get_positions()}
        signed = positions.get(request.symbol)
        if signed is None:
            return self._local_rejection(request, "reduce_only_verified_position_missing")
        direction_valid = signed > 0 if request.side == "SELL" else signed < 0
        if not direction_valid:
            return self._local_rejection(request, "reduce_only_direction_mismatch")
        if request.quantity > abs(signed) + 1e-12:
            return self._local_rejection(request, "reduce_only_size_exceeds_verified_position")
        return None

    def _normalize_submit_response(
        self, request: SubmissionRequest, cloid_raw: str, response: object,
    ) -> VenueOrder:
        if not isinstance(response, dict):
            raise HyperliquidVenueEvidenceRefused("submission_response_malformed")
        if response.get("status") != "ok":
            if response.get("status") == "err" or "error" in response:
                return self._local_rejection(request, "hyperliquid_definitive_rejection", cloid=cloid_raw)
            raise HyperliquidVenueEvidenceRefused("submission_response_status_unknown")
        body = response.get("response")
        data = body.get("data") if isinstance(body, dict) and body.get("type") == "order" else None
        statuses = data.get("statuses") if isinstance(data, dict) else None
        if not isinstance(statuses, list) or len(statuses) != 1 or not isinstance(statuses[0], dict):
            raise HyperliquidVenueEvidenceRefused("submission_statuses_malformed")
        item = statuses[0]
        if "error" in item:
            return self._local_rejection(request, "hyperliquid_definitive_rejection", cloid=cloid_raw)
        if isinstance(item.get("resting"), dict):
            evidence = item["resting"]
            status, filled = VenueOrderStatus.ACKNOWLEDGED, 0.0
        elif isinstance(item.get("filled"), dict):
            evidence = item["filled"]
            status, filled = VenueOrderStatus.FILLED, request.quantity
        else:
            raise HyperliquidVenueEvidenceRefused("submission_status_unknown")
        venue_order_id = self._required_identifier(evidence.get("oid"), "venue_order_id")
        return VenueOrder(
            client_order_id=request.client_order_id,
            venue_order_id=venue_order_id,
            status=status,
            requested_quantity=request.quantity,
            filled_quantity=filled,
            reason="hyperliquid_order_accepted",
            venue_timestamp=as_utc(self._now_ms()),
            raw_payload={
                "venue": "HYPERLIQUID_TESTNET", "cloid": cloid_raw,
                "venue_order_id": venue_order_id, "status": status.value,
            },
        )

    def _lookup_order(self, client_order_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        cloid_raw = self._cloid_for(client_order_id)
        response = self.info.query_order_by_cloid(
            self.config.account_address, Cloid.from_str(cloid_raw),
        )
        if not isinstance(response, dict):
            raise HyperliquidVenueEvidenceRefused("order_status_response_malformed")
        self._require_account_match(response)
        if response.get("status") == "unknownOid":
            return None
        if response.get("status") != "order" or not isinstance(response.get("order"), dict):
            raise HyperliquidVenueEvidenceRefused("order_status_response_unknown")
        wrapper = response["order"]
        order = wrapper.get("order")
        if not isinstance(order, dict):
            raise HyperliquidVenueEvidenceRefused("order_status_order_malformed")
        observed_cloid = order.get("cloid")
        if observed_cloid is not None and str(observed_cloid).lower() != cloid_raw:
            raise HyperliquidVenueEvidenceRefused("order_status_cloid_mismatch")
        return wrapper, order

    def _user_state(self) -> dict[str, Any]:
        state = self.info.user_state(self.config.account_address)
        if not isinstance(state, dict):
            raise HyperliquidVenueEvidenceRefused("account_state_response_malformed")
        self._require_account_match(state)
        return state

    def _require_account_match(self, payload: dict[str, Any]) -> None:
        observed = payload.get("user", payload.get("account"))
        if observed is not None and str(observed).lower() != self.config.normalized_account_address:
            raise HyperliquidVenueEvidenceRefused("hyperliquid_testnet_account_mismatch")

    def _require_cancel_success(self, response: object) -> None:
        self._require_write_ok(response, "cancel_rejected")
        assert isinstance(response, dict)
        body = response.get("response")
        data = body.get("data") if isinstance(body, dict) and body.get("type") == "cancel" else None
        statuses = data.get("statuses") if isinstance(data, dict) else None
        if not isinstance(statuses, list) or statuses != ["success"]:
            if isinstance(statuses, list) and statuses and isinstance(statuses[0], dict) and "error" in statuses[0]:
                raise HyperliquidVenueEvidenceRefused("cancel_definitively_rejected")
            raise HyperliquidVenueEvidenceRefused("cancel_response_ambiguous")

    @staticmethod
    def _require_write_ok(response: object, reason: str) -> None:
        if not isinstance(response, dict) or response.get("status") != "ok":
            raise HyperliquidVenueEvidenceRefused(reason)

    def _local_rejection(
        self, request: SubmissionRequest, reason: str, *, cloid: str | None = None,
    ) -> VenueOrder:
        requested = (
            float(request.quantity)
            if isinstance(request.quantity, (int, float))
            and not isinstance(request.quantity, bool)
            and math.isfinite(float(request.quantity))
            else 0.0
        )
        return VenueOrder(
            client_order_id=request.client_order_id,
            venue_order_id=None,
            status=VenueOrderStatus.REJECTED,
            requested_quantity=requested,
            filled_quantity=0.0,
            reason=reason,
            venue_timestamp=as_utc(self._now_ms()),
            raw_payload={
                "venue": "HYPERLIQUID_TESTNET", "definitive_pre_transmission": cloid is None,
                "cloid": cloid, "reason": reason,
            },
        )

    def _aggressive_limit_price(self, *, mid_price: float, is_buy: bool, size_decimals: int) -> float:
        slippage = self.config.maximum_slippage_bps / 10_000.0
        price = mid_price * ((1.0 + slippage) if is_buy else (1.0 - slippage))
        decimals = 6 - size_decimals
        result = round(float(f"{price:.5g}"), decimals)
        if not math.isfinite(result) or result <= 0:
            raise HyperliquidVenueEvidenceRefused("bounded_limit_price_invalid")
        return result

    def _cloid_for(self, client_order_id: str) -> str:
        return self._known_cloids.get(client_order_id) or derive_hyperliquid_cloid(
            execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
            client_order_id=client_order_id,
        )

    def _remember(self, client_order_id: str, cloid_raw: str, symbol: str) -> None:
        existing = self._known_cloids.get(client_order_id)
        if existing is not None and existing != cloid_raw:
            raise HyperliquidVenueEvidenceRefused("client_order_id_cloid_conflict")
        reverse = self._known_clients.get(cloid_raw)
        if reverse is not None and reverse != client_order_id:
            raise HyperliquidVenueEvidenceRefused("cloid_client_order_id_conflict")
        self._known_cloids[client_order_id] = cloid_raw
        self._known_clients[cloid_raw] = client_order_id
        self._symbols[client_order_id] = symbol

    def _degrade_after_write_exception(self, exc: Exception) -> None:
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            self._entry_degraded_until_ms = max(
                self._entry_degraded_until_ms,
                self._now_ms() + int(self.config.rate_limit_cooldown_seconds * 1_000),
            )
        else:
            self._degrade_transport()

    def _degrade_transport(self) -> None:
        self._entry_degraded_until_ms = max(
            self._entry_degraded_until_ms,
            self._now_ms() + int(self.config.rate_limit_cooldown_seconds * 1_000),
        )

    def _assert_testnet_binding(self) -> None:
        if (
            self.config.base_url != HYPERLIQUID_TESTNET_API_URL
            or getattr(self.exchange, "base_url", None) != HYPERLIQUID_TESTNET_API_URL
            or getattr(self.info, "base_url", None) != HYPERLIQUID_TESTNET_API_URL
        ):
            raise HyperliquidConfigurationRefused("hyperliquid_testnet_transport_binding_changed")

    def _now_ms(self) -> int:
        return int(as_utc(self.clock()).timestamp() * 1_000)

    @staticmethod
    def _required_symbol(value: object) -> str:
        if not isinstance(value, str) or _SYMBOL.fullmatch(value) is None:
            raise HyperliquidVenueEvidenceRefused("venue_symbol_invalid")
        return value

    @staticmethod
    def _required_identifier(value: object, field: str) -> str:
        if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid")
        return str(value)

    @staticmethod
    def _finite_float(value: object, field: str) -> float:
        if isinstance(value, bool):
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid")
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid") from exc
        if not math.isfinite(result):
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid")
        return result

    @classmethod
    def _positive_float(cls, value: object, field: str) -> float:
        result = cls._finite_float(value, field)
        if result <= 0:
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid")
        return result

    @classmethod
    def _nonnegative_float(cls, value: object, field: str) -> float:
        result = cls._finite_float(value, field)
        if result < 0:
            raise HyperliquidVenueEvidenceRefused(f"{field}_invalid")
        return result

    @classmethod
    def _optional_finite(cls, value: object) -> float | None:
        return None if value is None else cls._finite_float(value, "optional_numeric_evidence")

    @staticmethod
    def _timestamp(value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise HyperliquidVenueEvidenceRefused("venue_timestamp_invalid")
        try:
            return as_utc(value)
        except (TypeError, ValueError, OSError) as exc:
            raise HyperliquidVenueEvidenceRefused("venue_timestamp_invalid") from exc

    @staticmethod
    def _bounded_text(value: object, limit: int) -> str | None:
        if value is None:
            return None
        return str(value)[:limit]
