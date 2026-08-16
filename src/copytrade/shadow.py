"""Phase-D.4 read-only real-venue shadow observations.

This module intentionally does not implement ``ExecutionAdapter``.  It holds
only the public ``/info`` read capability needed to observe a supplied public
account and has no order, cancellation, signing, credential, or transport
write API.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlparse

from .config import ShadowObservationConfig, SourceConfig
from .hyperliquid import HyperliquidPublicAdapter
from .models import as_utc, iso, stable_id, utc_now
from .storage import CopyTradeDatabase


SHADOW_EXECUTION_DOMAIN = "SHADOW_REAL_VENUE"
_OPEN_STATUSES = {"OPEN", "ACKNOWLEDGED", "PARTIALLY_FILLED"}
_MAX_EVIDENCE_ITEMS = 100
_MAX_EVIDENCE_TEXT = 2_048
_MAX_EVIDENCE_NODES = 1_000
_HYPERLIQUID_INFO_HOSTS = {"api.hyperliquid.xyz", "api.hyperliquid-testnet.xyz"}


class ReadOnlyShadowVenueAdapter(Protocol):
    """Capability-separated D.4 boundary: one account observation, reads only."""

    adapter_name: str
    adapter_mode: str
    venue: str

    def observe_account(self, account_id: str, *, max_age_seconds: float, received_at: object | None = None) -> "ShadowObservation": ...


def normalize_shadow_account_id(account_id: str) -> str:
    normalized = str(account_id).strip().lower()
    if not (normalized.startswith("0x") and len(normalized) == 42
            and all(character in "0123456789abcdef" for character in normalized[2:])):
        raise ValueError("Shadow account_id must be a public 0x-prefixed 20-byte account address.")
    return normalized


def shadow_execution_account_id(venue: str, account_id: str) -> str:
    return f"SHADOW:{venue.lower()}:{normalize_shadow_account_id(account_id)}"


@dataclass(frozen=True)
class ShadowObservation:
    """One immutable, auditable result of independent real-venue reads."""

    observation_id: str
    execution_domain: str
    execution_account_id: str
    venue: str
    account_id: str
    state: str
    freshness: str
    observed_at: object | None
    received_at: object
    components: dict[str, dict[str, Any]]
    normalized: dict[str, Any]
    raw_evidence: dict[str, Any]
    reason: str = ""

    def as_storage_record(self, comparison: Mapping[str, Any], *, attempted_at: object | None = None) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id,
            "execution_domain": self.execution_domain,
            "execution_account_id": self.execution_account_id,
            "venue": self.venue,
            "account_id": self.account_id,
            "state": self.state,
            "freshness": self.freshness,
            "observed_at": iso(self.observed_at) if self.observed_at is not None else None,
            "attempted_at": iso(attempted_at if attempted_at is not None else self.received_at),
            "received_at": iso(self.received_at),
            "reason": self.reason,
            "components": self.components,
            "normalized": self.normalized,
            "raw_evidence": self.raw_evidence,
            "comparison": dict(comparison),
        }


class HyperliquidReadOnlyShadowAdapter:
    """Public Hyperliquid account observer with no execution capability.

    The wrapped client is ``HyperliquidPublicAdapter``: it issues only the
    unauthenticated ``/info`` requests already governed by the shared host
    limiter.  This class deliberately exposes no methods named submit, cancel,
    amend, transfer, withdraw, or sign.
    """

    adapter_name = "hyperliquid_read_only_shadow"
    adapter_mode = "READ_ONLY_SHADOW"
    venue = "hyperliquid"

    def __init__(
        self, source_config: SourceConfig, *, public_client: HyperliquidPublicAdapter | None = None,
        clock: Callable[[], object] = utc_now,
    ) -> None:
        _validate_hyperliquid_info_url(source_config.info_url)
        self._public_client = public_client or HyperliquidPublicAdapter(source_config)
        self._clock = clock

    def observe_account(self, account_id: str, *, max_age_seconds: float, received_at: object | None = None) -> ShadowObservation:
        account = normalize_shadow_account_id(account_id)
        raw: dict[str, Any] = {}
        components: dict[str, dict[str, Any]] = {}
        normalized: dict[str, Any] = {"positions": [], "open_orders": [], "balances": {}, "instruments": []}

        clearing = self._read("clearinghouse_state", {"type": "clearinghouseState", "user": account}, raw)
        orders = self._read("open_orders", {"type": "openOrders", "user": account}, raw)
        metadata = self._read("instrument_metadata", {"type": "meta"}, raw)
        # Production receipt time is captured after every public read. A
        # caller-supplied timestamp exists solely for deterministic tests.
        received = as_utc(received_at if received_at is not None else self._clock())
        if isinstance(clearing, dict):
            positions, position_error = _normalize_positions(clearing)
            balances, balance_error = _normalize_balances(clearing)
            timestamp, timestamp_error = _snapshot_timestamp(clearing)
            normalized["positions"] = positions
            normalized["balances"] = balances
            components["positions"] = _component(
                values=positions, observed_at=timestamp, received_at=received, max_age_seconds=max_age_seconds,
                parse_error=position_error or timestamp_error,
            )
            components["balances"] = _component(
                values=balances, observed_at=timestamp, received_at=received, max_age_seconds=max_age_seconds,
                parse_error=balance_error or timestamp_error,
            )
        else:
            components["positions"] = _failed_component("clearinghouse_state_unavailable")
            components["balances"] = _failed_component("clearinghouse_state_unavailable")

        order_payload, order_timestamp_source = _open_order_payload(orders)
        if order_payload is not None:
            open_orders, order_error = _normalize_open_orders(order_payload)
            timestamp, timestamp_error = _snapshot_timestamp(order_timestamp_source)
            normalized["open_orders"] = open_orders
            components["open_orders"] = _component(
                values=open_orders, observed_at=timestamp, received_at=received, max_age_seconds=max_age_seconds,
                parse_error=order_error or timestamp_error,
            )
        else:
            components["open_orders"] = _failed_component("open_order_observation_unavailable")

        if isinstance(metadata, dict):
            instruments, metadata_error = _normalize_instruments(metadata)
            timestamp, timestamp_error = _snapshot_timestamp(metadata)
            normalized["instruments"] = instruments
            components["instrument_metadata"] = _component(
                values=instruments, observed_at=timestamp, received_at=received, max_age_seconds=max_age_seconds,
                parse_error=metadata_error or timestamp_error,
                advisory=True,
            )
        else:
            components["instrument_metadata"] = _failed_component("instrument_metadata_unavailable", advisory=True)

        required = ("positions", "open_orders", "balances")
        state = "COMPLETE" if all(components[name]["state"] == "OBSERVED" for name in required) else "INCOMPLETE"
        freshness = _aggregate_freshness(components[name]["freshness"] for name in required)
        reason = "shadow_observation_complete" if state == "COMPLETE" else "shadow_observation_incomplete"
        observation_id = stable_id(
            "phase_d4_shadow_observation", self.venue, account, received, state, components, normalized,
        )
        return ShadowObservation(
            observation_id=observation_id, execution_domain=SHADOW_EXECUTION_DOMAIN,
            execution_account_id=shadow_execution_account_id(self.venue, account), venue=self.venue,
            account_id=account, state=state, freshness=freshness,
            observed_at=_aggregate_observed_at(components.values()), received_at=received,
            components=components, normalized=normalized, raw_evidence=_bounded_evidence(raw), reason=reason,
        )

    def _read(self, name: str, payload: dict[str, Any], raw: dict[str, Any]) -> Any | None:
        try:
            response = self._public_client.info(payload)
        except Exception as exc:
            # Exception text can include transport details.  Persist only its
            # class, never a potentially sensitive request diagnostic.
            raw[name] = {"error_class": type(exc).__name__}
            return None
        raw[name] = _bounded_evidence(response)
        return response


class ShadowObservationService:
    """Persist and compare a D.4 observation without changing execution safety."""

    def __init__(
        self, store: CopyTradeDatabase, adapter: ReadOnlyShadowVenueAdapter, config: ShadowObservationConfig,
        *, clock: Callable[[], object] = utc_now,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.config = config
        self._clock = clock

    def refresh(self, *, received_at: object | None = None, attempted_at: object | None = None) -> dict[str, Any]:
        if not self.config.enabled:
            return self.store.shadow_read_model(configured=False)
        attempted = as_utc(attempted_at if attempted_at is not None else self._clock())
        try:
            observation = self.adapter.observe_account(
                self.config.account_id, max_age_seconds=self.config.max_age_seconds, received_at=received_at,
            )
        except Exception as exc:
            # An observer implementation fault must not leave a prior healthy
            # result looking current. Persist one bounded, failed snapshot
            # instead; this never propagates exception text or credentials.
            received = as_utc(received_at if received_at is not None else self._clock())
            observation = _failed_observation(self.config, received, type(exc).__name__)
        if not _matches_configured_scope(observation, self.config):
            observation = _failed_observation(
                self.config,
                as_utc(received_at if received_at is not None else self._clock()),
                "ShadowObservationScopeMismatch",
                reason="shadow_observation_scope_mismatch",
            )
        comparison = compare_shadow_observation(self.store, observation)
        self.store.record_shadow_observation(observation.as_storage_record(comparison, attempted_at=attempted))
        return self.store.shadow_read_model(
            configured=True, venue=observation.venue, account_id=observation.account_id,
            execution_domain=observation.execution_domain, execution_account_id=observation.execution_account_id,
        )


def compare_shadow_observation(store: CopyTradeDatabase, observation: ShadowObservation) -> dict[str, Any]:
    """Compare against simulator ledger only as explicit, non-authoritative context."""
    reference_scope = {"execution_domain": "SIMULATOR", "execution_account_id": "SIMULATOR:default"}
    local_positions = store.phase_d_local_positions(**reference_scope)
    position_component = observation.components["positions"]
    metadata_component = observation.components["instrument_metadata"]
    supported_symbols = (
        {item["symbol"] for item in observation.normalized["instruments"]}
        if metadata_component["state"] == "OBSERVED" else None
    )
    if position_component["state"] != "OBSERVED":
        position_comparison = {
            "state": "INCOMPLETE", "reason": "shadow_positions_not_current",
            "items": [],
        }
    else:
        venue_positions = {item["symbol"]: float(item["signed_quantity"]) for item in observation.normalized["positions"]}
        items: list[dict[str, Any]] = []
        for symbol in sorted(set(local_positions) | set(venue_positions)):
            local = local_positions.get(symbol, 0.0)
            venue = venue_positions.get(symbol, 0.0)
            if symbol in venue_positions and supported_symbols is not None and symbol not in supported_symbols:
                state, reason = "UNSUPPORTED_SYMBOL", "venue_position_symbol_missing_from_fresh_instrument_metadata"
            elif symbol not in local_positions:
                state, reason = "VENUE_ONLY_POSITION", "venue_position_has_no_simulator_provenance"
            elif symbol not in venue_positions:
                state, reason = "LOCAL_ONLY_POSITION", "simulator_position_not_observed_at_shadow_venue"
            elif local == venue:
                state, reason = "MATCHED", "quantity_and_direction_match"
            elif local * venue < 0:
                state, reason = "DIRECTION_MISMATCH", "signed_position_direction_differs"
            else:
                state, reason = "QUANTITY_MISMATCH", "signed_position_quantity_differs"
            items.append({"symbol": symbol, "state": state, "reason": reason, "local_signed_quantity": local,
                          "venue_signed_quantity": venue})
        position_comparison = {
            "state": "MATCHED" if all(item["state"] == "MATCHED" for item in items) else "DISCREPANCY",
            "reason": "simulator_reference_non_authoritative", "items": items,
        }

    order_component = observation.components["open_orders"]
    external_orders = [
        {
            "order_id": item["order_id"], "symbol": item["symbol"], "side": item["side"], "quantity": item["quantity"],
            "state": "UNSUPPORTED_SYMBOL" if supported_symbols is not None and item["symbol"] not in supported_symbols
            else "EXTERNAL_UNATTRIBUTED_ORDER",
            "reason": "venue_order_symbol_missing_from_fresh_instrument_metadata"
            if supported_symbols is not None and item["symbol"] not in supported_symbols
            else "shadow_order_has_no_phase_d_real_execution_provenance",
        }
        for item in observation.normalized["open_orders"]
        if item["status"] in _OPEN_STATUSES
    ]
    order_comparison = (
        {"state": "INCOMPLETE", "reason": "shadow_open_orders_not_current", "items": []}
        if order_component["state"] != "OBSERVED" else
        {"state": "MATCHED" if not external_orders else "DISCREPANCY",
         "reason": "no_phase_d_real_execution_domain_exists_in_d4", "items": external_orders}
    )
    comparison_state = "INCOMPLETE" if "INCOMPLETE" in {position_comparison["state"], order_comparison["state"]} else (
        "MATCHED" if position_comparison["state"] == "MATCHED" and order_comparison["state"] == "MATCHED" else "DISCREPANCY"
    )
    return {
        "state": comparison_state,
        "reference": {**reference_scope, "authority": "NON_AUTHORITATIVE_SIMULATOR_CONTEXT"},
        "positions": position_comparison,
        "open_orders": order_comparison,
        "balances": observation.components["balances"],
        "instrument_metadata": observation.components["instrument_metadata"],
    }


def _normalize_positions(payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    rows = payload.get("assetPositions")
    if not isinstance(rows, list):
        return [], "position_payload_missing_asset_positions"
    positions: list[dict[str, Any]] = []
    symbols: set[str] = set()
    try:
        for row in rows:
            position = row.get("position") if isinstance(row, Mapping) else None
            if not isinstance(position, Mapping):
                raise ValueError("position_item_invalid")
            symbol = _symbol(position.get("coin"))
            quantity = _finite_number(position.get("szi"))
            if symbol in symbols:
                raise ValueError("position_symbol_duplicate")
            symbols.add(symbol)
            # Retain finite dust as exposure evidence. Exact zero (including
            # -0.0) is the only representation that means flat here.
            if quantity != 0.0:
                positions.append({"symbol": symbol, "signed_quantity": quantity})
    except ValueError as exc:
        return [], str(exc)
    return positions, None


def _normalize_balances(payload: Mapping[str, Any]) -> tuple[dict[str, float | None], str | None]:
    margin = payload.get("marginSummary")
    if not isinstance(margin, Mapping):
        return {}, "balance_payload_missing_margin_summary"
    try:
        account_value = _finite_number(margin.get("accountValue"))
        total_notional = _finite_number(margin.get("totalNtlPos"))
        withdrawable = _optional_finite_number(payload.get("withdrawable"))
    except ValueError as exc:
        return {}, str(exc)
    return {"account_value": account_value, "total_notional_position": total_notional, "withdrawable": withdrawable}, None


def _open_order_payload(payload: Any) -> tuple[list[Any] | None, Mapping[str, Any]]:
    if isinstance(payload, list):
        return payload, {}
    if isinstance(payload, Mapping) and isinstance(payload.get("orders"), list):
        return list(payload["orders"]), payload
    return None, {}


def _normalize_open_orders(rows: list[Any]) -> tuple[list[dict[str, Any]], str | None]:
    orders: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    try:
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("open_order_item_invalid")
            order_id = str(row.get("oid") if row.get("oid") is not None else row.get("orderId") or "").strip()
            if not order_id or order_id in identifiers:
                raise ValueError("open_order_identifier_missing_or_duplicate")
            identifiers.add(order_id)
            side = str(row.get("side") or "").upper()
            normalized_side = {"B": "BUY", "BUY": "BUY", "A": "SELL", "S": "SELL", "SELL": "SELL"}.get(side)
            if normalized_side is None:
                raise ValueError("open_order_side_invalid")
            status_value = row.get("status")
            if not isinstance(status_value, str) or not status_value.strip():
                raise ValueError("open_order_status_missing")
            status = status_value.upper()
            if status not in _OPEN_STATUSES:
                raise ValueError("open_order_status_unknown")
            orders.append({
                "order_id": order_id, "symbol": _symbol(row.get("coin")), "side": normalized_side,
                "quantity": abs(_finite_number(row.get("sz") if row.get("sz") is not None else row.get("origSz"))),
                "status": status,
            })
    except ValueError as exc:
        return [], str(exc)
    return orders, None


def _normalize_instruments(payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    universe = payload.get("universe")
    if not isinstance(universe, list):
        return [], "instrument_metadata_missing_universe"
    instruments: list[dict[str, Any]] = []
    symbols: set[str] = set()
    try:
        for item in universe:
            if not isinstance(item, Mapping):
                raise ValueError("instrument_metadata_item_invalid")
            symbol = _symbol(item.get("name") if item.get("name") is not None else item.get("coin"))
            if symbol in symbols:
                raise ValueError("instrument_symbol_duplicate")
            symbols.add(symbol)
            decimals = item.get("szDecimals")
            if not isinstance(decimals, int) or decimals < 0:
                raise ValueError("instrument_precision_invalid")
            leverage = _optional_finite_number(item.get("maxLeverage"))
            instruments.append({"symbol": symbol, "quantity_precision": decimals, "max_leverage": leverage})
    except ValueError as exc:
        return [], str(exc)
    return instruments, None


def _component(
    *, values: Any, observed_at: object | None, received_at: object, max_age_seconds: float,
    parse_error: str | None, advisory: bool = False,
) -> dict[str, Any]:
    freshness, freshness_reason = _freshness(observed_at, received_at, max_age_seconds)
    reason = parse_error or freshness_reason or "observed"
    state = "OBSERVED" if parse_error is None and freshness == "FRESH" else "INCOMPLETE"
    return {
        "state": state, "freshness": freshness, "reason": reason, "observed_at": iso(observed_at) if observed_at else None,
        # An empty normalized container proves absence only after successful
        # parsing. A malformed/unsupported payload must remain non-empty
        # failure evidence even if its safe normalized fallback is [].
        "empty": parse_error is None and values in ({}, []), "advisory": advisory,
    }


def _failed_component(reason: str, *, advisory: bool = False) -> dict[str, Any]:
    return {"state": "INCOMPLETE", "freshness": "UNKNOWN", "reason": reason, "observed_at": None,
            "empty": False, "advisory": advisory}


def _failed_observation(
    config: ShadowObservationConfig, received_at: object, error_class: str, *, reason: str = "shadow_adapter_observation_failed",
) -> ShadowObservation:
    """Turn an unexpected observer failure into current, append-only evidence."""
    venue = str(config.venue).lower()
    account = normalize_shadow_account_id(config.account_id)
    components = {
        "positions": _failed_component("shadow_adapter_observation_failed"),
        "balances": _failed_component("shadow_adapter_observation_failed"),
        "open_orders": _failed_component("shadow_adapter_observation_failed"),
        "instrument_metadata": _failed_component("shadow_adapter_observation_failed", advisory=True),
    }
    normalized = {"positions": [], "open_orders": [], "balances": {}, "instruments": []}
    return ShadowObservation(
        observation_id=stable_id("phase_d4_shadow_observation_failed", venue, account, received_at, error_class),
        execution_domain=SHADOW_EXECUTION_DOMAIN,
        execution_account_id=shadow_execution_account_id(venue, account),
        venue=venue,
        account_id=account,
        state="INCOMPLETE",
        freshness="UNKNOWN",
        observed_at=None,
        received_at=received_at,
        components=components,
        normalized=normalized,
        raw_evidence={"observer": {"error_class": error_class}},
        reason=reason,
    )


def _matches_configured_scope(observation: Any, config: ShadowObservationConfig) -> bool:
    """Keep a future/injected observer from redirecting evidence to another account."""
    if not isinstance(observation, ShadowObservation):
        return False
    venue = str(config.venue).lower()
    try:
        account = normalize_shadow_account_id(config.account_id)
    except ValueError:
        return False
    return (
        observation.execution_domain == SHADOW_EXECUTION_DOMAIN
        and observation.venue == venue
        and observation.account_id == account
        and observation.execution_account_id == shadow_execution_account_id(venue, account)
    )


def _validate_hyperliquid_info_url(value: object) -> None:
    """Permit the public Hyperliquid information endpoint, never an arbitrary URL."""
    parsed = urlparse(str(value))
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _HYPERLIQUID_INFO_HOSTS
        or parsed.port is not None
        or parsed.path.rstrip("/") != "/info"
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("D.4 shadow observation requires the public Hyperliquid HTTPS /info endpoint.")


def _snapshot_timestamp(payload: Mapping[str, Any]) -> tuple[object | None, str | None]:
    for key in ("time", "timestamp", "serverTime", "observedAt"):
        if key in payload:
            try:
                if isinstance(payload[key], bool):
                    raise ValueError("boolean timestamp")
                return as_utc(payload[key]), None
            except (TypeError, ValueError, OverflowError):
                return None, "venue_timestamp_invalid"
    return None, "venue_timestamp_missing"


def _freshness(observed_at: object | None, received_at: object, max_age_seconds: float) -> tuple[str, str | None]:
    if observed_at is None:
        return "UNKNOWN", "venue_timestamp_missing"
    age_seconds = (as_utc(received_at) - as_utc(observed_at)).total_seconds()
    if age_seconds < 0:
        return "UNKNOWN", "venue_timestamp_in_future"
    if age_seconds > max_age_seconds:
        return "STALE", "venue_timestamp_stale"
    return "FRESH", None


def _aggregate_freshness(values: Any) -> str:
    states = set(values)
    if "UNKNOWN" in states:
        return "UNKNOWN"
    if "STALE" in states:
        return "STALE"
    return "FRESH"


def _aggregate_observed_at(components: Any) -> object | None:
    values = [item.get("observed_at") for item in components if item.get("observed_at")]
    return max((as_utc(value) for value in values), default=None)


def _finite_number(value: Any) -> float:
    if isinstance(value, bool) or value in (None, ""):
        raise ValueError("numeric_value_missing")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("numeric_value_invalid") from exc
    if not math.isfinite(result):
        raise ValueError("numeric_value_non_finite")
    return result


def _optional_finite_number(value: Any) -> float | None:
    return None if value in (None, "") else _finite_number(value)


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 32 or not all(character.isalnum() or character in {"-", "_"} for character in symbol):
        raise ValueError("symbol_invalid")
    return symbol


def _bounded_evidence(value: Any) -> Any:
    return _bounded_evidence_value(value, depth=0, remaining=[_MAX_EVIDENCE_NODES])


def _bounded_evidence_value(value: Any, *, depth: int, remaining: list[int]) -> Any:
    if remaining[0] <= 0:
        return "<truncated_items>"
    remaining[0] -= 1
    if depth >= 8:
        return "<truncated_depth>"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_EVIDENCE_ITEMS or remaining[0] <= 0:
                result["<truncated_items>"] = True
                break
            result[str(key)[:128]] = _bounded_evidence_value(item, depth=depth + 1, remaining=remaining)
        return result
    if isinstance(value, (list, tuple)):
        result = []
        for index, item in enumerate(value):
            if index >= _MAX_EVIDENCE_ITEMS or remaining[0] <= 0:
                result.append("<truncated_items>")
                break
            result.append(_bounded_evidence_value(item, depth=depth + 1, remaining=remaining))
        return result
    if isinstance(value, str):
        return value[:_MAX_EVIDENCE_TEXT]
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else "<non_finite_float>"
    if isinstance(value, int):
        try:
            return value if len(str(value)) <= _MAX_EVIDENCE_TEXT else "<integer_too_large>"
        except ValueError:
            return "<integer_too_large>"
    return f"<unsupported_type:{type(value).__name__}>"
