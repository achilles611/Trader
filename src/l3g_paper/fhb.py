"""Focused Horizon Band admission for the experimental Pong paper profile.

FHB never filters transport or mutates the native positional book.  It consumes
the signed best-to-worst snapshots after source sequencing has advanced, keeps
minimal exact-price shadow state, and decides whether one depth record should
trigger the expensive microstructure pipeline.  Ordinary Pong operation does
not construct this object and therefore retains its original behavior.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from functools import lru_cache
import hashlib
import json
import time
from typing import Mapping

from .pong_microstructure import (
    BookSide,
    CausalEventRef,
    DepthEventKind,
    DepthObservation,
    PriceResponse,
)


FHB_PROFILE_ID = "TOP10_CORE_WITH_5S_SHADOW_V1"
FHB_SUPPORTED_SOURCE_LEVELS_PER_SIDE = 10
FHB_ENTRY_CADENCE_POLICY_ID = "FHB_ENTRY_CADENCE_250MS_V1"
FHB_ENTRY_CADENCE_INTERVAL_MS = 250
FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID = "FHB_OPTION3_ACTUAL_OFFERED_SERVICE_V1"
FHB_ACTUAL_OFFERED_SERVICE_POLICY = {
    "policy_id": FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID,
    "minimum_observed_event_rate": "NONE",
    "service_requirement": (
        "ACTUAL_OFFERED_TRAFFIC_ACCOUNTED_WITH_ZERO_NEW_LOSS_AND_"
        "NON_GROWING_PENDING_V1"
    ),
    "freshness_requirement": "EXISTING_FHB_1000MS_GATES",
    "integrity_requirement": "SEQUENCE_GENERATION_AND_ATTESTATION_REQUIRED",
    "durability_requirement": "PENDING_AWARE_FSYNC_ACCOUNTING_REQUIRED",
}
FHB_ACTUAL_OFFERED_SERVICE_POLICY_HASH = hashlib.sha256(
    json.dumps(
        FHB_ACTUAL_OFFERED_SERVICE_POLICY, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8"),
).hexdigest()
FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID = (
    "FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_V1"
)
FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY = {
    "policy_id": FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID,
    "destination": "Sim101/LOCAL_SIMULATION",
    "scheduling_owner": "PONG_PERIODIC_DEADLINE",
    "minimum_observed_event_rate": "NONE",
    "service_requirement": (
        "ACTUAL_OFFERED_TRAFFIC_ACCOUNTED_WITH_ZERO_NEW_LOSS_AND_"
        "NON_GROWING_PENDING_V1"
    ),
    "freshness_requirement": "EXISTING_FHB_1000MS_GATES",
    "integrity_requirement": "SEQUENCE_GENERATION_AND_ATTESTATION_REQUIRED",
    "durability_requirement": "PENDING_AWARE_FSYNC_ACCOUNTING_REQUIRED",
}
FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_HASH = hashlib.sha256(
    json.dumps(
        FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8"),
).hexdigest()


def _policy_fingerprint(payload: Mapping[str, object]) -> str:
    """Identify the exact effective FHB policy without implying authority."""
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8"),
    ).hexdigest()


@lru_cache(maxsize=8192)
def _mnq_price_ticks(value: str) -> int:
    """Convert one exact MNQ price string under the fixed signed tick contract."""
    price = Decimal(value)
    if not price.is_finite() or price <= 0:
        raise ValueError("FHB_PRICE_INVALID")
    ticks = price / Decimal("0.25")
    if ticks != ticks.to_integral_value():
        raise ValueError("FHB_PRICE_NOT_ON_TICK_GRID")
    return int(ticks)


class FHBAction(StrEnum):
    EVALUATE = "EVALUATE"
    SKIP_OUT_OF_CORE = "SKIP_OUT_OF_CORE"
    REWARM_REQUIRED = "REWARM_REQUIRED"


@dataclass(frozen=True)
class FHBConfig:
    profile_id: str = FHB_PROFILE_ID
    core_levels_per_side: int = 10
    shadow_history_seconds: int = 5
    movement_reserve_ticks: int = 32
    tick_size: Decimal = Decimal("0.25")
    maximum_shadow_prices_per_side: int = 512
    maximum_shadow_events: int = 65536
    maximum_queue_age_milliseconds: int = 1000
    maximum_event_age_milliseconds: int = 1000
    source_heartbeat_timeout_seconds: int = 5
    persistent_breach_seconds: int = 5
    entry_policy_id: str | None = None
    entry_evaluation_interval_ms: int = 0
    qualification_policy_id: str | None = None
    qualification_policy_hash: str | None = None

    def __post_init__(self) -> None:
        if self.profile_id != FHB_PROFILE_ID:
            raise ValueError("FHB_PROFILE_UNSUPPORTED")
        if self.core_levels_per_side != 10:
            raise ValueError("FHB_CORE_WIDTH_UNSUPPORTED")
        if self.core_levels_per_side > FHB_SUPPORTED_SOURCE_LEVELS_PER_SIDE:
            raise ValueError("FHB_CORE_WIDTH_EXCEEDS_SIGNED_SOURCE_COVERAGE")
        if self.shadow_history_seconds != 5:
            raise ValueError("FHB_SHADOW_WINDOW_UNSUPPORTED")
        if self.movement_reserve_ticks != 32:
            raise ValueError("FHB_MOVEMENT_RESERVE_UNSUPPORTED")
        if self.tick_size != Decimal("0.25"):
            raise ValueError("FHB_TICK_SIZE_UNSUPPORTED")
        integer_bounds = (
            self.maximum_shadow_prices_per_side,
            self.maximum_shadow_events,
            self.maximum_queue_age_milliseconds,
            self.maximum_event_age_milliseconds,
            self.source_heartbeat_timeout_seconds,
            self.persistent_breach_seconds,
        )
        if any(type(value) is not int or value <= 0 for value in integer_bounds):
            raise ValueError("FHB_BOUND_INVALID")
        if self.maximum_shadow_prices_per_side < 2 * self.core_levels_per_side:
            raise ValueError("FHB_PRICE_CAP_BELOW_CORE_BOUND")
        if (
            self.entry_policy_id,
            self.entry_evaluation_interval_ms,
        ) not in {
            (None, 0),
            (FHB_ENTRY_CADENCE_POLICY_ID, FHB_ENTRY_CADENCE_INTERVAL_MS),
        }:
            raise ValueError("FHB_ENTRY_EVALUATION_POLICY_UNSUPPORTED")
        expected_qualification = (
            (
                FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID,
                FHB_ACTUAL_OFFERED_SERVICE_POLICY_HASH,
            )
            if self.entry_evaluation_interval_ms
            else (
                FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID,
                FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_HASH,
            )
            if self.qualification_policy_id is not None
            else (None, None)
        )
        if (
            self.qualification_policy_id,
            self.qualification_policy_hash,
        ) != expected_qualification:
            raise ValueError("FHB_QUALIFICATION_POLICY_UNSUPPORTED")

    @classmethod
    def option_3(cls) -> "FHBConfig":
        return cls(
            entry_policy_id=FHB_ENTRY_CADENCE_POLICY_ID,
            entry_evaluation_interval_ms=FHB_ENTRY_CADENCE_INTERVAL_MS,
            qualification_policy_id=FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID,
            qualification_policy_hash=FHB_ACTUAL_OFFERED_SERVICE_POLICY_HASH,
        )

    @classmethod
    def periodic_protected(cls) -> "FHBConfig":
        """Bind FHB to periodic scoring without reinstating a fixed rate floor."""
        return cls(
            qualification_policy_id=(
                FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID
            ),
            qualification_policy_hash=(
                FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_HASH
            ),
        )

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "FHBConfig":
        if not isinstance(payload, Mapping):
            raise ValueError("FHB_CONFIGURATION_INVALID")
        candidate = cls(
            profile_id=str(payload.get("profile_id") or ""),
            core_levels_per_side=payload.get("core_levels_per_side"),
            shadow_history_seconds=payload.get("shadow_history_seconds"),
            movement_reserve_ticks=payload.get("movement_reserve_ticks"),
            tick_size=Decimal(str(payload.get("tick_size"))),
            maximum_shadow_prices_per_side=payload.get(
                "maximum_shadow_prices_per_side",
            ),
            maximum_shadow_events=payload.get("maximum_shadow_events"),
            maximum_queue_age_milliseconds=payload.get(
                "maximum_queue_age_milliseconds",
            ),
            maximum_event_age_milliseconds=payload.get(
                "maximum_event_age_milliseconds",
            ),
            source_heartbeat_timeout_seconds=payload.get(
                "source_heartbeat_timeout_seconds",
            ),
            persistent_breach_seconds=payload.get("persistent_breach_seconds"),
            entry_policy_id=payload.get("entry_policy_id"),
            entry_evaluation_interval_ms=payload.get(
                "entry_evaluation_interval_ms",
                0,
            ),
            qualification_policy_id=payload.get("qualification_policy_id"),
            qualification_policy_hash=payload.get("qualification_policy_hash"),
        )
        if dict(payload) != candidate.payload():
            raise ValueError("FHB_CONFIGURATION_INVALID")
        return candidate

    def payload(self) -> dict[str, object]:
        result = {
            "profile_id": self.profile_id,
            "core_levels_per_side": self.core_levels_per_side,
            "core_selector": "SIGNED_PRICE_CANONICAL_BEST_TO_WORST_PREFIX_V1",
            "shadow_history_seconds": self.shadow_history_seconds,
            "movement_reserve_ticks": self.movement_reserve_ticks,
            "movement_reserve_policy": "DIAGNOSTIC_ALL_SOURCE_COVERED_PRICES_RETAINED",
            "tick_size": str(self.tick_size),
            "maximum_shadow_prices_per_side": self.maximum_shadow_prices_per_side,
            "maximum_shadow_events": self.maximum_shadow_events,
            "maximum_queue_age_milliseconds": self.maximum_queue_age_milliseconds,
            "maximum_event_age_milliseconds": self.maximum_event_age_milliseconds,
            "source_heartbeat_timeout_seconds": self.source_heartbeat_timeout_seconds,
            "persistent_breach_seconds": self.persistent_breach_seconds,
            "execution_owner": "SERIAL_SINGLE_ORDERED_OWNER",
            "transport_reduction": "NONE",
        }
        # Preserve byte-for-byte compatibility for the established FHB policy.
        # The Option 3 keys exist only when that experiment is explicitly
        # selected, so old manifests remain valid and legacy behavior remains
        # every eligible event.
        if self.entry_evaluation_interval_ms:
            result.update({
                "entry_policy_id": self.entry_policy_id,
                "entry_evaluation_interval_ms": self.entry_evaluation_interval_ms,
            })
        if self.qualification_policy_id is not None:
            result.update({
                "qualification_policy_id": self.qualification_policy_id,
                "qualification_policy_hash": self.qualification_policy_hash,
            })
        return result

    def actual_offered_service_policy_payload(self) -> dict[str, object] | None:
        """Return the closed Option 3 service contract, never an arbitrary rate."""
        if self.qualification_policy_id == FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID:
            return {
                **FHB_ACTUAL_OFFERED_SERVICE_POLICY,
                "policy_hash": FHB_ACTUAL_OFFERED_SERVICE_POLICY_HASH,
            }
        if (
            self.qualification_policy_id
            == FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID
        ):
            return {
                **FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY,
                "policy_hash": FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_HASH,
            }
        return None

    @property
    def policy_fingerprint(self) -> str:
        """Hash the admitted effective policy; this is not a launch signature."""
        return _policy_fingerprint(self.payload())


@dataclass(frozen=True)
class FHBDecision:
    action: FHBAction
    reason: str
    side: BookSide | None
    price_ticks: int | None
    pre_core_member: bool
    post_core_member: bool
    promoted_price_ticks: tuple[int, ...]
    seed_observations: tuple[DepthObservation, ...]
    clear_feature_windows: bool = False


@dataclass
class _PriceShadow:
    coverage_started_at: datetime
    last_seen_at: datetime
    current_quantity: Decimal
    events: deque[DepthObservation]
    forwarded: dict[str, datetime]
    coverage_loss_reason: str | None = None


class FocusedHorizonBand:
    """Incremental signed-core selector and exact-price shadow owner."""

    def __init__(self, config: FHBConfig = FHBConfig()) -> None:
        self.config = config
        self._generation: int | None = None
        self._core: dict[BookSide, tuple[int, ...]] = {
            BookSide.BID: (),
            BookSide.ASK: (),
        }
        self._position_cursor: dict[
            BookSide, dict[int, tuple[int, Decimal]]
        ] = {BookSide.BID: {}, BookSide.ASK: {}}
        self._position_cursor_initialized = False
        self._shadow: dict[BookSide, dict[int, _PriceShadow]] = {
            BookSide.BID: {},
            BookSide.ASK: {},
        }
        self._shadow_event_count = 0
        self._source_depth_records = 0
        self._eligible_evaluations = 0
        self._out_of_core_skips = 0
        self._rewarm_required = 0
        self._reset_count = 0
        self._capacity_losses = 0
        self._movement_reserve_observations = 0
        self._shadow_maintenance_ns = 0
        self._maintenance_ticks = 0
        self._last_source_sequence: int | None = None
        self._last_source_event_id: str | None = None
        self._last_decision: FHBDecision | None = None
        self._current_rewarm_prices: tuple[str, ...] = ()
        self._last_invalidation_reason: str | None = None
        self._unavailable_reason: str | None = "FHB_NOT_INITIALIZED"
        self._rewarm_started_at: datetime | None = None
        self._last_rewarm_seconds: float | None = None
        self._maximum_rewarm_seconds = 0.0
        self._completed_rewarms = 0
        self._coherence_pending = False
        self._noncoherent_records = 0
        self._crossed_unavailable_records = 0
        self._partial_signed_records = 0
        self._coherence_reconciliations = 0
        self._coherence_reconciliation_failures = 0

    @staticmethod
    def _at(value: str) -> datetime:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("FHB_TIME_OFFSET_REQUIRED")
        return parsed

    def _ticks(self, value: object) -> int:
        try:
            return _mnq_price_ticks(str(value))
        except (InvalidOperation, TypeError, ValueError) as error:
            reason = str(error)
            if reason == "FHB_PRICE_NOT_ON_TICK_GRID":
                raise ValueError(reason) from error
            raise ValueError("FHB_PRICE_INVALID") from error

    def _price(self, ticks: int) -> Decimal:
        return Decimal(ticks) * self.config.tick_size

    def invalidate(self, reason: str, *, at: datetime | None = None) -> None:
        if not reason:
            raise ValueError("FHB_INVALIDATION_REASON_REQUIRED")
        self._core = {BookSide.BID: (), BookSide.ASK: ()}
        self._position_cursor = {BookSide.BID: {}, BookSide.ASK: {}}
        self._position_cursor_initialized = False
        self._shadow = {BookSide.BID: {}, BookSide.ASK: {}}
        self._shadow_event_count = 0
        self._current_rewarm_prices = ()
        self._rewarm_started_at = at
        self._last_invalidation_reason = reason
        self._unavailable_reason = reason
        self._coherence_pending = False
        self._reset_count += 1

    def _unavailable_decision(
        self,
        reason: str,
        *,
        at: datetime,
        side: BookSide | None,
        price_ticks: int | None,
        clear: bool,
    ) -> FHBDecision:
        self.invalidate(reason, at=at)
        self._rewarm_required += 1
        decision = FHBDecision(
            FHBAction.REWARM_REQUIRED,
            reason,
            side,
            price_ticks,
            False,
            False,
            (),
            (),
            clear,
        )
        self._last_decision = decision
        return decision

    def _reset_price_coverage(
        self,
        side: BookSide,
        ticks: int,
        quantity: Decimal,
        at: datetime,
    ) -> None:
        prior = self._shadow[side].pop(ticks, None)
        if prior is not None:
            self._shadow_event_count -= len(prior.events)
        self._ensure_price(side, ticks, quantity, at, set(self._core[side]))

    def _record_projected_event(
        self,
        payload: Mapping[str, object],
        event: CausalEventRef,
        side: BookSide,
        price_ticks: int | None,
        operation: str,
        previous: tuple[int, ...],
        current: tuple[int, ...],
        at: datetime,
    ) -> None:
        if price_ticks is None:
            return
        quantity = (
            Decimal("0")
            if operation == "REMOVE"
            else Decimal(str(payload.get("mutation_volume") or 0))
        )
        if not quantity.is_finite() or quantity < 0:
            raise ValueError("FHB_MUTATION_QUANTITY_INVALID")
        context = previous if operation == "REMOVE" and price_ticks in previous else current
        if not context:
            self._reset_price_coverage(side, price_ticks, quantity, at)
            return
        quantities = self._shadow[side]
        known_quantities: dict[int, Decimal] = {
            ticks: value.current_quantity for ticks, value in quantities.items()
        }
        if operation != "REMOVE":
            known_quantities[price_ticks] = quantity
        if any(ticks not in known_quantities for ticks in context):
            self._reset_price_coverage(side, price_ticks, quantity, at)
            return
        touch_ticks = context[0]
        price = self._price(price_ticks)
        touch = self._price(touch_ticks)
        if (
            (side is BookSide.BID and price > touch)
            or (side is BookSide.ASK and price < touch)
        ):
            self._reset_price_coverage(side, price_ticks, quantity, at)
            return
        observation = DepthObservation(
            event,
            str(payload.get("contract_id") or ""),
            side,
            price,
            quantity,
            sum((known_quantities[ticks] for ticks in context), Decimal("0")),
            touch,
            DepthEventKind.UPDATE,
            Decimal("0"),
            Decimal("0"),
            False,
            PriceResponse.UNAVAILABLE,
        )
        self._append_event(side, price_ticks, observation, at, set(current))

    def _noncoherent_decision(
        self,
        payload: Mapping[str, object],
        event: CausalEventRef,
        *,
        at: datetime,
        side: BookSide | None,
        price_ticks: int | None,
        reason: str,
        signed_prefixes: dict[BookSide, tuple[tuple[int, Decimal], ...]],
        crossed_unavailable: bool = False,
    ) -> FHBDecision:
        try:
            previous = {book_side: self._core[book_side] for book_side in self._core}
            projected = {
                book_side: tuple(ticks for ticks, _ in signed_prefixes[book_side])
                for book_side in (BookSide.BID, BookSide.ASK)
            }
            operation = str(
                payload.get("operation")
                or payload.get("mutation_operation")
                or ""
            ).upper()
            position = payload.get("mutation_position")
            if side is None or operation not in {"ADD", "UPDATE", "REMOVE"}:
                raise ValueError("FHB_POSITIONAL_MUTATION_IDENTITY_INVALID")
            if type(position) is not int or position < 0:
                raise ValueError("FHB_POSITIONAL_POSITION_INVALID")
            resolved_price = price_ticks
            if operation == "REMOVE" and resolved_price is None:
                removed = tuple(
                    ticks for ticks in previous[side]
                    if ticks not in set(projected[side])
                )
                if len(removed) == 1:
                    resolved_price = removed[0]
                else:
                    raise ValueError("FHB_POSITIONAL_REMOVE_IDENTITY_UNRESOLVED")
            elif operation != "REMOVE" and resolved_price is None:
                raise ValueError("FHB_POSITIONAL_UPSERT_PRICE_UNRESOLVED")
            for book_side in (BookSide.BID, BookSide.ASK):
                current_set = set(projected[book_side])
                for ticks, quantity in signed_prefixes[book_side]:
                    self._ensure_price(book_side, ticks, quantity, at, current_set)
            self._core = projected
            if side is None:
                raise ValueError("FHB_POSITIONAL_SIDE_UNRESOLVED")
            self._record_projected_event(
                payload,
                event,
                side,
                resolved_price,
                operation,
                previous[side],
                projected[side],
                at,
            )
        except (InvalidOperation, RuntimeError, TypeError, ValueError) as error:
            self._coherence_reconciliation_failures += 1
            return self._unavailable_decision(
                f"FHB_NONCOHERENT_POSITIONAL_AMBIGUITY:{error}",
                at=at,
                side=side,
                price_ticks=price_ticks,
                clear=True,
            )
        self._coherence_pending = True
        self._noncoherent_records += 1
        if crossed_unavailable:
            self._crossed_unavailable_records += 1
        else:
            self._partial_signed_records += 1
        self._rewarm_required += 1
        if self._rewarm_started_at is None:
            self._rewarm_started_at = at
        self._unavailable_reason = reason
        decision = FHBDecision(
            FHBAction.REWARM_REQUIRED,
            reason,
            side,
            resolved_price,
            False,
            False,
            (),
            (),
            True,
        )
        self._last_decision = decision
        return decision

    def _snapshot(
        self, payload: Mapping[str, object], side: BookSide,
    ) -> tuple[tuple[int, Decimal], ...]:
        raw = payload.get("bids" if side is BookSide.BID else "asks")
        if not isinstance(raw, list):
            raise ValueError("FHB_SIGNED_BOOK_SIDE_MISSING")
        rows: list[tuple[int, Decimal]] = []
        for item in raw:
            if not isinstance(item, Mapping):
                raise ValueError("FHB_SIGNED_BOOK_LEVEL_INVALID")
            ticks = self._ticks(item.get("price"))
            quantity = Decimal(str(item.get("size")))
            if not quantity.is_finite() or quantity <= 0:
                raise ValueError("FHB_SIGNED_BOOK_QUANTITY_INVALID")
            rows.append((ticks, quantity))
        prices = tuple(item[0] for item in rows)
        if side is BookSide.BID and prices != tuple(sorted(prices, reverse=True)):
            raise ValueError("FHB_SIGNED_BID_ORDER_INVALID")
        if side is BookSide.ASK and prices != tuple(sorted(prices)):
            raise ValueError("FHB_SIGNED_ASK_ORDER_INVALID")
        if len(prices) != len(set(prices)):
            raise ValueError("FHB_SIGNED_BOOK_DUPLICATE_PRICE")
        if len(rows) > FHB_SUPPORTED_SOURCE_LEVELS_PER_SIDE:
            raise ValueError("FHB_SIGNED_SOURCE_LEVEL_BOUND_EXCEEDED")
        return tuple(rows)

    def _position_snapshot(
        self,
        payload: Mapping[str, object],
    ) -> dict[BookSide, dict[int, tuple[int, Decimal]]] | None:
        keys = {
            BookSide.BID: "position_bids",
            BookSide.ASK: "position_asks",
        }
        present = tuple(key in payload for key in keys.values())
        if present == (False, False):
            return None
        if present != (True, True):
            raise ValueError("FHB_POSITION_CURSOR_SIDE_MISSING")
        result: dict[BookSide, dict[int, tuple[int, Decimal]]] = {
            BookSide.BID: {}, BookSide.ASK: {},
        }
        for side, key in keys.items():
            raw = payload.get(key)
            if not isinstance(raw, list):
                raise ValueError("FHB_POSITION_CURSOR_INVALID")
            for item in raw:
                if not isinstance(item, Mapping):
                    raise ValueError("FHB_POSITION_CURSOR_ROW_INVALID")
                position = item.get("position")
                if type(position) is not int or position < 0:
                    raise ValueError("FHB_POSITION_CURSOR_POSITION_INVALID")
                if position in result[side]:
                    raise ValueError("FHB_POSITION_CURSOR_DUPLICATE_POSITION")
                ticks = self._ticks(item.get("price"))
                quantity = Decimal(str(item.get("size")))
                if not quantity.is_finite() or quantity <= 0:
                    raise ValueError("FHB_POSITION_CURSOR_QUANTITY_INVALID")
                result[side][position] = (ticks, quantity)
        return result

    def _position_prefix(
        self,
        cursor: dict[int, tuple[int, Decimal]],
        side: BookSide,
    ) -> tuple[tuple[int, Decimal], ...]:
        rows: list[tuple[int, Decimal]] = []
        for position in range(self.config.core_levels_per_side):
            row = cursor.get(position)
            if row is None:
                break
            if rows and (
                (side is BookSide.BID and rows[-1][0] <= row[0])
                or (side is BookSide.ASK and rows[-1][0] >= row[0])
            ):
                break
            rows.append(row)
        return tuple(rows)

    @staticmethod
    def _shift_position_cursor(
        cursor: dict[int, tuple[int, Decimal]],
        position: int,
        inserting: bool,
    ) -> None:
        positions = sorted(
            (
                current for current in cursor
                if (current >= position if inserting else current > position)
            ),
            reverse=inserting,
        )
        for current in positions:
            cursor[current + 1 if inserting else current - 1] = cursor.pop(current)

    def _validate_position_transition(
        self,
        payload: Mapping[str, object],
        side: BookSide | None,
        supplied_price_ticks: int | None,
        snapshots: dict[BookSide, tuple[tuple[int, Decimal], ...]],
    ) -> int | None:
        transmitted = self._position_snapshot(payload)
        operation = str(
            payload.get("operation")
            or payload.get("mutation_operation")
            or ""
        ).upper()
        position = payload.get("mutation_position")
        reset = (
            payload.get("is_reset") is True
            or payload.get("mutation_is_reset") is True
        )
        if side is None or operation not in {"ADD", "UPDATE", "REMOVE"}:
            raise ValueError("FHB_POSITION_CURSOR_MUTATION_INVALID")
        if type(position) is not int or position < 0:
            raise ValueError("FHB_POSITION_CURSOR_MUTATION_POSITION_INVALID")
        resolved_price = supplied_price_ticks
        if self._position_cursor_initialized:
            projected = {
                book_side: dict(self._position_cursor[book_side])
                for book_side in (BookSide.BID, BookSide.ASK)
            }
            if reset:
                projected = {BookSide.BID: {}, BookSide.ASK: {}}
            target = projected[side]
            if operation == "REMOVE":
                removed = target.get(position)
                if removed is not None:
                    if resolved_price is not None and resolved_price != removed[0]:
                        raise ValueError("FHB_POSITION_CURSOR_REMOVE_IDENTITY_MISMATCH")
                    resolved_price = removed[0]
                target.pop(position, None)
                self._shift_position_cursor(target, position, False)
            else:
                if supplied_price_ticks is None:
                    raise ValueError("FHB_POSITION_CURSOR_UPSERT_PRICE_UNRESOLVED")
                quantity = Decimal(str(payload.get("mutation_volume") or 0))
                if not quantity.is_finite() or quantity <= 0:
                    raise ValueError("FHB_POSITION_CURSOR_UPSERT_QUANTITY_INVALID")
                if operation == "ADD":
                    self._shift_position_cursor(target, position, True)
                target[position] = (supplied_price_ticks, quantity)
            if transmitted is not None and projected != transmitted:
                raise ValueError("FHB_POSITION_CURSOR_TRANSITION_MISMATCH")
            current = projected
        elif transmitted is not None:
            # A signed full disclosure is the authoritative anchor for a
            # collector that attached after the native cursor was initialized.
            current = transmitted
        else:
            return supplied_price_ticks
        derived = {
            book_side: self._position_prefix(current[book_side], book_side)
            for book_side in (BookSide.BID, BookSide.ASK)
        }
        if any(
            derived[book_side] != snapshots[book_side]
            for book_side in (BookSide.BID, BookSide.ASK)
        ):
            raise ValueError("FHB_POSITION_CURSOR_PREFIX_MISMATCH")
        self._position_cursor = current
        self._position_cursor_initialized = True
        return resolved_price

    def _ensure_price(
        self,
        side: BookSide,
        ticks: int,
        quantity: Decimal,
        at: datetime,
        current_core: set[int],
    ) -> _PriceShadow:
        values = self._shadow[side]
        existing = values.get(ticks)
        if existing is not None:
            existing.last_seen_at = at
            existing.current_quantity = quantity
            return existing
        if len(values) >= self.config.maximum_shadow_prices_per_side:
            candidates = [
                (value.last_seen_at, price)
                for price, value in values.items()
                if price not in current_core
            ]
            if not candidates:
                raise RuntimeError("FHB_SHADOW_PRICE_CAPACITY_EXHAUSTED")
            _, evicted = min(candidates)
            self._shadow_event_count -= len(values[evicted].events)
            del values[evicted]
            self._capacity_losses += 1
        value = _PriceShadow(at, at, quantity, deque(), {})
        values[ticks] = value
        return value

    def _prune(self, value: _PriceShadow, cutoff: datetime) -> None:
        while value.events and value.events[0].event._ordering_key[0] < cutoff:
            value.events.popleft()
            self._shadow_event_count -= 1
        value.forwarded = {
            event_id: observed
            for event_id, observed in value.forwarded.items()
            if observed >= cutoff
        }
    def _append_event(
        self,
        side: BookSide,
        ticks: int,
        observation: DepthObservation,
        at: datetime,
        current_core: set[int],
    ) -> _PriceShadow:
        value = self._ensure_price(
            side, ticks, observation.displayed_quantity, at, current_core,
        )
        cutoff = at - timedelta(seconds=self.config.shadow_history_seconds)
        self._prune(value, cutoff)
        if self._shadow_event_count >= self.config.maximum_shadow_events:
            value.coverage_started_at = at
            value.coverage_loss_reason = "FHB_SHADOW_EVENT_CAPACITY_EXHAUSTED"
            self._capacity_losses += 1
            raise RuntimeError(value.coverage_loss_reason)
        value.events.append(observation)
        self._shadow_event_count += 1
        return value

    def _warm(self, side: BookSide, ticks: int, cutoff: datetime) -> bool:
        value = self._shadow[side].get(ticks)
        return bool(
            value is not None
            and value.coverage_loss_reason is None
            and value.coverage_started_at <= cutoff
        )

    def _rewarm_prices(self, cutoff: datetime) -> tuple[str, ...]:
        missing: list[str] = []
        for side in (BookSide.BID, BookSide.ASK):
            for ticks in self._core[side]:
                if not self._warm(side, ticks, cutoff):
                    missing.append(f"{side.value}:{ticks}")
        return tuple(missing)

    def _seed(
        self, side: BookSide, ticks: int, cutoff: datetime,
    ) -> tuple[DepthObservation, ...]:
        value = self._shadow[side][ticks]
        self._prune(value, cutoff)
        result = tuple(
            item for item in value.events
            if item.event._ordering_key[0] >= cutoff
            and item.event.event_id not in value.forwarded
        )
        for item in result:
            value.forwarded[item.event.event_id] = item.event._ordering_key[0]
        return result

    def process_depth(
        self,
        payload: Mapping[str, object],
        event: CausalEventRef,
    ) -> FHBDecision:
        started = time.perf_counter_ns()
        self._source_depth_records += 1
        self._maintenance_ticks += 1
        sequence = payload.get("source_event_sequence")
        if type(sequence) is not int or sequence <= 0:
            raise ValueError("FHB_SOURCE_SEQUENCE_INVALID")
        prior_sequence = self._last_source_sequence
        self._last_source_event_id = event.event_id
        at = event._ordering_key[0]
        clear = False
        try:
            generation = payload.get("source_generation")
            if generation is not None and (type(generation) is not int or generation <= 0):
                raise ValueError("FHB_SOURCE_GENERATION_INVALID")
            if (
                generation is not None
                and self._generation is not None
                and generation != self._generation
            ):
                self.invalidate("FHB_SOURCE_GENERATION_CHANGED", at=at)
                clear = True
                prior_sequence = None
            if generation is not None:
                self._generation = generation
            if prior_sequence is not None and sequence != prior_sequence + 1:
                self._last_source_sequence = sequence
                return self._unavailable_decision(
                    "FHB_SOURCE_SEQUENCE_DISCONTINUITY",
                    at=at,
                    side=None,
                    price_ticks=None,
                    clear=True,
                )
            self._last_source_sequence = sequence
            side_text = str(
                payload.get("side") or payload.get("mutation_side") or ""
            ).upper()
            side = (
                BookSide.BID if side_text == "BID"
                else BookSide.ASK if side_text == "ASK"
                else None
            )
            price_ticks: int | None = None
            for price_field in ("mutation_resolved_price", "mutation_price"):
                try:
                    price_ticks = self._ticks(payload.get(price_field))
                    break
                except ValueError:
                    pass
            unavailable = str(payload.get("status") or "").upper() == "UNAVAILABLE"
            reset = (
                payload.get("is_reset") is True
                or payload.get("mutation_is_reset") is True
            )
            try:
                snapshots = {
                    BookSide.BID: self._snapshot(payload, BookSide.BID),
                    BookSide.ASK: self._snapshot(payload, BookSide.ASK),
                }
                has_position_contract = (
                    "position_bids" in payload and "position_asks" in payload
                )
                price_ticks = self._validate_position_transition(
                    payload, side, price_ticks, snapshots,
                )
            except (InvalidOperation, TypeError, ValueError) as error:
                self._coherence_reconciliation_failures += 1
                return self._unavailable_decision(
                    f"FHB_POSITION_CURSOR_INVALID:{error}",
                    at=at,
                    side=side,
                    price_ticks=price_ticks,
                    clear=True,
                )
            if unavailable:
                source_reason = str(payload.get("reason") or "SOURCE_UNAVAILABLE")
                if reset:
                    return self._unavailable_decision(
                        "FHB_SOURCE_RESET:" + source_reason,
                        at=at,
                        side=side,
                        price_ticks=price_ticks,
                        clear=True,
                    )
                if has_position_contract and source_reason in {
                    "CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
                    "NATIVE_DEPTH_POSITION_ZERO_UNAVAILABLE",
                }:
                    return self._noncoherent_decision(
                        payload,
                        event,
                        at=at,
                        side=side,
                        price_ticks=price_ticks,
                        reason="FHB_SOURCE_UNAVAILABLE:" + source_reason,
                        signed_prefixes=snapshots,
                        crossed_unavailable=(
                            source_reason == "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"
                        ),
                    )
                return self._unavailable_decision(
                    "FHB_SOURCE_UNAVAILABLE:" + source_reason,
                    at=at,
                    side=side,
                    price_ticks=price_ticks,
                    clear=True,
                )
            if any(
                len(snapshots[book_side]) != self.config.core_levels_per_side
                for book_side in (BookSide.BID, BookSide.ASK)
            ):
                if reset:
                    return self._unavailable_decision(
                        "FHB_SOURCE_RESET_INCOMPLETE",
                        at=at,
                        side=side,
                        price_ticks=price_ticks,
                        clear=True,
                    )
                if not has_position_contract:
                    return self._unavailable_decision(
                        "FHB_POSITION_CURSOR_REQUIRED_FOR_INCOMPLETE_CORE",
                        at=at,
                        side=side,
                        price_ticks=price_ticks,
                        clear=True,
                    )
                return self._noncoherent_decision(
                    payload,
                    event,
                    at=at,
                    side=side,
                    price_ticks=price_ticks,
                    reason="FHB_SIGNED_CORE_INCOMPLETE",
                    signed_prefixes=snapshots,
                    crossed_unavailable=False,
                )
            if (
                snapshots[BookSide.BID][0][0]
                >= snapshots[BookSide.ASK][0][0]
            ):
                return self._unavailable_decision(
                    "FHB_SIGNED_BOOK_CROSSED",
                    at=at,
                    side=side,
                    price_ticks=price_ticks,
                    clear=True,
                )
            if reset:
                retained_cursor = self._position_cursor
                retained_initialized = self._position_cursor_initialized
                self.invalidate("FHB_SOURCE_RESET", at=at)
                self._position_cursor = retained_cursor
                self._position_cursor_initialized = retained_initialized
                clear = True
            previous = {side: self._core[side] for side in self._core}
            previous_quantities = {
                side: {
                    ticks: value.current_quantity
                    for ticks, value in self._shadow[side].items()
                }
                for side in (BookSide.BID, BookSide.ASK)
            }
            operation = str(
                payload.get("operation")
                or payload.get("mutation_operation")
                or "Update"
            )
            mutation_position = payload.get("mutation_position")
            current: dict[BookSide, tuple[int, ...]] = {
                book_side: tuple(
                    ticks for ticks, _ in snapshots[book_side][
                        : self.config.core_levels_per_side
                    ]
                )
                for book_side in (BookSide.BID, BookSide.ASK)
            }
            if self._coherence_pending:
                self._coherence_pending = False
                self._coherence_reconciliations += 1
                clear = True
            proven_out_of_core_remove = False
            if (
                price_ticks is None
                and operation.upper() == "REMOVE"
                and side is not None
                and type(mutation_position) is int
            ):
                if 0 <= mutation_position < len(previous[side]):
                    # NinjaTrader positional Remove callbacks can carry
                    # price=0. The coherent pre-mutation signed cursor and
                    # zero-based Position identify an authentic removed core
                    # price; never guess from the post-mutation snapshot.
                    price_ticks = previous[side][mutation_position]
                elif (
                    mutation_position >= self.config.core_levels_per_side
                    and all(
                        current[book_side] == previous[book_side]
                        for book_side in (BookSide.BID, BookSide.ASK)
                    )
                ):
                    # A deep positional removal has no signed price identity.
                    # It is safely out of core only when both signed prefixes
                    # prove that the core is unchanged across the mutation.
                    proven_out_of_core_remove = True
            if side is None or (
                price_ticks is None and not proven_out_of_core_remove
            ):
                return self._unavailable_decision(
                    "FHB_MUTATION_IDENTITY_UNRESOLVED",
                    at=at,
                    side=side,
                    price_ticks=price_ticks,
                    clear=True,
                )
            for book_side in (BookSide.BID, BookSide.ASK):
                current_set = set(current[book_side])
                for ticks, quantity in snapshots[book_side]:
                    self._ensure_price(
                        book_side, ticks, quantity, at, current_set,
                    )
            self._core = current

            promoted: list[int] = []
            for book_side in (BookSide.BID, BookSide.ASK):
                promoted.extend(
                    ticks for ticks in current[book_side]
                    if ticks not in set(previous[book_side])
                )

            pre_member = bool(side is not None and price_ticks in previous[side])
            post_member = bool(side is not None and price_ticks in current[side])
            if side is not None and price_ticks is not None and snapshots[side]:
                level_map = dict(snapshots[side])
                quantity = (
                    Decimal("0")
                    if operation.upper() == "REMOVE"
                    else Decimal(str(payload.get("mutation_volume") or level_map.get(price_ticks) or 0))
                )
                if not quantity.is_finite() or quantity < 0:
                    raise ValueError("FHB_MUTATION_QUANTITY_INVALID")
                touch_ticks = snapshots[side][0][0]
                touch = self._price(touch_ticks)
                price = self._price(price_ticks)
                nearby = sum((item[1] for item in snapshots[side]), Decimal("0"))
                if operation.upper() == "REMOVE" and pre_member and (
                    (side is BookSide.BID and price > touch)
                    or (side is BookSide.ASK and price < touch)
                ):
                    # The signed payload is post-mutation.  A removed best level
                    # must retain its pre-mutation touch/nearby identity instead
                    # of being discarded merely because the new touch moved.
                    prior_touch_ticks = previous[side][0]
                    touch_ticks = prior_touch_ticks
                    touch = self._price(prior_touch_ticks)
                    nearby = sum(
                        (previous_quantities[side].get(item, Decimal("0"))
                         for item in previous[side]),
                        Decimal("0"),
                    )
                if not (
                    (side is BookSide.BID and price > touch)
                    or (side is BookSide.ASK and price < touch)
                ):
                    observation = DepthObservation(
                        event,
                        str(payload.get("contract_id") or ""),
                        side,
                        price,
                        quantity,
                        nearby,
                        touch,
                        DepthEventKind.RECONNECT_RESET
                        if payload.get("is_reset") is True
                        else DepthEventKind.UPDATE,
                        Decimal("0"),
                        Decimal("0"),
                        False,
                        PriceResponse.UNAVAILABLE,
                    )
                    self._append_event(
                        side, price_ticks, observation, at, set(current[side]),
                    )
                    if abs(price_ticks - touch_ticks) > self.config.movement_reserve_ticks:
                        # Diagnostic only. Required signed-core prices remain
                        # admitted even when gaps make their distance large.
                        self._movement_reserve_observations += 1

            cutoff = at - timedelta(seconds=self.config.shadow_history_seconds)
            missing = self._rewarm_prices(cutoff)
            self._current_rewarm_prices = missing
            if not missing and self._rewarm_started_at is not None:
                duration = max(
                    0.0,
                    (at - self._rewarm_started_at).total_seconds(),
                )
                self._last_rewarm_seconds = duration
                self._maximum_rewarm_seconds = max(
                    self._maximum_rewarm_seconds,
                    duration,
                )
                self._completed_rewarms += 1
                self._rewarm_started_at = None
            if missing:
                if self._rewarm_started_at is None:
                    self._rewarm_started_at = at
                self._rewarm_required += 1
                decision = FHBDecision(
                    FHBAction.REWARM_REQUIRED,
                    "FHB_REWARM_REQUIRED",
                    side,
                    price_ticks,
                    pre_member,
                    post_member,
                    tuple(promoted),
                    (),
                    clear,
                )
            elif side is not None and price_ticks is not None and (pre_member or post_member):
                seed = self._seed(side, price_ticks, cutoff)
                if not seed:
                    self._rewarm_required += 1
                    decision = FHBDecision(
                        FHBAction.REWARM_REQUIRED,
                        "FHB_REWARM_REQUIRED:NO_CURRENT_EXACT_PRICE_EVENT",
                        side,
                        price_ticks,
                        pre_member,
                        post_member,
                        tuple(promoted),
                        (),
                        clear,
                    )
                else:
                    self._eligible_evaluations += 1
                    decision = FHBDecision(
                        FHBAction.EVALUATE,
                        "FHB_CORE_EVENT_ELIGIBLE",
                        side,
                        price_ticks,
                        pre_member,
                        post_member,
                        tuple(promoted),
                        seed,
                        clear,
                    )
            else:
                self._out_of_core_skips += 1
                decision = FHBDecision(
                    FHBAction.SKIP_OUT_OF_CORE,
                    "FHB_INTENTIONAL_OUT_OF_CORE_SKIP",
                    side,
                    price_ticks,
                    pre_member,
                    post_member,
                    tuple(promoted),
                    (),
                    clear,
                )
            self._last_decision = decision
            if self.entry_ready:
                self._unavailable_reason = None
            elif self._unavailable_reason in {None, "FHB_NOT_INITIALIZED"}:
                self._unavailable_reason = decision.reason
            return decision
        finally:
            self._shadow_maintenance_ns += time.perf_counter_ns() - started

    @property
    def entry_ready(self) -> bool:
        return (
            not self._coherence_pending
            and not self._current_rewarm_prices
            and all(
                len(self._core[side]) == self.config.core_levels_per_side
                for side in (BookSide.BID, BookSide.ASK)
            )
        )

    def status(self) -> dict[str, object]:
        last = self._last_decision
        return {
            "enabled": True,
            "effective_profile": self.config.profile_id,
            "effective_policy": self.config.payload(),
            "effective_policy_fingerprint": self.config.policy_fingerprint,
            "configuration": self.config.payload(),
            "source_generation": self._generation,
            "maintenance_ticks": self._maintenance_ticks,
            "last_source_sequence": self._last_source_sequence,
            "last_source_event_id": self._last_source_event_id,
            "signed_core_ticks": {
                side.value: list(self._core[side])
                for side in (BookSide.BID, BookSide.ASK)
            },
            "band": {
                "dynamic": True,
                "core_levels_per_side": self.config.core_levels_per_side,
                "bid_ticks": list(self._core[BookSide.BID]),
                "ask_ticks": list(self._core[BookSide.ASK]),
                "source_levels_per_side_maximum": (
                    FHB_SUPPORTED_SOURCE_LEVELS_PER_SIDE
                ),
            },
            "shadow_prices": {
                side.value: len(self._shadow[side])
                for side in (BookSide.BID, BookSide.ASK)
            },
            "shadow_events": self._shadow_event_count,
            "source_depth_records": self._source_depth_records,
            "eligible_evaluations": self._eligible_evaluations,
            "out_of_core_skips": self._out_of_core_skips,
            "rewarm_required_events": self._rewarm_required,
            "rewarm_prices": list(self._current_rewarm_prices),
            "rewarm_started_at": (
                None if self._rewarm_started_at is None
                else self._rewarm_started_at.isoformat().replace("+00:00", "Z")
            ),
            "last_rewarm_seconds": self._last_rewarm_seconds,
            "maximum_rewarm_seconds": self._maximum_rewarm_seconds,
            "completed_rewarms": self._completed_rewarms,
            "coherence_pending": self._coherence_pending,
            "noncoherent_records": self._noncoherent_records,
            "crossed_unavailable_records": self._crossed_unavailable_records,
            "partial_signed_records": self._partial_signed_records,
            "coherence_reconciliations": self._coherence_reconciliations,
            "coherence_reconciliation_failures": (
                self._coherence_reconciliation_failures
            ),
            "position_cursor_initialized": self._position_cursor_initialized,
            "position_cursor_rows": {
                side.value: len(self._position_cursor[side])
                for side in (BookSide.BID, BookSide.ASK)
            },
            "reset_count": self._reset_count,
            "capacity_losses": self._capacity_losses,
            "movement_reserve_observations": self._movement_reserve_observations,
            "movement_guard": {
                "reserve_ticks": self.config.movement_reserve_ticks,
                "mode": "DIAGNOSTIC_ONLY_ALL_AUTHENTIC_INPUT_RETAINED",
                "observations_outside_reserve": self._movement_reserve_observations,
            },
            "shadow_maintenance_nanoseconds": self._shadow_maintenance_ns,
            "last_invalidation_reason": self._last_invalidation_reason,
            "unavailable_reason": self._unavailable_reason,
            "history": {
                "window_seconds": self.config.shadow_history_seconds,
                "ready": self.entry_ready,
                "rewarm_prices": list(self._current_rewarm_prices),
                "shadow_events": self._shadow_event_count,
            },
            "last_decision": None if last is None else {
                "action": last.action.value,
                "reason": last.reason,
                "side": None if last.side is None else last.side.value,
                "price_ticks": last.price_ticks,
                "pre_core_member": last.pre_core_member,
                "post_core_member": last.post_core_member,
                "promoted_price_ticks": list(last.promoted_price_ticks),
                "seed_observations": len(last.seed_observations),
            },
            "entry_ready": self.entry_ready,
        }
