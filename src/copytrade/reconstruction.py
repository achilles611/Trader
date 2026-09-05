from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from datetime import datetime
from math import gcd, isfinite
from typing import Iterable

from .models import PositionCampaign, PositionEvent, PositionEventType, RawFill, stable_id


EPSILON = 1e-12
_CANONICAL_SOURCE_POSITION = "_copytrade_canonical_start_position"


class SourcePositionContinuityError(ValueError):
    """Source evidence does not prove one causal position path.

    This is intentionally a data-integrity outcome, not a performance result.
    The caller must preserve saved fills and keep campaign-derived metrics out
    of selection until a source-backed continuation is available.
    """

    def __init__(
        self, reason: str, *, wallet: str, symbol: str, timestamp: datetime,
        detail: str = "",
    ) -> None:
        self.reason = reason
        self.wallet = wallet.lower()
        self.symbol = symbol
        self.timestamp = timestamp
        self.detail = detail
        suffix = f" ({detail})" if detail else ""
        super().__init__(f"{reason}: {self.wallet}/{symbol} at {timestamp.isoformat()}{suffix}")


@dataclass(frozen=True)
class FillAggregate:
    fills: tuple[RawFill, ...]
    target_wallet: str
    symbol: str
    signed_quantity: float
    price: float
    notional: float
    fee: float
    event_timestamp: datetime
    target_equity: float | None
    position_before: float | None
    position_tolerance: float | None
    source_closed_pnl: float | None
    is_liquidation: bool

    @property
    def raw_fill_ids(self) -> tuple[str, ...]:
        return tuple(fill.event_id for fill in self.fills)


@dataclass(frozen=True)
class ReconstructionResult:
    events: tuple[PositionEvent, ...]
    campaigns: tuple[PositionCampaign, ...]

    @property
    def reconciliation(self) -> dict[str, float | int]:
        observed = [item for item in self.campaigns if item.source_closed_pnl_observed]
        reconstructed = [item for item in observed if item.history_complete]
        source_gross = sum(item.source_closed_pnl for item in observed)
        reconstructed_gross = sum(item.realized_pnl for item in reconstructed)
        source_fees = sum(item.target_fees for item in self.campaigns)
        reconstructed_fees = sum(item.target_fees for item in self.campaigns)
        mismatched = [item for item in observed if item.reconciliation_gross_difference is not None and abs(item.reconciliation_gross_difference) > 1e-8]
        return {
            "source_closed_pnl": source_gross,
            "reconstructed_gross_realized_pnl": reconstructed_gross,
            "source_fees": source_fees,
            "reconstructed_fees": reconstructed_fees,
            "source_net_pnl": source_gross - source_fees,
            "reconstructed_net_pnl": reconstructed_gross - reconstructed_fees,
            "absolute_difference": abs(source_gross - reconstructed_gross),
            "relative_difference": abs(source_gross - reconstructed_gross) / max(abs(source_gross), 1e-12),
            "mismatched_campaigns": len(mismatched),
            "mismatched_fills": sum(len(item.raw_fill_ids) for item in mismatched),
            "unreconstructable_campaigns": sum(not item.history_complete for item in observed),
        }


@dataclass
class IncrementalReconstructionState:
    """Minimal durable state needed to continue a wallet without rereading fills.

    Closed campaigns deliberately are not retained here.  The only historical
    accounting needed by the next source aggregate is the current source
    position and its active campaign; the complete campaign ledger remains in
    SQLite for reporting and explicit rebuilds.
    """

    positions: dict[tuple[str, str], float]
    active_campaigns: dict[tuple[str, str], PositionCampaign]
    # Float campaign economics remain separate from exact source continuity.
    # This is populated only from a source-proven aggregate boundary; restored
    # legacy state intentionally leaves it empty until fresh source evidence
    # establishes a new exact boundary.
    source_positions: dict[tuple[str, str], Decimal] = field(default_factory=dict)


def aggregate_partial_fills(fills: Iterable[RawFill]) -> list[FillAggregate]:
    """Aggregate only a source-proven causal fill sequence.

    Native event IDs provide stable uniqueness but not causal ordering.  For
    fills with an identical wallet/symbol/timestamp, Hyperliquid's reported
    ``startPosition`` is the only admissible ordering input: buys advance it
    and sells reduce it.  A duplicate boundary, side mix, missing source field,
    or non-continuous chain is explicitly rejected rather than guessed from
    lexical IDs, PnL, or desired reconciliation.
    """
    ordered = _causally_ordered_fills(fills)
    groups: list[list[RawFill]] = []
    for fill in ordered:
        if not groups:
            groups.append([fill])
            continue
        previous = groups[-1][-1]
        same_order = (
            fill.target_order_id is not None
            and fill.target_order_id == previous.target_order_id
            and fill.target_wallet == previous.target_wallet
            and fill.symbol == previous.symbol
            and (fill.signed_quantity >= 0) == (previous.signed_quantity >= 0)
            # An order ID is an attribution key, not proof that separate
            # timestamps form one uninterrupted position transition.  Keeping
            # a gap in one aggregate would hide it from apply_aggregate's
            # source-position continuity check.
            and _source_transition_is_contiguous(previous, fill)
        )
        if same_order:
            groups[-1].append(fill)
        else:
            groups.append([fill])

    aggregates: list[FillAggregate] = []
    for group in groups:
        total_quantity = sum(fill.signed_quantity for fill in group)
        absolute_quantity = sum(abs(fill.signed_quantity) for fill in group)
        notional = sum(fill.notional for fill in group)
        source_closed = [fill.source_closed_pnl for fill in group if fill.source_closed_pnl is not None]
        # ``group`` is already source-orderable.  Its first source position is
        # therefore the causal boundary; taking a min/max alone would hide an
        # interleaving or source-position gap.
        position_before = group[0].target_position_before
        aggregates.append(FillAggregate(
            fills=tuple(group), target_wallet=group[0].target_wallet, symbol=group[0].symbol,
            signed_quantity=total_quantity, price=notional / absolute_quantity if absolute_quantity else group[-1].price,
            notional=notional, fee=sum(fill.fee for fill in group), event_timestamp=group[-1].event_timestamp,
            target_equity=next((fill.target_account_equity for fill in reversed(group) if fill.target_account_equity is not None), None),
            position_before=position_before,
            position_tolerance=float(_source_position_tolerance(group[0])) if position_before is not None else None,
            source_closed_pnl=sum(source_closed) if source_closed else None,
            is_liquidation=any(fill.is_liquidation for fill in group),
        ))
    return aggregates


def _causally_ordered_fills(fills: Iterable[RawFill]) -> list[RawFill]:
    buckets: dict[tuple[str, str, datetime], list[RawFill]] = {}
    for fill in fills:
        key = (fill.target_wallet.lower(), fill.symbol, fill.event_timestamp)
        buckets.setdefault(key, []).append(fill)
    ordered: list[RawFill] = []
    # A source size lattice is accrued only from source ``sz`` values already
    # observed for this wallet/symbol.  It is not a configurable tolerance and
    # is never inferred from price, P&L, or a desired reconstruction outcome.
    size_lattices: dict[tuple[str, str], Decimal] = {}
    for (wallet, symbol, timestamp), bundle in sorted(
        buckets.items(), key=lambda item: (item[0][2], item[0][0], item[0][1]),
    ):
        lattice_key = (wallet, symbol)
        lattice = _update_source_size_lattice(size_lattices.get(lattice_key), bundle)
        if lattice is not None:
            size_lattices[lattice_key] = lattice
        normalized_bundle = _normalize_decimal_position_aliases(bundle, lattice)
        ordered.extend(_order_same_timestamp_bundle(
            normalized_bundle, wallet=wallet, symbol=symbol, timestamp=timestamp,
        ))
    return ordered


def _update_source_size_lattice(current: Decimal | None, bundle: Iterable[RawFill]) -> Decimal | None:
    """Accrue the exact Decimal GCD of observed source sizes.

    A missing, invalid, or zero source size cannot establish a lattice and is
    deliberately ignored here; the normal reconstruction checks retain their
    existing fail-closed behavior for the raw fill itself.
    """
    lattice = current
    for fill in bundle:
        raw = fill.raw_payload.get("sz")
        if raw in (None, ""):
            continue
        try:
            quantity = abs(Decimal(str(raw)))
        except InvalidOperation:
            continue
        if not quantity.is_finite() or quantity == 0:
            continue
        lattice = quantity if lattice is None else _decimal_gcd(lattice, quantity)
    return lattice


def _decimal_gcd(left: Decimal, right: Decimal) -> Decimal:
    """Return the exact Decimal lattice shared by two positive quantities."""
    exponent = min(left.as_tuple().exponent, right.as_tuple().exponent)
    scaled_left = int(left.scaleb(-exponent))
    scaled_right = int(right.scaleb(-exponent))
    return Decimal(gcd(scaled_left, scaled_right)).scaleb(exponent)


def _normalize_decimal_position_aliases(bundle: list[RawFill], lattice: Decimal | None) -> list[RawFill]:
    """Canonically replace only source-proven Decimal representation aliases.

    Public source payloads can spell a lattice point with a binary-decimal
    tail (for example ``1266979.3999999999`` for a 0.1 position lattice).
    This is not a tolerance relaxation: Decimal arithmetic chooses the unique
    source-size lattice point, and binary64 equality merely classifies two
    textual spellings as one representational alias.  The raw
    ``startPosition`` field and its stated precision are retained; an
    ephemeral private canonical value is used only for causal ordering when
    source ordering remains identical.
    """
    if lattice is None or not lattice.is_finite() or lattice <= 0:
        return list(bundle)
    try:
        positions = {fill.event_id: _source_position(fill) for fill in bundle}
    except (InvalidOperation, ValueError):
        return list(bundle)
    if any(not position.is_finite() for position in positions.values()):
        return list(bundle)

    # Do not normalise an ambiguity away.  The existing strict checker emits
    # the authoritative reason for zero quantities, mixed sides, or duplicate
    # source boundaries after this helper returns the untouched bundle.
    signs = {1 if fill.signed_quantity > 0 else -1 if fill.signed_quantity < 0 else 0 for fill in bundle}
    if 0 in signs or len(signs) != 1 or len(set(positions.values())) != len(bundle):
        return list(bundle)
    sign = next(iter(signs))
    raw_order = sorted(bundle, key=lambda fill: (positions[fill.event_id], fill.event_id), reverse=sign < 0)

    replacements: dict[str, Decimal] = {}
    for fill in bundle:
        position = positions[fill.event_id]
        if position % lattice == 0:
            continue
        canonical = (position / lattice).to_integral_value(rounding=ROUND_HALF_EVEN) * lattice
        if (
            abs(position - canonical) >= lattice / 2
            or not _same_binary64(position, canonical)
        ):
            continue
        replacements[fill.event_id] = canonical
    if not replacements:
        return list(bundle)

    normalized_positions = {fill.event_id: replacements.get(fill.event_id, positions[fill.event_id]) for fill in bundle}
    # A correction may not turn distinct raw boundaries into a duplicate or
    # choose a different causal order.  Leave the source untouched in either
    # case and let the strict validator fail closed.
    if len(set(normalized_positions.values())) != len(bundle):
        return list(bundle)
    normalized_order = sorted(
        bundle, key=lambda fill: (normalized_positions[fill.event_id], fill.event_id), reverse=sign < 0,
    )
    if [fill.event_id for fill in raw_order] != [fill.event_id for fill in normalized_order]:
        return list(bundle)

    normalized: list[RawFill] = []
    for fill in bundle:
        canonical = replacements.get(fill.event_id)
        if canonical is None:
            normalized.append(fill)
            continue
        payload = dict(fill.raw_payload)
        payload[_CANONICAL_SOURCE_POSITION] = format(canonical, "f")
        normalized.append(replace(
            fill, raw_payload=payload, target_position_before=float(canonical),
        ))
    return normalized


def _same_binary64(left: Decimal, right: Decimal) -> bool:
    """Whether two Decimal spellings identify one finite IEEE-754 value."""
    try:
        left_float, right_float = float(left), float(right)
    except (OverflowError, ValueError):
        return False
    return isfinite(left_float) and isfinite(right_float) and left_float == right_float


def _order_same_timestamp_bundle(
    bundle: list[RawFill], *, wallet: str, symbol: str, timestamp: datetime,
) -> list[RawFill]:
    if len(bundle) <= 1:
        return list(bundle)
    signs = {1 if fill.signed_quantity > 0 else -1 if fill.signed_quantity < 0 else 0 for fill in bundle}
    if 0 in signs:
        raise SourcePositionContinuityError(
            "UNRESOLVED_SOURCE_POSITION_ZERO_QUANTITY", wallet=wallet, symbol=symbol, timestamp=timestamp,
        )
    if len(signs) != 1:
        raise SourcePositionContinuityError(
            "UNRESOLVED_SOURCE_POSITION_MIXED_SIDE", wallet=wallet, symbol=symbol, timestamp=timestamp,
        )
    try:
        positions = {fill.event_id: _source_position(fill) for fill in bundle}
    except (InvalidOperation, ValueError) as error:
        raise SourcePositionContinuityError(
            "UNRESOLVED_SOURCE_POSITION_MISSING_OR_INVALID", wallet=wallet, symbol=symbol, timestamp=timestamp,
            detail=str(error),
        ) from error
    if len(set(positions.values())) != len(bundle):
        raise SourcePositionContinuityError(
            "UNRESOLVED_SOURCE_POSITION_DUPLICATE_BOUNDARY", wallet=wallet, symbol=symbol, timestamp=timestamp,
        )
    sign = next(iter(signs))
    ordered = sorted(bundle, key=lambda fill: (positions[fill.event_id], fill.event_id), reverse=sign < 0)
    for previous, following in zip(ordered, ordered[1:]):
        expected = positions[previous.event_id] + _signed_source_quantity(previous)
        actual = positions[following.event_id]
        if abs(expected - actual) > _source_position_tolerance(following):
            raise SourcePositionContinuityError(
                "UNRESOLVED_SOURCE_POSITION_GAP", wallet=wallet, symbol=symbol, timestamp=timestamp,
                detail=f"expected={expected} reported={actual}",
            )
    return ordered


def _source_position(fill: RawFill) -> Decimal:
    raw = fill.raw_payload.get(_CANONICAL_SOURCE_POSITION, fill.raw_payload.get("startPosition"))
    if raw in (None, ""):
        raise ValueError("missing startPosition")
    return Decimal(str(raw))


def _signed_source_quantity(fill: RawFill) -> Decimal:
    raw = fill.raw_payload.get("sz")
    quantity = Decimal(str(raw if raw not in (None, "") else abs(fill.base_quantity)))
    return quantity if fill.signed_quantity > 0 else -quantity


def _source_position_tolerance(fill: RawFill) -> Decimal:
    raw = fill.raw_payload.get("startPosition")
    if raw in (None, ""):
        raise ValueError("missing startPosition")
    decimal = Decimal(str(raw))
    # The half-unit of the source field's own stated scale is the only
    # permitted comparison interval; this is not a global reconciliation
    # tolerance and cannot be tuned to improve P&L.
    return Decimal(5).scaleb(decimal.as_tuple().exponent - 1)


def _source_transition_is_contiguous(previous: RawFill, following: RawFill) -> bool:
    """Whether adjacent source fills prove a single aggregate boundary.

    This deliberately returns ``False`` for absent or unparsable source
    evidence.  The next aggregate then reaches the position reconstructor,
    which records the precise fail-closed integrity outcome instead of
    treating a repeated order ID as evidence of continuity.
    """
    try:
        expected = _source_position(previous) + _signed_source_quantity(previous)
        actual = _source_position(following)
        return abs(expected - actual) <= _source_position_tolerance(following)
    except (InvalidOperation, ValueError):
        return False


def _aggregate_source_position_before(aggregate: FillAggregate) -> Decimal | None:
    """Return the first exact source boundary of a causally ordered aggregate."""
    if not aggregate.fills:
        return None
    try:
        return _source_position(aggregate.fills[0])
    except (InvalidOperation, ValueError):
        return None


def _aggregate_source_position_after(aggregate: FillAggregate) -> Decimal | None:
    """Return the final exact source boundary without float accumulation."""
    if not aggregate.fills:
        return None
    try:
        final = aggregate.fills[-1]
        return _source_position(final) + _signed_source_quantity(final)
    except (InvalidOperation, ValueError):
        return None


class PositionReconstructor:
    """Rebuild campaigns causally; an unknown historical entry is never priced in."""

    def reconstruct(self, fills: Iterable[RawFill]) -> ReconstructionResult:
        state = IncrementalReconstructionState({}, {})
        campaigns: dict[str, PositionCampaign] = {}
        events: list[PositionEvent] = []

        for aggregate in aggregate_partial_fills(fills):
            generated, changed = self.apply_aggregate(state, aggregate)
            events.extend(generated)
            for campaign in changed:
                campaigns[campaign.campaign_id] = campaign

        for campaign in campaigns.values():
            self._refresh_reconciliation(campaign)
        return ReconstructionResult(tuple(events), tuple(campaigns.values()))

    @staticmethod
    def incremental_state(campaigns: Iterable[PositionCampaign]) -> IncrementalReconstructionState:
        """Restore only active source campaigns for cursor-driven processing."""
        positions: dict[tuple[str, str], float] = {}
        active: dict[tuple[str, str], PositionCampaign] = {}
        for campaign in campaigns:
            if campaign.is_closed:
                continue
            key = (campaign.target_wallet.lower(), campaign.symbol)
            # An invariant violation should not silently pick an arbitrary
            # campaign.  A full rebuild is the caller's safe repair path.
            if key in active:
                raise ValueError(f"Multiple active campaigns for {campaign.target_wallet}/{campaign.symbol}")
            active[key] = campaign
            positions[key] = campaign.open_quantity if campaign.direction == "long" else -campaign.open_quantity
        return IncrementalReconstructionState(positions, active)

    def apply_aggregate(
        self, state: IncrementalReconstructionState, aggregate: FillAggregate,
    ) -> tuple[tuple[PositionEvent, ...], tuple[PositionCampaign, ...]]:
        """Apply one finalized aggregate without rereading older raw evidence.

        The transition logic is shared with full reconstruction so a complete
        history and a clean cursor plus incremental chunks have identical
        events and campaign economics.
        """
        key = (aggregate.target_wallet.lower(), aggregate.symbol)
        previous = state.positions.get(key, 0.0)
        source_before = _aggregate_source_position_before(aggregate)
        source_after = _aggregate_source_position_after(aggregate)
        if aggregate.position_before is None:
            if key in state.positions:
                raise SourcePositionContinuityError(
                    "UNRESOLVED_SOURCE_POSITION_MISSING", wallet=aggregate.target_wallet,
                    symbol=aggregate.symbol, timestamp=aggregate.event_timestamp,
                    detail=f"expected prior position {previous}",
                )
            before = previous
        else:
            if key in state.positions:
                prior_source = state.source_positions.get(key)
                # Exact Decimal source boundaries prevent an otherwise valid
                # lattice alias from being rejected solely because binary64
                # arithmetic accumulated one ULP in the economic state.  The
                # source field's existing stated-scale tolerance is retained.
                if prior_source is not None and source_before is not None:
                    tolerance = _source_position_tolerance(aggregate.fills[0])
                    discontinuous = abs(prior_source - source_before) > tolerance
                    expected, reported = prior_source, source_before
                else:
                    tolerance = aggregate.position_tolerance
                    discontinuous = tolerance is None or abs(previous - aggregate.position_before) > tolerance
                    expected, reported = previous, aggregate.position_before
                if discontinuous:
                    raise SourcePositionContinuityError(
                        "UNRESOLVED_SOURCE_POSITION_CONTINUITY", wallet=aggregate.target_wallet,
                        symbol=aggregate.symbol, timestamp=aggregate.event_timestamp,
                        detail=f"expected={expected} reported={reported}",
                    )
            before = aggregate.position_before
        after = before + aggregate.signed_quantity
        if abs(after) < EPSILON:
            after = 0.0
        kind = self._event_type(before, after)
        active = state.active_campaigns.get(key)
        events: list[PositionEvent] = []
        changed: dict[str, PositionCampaign] = {}

        if kind is PositionEventType.OPEN:
            active = self._new_campaign(aggregate, self._direction(after))
            state.active_campaigns[key] = active
            self._entry(active, aggregate, abs(after), 1.0)
            events.append(self._event(aggregate, active, PositionEventType.OPEN, before, after,
                                      aggregate.signed_quantity, 1.0, "opening"))
            changed[active.campaign_id] = active
        elif kind is PositionEventType.ADD:
            if active is None or active.direction != self._direction(after):
                # State proves there was already exposure, but not what it cost.
                active = self._unknown_campaign(aggregate, self._direction(before), abs(before))
                state.active_campaigns[key] = active
            self._entry(active, aggregate, abs(aggregate.signed_quantity), 1.0)
            events.append(self._event(aggregate, active, PositionEventType.ADD, before, after,
                                      aggregate.signed_quantity, 1.0, None))
            changed[active.campaign_id] = active
        elif kind in {PositionEventType.REDUCE, PositionEventType.CLOSE}:
            if active is None:
                active = self._unknown_campaign(aggregate, self._direction(before), abs(before))
                state.active_campaigns[key] = active
            closing_quantity = min(abs(aggregate.signed_quantity), abs(before))
            self._exit(active, aggregate, closing_quantity, 1.0)
            events.append(self._event(aggregate, active, kind, before, after,
                                      aggregate.signed_quantity, 1.0, None))
            if kind is PositionEventType.CLOSE:
                self._close(active, aggregate)
                state.active_campaigns.pop(key, None)
            changed[active.campaign_id] = active
        else:
            # A flip is one immutable source fill, but two independent
            # economic actions.  Allocate notional, fees, source closedPnl,
            # event attribution and event counts proportionally.
            close_quantity, open_quantity = abs(before), abs(after)
            total_quantity = close_quantity + open_quantity
            close_fraction = close_quantity / max(total_quantity, EPSILON)
            open_fraction = open_quantity / max(total_quantity, EPSILON)
            if active is None:
                active = self._unknown_campaign(aggregate, self._direction(before), close_quantity)
            self._exit(active, aggregate, close_quantity, close_fraction)
            events.append(self._event(aggregate, active, PositionEventType.CLOSE, before, 0.0,
                                      -before, close_fraction, "closing", source_event_type="FLIP"))
            self._close(active, aggregate)
            changed[active.campaign_id] = active
            new = self._new_campaign(aggregate, self._direction(after))
            state.active_campaigns[key] = new
            self._entry(new, aggregate, open_quantity, open_fraction)
            events.append(self._event(aggregate, new, PositionEventType.OPEN, 0.0, after,
                                      after, open_fraction, "opening", source_event_type="FLIP"))
            changed[new.campaign_id] = new

        state.positions[key] = after
        if source_after is None:
            state.source_positions.pop(key, None)
        else:
            state.source_positions[key] = source_after
        for campaign in changed.values():
            self._refresh_reconciliation(campaign)
        return tuple(events), tuple(changed.values())

    @staticmethod
    def _refresh_reconciliation(campaign: PositionCampaign) -> None:
        if campaign.source_closed_pnl_observed and campaign.history_complete:
            campaign.reconciliation_gross_difference = campaign.realized_pnl - campaign.source_closed_pnl

    @staticmethod
    def _event_type(before: float, after: float) -> PositionEventType:
        if abs(before) < EPSILON:
            return PositionEventType.OPEN
        if abs(after) < EPSILON:
            return PositionEventType.CLOSE
        if before * after < 0:
            return PositionEventType.FLIP
        return PositionEventType.ADD if abs(after) > abs(before) + EPSILON else PositionEventType.REDUCE

    @staticmethod
    def _direction(quantity: float) -> str:
        return "long" if quantity >= 0 else "short"

    @staticmethod
    def _new_campaign(aggregate: FillAggregate, direction: str) -> PositionCampaign:
        return PositionCampaign(
            campaign_id=stable_id("campaign", aggregate.target_wallet.lower(), aggregate.symbol, direction, aggregate.raw_fill_ids[0]),
            target_wallet=aggregate.target_wallet.lower(), symbol=aggregate.symbol, direction=direction,
            opened_at=aggregate.event_timestamp,
        )

    @staticmethod
    def _unknown_campaign(aggregate: FillAggregate, direction: str, opening_quantity: float) -> PositionCampaign:
        campaign = PositionReconstructor._new_campaign(aggregate, direction)
        campaign.open_quantity = opening_quantity
        campaign.max_open_quantity = opening_quantity
        campaign.history_complete = False
        campaign.entry_basis_quality = "unknown_truncated"
        return campaign

    @staticmethod
    def _entry(campaign: PositionCampaign, aggregate: FillAggregate, quantity: float, fraction: float) -> None:
        if quantity <= EPSILON:
            return
        known_open = campaign.open_quantity if campaign.history_complete else campaign.entry_quantity
        average_before = campaign.remaining_entry_notional / max(campaign.open_quantity, EPSILON)
        if known_open > EPSILON and ((campaign.direction == "long" and aggregate.price < average_before) or
                                     (campaign.direction == "short" and aggregate.price > average_before)):
            campaign.adverse_add_count += 1
        campaign.entry_quantity += quantity
        campaign.entry_notional += aggregate.price * quantity
        campaign.remaining_entry_notional += aggregate.price * quantity
        campaign.open_quantity += quantity
        campaign.max_open_quantity = max(campaign.max_open_quantity, campaign.open_quantity)
        PositionReconstructor._attribute(campaign, aggregate, fraction, source_closed_pnl_fraction=0.0)

    @staticmethod
    def _exit(campaign: PositionCampaign, aggregate: FillAggregate, quantity: float, fraction: float) -> None:
        if quantity <= EPSILON:
            return
        # A source closedPnl is still retained for audit, but incomplete history
        # means we do not manufacture a synthetic entry price or trusted P&L.
        average_entry = campaign.remaining_entry_notional / max(campaign.open_quantity, EPSILON)
        if campaign.history_complete and average_entry:
            pnl_sign = 1.0 if campaign.direction == "long" else -1.0
            campaign.realized_pnl += (aggregate.price - average_entry) * quantity * pnl_sign
        campaign.exit_notional += aggregate.price * quantity
        campaign.remaining_entry_notional = max(0.0, campaign.remaining_entry_notional - average_entry * quantity)
        campaign.open_quantity = max(0.0, campaign.open_quantity - quantity)
        PositionReconstructor._attribute(campaign, aggregate, fraction, source_closed_pnl_fraction=1.0)

    @staticmethod
    def _attribute(
        campaign: PositionCampaign, aggregate: FillAggregate, fraction: float, *, source_closed_pnl_fraction: float,
    ) -> None:
        campaign.event_count += 1
        campaign.raw_fill_ids.extend(aggregate.raw_fill_ids)
        campaign.target_fees += aggregate.fee * fraction
        campaign.liquidation_count += int(aggregate.is_liquidation)
        if aggregate.source_closed_pnl is not None and source_closed_pnl_fraction > 0:
            campaign.source_closed_pnl += aggregate.source_closed_pnl * source_closed_pnl_fraction
            campaign.source_closed_pnl_observed = True

    @staticmethod
    def _close(campaign: PositionCampaign, aggregate: FillAggregate) -> None:
        campaign.open_quantity = 0.0
        campaign.remaining_entry_notional = 0.0
        campaign.closed_at = aggregate.event_timestamp

    @staticmethod
    def _event(
        aggregate: FillAggregate, campaign: PositionCampaign, event_type: PositionEventType,
        before: float, after: float, delta: float, fraction: float, split_role: str | None,
        *, source_event_type: str | None = None,
    ) -> PositionEvent:
        notional = aggregate.notional * fraction
        quantity = abs(delta)
        return PositionEvent(
            event_id=stable_id("posevent", aggregate.raw_fill_ids, event_type.value, before, after, split_role),
            target_wallet=aggregate.target_wallet.lower(), symbol=aggregate.symbol, event_type=event_type,
            direction=campaign.direction, delta_quantity=delta, before_quantity=before, after_quantity=after,
            price=aggregate.price, notional=notional, event_timestamp=aggregate.event_timestamp,
            campaign_id=campaign.campaign_id, raw_fill_ids=aggregate.raw_fill_ids,
            target_equity=aggregate.target_equity,
            initial_delta_notional=notional if event_type is PositionEventType.OPEN else 0.0,
            equity_source="exact" if aggregate.target_equity is not None else "missing",
            source_event_type=source_event_type, split_role=split_role, split_quantity=quantity,
            split_notional=notional, split_fee=aggregate.fee * fraction,
            source_closed_pnl=(aggregate.source_closed_pnl * (0.0 if event_type is PositionEventType.OPEN else 1.0)
                               if aggregate.source_closed_pnl is not None else None),
        )
