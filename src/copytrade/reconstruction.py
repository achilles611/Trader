from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .models import PositionCampaign, PositionEvent, PositionEventType, RawFill, stable_id


EPSILON = 1e-12


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


def aggregate_partial_fills(fills: Iterable[RawFill]) -> list[FillAggregate]:
    """Combine only contiguous partials while preserving source-fill evidence."""
    ordered = sorted(fills, key=lambda fill: (fill.event_timestamp, fill.event_id))
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
        aggregates.append(FillAggregate(
            fills=tuple(group), target_wallet=group[0].target_wallet, symbol=group[0].symbol,
            signed_quantity=total_quantity, price=notional / absolute_quantity if absolute_quantity else group[-1].price,
            notional=notional, fee=sum(fill.fee for fill in group), event_timestamp=group[-1].event_timestamp,
            target_equity=next((fill.target_account_equity for fill in reversed(group) if fill.target_account_equity is not None), None),
            position_before=group[0].target_position_before,
            source_closed_pnl=sum(source_closed) if source_closed else None,
            is_liquidation=any(fill.is_liquidation for fill in group),
        ))
    return aggregates


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
        before = aggregate.position_before if aggregate.position_before is not None else previous
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
