"""Frozen, causal definitions for Joseph's bounded experimental Pong policy.

This module grants no transport, account, registration or arming authority.
Inputs must originate at an authenticated adapter; typed data alone is not a
signature. The operational boundary must verify that adapter before use.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re

from src.lane_iii.contracts import canonical_hash, normalized_utc
from .daily_bias import (
    BiasInputProvenance, CoreComponentEvidence, CoreComponentKind,
    DailyBiasDirection, OptionsDayClass, OptionsOverlayEvidence,
    OptionsOverlayOutcome, PONG_TRIAL_OPTIONS_AUTHORITY,
)
from .pong_microstructure import MicrostructureEvidenceMappingPolicy


ELEVATED_AUTHORITY_SHA256 = "b4a7b3c3da4305a1be9bb1c28e8086deba4d29ee5d1733f4564f606b997cf17e"


def utc(value: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, "Pong time").replace("Z", "+00:00"))


def wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def experimental_definition() -> dict[str, object]:
    """Definitions frozen before any prospective evidence or P&L evaluation."""
    return {
        "schema": "pong-bounded-experimental-definition-v1",
        "elevated_authority_sha256": ELEVATED_AUTHORITY_SHA256,
        "profile": "BEELZEBUB_PONG_DISCERNMENT_V1",
        "classification": "EXPERIMENTAL_UNVALIDATED_DIRECTIONAL_SUPPORT",
        "account": "Sim101", "account_class": "LOCAL_SIMULATION",
        "native_contract": "MNQ SEP26", "canonical_contract": "MNQU6",
        "maximum_execution_quantity": 1, "duration_seconds": 3600,
        "options_policy_hash": PONG_TRIAL_OPTIONS_AUTHORITY,
        "trend": "TWO_COMPLETED_UTC_HOURS_HIGHER_HIGH_AND_HIGHER_LOW_LONG_MIRRORED_SHORT_ELSE_NEUTRAL",
        "crt": "LATEST_COMPLETED_HOUR_SWEEPS_PRIOR_LOW_AND_CLOSES_STRICTLY_INSIDE_PRIOR_RANGE_LONG_MIRRORED_SHORT_BOTH_OR_NEITHER_NEUTRAL",
        "core_vote": "EXISTING_TWO_OF_THREE_ALL_THREE_REQUIRED",
        "order_flow": "EXISTING_COMPLETED_HOUR_MINIMUM_8_CLASSIFIED_TRADES_PLUS_MINUS_0_10",
        "hourly_missing": "DENY_NO_PARTIAL_HOUR_OR_SYNTHETIC_HISTORY",
        "hourly_publication_maximum_seconds": 300,
        "hourly_validity_seconds": 3900,
        "sessions": "REPORTING_METADATA_ONLY",
        "market_scope": "ESTABLISHED_EXCHANGE_AND_RISK_GATES",
        "discernment_mapping_hash": MicrostructureEvidenceMappingPolicy().mapping_hash,
        "discernment_policy_hash": MicrostructureEvidenceMappingPolicy().discernment_policy().policy_hash,
        "switch_thresholds": {"long": "0.65", "short_long_axis": "0.35"},
        "strong_desired_sizing_threshold": "0.85",
        "live_authority": "DISARMED", "live_capital": "DENIED",
    }


EXPERIMENTAL_DEFINITION_HASH = canonical_hash(experimental_definition())


@dataclass(frozen=True)
class CompletedPongHour:
    """Two-sided OHLC from one fully observed, causal UTC hour.

    Coverage and authenticity must be established by the owning source adapter.
    Historical bars alone cannot establish classified flow or depth coverage.
    """

    start: str
    end: str
    available_at: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    source: str
    continuity_domain: str
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    complete: bool
    instrument: str = "MNQ SEP26"

    def __post_init__(self) -> None:
        start, end, available = utc(self.start), utc(self.end), utc(self.available_at)
        if end - start != timedelta(hours=1) or any((start.minute, start.second, start.microsecond)):
            raise ValueError("Pong bars require exact completed UTC hours.")
        if available < end or self.instrument not in {"MNQ SEP26", "MNQ DEC26"}:
            raise ValueError("Pong bar is future-dated or has the wrong contract.")
        if type(self.complete) is not bool or not self.source or not self.continuity_domain:
            raise ValueError("Pong hour requires explicit coverage and source identity.")
        values = (self.open, self.high, self.low, self.close)
        if any(type(x) is not Decimal or not x.is_finite() or x <= 0 for x in values):
            raise ValueError("Pong OHLC must contain positive finite decimals.")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("Pong OHLC range is inconsistent.")
        if not self.source_ids or len(self.source_ids) != len(self.source_hashes):
            raise ValueError("Pong bar source references must be complete.")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Pong bar source references must be unique.")
        if any(not x for x in self.source_ids) or any(re.fullmatch(r"[a-f0-9]{64}", x) is None for x in self.source_hashes):
            raise ValueError("Pong bar source references are invalid.")

    def payload(self) -> dict[str, object]:
        return {
            "start": wire(utc(self.start)), "end": wire(utc(self.end)),
            "available_at": wire(utc(self.available_at)),
            "open": str(self.open), "high": str(self.high),
            "low": str(self.low), "close": str(self.close),
            "source": self.source, "continuity_domain": self.continuity_domain,
            "source_ids": list(self.source_ids), "source_hashes": list(self.source_hashes),
            "complete": self.complete, "instrument": self.instrument,
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


def hourly_structure_components(
    previous: CompletedPongHour, current: CompletedPongHour, *, available_at: str,
) -> tuple[CoreComponentEvidence, CoreComponentEvidence]:
    """Strict structure comparison and prior-range sweep/re-entry, mirrored."""
    if type(previous) is not CompletedPongHour or type(current) is not CompletedPongHour:
        raise ValueError("Exact Pong completed-hour envelopes are required.")
    now = utc(available_at)
    if utc(previous.end) != utc(current.start):
        raise ValueError("Hourly structure cannot bridge a missing hour.")
    if previous.continuity_domain != current.continuity_domain or previous.source != current.source:
        raise ValueError("Hourly structure cannot bridge an unverified source/continuity change.")
    if not previous.complete or not current.complete:
        raise ValueError("Partial hours cannot establish daily-bias authority.")
    if max(utc(previous.available_at), utc(current.available_at), utc(current.end)) > now:
        raise ValueError("Hourly structure cannot use future availability.")
    if now > utc(current.end) + timedelta(seconds=300):
        raise ValueError("Hourly structure was not published within the causal deadline.")
    trend = DailyBiasDirection.NEUTRAL
    if current.high > previous.high and current.low > previous.low:
        trend = DailyBiasDirection.LONG
    elif current.high < previous.high and current.low < previous.low:
        trend = DailyBiasDirection.SHORT
    inside = previous.low < current.close < previous.high
    swept_low, swept_high = current.low < previous.low, current.high > previous.high
    crt = DailyBiasDirection.NEUTRAL
    if inside and swept_low and not swept_high:
        crt = DailyBiasDirection.LONG
    elif inside and swept_high and not swept_low:
        crt = DailyBiasDirection.SHORT
    source_ids = ("completed-hour-" + previous.result_hash, "completed-hour-" + current.result_hash)
    provenance = BiasInputProvenance(
        source=current.source, calculation_id="pong-trial-hourly-structure",
        calculation_version="1", calculation_hash=EXPERIMENTAL_DEFINITION_HASH,
        window_start=previous.start, window_end=current.end, latest_event_at=current.end,
        observed_at=available_at, available_at=available_at,
        valid_until=wire(utc(current.end) + timedelta(seconds=3900)),
        input_ids=source_ids, input_hashes=(previous.result_hash, current.result_hash),
    )
    metrics = (("prior_high", str(previous.high)), ("prior_low", str(previous.low)),
               ("current_high", str(current.high)), ("current_low", str(current.low)),
               ("current_close", str(current.close)))
    return tuple(
        CoreComponentEvidence(kind, direction, True, True, False,
                              ("EXPERIMENTAL_" + kind.value + "_" + direction.value,),
                              replace(provenance, calculation_id="pong-trial-" + kind.value.lower()), metrics)
        for kind, direction in ((CoreComponentKind.HOURLY_TREND, trend), (CoreComponentKind.HOURLY_CRT, crt))
    )


def unavailable_nonvoting_options(*, start: str, end: str, available_at: str) -> OptionsOverlayEvidence:
    """Record absence without claiming an ordinary day or positive evidence."""
    if utc(available_at) < utc(end) or utc(available_at) > utc(end) + timedelta(seconds=300):
        raise ValueError("Options absence envelope must use the causal hourly publication window.")
    provenance = BiasInputProvenance(
        source="UNAVAILABLE", calculation_id="pong-trial-options-nonvoting",
        calculation_version="1", calculation_hash=PONG_TRIAL_OPTIONS_AUTHORITY,
        window_start=start, window_end=end, latest_event_at=end,
        observed_at=available_at, available_at=available_at,
        valid_until=wire(utc(end) + timedelta(seconds=3900)),
        input_ids=("explicit-options-absence",),
        input_hashes=(canonical_hash({"source": "UNAVAILABLE", "authority": PONG_TRIAL_OPTIONS_AUTHORITY}),),
    )
    return OptionsOverlayEvidence(
        OptionsDayClass.UNKNOWN, OptionsOverlayOutcome.UNAVAILABLE, None, False,
        ("OPTIONS_UNAVAILABLE_NON_VOTING_EXPERIMENT_ONLY",), provenance,
        experimental_nonvoting_authority=PONG_TRIAL_OPTIONS_AUTHORITY,
    )
