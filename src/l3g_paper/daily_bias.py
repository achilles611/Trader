"""Causal, execution-independent daily-bias contracts for Pong design review.

The engine consumes completed-hour component evidence and produces immutable
LONG, SHORT, NEUTRAL, or UNAVAILABLE results.  It deliberately imports no
runtime, account, transport, selector, persistence, or order capability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Iterable

from src.lane_iii.contracts import canonical_hash, normalized_utc


PONG_DAILY_BIAS_SCHEMA = "beelzebub-pong-daily-bias-v0"
PONG_DAILY_BIAS_CALCULATION_ID = "pong-core-vote-options-overlay"
PONG_DAILY_BIAS_CALCULATION_VERSION = "0"
PONG_DAILY_BIAS_PRODUCTION_CONFIGURED = False
PONG_DAILY_BIAS_INSTRUMENT = "MNQ SEP26"
PONG_TRIAL_OPTIONS_AUTHORITY = canonical_hash({
    "schema": "pong-one-hour-options-nonvoting-v1",
    "authority": "JOSEPH_ELEVATED_PAPER_COMMISSIONING_20260911",
    "missing_options": "UNAVAILABLE_NON_VOTING_NO_CONFIDENCE_CREDIT",
    "maximum_execution_quantity": 1,
})


class DailyBiasDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"
    UNAVAILABLE = "UNAVAILABLE"


class CoreComponentKind(StrEnum):
    HOURLY_TREND = "HOURLY_TREND"
    HOURLY_CRT = "HOURLY_CRT"
    ORDER_FLOW = "ORDER_FLOW"


class BiasConfidenceTier(StrEnum):
    NONE = "NONE"
    CORE_2_OF_3 = "CORE_2_OF_3"
    CORE_3_OF_3 = "CORE_3_OF_3"
    CORE_2_OF_3_OPTIONS_CONFIRMED = "CORE_2_OF_3_OPTIONS_CONFIRMED"
    CORE_3_OF_3_OPTIONS_CONFIRMED = "CORE_3_OF_3_OPTIONS_CONFIRMED"
    CORE_3_OF_3_OPTIONS_CAUTION = "CORE_3_OF_3_OPTIONS_CAUTION"


class OptionsDayClass(StrEnum):
    ORDINARY = "ORDINARY"
    MATERIAL_EXPIRY = "MATERIAL_EXPIRY"
    UNKNOWN = "UNKNOWN"


class OptionsOverlayOutcome(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    CONFIRM = "CONFIRM"
    CAUTION = "CAUTION"
    VETO = "VETO"
    UNAVAILABLE = "UNAVAILABLE"


class BiasUpdateReason(StrEnum):
    COMPLETED_HOURLY_BOUNDARY = "COMPLETED_HOURLY_BOUNDARY"
    DATA_INVALIDATION = "DATA_INVALIDATION"
    OPTIONS_OVERLAY_CHANGE = "OPTIONS_OVERLAY_CHANGE"


class ClassifiedSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


def _time(value: str, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f"{field} must be a finite{' positive' if positive else ''} decimal.")
    return result


@dataclass(frozen=True)
class BiasInputProvenance:
    """Exact causal envelope for one component or overlay result."""

    source: str
    calculation_id: str
    calculation_version: str
    calculation_hash: str
    window_start: str
    window_end: str
    latest_event_at: str
    observed_at: str
    available_at: str
    valid_until: str
    input_ids: tuple[str, ...]
    input_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.source, "Bias source")
        _text(self.calculation_id, "Bias calculation ID")
        _text(self.calculation_version, "Bias calculation version")
        _sha256(self.calculation_hash, "Bias calculation hash")
        start = _time(self.window_start, "Bias input window start")
        end = _time(self.window_end, "Bias input window end")
        event = _time(self.latest_event_at, "Bias latest event time")
        observed = _time(self.observed_at, "Bias observed time")
        available = _time(self.available_at, "Bias available-at time")
        valid_until = _time(self.valid_until, "Bias validity time")
        if not start < end:
            raise ValueError("Bias input window must have positive duration.")
        if event < start or event > end or event > observed or observed > available:
            raise ValueError("Bias input provenance is non-causal.")
        if valid_until <= available:
            raise ValueError("Bias input validity must follow availability.")
        if not self.input_ids or len(self.input_ids) != len(self.input_hashes):
            raise ValueError("Bias input IDs and hashes must be non-empty and aligned.")
        if len(set(self.input_ids)) != len(self.input_ids):
            raise ValueError("Bias input IDs must be unique.")
        for identifier in self.input_ids:
            _text(identifier, "Bias input ID")
        for digest in self.input_hashes:
            _sha256(digest, "Bias input hash")

    def payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "calculation_id": self.calculation_id,
            "calculation_version": self.calculation_version,
            "calculation_hash": self.calculation_hash,
            "window_start": normalized_utc(self.window_start, "Bias input window start"),
            "window_end": normalized_utc(self.window_end, "Bias input window end"),
            "latest_event_at": normalized_utc(self.latest_event_at, "Bias latest event time"),
            "observed_at": normalized_utc(self.observed_at, "Bias observed time"),
            "available_at": normalized_utc(self.available_at, "Bias available-at time"),
            "valid_until": normalized_utc(self.valid_until, "Bias validity time"),
            "input_ids": list(self.input_ids),
            "input_hashes": list(self.input_hashes),
        }


@dataclass(frozen=True)
class CoreComponentEvidence:
    component: CoreComponentKind
    direction: DailyBiasDirection
    configured: bool
    valid: bool
    veto: bool
    reason_codes: tuple[str, ...]
    provenance: BiasInputProvenance
    metrics: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if type(self.component) is not CoreComponentKind:
            raise ValueError("Core component kind is required.")
        if type(self.direction) is not DailyBiasDirection:
            raise ValueError("Core component direction is required.")
        if any(type(value) is not bool for value in (self.configured, self.valid, self.veto)):
            raise ValueError("Core component state flags must be booleans.")
        if not self.reason_codes or any(not str(value).strip() for value in self.reason_codes):
            raise ValueError("Core component reason codes are required.")
        if not self.configured and (self.valid or self.direction is not DailyBiasDirection.UNAVAILABLE):
            raise ValueError("An unconfigured core component must be unavailable and invalid.")
        if self.valid and self.direction is DailyBiasDirection.UNAVAILABLE:
            raise ValueError("Valid core evidence cannot be unavailable.")
        if not self.valid and self.direction is not DailyBiasDirection.UNAVAILABLE:
            raise ValueError("Invalid core evidence must be unavailable.")
        if self.veto and not self.valid:
            raise ValueError("An invalid core component cannot assert a veto.")
        names = [name for name, _ in self.metrics]
        if len(names) != len(set(names)) or any(not name or not value for name, value in self.metrics):
            raise ValueError("Core component metrics must be unique non-empty text pairs.")

    @property
    def evidence_hash(self) -> str:
        return canonical_hash(self.payload())

    def payload(self) -> dict[str, object]:
        return {
            "component": self.component.value,
            "direction": self.direction.value,
            "configured": self.configured,
            "valid": self.valid,
            "veto": self.veto,
            "reason_codes": list(self.reason_codes),
            "provenance": self.provenance.payload(),
            "metrics": {name: value for name, value in self.metrics},
        }


@dataclass(frozen=True)
class OptionsOverlayEvidence:
    day_class: OptionsDayClass
    outcome: OptionsOverlayOutcome
    direction: DailyBiasDirection | None
    required: bool
    reason_codes: tuple[str, ...]
    provenance: BiasInputProvenance
    experimental_nonvoting_authority: str | None = None

    def __post_init__(self) -> None:
        nonvoting_trial = self.experimental_nonvoting_authority is not None
        if nonvoting_trial and (
            self.experimental_nonvoting_authority != PONG_TRIAL_OPTIONS_AUTHORITY
            or self.day_class is not OptionsDayClass.UNKNOWN
            or self.outcome is not OptionsOverlayOutcome.UNAVAILABLE
            or self.direction is not None
            or self.required is not False
            or self.provenance.source != "UNAVAILABLE"
            or self.reason_codes != ("OPTIONS_UNAVAILABLE_NON_VOTING_EXPERIMENT_ONLY",)
        ):
            raise ValueError("Non-voting options are sealed to the explicit one-hour Pong experiment.")
        if type(self.day_class) is not OptionsDayClass or type(self.outcome) is not OptionsOverlayOutcome:
            raise ValueError("Options day class and overlay outcome are required.")
        if self.direction is not None and type(self.direction) is not DailyBiasDirection:
            raise ValueError("Options direction must be LONG, SHORT, or absent.")
        if self.direction in {DailyBiasDirection.NEUTRAL, DailyBiasDirection.UNAVAILABLE}:
            raise ValueError("Options direction must be LONG, SHORT, or absent.")
        if type(self.required) is not bool:
            raise ValueError("Options required state must be a boolean.")
        if not self.reason_codes or any(not str(value).strip() for value in self.reason_codes):
            raise ValueError("Options overlay reason codes are required.")
        if self.day_class is OptionsDayClass.ORDINARY and (
            self.outcome is not OptionsOverlayOutcome.NOT_APPLICABLE or self.required
        ):
            raise ValueError("Ordinary options days must be non-required and not applicable.")
        if self.outcome is OptionsOverlayOutcome.NOT_APPLICABLE and self.day_class is not OptionsDayClass.ORDINARY:
            raise ValueError("Only an ordinary options day may be not applicable.")
        if self.day_class is OptionsDayClass.MATERIAL_EXPIRY and not self.required:
            raise ValueError("Material expiry options context must be required.")
        if not nonvoting_trial and self.day_class is OptionsDayClass.UNKNOWN and (
            self.outcome is not OptionsOverlayOutcome.UNAVAILABLE or not self.required
        ):
            raise ValueError("An unknown options day must fail as required and unavailable.")
        if self.outcome is OptionsOverlayOutcome.CONFIRM and self.direction is None:
            raise ValueError("A confirming options overlay requires a direction.")
        if not nonvoting_trial and self.outcome is OptionsOverlayOutcome.UNAVAILABLE and not self.required:
            raise ValueError("Unavailable options context must represent a required/unknown day.")

    @property
    def evidence_hash(self) -> str:
        return canonical_hash(self.payload())

    def payload(self) -> dict[str, object]:
        return {
            "day_class": self.day_class.value,
            "outcome": self.outcome.value,
            "direction": None if self.direction is None else self.direction.value,
            "required": self.required,
            "reason_codes": list(self.reason_codes),
            "provenance": self.provenance.payload(),
            **({"experimental_nonvoting_authority": self.experimental_nonvoting_authority}
               if self.experimental_nonvoting_authority is not None else {}),
        }


@dataclass(frozen=True)
class HourlyBiasFrame:
    instrument: str
    boundary_start: str
    boundary_end: str
    decision_at: str
    components: tuple[CoreComponentEvidence, ...]
    options: OptionsOverlayEvidence
    continuity_valid: bool
    prior_result_id: str | None

    def __post_init__(self) -> None:
        if self.instrument != PONG_DAILY_BIAS_INSTRUMENT:
            raise ValueError("Daily bias is sealed to MNQ SEP26.")
        if type(self.continuity_valid) is not bool:
            raise ValueError("Daily-bias continuity state must be a boolean.")
        start = _time(self.boundary_start, "Hourly boundary start")
        end = _time(self.boundary_end, "Hourly boundary end")
        decision = _time(self.decision_at, "Bias decision time")
        if end - start != timedelta(hours=1) or any((end.minute, end.second, end.microsecond)):
            raise ValueError("Daily bias requires an exact completed UTC hour.")
        if decision < end:
            raise ValueError("Daily bias cannot be available before the hour completes.")
        expected = set(CoreComponentKind)
        actual = {item.component for item in self.components}
        if len(self.components) != 3 or actual != expected:
            raise ValueError("Daily bias requires exactly one result for each core component.")
        for item in self.components:
            provenance = item.provenance
            if _time(provenance.window_end, "Component window end") > end:
                raise ValueError("Core evidence uses information after the completed boundary.")
            if _time(provenance.latest_event_at, "Component event time") > end:
                raise ValueError("Core evidence contains a future event.")
            available = _time(provenance.available_at, "Component available-at time")
            if available < end or available > decision:
                raise ValueError("Core evidence was not causally available at the decision.")
        if _time(self.options.provenance.available_at, "Options available-at time") > decision:
            raise ValueError("Options evidence was not causally available at the decision.")

    @property
    def frame_hash(self) -> str:
        return canonical_hash(self.payload())

    def payload(self) -> dict[str, object]:
        return {
            "instrument": self.instrument,
            "boundary_start": normalized_utc(self.boundary_start, "Hourly boundary start"),
            "boundary_end": normalized_utc(self.boundary_end, "Hourly boundary end"),
            "decision_at": normalized_utc(self.decision_at, "Bias decision time"),
            "components": [
                item.payload() for item in sorted(self.components, key=lambda value: value.component.value)
            ],
            "options": self.options.payload(),
            "continuity_valid": self.continuity_valid,
            "prior_result_id": self.prior_result_id,
        }


@dataclass(frozen=True)
class DailyBiasResult:
    result_id: str
    direction: DailyBiasDirection
    confidence: BiasConfidenceTier
    instrument: str
    boundary_start: str
    boundary_end: str
    available_at: str
    expires_at: str
    vote_counts: tuple[tuple[str, int], ...]
    reason_codes: tuple[str, ...]
    components: tuple[CoreComponentEvidence, ...]
    options: OptionsOverlayEvidence
    update_reason: BiasUpdateReason
    prior_result_id: str | None
    frame_hash: str
    configured: bool

    def __post_init__(self) -> None:
        _text(self.result_id, "Bias result ID")
        if re.fullmatch(r"pong-bias-[0-9a-f]{32}", self.result_id) is None:
            raise ValueError("Bias result ID must use the Pong bias namespace.")
        if type(self.direction) is not DailyBiasDirection:
            raise ValueError("Bias result direction is required.")
        if type(self.confidence) is not BiasConfidenceTier:
            raise ValueError("Bias confidence tier is required.")
        if self.instrument != PONG_DAILY_BIAS_INSTRUMENT:
            raise ValueError("Bias result instrument mismatch.")
        start = _time(self.boundary_start, "Bias result boundary start")
        end = _time(self.boundary_end, "Bias result boundary end")
        available = _time(self.available_at, "Bias result available-at time")
        expires = _time(self.expires_at, "Bias result expiry time")
        if end - start != timedelta(hours=1) or any((end.minute, end.second, end.microsecond)):
            raise ValueError("Bias result must describe one exact completed UTC hour.")
        if available < end or expires <= end:
            raise ValueError("Bias result availability or expiry is invalid.")
        expected_components = set(CoreComponentKind)
        if len(self.components) != 3 or {item.component for item in self.components} != expected_components:
            raise ValueError("Bias result requires exactly the three core components.")
        counts = dict(self.vote_counts)
        expected_counts = {item.value for item in DailyBiasDirection}
        if set(counts) != expected_counts or any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("Bias result vote counts are invalid.")
        if sum(counts.values()) != 3:
            raise ValueError("Bias result vote counts must total three.")
        derived_counts = {item.value: 0 for item in DailyBiasDirection}
        for component in self.components:
            derived_counts[component.direction.value] += 1
        if counts != derived_counts:
            raise ValueError("Bias result vote counts do not match its components.")
        if not self.reason_codes or any(not str(value).strip() for value in self.reason_codes):
            raise ValueError("Bias result reason codes are required.")
        directional = self.direction in {DailyBiasDirection.LONG, DailyBiasDirection.SHORT}
        if directional != (self.confidence is not BiasConfidenceTier.NONE):
            raise ValueError("Bias direction and confidence tier are inconsistent.")
        if type(self.update_reason) is not BiasUpdateReason:
            raise ValueError("Bias result update reason is required.")
        _sha256(self.frame_hash, "Bias result frame hash")
        if self.configured != all(item.configured for item in self.components):
            raise ValueError("Bias configured state must match its core components.")
        if type(self.configured) is not bool:
            raise ValueError("Bias configured state must be a boolean.")
        expected_result_id = "pong-bias-" + canonical_hash(self.payload(include_id=False))[:32]
        if self.result_id != expected_result_id:
            raise ValueError("Bias result ID does not match its canonical payload.")

    @property
    def source_id(self) -> str:
        return self.result_id

    @property
    def as_of(self) -> str:
        return self.boundary_end

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload(include_id=False))

    def payload(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": PONG_DAILY_BIAS_SCHEMA,
            "calculation_id": PONG_DAILY_BIAS_CALCULATION_ID,
            "calculation_version": PONG_DAILY_BIAS_CALCULATION_VERSION,
            "direction": self.direction.value,
            "confidence": self.confidence.value,
            "instrument": self.instrument,
            "boundary_start": normalized_utc(self.boundary_start, "Bias boundary start"),
            "boundary_end": normalized_utc(self.boundary_end, "Bias boundary end"),
            "available_at": normalized_utc(self.available_at, "Bias available-at time"),
            "expires_at": normalized_utc(self.expires_at, "Bias expiry time"),
            "vote_counts": {name: count for name, count in self.vote_counts},
            "reason_codes": list(self.reason_codes),
            "components": [
                item.payload() for item in sorted(self.components, key=lambda value: value.component.value)
            ],
            "options": self.options.payload(),
            "update_reason": self.update_reason.value,
            "prior_result_id": self.prior_result_id,
            "frame_hash": self.frame_hash,
            "configured": self.configured,
            "production_configured": PONG_DAILY_BIAS_PRODUCTION_CONFIGURED,
            "live_capital": "DENIED",
        }
        if include_id:
            payload["result_id"] = self.result_id
            payload["result_hash"] = self.result_hash
        return payload

    def eligibility(self, *, at: str, direction: object) -> str:
        if not self.configured:
            return "DAILY_BIAS_UNCONFIGURED"
        decision = _time(at, "Pong bias eligibility time")
        if decision < _time(self.available_at, "Bias available-at time"):
            return "DAILY_BIAS_NOT_YET_AVAILABLE"
        if decision > _time(self.expires_at, "Bias expiry time"):
            return "DAILY_BIAS_STALE"
        if self.direction is DailyBiasDirection.UNAVAILABLE:
            return "DAILY_BIAS_UNAVAILABLE"
        if self.direction is DailyBiasDirection.NEUTRAL:
            return "DAILY_BIAS_NOT_DIRECTIONAL"
        requested = getattr(direction, "value", direction)
        if requested not in {DailyBiasDirection.LONG.value, DailyBiasDirection.SHORT.value}:
            return "DAILY_BIAS_DIRECTION_INVALID"
        if self.direction.value != requested:
            return "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS"
        return "DAILY_BIAS_ALIGNED"


@dataclass(frozen=True)
class DailyBiasUpdate:
    result: DailyBiasResult
    changed: bool
    reason_code: str


class DailyBiasEngine:
    """Pure boundary-gated voter; it has no observation or execution side effects."""

    def __init__(self) -> None:
        self._current: DailyBiasResult | None = None

    @property
    def current(self) -> DailyBiasResult:
        return self._current if self._current is not None else default_unconfigured_bias()

    def update(self, frame: HourlyBiasFrame, reason: BiasUpdateReason) -> DailyBiasUpdate:
        current = self._current
        boundary = _time(frame.boundary_end, "Bias boundary end")
        if current is None:
            if reason is not BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY:
                return DailyBiasUpdate(default_unconfigured_bias(), False, "INITIAL_HOURLY_BOUNDARY_REQUIRED")
            if frame.prior_result_id is not None:
                return DailyBiasUpdate(default_unconfigured_bias(), False, "INITIAL_BIAS_PRIOR_RESULT_FORBIDDEN")
        else:
            current_boundary = _time(current.boundary_end, "Current bias boundary")
            if boundary < current_boundary:
                return DailyBiasUpdate(current, False, "OUT_OF_ORDER_BIAS_FRAME_IGNORED")
            if boundary == current_boundary:
                if frame.frame_hash == current.frame_hash:
                    return DailyBiasUpdate(current, False, "DUPLICATE_BIAS_FRAME_IGNORED")
                if reason not in {
                    BiasUpdateReason.DATA_INVALIDATION,
                    BiasUpdateReason.OPTIONS_OVERLAY_CHANGE,
                }:
                    return DailyBiasUpdate(current, False, "INTRA_HOUR_BIAS_UPDATE_DENIED")
                if frame.prior_result_id != current.result_id:
                    return DailyBiasUpdate(current, False, "BIAS_PRIOR_RESULT_MISMATCH")
                if reason is BiasUpdateReason.OPTIONS_OVERLAY_CHANGE:
                    prior_components = tuple(item.evidence_hash for item in current.components)
                    next_components = tuple(
                        item.evidence_hash
                        for item in sorted(frame.components, key=lambda value: value.component.value)
                    )
                    if prior_components != next_components:
                        return DailyBiasUpdate(current, False, "OPTIONS_UPDATE_CHANGED_CORE_EVIDENCE")
            else:
                if reason is not BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY:
                    return DailyBiasUpdate(current, False, "NEW_BOUNDARY_REASON_INVALID")
                if frame.prior_result_id != current.result_id:
                    return DailyBiasUpdate(current, False, "BIAS_PRIOR_RESULT_MISMATCH")

        continuity = frame.continuity_valid
        if current is not None and boundary > _time(current.boundary_end, "Current bias boundary"):
            continuity = continuity and (
                _time(frame.boundary_start, "Bias boundary start")
                == _time(current.boundary_end, "Current bias boundary")
            )
        result = self._evaluate(frame, reason, continuity_valid=continuity)
        self._current = result
        return DailyBiasUpdate(result, True, "BIAS_UPDATED")

    @staticmethod
    def _evaluate(
        frame: HourlyBiasFrame,
        reason: BiasUpdateReason,
        *,
        continuity_valid: bool,
    ) -> DailyBiasResult:
        decision = _time(frame.decision_at, "Bias decision time")
        components = tuple(sorted(frame.components, key=lambda value: value.component.value))
        counts = {
            DailyBiasDirection.LONG.value: 0,
            DailyBiasDirection.SHORT.value: 0,
            DailyBiasDirection.NEUTRAL.value: 0,
            DailyBiasDirection.UNAVAILABLE.value: 0,
        }
        for item in components:
            counts[item.direction.value] += 1
        configured = all(item.configured for item in components)
        usable = all(
            item.valid
            and item.direction is not DailyBiasDirection.UNAVAILABLE
            and _time(item.provenance.valid_until, "Component validity") >= decision
            for item in components
        )
        reasons: list[str] = []
        direction = DailyBiasDirection.UNAVAILABLE
        confidence = BiasConfidenceTier.NONE

        if not continuity_valid:
            reasons.append("HOURLY_BIAS_CONTINUITY_GAP")
        elif not configured:
            reasons.append("CORE_COMPONENT_UNCONFIGURED")
        elif not usable:
            reasons.append("CORE_COMPONENT_STALE_OR_UNAVAILABLE")
        elif any(item.veto for item in components):
            direction = DailyBiasDirection.NEUTRAL
            reasons.append("CORE_STRONG_CONTRARY_VETO")
        else:
            long_votes = counts[DailyBiasDirection.LONG.value]
            short_votes = counts[DailyBiasDirection.SHORT.value]
            if long_votes >= 2:
                direction = DailyBiasDirection.LONG
                confidence = (
                    BiasConfidenceTier.CORE_3_OF_3
                    if long_votes == 3 else BiasConfidenceTier.CORE_2_OF_3
                )
                reasons.append(f"CORE_LONG_{long_votes}_OF_3")
            elif short_votes >= 2:
                direction = DailyBiasDirection.SHORT
                confidence = (
                    BiasConfidenceTier.CORE_3_OF_3
                    if short_votes == 3 else BiasConfidenceTier.CORE_2_OF_3
                )
                reasons.append(f"CORE_SHORT_{short_votes}_OF_3")
            else:
                direction = DailyBiasDirection.NEUTRAL
                reasons.append("CORE_NO_TWO_OF_THREE_AGREEMENT")

        overlay = frame.options
        options_available = _time(overlay.provenance.valid_until, "Options validity") >= decision
        if overlay.outcome is OptionsOverlayOutcome.UNAVAILABLE or not options_available:
            if overlay.required:
                direction = DailyBiasDirection.UNAVAILABLE
                confidence = BiasConfidenceTier.NONE
                reasons.append("REQUIRED_OPTIONS_CONTEXT_UNAVAILABLE")
            elif overlay.experimental_nonvoting_authority == PONG_TRIAL_OPTIONS_AUTHORITY:
                reasons.append("OPTIONS_UNAVAILABLE_NON_VOTING_EXPERIMENT_ONLY")
        elif overlay.outcome is OptionsOverlayOutcome.VETO and direction is not DailyBiasDirection.UNAVAILABLE:
            direction = DailyBiasDirection.NEUTRAL
            confidence = BiasConfidenceTier.NONE
            reasons.append("OPTIONS_VETO")
        elif overlay.outcome is OptionsOverlayOutcome.CAUTION:
            if confidence is BiasConfidenceTier.CORE_2_OF_3:
                direction = DailyBiasDirection.NEUTRAL
                confidence = BiasConfidenceTier.NONE
                reasons.append("OPTIONS_CAUTION_DENIED_MARGINAL_CORE")
            elif confidence is BiasConfidenceTier.CORE_3_OF_3:
                confidence = BiasConfidenceTier.CORE_3_OF_3_OPTIONS_CAUTION
                reasons.append("OPTIONS_CAUTION_REDUCED_CONFIDENCE")
        elif overlay.outcome is OptionsOverlayOutcome.CONFIRM:
            if direction in {DailyBiasDirection.LONG, DailyBiasDirection.SHORT}:
                if overlay.direction is direction:
                    confidence = (
                        BiasConfidenceTier.CORE_3_OF_3_OPTIONS_CONFIRMED
                        if confidence is BiasConfidenceTier.CORE_3_OF_3
                        else BiasConfidenceTier.CORE_2_OF_3_OPTIONS_CONFIRMED
                    )
                    reasons.append("OPTIONS_CONFIRMED_CORE_DIRECTION")
                else:
                    direction = DailyBiasDirection.NEUTRAL
                    confidence = BiasConfidenceTier.NONE
                    reasons.append("OPTIONS_CONFIRM_DIRECTION_CONFLICT")
        else:
            reasons.append("OPTIONS_NOT_APPLICABLE")

        expiries = [
            _time(item.provenance.valid_until, "Core validity") for item in components
        ]
        expiries.append(_time(overlay.provenance.valid_until, "Options validity"))
        expires_at = min(expiries).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        body = {
            "schema": PONG_DAILY_BIAS_SCHEMA,
            "calculation_id": PONG_DAILY_BIAS_CALCULATION_ID,
            "calculation_version": PONG_DAILY_BIAS_CALCULATION_VERSION,
            "direction": direction.value,
            "confidence": confidence.value,
            "instrument": frame.instrument,
            "boundary_start": normalized_utc(frame.boundary_start, "Bias boundary start"),
            "boundary_end": normalized_utc(frame.boundary_end, "Bias boundary end"),
            "available_at": normalized_utc(frame.decision_at, "Bias decision time"),
            "expires_at": expires_at,
            "vote_counts": counts,
            "reason_codes": reasons,
            "components": [item.payload() for item in components],
            "options": overlay.payload(),
            "update_reason": reason.value,
            "prior_result_id": frame.prior_result_id,
            "frame_hash": frame.frame_hash,
            "configured": configured,
            "production_configured": PONG_DAILY_BIAS_PRODUCTION_CONFIGURED,
            "live_capital": "DENIED",
        }
        result_id = "pong-bias-" + canonical_hash(body)[:32]
        return DailyBiasResult(
            result_id=result_id,
            direction=direction,
            confidence=confidence,
            instrument=frame.instrument,
            boundary_start=frame.boundary_start,
            boundary_end=frame.boundary_end,
            available_at=frame.decision_at,
            expires_at=expires_at,
            vote_counts=tuple((name, counts[name]) for name in sorted(counts)),
            reason_codes=tuple(reasons),
            components=components,
            options=overlay,
            update_reason=reason,
            prior_result_id=frame.prior_result_id,
            frame_hash=frame.frame_hash,
            configured=configured,
        )


@dataclass(frozen=True)
class HourlyOrderFlowPolicy:
    """Unoptimized design parameters; no production adapter selects this policy."""

    calculation_id: str = "pong-hourly-authenticated-classified-flow"
    calculation_version: str = "0"
    minimum_classified_trades: int = 8
    minimum_absolute_imbalance: Decimal = Decimal("0.10")
    maximum_publication_lag_seconds: int = 300
    freshness_seconds_from_boundary: int = 3900

    def __post_init__(self) -> None:
        _text(self.calculation_id, "Order-flow calculation ID")
        _text(self.calculation_version, "Order-flow calculation version")
        if type(self.minimum_classified_trades) is not int or self.minimum_classified_trades < 2:
            raise ValueError("Hourly order flow requires at least two classified trades.")
        threshold = _decimal(self.minimum_absolute_imbalance, "Order-flow imbalance")
        if threshold <= 0 or threshold >= 1:
            raise ValueError("Order-flow imbalance must be between zero and one.")
        if (
            type(self.maximum_publication_lag_seconds) is not int
            or type(self.freshness_seconds_from_boundary) is not int
            or self.maximum_publication_lag_seconds <= 0
            or self.freshness_seconds_from_boundary <= 3600
            or self.freshness_seconds_from_boundary <= self.maximum_publication_lag_seconds
        ):
            raise ValueError("Order-flow timing limits are invalid.")

    def payload(self) -> dict[str, object]:
        return {
            "calculation_id": self.calculation_id,
            "calculation_version": self.calculation_version,
            "aggregation_interval": "COMPLETED_UTC_HOUR",
            "observations": "AUTHENTICATED_CLASSIFIED_TRADES",
            "minimum_classified_trades": self.minimum_classified_trades,
            "minimum_absolute_imbalance": str(self.minimum_absolute_imbalance),
            "maximum_publication_lag_seconds": self.maximum_publication_lag_seconds,
            "freshness_seconds_from_boundary": self.freshness_seconds_from_boundary,
            "production_configured": False,
        }

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


PONG_HOURLY_ORDER_FLOW_POLICY_V0 = HourlyOrderFlowPolicy()


@dataclass(frozen=True)
class ClassifiedTradeInput:
    event_id: str
    payload_hash: str
    source: str
    instrument: str
    side: ClassifiedSide
    size: Decimal
    event_at: str
    observed_at: str
    available_at: str
    authenticated: bool

    def __post_init__(self) -> None:
        _text(self.event_id, "Classified trade ID")
        _sha256(self.payload_hash, "Classified trade hash")
        _text(self.source, "Classified trade source")
        if self.instrument != PONG_DAILY_BIAS_INSTRUMENT:
            raise ValueError("Classified trade instrument mismatch.")
        if type(self.side) is not ClassifiedSide:
            raise ValueError("Classified trade side is required.")
        if type(self.authenticated) is not bool:
            raise ValueError("Classified trade authentication state must be a boolean.")
        _decimal(self.size, "Classified trade size", positive=True)
        event = _time(self.event_at, "Classified trade event time")
        observed = _time(self.observed_at, "Classified trade observed time")
        available = _time(self.available_at, "Classified trade available-at time")
        if event > observed or observed > available:
            raise ValueError("Classified trade provenance is non-causal.")


@dataclass(frozen=True)
class CompletedHourlyOrderFlow:
    interval_start: str
    interval_end: str
    published_at: str
    coverage_start: str
    coverage_end: str
    continuity_valid: bool
    trades: tuple[ClassifiedTradeInput, ...]


class HourlyOrderFlowEvaluator:
    """Aggregate authenticated aggressor-classified trades over one completed UTC hour."""

    def __init__(self, policy: HourlyOrderFlowPolicy = PONG_HOURLY_ORDER_FLOW_POLICY_V0) -> None:
        self.policy = policy

    def evaluate(self, frame: CompletedHourlyOrderFlow) -> CoreComponentEvidence:
        if type(frame.continuity_valid) is not bool:
            raise ValueError("Order-flow continuity state must be a boolean.")
        start = _time(frame.interval_start, "Order-flow interval start")
        end = _time(frame.interval_end, "Order-flow interval end")
        published = _time(frame.published_at, "Order-flow publication time")
        if end - start != timedelta(hours=1) or any((end.minute, end.second, end.microsecond)):
            raise ValueError("Order flow requires an exact completed UTC hour.")
        if published < end or published > end + timedelta(seconds=self.policy.maximum_publication_lag_seconds):
            raise ValueError("Order-flow publication is outside the causal lag bound.")
        if _time(frame.coverage_start, "Order-flow coverage start") > start or _time(
            frame.coverage_end, "Order-flow coverage end"
        ) < end:
            raise ValueError("Order-flow coverage does not prove the complete hour.")
        if len({item.event_id for item in frame.trades}) != len(frame.trades):
            raise ValueError("Duplicate classified trade IDs are forbidden.")
        for item in frame.trades:
            event = _time(item.event_at, "Classified trade event time")
            if not start <= event < end:
                raise ValueError("Order-flow trade lies outside the completed hour.")
            if _time(item.available_at, "Classified trade available-at time") > published:
                raise ValueError("Order-flow trade was not available at publication.")
            if not item.authenticated:
                raise ValueError("Hourly order flow requires authenticated classified trades.")

        latest = max((_time(item.event_at, "Trade event") for item in frame.trades), default=start)
        observed = max((_time(item.observed_at, "Trade observed") for item in frame.trades), default=end)
        valid_until = end + timedelta(seconds=self.policy.freshness_seconds_from_boundary)
        source_names = sorted({item.source for item in frame.trades}) or ["NO_AUTHENTICATED_INPUT"]
        if frame.trades:
            input_ids = tuple(item.event_id for item in frame.trades)
            input_hashes = tuple(item.payload_hash for item in frame.trades)
        else:
            empty_input = {
                "kind": "EMPTY_AUTHENTICATED_CLASSIFIED_TRADE_SET",
                "interval_start": normalized_utc(frame.interval_start, "Order-flow interval start"),
                "interval_end": normalized_utc(frame.interval_end, "Order-flow interval end"),
            }
            input_ids = ("empty-authenticated-classified-trade-set",)
            input_hashes = (canonical_hash(empty_input),)
        provenance = BiasInputProvenance(
            source="+".join(source_names),
            calculation_id=self.policy.calculation_id,
            calculation_version=self.policy.calculation_version,
            calculation_hash=self.policy.configuration_hash,
            window_start=frame.interval_start,
            window_end=frame.interval_end,
            latest_event_at=latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            observed_at=observed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            available_at=frame.published_at,
            valid_until=valid_until.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            input_ids=input_ids,
            input_hashes=input_hashes,
        )
        if not frame.continuity_valid:
            return CoreComponentEvidence(
                CoreComponentKind.ORDER_FLOW,
                DailyBiasDirection.UNAVAILABLE,
                True,
                False,
                False,
                ("ORDER_FLOW_CONTINUITY_INVALID",),
                provenance,
            )
        if len(frame.trades) < self.policy.minimum_classified_trades:
            return CoreComponentEvidence(
                CoreComponentKind.ORDER_FLOW,
                DailyBiasDirection.UNAVAILABLE,
                True,
                False,
                False,
                ("ORDER_FLOW_MINIMUM_EVIDENCE_NOT_MET",),
                provenance,
                (("classified_trade_count", str(len(frame.trades))),),
            )
        buy = sum((item.size for item in frame.trades if item.side is ClassifiedSide.BUY), Decimal("0"))
        sell = sum((item.size for item in frame.trades if item.side is ClassifiedSide.SELL), Decimal("0"))
        total = buy + sell
        imbalance = (buy - sell) / total
        threshold = self.policy.minimum_absolute_imbalance
        direction = (
            DailyBiasDirection.LONG
            if imbalance >= threshold
            else DailyBiasDirection.SHORT
            if imbalance <= -threshold
            else DailyBiasDirection.NEUTRAL
        )
        reason = {
            DailyBiasDirection.LONG: "HOURLY_CLASSIFIED_BUY_IMBALANCE",
            DailyBiasDirection.SHORT: "HOURLY_CLASSIFIED_SELL_IMBALANCE",
            DailyBiasDirection.NEUTRAL: "HOURLY_CLASSIFIED_FLOW_NEUTRAL",
        }[direction]
        return CoreComponentEvidence(
            CoreComponentKind.ORDER_FLOW,
            direction,
            True,
            True,
            False,
            (reason,),
            provenance,
            (
                ("buy_volume", str(buy)),
                ("sell_volume", str(sell)),
                ("total_volume", str(total)),
                ("signed_imbalance", str(imbalance)),
                ("classified_trade_count", str(len(frame.trades))),
            ),
        )


def unconfigured_component(
    component: CoreComponentKind,
    *,
    boundary_start: str,
    boundary_end: str,
    available_at: str,
) -> CoreComponentEvidence:
    """Return a provenance-complete denial without inventing component semantics."""
    interface = {
        "component": component.value,
        "status": "UNCONFIGURED",
        "schema": PONG_DAILY_BIAS_SCHEMA,
    }
    valid_until = _time(boundary_end, "Boundary end") + timedelta(hours=1, minutes=5)
    provenance = BiasInputProvenance(
        source="UNCONFIGURED",
        calculation_id=f"pong-{component.value.lower()}-interface",
        calculation_version="0",
        calculation_hash=canonical_hash(interface),
        window_start=boundary_start,
        window_end=boundary_end,
        latest_event_at=boundary_end,
        observed_at=available_at,
        available_at=available_at,
        valid_until=valid_until.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        input_ids=(f"{component.value.lower()}-unconfigured",),
        input_hashes=(canonical_hash(interface),),
    )
    return CoreComponentEvidence(
        component,
        DailyBiasDirection.UNAVAILABLE,
        False,
        False,
        False,
        (f"{component.value}_UNCONFIGURED",),
        provenance,
    )


def unconfigured_options_overlay(
    *, boundary_start: str, boundary_end: str, available_at: str,
) -> OptionsOverlayEvidence:
    interface = {"component": "OPTIONS_OVERLAY", "status": "UNCONFIGURED", "schema": PONG_DAILY_BIAS_SCHEMA}
    valid_until = _time(boundary_end, "Boundary end") + timedelta(hours=1, minutes=5)
    return OptionsOverlayEvidence(
        day_class=OptionsDayClass.UNKNOWN,
        outcome=OptionsOverlayOutcome.UNAVAILABLE,
        direction=None,
        required=True,
        reason_codes=("OPTIONS_DAY_CLASSIFICATION_AND_GREEKS_UNCONFIGURED",),
        provenance=BiasInputProvenance(
            source="UNCONFIGURED",
            calculation_id="pong-options-overlay-interface",
            calculation_version="0",
            calculation_hash=canonical_hash(interface),
            window_start=boundary_start,
            window_end=boundary_end,
            latest_event_at=boundary_end,
            observed_at=available_at,
            available_at=available_at,
            valid_until=valid_until.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            input_ids=("options-overlay-unconfigured",),
            input_hashes=(canonical_hash(interface),),
        ),
    )


def default_unconfigured_bias() -> DailyBiasResult:
    start = "1970-01-01T00:00:00Z"
    end = "1970-01-01T01:00:00Z"
    available = "1970-01-01T01:00:00Z"
    components = tuple(
        unconfigured_component(
            component,
            boundary_start=start,
            boundary_end=end,
            available_at=available,
        )
        for component in CoreComponentKind
    )
    options = unconfigured_options_overlay(
        boundary_start=start,
        boundary_end=end,
        available_at=available,
    )
    frame = HourlyBiasFrame(
        PONG_DAILY_BIAS_INSTRUMENT,
        start,
        end,
        available,
        components,
        options,
        False,
        None,
    )
    return DailyBiasEngine._evaluate(
        frame,
        BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        continuity_valid=False,
    )
