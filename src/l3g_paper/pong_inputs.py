"""Authenticated, causal NinjaTrader inputs for the bounded Pong candidate.

This module is an admission boundary only.  It has no order, controller,
profile-selection, or runtime-start surface.  Legacy unsigned L3-F2 records
remain available to existing observers, but they can never pass this gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Mapping

from src.lane_iii.contracts import canonical_hash, normalized_utc
from src.l3f_provider.ninjatrader_observation import (
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
)

from .pong_experiment import CompletedPongHour


PONG_NATIVE_INPUT_POLICY_VERSION = "pong-native-input-admission-v1"
PONG_HOURLY_WARM_START_POLICY_VERSION = "pong-hourly-warm-start-v1"
PONG_HISTORICAL_ORDER_FLOW_STATUS = "UNAVAILABLE_NO_AUTHENTIC_AGGRESSOR_SEQUENCE"
PONG_HOURLY_WARM_START_POLICY_HASH = canonical_hash({
    "schema": PONG_HOURLY_WARM_START_POLICY_VERSION,
    "instrument": "MNQ SEP26",
    "canonical_contract": "MNQU6",
    "source": "NINJATRADER_BARS_REQUEST_PROVIDER",
    "bar_period_minutes": 60,
    "required_completed_intervals": 2,
    "alignment": "UTC_HOUR",
    "future_data": "DENIED",
    "merge_policy": "DO_NOT_MERGE",
    "historical_to_live_overlap": "IDENTICAL_ONLY_DEDUPLICATED",
})

_SOURCE_STREAMS = {
    "TRADE": "MARKET_DATA_LAST",
    "QUOTE": "MARKET_DATA_BID_ASK",
    "DEPTH": "MARKET_DEPTH",
    "HISTORICAL_WARM_START": "HISTORICAL_BARS",
    "HOURLY_BAR": "HISTORICAL_BARS",
}
_PUBLICATION_POLICIES = {
    "TRADE": frozenset({"EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1"}),
    "QUOTE": frozenset({"BOUNDED_LATEST_STATE_2HZ", "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1"}),
    "DEPTH": frozenset({"EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1"}),
    "HISTORICAL_WARM_START": frozenset({"CAUSAL_HISTORICAL_COMPLETED_HOUR_V1"}),
    "HOURLY_BAR": frozenset({"CAUSAL_LIVE_COMPLETED_HOUR_V1"}),
}


class PongNativeInputRefused(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(code + (":" + detail if detail else ""))


def _time(value: object, name: str) -> datetime:
    try:
        normalized = normalized_utc(value, name)
    except ValueError as exc:
        raise PongNativeInputRefused("PONG_INPUT_TIMESTAMP_INVALID", name) from exc
    return datetime.fromisoformat(normalized.replace("Z", "+00:00"))


def _wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", name)
    return value


def _integer(value: object, name: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", name)
    return value


def _decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", name) from exc
    if not result.is_finite() or result <= 0:
        raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", name)
    return result


def _require_verified(record: NinjaTraderObservation) -> None:
    if type(record) is not NinjaTraderObservation:
        raise PongNativeInputRefused("PONG_NATIVE_INPUT_UNTYPED")
    if (
        record.schema != L3F3_SCHEMA
        or record.signature_verified is not True
        or record.producer is None
        or record.producer.protocol_version != NINJATRADER_ATTESTATION_PROTOCOL
    ):
        raise PongNativeInputRefused("PONG_NATIVE_INPUT_ATTESTATION_UNVERIFIED")
    expected_stream = _SOURCE_STREAMS.get(record.observation_type)
    allowed_policies = _PUBLICATION_POLICIES.get(record.observation_type)
    if expected_stream is None or record.producer.source_stream != expected_stream:
        raise PongNativeInputRefused("PONG_NATIVE_INPUT_SOURCE_STREAM_MISMATCH")
    if allowed_policies is None or record.producer.publication_policy not in allowed_policies:
        raise PongNativeInputRefused("PONG_NATIVE_INPUT_PUBLICATION_POLICY_REFUSED")
    if record.payload.get("publication_policy") != record.producer.publication_policy:
        raise PongNativeInputRefused("PONG_NATIVE_INPUT_PUBLICATION_POLICY_MISMATCH")


@dataclass(frozen=True)
class PongWarmStartCoverage:
    instrument: str
    source: str
    requested_at: str
    earliest_required: str
    latest_included_timestamp: str
    expected_intervals: int
    actual_intervals: int
    gaps: tuple[str, ...]
    timezone_name: str
    day_anchor: str
    policy_version: str
    policy_hash: str
    producer_session_id: str
    producer_source_fingerprint: str
    producer_build_fingerprint: str
    observation_id: str
    observation_payload_hash: str

    @property
    def complete(self) -> bool:
        return self.expected_intervals == self.actual_intervals == 2 and not self.gaps

    def payload(self) -> dict[str, object]:
        return {
            "instrument": self.instrument,
            "source": self.source,
            "requested_at": self.requested_at,
            "earliest_required": self.earliest_required,
            "latest_included_timestamp": self.latest_included_timestamp,
            "expected_intervals": self.expected_intervals,
            "actual_intervals": self.actual_intervals,
            "gaps": list(self.gaps),
            "timezone": self.timezone_name,
            "day_anchor": self.day_anchor,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "producer_session_id": self.producer_session_id,
            "producer_source_fingerprint": self.producer_source_fingerprint,
            "producer_build_fingerprint": self.producer_build_fingerprint,
            "observation_id": self.observation_id,
            "observation_payload_hash": self.observation_payload_hash,
            "complete": self.complete,
            "historical_order_flow": PONG_HISTORICAL_ORDER_FLOW_STATUS,
        }

    @property
    def coverage_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class PongWarmStart:
    coverage: PongWarmStartCoverage
    hours: tuple[CompletedPongHour, ...]

    @classmethod
    def from_observation(cls, record: NinjaTraderObservation) -> "PongWarmStart":
        _require_verified(record)
        if record.observation_type != "HISTORICAL_WARM_START":
            raise PongNativeInputRefused("PONG_WARM_START_TYPE_REFUSED")
        payload = record.payload
        requested = _time(payload.get("requested_at"), "Warm-start request time")
        earliest = _time(payload.get("earliest_required"), "Warm-start earliest required time")
        latest = _time(payload.get("latest_included_timestamp"), "Warm-start latest included time")
        expected = _integer(payload.get("expected_intervals"), "Warm-start expected intervals", positive=True)
        actual = _integer(payload.get("actual_intervals"), "Warm-start actual intervals")
        gaps_value = payload.get("gaps")
        bars_value = payload.get("bars")
        if not isinstance(gaps_value, list) or not all(isinstance(item, str) for item in gaps_value):
            raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", "Warm-start gaps")
        if not isinstance(bars_value, list):
            raise PongNativeInputRefused("PONG_INPUT_SHAPE_INVALID", "Warm-start bars")
        if (
            payload.get("status") != "COMPLETE"
            or payload.get("contract_id") != "MNQ SEP26"
            or payload.get("source") != "NINJATRADER_BARS_REQUEST_PROVIDER"
            or payload.get("timezone") != "UTC"
            or payload.get("policy_version") != PONG_HOURLY_WARM_START_POLICY_VERSION
            or payload.get("policy_hash") != PONG_HOURLY_WARM_START_POLICY_HASH
            or expected != 2
            or actual != len(bars_value)
            or actual != 2
            or gaps_value
        ):
            raise PongNativeInputRefused("PONG_HOURLY_WARM_START_INCOMPLETE")
        expected_latest = requested.replace(minute=0, second=0, microsecond=0)
        expected_earliest = expected_latest - timedelta(hours=2)
        if earliest != expected_earliest or latest != expected_latest:
            raise PongNativeInputRefused("PONG_HOURLY_WARM_START_COVERAGE_MISMATCH")
        if payload.get("day_anchor") != expected_earliest.date().isoformat():
            raise PongNativeInputRefused("PONG_HOURLY_WARM_START_DAY_ANCHOR_MISMATCH")

        continuity_domain = (
            "NINJATRADER_HOURLY:"
            + record.session_id + ":"
            + record.producer.source_fingerprint + ":"
            + record.producer.build_fingerprint
        )
        hours: list[CompletedPongHour] = []
        for index, raw_bar in enumerate(bars_value):
            bar = _mapping(raw_bar, f"Warm-start bar {index}")
            start = _time(bar.get("start"), "Warm-start bar start")
            end = _time(bar.get("end"), "Warm-start bar end")
            expected_start = expected_earliest + timedelta(hours=index)
            if start != expected_start or end != start + timedelta(hours=1) or end > requested:
                raise PongNativeInputRefused("PONG_HOURLY_WARM_START_BAR_MISMATCH")
            bar_core = {
                "start": _wire(start), "end": _wire(end),
                "open": str(bar.get("open")), "high": str(bar.get("high")),
                "low": str(bar.get("low")), "close": str(bar.get("close")),
                "volume": bar.get("volume"),
            }
            bar_hash = canonical_hash(bar_core)
            hours.append(CompletedPongHour(
                start=_wire(start), end=_wire(end), available_at=_wire(requested),
                open=_decimal(bar.get("open"), "Warm-start open"),
                high=_decimal(bar.get("high"), "Warm-start high"),
                low=_decimal(bar.get("low"), "Warm-start low"),
                close=_decimal(bar.get("close"), "Warm-start close"),
                source="NINJATRADER_BARS_REQUEST_PROVIDER",
                continuity_domain=continuity_domain,
                source_ids=(record.observation_id + f":hour:{index}",),
                source_hashes=(bar_hash,), complete=True,
            ))
        coverage = PongWarmStartCoverage(
            instrument="MNQ SEP26", source="NINJATRADER_BARS_REQUEST_PROVIDER",
            requested_at=_wire(requested), earliest_required=_wire(earliest),
            latest_included_timestamp=_wire(latest), expected_intervals=expected,
            actual_intervals=actual, gaps=tuple(gaps_value), timezone_name="UTC",
            day_anchor=str(payload["day_anchor"]), policy_version=PONG_HOURLY_WARM_START_POLICY_VERSION,
            policy_hash=PONG_HOURLY_WARM_START_POLICY_HASH,
            producer_session_id=record.session_id,
            producer_source_fingerprint=record.producer.source_fingerprint,
            producer_build_fingerprint=record.producer.build_fingerprint,
            observation_id=record.observation_id,
            observation_payload_hash=str(record.payload_hash),
        )
        return cls(coverage, tuple(hours))


@dataclass(frozen=True)
class PongSequenceCoverage:
    session_id: str
    source_fingerprint: str
    build_fingerprint: str
    sequences: Mapping[str, int]
    gap_free: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "sequences", MappingProxyType(dict(self.sequences)))


class PongCausalInputAdapter:
    """Linearize verified native events and the historical-to-live hour seam."""

    def __init__(self, warm_start: PongWarmStart) -> None:
        if type(warm_start) is not PongWarmStart or not warm_start.coverage.complete:
            raise PongNativeInputRefused("PONG_HOURLY_WARM_START_INCOMPLETE")
        self.warm_start = warm_start
        self._session_id = warm_start.coverage.producer_session_id
        self._source_fingerprint = warm_start.coverage.producer_source_fingerprint
        self._build_fingerprint = warm_start.coverage.producer_build_fingerprint
        self._last_sequences: dict[str, int] = {}
        self._seen_observations: set[str] = {warm_start.coverage.observation_id}
        self._hours: dict[str, CompletedPongHour] = {hour.start: hour for hour in warm_start.hours}

    def _identity(self, record: NinjaTraderObservation) -> None:
        _require_verified(record)
        if (
            record.session_id != self._session_id
            or record.producer.source_fingerprint != self._source_fingerprint
            or record.producer.build_fingerprint != self._build_fingerprint
        ):
            raise PongNativeInputRefused("PONG_NATIVE_INPUT_PRODUCER_CHANGED")

    def admit_market_observation(self, record: NinjaTraderObservation) -> bool:
        self._identity(record)
        if record.observation_type not in {"TRADE", "QUOTE", "DEPTH"}:
            raise PongNativeInputRefused("PONG_NATIVE_MARKET_INPUT_TYPE_REFUSED")
        if record.observation_id in self._seen_observations:
            return False
        sequence = _integer(record.payload.get("source_event_sequence"), "Source event sequence", positive=True)
        previous = self._last_sequences.get(record.observation_type)
        expected = 1 if previous is None else previous + 1
        if sequence < expected:
            raise PongNativeInputRefused("PONG_NATIVE_INPUT_DUPLICATE_OR_OUT_OF_ORDER")
        if sequence > expected:
            raise PongNativeInputRefused(
                "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP",
                f"{record.observation_type}:{expected}:{sequence}",
            )
        self._last_sequences[record.observation_type] = sequence
        self._seen_observations.add(record.observation_id)
        return True

    def admit_live_hour(self, record: NinjaTraderObservation) -> CompletedPongHour | None:
        self._identity(record)
        if record.observation_type != "HOURLY_BAR":
            raise PongNativeInputRefused("PONG_LIVE_HOUR_TYPE_REFUSED")
        if record.observation_id in self._seen_observations:
            return None
        payload = record.payload
        if (
            payload.get("contract_id") != "MNQ SEP26"
            or payload.get("source") != "NINJATRADER_BARS_REQUEST_PROVIDER"
            or payload.get("policy_version") != PONG_HOURLY_WARM_START_POLICY_VERSION
            or payload.get("policy_hash") != PONG_HOURLY_WARM_START_POLICY_HASH
        ):
            raise PongNativeInputRefused("PONG_LIVE_HOUR_PROVENANCE_MISMATCH")
        start = _time(payload.get("start"), "Live hour start")
        end = _time(payload.get("end"), "Live hour end")
        observed = _time(record.ninja_receipt_time, "Live hour observed time")
        if (
            end != start + timedelta(hours=1)
            or any((start.minute, start.second, start.microsecond))
            or end > observed
            or observed > end + timedelta(seconds=300)
        ):
            raise PongNativeInputRefused("PONG_LIVE_HOUR_CAUSALITY_REFUSED")
        key = _wire(start)
        bar_core = {
            "start": _wire(start), "end": _wire(end),
            "open": str(payload.get("open")), "high": str(payload.get("high")),
            "low": str(payload.get("low")), "close": str(payload.get("close")),
            "volume": payload.get("volume"),
        }
        hour = CompletedPongHour(
            start=key, end=_wire(end), available_at=_wire(observed),
            open=_decimal(payload.get("open"), "Live hour open"),
            high=_decimal(payload.get("high"), "Live hour high"),
            low=_decimal(payload.get("low"), "Live hour low"),
            close=_decimal(payload.get("close"), "Live hour close"),
            source=self.warm_start.coverage.source,
            continuity_domain=self.warm_start.hours[-1].continuity_domain,
            source_ids=(record.observation_id,),
            source_hashes=(canonical_hash(bar_core),), complete=True,
        )
        existing = self._hours.get(key)
        if existing is not None:
            if existing.result_hash != hour.result_hash:
                raise PongNativeInputRefused("PONG_HISTORICAL_LIVE_OVERLAP_MISMATCH")
            self._seen_observations.add(record.observation_id)
            return None
        latest = max(_time(item.end, "Existing hour end") for item in self._hours.values())
        if start != latest:
            raise PongNativeInputRefused("PONG_HISTORICAL_LIVE_HOUR_GAP")
        self._hours[key] = hour
        self._seen_observations.add(record.observation_id)
        return hour

    def latest_hour_pair(self) -> tuple[CompletedPongHour, CompletedPongHour]:
        hours = sorted(self._hours.values(), key=lambda item: _time(item.start, "Pong hour start"))
        if len(hours) < 2 or _time(hours[-2].end, "Prior hour end") != _time(hours[-1].start, "Current hour start"):
            raise PongNativeInputRefused("PONG_HOURLY_PAIR_UNAVAILABLE")
        return hours[-2], hours[-1]

    @property
    def sequence_coverage(self) -> PongSequenceCoverage:
        return PongSequenceCoverage(
            self._session_id, self._source_fingerprint, self._build_fingerprint,
            self._last_sequences, True,
        )
