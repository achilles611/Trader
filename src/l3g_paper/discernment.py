"""Pure, causal, execution-disabled directional Discernment core for Pong.

The engine consumes already-derived research evidence.  It does not learn an
evidence mapping, read a clock, inspect a position, submit an order, or confer
execution authority.  A caller must invoke :meth:`on_evidence` for each newly
admitted causal observation; maintenance callbacks can invalidate a snapshot
but can never manufacture a logical switch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import math
import re
from typing import Iterable

from src.lane_iii.contracts import canonical_hash, normalized_utc
from src.shared.confidence_math import (
    LOG_ODDS_CLIP,
    attainable_effective_range,
    logit_probability,
    shrink_toward_neutral,
    sigmoid_clipped,
    unit_interval,
)


DISCERNMENT_SCHEMA = "beelzebub-pong-discernment-v1"
DISCERNMENT_MODEL_ID = "pong-discernment-research-log-odds"
DISCERNMENT_MODEL_VERSION = "1"
DISCERNMENT_EXECUTION_ENABLED = False
DISCERNMENT_PRODUCTION_CONFIGURED = False
DISCERNMENT_INSTRUMENT = "MNQ SEP26"
DISCERNMENT_CONTRACT = "MNQU6"
EXPERIMENTAL_DIRECTIONAL_SUPPORT = "EXPERIMENTAL_DIRECTIONAL_SUPPORT"
STRONG_THRESHOLD_UNREACHABLE = "STRONG_THRESHOLD_UNREACHABLE_UNDER_CURRENT_CONFIGURATION"


class DiscernmentDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class DiscernmentValidity(StrEnum):
    VALID = "VALID"
    UNAVAILABLE = "UNAVAILABLE"


class VolumeRegime(StrEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    UNAVAILABLE = "UNAVAILABLE"


class MaintenanceEventKind(StrEnum):
    TIME = "TIME"
    EXPIRY = "EXPIRY"
    DAILY_BIAS = "DAILY_BIAS"
    FILL = "FILL"


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _utc(value: object, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _finite(value: object, field: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite.") from exc
    if not math.isfinite(converted):
        raise ValueError(f"{field} must be finite.")
    return converted


@dataclass(frozen=True)
class DiscernmentEvidence:
    """One replacement-style component result with a causal envelope."""

    event_id: str
    component_id: str
    component_version: str
    dependency_group: str
    log_odds_effect: float
    required: bool
    source_event_at: str
    received_at: str
    available_at: str
    valid_until: str
    evidence_revision: int
    continuity_domain: str
    continuity_valid: bool
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, field in (
            (self.event_id, "Evidence event ID"),
            (self.component_id, "Evidence component ID"),
            (self.component_version, "Evidence component version"),
            (self.dependency_group, "Evidence dependency group"),
            (self.continuity_domain, "Evidence continuity domain"),
        ):
            _required_text(value, field)
        _finite(self.log_odds_effect, "Evidence log-odds effect")
        if type(self.required) is not bool or type(self.continuity_valid) is not bool:
            raise ValueError("Evidence state flags must be booleans.")
        if type(self.evidence_revision) is not int or self.evidence_revision <= 0:
            raise ValueError("Evidence revision must be a positive integer.")
        event_at = _utc(self.source_event_at, "Evidence source event time")
        received_at = _utc(self.received_at, "Evidence receipt time")
        available_at = _utc(self.available_at, "Evidence available-at time")
        valid_until = _utc(self.valid_until, "Evidence valid-until time")
        if event_at > received_at or received_at > available_at:
            raise ValueError("Evidence timing is non-causal.")
        if valid_until <= available_at:
            raise ValueError("Evidence must still be valid when it becomes available.")
        if not self.source_ids or len(self.source_ids) != len(self.source_hashes):
            raise ValueError("Evidence source IDs and hashes must be non-empty and aligned.")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Evidence source IDs must be unique.")
        if tuple(sorted(zip(self.source_ids, self.source_hashes))) != tuple(
            zip(self.source_ids, self.source_hashes)
        ):
            raise ValueError("Evidence source IDs and hashes must be deterministically ordered.")
        for source_id in self.source_ids:
            _required_text(source_id, "Evidence source ID")
        for source_hash in self.source_hashes:
            _sha256(source_hash, "Evidence source hash")

    @property
    def component_key(self) -> tuple[str, str]:
        return self.component_id, self.component_version

    @property
    def evidence_hash(self) -> str:
        return canonical_hash(self.payload())

    @property
    def semantic_hash(self) -> str:
        """Identity of source facts, excluding delivery/replay envelope fields."""
        return canonical_hash(
            {
                "component_id": self.component_id,
                "component_version": self.component_version,
                "dependency_group": self.dependency_group,
                "log_odds_effect": self.log_odds_effect,
                "required": self.required,
                "source_event_at": normalized_utc(self.source_event_at, "Evidence source event time"),
                "source_ids": list(self.source_ids),
                "source_hashes": list(self.source_hashes),
            }
        )

    def payload(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "component_id": self.component_id,
            "component_version": self.component_version,
            "dependency_group": self.dependency_group,
            "log_odds_effect": self.log_odds_effect,
            "required": self.required,
            "source_event_at": normalized_utc(self.source_event_at, "Evidence source event time"),
            "received_at": normalized_utc(self.received_at, "Evidence receipt time"),
            "available_at": normalized_utc(self.available_at, "Evidence available-at time"),
            "valid_until": normalized_utc(self.valid_until, "Evidence valid-until time"),
            "evidence_revision": self.evidence_revision,
            "continuity_domain": self.continuity_domain,
            "continuity_valid": self.continuity_valid,
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
        }


@dataclass(frozen=True)
class ComponentEffect:
    component_id: str
    component_version: str
    dependency_group: str
    log_odds_effect: float
    event_id: str
    source_event_at: str
    available_at: str
    source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, field in (
            (self.component_id, "Component effect ID"),
            (self.component_version, "Component effect version"),
            (self.dependency_group, "Component effect dependency group"),
            (self.event_id, "Component effect event ID"),
        ):
            _required_text(value, field)
        _finite(self.log_odds_effect, "Component log-odds effect")
        if not self.source_ids or tuple(sorted(self.source_ids)) != self.source_ids:
            raise ValueError("Component source IDs must be non-empty and sorted.")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Component source IDs must be unique.")
        if _utc(self.source_event_at, "Component source event time") > _utc(
            self.available_at, "Component available-at time"
        ):
            raise ValueError("Component effect timing is non-causal.")

    def payload(self) -> dict[str, object]:
        return {
            "component_id": self.component_id,
            "component_version": self.component_version,
            "dependency_group": self.dependency_group,
            "log_odds_effect": self.log_odds_effect,
            "event_id": self.event_id,
            "source_event_at": normalized_utc(self.source_event_at, "Component source event time"),
            "available_at": normalized_utc(self.available_at, "Component available-at time"),
            "source_ids": list(self.source_ids),
        }


@dataclass(frozen=True)
class DiscernmentReachability:
    minimum_effective_long_score: float
    maximum_effective_long_score: float
    strong_long_reachable: bool
    strong_short_reachable: bool
    reason_code: str | None


@dataclass(frozen=True)
class DiscernmentPolicy:
    """Frozen research-only score and hysteresis contract."""

    prior_long_score: float = 0.5
    reliability: float = 0.70
    long_on_threshold: float = 0.65
    short_on_threshold: float = 0.35
    strong_threshold: float = 0.85
    required_components: tuple[tuple[str, str], ...] = ()
    dependency_group_bounds: tuple[tuple[str, float], ...] = ()
    model_id: str = DISCERNMENT_MODEL_ID
    model_version: str = DISCERNMENT_MODEL_VERSION
    calibration_status: str = EXPERIMENTAL_DIRECTIONAL_SUPPORT
    production_configured: bool = DISCERNMENT_PRODUCTION_CONFIGURED

    def __post_init__(self) -> None:
        unit_interval(self.prior_long_score, "prior_long_score")
        unit_interval(self.reliability, "reliability")
        long_on = unit_interval(self.long_on_threshold, "long_on_threshold")
        short_on = unit_interval(self.short_on_threshold, "short_on_threshold")
        strong = unit_interval(self.strong_threshold, "strong_threshold")
        if not 0 < short_on < 0.5 < long_on < 1 or not math.isclose(
            short_on, 1 - long_on, rel_tol=0, abs_tol=1e-12
        ):
            raise ValueError("Discernment thresholds must be mirrored around 0.5.")
        if strong <= long_on:
            raise ValueError("Strong support must be above the LONG switch threshold.")
        if type(self.production_configured) is not bool:
            raise ValueError("Production configured state must be a boolean.")
        _required_text(self.model_id, "Discernment model ID")
        _required_text(self.model_version, "Discernment model version")
        if self.calibration_status != EXPERIMENTAL_DIRECTIONAL_SUPPORT:
            raise ValueError("The Pong research fixture is not a calibrated probability model.")
        if len(set(self.required_components)) != len(self.required_components):
            raise ValueError("Required component identities must be unique.")
        for component_id, component_version in self.required_components:
            _required_text(component_id, "Required component ID")
            _required_text(component_version, "Required component version")
        groups = [group for group, _ in self.dependency_group_bounds]
        if len(groups) != len(set(groups)):
            raise ValueError("Dependency group bounds must be unique.")
        for group, bound in self.dependency_group_bounds:
            _required_text(group, "Dependency group")
            if _finite(bound, "Dependency group bound") <= 0:
                raise ValueError("Dependency group bound must be positive.")

    @classmethod
    def research_fixture(
        cls,
        *,
        required_components: Iterable[tuple[str, str]] = (),
        dependency_group_bounds: Iterable[tuple[str, float]] = (),
    ) -> "DiscernmentPolicy":
        return cls(
            required_components=tuple(required_components),
            dependency_group_bounds=tuple(dependency_group_bounds),
        )

    @property
    def model_hash(self) -> str:
        return canonical_hash(
            {
                "model_id": self.model_id,
                "model_version": self.model_version,
                "prior_long_score": self.prior_long_score,
                "reliability": self.reliability,
                "required_components": [list(value) for value in sorted(self.required_components)],
                "dependency_group_bounds": [
                    [group, bound] for group, bound in sorted(self.dependency_group_bounds)
                ],
                "calibration_status": self.calibration_status,
            }
        )

    @property
    def policy_hash(self) -> str:
        return canonical_hash(
            {
                "schema": DISCERNMENT_SCHEMA,
                "model_hash": self.model_hash,
                "long_on_threshold": self.long_on_threshold,
                "short_on_threshold": self.short_on_threshold,
                "strong_threshold": self.strong_threshold,
                "production_configured": self.production_configured,
                "execution_enabled": DISCERNMENT_EXECUTION_ENABLED,
            }
        )

    @property
    def reachability(self) -> DiscernmentReachability:
        minimum, maximum = attainable_effective_range(self.reliability)
        long_reachable = maximum >= self.strong_threshold
        short_reachable = (1 - minimum) >= self.strong_threshold
        return DiscernmentReachability(
            minimum,
            maximum,
            long_reachable,
            short_reachable,
            None if long_reachable and short_reachable else STRONG_THRESHOLD_UNREACHABLE,
        )


@dataclass(frozen=True)
class DiscernmentSnapshot:
    instrument: str
    contract: str
    as_of: str
    received_at: str
    available_at: str
    evidence_revision: int
    continuity_domain: str
    continuity_valid: bool
    raw_long_score: float | None
    effective_long_score: float | None
    long_discernment_pct: float | None
    short_discernment_pct: float | None
    reliability: float
    calibration_status: str
    component_effects: tuple[ComponentEffect, ...]
    dependency_group_effects: tuple[tuple[str, float], ...]
    volume_regime: VolumeRegime
    source_ids: tuple[str, ...]
    policy_hash: str
    model_hash: str
    validity: DiscernmentValidity
    reason_codes: tuple[str, ...]
    result_hash: str
    execution_enabled: bool = DISCERNMENT_EXECUTION_ENABLED

    def __post_init__(self) -> None:
        if self.instrument != DISCERNMENT_INSTRUMENT or self.contract != DISCERNMENT_CONTRACT:
            raise ValueError("Discernment snapshot instrument/contract mismatch.")
        as_of = _utc(self.as_of, "Snapshot as-of time")
        received_at = _utc(self.received_at, "Snapshot receipt time")
        available_at = _utc(self.available_at, "Snapshot available-at time")
        if as_of > received_at or received_at > available_at:
            raise ValueError("Discernment snapshot timing is non-causal.")
        if type(self.evidence_revision) is not int or self.evidence_revision < 0:
            raise ValueError("Snapshot evidence revision must be nonnegative.")
        _required_text(self.continuity_domain, "Snapshot continuity domain")
        if type(self.continuity_valid) is not bool or type(self.execution_enabled) is not bool:
            raise ValueError("Snapshot state flags must be booleans.")
        if self.execution_enabled:
            raise ValueError("The Discernment core cannot enable execution.")
        unit_interval(self.reliability, "snapshot reliability")
        if self.calibration_status != EXPERIMENTAL_DIRECTIONAL_SUPPORT:
            raise ValueError("Discernment snapshot calibration status is invalid.")
        if type(self.volume_regime) is not VolumeRegime or type(self.validity) is not DiscernmentValidity:
            raise ValueError("Discernment snapshot enum state is invalid.")
        _sha256(self.policy_hash, "Discernment policy hash")
        _sha256(self.model_hash, "Discernment model hash")
        _sha256(self.result_hash, "Discernment result hash")
        if not self.reason_codes or any(not str(reason).strip() for reason in self.reason_codes):
            raise ValueError("Discernment snapshot reason codes are required.")
        if tuple(sorted(self.source_ids)) != self.source_ids or len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Snapshot source IDs must be sorted and unique.")
        component_keys = [(item.component_id, item.component_version) for item in self.component_effects]
        if component_keys != sorted(component_keys) or len(component_keys) != len(set(component_keys)):
            raise ValueError("Snapshot component effects must be sorted and unique.")
        group_names = [name for name, _ in self.dependency_group_effects]
        if group_names != sorted(group_names) or len(group_names) != len(set(group_names)):
            raise ValueError("Snapshot dependency groups must be sorted and unique.")
        for _, effect in self.dependency_group_effects:
            _finite(effect, "Snapshot dependency group effect")
        scores = (
            self.raw_long_score,
            self.effective_long_score,
            self.long_discernment_pct,
            self.short_discernment_pct,
        )
        if self.validity is DiscernmentValidity.UNAVAILABLE:
            if any(value is not None for value in scores):
                raise ValueError("Unavailable Discernment must not expose neutral-looking scores.")
        else:
            if any(value is None for value in scores):
                raise ValueError("Valid Discernment requires raw, effective, and display scores.")
            raw = unit_interval(self.raw_long_score, "raw_long_score")  # type: ignore[arg-type]
            effective = unit_interval(self.effective_long_score, "effective_long_score")  # type: ignore[arg-type]
            long_pct = _finite(self.long_discernment_pct, "long_discernment_pct")  # type: ignore[arg-type]
            short_pct = _finite(self.short_discernment_pct, "short_discernment_pct")  # type: ignore[arg-type]
            if not 0 <= long_pct <= 100 or not 0 <= short_pct <= 100:
                raise ValueError("Discernment display percentages must be in [0, 100].")
            if not math.isclose(long_pct, effective * 100, rel_tol=0, abs_tol=1e-12):
                raise ValueError("LONG display percentage must derive from the effective score.")
            if not math.isclose(short_pct, (1 - effective) * 100, rel_tol=0, abs_tol=1e-12):
                raise ValueError("SHORT display percentage must mirror LONG support.")
            if not math.isclose(
                effective,
                shrink_toward_neutral(raw, self.reliability),
                rel_tol=0,
                abs_tol=1e-15,
            ):
                raise ValueError("Effective Discernment must be reliability-shrunk raw support.")
        if canonical_hash(self.payload(include_result_hash=False)) != self.result_hash:
            raise ValueError("Discernment snapshot result hash mismatch.")

    @property
    def valid(self) -> bool:
        return self.validity is DiscernmentValidity.VALID

    def payload(self, *, include_result_hash: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": DISCERNMENT_SCHEMA,
            "instrument": self.instrument,
            "contract": self.contract,
            "as_of": normalized_utc(self.as_of, "Snapshot as-of time"),
            "received_at": normalized_utc(self.received_at, "Snapshot receipt time"),
            "available_at": normalized_utc(self.available_at, "Snapshot available-at time"),
            "evidence_revision": self.evidence_revision,
            "continuity_domain": self.continuity_domain,
            "continuity_valid": self.continuity_valid,
            "raw_long_score": self.raw_long_score,
            "effective_long_score": self.effective_long_score,
            "long_discernment_pct": self.long_discernment_pct,
            "short_discernment_pct": self.short_discernment_pct,
            "reliability": self.reliability,
            "calibration_status": self.calibration_status,
            "component_effects": [item.payload() for item in self.component_effects],
            "dependency_group_effects": [list(item) for item in self.dependency_group_effects],
            "volume_regime": self.volume_regime.value,
            "source_ids": list(self.source_ids),
            "policy_hash": self.policy_hash,
            "model_hash": self.model_hash,
            "validity": self.validity.value,
            "reason_codes": list(self.reason_codes),
            "execution_enabled": self.execution_enabled,
        }
        if include_result_hash:
            payload["result_hash"] = self.result_hash
        return payload


@dataclass(frozen=True)
class DiscernmentSwitch:
    event_id: str
    sequence: int
    prior_switch_id: str | None
    previous_direction: DiscernmentDirection
    direction: DiscernmentDirection
    occurred_at: str
    available_at: str
    continuity_valid: bool
    continuity_domain: str
    snapshot: DiscernmentSnapshot

    def __post_init__(self) -> None:
        if not self.event_id.startswith("pong-discernment-switch-"):
            raise ValueError("Discernment switch ID is invalid.")
        if type(self.sequence) is not int or self.sequence <= 0:
            raise ValueError("Discernment switch sequence must be positive.")
        if (
            type(self.previous_direction) is not DiscernmentDirection
            or type(self.direction) is not DiscernmentDirection
        ):
            raise ValueError("Discernment switch directions are invalid.")
        if self.previous_direction is self.direction:
            raise ValueError("Discernment switch requires a distinct direction transition.")
        if normalized_utc(self.occurred_at, "Switch occurrence time") != normalized_utc(
            self.available_at, "Switch available-at time"
        ):
            raise ValueError("Discernment switches occur only when evidence becomes available.")
        if (
            type(self.continuity_valid) is not bool
            or not self.continuity_valid
            or not self.snapshot.valid
        ):
            raise ValueError("Discernment switch requires valid continuous evidence.")
        if self.continuity_domain != self.snapshot.continuity_domain:
            raise ValueError("Discernment switch continuity domain mismatch.")
        if normalized_utc(self.available_at, "Switch available-at time") != self.snapshot.available_at:
            raise ValueError("Discernment switch must bind its causal snapshot.")

    @property
    def switch_id(self) -> str:
        return self.event_id

    @property
    def effective_long_score(self) -> float:
        assert self.snapshot.effective_long_score is not None
        return self.snapshot.effective_long_score

    def payload(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "switch_id": self.switch_id,
            "sequence": self.sequence,
            "prior_switch_id": self.prior_switch_id,
            "previous_direction": self.previous_direction.value,
            "direction": self.direction.value,
            "occurred_at": normalized_utc(self.occurred_at, "Switch occurrence time"),
            "available_at": normalized_utc(self.available_at, "Switch available-at time"),
            "continuity_valid": self.continuity_valid,
            "continuity_domain": self.continuity_domain,
            "snapshot_hash": self.snapshot.result_hash,
            "effective_long_score": self.effective_long_score,
        }


@dataclass(frozen=True)
class DiscernmentUpdate:
    snapshot: DiscernmentSnapshot
    switch: DiscernmentSwitch | None
    admitted: bool
    reason_code: str


class DiscernmentEngine:
    """Deterministic replacement-set scorer and hysteretic direction latch."""

    def __init__(
        self,
        policy: DiscernmentPolicy | None = None,
        *,
        instrument: str = DISCERNMENT_INSTRUMENT,
        contract: str = DISCERNMENT_CONTRACT,
    ) -> None:
        self.policy = policy or DiscernmentPolicy.research_fixture()
        if instrument != DISCERNMENT_INSTRUMENT or contract != DISCERNMENT_CONTRACT:
            raise ValueError("Discernment is sealed to verified MNQ SEP26 / MNQU6.")
        self.instrument = instrument
        self.contract = contract
        self._active: dict[tuple[str, str], DiscernmentEvidence] = {}
        self._seen_event_hashes: dict[str, str] = {}
        self._continuity_domain: str | None = None
        self._continuity_valid = True
        self._evidence_revision = 0
        self._latest_available_at: str | None = None
        self._latched_direction: DiscernmentDirection | None = None
        self._switch_sequence = 0
        self._last_switch_id: str | None = None
        self._last_snapshot: DiscernmentSnapshot | None = None
        self._volume_regime = VolumeRegime.UNAVAILABLE

    @property
    def latched_direction(self) -> DiscernmentDirection | None:
        return self._latched_direction

    @property
    def last_snapshot(self) -> DiscernmentSnapshot | None:
        return self._last_snapshot

    @property
    def last_switch_id(self) -> str | None:
        return self._last_switch_id

    @property
    def switch_sequence(self) -> int:
        return self._switch_sequence

    def on_evidence(
        self,
        evidence: DiscernmentEvidence,
        *,
        volume_regime: VolumeRegime = VolumeRegime.UNAVAILABLE,
    ) -> DiscernmentUpdate:
        if type(evidence) is not DiscernmentEvidence:
            raise ValueError("Discernment requires validated DiscernmentEvidence.")
        if type(volume_regime) is not VolumeRegime:
            raise ValueError("Discernment volume regime is invalid.")
        event_hash = evidence.evidence_hash
        seen_hash = self._seen_event_hashes.get(evidence.event_id)
        if seen_hash is not None:
            if seen_hash == event_hash:
                return self._current_update("DUPLICATE_EVIDENCE_IGNORED")
            return self._invalidate(evidence.available_at, "EVIDENCE_EVENT_IDENTITY_CONFLICT")

        available = normalized_utc(evidence.available_at, "Evidence available-at time")
        if self._latest_available_at is not None and available < self._latest_available_at:
            return self._current_update("OUT_OF_ORDER_EVIDENCE_IGNORED")

        if self._continuity_domain is not None and evidence.continuity_domain != self._continuity_domain:
            self._active.clear()
            self._latched_direction = None
            self._continuity_valid = True
            self._evidence_revision = 0
        self._continuity_domain = evidence.continuity_domain

        if not evidence.continuity_valid:
            self._seen_event_hashes[evidence.event_id] = event_hash
            self._latest_available_at = available
            self._evidence_revision = evidence.evidence_revision
            self._continuity_valid = False
            return self._snapshot_update(available, "EVIDENCE_CONTINUITY_GAP", admitted=False)

        if not self._continuity_valid:
            return self._current_update("CONTINUITY_DOMAIN_REQUIRES_RESEED")
        if self._evidence_revision and evidence.evidence_revision != self._evidence_revision + 1:
            if evidence.evidence_revision <= self._evidence_revision:
                return self._current_update("OUT_OF_ORDER_EVIDENCE_IGNORED")
            self._continuity_valid = False
            self._seen_event_hashes[evidence.event_id] = event_hash
            self._latest_available_at = available
            self._evidence_revision = evidence.evidence_revision
            return self._snapshot_update(available, "EVIDENCE_REVISION_GAP", admitted=False)

        required_by_policy = evidence.component_key in set(self.policy.required_components)
        if evidence.required != required_by_policy:
            return self._invalidate(available, "EVIDENCE_REQUIRED_FLAG_MISMATCH")
        bounds = dict(self.policy.dependency_group_bounds)
        if evidence.dependency_group not in bounds:
            return self._invalidate(available, "DEPENDENCY_GROUP_BOUND_MISSING")

        prior = self._active.get(evidence.component_key)
        self._seen_event_hashes[evidence.event_id] = event_hash
        self._latest_available_at = available
        self._evidence_revision = evidence.evidence_revision
        self._volume_regime = volume_regime
        if prior is not None and prior.semantic_hash == evidence.semantic_hash:
            return self._snapshot_update(available, "IDENTICAL_EVIDENCE_NO_DECISION", admitted=False)

        self._active[evidence.component_key] = evidence
        return self._snapshot_update(available, "EVIDENCE_ADMITTED", admitted=True, may_switch=True)

    def on_time(self, available_at: str) -> DiscernmentUpdate:
        return self._maintenance(MaintenanceEventKind.TIME, available_at)

    def on_expiry(self, available_at: str) -> DiscernmentUpdate:
        return self._maintenance(MaintenanceEventKind.EXPIRY, available_at)

    def on_bias_update(self, available_at: str) -> DiscernmentUpdate:
        return self._maintenance(MaintenanceEventKind.DAILY_BIAS, available_at)

    def on_fill(self, available_at: str) -> DiscernmentUpdate:
        return self._maintenance(MaintenanceEventKind.FILL, available_at)

    def _maintenance(
        self, kind: MaintenanceEventKind, available_at: str,
    ) -> DiscernmentUpdate:
        available = normalized_utc(available_at, f"{kind.value} available-at time")
        if self._latest_available_at is not None and available < self._latest_available_at:
            return self._current_update(f"OUT_OF_ORDER_{kind.value}_IGNORED")
        self._latest_available_at = available
        return self._snapshot_update(
            available,
            f"{kind.value}_CALLBACK_NO_SWITCH",
            admitted=False,
            may_switch=False,
        )

    def _current_update(self, reason: str) -> DiscernmentUpdate:
        if self._last_snapshot is None:
            raise ValueError("Discernment has no snapshot before its first causal event.")
        return DiscernmentUpdate(self._last_snapshot, None, False, reason)

    def _invalidate(self, available_at: str, reason: str) -> DiscernmentUpdate:
        self._continuity_valid = False
        self._latest_available_at = normalized_utc(available_at, "Invalidation available-at time")
        return self._snapshot_update(self._latest_available_at, reason, admitted=False)

    def _snapshot_update(
        self,
        available_at: str,
        reason: str,
        *,
        admitted: bool,
        may_switch: bool = False,
    ) -> DiscernmentUpdate:
        snapshot = self._make_snapshot(available_at, additional_reason=reason)
        self._last_snapshot = snapshot
        switch = self._consider_switch(snapshot) if may_switch else None
        return DiscernmentUpdate(snapshot, switch, admitted, reason)

    def _make_snapshot(self, available_at: str, *, additional_reason: str) -> DiscernmentSnapshot:
        decision_time = _utc(available_at, "Snapshot available-at time")
        active = [
            item
            for item in self._active.values()
            if _utc(item.valid_until, "Evidence valid-until time") > decision_time
        ]
        active.sort(key=lambda item: item.component_key)
        active_keys = {item.component_key for item in active}
        missing = sorted(set(self.policy.required_components) - active_keys)
        reasons: list[str] = [additional_reason]
        if not self._continuity_valid:
            reasons.append("CONTINUITY_INVALID")
        if missing:
            reasons.extend(f"MISSING_REQUIRED_EVIDENCE:{item[0]}@{item[1]}" for item in missing)
        if not active:
            reasons.append("NO_ACTIVE_EVIDENCE")

        components = tuple(
            ComponentEffect(
                item.component_id,
                item.component_version,
                item.dependency_group,
                float(item.log_odds_effect),
                item.event_id,
                item.source_event_at,
                item.available_at,
                item.source_ids,
            )
            for item in active
        )
        grouped: dict[str, float] = {}
        for item in active:
            grouped[item.dependency_group] = grouped.get(item.dependency_group, 0.0) + float(
                item.log_odds_effect
            )
        bounds = dict(self.policy.dependency_group_bounds)
        group_effects = tuple(
            (group, max(-bounds[group], min(bounds[group], effect)))
            for group, effect in sorted(grouped.items())
        )

        valid = self._continuity_valid and not missing and bool(active)
        if valid:
            log_odds = logit_probability(self.policy.prior_long_score, name="prior_long_score")
            for _, effect in group_effects:
                log_odds += effect
            raw = sigmoid_clipped(log_odds, clip=LOG_ODDS_CLIP)
            effective = shrink_toward_neutral(raw, self.policy.reliability)
            long_pct = effective * 100
            short_pct = (1 - effective) * 100
            validity = DiscernmentValidity.VALID
        else:
            raw = effective = long_pct = short_pct = None
            validity = DiscernmentValidity.UNAVAILABLE

        if active:
            as_of = max(normalized_utc(item.source_event_at, "Evidence source event time") for item in active)
            received_at = max(normalized_utc(item.received_at, "Evidence receipt time") for item in active)
            source_ids = tuple(sorted({source_id for item in active for source_id in item.source_ids}))
        else:
            as_of = received_at = normalized_utc(available_at, "Snapshot available-at time")
            source_ids = ()
        payload_without_hash: dict[str, object] = {
            "schema": DISCERNMENT_SCHEMA,
            "instrument": self.instrument,
            "contract": self.contract,
            "as_of": as_of,
            "received_at": received_at,
            "available_at": normalized_utc(available_at, "Snapshot available-at time"),
            "evidence_revision": self._evidence_revision,
            "continuity_domain": self._continuity_domain or "UNESTABLISHED",
            "continuity_valid": self._continuity_valid,
            "raw_long_score": raw,
            "effective_long_score": effective,
            "long_discernment_pct": long_pct,
            "short_discernment_pct": short_pct,
            "reliability": self.policy.reliability,
            "calibration_status": self.policy.calibration_status,
            "component_effects": [item.payload() for item in components],
            "dependency_group_effects": [list(item) for item in group_effects],
            "volume_regime": self._volume_regime.value,
            "source_ids": list(source_ids),
            "policy_hash": self.policy.policy_hash,
            "model_hash": self.policy.model_hash,
            "validity": validity.value,
            "reason_codes": reasons,
            "execution_enabled": DISCERNMENT_EXECUTION_ENABLED,
        }
        return DiscernmentSnapshot(
            self.instrument,
            self.contract,
            as_of,
            received_at,
            normalized_utc(available_at, "Snapshot available-at time"),
            self._evidence_revision,
            self._continuity_domain or "UNESTABLISHED",
            self._continuity_valid,
            raw,
            effective,
            long_pct,
            short_pct,
            self.policy.reliability,
            self.policy.calibration_status,
            components,
            group_effects,
            self._volume_regime,
            source_ids,
            self.policy.policy_hash,
            self.policy.model_hash,
            validity,
            tuple(reasons),
            canonical_hash(payload_without_hash),
        )

    def _consider_switch(self, snapshot: DiscernmentSnapshot) -> DiscernmentSwitch | None:
        if not snapshot.valid or snapshot.effective_long_score is None:
            return None
        score = snapshot.effective_long_score
        candidate: DiscernmentDirection | None = None
        if score >= self.policy.long_on_threshold:
            candidate = DiscernmentDirection.LONG
        elif score <= self.policy.short_on_threshold:
            candidate = DiscernmentDirection.SHORT
        if candidate is None or candidate is self._latched_direction:
            return None
        if self._latched_direction is None:
            self._latched_direction = candidate
            return None

        previous = self._latched_direction
        sequence = self._switch_sequence + 1
        material = {
            "schema": DISCERNMENT_SCHEMA,
            "sequence": sequence,
            "prior_switch_id": self._last_switch_id,
            "previous_direction": previous.value,
            "direction": candidate.value,
            "snapshot_hash": snapshot.result_hash,
            "evidence_revision": snapshot.evidence_revision,
            "policy_hash": snapshot.policy_hash,
            "continuity_domain": snapshot.continuity_domain,
        }
        event_id = "pong-discernment-switch-" + canonical_hash(material)[:32]
        switch = DiscernmentSwitch(
            event_id,
            sequence,
            self._last_switch_id,
            previous,
            candidate,
            snapshot.available_at,
            snapshot.available_at,
            snapshot.continuity_valid,
            snapshot.continuity_domain,
            snapshot,
        )
        self._latched_direction = candidate
        self._switch_sequence = sequence
        self._last_switch_id = event_id
        return switch
