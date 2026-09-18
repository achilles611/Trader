"""Pure, causal, execution-disabled directional Discernment core for Pong.

The engine consumes already-derived research evidence.  It does not learn an
evidence mapping, read a clock, inspect a position, submit an order, or confer
execution authority.  A caller must invoke :meth:`on_evidence` for each newly
admitted causal observation; maintenance callbacks can invalidate a snapshot
but can never manufacture a logical switch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import math
import re
from typing import Iterable

from src.lane_iii.contracts import canonical_hash
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


@dataclass(frozen=True)
class ResearchEvidenceEffect:
    """One explicitly versioned, uncalibrated research-state mapping."""

    component_id: str
    component_version: str
    dependency_group: str
    state: str
    volume_regime: VolumeRegime
    log_odds_effect: float
    required: bool

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (
                self.component_id,
                self.component_version,
                self.dependency_group,
                self.state,
            )
        ):
            raise ValueError("Research evidence mapping identity fields are required.")
        if type(self.volume_regime) is not VolumeRegime:
            raise ValueError("Research evidence mapping volume regime is invalid.")
        if not math.isfinite(float(self.log_odds_effect)):
            raise ValueError("Research evidence mapping effect must be finite.")
        if type(self.required) is not bool:
            raise ValueError("Research evidence mapping required flag must be boolean.")

    def payload(self) -> dict[str, object]:
        return {
            "component_id": self.component_id,
            "component_version": self.component_version,
            "dependency_group": self.dependency_group,
            "state": self.state,
            "volume_regime": self.volume_regime.value,
            "log_odds_effect": self.log_odds_effect,
            "required": self.required,
        }


@dataclass(frozen=True)
class ResearchEvidenceMapping:
    """Frozen synthetic hypothesis mapping; never a calibrated probability."""

    mapping_id: str
    mapping_version: str
    effects: tuple[ResearchEvidenceEffect, ...]
    dependency_group_bounds: tuple[tuple[str, float], ...]
    calibration_status: str = EXPERIMENTAL_DIRECTIONAL_SUPPORT
    production_configured: bool = False

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (self.mapping_id, self.mapping_version)
        ):
            raise ValueError("Research evidence mapping identity is required.")
        if self.calibration_status != EXPERIMENTAL_DIRECTIONAL_SUPPORT:
            raise ValueError("The frozen mapping is not a calibrated probability model.")
        if self.production_configured or DISCERNMENT_EXECUTION_ENABLED:
            raise ValueError("The research evidence mapping cannot enable execution.")
        effect_keys = [
            (
                item.component_id,
                item.component_version,
                item.state,
                item.volume_regime.value,
            )
            for item in self.effects
        ]
        if not effect_keys or len(effect_keys) != len(set(effect_keys)):
            raise ValueError("Research evidence mapping entries must be non-empty and unique.")
        groups = [group for group, _ in self.dependency_group_bounds]
        if len(groups) != len(set(groups)):
            raise ValueError("Research evidence dependency group bounds must be unique.")
        for group, bound in self.dependency_group_bounds:
            if not isinstance(group, str) or not group.strip():
                raise ValueError("Research evidence dependency group is required.")
            if not math.isfinite(float(bound)) or float(bound) <= 0:
                raise ValueError("Research evidence dependency group bound must be positive.")
        if {item.dependency_group for item in self.effects} - set(groups):
            raise ValueError("Every research evidence dependency group requires a bound.")

    @property
    def mapping_hash(self) -> str:
        return canonical_hash(self.payload())

    @property
    def required_components(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                {
                    (effect.component_id, effect.component_version)
                    for effect in self.effects
                    if effect.required
                }
            )
        )

    def effect_for(
        self,
        component_id: str,
        component_version: str,
        state: str,
        volume_regime: VolumeRegime,
    ) -> float:
        matches = [
            item.log_odds_effect
            for item in self.effects
            if (
                item.component_id == component_id
                and item.component_version == component_version
                and item.state == state
                and item.volume_regime is volume_regime
            )
        ]
        if len(matches) != 1:
            raise ValueError("Research evidence state mapping is unavailable or ambiguous.")
        return matches[0]

    def payload(self) -> dict[str, object]:
        return {
            "mapping_id": self.mapping_id,
            "mapping_version": self.mapping_version,
            "effects": [
                item.payload()
                for item in sorted(
                    self.effects,
                    key=lambda value: (
                        value.component_id,
                        value.component_version,
                        value.state,
                        value.volume_regime.value,
                    ),
                )
            ],
            "dependency_group_bounds": [
                [group, bound] for group, bound in sorted(self.dependency_group_bounds)
            ],
            "calibration_status": self.calibration_status,
            "production_configured": self.production_configured,
            "execution_enabled": DISCERNMENT_EXECUTION_ENABLED,
        }


_RESEARCH_EFFECT_ROWS = (
    # LOW gives validated flow/interaction hypotheses more influence than the
    # weak structural fixture.  Group bounds still prevent independent votes.
    ("CLASSIFIED_FLOW", "BUY_PROGRESS", 0.90, 0.75),
    ("CLASSIFIED_FLOW", "BUY_ABSORBED", -0.45, -0.40),
    ("CLASSIFIED_FLOW", "SELL_PROGRESS", -0.90, -0.75),
    ("CLASSIFIED_FLOW", "SELL_ABSORBED", 0.45, 0.40),
    ("INTERACTING_LIQUIDITY", "LONG_INTERACTION", 0.75, 0.60),
    ("INTERACTING_LIQUIDITY", "SHORT_INTERACTION", -0.75, -0.60),
    ("WEAK_STRUCTURE", "LONG", 0.10, 0.25),
    ("WEAK_STRUCTURE", "SHORT", -0.10, -0.25),
)

PONG_RESEARCH_EVIDENCE_MAPPING = ResearchEvidenceMapping(
    mapping_id="pong-discernment-synthetic-research-mapping",
    mapping_version="1",
    effects=tuple(
        ResearchEvidenceEffect(
            component_id,
            "1",
            (
                "FLOW_AND_INTERACTION_SHARED"
                if component_id != "WEAK_STRUCTURE"
                else "WEAK_STRUCTURE"
            ),
            state,
            regime,
            low_effect if regime is VolumeRegime.LOW else normal_effect,
            component_id in {"CLASSIFIED_FLOW", "INTERACTING_LIQUIDITY"},
        )
        for component_id, state, low_effect, normal_effect in _RESEARCH_EFFECT_ROWS
        for regime in (VolumeRegime.LOW, VolumeRegime.NORMAL)
    ),
    dependency_group_bounds=(
        ("FLOW_AND_INTERACTION_SHARED", 1.25),
        ("WEAK_STRUCTURE", 0.35),
    ),
)


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


_PRECISE_UTC = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<clock>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<offset>Z|[+-]\d{2}:\d{2})$"
)


def normalized_utc(value: object, field: str) -> str:
    """Canonical UTC text without silently discarding sub-microsecond input.

    Discernment identities are text based, so a lexical comparison of an exact
    second and a fractional second is not chronological.  The canonical form
    accepts the existing explicit-UTC-offset grammar, normalizes the offset,
    and retains up to nanosecond fractional precision (including 100 ns input).
    Numerically equal representations have one canonical value.
    """
    if not isinstance(value, str):
        raise ValueError(f"{field} must be ISO-8601 text with an explicit UTC offset.")
    matched = _PRECISE_UTC.fullmatch(value)
    if matched is None:
        raise ValueError(f"{field} must be ISO-8601 text with an explicit UTC offset.")
    offset = "+00:00" if matched["offset"] == "Z" else matched["offset"]
    try:
        whole = datetime.fromisoformat(
            matched["date"] + "T" + matched["clock"] + offset,
        )
    except ValueError as error:
        raise ValueError(f"{field} must be ISO-8601 text with an explicit UTC offset.") from error
    whole = whole.astimezone(timezone.utc)
    fraction = (matched["fraction"] or "").rstrip("0")
    return whole.strftime("%Y-%m-%dT%H:%M:%S") + (
        "" if not fraction else "." + fraction
    ) + "Z"


def _utc(value: object, field: str) -> str:
    """Fixed-width chronological key for valid canonical UTC timestamp text."""
    canonical = normalized_utc(value, field)
    second, separator, fractional = canonical[:-1].partition(".")
    return second + "." + (fractional if separator else "").ljust(9, "0") + "Z"


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
    received_at: str
    available_at: str
    valid_until: str
    evidence_revision: int
    continuity_domain: str
    continuity_valid: bool
    required: bool
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    evidence_hash: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.component_id, "Component effect ID"),
            (self.component_version, "Component effect version"),
            (self.dependency_group, "Component effect dependency group"),
            (self.event_id, "Component effect event ID"),
            (self.continuity_domain, "Component effect continuity domain"),
        ):
            _required_text(value, field)
        _finite(self.log_odds_effect, "Component log-odds effect")
        if type(self.evidence_revision) is not int or self.evidence_revision <= 0:
            raise ValueError("Component evidence revision must be positive.")
        if type(self.continuity_valid) is not bool or type(self.required) is not bool:
            raise ValueError("Component evidence state flags must be booleans.")
        source_at = _utc(self.source_event_at, "Component source event time")
        received_at = _utc(self.received_at, "Component receipt time")
        available_at = _utc(self.available_at, "Component available-at time")
        valid_until = _utc(self.valid_until, "Component valid-until time")
        if source_at > received_at or received_at > available_at or available_at >= valid_until:
            raise ValueError("Component effect timing is non-causal.")
        if not self.source_ids or len(self.source_ids) != len(self.source_hashes):
            raise ValueError("Component source IDs and hashes must be non-empty and aligned.")
        if tuple(sorted(zip(self.source_ids, self.source_hashes))) != tuple(
            zip(self.source_ids, self.source_hashes)
        ):
            raise ValueError("Component source identities must be deterministically ordered.")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Component source IDs must be unique.")
        for source_hash in self.source_hashes:
            _sha256(source_hash, "Component source hash")
        _sha256(self.evidence_hash, "Component evidence hash")

    def payload(self) -> dict[str, object]:
        return {
            "component_id": self.component_id,
            "component_version": self.component_version,
            "dependency_group": self.dependency_group,
            "log_odds_effect": self.log_odds_effect,
            "event_id": self.event_id,
            "source_event_at": normalized_utc(self.source_event_at, "Component source event time"),
            "received_at": normalized_utc(self.received_at, "Component receipt time"),
            "available_at": normalized_utc(self.available_at, "Component available-at time"),
            "valid_until": normalized_utc(self.valid_until, "Component valid-until time"),
            "evidence_revision": self.evidence_revision,
            "continuity_domain": self.continuity_domain,
            "continuity_valid": self.continuity_valid,
            "required": self.required,
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
            "evidence_hash": self.evidence_hash,
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
    evidence_mapping_hash: str | None = None

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
        if self.evidence_mapping_hash is not None:
            _sha256(self.evidence_mapping_hash, "Discernment evidence mapping hash")
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

    @classmethod
    def frozen_research_mapping(cls) -> "DiscernmentPolicy":
        """Use the explicit v1 hypothesis mapping without enabling operation."""
        mapping = PONG_RESEARCH_EVIDENCE_MAPPING
        return cls(
            required_components=mapping.required_components,
            dependency_group_bounds=mapping.dependency_group_bounds,
            evidence_mapping_hash=mapping.mapping_hash,
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
                "evidence_mapping_hash": self.evidence_mapping_hash,
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


PONG_RESEARCH_POLICY = DiscernmentPolicy.frozen_research_mapping()


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

    def verification_error(
        self,
        policy: DiscernmentPolicy,
        *,
        at: str | None = None,
    ) -> str | None:
        """Verify policy, source identities, causality, and point-in-time validity."""
        if type(policy) is not DiscernmentPolicy:
            return "DISCERNMENT_SNAPSHOT_POLICY_TYPE_INVALID"
        if self.policy_hash != policy.policy_hash:
            return "DISCERNMENT_SNAPSHOT_POLICY_HASH_MISMATCH"
        if self.model_hash != policy.model_hash:
            return "DISCERNMENT_SNAPSHOT_MODEL_HASH_MISMATCH"
        if not math.isclose(
            self.reliability, policy.reliability, rel_tol=0, abs_tol=1e-15,
        ):
            return "DISCERNMENT_SNAPSHOT_RELIABILITY_MISMATCH"
        if not self.valid:
            return "DISCERNMENT_UNAVAILABLE"

        component_keys = {
            (item.component_id, item.component_version)
            for item in self.component_effects
        }
        required_keys = set(policy.required_components)
        if not required_keys.issubset(component_keys):
            return "DISCERNMENT_REQUIRED_EVIDENCE_MISSING"
        group_bounds = dict(policy.dependency_group_bounds)
        source_ids: set[str] = set()
        grouped_effects: dict[str, float] = {}
        snapshot_available = _utc(self.available_at, "Snapshot available-at time")
        evaluated = snapshot_available if at is None else _utc(at, "Snapshot evaluation time")
        if snapshot_available > evaluated:
            return "DISCERNMENT_NOT_YET_AVAILABLE"
        for component in self.component_effects:
            if type(component) is not ComponentEffect:
                return "DISCERNMENT_COMPONENT_CONTRACT_INVALID"
            key = (component.component_id, component.component_version)
            if component.required != (key in required_keys):
                return "DISCERNMENT_COMPONENT_REQUIRED_FLAG_MISMATCH"
            if component.dependency_group not in group_bounds:
                return "DISCERNMENT_COMPONENT_DEPENDENCY_UNBOUND"
            if (
                not component.continuity_valid
                or component.continuity_domain != self.continuity_domain
                or component.evidence_revision > self.evidence_revision
                or _utc(component.available_at, "Component available-at time")
                > snapshot_available
            ):
                return "DISCERNMENT_COMPONENT_CAUSALITY_INVALID"
            if _utc(component.valid_until, "Component valid-until time") <= evaluated:
                return "DISCERNMENT_EVIDENCE_EXPIRED_AT_ADMISSION"
            evidence_payload = {
                "event_id": component.event_id,
                "component_id": component.component_id,
                "component_version": component.component_version,
                "dependency_group": component.dependency_group,
                "log_odds_effect": component.log_odds_effect,
                "required": component.required,
                "source_event_at": normalized_utc(
                    component.source_event_at, "Component source event time",
                ),
                "received_at": normalized_utc(
                    component.received_at, "Component receipt time",
                ),
                "available_at": normalized_utc(
                    component.available_at, "Component available-at time",
                ),
                "valid_until": normalized_utc(
                    component.valid_until, "Component valid-until time",
                ),
                "evidence_revision": component.evidence_revision,
                "continuity_domain": component.continuity_domain,
                "continuity_valid": component.continuity_valid,
                "source_ids": list(component.source_ids),
                "source_hashes": list(component.source_hashes),
            }
            if canonical_hash(evidence_payload) != component.evidence_hash:
                return "DISCERNMENT_COMPONENT_EVIDENCE_HASH_MISMATCH"
            source_ids.update(component.source_ids)
            grouped_effects[component.dependency_group] = (
                grouped_effects.get(component.dependency_group, 0.0)
                + float(component.log_odds_effect)
            )
        if tuple(sorted(source_ids)) != self.source_ids:
            return "DISCERNMENT_SNAPSHOT_SOURCE_IDENTITY_MISMATCH"
        expected_group_effects = tuple(
            (
                group,
                max(-group_bounds[group], min(group_bounds[group], effect)),
            )
            for group, effect in sorted(grouped_effects.items())
        )
        if (
            len(expected_group_effects) != len(self.dependency_group_effects)
            or any(
                expected_group != actual_group
                or not math.isclose(
                    expected_effect, actual_effect, rel_tol=0, abs_tol=1e-15,
                )
                for (expected_group, expected_effect), (actual_group, actual_effect)
                in zip(expected_group_effects, self.dependency_group_effects)
            )
        ):
            return "DISCERNMENT_DEPENDENCY_EFFECT_DERIVATION_MISMATCH"
        expected_log_odds = logit_probability(
            policy.prior_long_score, name="prior_long_score",
        ) + sum(effect for _, effect in expected_group_effects)
        expected_raw = sigmoid_clipped(expected_log_odds, clip=LOG_ODDS_CLIP)
        expected_effective = shrink_toward_neutral(
            expected_raw, policy.reliability,
        )
        assert self.raw_long_score is not None
        assert self.effective_long_score is not None
        assert self.long_discernment_pct is not None
        assert self.short_discernment_pct is not None
        if not all((
            math.isclose(
                self.raw_long_score, expected_raw, rel_tol=0, abs_tol=1e-15,
            ),
            math.isclose(
                self.effective_long_score,
                expected_effective,
                rel_tol=0,
                abs_tol=1e-15,
            ),
            math.isclose(
                self.long_discernment_pct,
                expected_effective * 100,
                rel_tol=0,
                abs_tol=1e-12,
            ),
            math.isclose(
                self.short_discernment_pct,
                (1 - expected_effective) * 100,
                rel_tol=0,
                abs_tol=1e-12,
            ),
        )):
            return "DISCERNMENT_SCORE_DERIVATION_MISMATCH"
        return None

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


def _switch_identity_material(
    *,
    sequence: int,
    prior_switch_id: str | None,
    previous_direction: DiscernmentDirection,
    direction: DiscernmentDirection,
    occurred_at: str,
    available_at: str,
    continuity_valid: bool,
    continuity_domain: str,
    snapshot: DiscernmentSnapshot,
    long_on_threshold: float,
    short_on_threshold: float,
    strong_threshold: float,
) -> dict[str, object]:
    return {
        "schema": DISCERNMENT_SCHEMA,
        "event_type": "DISCERNMENT_SWITCH",
        "sequence": sequence,
        "prior_switch_id": prior_switch_id,
        "prior_latch": previous_direction.value,
        "next_latch": direction.value,
        "previous_direction": previous_direction.value,
        "direction": direction.value,
        "occurred_at": normalized_utc(occurred_at, "Switch occurrence time"),
        "available_at": normalized_utc(available_at, "Switch available-at time"),
        "continuity_valid": continuity_valid,
        "continuity_domain": continuity_domain,
        "snapshot_hash": snapshot.result_hash,
        "snapshot_evidence_revision": snapshot.evidence_revision,
        "snapshot_as_of": snapshot.as_of,
        "snapshot_received_at": snapshot.received_at,
        "snapshot_available_at": snapshot.available_at,
        "instrument": snapshot.instrument,
        "contract": snapshot.contract,
        "policy_hash": snapshot.policy_hash,
        "model_hash": snapshot.model_hash,
        "long_on_threshold": long_on_threshold,
        "short_on_threshold": short_on_threshold,
        "strong_threshold": strong_threshold,
        "calibration_status": snapshot.calibration_status,
    }


_VERIFY_UNSET = object()


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
    long_on_threshold: float = 0.65
    short_on_threshold: float = 0.35
    strong_threshold: float = 0.85

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
        long_on = unit_interval(self.long_on_threshold, "switch long_on_threshold")
        short_on = unit_interval(self.short_on_threshold, "switch short_on_threshold")
        strong = unit_interval(self.strong_threshold, "switch strong_threshold")
        if not 0 < short_on < 0.5 < long_on < strong < 1 or not math.isclose(
            short_on, 1 - long_on, rel_tol=0, abs_tol=1e-12
        ):
            raise ValueError("Discernment switch thresholds are invalid.")
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
        score = self.effective_long_score
        if self.direction is DiscernmentDirection.LONG:
            valid_transition = (
                self.previous_direction is DiscernmentDirection.SHORT and score >= long_on
            )
        else:
            valid_transition = (
                self.previous_direction is DiscernmentDirection.LONG and score <= short_on
            )
        if not valid_transition:
            raise ValueError("Discernment direction is not the snapshot's opposite threshold transition.")

    @classmethod
    def create(
        cls,
        *,
        policy: DiscernmentPolicy,
        sequence: int,
        prior_switch_id: str | None,
        previous_direction: DiscernmentDirection,
        direction: DiscernmentDirection,
        snapshot: DiscernmentSnapshot,
    ) -> "DiscernmentSwitch":
        """Create a deterministic switch bound to the approved policy and snapshot."""
        snapshot_error = snapshot.verification_error(
            policy, at=snapshot.available_at,
        )
        if snapshot_error is not None:
            raise ValueError(snapshot_error)
        occurred_at = snapshot.available_at
        available_at = snapshot.available_at
        material = _switch_identity_material(
            sequence=sequence,
            prior_switch_id=prior_switch_id,
            previous_direction=previous_direction,
            direction=direction,
            occurred_at=occurred_at,
            available_at=available_at,
            continuity_valid=snapshot.continuity_valid,
            continuity_domain=snapshot.continuity_domain,
            snapshot=snapshot,
            long_on_threshold=policy.long_on_threshold,
            short_on_threshold=policy.short_on_threshold,
            strong_threshold=policy.strong_threshold,
        )
        result = cls(
            event_id="pong-discernment-switch-" + canonical_hash(material)[:32],
            sequence=sequence,
            prior_switch_id=prior_switch_id,
            previous_direction=previous_direction,
            direction=direction,
            occurred_at=occurred_at,
            available_at=available_at,
            continuity_valid=snapshot.continuity_valid,
            continuity_domain=snapshot.continuity_domain,
            snapshot=snapshot,
            long_on_threshold=policy.long_on_threshold,
            short_on_threshold=policy.short_on_threshold,
            strong_threshold=policy.strong_threshold,
        )
        result.require_verified(policy)
        return result

    @property
    def switch_id(self) -> str:
        return self.event_id

    @property
    def effective_long_score(self) -> float:
        assert self.snapshot.effective_long_score is not None
        return self.snapshot.effective_long_score

    @property
    def identity_material(self) -> dict[str, object]:
        return _switch_identity_material(
            sequence=self.sequence,
            prior_switch_id=self.prior_switch_id,
            previous_direction=self.previous_direction,
            direction=self.direction,
            occurred_at=self.occurred_at,
            available_at=self.available_at,
            continuity_valid=self.continuity_valid,
            continuity_domain=self.continuity_domain,
            snapshot=self.snapshot,
            long_on_threshold=self.long_on_threshold,
            short_on_threshold=self.short_on_threshold,
            strong_threshold=self.strong_threshold,
        )

    def verification_error(
        self,
        policy: DiscernmentPolicy,
        *,
        expected_previous_direction: DiscernmentDirection | None = None,
        expected_prior_switch_id: object = _VERIFY_UNSET,
        expected_sequence: int | None = None,
    ) -> str | None:
        """Return a stable reason if this event is not an approved causal transition."""
        if type(policy) is not DiscernmentPolicy:
            return "DISCERNMENT_SWITCH_POLICY_TYPE_INVALID"
        if self.snapshot.policy_hash != policy.policy_hash:
            return "DISCERNMENT_SWITCH_POLICY_HASH_MISMATCH"
        if self.snapshot.model_hash != policy.model_hash:
            return "DISCERNMENT_SWITCH_MODEL_HASH_MISMATCH"
        if not all(
            math.isclose(actual, expected, rel_tol=0, abs_tol=1e-15)
            for actual, expected in (
                (self.long_on_threshold, policy.long_on_threshold),
                (self.short_on_threshold, policy.short_on_threshold),
                (self.strong_threshold, policy.strong_threshold),
            )
        ):
            return "DISCERNMENT_SWITCH_THRESHOLD_CONTRACT_MISMATCH"
        snapshot_error = self.snapshot.verification_error(
            policy, at=self.available_at,
        )
        if snapshot_error is not None:
            return snapshot_error
        if expected_previous_direction is not None and (
            self.previous_direction is not expected_previous_direction
        ):
            return "DISCERNMENT_SWITCH_PRIOR_LATCH_MISMATCH"
        if expected_prior_switch_id is not _VERIFY_UNSET and (
            self.prior_switch_id != expected_prior_switch_id
        ):
            return "DISCERNMENT_SWITCH_PRIOR_ID_MISMATCH"
        if expected_sequence is not None and self.sequence != expected_sequence:
            return "DISCERNMENT_SWITCH_SEQUENCE_MISMATCH"
        expected_id = "pong-discernment-switch-" + canonical_hash(self.identity_material)[:32]
        if self.event_id != expected_id:
            return "DISCERNMENT_SWITCH_IDENTITY_MISMATCH"
        return None

    def verify(
        self,
        policy: DiscernmentPolicy,
        *,
        expected_previous_direction: DiscernmentDirection | None = None,
        expected_prior_switch_id: object = _VERIFY_UNSET,
        expected_sequence: int | None = None,
    ) -> bool:
        return self.verification_error(
            policy,
            expected_previous_direction=expected_previous_direction,
            expected_prior_switch_id=expected_prior_switch_id,
            expected_sequence=expected_sequence,
        ) is None

    def require_verified(
        self,
        policy: DiscernmentPolicy,
        *,
        expected_previous_direction: DiscernmentDirection | None = None,
        expected_prior_switch_id: object = _VERIFY_UNSET,
        expected_sequence: int | None = None,
    ) -> None:
        reason = self.verification_error(
            policy,
            expected_previous_direction=expected_previous_direction,
            expected_prior_switch_id=expected_prior_switch_id,
            expected_sequence=expected_sequence,
        )
        if reason is not None:
            raise ValueError(reason)

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
            "snapshot_evidence_revision": self.snapshot.evidence_revision,
            "effective_long_score": self.effective_long_score,
            "policy_hash": self.snapshot.policy_hash,
            "model_hash": self.snapshot.model_hash,
            "long_on_threshold": self.long_on_threshold,
            "short_on_threshold": self.short_on_threshold,
            "strong_threshold": self.strong_threshold,
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
        self.policy = policy or PONG_RESEARCH_POLICY
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
        self._latest_available_key: str | None = None
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
    def evidence_revision(self) -> int:
        """Current admitted evidence cursor for a production composition root."""
        return self._evidence_revision

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
        available_key = _utc(evidence.available_at, "Evidence available-at time")
        if self._latest_available_key is not None and available_key < self._latest_available_key:
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
            self._latest_available_key = available_key
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
            self._latest_available_key = available_key
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
        self._latest_available_key = available_key
        self._evidence_revision = evidence.evidence_revision
        self._volume_regime = volume_regime
        if prior is not None and prior.semantic_hash == evidence.semantic_hash:
            return self._snapshot_update(available, "IDENTICAL_EVIDENCE_NO_DECISION", admitted=False)

        self._active[evidence.component_key] = evidence
        return self._snapshot_update(available, "EVIDENCE_ADMITTED", admitted=True, may_switch=True)

    def on_time(self, available_at: str) -> DiscernmentUpdate:
        return self._maintenance(MaintenanceEventKind.TIME, available_at)

    def on_evidence_frame(
        self, evidence: Iterable[DiscernmentEvidence], *, volume_regime: VolumeRegime,
    ) -> DiscernmentUpdate:
        """Commit a complete callback frame with at most one logical switch.

        Component replacement order must not emit transitions from a mixture
        of the old and new values of one atomic microstructure observation.
        The scalar ingestion API and its accepted behavior remain unchanged.
        """
        import copy
        values = tuple(evidence)
        if not values or any(type(item) is not DiscernmentEvidence for item in values):
            raise ValueError("A complete typed Discernment frame is required.")
        if {item.component_key for item in values} != set(self.policy.required_components) or len(values) != len(self.policy.required_components):
            raise ValueError("A Discernment frame requires every component exactly once.")
        if len({(_utc(item.available_at, "Evidence available-at time"), item.continuity_domain) for item in values}) != 1:
            raise ValueError("Frame components must share causal availability and continuity.")
        if all(self._seen_event_hashes.get(item.event_id) == item.evidence_hash for item in values):
            return self._current_update("DUPLICATE_EVIDENCE_FRAME_IGNORED")
        expected = self._evidence_revision + 1
        if tuple(item.evidence_revision for item in values) != tuple(range(expected,expected+len(values))):
            return self._invalidate(values[-1].available_at,"EVIDENCE_FRAME_REVISION_GAP")
        if self._continuity_domain is not None and values[0].continuity_domain != self._continuity_domain:
            return self._invalidate(values[-1].available_at,"EVIDENCE_FRAME_CONTINUITY_CHANGED")
        stage = copy.copy(self)
        stage._active = dict(self._active)
        stage._seen_event_hashes = dict(self._seen_event_hashes)
        cursor = (self._latched_direction,self._switch_sequence,self._last_switch_id)
        admitted = False
        for item in values:
            result = stage.on_evidence(item,volume_regime=volume_regime)
            admitted = admitted or result.admitted
            stage._latched_direction,stage._switch_sequence,stage._last_switch_id = cursor
        result = stage._snapshot_update(values[-1].available_at,"EVIDENCE_FRAME_ADMITTED",admitted=admitted,may_switch=admitted)
        self.__dict__.update(stage.__dict__)
        return result

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
        available_key = _utc(available_at, f"{kind.value} available-at time")
        if self._latest_available_key is not None and available_key < self._latest_available_key:
            return self._current_update(f"OUT_OF_ORDER_{kind.value}_IGNORED")
        self._latest_available_at = available
        self._latest_available_key = available_key
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
        self._latest_available_key = _utc(available_at, "Invalidation available-at time")
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
                component_id=item.component_id,
                component_version=item.component_version,
                dependency_group=item.dependency_group,
                log_odds_effect=float(item.log_odds_effect),
                event_id=item.event_id,
                source_event_at=item.source_event_at,
                received_at=item.received_at,
                available_at=item.available_at,
                valid_until=item.valid_until,
                evidence_revision=item.evidence_revision,
                continuity_domain=item.continuity_domain,
                continuity_valid=item.continuity_valid,
                required=item.required,
                source_ids=item.source_ids,
                source_hashes=item.source_hashes,
                evidence_hash=item.evidence_hash,
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
            latest_event = max(
                active,
                key=lambda item: _utc(item.source_event_at, "Evidence source event time"),
            )
            latest_receipt = max(
                active,
                key=lambda item: _utc(item.received_at, "Evidence receipt time"),
            )
            as_of = normalized_utc(
                latest_event.source_event_at, "Evidence source event time",
            )
            received_at = normalized_utc(
                latest_receipt.received_at, "Evidence receipt time",
            )
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
        switch = DiscernmentSwitch.create(
            policy=self.policy,
            sequence=sequence,
            prior_switch_id=self._last_switch_id,
            previous_direction=previous,
            direction=candidate,
            snapshot=snapshot,
        )
        self._latched_direction = candidate
        self._switch_sequence = sequence
        self._last_switch_id = switch.event_id
        return switch
