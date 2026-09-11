"""Execution-disabled Pong quantity, risk, and exposure contracts.

The latest recovered historical Lane III paper evidence is deliberately not
reused as Pong execution authority.  This module models a separately versioned
Pong candidate whose defaults retain those historical one-contract native/risk
limits as conservative blockers; current runtime and account state were not
queried.  It has no sender or runtime registration and cannot create execution
authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR
from enum import StrEnum
import hashlib
import json
import math


PONG_QUANTITY_CONTRACT_VERSION = "BEELZEBUB_PONG_QUANTITY_V1"
PONG_MODE = "PAPER_SIM101"
PONG_LIVE_AUTHORITY = "DISARMED"
PONG_STRONG_DISCERNMENT_THRESHOLD = 0.85
PONG_MAXIMUM_ABSOLUTE_QUANTITY = 2
PONG_QUANTITY_EXECUTION_ENABLED = False
PONG_RECONCILIATION_MAX_AGE_SECONDS = 5.0
PONG_RECONCILIATION_SOURCE_ID = "l3g-paper-addon-provenance-v1"
PONG_RECONCILIATION_SOURCE_FINGERPRINT = (
    "ded3309410f44aeb56018838fe1e59347c552664c4052cdab2bc56e9a71b2ef0"
)
PONG_RECONCILIATION_BUILD_FINGERPRINT = (
    "a2b889ca30929582e15ce46aaaaaf8ac430807c67eb79237dc35dc4fbfc14af6"
)


class PongQuantityDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


def _canonical_hash(value: object) -> str:
    material = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _decimal(value: Decimal | str | int, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:  # pragma: no cover - defensive conversion fence
        raise ValueError(f"{name} must be a finite decimal.") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite decimal.")
    return result


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Pong timestamps must include a UTC offset.")
    return parsed


@dataclass(frozen=True)
class PongQuantityPolicy:
    """Candidate-only identity using latest recovered historical constraints."""

    version: str = PONG_QUANTITY_CONTRACT_VERSION
    mode: str = PONG_MODE
    account_name: str = "Sim101"
    account_class: str = "LOCAL_SIMULATION"
    instrument: str = "MNQ SEP26"
    canonical_contract: str = "MNQU6"
    point_value_dollars: Decimal = Decimal("2.00")
    stop_distance_points: Decimal = Decimal("25.00")
    maximum_position_risk_dollars: Decimal = Decimal("50.00")
    maximum_absolute_quantity: int = PONG_MAXIMUM_ABSOLUTE_QUANTITY
    native_maximum_quantity: int = 1
    native_capability_status: str = "HISTORICAL_ADDON_SEALED_TO_ONE_NOT_CURRENT_AUTHORITY"
    live_authority: str = PONG_LIVE_AUTHORITY
    live_capital: str = "DENIED"
    execution_enabled: bool = PONG_QUANTITY_EXECUTION_ENABLED

    def __post_init__(self) -> None:
        if (
            self.version != PONG_QUANTITY_CONTRACT_VERSION
            or self.mode != PONG_MODE
            or self.account_name != "Sim101"
            or self.account_class != "LOCAL_SIMULATION"
            or self.instrument != "MNQ SEP26"
            or self.canonical_contract != "MNQU6"
            or self.maximum_absolute_quantity != 2
            or self.live_authority != PONG_LIVE_AUTHORITY
            or self.live_capital != "DENIED"
            or self.execution_enabled
        ):
            raise ValueError("Pong quantity identity is sealed to disabled Sim101/MNQU6.")
        for name in (
            "point_value_dollars", "stop_distance_points",
            "maximum_position_risk_dollars",
        ):
            if _decimal(getattr(self, name), name) <= 0:
                raise ValueError(f"{name} must be positive.")
        if type(self.native_maximum_quantity) is not int or not 0 <= self.native_maximum_quantity <= 2:
            raise ValueError("native_maximum_quantity must be an integer in [0, 2].")
        if not self.native_capability_status:
            raise ValueError("native_capability_status is required.")

    @property
    def nominal_risk_per_contract(self) -> Decimal:
        return self.point_value_dollars * self.stop_distance_points

    @property
    def configuration_hash(self) -> str:
        payload = asdict(self)
        payload["point_value_dollars"] = str(self.point_value_dollars)
        payload["stop_distance_points"] = str(self.stop_distance_points)
        payload["maximum_position_risk_dollars"] = str(
            self.maximum_position_risk_dollars,
        )
        return _canonical_hash(payload)


PONG_QUANTITY_POLICY = PongQuantityPolicy()


@dataclass(frozen=True)
class PongScoreAuthorization:
    direction: PongQuantityDirection
    effective_long_score: float
    position_side_support: float
    requested_total_quantity: int
    otherwise_eligible: bool
    evidence_valid: bool
    reason_code: str

    @property
    def authorization_hash(self) -> str:
        return _canonical_hash({
            "direction": self.direction.value,
            "effective_long_score": self.effective_long_score,
            "position_side_support": self.position_side_support,
            "requested_total_quantity": self.requested_total_quantity,
            "otherwise_eligible": self.otherwise_eligible,
            "evidence_valid": self.evidence_valid,
            "reason_code": self.reason_code,
        })


def score_authorized_quantity(
    direction: PongQuantityDirection,
    effective_long_score: float,
    *,
    evidence_valid: bool,
    otherwise_eligible: bool,
) -> PongScoreAuthorization:
    """Apply the inclusive 85% rule to full precision, never display rounding."""
    if type(direction) is not PongQuantityDirection:
        raise ValueError("Pong quantity direction must be LONG or SHORT.")
    if isinstance(effective_long_score, bool) or not isinstance(
        effective_long_score, (int, float),
    ):
        raise ValueError("effective_long_score must be a finite number in [0, 1].")
    score = float(effective_long_score)
    if not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("effective_long_score must be a finite number in [0, 1].")
    if type(evidence_valid) is not bool or type(otherwise_eligible) is not bool:
        raise ValueError("Pong eligibility flags must be booleans.")
    support = score if direction is PongQuantityDirection.LONG else 1.0 - score
    if not evidence_valid:
        quantity, reason = 0, "DISCERNMENT_INVALID"
    elif not otherwise_eligible:
        quantity, reason = 0, "ENTRY_NOT_OTHERWISE_ELIGIBLE"
    elif support >= PONG_STRONG_DISCERNMENT_THRESHOLD:
        quantity, reason = 2, "STRONG_DISCERNMENT_TWO_MNQ_REQUESTED"
    else:
        quantity, reason = 1, "ORDINARY_DISCERNMENT_ONE_MNQ_REQUESTED"
    return PongScoreAuthorization(
        direction, score, support, quantity, otherwise_eligible,
        evidence_valid, reason,
    )


@dataclass(frozen=True)
class PongReconciliationProof:
    """Hash-bound candidate input; native authentication remains uncommissioned."""

    mode: str
    account_name: str
    account_class: str
    instrument: str
    canonical_contract: str
    source_id: str
    source_fingerprint: str
    build_fingerprint: str
    source_hash: str
    observed_at: str
    available_at: str
    position_quantity: int
    working_owned_orders: int
    unresolved_execution: bool
    live_authority: str
    signature_status: str
    result_hash: str

    @classmethod
    def create(
        cls,
        *,
        mode: str = PONG_MODE,
        source_id: str = PONG_RECONCILIATION_SOURCE_ID,
        source_fingerprint: str = PONG_RECONCILIATION_SOURCE_FINGERPRINT,
        build_fingerprint: str = PONG_RECONCILIATION_BUILD_FINGERPRINT,
        source_hash: str,
        observed_at: str,
        available_at: str,
        position_quantity: int,
        working_owned_orders: int,
        unresolved_execution: bool,
        live_authority: str = PONG_LIVE_AUTHORITY,
        signature_status: str = "SIGNED_RECONCILIATION",
        account_name: str = "Sim101",
        account_class: str = "LOCAL_SIMULATION",
        instrument: str = "MNQ SEP26",
        canonical_contract: str = "MNQU6",
    ) -> PongReconciliationProof:
        values = {
            "mode": mode,
            "account_name": account_name,
            "account_class": account_class,
            "instrument": instrument,
            "canonical_contract": canonical_contract,
            "source_id": source_id,
            "source_fingerprint": source_fingerprint,
            "build_fingerprint": build_fingerprint,
            "source_hash": source_hash,
            "observed_at": observed_at,
            "available_at": available_at,
            "position_quantity": position_quantity,
            "working_owned_orders": working_owned_orders,
            "unresolved_execution": unresolved_execution,
            "live_authority": live_authority,
            "signature_status": signature_status,
        }
        return cls(**values, result_hash=_canonical_hash(values))

    def validation_reason(
        self,
        *,
        at: str,
        policy: PongQuantityPolicy = PONG_QUANTITY_POLICY,
    ) -> str | None:
        try:
            payload = {
                "mode": self.mode,
                "account_name": self.account_name,
                "account_class": self.account_class,
                "instrument": self.instrument,
                "canonical_contract": self.canonical_contract,
                "source_id": self.source_id,
                "source_fingerprint": self.source_fingerprint,
                "build_fingerprint": self.build_fingerprint,
                "source_hash": self.source_hash,
                "observed_at": self.observed_at,
                "available_at": self.available_at,
                "position_quantity": self.position_quantity,
                "working_owned_orders": self.working_owned_orders,
                "unresolved_execution": self.unresolved_execution,
                "live_authority": self.live_authority,
                "signature_status": self.signature_status,
            }
            if self.result_hash != _canonical_hash(payload):
                return "PONG_RECONCILIATION_INTEGRITY_FAILED"
            if (
                self.mode,
                self.account_name,
                self.account_class,
                self.instrument,
                self.canonical_contract,
                self.live_authority,
            ) != (
                policy.mode,
                policy.account_name,
                policy.account_class,
                policy.instrument,
                policy.canonical_contract,
                policy.live_authority,
            ):
                return "PONG_EXECUTION_IDENTITY_MISMATCH"
            if (
                self.source_id != PONG_RECONCILIATION_SOURCE_ID
                or self.source_fingerprint != PONG_RECONCILIATION_SOURCE_FINGERPRINT
                or self.build_fingerprint != PONG_RECONCILIATION_BUILD_FINGERPRINT
                or len(self.source_hash) != 64
                or any(character not in "0123456789abcdef" for character in self.source_hash)
                or self.signature_status != "SIGNED_RECONCILIATION"
                or type(self.position_quantity) is not int
                or type(self.working_owned_orders) is not int
                or self.working_owned_orders < 0
                or type(self.unresolved_execution) is not bool
            ):
                return "PONG_RECONCILIATION_CONTRACT_INVALID"
            observed = _utc(self.observed_at)
            available = _utc(self.available_at)
            evaluated = _utc(at)
            if observed > available or available > evaluated:
                return "PONG_RECONCILIATION_TIME_INVALID"
            if (evaluated - available).total_seconds() > PONG_RECONCILIATION_MAX_AGE_SECONDS:
                return "PONG_RECONCILIATION_STALE"
        except (AttributeError, TypeError, ValueError):
            return "PONG_RECONCILIATION_CONTRACT_INVALID"
        return None


@dataclass(frozen=True)
class PongAdmissionResult:
    admission_id: str
    opportunity_action_id: str
    source_switch_id: str
    snapshot_hash: str
    bias_result_hash: str
    admission_at: str
    reconciliation_hash: str
    authorization_hash: str
    policy_hash: str
    score_authorized_quantity: int
    risk_capacity_quantity: int
    native_capacity_quantity: int
    admitted_quantity: int
    available_loss_allowance_dollars: Decimal
    already_reserved_risk_dollars: Decimal
    reserved_risk_dollars: Decimal
    nominal_risk_per_contract: Decimal
    blockers: tuple[str, ...]

    @property
    def admitted(self) -> bool:
        return self.admitted_quantity > 0


def assess_quantity_admission(
    authorization: PongScoreAuthorization,
    *,
    opportunity_action_id: str,
    source_switch_id: str,
    snapshot_hash: str,
    bias_result_hash: str,
    admission_at: str,
    reconciliation: PongReconciliationProof,
    available_loss_allowance_dollars: Decimal | str | int,
    already_reserved_risk_dollars: Decimal | str | int = Decimal("0"),
    policy: PongQuantityPolicy = PONG_QUANTITY_POLICY,
) -> PongAdmissionResult:
    """Reduce score permission by exact identity, risk, and native capability."""
    if (
        type(authorization) is not PongScoreAuthorization
        or type(policy) is not PongQuantityPolicy
        or type(reconciliation) is not PongReconciliationProof
    ):
        raise ValueError("Pong admission requires exact candidate contracts.")
    if not all((opportunity_action_id, source_switch_id, snapshot_hash, bias_result_hash)):
        raise ValueError("Pong admission requires complete opportunity provenance.")
    reconciliation_reason = reconciliation.validation_reason(at=admission_at, policy=policy)
    allowance = _decimal(available_loss_allowance_dollars, "available loss allowance")
    reserved = _decimal(already_reserved_risk_dollars, "reserved risk")
    if allowance < 0 or reserved < 0:
        raise ValueError("Loss allowance and reserved risk cannot be negative.")

    blockers: list[str] = []
    if reconciliation_reason is not None:
        blockers.append(reconciliation_reason)
    if reconciliation.position_quantity != 0:
        blockers.append("PYRAMIDING_OR_REVERSAL_DENIED")
    if reconciliation.working_owned_orders:
        blockers.append("WORKING_OWNED_ORDERS_PRESENT")
    if reconciliation.unresolved_execution:
        blockers.append("UNRESOLVED_EXECUTION_TRUTH")

    remaining_position_budget = max(
        Decimal("0"), policy.maximum_position_risk_dollars - reserved,
    )
    effective_budget = min(allowance, remaining_position_budget)
    risk_capacity = int(
        (effective_budget / policy.nominal_risk_per_contract).to_integral_value(
            rounding=ROUND_FLOOR,
        ),
    )
    risk_capacity = max(0, min(policy.maximum_absolute_quantity, risk_capacity))
    if authorization.requested_total_quantity == 2 and risk_capacity < 2:
        blockers.append("TWO_MNQ_EXCEEDS_POSITION_RISK_LIMIT")
    if authorization.requested_total_quantity == 2 and policy.native_maximum_quantity < 2:
        blockers.append("PONG_NATIVE_QUANTITY_CAPABILITY_UNPROVEN")
    if authorization.requested_total_quantity and risk_capacity == 0:
        blockers.append("PONG_RISK_BUDGET_INSUFFICIENT")
    if authorization.requested_total_quantity and policy.native_maximum_quantity == 0:
        blockers.append("PONG_NATIVE_QUANTITY_UNSUPPORTED")

    hard_block = any(
        reason in blockers
        for reason in (
            "PONG_RECONCILIATION_INTEGRITY_FAILED",
            "PONG_EXECUTION_IDENTITY_MISMATCH",
            "PONG_RECONCILIATION_CONTRACT_INVALID",
            "PONG_RECONCILIATION_TIME_INVALID", "PONG_RECONCILIATION_STALE",
            "PYRAMIDING_OR_REVERSAL_DENIED",
            "WORKING_OWNED_ORDERS_PRESENT", "UNRESOLVED_EXECUTION_TRUTH",
        )
    )
    admitted = 0 if hard_block else min(
        authorization.requested_total_quantity,
        policy.maximum_absolute_quantity,
        risk_capacity,
        policy.native_maximum_quantity,
    )
    reserved_risk = policy.nominal_risk_per_contract * admitted
    identity = {
        "opportunity_action_id": opportunity_action_id,
        "source_switch_id": source_switch_id,
        "snapshot_hash": snapshot_hash,
        "bias_result_hash": bias_result_hash,
        "admission_at": admission_at,
        "reconciliation_hash": reconciliation.result_hash,
        "authorization_hash": authorization.authorization_hash,
        "policy_hash": policy.configuration_hash,
        "score_authorized_quantity": authorization.requested_total_quantity,
        "risk_capacity_quantity": risk_capacity,
        "native_capacity_quantity": policy.native_maximum_quantity,
        "admitted_quantity": admitted,
        "available_loss_allowance_dollars": str(allowance),
        "already_reserved_risk_dollars": str(reserved),
        "reserved_risk_dollars": str(reserved_risk),
        "nominal_risk_per_contract": str(policy.nominal_risk_per_contract),
        "blockers": list(dict.fromkeys(blockers)),
    }
    return PongAdmissionResult(
        "pong-admission-" + _canonical_hash(identity)[:32],
        opportunity_action_id,
        source_switch_id,
        snapshot_hash,
        bias_result_hash,
        admission_at,
        reconciliation.result_hash,
        authorization.authorization_hash,
        policy.configuration_hash,
        authorization.requested_total_quantity,
        risk_capacity,
        policy.native_maximum_quantity,
        admitted,
        allowance,
        reserved,
        reserved_risk,
        policy.nominal_risk_per_contract,
        tuple(dict.fromkeys(blockers)),
    )


@dataclass
class PongExposureTracker:
    """Exact-once model for one candidate position lifecycle (no transport)."""

    target_quantity: int
    filled_quantity: int = 0
    protected_quantity: int = 0
    remaining_entry_quantity: int = 0
    exit_filled_quantity: int = 0
    cancellation_requested: bool = False
    _fill_ids: set[str] | None = None
    _exit_fill_ids: set[str] | None = None

    def __post_init__(self) -> None:
        if type(self.target_quantity) is not int or not 1 <= self.target_quantity <= 2:
            raise ValueError("A Pong lifecycle target must be one or two MNQ.")
        if self.filled_quantity or self.protected_quantity or self.remaining_entry_quantity or self.exit_filled_quantity:
            raise ValueError("Restore of a Pong exposure tracker requires a reviewed recovery contract.")
        self.remaining_entry_quantity = self.target_quantity
        self._fill_ids = set()
        self._exit_fill_ids = set()

    @property
    def owned_open_quantity(self) -> int:
        return self.filled_quantity - self.exit_filled_quantity

    @property
    def required_exit_quantity(self) -> int:
        return self.owned_open_quantity

    @property
    def protection_complete(self) -> bool:
        return self.protected_quantity == self.owned_open_quantity

    def record_entry_fill(self, fill_id: str, quantity: int) -> int:
        if not fill_id or type(quantity) is not int or quantity <= 0:
            raise ValueError("Entry fills require an ID and positive integer quantity.")
        assert self._fill_ids is not None
        assert self._exit_fill_ids is not None
        if fill_id in self._fill_ids or fill_id in self._exit_fill_ids:
            return self.owned_open_quantity
        if quantity > self.remaining_entry_quantity:
            raise ValueError("Pong entry fill exceeds the one lifecycle remainder.")
        self._fill_ids.add(fill_id)
        self.filled_quantity += quantity
        self.remaining_entry_quantity -= quantity
        # The caller must replace protection for the full actual exposure now.
        return self.owned_open_quantity

    def confirm_protection(self, quantity: int) -> None:
        if type(quantity) is not int or quantity != self.owned_open_quantity:
            raise ValueError("Protective quantity must equal actual owned exposure.")
        self.protected_quantity = quantity

    def request_cancel_remainder(self) -> int:
        self.cancellation_requested = True
        return self.remaining_entry_quantity

    def confirm_entry_remainder_cancelled(self, quantity: int) -> None:
        if type(quantity) is not int or quantity != self.remaining_entry_quantity:
            raise ValueError("Cancelled quantity must equal the outstanding remainder.")
        self.remaining_entry_quantity = 0

    def record_exit_fill(self, fill_id: str, quantity: int) -> int:
        if not fill_id or type(quantity) is not int or quantity <= 0:
            raise ValueError("Exit fills require an ID and positive integer quantity.")
        assert self._exit_fill_ids is not None
        assert self._fill_ids is not None
        if fill_id in self._exit_fill_ids or fill_id in self._fill_ids:
            return self.owned_open_quantity
        if quantity > self.owned_open_quantity:
            raise ValueError("Pong exit fill would over-exit owned exposure.")
        self._exit_fill_ids.add(fill_id)
        self.exit_filled_quantity += quantity
        self.protected_quantity = min(self.protected_quantity, self.owned_open_quantity)
        return self.owned_open_quantity
