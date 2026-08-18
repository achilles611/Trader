"""Authority-preserving Phase F.2 bridge from Trader V0 into Phase D.

The bridge is the only module that imports both the frozen Lane II contracts
and Phase D.  Trader V0 remains transport-blind and Phase D remains the sole
owner of risk, sizing, submission, and reconciliation decisions.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Callable

from src.copytrade.execution_contracts import ExecutionIntent, ExposureEffect
from src.copytrade.models import as_utc, stable_id, utc_now
from src.copytrade.storage import CopyTradeDatabase

from .boundary import F0_AUTHORITY_MANIFEST_HASH, F0_MANIFEST, TradeDirection, TradeIntentRequest
from .trader_v0 import (
    F1_AUTHORITY_MANIFEST_HASH,
    F1_MANIFEST,
    F1_STRATEGY_REGISTRY,
    F1AuthorityEvaluation,
    TRADER_V0_ARTIFACT_HASH,
    TRADER_V0_EXIT_POLICY_REF,
    TRADER_V0_MAXIMUM_REQUESTED_NOTIONAL_CEILING,
    TRADER_V0_RISK_POLICY_REF,
    TRADER_V0_STRATEGY,
    TRADER_V0_STRATEGY_ID,
    TRADER_V0_STRATEGY_VERSION,
    TraderV0,
    TraderV0Action,
    TraderV0Decision,
)


LANE_II_SIMULATOR_DOMAIN = "LANE_II_SIMULATOR"
LANE_II_SOURCE = "LANE_II"
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,31}")


class LaneIIAdmissionRefused(RuntimeError):
    """The supplied request is not the exact commissioned F.1 authority."""


class LaneIISizingRefused(LaneIIAdmissionRefused):
    """Fresh execution-aware evidence cannot produce a safe quantity."""


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise LaneIISizingRefused(f"{field}_invalid")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise LaneIISizingRefused(f"{field}_invalid") from exc
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise LaneIISizingRefused(f"{field}_invalid")
    return result


@dataclass(frozen=True)
class ExecutionSizingEvidence:
    """Fresh price and instrument metadata owned by the execution boundary."""

    symbol: str
    mark_price: float
    price_observed_at: object
    metadata_observed_at: object
    quantity_decimals: int
    minimum_quantity: float
    source: str

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise LaneIISizingRefused("instrument_symbol_invalid")
        _decimal(self.mark_price, "mark_price")
        _decimal(self.minimum_quantity, "minimum_quantity", allow_zero=True)
        if isinstance(self.quantity_decimals, bool) or not isinstance(self.quantity_decimals, int):
            raise LaneIISizingRefused("quantity_precision_invalid")
        if not 0 <= self.quantity_decimals <= 18:
            raise LaneIISizingRefused("quantity_precision_invalid")
        try:
            as_utc(self.price_observed_at)
            as_utc(self.metadata_observed_at)
        except (TypeError, ValueError) as exc:
            raise LaneIISizingRefused("sizing_evidence_timestamp_invalid") from exc
        if not isinstance(self.source, str) or not self.source.strip():
            raise LaneIISizingRefused("sizing_evidence_source_missing")

    def payload(self) -> dict[str, object]:
        return {
            "schema": "phase-f2-execution-sizing-evidence-v1",
            "symbol": self.symbol,
            "mark_price": str(_decimal(self.mark_price, "mark_price")),
            "price_observed_at": as_utc(self.price_observed_at).isoformat(),
            "metadata_observed_at": as_utc(self.metadata_observed_at).isoformat(),
            "quantity_decimals": self.quantity_decimals,
            "minimum_quantity": str(_decimal(self.minimum_quantity, "minimum_quantity", allow_zero=True)),
            "source": self.source,
        }

    @property
    def evidence_hash(self) -> str:
        return _canonical_hash(self.payload())


@dataclass(frozen=True)
class VerifiedPositionTruth:
    """Phase D/venue account truth used to bound an F.1 EXIT decision."""

    symbol: str
    signed_quantity: float
    observed_at: object
    provenance_hash: str
    authoritative: bool

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise LaneIIAdmissionRefused("verified_position_symbol_invalid")
        if isinstance(self.signed_quantity, bool) or not isinstance(self.signed_quantity, (int, float)):
            raise LaneIIAdmissionRefused("verified_position_quantity_invalid")
        if not math.isfinite(float(self.signed_quantity)):
            raise LaneIIAdmissionRefused("verified_position_quantity_invalid")
        try:
            as_utc(self.observed_at)
        except (TypeError, ValueError) as exc:
            raise LaneIIAdmissionRefused("verified_position_timestamp_invalid") from exc
        if re.fullmatch(r"[0-9a-f]{64}", self.provenance_hash) is None:
            raise LaneIIAdmissionRefused("verified_position_provenance_invalid")
        if type(self.authoritative) is not bool:
            raise LaneIIAdmissionRefused("verified_position_authority_ambiguous")


class LaneIIPhaseDBridge:
    """Translate exact F.1 authority into immutable Phase D intents.

    ``phase_d_notional_limit`` is a Phase D policy ceiling.  It may reduce an
    F.1 entry request but can never enlarge it.  The later Phase D risk gate is
    still authoritative and may block the resulting intent entirely.
    """

    def __init__(
        self,
        store: CopyTradeDatabase,
        *,
        execution_account_id: str,
        phase_d_notional_limit: float,
        execution_domain: str = LANE_II_SIMULATOR_DOMAIN,
        evidence_ttl_seconds: float = 30.0,
        clock: Callable[[], object] = utc_now,
    ) -> None:
        if execution_domain not in {LANE_II_SIMULATOR_DOMAIN, "HYPERLIQUID_TESTNET"}:
            raise LaneIIAdmissionRefused("lane_ii_execution_domain_not_commissioned")
        if not isinstance(execution_account_id, str) or not execution_account_id.strip():
            raise LaneIIAdmissionRefused("execution_account_identity_required")
        if execution_account_id in {"SIMULATOR:default", "HYPERLIQUID:default"}:
            raise LaneIIAdmissionRefused("explicit_execution_account_identity_required")
        limit = _decimal(phase_d_notional_limit, "phase_d_notional_limit")
        ttl = _decimal(evidence_ttl_seconds, "evidence_ttl_seconds")
        self.store = store
        self.execution_domain = execution_domain
        self.execution_account_id = execution_account_id
        self.phase_d_notional_limit = limit
        self.evidence_ttl = timedelta(seconds=float(ttl))
        self.clock = clock

    def admit_entry(self, request: object, *, sizing: ExecutionSizingEvidence) -> ExecutionIntent:
        checked = self._verify_entry_request(request)
        now = as_utc(self.clock())
        self._verify_sizing(sizing, symbol=checked.symbol, now=now)
        authorized_notional = min(_decimal(checked.requested_notional_ceiling, "requested_notional"), self.phase_d_notional_limit)
        quantity = self._quantity_for_notional(authorized_notional, sizing)
        lane_provenance = self._entry_provenance(checked, sizing, authorized_notional)
        intent = ExecutionIntent(
            intent_id=stable_id(
                "phase_f2_lane_ii_execution_intent_v1", self.execution_domain,
                self.execution_account_id, checked.intent_id,
            ),
            signal_id=stable_id(
                "phase_f2_lane_ii_signal_v1", self.execution_domain,
                self.execution_account_id, checked.intent_id,
            ),
            source_event_id=checked.intent_id,
            target_wallet=f"lane_ii:{checked.strategy_identity}",
            campaign_id=None,
            symbol=checked.symbol,
            action="open",
            direction=checked.direction.value.lower(),
            requested_quantity=float(quantity),
            requested_capital=float(authorized_notional),
            source_event_timestamp=as_utc(checked.created_at),
            accepted_at=now,
            provenance={"lane_ii": lane_provenance},
            exposure_effect=ExposureEffect.INCREASE,
            execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
            updated_at=now,
        )
        return self.store.create_or_get_execution_intent(intent)

    def admit_verified_flatten(
        self,
        decision: object,
        *,
        position: VerifiedPositionTruth,
        sizing: ExecutionSizingEvidence,
    ) -> ExecutionIntent:
        """Admit frozen F.1 EXIT semantics without modifying its entry-only request.

        F.1 deliberately refuses to turn EXIT into ``TradeIntentRequest``.  F.2
        therefore replays the exact F.1 decision and independently requires
        authoritative Phase D position truth.  Lane II never asserts that a
        position exists or chooses the exit side/size.
        """
        checked = self._verify_exit_decision(decision)
        now = as_utc(self.clock())
        if not position.authoritative:
            raise LaneIIAdmissionRefused("verified_position_authority_required")
        if checked.symbol is None or position.symbol != checked.symbol:
            raise LaneIIAdmissionRefused("verified_position_symbol_mismatch")
        self._require_fresh(position.observed_at, now, "verified_position_stale")
        self._verify_sizing(sizing, symbol=position.symbol, now=now)
        raw_quantity = _decimal(abs(position.signed_quantity), "verified_position_quantity")
        quantum = Decimal(1).scaleb(-sizing.quantity_decimals)
        if raw_quantity.quantize(quantum, rounding=ROUND_DOWN) != raw_quantity:
            raise LaneIISizingRefused("verified_position_precision_invalid")
        minimum = _decimal(sizing.minimum_quantity, "minimum_quantity", allow_zero=True)
        if raw_quantity < minimum:
            raise LaneIISizingRefused("verified_position_below_minimum_quantity")
        direction = "long" if position.signed_quantity > 0 else "short"
        decision_id = "trader-exit-" + checked.decision_hash[:32]
        provenance = {
            "source": LANE_II_SOURCE,
            "source_contract": "F1_EXIT_DECISION_COMPATIBILITY",
            "f0_manifest_hash": F0_AUTHORITY_MANIFEST_HASH,
            "f1_manifest_hash": F1_AUTHORITY_MANIFEST_HASH,
            "strategy_id": TRADER_V0_STRATEGY_ID,
            "strategy_version": TRADER_V0_STRATEGY_VERSION,
            "strategy_identity": TRADER_V0_STRATEGY.strategy_identity,
            "strategy_artifact_hash": TRADER_V0_ARTIFACT_HASH,
            "trade_intent_id": None,
            "exit_decision_id": decision_id,
            "exit_decision_hash": checked.decision_hash,
            "authority_decision_hash": checked.authority.decision_hash,
            "input_provenance_hashes": list(checked.input_provenance_hashes),
            "risk_policy_ref": TRADER_V0_RISK_POLICY_REF,
            "exit_policy_ref": TRADER_V0_EXIT_POLICY_REF,
            "verified_position_provenance_hash": position.provenance_hash,
            "sizing_evidence_hash": sizing.evidence_hash,
            "execution_authority": "PHASE_D_EXECUTION_SOVEREIGN",
            "lane_ii_execution_authority": False,
        }
        intent = ExecutionIntent(
            intent_id=stable_id(
                "phase_f2_lane_ii_exit_intent_v1", self.execution_domain,
                self.execution_account_id, decision_id, position.provenance_hash,
            ),
            signal_id=stable_id(
                "phase_f2_lane_ii_exit_signal_v1", self.execution_domain,
                self.execution_account_id, decision_id, position.provenance_hash,
            ),
            source_event_id=decision_id,
            target_wallet=f"lane_ii:{TRADER_V0_STRATEGY.strategy_identity}",
            campaign_id=None,
            symbol=position.symbol,
            action="close",
            direction=direction,
            requested_quantity=float(raw_quantity),
            requested_capital=float(raw_quantity * _decimal(sizing.mark_price, "mark_price")),
            source_event_timestamp=as_utc(checked.created_at),
            accepted_at=now,
            provenance={"lane_ii": provenance},
            exposure_effect=ExposureEffect.FLATTEN,
            execution_domain=self.execution_domain,
            execution_account_id=self.execution_account_id,
            updated_at=now,
        )
        return self.store.create_or_get_execution_intent(intent)

    def _verify_entry_request(self, request: object) -> TradeIntentRequest:
        if type(request) is not TradeIntentRequest:
            raise LaneIIAdmissionRefused("exact_f1_trade_intent_required")
        checked = request
        try:
            reconstructed = TradeIntentRequest(
                strategy_id=checked.strategy_id,
                strategy_version=checked.strategy_version,
                strategy_identity=checked.strategy_identity,
                symbol=checked.symbol,
                direction=checked.direction,
                requested_notional_ceiling=checked.requested_notional_ceiling,
                created_at=checked.created_at,
                expires_at=checked.expires_at,
                authority_decision_hash=checked.authority_decision_hash,
                input_provenance_hashes=checked.input_provenance_hashes,
                exit_policy_ref=checked.exit_policy_ref,
                risk_policy_ref=checked.risk_policy_ref,
            )
        except (TypeError, ValueError) as exc:
            raise LaneIIAdmissionRefused("trade_intent_integrity_invalid") from exc
        if reconstructed.payload() != checked.payload() or reconstructed.intent_id != checked.intent_id:
            raise LaneIIAdmissionRefused("trade_intent_integrity_invalid")
        if (
            checked.strategy_id != TRADER_V0_STRATEGY_ID
            or checked.strategy_version != TRADER_V0_STRATEGY_VERSION
            or checked.strategy_identity != TRADER_V0_STRATEGY.strategy_identity
            or checked.exit_policy_ref != TRADER_V0_EXIT_POLICY_REF
            or checked.risk_policy_ref != TRADER_V0_RISK_POLICY_REF
            or checked.requested_notional_ceiling > TRADER_V0_MAXIMUM_REQUESTED_NOTIONAL_CEILING
        ):
            raise LaneIIAdmissionRefused("unknown_or_changed_f1_strategy")
        if (
            F0_MANIFEST.manifest_hash != F0_AUTHORITY_MANIFEST_HASH
            or F1_MANIFEST.manifest_hash != F1_AUTHORITY_MANIFEST_HASH
            or F1_MANIFEST.f0_manifest_hash != F0_AUTHORITY_MANIFEST_HASH
            or F1_MANIFEST.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH
            or F1_STRATEGY_REGISTRY.find(TRADER_V0_STRATEGY) is None
        ):
            raise LaneIIAdmissionRefused("frozen_authority_registration_invalid")
        hashes = checked.input_provenance_hashes
        if hashes != tuple(sorted(hashes)) or len(hashes) != len(set(hashes)):
            raise LaneIIAdmissionRefused("input_provenance_integrity_invalid")
        expected_authority = F1AuthorityEvaluation(
            allowed=True,
            reason_code="F1_TRADER_V0_SIGNAL_AUTHORITY_GRANTED",
            f0_manifest_hash=F0_AUTHORITY_MANIFEST_HASH,
            f1_manifest_hash=F1_AUTHORITY_MANIFEST_HASH,
            strategy_identity=TRADER_V0_STRATEGY.strategy_identity,
            strategy_version=TRADER_V0_STRATEGY_VERSION,
            input_provenance_hashes=hashes,
        )
        if checked.authority_decision_hash != expected_authority.decision_hash:
            raise LaneIIAdmissionRefused("authority_decision_integrity_invalid")
        if _SYMBOL.fullmatch(checked.symbol) is None or type(checked.direction) is not TradeDirection:
            raise LaneIIAdmissionRefused("symbol_or_direction_invalid")
        now = as_utc(self.clock())
        if as_utc(checked.created_at) > now or as_utc(checked.expires_at) <= now:
            raise LaneIIAdmissionRefused("trade_intent_expired_or_not_yet_valid")
        return checked

    def _verify_exit_decision(self, decision: object) -> TraderV0Decision:
        if type(decision) is not TraderV0Decision or decision.action is not TraderV0Action.EXIT:
            raise LaneIIAdmissionRefused("exact_f1_exit_decision_required")
        fresh = TraderV0().decide(decision.decision_input)
        if fresh.decision_hash != decision.decision_hash or fresh.action is not TraderV0Action.EXIT:
            raise LaneIIAdmissionRefused("f1_exit_decision_replay_mismatch")
        if (
            decision.strategy != TRADER_V0_STRATEGY
            or not decision.authority.allowed
            or decision.authority.f0_manifest_hash != F0_AUTHORITY_MANIFEST_HASH
            or decision.authority.f1_manifest_hash != F1_AUTHORITY_MANIFEST_HASH
            or decision.authority.decision_hash != fresh.authority.decision_hash
            or decision.created_at is None
            or decision.expires_at is None
        ):
            raise LaneIIAdmissionRefused("f1_exit_authority_invalid")
        now = as_utc(self.clock())
        if as_utc(decision.created_at) > now or as_utc(decision.expires_at) <= now:
            raise LaneIIAdmissionRefused("f1_exit_decision_expired_or_not_yet_valid")
        return decision

    def _entry_provenance(
        self,
        request: TradeIntentRequest,
        sizing: ExecutionSizingEvidence,
        authorized_notional: Decimal,
    ) -> dict[str, object]:
        return {
            "source": LANE_II_SOURCE,
            "source_contract": "F1_TRADE_INTENT_REQUEST",
            "f0_manifest_hash": F0_AUTHORITY_MANIFEST_HASH,
            "f1_manifest_hash": F1_AUTHORITY_MANIFEST_HASH,
            "strategy_id": TRADER_V0_STRATEGY_ID,
            "strategy_version": TRADER_V0_STRATEGY_VERSION,
            "strategy_identity": TRADER_V0_STRATEGY.strategy_identity,
            "strategy_artifact_hash": TRADER_V0_ARTIFACT_HASH,
            "trade_intent_id": request.intent_id,
            "trade_intent_integrity_hash": _canonical_hash(request.payload()),
            "authority_decision_hash": request.authority_decision_hash,
            "input_provenance_hashes": list(request.input_provenance_hashes),
            "risk_policy_ref": request.risk_policy_ref,
            "exit_policy_ref": request.exit_policy_ref,
            "requested_notional_ceiling": float(request.requested_notional_ceiling),
            "phase_d_notional_ceiling": float(authorized_notional),
            "sizing_evidence_hash": sizing.evidence_hash,
            "execution_authority": "PHASE_D_EXECUTION_SOVEREIGN",
            "lane_ii_execution_authority": False,
        }

    def _verify_sizing(self, sizing: object, *, symbol: str, now: object) -> None:
        if type(sizing) is not ExecutionSizingEvidence:
            raise LaneIISizingRefused("exact_execution_sizing_evidence_required")
        if sizing.symbol != symbol:
            raise LaneIISizingRefused("sizing_symbol_mismatch")
        self._require_fresh(sizing.price_observed_at, now, "market_price_stale")
        self._require_fresh(sizing.metadata_observed_at, now, "instrument_metadata_stale")

    def _require_fresh(self, observed_at: object, now: object, reason: str) -> None:
        age = as_utc(now) - as_utc(observed_at)
        if age.total_seconds() < 0 or age > self.evidence_ttl:
            raise LaneIISizingRefused(reason)

    @staticmethod
    def _quantity_for_notional(notional: Decimal, sizing: ExecutionSizingEvidence) -> Decimal:
        price = _decimal(sizing.mark_price, "mark_price")
        quantum = Decimal(1).scaleb(-sizing.quantity_decimals)
        quantity = (notional / price).quantize(quantum, rounding=ROUND_DOWN)
        minimum = _decimal(sizing.minimum_quantity, "minimum_quantity", allow_zero=True)
        if quantity <= 0 or quantity < minimum:
            raise LaneIISizingRefused("authorized_notional_below_instrument_minimum")
        if quantity * price > notional:
            raise LaneIISizingRefused("quantity_exceeds_authorized_notional")
        return quantity
