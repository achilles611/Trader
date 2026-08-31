"""Operator-visible L3H capability bootstrap and honest account classification.

This module never discovers credentials, starts a gateway, or writes a signed
capability by itself.  It turns already-observed NinjaTrader/provider facts
into a narrow, reviewable record and refuses to classify ambiguous evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import hmac
from typing import Mapping

from .contracts import AccountClass, canonical_hash, canonical_json, utc_now


@dataclass(frozen=True)
class AccountEvidence:
    account_identifier: str
    display_name: str
    connection_name: str | None
    provider_name: str | None
    provider_program: str | None
    metadata_class: AccountClass | None
    observed_at: str

    @property
    def binding_hash(self) -> str:
        return canonical_hash(asdict(self))


@dataclass(frozen=True)
class AccountAttestation:
    schema: str
    attestation_id: str
    account_binding_hash: str
    classification: AccountClass
    observed_evidence_hash: str
    created_at: str
    operator_confirmation: bool
    signature: str = ""

    def signed(self, key: bytes) -> "AccountAttestation":
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("L3H attestation key must contain at least 256 bits.")
        payload = asdict(self)
        payload.pop("signature")
        signature = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return AccountAttestation(**{**asdict(self), "signature": signature})

    def verify(self, key: bytes) -> None:
        if self.schema != "lane-iii-phase-h-account-attestation-v1" or not self.operator_confirmation:
            raise ValueError("ATTESTATION_INVALID")
        if self.classification is AccountClass.UNKNOWN:
            raise ValueError("ATTESTATION_UNKNOWN_CLASS")
        expected = self.signed(key).signature
        if not self.signature or not hmac.compare_digest(expected, self.signature):
            raise ValueError("ATTESTATION_SIGNATURE_INVALID")


@dataclass(frozen=True)
class NativeCapabilityBinding:
    """The AddOn-readable half of a reviewed local capability.

    It deliberately lives outside Git and has no password/credential fields.
    The HMAC keeps an altered account name from redirecting a valid capability.
    """

    schema: str
    native_account_id: str
    account_binding_hash: str
    capability_hash: str
    capability_generation: str
    commissioning_epoch: str
    signature: str = ""

    def signed(self, key: bytes) -> "NativeCapabilityBinding":
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("L3H binding key must contain at least 256 bits.")
        payload = asdict(self)
        payload.pop("signature")
        signature = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
        return NativeCapabilityBinding(**{**asdict(self), "signature": signature})

    def verify(self, key: bytes) -> None:
        if self.schema != "lane-iii-phase-h-native-binding-v1" or not self.native_account_id:
            raise ValueError("NATIVE_BINDING_INVALID")
        expected = self.signed(key).signature
        if not self.signature or not hmac.compare_digest(expected, self.signature):
            raise ValueError("NATIVE_BINDING_SIGNATURE_INVALID")


def classify_account(evidence: AccountEvidence) -> tuple[AccountClass, tuple[str, ...]]:
    """Classify only explicit, concordant evidence; otherwise return UNKNOWN."""

    facts = " ".join(value.lower() for value in (
        evidence.display_name, evidence.connection_name or "", evidence.provider_name or "", evidence.provider_program or "",
    ))
    hints: set[AccountClass] = set()
    if evidence.metadata_class is not None:
        hints.add(evidence.metadata_class)
    # Sim101 is NinjaTrader's unmistakable simulation identity even when an
    # operator/provider appends a descriptive suffix.  A simultaneous
    # evaluation/funded hint is conflict, not a reason to relabel it live.
    if "sim101" in evidence.display_name.lower() or "simulation" in facts:
        hints.add(AccountClass.LOCAL_SIMULATION)
    if "evaluation" in facts or " eval" in facts:
        hints.add(AccountClass.PROVIDER_EVALUATION)
    if "funded" in facts or "performance account" in facts:
        hints.add(AccountClass.PROVIDER_FUNDED)
    if "brokerage" in facts or "cash account" in facts or "live broker" in facts:
        hints.add(AccountClass.BROKERAGE_LIVE)
    hints.discard(AccountClass.UNKNOWN)
    if len(hints) != 1:
        return AccountClass.UNKNOWN, ("ACCOUNT_CLASSIFICATION_AMBIGUOUS",)
    account_class = next(iter(hints))
    return account_class, ("CLASSIFIED_FROM_CONCORDANT_METADATA",)


def create_attestation(
    evidence: AccountEvidence, classification: AccountClass, *, operator_confirmation: bool,
    attestation_id: str,
) -> AccountAttestation:
    """Create an unsigned artifact which must be visibly signed/stored locally."""

    classified, _ = classify_account(evidence)
    if classification is AccountClass.UNKNOWN or classified is not classification:
        raise ValueError("ATTESTATION_CLASSIFICATION_NOT_PROVEN")
    if not operator_confirmation:
        raise ValueError("ATTESTATION_OPERATOR_CONFIRMATION_REQUIRED")
    return AccountAttestation(
        schema="lane-iii-phase-h-account-attestation-v1", attestation_id=attestation_id,
        account_binding_hash=evidence.binding_hash, classification=classification,
        observed_evidence_hash=canonical_hash(asdict(evidence)), created_at=utc_now(), operator_confirmation=True,
    )
