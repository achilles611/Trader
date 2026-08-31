"""Ephemeral, one-shot L3H.3 live-capital authorization boundary.

Historical evidence is durable, but authority exists only in this process and
is never reconstructed from the event store.  A live entry must pass this
boundary and the independently authenticated native AddOn boundary.  Safety
actions are deliberately classified outside the live-entry capability.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import hmac
import secrets
import threading
from typing import Mapping
from uuid import uuid4

from .contracts import CANONICAL_INSTRUMENT, NATIVE_INSTRUMENT, canonical_hash, canonical_json, parse_utc, utc_now
from .event_store import LiveEventStore


AUTHORIZATION_TTL_SECONDS = 60
PREFLIGHT_TTL_SECONDS = 60
RECONCILIATION_MAXIMUM_AGE_SECONDS = 15
LIVE_AUTHORITY_TYPE = "ONE_SHOT_LIVE_CANARY"
LIVE_ACCOUNT_CLASS = "LIVE_CAPITAL"
NATIVE_ADMISSION_SCHEMA = "lane-iii-phase-h-live-admission-v1"


class LiveAuthorizationState(StrEnum):
    DISARMED = "DISARMED"
    PREFLIGHT_PENDING = "PREFLIGHT_PENDING"
    PREFLIGHT_READY = "PREFLIGHT_READY"
    AUTHORIZATION_PENDING = "AUTHORIZATION_PENDING"
    CANARY_AUTHORIZED = "CANARY_AUTHORIZED"
    CANARY_CONSUMED = "CANARY_CONSUMED"
    VERIFYING_FLAT = "VERIFYING_FLAT"
    COMMISSIONED_DISARMED = "COMMISSIONED_DISARMED"
    QUARANTINED = "QUARANTINED"
    LOCKED = "LOCKED"


class AuthorizationAccountClass(StrEnum):
    LOCAL_SIMULATION = "LOCAL_SIMULATION"
    LIVE_CAPITAL = LIVE_ACCOUNT_CLASS
    UNKNOWN = "UNKNOWN"


class ActionClass(StrEnum):
    RISK_INCREASING = "RISK_INCREASING"
    RISK_REDUCING = "RISK_REDUCING"
    PROHIBITED = "PROHIBITED"


_RISK_INCREASING_ACTIONS = frozenset({"ENTER_LONG", "ENTER_SHORT", "SCALE_IN", "PYRAMID", "ATOMIC_REVERSAL"})
_RISK_REDUCING_ACTIONS = frozenset({"CANCEL", "CANCEL_OWNED_ORDERS", "PROTECT", "FLATTEN", "KILL_FLATTEN_DISARM", "EMERGENCY_LIQUIDATE"})


def classify_action(action: str) -> ActionClass:
    if action in _RISK_INCREASING_ACTIONS:
        return ActionClass.RISK_INCREASING
    if action in _RISK_REDUCING_ACTIONS:
        return ActionClass.RISK_REDUCING
    return ActionClass.PROHIBITED


def _is_hash(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _safe_time(value: str, name: str) -> datetime:
    return parse_utc(value, name)


def _time(value: str | None = None) -> datetime:
    return _safe_time(value or utc_now(), "L3H.3 authorization time")


def _utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class ExactLiveAccountIdentity:
    """Non-secret identity hashes derived from exact native platform facts."""

    safe_account_id: str
    account_fingerprint: str
    account_class: AuthorizationAccountClass
    provider_identity_hash: str
    connection_identity_hash: str
    metadata_fingerprint: str
    platform_provenance: str
    observed_at: str

    def __post_init__(self) -> None:
        if not self.safe_account_id or len(self.safe_account_id) > 64:
            raise ValueError("SAFE_ACCOUNT_ID_INVALID")
        for value, name in (
            (self.account_fingerprint, "ACCOUNT_FINGERPRINT"),
            (self.provider_identity_hash, "PROVIDER_IDENTITY"),
            (self.connection_identity_hash, "CONNECTION_IDENTITY"),
            (self.metadata_fingerprint, "METADATA_FINGERPRINT"),
        ):
            if not _is_hash(value):
                raise ValueError(name + "_INVALID")
        if self.platform_provenance != "NINJATRADER":
            raise ValueError("PLATFORM_PROVENANCE_INVALID")
        _safe_time(self.observed_at, "Account identity observation time")


def identity_from_native_metadata(
    metadata: Mapping[str, object], *, safe_account_id: str,
    account_class: AuthorizationAccountClass, observed_at: str,
) -> ExactLiveAccountIdentity:
    """Hash the same explicit fields used by the native NinjaTrader AddOn.

    The raw account identifier is consumed only for hashing and is never part
    of the returned identity or audit projection.
    """

    required = {
        "account_id", "account_name", "display_name", "fcm", "account_provider", "account_status",
        "connection_name", "connection_provider", "connection_brand", "connection_type", "connection_mode",
        "connection_is_demo", "connection_can_manage_orders", "connection_status",
    }
    if set(metadata) != required:
        raise ValueError("NATIVE_ACCOUNT_METADATA_FIELDS_INVALID")
    if not all(isinstance(metadata[name], str) for name in required - {"connection_is_demo", "connection_can_manage_orders"}):
        raise ValueError("NATIVE_ACCOUNT_METADATA_TYPES_INVALID")
    if not all(isinstance(metadata[name], bool) for name in {"connection_is_demo", "connection_can_manage_orders"}):
        raise ValueError("NATIVE_ACCOUNT_METADATA_TYPES_INVALID")
    connection = {
        "name": metadata["connection_name"], "provider": metadata["connection_provider"],
        "brand": metadata["connection_brand"], "type": metadata["connection_type"],
        "mode": metadata["connection_mode"], "is_demo": metadata["connection_is_demo"],
        "can_manage_orders": metadata["connection_can_manage_orders"],
        "connection_status": metadata["connection_status"],
    }
    connection_hash = canonical_hash(connection)
    provider = {
        "account_provider": metadata["account_provider"],
        "connection_provider": metadata["connection_provider"],
        "brand": metadata["connection_brand"], "fcm": metadata["fcm"],
    }
    provider_hash = canonical_hash(provider)
    account = {
        "platform": "NINJATRADER", "account_id": metadata["account_id"],
        "account_name": metadata["account_name"], "display_name": metadata["display_name"],
        "fcm": metadata["fcm"], "account_provider": metadata["account_provider"],
        "account_status": metadata["account_status"], "connection_identity_hash": connection_hash,
    }
    return ExactLiveAccountIdentity(
        safe_account_id=safe_account_id, account_fingerprint=canonical_hash(account),
        account_class=account_class, provider_identity_hash=provider_hash,
        connection_identity_hash=connection_hash,
        metadata_fingerprint=canonical_hash(dict(metadata)),
        platform_provenance="NINJATRADER", observed_at=observed_at,
    )


@dataclass(frozen=True)
class AuthorizationFacts:
    """Complete facts required at preflight and again at atomic admission."""

    observed_at: str
    account: ExactLiveAccountIdentity
    native_instrument: str
    canonical_contract: str
    maximum_quantity: int
    position: str
    quantity: int | None
    owned_working_entry_orders: int | None
    unresolved_owned_protective_orders: int | None
    foreign_or_unknown_activity: int | None
    gateway_authenticated: bool
    gateway_session_id: str
    addon_session_id: str
    addon_provenance: str
    addon_build_identity: str
    provider_connected: bool
    connection_fresh: bool
    reconciliation_status: str
    reconciliation_observed_at: str
    protection_status: str
    command_kill_ready: bool
    native_menu_kill_ready: bool
    out_of_band_kill_ready: bool
    beelzebub_build_identity: str
    strategy_runtime_identity: str
    runtime_session_id: str
    stale_or_unknown_state: bool = False
    quarantine_reason: str | None = None
    lock_reason: str | None = None

    def __post_init__(self) -> None:
        _safe_time(self.observed_at, "Authorization facts observation time")
        _safe_time(self.reconciliation_observed_at, "Reconciliation observation time")
        if self.position not in {"FLAT", "LONG", "SHORT", "UNKNOWN"}:
            raise ValueError("POSITION_STATE_INVALID")

    @property
    def digest(self) -> str:
        return canonical_hash(asdict(self))

    def blockers(self, *, now: str | None = None) -> tuple[str, ...]:
        observed = _time(now)
        blockers: list[str] = []
        if self.account.account_class is not AuthorizationAccountClass.LIVE_CAPITAL:
            blockers.append("BLOCKED_LIVE_ACCOUNT_IDENTITY")
        if self.account.platform_provenance != "NINJATRADER" or not all(_is_hash(value) for value in (
            self.account.account_fingerprint, self.account.provider_identity_hash,
            self.account.connection_identity_hash, self.account.metadata_fingerprint,
        )):
            blockers.append("BLOCKED_LIVE_ACCOUNT_IDENTITY")
        identity_age = observed - _safe_time(self.account.observed_at, "Account identity observation time")
        if identity_age < timedelta(0) or identity_age > timedelta(seconds=RECONCILIATION_MAXIMUM_AGE_SECONDS):
            blockers.append("STALE_ACCOUNT_OBSERVATION")
        facts_age = observed - _safe_time(self.observed_at, "Authorization facts observation time")
        if facts_age < timedelta(0) or facts_age > timedelta(seconds=RECONCILIATION_MAXIMUM_AGE_SECONDS):
            blockers.append("STALE_PREFLIGHT_FACTS")
        if self.native_instrument != NATIVE_INSTRUMENT or self.canonical_contract != CANONICAL_INSTRUMENT:
            blockers.append("WRONG_CONTRACT")
        if self.maximum_quantity != 1:
            blockers.append("QUANTITY_NOT_ONE")
        if self.position != "FLAT" or self.quantity != 0:
            blockers.append("POSITION_NOT_PROVEN_FLAT")
        if self.owned_working_entry_orders != 0:
            blockers.append("OWNED_WORKING_ENTRY_ORDERS_PRESENT")
        if self.unresolved_owned_protective_orders != 0:
            blockers.append("UNRESOLVED_PROTECTIVE_ORDERS_PRESENT")
        if self.foreign_or_unknown_activity != 0:
            blockers.append("FOREIGN_OR_UNKNOWN_ACTIVITY")
        if not self.gateway_authenticated or not self.gateway_session_id:
            blockers.append("GATEWAY_NOT_AUTHENTICATED")
        if not self.addon_session_id or not _is_hash(self.addon_provenance) or not _is_hash(self.addon_build_identity):
            blockers.append("BLOCKED_PROVENANCE")
        if not self.provider_connected or not self.connection_fresh:
            blockers.append("PROVIDER_DISCONNECTED_OR_STALE")
        reconciliation_age = observed - _safe_time(self.reconciliation_observed_at, "Reconciliation observation time")
        if self.reconciliation_status != "PASS" or reconciliation_age < timedelta(0) or reconciliation_age > timedelta(seconds=RECONCILIATION_MAXIMUM_AGE_SECONDS):
            blockers.append("BLOCKED_RECONCILIATION")
        if self.protection_status != "PASS":
            blockers.append("BLOCKED_PROTECTION")
        if not (self.command_kill_ready and self.native_menu_kill_ready and self.out_of_band_kill_ready):
            blockers.append("KILL_PATH_NOT_READY")
        if not _is_hash(self.beelzebub_build_identity) or not self.strategy_runtime_identity or not self.runtime_session_id:
            blockers.append("BUILD_OR_RUNTIME_IDENTITY_INVALID")
        if self.stale_or_unknown_state:
            blockers.append("UNKNOWN_STATE")
        if self.quarantine_reason:
            blockers.append("QUARANTINE_PRESENT")
        if self.lock_reason:
            blockers.append("LOCK_PRESENT")
        return tuple(dict.fromkeys(blockers))


def _critical_binding(facts: AuthorizationFacts) -> str:
    """Digest every admission-critical fact while permitting fresher timestamps."""

    return canonical_hash({
        "account": asdict(facts.account) | {"observed_at": "FRESH_NATIVE_OBSERVATION"},
        "native_instrument": facts.native_instrument, "canonical_contract": facts.canonical_contract,
        "maximum_quantity": facts.maximum_quantity, "position": facts.position, "quantity": facts.quantity,
        "owned_working_entry_orders": facts.owned_working_entry_orders,
        "unresolved_owned_protective_orders": facts.unresolved_owned_protective_orders,
        "foreign_or_unknown_activity": facts.foreign_or_unknown_activity,
        "gateway_authenticated": facts.gateway_authenticated, "gateway_session_id": facts.gateway_session_id,
        "addon_session_id": facts.addon_session_id, "addon_provenance": facts.addon_provenance,
        "addon_build_identity": facts.addon_build_identity, "provider_connected": facts.provider_connected,
        "connection_fresh": facts.connection_fresh, "reconciliation_status": facts.reconciliation_status,
        "protection_status": facts.protection_status, "command_kill_ready": facts.command_kill_ready,
        "native_menu_kill_ready": facts.native_menu_kill_ready, "out_of_band_kill_ready": facts.out_of_band_kill_ready,
        "beelzebub_build_identity": facts.beelzebub_build_identity,
        "strategy_runtime_identity": facts.strategy_runtime_identity, "runtime_session_id": facts.runtime_session_id,
        "stale_or_unknown_state": facts.stale_or_unknown_state,
        "quarantine_reason": facts.quarantine_reason, "lock_reason": facts.lock_reason,
    })


@dataclass(frozen=True)
class PreflightRecord:
    preflight_id: str
    created_at: str
    expires_at: str
    nonce: str
    challenge: str
    facts: AuthorizationFacts
    blockers: tuple[str, ...]
    digest: str

    @classmethod
    def create(cls, facts: AuthorizationFacts, blockers: tuple[str, ...], *, now: str | None = None) -> "PreflightRecord":
        created = _time(now)
        values: dict[str, object] = {
            "preflight_id": "l3h3-preflight-" + uuid4().hex,
            "created_at": _utc(created),
            "expires_at": _utc(created + timedelta(seconds=PREFLIGHT_TTL_SECONDS)),
            "nonce": "l3h3-preflight-nonce-" + secrets.token_hex(24),
            "challenge": "l3h3-challenge-" + secrets.token_hex(24),
            "facts": facts,
            "blockers": blockers,
        }
        digest = canonical_hash({**values, "facts": asdict(facts), "blockers": list(blockers)})
        return cls(**values, digest=digest)  # type: ignore[arg-type]


@dataclass(frozen=True)
class AuthorizationChallenge:
    preflight_id: str
    preflight_digest: str
    challenge: str
    expires_at: str
    safe_account_id: str
    account_class: str
    native_instrument: str
    quantity: int
    authority_type: str
    acknowledgement: str


@dataclass(frozen=True)
class HumanAuthorization:
    preflight_id: str
    preflight_digest: str
    challenge: str
    safe_account_id: str
    account_class: str
    native_instrument: str
    quantity: int
    authority_type: str
    acknowledgement: str
    actor_type: str
    local_transport: bool


@dataclass(frozen=True)
class LiveEntryCapability:
    capability_id: str
    issued_at: str
    expires_at: str
    nonce: str
    authorization_session_id: str
    preflight_id: str
    preflight_digest: str
    account_fingerprint: str
    account_class: str
    provider_identity_hash: str
    connection_identity_hash: str
    native_instrument: str
    canonical_contract: str
    quantity: int
    beelzebub_build_identity: str
    addon_provenance: str
    addon_session_id: str
    gateway_session_id: str
    authority_type: str
    signature: str = ""

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result.pop("signature")
        return result

    def signed(self, key: bytes) -> "LiveEntryCapability":
        signature = hmac.new(key, canonical_json(self.payload()), hashlib.sha256).hexdigest()
        return LiveEntryCapability(**{**asdict(self), "signature": signature})

    def verify(self, key: bytes, *, now: str | None = None) -> None:
        expected = hmac.new(key, canonical_json(self.payload()), hashlib.sha256).hexdigest()
        if not self.signature or not hmac.compare_digest(expected, self.signature):
            raise ValueError("LIVE_CAPABILITY_SIGNATURE_INVALID")
        if _time(now) >= _safe_time(self.expires_at, "Live capability expiry"):
            raise ValueError("LIVE_CAPABILITY_EXPIRED")


@dataclass(frozen=True)
class LiveEntryRequest:
    request_id: str
    strategy_signal_id: str
    action: str
    account_fingerprint: str
    account_class: str
    native_instrument: str
    canonical_contract: str
    quantity: int
    resulting_position_quantity: int


@dataclass(frozen=True)
class NativeAdmissionEnvelope:
    schema: str
    authorization_id: str
    authorization_session_id: str
    addon_session_id: str
    gateway_session_id: str
    preflight_digest: str
    admission_facts_digest: str
    account_fingerprint: str
    account_class: str
    provider_identity_hash: str
    connection_identity_hash: str
    native_instrument: str
    canonical_contract: str
    quantity: int
    action: str
    command_id: str
    request_id: str
    nonce: str
    issued_at: str
    expires_at: str
    beelzebub_build_identity: str
    addon_provenance: str
    signature: str = ""

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result.pop("signature")
        return result

    def signed(self, key: bytes) -> "NativeAdmissionEnvelope":
        signature = hmac.new(key, canonical_json(self.payload()), hashlib.sha256).hexdigest()
        return NativeAdmissionEnvelope(**{**asdict(self), "signature": signature})

    def as_mapping(self) -> dict[str, object]:
        return asdict(self)


def verify_native_admission_envelope(
    envelope: Mapping[str, object], key: bytes, *, authorization_session_id: str,
    addon_session_id: str, gateway_session_id: str, command: Mapping[str, object], now: str | None = None,
) -> None:
    required = {field.name for field in NativeAdmissionEnvelope.__dataclass_fields__.values()}
    if set(envelope) != required:
        raise ValueError("LIVE_AUTHORIZATION_FIELDS_INVALID")
    signature = envelope.get("signature")
    payload = {name: envelope[name] for name in required if name != "signature"}
    expected = hmac.new(key, canonical_json(payload), hashlib.sha256).hexdigest()
    if not isinstance(signature, str) or not hmac.compare_digest(expected, signature):
        raise ValueError("LIVE_AUTHORIZATION_SIGNATURE_INVALID")
    if envelope.get("schema") != NATIVE_ADMISSION_SCHEMA:
        raise ValueError("LIVE_AUTHORIZATION_SCHEMA_INVALID")
    if envelope.get("authorization_session_id") != authorization_session_id:
        raise ValueError("LIVE_AUTHORIZATION_SESSION_MISMATCH")
    if envelope.get("addon_session_id") != addon_session_id or envelope.get("gateway_session_id") != gateway_session_id:
        raise ValueError("LIVE_AUTHORIZATION_TRANSPORT_SESSION_MISMATCH")
    if envelope.get("command_id") != command.get("command_id") or envelope.get("request_id") != command.get("request_id"):
        raise ValueError("LIVE_AUTHORIZATION_COMMAND_MISMATCH")
    for field in ("account_fingerprint", "account_class", "native_instrument", "canonical_contract", "quantity", "action"):
        if envelope.get(field) != command.get(field):
            raise ValueError("LIVE_AUTHORIZATION_BINDING_MISMATCH")
    observed = _time(now)
    issued = _safe_time(str(envelope.get("issued_at")), "Native admission issue time")
    expires = _safe_time(str(envelope.get("expires_at")), "Native admission expiry")
    if observed < issued or observed >= expires or expires - issued > timedelta(seconds=AUTHORIZATION_TTL_SECONDS):
        raise ValueError("LIVE_AUTHORIZATION_EXPIRED")


class LiveAuthorizationBoundary:
    """Thread-safe in-memory authority lease with durable transition evidence."""

    def __init__(
        self, store: LiveEventStore, *, native_hmac_key: bytes,
        beelzebub_build_identity: str, expected_addon_provenance: str,
    ) -> None:
        if not isinstance(native_hmac_key, bytes) or len(native_hmac_key) < 32:
            raise ValueError("L3H3_NATIVE_HMAC_KEY_INVALID")
        if not _is_hash(beelzebub_build_identity) or not _is_hash(expected_addon_provenance):
            raise ValueError("L3H3_BUILD_PROVENANCE_INVALID")
        self.store = store
        self._native_hmac_key = native_hmac_key
        self._session_key = secrets.token_bytes(32)
        self.beelzebub_build_identity = beelzebub_build_identity
        self.expected_addon_provenance = expected_addon_provenance
        self.authorization_session_id = "l3h3-auth-session-" + uuid4().hex
        self.state = LiveAuthorizationState.DISARMED
        self._preflight: PreflightRecord | None = None
        self._challenge: AuthorizationChallenge | None = None
        self._capability: LiveEntryCapability | None = None
        self._consumed_authorizations: set[str] = set()
        self._denial_reason: str | None = None
        self._lock = threading.RLock()
        self._stream_id = "live-authorization:" + canonical_hash(self.authorization_session_id)[:24]
        self._record(self.state, "PROCESS_START_DISARMED", "SYSTEM")

    @property
    def preflight(self) -> PreflightRecord | None:
        return self._preflight

    @property
    def challenge(self) -> AuthorizationChallenge | None:
        return self._challenge

    @property
    def capability_id(self) -> str | None:
        return None if self._capability is None else self._capability.capability_id

    def start_preflight(self, facts: AuthorizationFacts, *, now: str | None = None) -> PreflightRecord:
        with self._lock:
            if self.state in {LiveAuthorizationState.QUARANTINED, LiveAuthorizationState.LOCKED}:
                raise ValueError("SAFETY_LATCH_REQUIRES_PROCESS_RESTART_AND_FRESH_PREFLIGHT")
            if self._capability is not None:
                self._transition(
                    LiveAuthorizationState.DISARMED, "PREFLIGHT_SUPERSEDED_PRIOR_AUTHORITY", "LOCAL_HUMAN",
                )
            self._clear_authority()
            self._transition(LiveAuthorizationState.PREFLIGHT_PENDING, "PREFLIGHT_REQUESTED", "LOCAL_HUMAN")
            blockers = list(facts.blockers(now=now))
            if facts.addon_provenance != self.expected_addon_provenance:
                blockers.append("BLOCKED_PROVENANCE")
            if facts.beelzebub_build_identity != self.beelzebub_build_identity:
                blockers.append("BUILD_OR_RUNTIME_IDENTITY_INVALID")
            blockers = list(dict.fromkeys(blockers))
            record = PreflightRecord.create(facts, tuple(blockers), now=now)
            self._preflight = record
            if blockers:
                self._denial_reason = blockers[0]
                target = LiveAuthorizationState.QUARANTINED if any(value in blockers for value in (
                    "FOREIGN_OR_UNKNOWN_ACTIVITY", "UNKNOWN_STATE", "QUARANTINE_PRESENT",
                )) else LiveAuthorizationState.LOCKED if "LOCK_PRESENT" in blockers else LiveAuthorizationState.DISARMED
                self._transition(target, blockers[0], "SYSTEM", denial_reason=blockers[0])
            else:
                self._transition(LiveAuthorizationState.PREFLIGHT_READY, "PREFLIGHT_ALL_GATES_GREEN", "SYSTEM")
            return record

    def begin_authorization(self, *, preflight_id: str, preflight_digest: str, now: str | None = None) -> AuthorizationChallenge:
        with self._lock:
            preflight = self._require_fresh_preflight(preflight_id, preflight_digest, now=now)
            if self.state is not LiveAuthorizationState.PREFLIGHT_READY or preflight.blockers:
                raise ValueError("PREFLIGHT_NOT_READY")
            facts = preflight.facts
            acknowledgement = " | ".join((
                "AUTHORIZE ONE LIVE CANARY", facts.account.safe_account_id, LIVE_ACCOUNT_CLASS,
                NATIVE_INSTRUMENT, "QTY 1", LIVE_AUTHORITY_TYPE,
            ))
            challenge = AuthorizationChallenge(
                preflight.preflight_id, preflight.digest, preflight.challenge, preflight.expires_at,
                facts.account.safe_account_id, LIVE_ACCOUNT_CLASS, NATIVE_INSTRUMENT, 1,
                LIVE_AUTHORITY_TYPE, acknowledgement,
            )
            self._challenge = challenge
            self._transition(LiveAuthorizationState.AUTHORIZATION_PENDING, "EXACT_HUMAN_ACKNOWLEDGEMENT_REQUIRED", "SYSTEM")
            return challenge

    def authorize(self, authorization: HumanAuthorization, *, now: str | None = None) -> str:
        with self._lock:
            if self.state is not LiveAuthorizationState.AUTHORIZATION_PENDING or self._challenge is None:
                raise ValueError("AUTHORIZATION_NOT_PENDING")
            preflight = self._require_fresh_preflight(authorization.preflight_id, authorization.preflight_digest, now=now)
            challenge = self._challenge
            exact = (
                authorization.challenge == challenge.challenge,
                authorization.safe_account_id == challenge.safe_account_id,
                authorization.account_class == challenge.account_class,
                authorization.native_instrument == challenge.native_instrument,
                authorization.quantity == challenge.quantity,
                authorization.authority_type == challenge.authority_type,
                authorization.acknowledgement == challenge.acknowledgement,
                authorization.actor_type == "LOCAL_HUMAN",
                authorization.local_transport is True,
            )
            if not all(exact):
                self._deny_disarm("HUMAN_AUTHORIZATION_MISMATCH")
                raise ValueError("HUMAN_AUTHORIZATION_MISMATCH")
            issued = _time(now)
            preflight_expiry = _safe_time(preflight.expires_at, "Preflight expiry")
            expires = min(issued + timedelta(seconds=AUTHORIZATION_TTL_SECONDS), preflight_expiry)
            if expires <= issued:
                self._deny_disarm("AUTHORIZATION_EXPIRED")
                raise ValueError("AUTHORIZATION_EXPIRED")
            facts = preflight.facts
            capability = LiveEntryCapability(
                capability_id="l3h3-canary-cap-" + uuid4().hex, issued_at=_utc(issued), expires_at=_utc(expires),
                nonce="l3h3-capability-nonce-" + secrets.token_hex(24), authorization_session_id=self.authorization_session_id,
                preflight_id=preflight.preflight_id, preflight_digest=preflight.digest,
                account_fingerprint=facts.account.account_fingerprint, account_class=LIVE_ACCOUNT_CLASS,
                provider_identity_hash=facts.account.provider_identity_hash,
                connection_identity_hash=facts.account.connection_identity_hash,
                native_instrument=NATIVE_INSTRUMENT, canonical_contract=CANONICAL_INSTRUMENT, quantity=1,
                beelzebub_build_identity=self.beelzebub_build_identity, addon_provenance=self.expected_addon_provenance,
                addon_session_id=facts.addon_session_id, gateway_session_id=facts.gateway_session_id,
                authority_type=LIVE_AUTHORITY_TYPE,
            ).signed(self._session_key)
            self._capability = capability
            self._transition(LiveAuthorizationState.CANARY_AUTHORIZED, "EXACT_LOCAL_HUMAN_AUTHORIZATION_ACCEPTED", "LOCAL_HUMAN")
            return capability.capability_id

    def atomic_admit(
        self, capability_id: str, request: LiveEntryRequest, current_facts: AuthorizationFacts,
        *, command_id: str, now: str | None = None,
    ) -> NativeAdmissionEnvelope:
        """Consume one live-entry capability and return a native-bound envelope."""

        with self._lock:
            capability = self._capability
            if self.state is not LiveAuthorizationState.CANARY_AUTHORIZED or capability is None:
                self._record(
                    self.state, "CAPABILITY_REPLAY_OR_AUTHORITY_DISARMED", "SYSTEM",
                    previous=self.state, denial_reason="LIVE_AUTHORITY_DISARMED",
                )
                raise ValueError("LIVE_AUTHORITY_DISARMED")
            if capability_id != capability.capability_id or capability_id in self._consumed_authorizations:
                self._deny_disarm("CAPABILITY_REPLAY")
                raise ValueError("CAPABILITY_REPLAY")
            try:
                capability.verify(self._session_key, now=now)
            except ValueError as error:
                self._deny_disarm(str(error))
                raise
            if classify_action(request.action) is not ActionClass.RISK_INCREASING or request.action not in {"ENTER_LONG", "ENTER_SHORT"}:
                self._deny_disarm("RISK_INCREASING_ACTION_NOT_CANARY_ENTRY")
                raise ValueError("RISK_INCREASING_ACTION_NOT_CANARY_ENTRY")
            expected_resulting_quantity = 1 if request.action == "ENTER_LONG" else -1
            if request.resulting_position_quantity != expected_resulting_quantity:
                self._deny_disarm("REVERSAL_OR_SCALE_IN_DENIED")
                raise ValueError("REVERSAL_OR_SCALE_IN_DENIED")
            blockers = current_facts.blockers(now=now)
            if blockers:
                self._deny_or_quarantine(blockers[0])
                raise ValueError(blockers[0])
            prior = self._preflight
            if prior is None:
                self._deny_disarm("PREFLIGHT_MISSING")
                raise ValueError("PREFLIGHT_MISSING")
            if _critical_binding(current_facts) != _critical_binding(prior.facts):
                self._deny_or_quarantine("PREFLIGHT_CRITICAL_FACTS_CHANGED")
                raise ValueError("PREFLIGHT_CRITICAL_FACTS_CHANGED")
            bindings = (
                current_facts.account.account_fingerprint == capability.account_fingerprint,
                current_facts.account.provider_identity_hash == capability.provider_identity_hash,
                current_facts.account.connection_identity_hash == capability.connection_identity_hash,
                current_facts.addon_session_id == capability.addon_session_id,
                current_facts.gateway_session_id == capability.gateway_session_id,
                current_facts.addon_provenance == capability.addon_provenance,
                current_facts.beelzebub_build_identity == capability.beelzebub_build_identity,
                request.account_fingerprint == capability.account_fingerprint,
                request.account_class == LIVE_ACCOUNT_CLASS,
                request.native_instrument == NATIVE_INSTRUMENT,
                request.canonical_contract == CANONICAL_INSTRUMENT,
                request.quantity == 1,
            )
            if not all(bindings):
                self._deny_or_quarantine("ATOMIC_ADMISSION_BINDING_MISMATCH")
                raise ValueError("ATOMIC_ADMISSION_BINDING_MISMATCH")
            issued = _time(now)
            envelope = NativeAdmissionEnvelope(
                schema=NATIVE_ADMISSION_SCHEMA, authorization_id=capability.capability_id,
                authorization_session_id=self.authorization_session_id,
                addon_session_id=current_facts.addon_session_id, gateway_session_id=current_facts.gateway_session_id,
                preflight_digest=capability.preflight_digest, admission_facts_digest=current_facts.digest,
                account_fingerprint=capability.account_fingerprint, account_class=LIVE_ACCOUNT_CLASS,
                provider_identity_hash=capability.provider_identity_hash,
                connection_identity_hash=capability.connection_identity_hash,
                native_instrument=NATIVE_INSTRUMENT, canonical_contract=CANONICAL_INSTRUMENT, quantity=1,
                action=request.action, command_id=command_id, request_id=request.request_id,
                nonce="l3h3-admission-nonce-" + secrets.token_hex(24), issued_at=_utc(issued),
                expires_at=capability.expires_at, beelzebub_build_identity=capability.beelzebub_build_identity,
                addon_provenance=capability.addon_provenance,
            ).signed(self._native_hmac_key)
            self._consumed_authorizations.add(capability.capability_id)
            self._transition(LiveAuthorizationState.CANARY_CONSUMED, "ONE_SHOT_CAPABILITY_ATOMICALLY_CONSUMED", "SYSTEM")
            return envelope

    def admit_risk_reducing(self, action: str) -> tuple[bool, str]:
        """Safety actions never depend on live-entry authority."""

        if classify_action(action) is ActionClass.RISK_REDUCING:
            return True, "RISK_REDUCTION_INDEPENDENT_OF_ENTRY_AUTHORITY"
        if action == "ATOMIC_REVERSAL":
            return False, "REVERSAL_REQUIRES_FLATTEN_RECONCILE_FRESH_AUTHORIZATION"
        return False, "ACTION_NOT_RISK_REDUCING"

    def disarm(self, reason: str = "OPERATOR_DISARM") -> None:
        with self._lock:
            self._transition(LiveAuthorizationState.DISARMED, reason or "OPERATOR_DISARM", "LOCAL_HUMAN")
            self._clear_authority()

    def mark_verifying_flat(self) -> None:
        with self._lock:
            if self.state is not LiveAuthorizationState.CANARY_CONSUMED:
                raise ValueError("CANARY_NOT_CONSUMED")
            self._transition(LiveAuthorizationState.VERIFYING_FLAT, "POST_CANARY_RECONCILIATION_REQUIRED", "SYSTEM")

    def mark_commissioned_disarmed(self, facts: AuthorizationFacts, *, now: str | None = None) -> None:
        with self._lock:
            if self.state is not LiveAuthorizationState.VERIFYING_FLAT or facts.blockers(now=now):
                raise ValueError("POST_CANARY_FLAT_NOT_PROVEN")
            self._transition(LiveAuthorizationState.COMMISSIONED_DISARMED, "POST_CANARY_FLAT_PROVEN_DISARMED", "SYSTEM")
            self._clear_authority()

    def status(self, *, now: str | None = None) -> dict[str, object]:
        with self._lock:
            capability = self._capability
            preflight = self._preflight
            observed = _time(now)
            preflight_age = None if preflight is None else max(0.0, (observed - _safe_time(preflight.created_at, "Preflight creation time")).total_seconds())
            return {
                "schema": "lane-iii-phase-h3-authorization-status-v1",
                "authorization_boundary": "READY" if self.state in {
                    LiveAuthorizationState.PREFLIGHT_READY, LiveAuthorizationState.AUTHORIZATION_PENDING,
                    LiveAuthorizationState.CANARY_AUTHORIZED, LiveAuthorizationState.CANARY_CONSUMED,
                    LiveAuthorizationState.VERIFYING_FLAT, LiveAuthorizationState.COMMISSIONED_DISARMED,
                } else "IMPLEMENTED",
                "state": self.state.value,
                "live_authority": "DISARMED" if self.state is not LiveAuthorizationState.CANARY_AUTHORIZED else "ONE_SHOT_AUTHORIZED",
                "live_canary": "NOT_RUN" if self.state not in {LiveAuthorizationState.VERIFYING_FLAT, LiveAuthorizationState.COMMISSIONED_DISARMED} else "REQUIRES_EVIDENCE",
                "preflight_age_seconds": preflight_age,
                "authorization_expires_at": None if capability is None else capability.expires_at,
                "authorized_account": None if preflight is None else preflight.facts.account.safe_account_id,
                "authorized_account_fingerprint": None if preflight is None else preflight.facts.account.account_fingerprint,
                "account_class": None if preflight is None else preflight.facts.account.account_class.value,
                "contract": NATIVE_INSTRUMENT,
                "maximum_quantity": 1,
                "preflight_digest": None if preflight is None else preflight.digest,
                "capability_id_hash": None if capability is None else canonical_hash(capability.capability_id),
                "quarantine": self.state is LiveAuthorizationState.QUARANTINED,
                "locked": self.state is LiveAuthorizationState.LOCKED,
                "denial_reason": self._denial_reason,
            }

    def _require_fresh_preflight(self, preflight_id: str, preflight_digest: str, *, now: str | None) -> PreflightRecord:
        preflight = self._preflight
        if preflight is None or preflight.preflight_id != preflight_id or preflight.digest != preflight_digest:
            self._deny_disarm("PREFLIGHT_DIGEST_MISMATCH")
            raise ValueError("PREFLIGHT_DIGEST_MISMATCH")
        if _time(now) >= _safe_time(preflight.expires_at, "Preflight expiry"):
            self._deny_disarm("PREFLIGHT_EXPIRED")
            raise ValueError("PREFLIGHT_EXPIRED")
        return preflight

    def _clear_authority(self) -> None:
        self._challenge = None
        self._capability = None

    def _deny_disarm(self, reason: str) -> None:
        self._denial_reason = reason
        self._transition(LiveAuthorizationState.DISARMED, reason, "SYSTEM", denial_reason=reason)
        self._clear_authority()

    def _deny_or_quarantine(self, reason: str) -> None:
        self._denial_reason = reason
        target = LiveAuthorizationState.QUARANTINED if reason in {
            "FOREIGN_OR_UNKNOWN_ACTIVITY", "UNKNOWN_STATE", "ATOMIC_ADMISSION_BINDING_MISMATCH",
            "PREFLIGHT_CRITICAL_FACTS_CHANGED",
        } else LiveAuthorizationState.DISARMED
        self._transition(target, reason, "SYSTEM", denial_reason=reason)
        self._clear_authority()

    def _transition(
        self, next_state: LiveAuthorizationState, reason: str, actor_type: str,
        *, denial_reason: str | None = None,
    ) -> None:
        previous = self.state
        self.state = next_state
        self._record(next_state, reason, actor_type, previous=previous, denial_reason=denial_reason)

    def _record(
        self, next_state: LiveAuthorizationState, reason: str, actor_type: str,
        *, previous: LiveAuthorizationState | None = None, denial_reason: str | None = None,
    ) -> None:
        preflight = self._preflight
        capability = self._capability
        facts = None if preflight is None else preflight.facts
        self.store.append(self._stream_id, "LIVE_AUTHORITY_TRANSITION", {
            "previous_state": (previous or LiveAuthorizationState.DISARMED).value,
            "next_state": next_state.value,
            "reason": reason,
            "preflight_digest": None if preflight is None else preflight.digest,
            "capability_id_hash": None if capability is None else canonical_hash(capability.capability_id),
            "account_fingerprint": None if facts is None else facts.account.account_fingerprint,
            "account_class": None if facts is None else facts.account.account_class.value,
            "instrument": NATIVE_INSTRUMENT,
            "quantity": 1,
            "build_identity": self.beelzebub_build_identity,
            "addon_provenance": self.expected_addon_provenance,
            "actor_type": actor_type,
            "denial_reason": denial_reason,
            "authorization_session_hash": canonical_hash(self.authorization_session_id),
        })
