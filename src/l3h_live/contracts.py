"""Signed, local-only authority contracts for Lane III-H.

The tracked source contains only schemas and templates.  A real capability is
created outside Git, signed with an ACL-restricted local key, and is rejected
unless every identity and policy binding is exact.  There is intentionally no
fallback capability and no configuration flag that changes a paper capability
into a live one.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
import hashlib
import hmac
import json
from pathlib import Path
import re
from typing import Any, Mapping


CAPABILITY_SCHEMA = "lane-iii-phase-h-live-capability-v1"
CANONICAL_INSTRUMENT = "MNQU6"
NATIVE_INSTRUMENT = "MNQ SEP26"
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_ALIAS = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")


class AccountClass(StrEnum):
    LOCAL_SIMULATION = "LOCAL_SIMULATION"
    PROVIDER_EVALUATION = "PROVIDER_EVALUATION"
    PROVIDER_FUNDED = "PROVIDER_FUNDED"
    BROKERAGE_LIVE = "BROKERAGE_LIVE"
    UNKNOWN = "UNKNOWN"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field_name} must be an RFC3339 UTC value ending in Z.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a valid RFC3339 UTC value.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return parsed.astimezone(timezone.utc)


def canonical_json(value: object) -> bytes:
    def convert(item: object) -> object:
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, StrEnum):
            return item.value
        if isinstance(item, Mapping):
            return {str(key): convert(value) for key, value in item.items()}
        if isinstance(item, tuple):
            return [convert(value) for value in item]
        if isinstance(item, list):
            return [convert(value) for value in item]
        return item

    return json.dumps(convert(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def canonical_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _require_hash(value: str, name: str) -> None:
    if not isinstance(value, str) or _HEX_64.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hash.")


@dataclass(frozen=True)
class LiveCapability:
    """A single commissioning-epoch authority artifact.

    ``account_alias`` is an operator-facing alias only.  The actual account
    identity remains outside Git and is bound by ``account_binding_hash``.
    """

    schema: str
    capability_id: str
    created_at: str
    expires_at: str
    account_alias: str
    account_binding_hash: str
    account_class: AccountClass
    connection_identity_hash: str
    native_instrument: str
    canonical_contract: str
    exchange: str
    tick_size: Decimal
    tick_value_dollars: Decimal
    point_value_dollars: Decimal
    maximum_quantity: int
    live_capital: bool
    policy_hash: str
    risk_hash: str
    prop_rule_hash: str
    strategy_artifact_hash: str
    source_fingerprint: str
    ninjatrader_build_fingerprint: str
    allowed_session_profiles: tuple[str, ...]
    commissioning_epoch: str
    activation_nonce_family: str
    revoked: bool = False
    signature: str = ""

    def __post_init__(self) -> None:
        if self.schema != CAPABILITY_SCHEMA:
            raise ValueError("Unsupported L3H capability schema.")
        if not isinstance(self.capability_id, str) or not self.capability_id.startswith("l3h-cap-"):
            raise ValueError("Capability ID must use the l3h-cap namespace.")
        created = parse_utc(self.created_at, "Capability creation time")
        expires = parse_utc(self.expires_at, "Capability expiry")
        if expires <= created:
            raise ValueError("Capability expiry must be after creation.")
        if not _ALIAS.fullmatch(self.account_alias):
            raise ValueError("Account alias is not sanitized.")
        if self.account_class is AccountClass.UNKNOWN:
            raise ValueError("An unknown account class cannot receive capability authority.")
        if self.native_instrument != NATIVE_INSTRUMENT or self.canonical_contract != CANONICAL_INSTRUMENT:
            raise ValueError("L3H v1 is sealed to the exact MNQ SEP26 contract.")
        if self.exchange != "CME" or self.maximum_quantity != 1:
            raise ValueError("L3H v1 permits exactly one MNQ on CME.")
        if (self.tick_size, self.tick_value_dollars, self.point_value_dollars) != (
            Decimal("0.25"), Decimal("0.50"), Decimal("2.00"),
        ):
            raise ValueError("MNQ tick economics do not match the sealed L3H contract.")
        if self.live_capital and self.account_class not in {AccountClass.PROVIDER_FUNDED, AccountClass.BROKERAGE_LIVE}:
            raise ValueError("Simulation and evaluation accounts can never carry live-capital authority.")
        if not self.allowed_session_profiles or any(not isinstance(item, str) or not item for item in self.allowed_session_profiles):
            raise ValueError("Capability requires at least one exact session profile.")
        if not self.commissioning_epoch.startswith("l3h-") or not self.activation_nonce_family.startswith("l3h-activation-"):
            raise ValueError("Capability epoch and activation nonce family are invalid.")
        for value, name in (
            (self.account_binding_hash, "Account binding"),
            (self.connection_identity_hash, "Connection identity"),
            (self.policy_hash, "Policy"),
            (self.risk_hash, "Risk"),
            (self.prop_rule_hash, "Prop-rule"),
            (self.strategy_artifact_hash, "Strategy artifact"),
            (self.source_fingerprint, "Source fingerprint"),
            (self.ninjatrader_build_fingerprint, "NinjaTrader build fingerprint"),
        ):
            _require_hash(value, name)
        if self.signature:
            _require_hash(self.signature, "Capability signature")

    @property
    def capability_hash(self) -> str:
        return canonical_hash(self.payload())

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result.pop("signature", None)
        return result

    def signed(self, key: bytes) -> "LiveCapability":
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("Capability signing key must contain at least 256 bits.")
        signature = hmac.new(key, canonical_json(self.payload()), hashlib.sha256).hexdigest()
        return LiveCapability(**{**asdict(self), "signature": signature})

    def verify(self, key: bytes, *, now: str | None = None) -> None:
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("Capability verification key must contain at least 256 bits.")
        if self.revoked:
            raise ValueError("CAPABILITY_REVOKED")
        if not self.signature:
            raise ValueError("CAPABILITY_UNSIGNED")
        expected = hmac.new(key, canonical_json(self.payload()), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, self.signature):
            raise ValueError("CAPABILITY_SIGNATURE_INVALID")
        observed = parse_utc(now or utc_now(), "Capability verification time")
        if observed >= parse_utc(self.expires_at, "Capability expiry"):
            raise ValueError("CAPABILITY_EXPIRED")


def load_capability(path: str | Path) -> LiveCapability:
    """Load schema-exact JSON; callers must still verify it with a local key."""

    source = Path(path).expanduser().resolve()
    raw: Any = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Capability artifact must be a JSON object.")
    expected = {field.name for field in LiveCapability.__dataclass_fields__.values()}
    if set(raw) != expected:
        raise ValueError("Capability artifact fields do not match the sealed schema.")
    raw["account_class"] = AccountClass(raw["account_class"])
    raw["tick_size"] = Decimal(str(raw["tick_size"]))
    raw["tick_value_dollars"] = Decimal(str(raw["tick_value_dollars"]))
    raw["point_value_dollars"] = Decimal(str(raw["point_value_dollars"]))
    raw["allowed_session_profiles"] = tuple(raw["allowed_session_profiles"])
    return LiveCapability(**raw)
