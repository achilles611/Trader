"""Versioned, exact contract identities for the protected Pong path.

The historical SEP26 topology remains available for replay compatibility.  The
DEC26 topology is a separate bundle with its own state namespace and binding
hash; callers must select it explicitly or use the migration default.  This
module never grants execution authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from src.lane_iii.contracts import canonical_hash


@dataclass(frozen=True)
class PongContractTopology:
    version: str
    native_instrument: str
    canonical_contract: str
    expiry: str
    exchange: str = "CME"
    master_instrument: str = "MNQ"
    tick_size: Decimal = Decimal("0.25")
    point_value_dollars: Decimal = Decimal("2.00")
    account_name: str = "Sim101"
    account_class: str = "LOCAL_SIMULATION"
    live_authority: str = "DISARMED"
    live_capital: str = "DENIED"
    maximum_quantity: int = 1
    state_namespace: str = "pong/MNQU6/v1"

    def __post_init__(self) -> None:
        if not self.version.startswith("PONG_CONTRACT_TOPOLOGY_"):
            raise ValueError("PONG_CONTRACT_TOPOLOGY_VERSION_INVALID")
        if not self.native_instrument or not self.canonical_contract or not self.expiry:
            raise ValueError("PONG_CONTRACT_IDENTITY_INCOMPLETE")
        if self.account_name != "Sim101" or self.account_class != "LOCAL_SIMULATION":
            raise ValueError("PONG_CONTRACT_ACCOUNT_SCOPE_INVALID")
        if self.live_authority != "DISARMED" or self.live_capital != "DENIED":
            raise ValueError("PONG_CONTRACT_AUTHORITY_SCOPE_INVALID")
        if self.maximum_quantity != 1 or self.tick_size != Decimal("0.25"):
            raise ValueError("PONG_CONTRACT_RISK_METADATA_INVALID")
        if not self.state_namespace.startswith("pong/"):
            raise ValueError("PONG_CONTRACT_STATE_NAMESPACE_INVALID")

    def payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "native_instrument": self.native_instrument,
            "canonical_contract": self.canonical_contract,
            "expiry": self.expiry,
            "exchange": self.exchange,
            "master_instrument": self.master_instrument,
            "tick_size": str(self.tick_size),
            "point_value_dollars": str(self.point_value_dollars),
            "account_name": self.account_name,
            "account_class": self.account_class,
            "live_authority": self.live_authority,
            "live_capital": self.live_capital,
            "maximum_quantity": self.maximum_quantity,
            "state_namespace": self.state_namespace,
        }

    @property
    def binding_hash(self) -> str:
        return canonical_hash(self.payload())

    def matches(self, native_instrument: object, canonical_contract: object) -> bool:
        return native_instrument == self.native_instrument and canonical_contract == self.canonical_contract

    def require(self, native_instrument: object, canonical_contract: object) -> None:
        if not self.matches(native_instrument, canonical_contract):
            raise ValueError("PONG_CONTRACT_TOPOLOGY_IDENTITY_MISMATCH")


SEP26_TOPOLOGY = PongContractTopology(
    version="PONG_CONTRACT_TOPOLOGY_SEP26_V1",
    native_instrument="MNQ SEP26",
    canonical_contract="MNQU6",
    expiry="2026-09",
    state_namespace="pong/MNQU6/v1",
)

DEC26_TOPOLOGY = PongContractTopology(
    version="PONG_CONTRACT_TOPOLOGY_DEC26_V1",
    native_instrument="MNQ DEC26",
    canonical_contract="MNQZ6",
    expiry="2026-12",
    state_namespace="pong/MNQZ6/v1",
)

KNOWN_PONG_TOPOLOGIES: tuple[PongContractTopology, ...] = (SEP26_TOPOLOGY, DEC26_TOPOLOGY)
DEFAULT_PONG_TOPOLOGY = DEC26_TOPOLOGY


def topology_for_identity(native_instrument: object, canonical_contract: object) -> PongContractTopology | None:
    for topology in KNOWN_PONG_TOPOLOGIES:
        if topology.matches(native_instrument, canonical_contract):
            return topology
    return None


def topology_for_version(version: object) -> PongContractTopology | None:
    for topology in KNOWN_PONG_TOPOLOGIES:
        if topology.version == version:
            return topology
    return None


def resolve_pong_topology(value: str | None = None) -> PongContractTopology:
    """Resolve only exact selector names; unknown values never fall back."""
    selector = "DEC26" if value is None or not str(value).strip() else str(value).strip().upper()
    matches = {
        "SEP26": SEP26_TOPOLOGY,
        SEP26_TOPOLOGY.version: SEP26_TOPOLOGY,
        "DEC26": DEC26_TOPOLOGY,
        DEC26_TOPOLOGY.version: DEC26_TOPOLOGY,
    }
    try:
        return matches[selector]
    except KeyError as exc:
        raise ValueError("PONG_CONTRACT_TOPOLOGY_SELECTOR_UNKNOWN") from exc


def validate_topology_bundle(
    topology: PongContractTopology,
    bundle: Mapping[str, object],
) -> None:
    """Reject partial, stale, or mixed startup identities before processing."""
    if type(topology) is not PongContractTopology:
        raise ValueError("PONG_CONTRACT_TOPOLOGY_BUNDLE_TYPE_INVALID")
    expected = {
        "topology_version": topology.version,
        "topology_hash": topology.binding_hash,
        "native_instrument": topology.native_instrument,
        "canonical_contract": topology.canonical_contract,
        "state_namespace": topology.state_namespace,
        "account_name": topology.account_name,
        "account_class": topology.account_class,
        "live_authority": topology.live_authority,
        "live_capital": topology.live_capital,
        "maximum_quantity": topology.maximum_quantity,
    }
    if any(bundle.get(key) != value for key, value in expected.items()):
        raise ValueError("PONG_CONTRACT_TOPOLOGY_BUNDLE_MISMATCH")

