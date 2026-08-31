"""Lane III-H isolated live-capital authority boundary.

This package deliberately does not import :mod:`src.l3g_paper`.  L3G remains
the independently testable Sim101 paper capability; L3H starts disarmed and
can acquire authority only from a locally stored, signed capability artifact.
"""

from .authority import LiveReadiness, ReadinessGate, derive_terminal_status
from .bootstrap import AccountAttestation, AccountEvidence, NativeCapabilityBinding, classify_account
from .contracts import AccountClass, LiveCapability, load_capability, load_verified_capability
from .event_store import LiveEventStore
from .gateway import AuthenticatedLoopbackGateway, NoDispatchLiveGateway
from .lifecycle import ExecutionLifecycle, OrderLifecycleState, ProtectionLifecycle, ProtectionState
from .live_authorization import (
    ActionClass, AuthorizationAccountClass, AuthorizationFacts, ExactLiveAccountIdentity,
    HumanAuthorization, LiveAuthorizationBoundary, LiveAuthorizationState, LiveEntryRequest,
    classify_action, identity_from_native_metadata,
)
from .reconciliation import ExecutionSupervisor
from .risk import LiveCanaryRiskProfile, LiveRiskAuthority
from .runtime import LiveRuntime, LiveRuntimeState

__all__ = [
    "AccountClass",
    "AccountAttestation",
    "AccountEvidence",
    "AuthenticatedLoopbackGateway",
    "ExecutionLifecycle",
    "ExecutionSupervisor",
    "ExactLiveAccountIdentity",
    "HumanAuthorization",
    "LiveCanaryRiskProfile",
    "LiveCapability",
    "LiveAuthorizationBoundary",
    "LiveAuthorizationState",
    "LiveEntryRequest",
    "LiveEventStore",
    "LiveReadiness",
    "LiveRiskAuthority",
    "NativeCapabilityBinding",
    "NoDispatchLiveGateway",
    "OrderLifecycleState",
    "ProtectionLifecycle",
    "ProtectionState",
    "LiveRuntime",
    "LiveRuntimeState",
    "ReadinessGate",
    "ActionClass",
    "AuthorizationAccountClass",
    "AuthorizationFacts",
    "classify_action",
    "derive_terminal_status",
    "classify_account",
    "load_capability",
    "load_verified_capability",
    "identity_from_native_metadata",
]
