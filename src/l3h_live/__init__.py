"""Lane III-H isolated live-capital authority boundary.

This package deliberately does not import :mod:`src.l3g_paper`.  L3G remains
the independently testable Sim101 paper capability; L3H starts disarmed and
can acquire authority only from a locally stored, signed capability artifact.
"""

from .authority import LiveReadiness, ReadinessGate, derive_terminal_status
from .contracts import AccountClass, LiveCapability, load_capability
from .event_store import LiveEventStore
from .risk import LiveCanaryRiskProfile, LiveRiskAuthority
from .runtime import LiveRuntime, LiveRuntimeState

__all__ = [
    "AccountClass",
    "LiveCanaryRiskProfile",
    "LiveCapability",
    "LiveEventStore",
    "LiveReadiness",
    "LiveRiskAuthority",
    "LiveRuntime",
    "LiveRuntimeState",
    "ReadinessGate",
    "derive_terminal_status",
    "load_capability",
]
