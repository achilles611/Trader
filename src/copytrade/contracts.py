"""Versioned persisted contracts between the copy-trading research phases.

These values describe stored evidence, not a trading protocol.  They keep
newer readers from silently assigning a meaning to fields old writers did not
measure.
"""

from __future__ import annotations


PHASE_A_EVIDENCE_SCHEMA_VERSION = 2
"""Current discovery metadata contract accepted by the Phase B prefilter."""

PHASE_B_RECOMMENDATION_SCHEMA_VERSION = 1
"""Current persisted finalist-recommendation contract consumed by Phase C."""


# A current Phase-A record must carry every dimension that configurable Phase-B
# prefilter gates may inspect.  Zero is meaningful only when this whole
# contract is present and versioned.
PHASE_A_CHEAP_STATS_FIELDS = frozenset({
    "distinct_observed_events",
    "distinct_active_hours",
    "distinct_active_days",
    "observation_span_hours",
    "distinct_symbols",
    "symbols",
    "approximate_observed_notional",
    "independent_source_count",
    "first_observed_activity",
    "last_observed_activity",
})
