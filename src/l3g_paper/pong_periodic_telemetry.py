"""Pure aligned-window telemetry for Pong's protected periodic scheduler.

The module intentionally contains no acceptance threshold.  It proves only
whether two cumulative snapshots share one counter/producer/runtime identity
and, when they do, computes deltas and rates without treating aliases as
additional scheduler outcomes.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


PONG_PERIODIC_TELEMETRY_SCHEMA = "beelzebub-pong-periodic-telemetry-v1"
PONG_PERIODIC_WINDOW_SCHEMA = "beelzebub-pong-periodic-aligned-window-v1"

_COUNTER_KEYS = (
    "depth_events",
    "ordinary_evaluation_dispatches",
    "ordinary_completed_full_analyses",
    "protection_evaluation_dispatches",
    "protection_completed_full_analyses",
    "scheduled_opportunities",
    "claimed_current_opportunities",
    "dispatched_opportunities",
    "completed_opportunities",
    "applied_opportunities",
    "late_opportunities",
    "on_time_opportunities",
    "obsolete_opportunities",
    "protection_supersessions",
    "intentional_no_work",
    "shutdown_opportunities",
)

PERIODIC_DISQUALIFIER_REASONS = {
    "protection_transitions": "PROTECTION_TRANSITION_DURING_WINDOW",
    "bridge_unavailable_events": "RUNTIME_BRIDGE_UNAVAILABLE_DURING_WINDOW",
    "bridge_state_protection_events": "BRIDGE_STATE_PROTECTION_DURING_WINDOW",
    "bridge_exposure_or_intent_events": "BRIDGE_EXPOSURE_OR_INTENT_DURING_WINDOW",
    "runtime_reconciliation_loss_events": "RUNTIME_RECONCILIATION_LOST_DURING_WINDOW",
    "runtime_execution_unresolved_events": "RUNTIME_EXECUTION_UNRESOLVED_DURING_WINDOW",
    "runtime_position_not_flat_events": "RUNTIME_POSITION_NOT_FLAT_DURING_WINDOW",
    "runtime_owned_orders_events": "RUNTIME_OWNED_ORDERS_PRESENT_DURING_WINDOW",
    "runtime_entry_orders_events": "RUNTIME_ENTRY_ORDERS_PRESENT_DURING_WINDOW",
    "native_directive_pending_events": "NATIVE_DIRECTIVE_PENDING_DURING_WINDOW",
    "other_protection_events": "OTHER_PROTECTION_REASON_DURING_WINDOW",
    "freshness_breaches": "FRESHNESS_BREACH_DURING_WINDOW",
    "host_queue_overflows": "HOST_QUEUE_OVERFLOW_DURING_WINDOW",
    "collector_queue_overflows": "COLLECTOR_QUEUE_OVERFLOW_DURING_WINDOW",
    "producer_queue_overflows": "PRODUCER_QUEUE_OVERFLOW_DURING_WINDOW",
    "producer_transport_failures": "PRODUCER_TRANSPORT_FAILURE_DURING_WINDOW",
    "durability_lag_events": "DURABILITY_LAG_DURING_WINDOW",
    "durability_counter_regressions": "DURABILITY_COUNTER_REGRESSION_DURING_WINDOW",
    "canonical_depth_losses": "CANONICAL_DEPTH_LOSS_DURING_WINDOW",
    "stream_missing_events": "SOURCE_SEQUENCE_GAP_DURING_WINDOW",
    "stream_out_of_order_events": "SOURCE_OUT_OF_ORDER_DURING_WINDOW",
}
PERIODIC_DISQUALIFIER_KEYS = tuple(PERIODIC_DISQUALIFIER_REASONS)


def _mapping(value: object, reason: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(reason)
    return value


def _nonnegative_int(value: object, reason: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(reason)
    return value


def _utc(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("PONG_PERIODIC_TELEMETRY_UTC_INVALID")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("PONG_PERIODIC_TELEMETRY_UTC_INVALID") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("PONG_PERIODIC_TELEMETRY_UTC_INVALID")
    return parsed


def _append_reason(reasons: list[str], reason: str | None) -> None:
    if reason is not None and reason not in reasons:
        reasons.append(reason)


def _endpoint_reasons(label: str, context: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    prefix = "PERIODIC_WINDOW_" + label + "_"
    if context.get("active_path") != "PERIODIC_FLAT":
        reasons.append(prefix + "NOT_PERIODIC_FLAT")
    protection_reason = context.get("protection_reason")
    if protection_reason is not None:
        reasons.append(prefix + "PROTECTION_ACTIVE:" + str(protection_reason))
    if context.get("input_error") is not None:
        reasons.append(prefix + "INPUT_ERROR:" + str(context["input_error"]))
    if context.get("canonical_depth_ready") is not True:
        reasons.append(
            prefix + "CANONICAL_DEPTH_UNAVAILABLE:"
            + str(context.get("canonical_depth_reason") or "UNKNOWN")
        )
    freshness = context.get("freshness_blocker")
    if freshness is not None:
        reasons.append(prefix + "FRESHNESS_BLOCKED:" + str(freshness))
    if context.get("durability_counts_available") is not True:
        reasons.append(prefix + "DURABILITY_COUNTS_UNAVAILABLE")
    if context.get("collector_durability_coherent") is not True:
        reasons.append(prefix + "DURABILITY_NOT_COHERENT")
    lag = context.get("collector_durable_lag_records")
    if type(lag) is not int or lag != 0:
        reasons.append(prefix + "DURABILITY_LAG:" + str(lag))
    if context.get("collector_transport_counters_available") is not True:
        reasons.append(prefix + "COLLECTOR_TRANSPORT_COUNTERS_UNAVAILABLE")
    for key, reason in (
        ("host_observation_queue_depth", "HOST_QUEUE_NOT_DRAINED"),
        ("collector_queue_depth", "COLLECTOR_QUEUE_NOT_DRAINED"),
        ("compute_pending", "COMPUTE_QUEUE_NOT_DRAINED"),
    ):
        value = context.get(key)
        if type(value) is not int or value != 0:
            reasons.append(prefix + reason + ":" + str(value))
    return reasons


def _identity_reason(identity: Mapping[str, Any]) -> str | None:
    required_text = (
        "counter_epoch_id",
        "host_process_instance_id",
        "experiment_id",
        "loaded_artifact_hash",
        "selected_profile",
        "topology_hash",
        "runtime_execution_session_id",
        "runtime_addon_source_fingerprint",
        "runtime_addon_build_fingerprint",
        "source_session_id",
        "source_fingerprint",
        "source_build_fingerprint",
    )
    if any(not isinstance(identity.get(key), str) or not identity[key] for key in required_text):
        return "PERIODIC_WINDOW_IDENTITY_INCOMPLETE"
    if type(identity.get("counter_reset_ordinal")) is not int:
        return "PERIODIC_WINDOW_RESET_IDENTITY_INVALID"
    generation = identity.get("source_generation")
    if type(generation) is not int or generation <= 0:
        return "PERIODIC_WINDOW_SOURCE_GENERATION_UNAVAILABLE"
    return None


def aligned_periodic_window(
    start: Mapping[str, object],
    end: Mapping[str, object],
) -> dict[str, object]:
    """Return one fail-closed aligned delta window from cumulative snapshots."""
    if start.get("schema") != PONG_PERIODIC_TELEMETRY_SCHEMA or end.get("schema") != PONG_PERIODIC_TELEMETRY_SCHEMA:
        raise ValueError("PONG_PERIODIC_TELEMETRY_SCHEMA_INVALID")
    start_utc_text = start.get("captured_at_utc")
    end_utc_text = end.get("captured_at_utc")
    start_utc = _utc(start_utc_text)
    end_utc = _utc(end_utc_text)
    eligibility_reasons: list[str] = []
    if end_utc <= start_utc:
        eligibility_reasons.append("PERIODIC_WINDOW_UTC_NOT_INCREASING")
    start_identity = _mapping(start.get("identity"), "PONG_PERIODIC_TELEMETRY_IDENTITY_INVALID")
    end_identity = _mapping(end.get("identity"), "PONG_PERIODIC_TELEMETRY_IDENTITY_INVALID")
    identity_reason = _identity_reason(start_identity) or _identity_reason(end_identity)
    if identity_reason is None and dict(start_identity) != dict(end_identity):
        identity_reason = "PERIODIC_WINDOW_IDENTITY_CHANGED"
    _append_reason(eligibility_reasons, identity_reason)

    start_ns = _nonnegative_int(
        start.get("captured_monotonic_ns"),
        "PONG_PERIODIC_TELEMETRY_CLOCK_INVALID",
    )
    end_ns = _nonnegative_int(
        end.get("captured_monotonic_ns"),
        "PONG_PERIODIC_TELEMETRY_CLOCK_INVALID",
    )
    duration_ns = end_ns - start_ns
    if duration_ns <= 0:
        eligibility_reasons.append("PERIODIC_WINDOW_DURATION_NOT_POSITIVE")

    start_counters = _mapping(start.get("counters"), "PONG_PERIODIC_TELEMETRY_COUNTERS_INVALID")
    end_counters = _mapping(end.get("counters"), "PONG_PERIODIC_TELEMETRY_COUNTERS_INVALID")
    deltas: dict[str, int] = {}
    counter_reason: str | None = None
    for key in _COUNTER_KEYS:
        initial = _nonnegative_int(
            start_counters.get(key), "PONG_PERIODIC_TELEMETRY_COUNTER_INVALID:" + key,
        )
        final = _nonnegative_int(
            end_counters.get(key), "PONG_PERIODIC_TELEMETRY_COUNTER_INVALID:" + key,
        )
        if final < initial:
            counter_reason = "PERIODIC_WINDOW_COUNTER_REGRESSED:" + key
        deltas[key] = final - initial
    _append_reason(eligibility_reasons, counter_reason)

    start_disqualifiers = _mapping(
        start.get("disqualifier_counters"),
        "PONG_PERIODIC_TELEMETRY_DISQUALIFIERS_INVALID",
    )
    end_disqualifiers = _mapping(
        end.get("disqualifier_counters"),
        "PONG_PERIODIC_TELEMETRY_DISQUALIFIERS_INVALID",
    )
    disqualifying_deltas: dict[str, int] = {}
    for key in PERIODIC_DISQUALIFIER_KEYS:
        initial = _nonnegative_int(
            start_disqualifiers.get(key),
            "PONG_PERIODIC_TELEMETRY_DISQUALIFIER_INVALID:" + key,
        )
        final = _nonnegative_int(
            end_disqualifiers.get(key),
            "PONG_PERIODIC_TELEMETRY_DISQUALIFIER_INVALID:" + key,
        )
        delta = final - initial
        disqualifying_deltas[key] = delta
        if delta < 0:
            _append_reason(
                eligibility_reasons,
                "PERIODIC_WINDOW_DISQUALIFIER_REGRESSED:" + key,
            )
        elif delta > 0:
            _append_reason(
                eligibility_reasons,
                PERIODIC_DISQUALIFIER_REASONS[key],
            )

    start_scheduler = _mapping(start.get("scheduler"), "PONG_PERIODIC_TELEMETRY_SCHEDULER_INVALID")
    end_scheduler = _mapping(end.get("scheduler"), "PONG_PERIODIC_TELEMETRY_SCHEDULER_INVALID")
    scheduler_reason: str | None = None
    if (
        start_scheduler.get("deadline_reconciliation") is not True
        or end_scheduler.get("deadline_reconciliation") is not True
        or start_scheduler.get("completion_reconciliation") is not True
        or end_scheduler.get("completion_reconciliation") is not True
    ):
        scheduler_reason = "PERIODIC_WINDOW_SCHEDULER_UNRECONCILED"
    elif (
        start_scheduler.get("outstanding_opportunities") != 0
        or end_scheduler.get("outstanding_opportunities") != 0
    ):
        scheduler_reason = "PERIODIC_WINDOW_SCHEDULER_OUTSTANDING"
    _append_reason(eligibility_reasons, scheduler_reason)

    if deltas["protection_evaluation_dispatches"] > 0:
        _append_reason(
            eligibility_reasons,
            "PROTECTION_EVALUATION_DURING_WINDOW",
        )
    if deltas["protection_supersessions"] > 0:
        _append_reason(
            eligibility_reasons,
            "PROTECTION_SUPERSESSION_DURING_WINDOW",
        )

    start_context = _mapping(
        start.get("context"), "PONG_PERIODIC_TELEMETRY_CONTEXT_INVALID",
    )
    end_context = _mapping(
        end.get("context"), "PONG_PERIODIC_TELEMETRY_CONTEXT_INVALID",
    )
    for reason in _endpoint_reasons("START", start_context):
        _append_reason(eligibility_reasons, reason)
    for reason in _endpoint_reasons("END", end_context):
        _append_reason(eligibility_reasons, reason)

    eligibility_reason = eligibility_reasons[0] if eligibility_reasons else None
    denominator = deltas["ordinary_completed_full_analyses"]
    if eligibility_reason is not None:
        ratio: float | None = None
        ratio_reason = eligibility_reason
    elif denominator == 0:
        ratio = None
        ratio_reason = "ORDINARY_COMPLETED_FULL_ANALYSES_ZERO"
    else:
        ratio = deltas["depth_events"] / denominator
        ratio_reason = None

    duration_seconds = duration_ns / 1_000_000_000 if duration_ns > 0 else None
    rates = {
        "periodic_opportunities_per_second": None,
        "ordinary_completed_full_analyses_per_second": None,
        "protection_evaluations_per_second": None,
        "completed_opportunities_per_second": None,
    }
    if eligibility_reason is None and duration_seconds is not None:
        rates = {
            "periodic_opportunities_per_second": (
                deltas["scheduled_opportunities"] / duration_seconds
            ),
            "ordinary_completed_full_analyses_per_second": (
                denominator / duration_seconds
            ),
            "protection_evaluations_per_second": (
                deltas["protection_evaluation_dispatches"] / duration_seconds
            ),
            "completed_opportunities_per_second": (
                deltas["completed_opportunities"] / duration_seconds
            ),
        }

    return {
        "schema": PONG_PERIODIC_WINDOW_SCHEMA,
        "formula": "delta_depth_events / delta_ordinary_completed_full_analyses",
        "threshold": None,
        "threshold_reason": "POLICY_NOT_DEFINED",
        "accounting": {
            "terminal_outcomes": (
                "completed_opportunities + obsolete_opportunities + "
                "protection_supersessions + shutdown_opportunities"
            ),
            "missed_opportunities": (
                "alias of obsolete_opportunities; not an additive delta"
            ),
        },
        "eligible": eligibility_reason is None,
        "eligibility_reason": eligibility_reason,
        "eligibility_reasons": eligibility_reasons,
        "interval_start_utc": start_utc_text,
        "interval_end_utc": end_utc_text,
        "duration_seconds": duration_seconds,
        "identity": dict(end_identity),
        "deltas": deltas,
        "disqualifying_deltas": disqualifying_deltas,
        "rates": rates,
        "depth_events_per_ordinary_completed_full_analysis": ratio,
        "ratio_unavailable_reason": ratio_reason,
        "context": {
            "start": dict(start_context),
            "end": dict(end_context),
        },
    }


__all__ = [
    "PONG_PERIODIC_TELEMETRY_SCHEMA",
    "PONG_PERIODIC_WINDOW_SCHEMA",
    "PERIODIC_DISQUALIFIER_KEYS",
    "PERIODIC_DISQUALIFIER_REASONS",
    "aligned_periodic_window",
]
