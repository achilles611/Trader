"""API-facing scheduler service; routes never manipulate scheduler tables directly."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping
from uuid import uuid4

from .engine import SchedulerEngine
from .models import MissedRunPolicy, RunStatus, ScheduleLifecycle, assert_safe_payload, iso_utc, parse_utc, stable_key, utc_now
from .registry import TaskRegistry
from .store import OperationsStore
from .triggers import resolve_occurrences, validate_trigger


def _retry_policy(value: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(value or {})
    unknown = set(data) - {"max_attempts", "initial_delay_seconds", "backoff_multiplier", "maximum_delay_seconds"}
    if unknown:
        raise ValueError("Unsupported retry policy field.")
    result = {
        "max_attempts": int(data.get("max_attempts", 1)),
        "initial_delay_seconds": float(data.get("initial_delay_seconds", 30)),
        "backoff_multiplier": float(data.get("backoff_multiplier", 2)),
        "maximum_delay_seconds": float(data.get("maximum_delay_seconds", 3600)),
    }
    if not 1 <= result["max_attempts"] <= 10 or result["initial_delay_seconds"] < 0 or result["backoff_multiplier"] < 1 or result["maximum_delay_seconds"] < result["initial_delay_seconds"]:
        raise ValueError("Retry policy values are invalid.")
    return result


def scheduler_templates() -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = []
    for display, session in (("Asia", "ASIA"), ("London", "LONDON"), ("New York", "NEW_YORK")):
        templates.extend([
            {"template_id": f"{session.lower()}-readiness", "name": f"{display} readiness", "description": "Read-only session readiness before a deliberate operator arm.", "task_type": "lane_iii.session_readiness", "task_configuration": {"session": session, "freshness_threshold_seconds": 15}, "trigger_kind": "SESSION_RELATIVE", "trigger_specification": {"session": session, "event": "OPEN", "offset_minutes": -15}, "missed_run_policy": "SKIP", "retry_policy": {"max_attempts": 1}},
            {"template_id": f"{session.lower()}-opening-reminder", "name": f"{display} opening reminder", "description": "Reminder that manual arming happens outside the scheduler.", "task_type": "operator.reminder", "task_configuration": {"title": f"{display} session approaching", "message": f"{display} session is approaching. Review readiness and arm manually only if all gates pass.", "severity": "warning"}, "trigger_kind": "SESSION_RELATIVE", "trigger_specification": {"session": session, "event": "OPEN", "offset_minutes": -2}, "missed_run_policy": "SKIP", "retry_policy": {"max_attempts": 1}},
            {"template_id": f"{session.lower()}-close-audit", "name": f"{display} close audit", "description": "Read-only post-session close audit.", "task_type": "lane_iii.session_close_audit", "task_configuration": {"session": session}, "trigger_kind": "SESSION_RELATIVE", "trigger_specification": {"session": session, "event": "CLOSE", "offset_minutes": 2}, "missed_run_policy": "SKIP", "retry_policy": {"max_attempts": 1}},
            {"template_id": f"{session.lower()}-audit-export", "name": f"{display} audit export", "description": "Sanitized fixed-directory session audit export.", "task_type": "lane_iii.session_audit_export", "task_configuration": {"session": session}, "trigger_kind": "SESSION_RELATIVE", "trigger_specification": {"session": session, "event": "CLOSE", "offset_minutes": 5}, "missed_run_policy": "SKIP", "retry_policy": {"max_attempts": 1}},
        ])
    return templates


class SchedulerService:
    def __init__(self, store: OperationsStore, registry: TaskRegistry, engine: SchedulerEngine) -> None:
        self.store, self.registry, self.engine = store, registry, engine
        self.store.initialize()

    def status(self) -> dict[str, Any]:
        return self.engine.status()

    def catalog(self) -> dict[str, Any]:
        return {"tasks": self.registry.catalog(), "templates": scheduler_templates(), "authority": "OBSERVE_VERIFY_NOTIFY_EXPORT_ONLY"}

    def preview(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        assert_safe_payload(payload)
        timezone = str(payload.get("timezone") or self.engine.settings.default_timezone)
        entries = resolve_occurrences(str(payload["trigger_kind"]), payload.get("trigger_specification") or {}, after=utc_now(), count=5, default_timezone=timezone)
        return {"timezone": timezone, "occurrences": [entry.payload() for entry in entries]}

    def _validated(self, payload: Mapping[str, Any], *, updating: bool = False) -> dict[str, Any]:
        assert_safe_payload(payload)
        name = str(payload.get("name") or "").strip()
        if not name or len(name) > 240:
            raise ValueError("Schedule name is required and must be no longer than 240 characters.")
        description = str(payload.get("description") or "").strip()
        if len(description) > 8192:
            raise ValueError("Schedule description is too long.")
        task_type = str(payload.get("task_type") or "")
        task_configuration = self.registry.validate(task_type, payload.get("task_configuration") or {})
        timezone = str(payload.get("timezone") or self.engine.settings.default_timezone)
        trigger_kind = str(payload.get("trigger_kind") or "").upper()
        trigger_specification = validate_trigger(trigger_kind, payload.get("trigger_specification") or {}, default_timezone=timezone)
        policy = MissedRunPolicy(str(payload.get("missed_run_policy") or "SKIP").upper())
        if trigger_kind == "SESSION_RELATIVE" and policy is not MissedRunPolicy.SKIP:
            raise ValueError("SESSION_RELATIVE schedules are fail-closed and require missed_run_policy SKIP.")
        maximum = float(payload.get("max_lateness_seconds") if payload.get("max_lateness_seconds") is not None else self.engine.settings.default_max_lateness_seconds)
        if not 0 <= maximum <= 7 * 24 * 3600:
            raise ValueError("max_lateness_seconds must be between zero and seven days.")
        lifecycle = ScheduleLifecycle(str(payload.get("lifecycle") or "ENABLED").upper())
        return {
            "name": name, "description": description, "task_type": task_type, "task_configuration": task_configuration,
            "trigger_kind": trigger_kind, "trigger_specification": trigger_specification, "timezone": timezone,
            "missed_run_policy": policy.value, "max_lateness_seconds": maximum, "retry_policy": _retry_policy(payload.get("retry_policy")),
            "lifecycle": lifecycle.value,
        }

    def create_schedule(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        data = self._validated(payload)
        return self.store.create_schedule(**data)

    def update_schedule(self, schedule_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        data = self._validated(payload, updating=True)
        expected = payload.get("current_revision")
        if type(expected) is not int or expected < 1:
            raise ValueError("current_revision is required for schedule editing.")
        data.pop("lifecycle")  # revisions retain the existing schedule lifecycle.
        return self.store.revise_schedule(schedule_id, expected_revision=expected, **data)

    def schedule(self, schedule_id: str) -> dict[str, Any]:
        schedule = self.store.get_schedule(schedule_id)
        if schedule is None:
            raise KeyError(schedule_id)
        entries = resolve_occurrences(schedule["trigger_kind"], schedule["trigger_specification"], after=utc_now(), count=5, default_timezone=schedule["timezone"])
        schedule["next_five_occurrences"] = [entry.payload() for entry in entries]
        return schedule

    def schedules(self, **filters: Any) -> dict[str, Any]:
        return self.store.list_schedules(**filters)

    def set_lifecycle(self, schedule_id: str, lifecycle: ScheduleLifecycle) -> dict[str, Any]:
        return self.store.set_lifecycle(schedule_id, lifecycle.value)

    def run_now(self, schedule_id: str, *, operator_request_id: str | None = None) -> dict[str, Any]:
        schedule = self.schedule(schedule_id)
        if schedule["lifecycle"] == ScheduleLifecycle.ARCHIVED.value:
            raise ValueError("Archived schedules cannot be run.")
        request = operator_request_id or f"operator-{uuid4().hex}"
        run, _ = self.store.create_manual_run(schedule, request_id=request)
        if run["status"] == RunStatus.SCHEDULED.value:
            run = self.store.queue(run["run_id"], message="Operator requested immediate execution.")
        return run

    def retry(self, run_id: str) -> dict[str, Any]:
        prior = self.store.get_run(run_id)
        if prior is None:
            raise KeyError(run_id)
        definition = self.registry.get(str(prior["task_type"]))
        retryable = bool((prior.get("error") or {}).get("retryable", definition.retryable))
        if not definition.retryable or not retryable:
            raise ValueError("This run is not eligible for retry.")
        schedule = self.store.get_schedule(str(prior.get("schedule_id") or ""))
        if schedule and schedule["trigger_kind"] == "SESSION_RELATIVE":
            raise ValueError("Session-relative runs cannot retry outside their same valid session window.")
        key = stable_key(run_id, int(prior["attempt"]) + 1)
        run, _ = self.store.create_retry(run_id, idempotency_key=key)
        return run

    def runs(self, **filters: Any) -> dict[str, Any]:
        return self.store.list_runs(**filters)

    def run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    async def cancel(self, run_id: str) -> dict[str, Any]:
        return await self.engine.cancel_run(run_id)

    def notifications(self, **filters: Any) -> dict[str, Any]:
        return self.store.list_notifications(**filters)

    def mark_notification_read(self, notification_id: str) -> dict[str, Any]:
        return self.store.mark_notification_read(notification_id)

    def mark_all_notifications_read(self) -> dict[str, Any]:
        return {"updated": self.store.mark_all_notifications_read()}

    def instantiate_template(self, template_id: str, *, name: str | None = None) -> dict[str, Any]:
        template = next((item for item in scheduler_templates() if item["template_id"] == template_id), None)
        if template is None:
            raise KeyError(template_id)
        payload = {key: value for key, value in template.items() if key != "template_id"}
        payload["name"] = name or payload["name"]
        payload["lifecycle"] = "PAUSED"
        return self.create_schedule(payload)
