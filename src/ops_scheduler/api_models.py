"""Typed, executable-code-free FastAPI request models for scheduler routes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SchedulerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PreviewRequest(SchedulerRequest):
    trigger_kind: str
    trigger_specification: dict[str, Any]
    timezone: str | None = None


class ScheduleRequest(SchedulerRequest):
    name: str = Field(min_length=1, max_length=240)
    description: str = Field(default="", max_length=8192)
    task_type: str
    task_configuration: dict[str, Any] = Field(default_factory=dict)
    trigger_kind: str
    trigger_specification: dict[str, Any]
    timezone: str | None = None
    missed_run_policy: str = "SKIP"
    max_lateness_seconds: float | None = None
    retry_policy: dict[str, Any] = Field(default_factory=dict)
    lifecycle: str = "ENABLED"


class ScheduleUpdateRequest(ScheduleRequest):
    current_revision: int = Field(ge=1)


class RunNowRequest(SchedulerRequest):
    operator_request_id: str | None = Field(default=None, max_length=240)


class TemplateRequest(SchedulerRequest):
    name: str | None = Field(default=None, max_length=240)
