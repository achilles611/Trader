"""Allowlisted scheduler task definitions; no arbitrary command execution surface."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Awaitable, Callable, Mapping

from .models import AuthorityClassification, TaskOutcome, assert_safe_payload


TaskExecutor = Callable[[Any], TaskOutcome | Mapping[str, Any] | Awaitable[TaskOutcome | Mapping[str, Any]]]
TaskValidator = Callable[[Mapping[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class TaskDefinition:
    task_type: str
    display_name: str
    description: str
    domain: str
    authority_classification: AuthorityClassification
    validate_configuration: TaskValidator
    execute: TaskExecutor
    default_maximum_runtime_seconds: float = 60.0
    cancellable: bool = True
    retryable: bool = False
    required_resource_keys: tuple[str, ...] = ()
    commissioning_probe: bool = False

    def catalog_payload(self) -> dict[str, Any]:
        return {
            "task_type": self.task_type,
            "display_name": self.display_name,
            "description": self.description,
            "domain": self.domain,
            "authority_classification": self.authority_classification.value,
            "default_maximum_runtime_seconds": self.default_maximum_runtime_seconds,
            "cancellable": self.cancellable,
            "retryable": self.retryable,
            "required_resource_keys": list(self.required_resource_keys),
        }


class TaskRegistry:
    def __init__(self, definitions: tuple[TaskDefinition, ...] | list[TaskDefinition], *, include_commissioning_probes: bool = False) -> None:
        self._definitions = {definition.task_type: definition for definition in definitions if include_commissioning_probes or not definition.commissioning_probe}
        if len(self._definitions) != len([definition for definition in definitions if include_commissioning_probes or not definition.commissioning_probe]):
            raise ValueError("Duplicate scheduler task type.")

    def get(self, task_type: str) -> TaskDefinition:
        try:
            return self._definitions[task_type]
        except KeyError as exc:
            raise ValueError("Unknown scheduler task type.") from exc

    def validate(self, task_type: str, configuration: Mapping[str, Any]) -> dict[str, Any]:
        assert_safe_payload(configuration)
        return self.get(task_type).validate_configuration(configuration)

    def catalog(self) -> list[dict[str, Any]]:
        return [definition.catalog_payload() for definition in sorted(self._definitions.values(), key=lambda item: item.task_type)]

    async def execute(self, task_type: str, context: Any) -> TaskOutcome:
        result = self.get(task_type).execute(context)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, TaskOutcome):
            return result
        if isinstance(result, Mapping):
            return TaskOutcome(result=result)
        raise RuntimeError("Scheduler task returned an invalid result.")
