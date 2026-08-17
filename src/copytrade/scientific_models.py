"""Immutable model versions built from independently versioned indicators."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping

from .science_repository import ScientificRepository


class ModelState(StrEnum):
    CANDIDATE = "CANDIDATE"
    SHADOW = "SHADOW"
    ACTIVE_SIMULATION = "ACTIVE_SIMULATION"
    DEGRADED = "DEGRADED"
    RETIRED = "RETIRED"


@dataclass(frozen=True)
class ModelDefinition:
    model_id: str
    version: int
    input_indicator_versions: tuple[tuple[str, int], ...]
    fitting_data_window: Mapping[str, str]
    validation_data_window: Mapping[str, str]
    parameters: Mapping[str, Any]
    calibration: Mapping[str, Any]
    creation_sha: str
    config_hash: str
    performance: Mapping[str, Any]
    created_at: str
    predecessor_id: str | None = None

    def __post_init__(self) -> None:
        if not self.model_id or self.version <= 0 or not self.input_indicator_versions:
            raise ValueError("Models require an ID, positive version, and indicator inputs.")
        if not self.creation_sha or not self.config_hash:
            raise ValueError("Models require code and configuration provenance.")

    def payload(self) -> dict[str, Any]:
        data = asdict(self)
        data["input_indicator_versions"] = [{"indicator_id": identifier, "version": version} for identifier, version in self.input_indicator_versions]
        return data


class ScientificModelRegistry:
    def __init__(self, repository: ScientificRepository) -> None:
        self.repository = repository

    def register(self, definition: ModelDefinition, *, state: ModelState = ModelState.CANDIDATE) -> dict[str, Any]:
        return self.repository.register_model(definition.model_id, definition.version, state=state.value, definition=definition.payload(), created_at=definition.created_at, predecessor_id=definition.predecessor_id)
