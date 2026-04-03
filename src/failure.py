from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class FailureCode:
    DIRTY_REPO = "DIRTY_REPO"
    MISSING_ENV = "MISSING_ENV"
    CONFIG_INVALID = "CONFIG_INVALID"
    LOCK_HELD = "LOCK_HELD"
    DB_UNWRITABLE = "DB_UNWRITABLE"
    ARTIFACT_PATH_UNWRITABLE = "ARTIFACT_PATH_UNWRITABLE"
    SWARM_EXECUTION_FAILED = "SWARM_EXECUTION_FAILED"
    OPENAI_ANALYSIS_FAILED = "OPENAI_ANALYSIS_FAILED"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"


@dataclass(frozen=True)
class OrchestratorFailure(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    exit_code: int = 1

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "exit_code": self.exit_code,
        }
