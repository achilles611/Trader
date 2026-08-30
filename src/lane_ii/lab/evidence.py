"""Bounded f4 evidence persistence; it never stores arbitrary provider data."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Mapping

from .contracts import CounterfactualRunResult, CounterfactualScenario, ScenarioValidationError, canonical_json


_SECRET_TEXT = re.compile(r"(?:private[ _-]?key|mnemonic|seed phrase|authorization:|api[ _-]?key)", re.I)


def default_artifact_root(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root is not None else Path.cwd()
    return root / "runtime" / "lane_ii_lab"


def _reject_secret(value: object) -> None:
    serialized = canonical_json(value)
    if len(serialized) > 65536 or _SECRET_TEXT.search(serialized):
        raise ScenarioValidationError("Counterfactual artifact contains prohibited secret-like material.")


def persist_result(
    result: CounterfactualRunResult,
    *,
    scenario: CounterfactualScenario | None = None,
    artifact_root: str | Path | None = None,
) -> Path:
    if type(result) is not CounterfactualRunResult:
        raise ScenarioValidationError("Only exact counterfactual results may be persisted.")
    payload = result.payload()
    if scenario is not None:
        if type(scenario) is not CounterfactualScenario or scenario.scenario_hash != result.identity.scenario_hash:
            raise ScenarioValidationError("Replay manifest scenario does not match counterfactual result.")
        payload["scenario"] = scenario.payload()
    _reject_secret(payload)
    root = Path(artifact_root) if artifact_root is not None else default_artifact_root()
    run_directory = root / result.identity.run_id
    run_directory.mkdir(parents=True, exist_ok=False)
    target = run_directory / "run-manifest.json"
    target.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return target
