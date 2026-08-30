"""Bounded f4 evidence persistence; it never stores arbitrary provider data."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Mapping

from .contracts import CounterfactualRunResult, CounterfactualScenario, ScenarioValidationError, canonical_json
from .contracts import canonical_hash


_SECRET_TEXT = re.compile(r"(?:private[ _-]?key|mnemonic|seed phrase|authorization:|api[ _-]?key)", re.I)


def default_artifact_root(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root is not None else Path.cwd()
    return root / "runtime" / "lane_ii_lab"


def _reject_secret(value: object, *, maximum_bytes: int = 65536) -> None:
    serialized = canonical_json(value)
    if len(serialized.encode("utf-8")) > maximum_bytes or _SECRET_TEXT.search(serialized):
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
    for name, content in result.provider_artifacts.items():
        _reject_secret(content, maximum_bytes=64 * 1024 * 1024)
        (run_directory / name).write_text(content, encoding="utf-8", newline="\n")
    target = run_directory / "run-manifest.json"
    target.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return target


def validate_persisted_result(path: str | Path) -> dict[str, object]:
    """Validate hashes and required companion artifacts without replaying a run."""
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        restoration = payload["evidence"]["restoration_evidence"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ScenarioValidationError("Counterfactual evidence manifest is malformed.") from exc
    _reject_secret(payload)
    if not restoration:
        return {"valid": True, "provider_artifact_count": 0, "schema": payload["evidence"]["schema"]}

    root = manifest_path.parent
    required = {
        "raw_dump_before.txt", "raw_dump_after.txt", "raw_dump_structural_diff.json",
        "mutation_witness_manifest.json", "semantic_state_before.json", "semantic_state_after.json",
    }
    missing = [name for name in sorted(required) if not (root / name).is_file()]
    if missing:
        raise ScenarioValidationError(f"Counterfactual provider artifact is missing: {missing[0]}.")
    before_raw = (root / "raw_dump_before.txt").read_text(encoding="utf-8")
    after_raw = (root / "raw_dump_after.txt").read_text(encoding="utf-8")
    if hashlib.sha256(before_raw.encode("utf-8")).hexdigest() != restoration["raw_provider_dump_before_sha256"]:
        raise ScenarioValidationError("Raw provider dump before hash mismatch.")
    if hashlib.sha256(after_raw.encode("utf-8")).hexdigest() != restoration["raw_provider_dump_after_sha256"]:
        raise ScenarioValidationError("Raw provider dump after hash mismatch.")

    difference = json.loads((root / "raw_dump_structural_diff.json").read_text(encoding="utf-8"))
    recorded_difference_hash = difference.pop("structural_diff_sha256", None)
    if canonical_hash(difference) != recorded_difference_hash or recorded_difference_hash != restoration["structural_diff_sha256"]:
        raise ScenarioValidationError("Structural diff evidence hash mismatch.")
    witness = json.loads((root / "mutation_witness_manifest.json").read_text(encoding="utf-8"))
    recorded_witness_hash = witness.pop("manifest_sha256", None)
    if canonical_hash(witness) != recorded_witness_hash or recorded_witness_hash != restoration["mutation_witness_manifest_sha256"]:
        raise ScenarioValidationError("Mutation witness evidence hash mismatch.")
    before_semantic = json.loads((root / "semantic_state_before.json").read_text(encoding="utf-8"))
    after_semantic = json.loads((root / "semantic_state_after.json").read_text(encoding="utf-8"))
    if canonical_hash(before_semantic) != restoration["semantic_state_before_sha256"]:
        raise ScenarioValidationError("Semantic state before hash mismatch.")
    if canonical_hash(after_semantic) != restoration["semantic_state_after_sha256"]:
        raise ScenarioValidationError("Semantic state after hash mismatch.")
    for name in required:
        _reject_secret((root / name).read_text(encoding="utf-8"), maximum_bytes=64 * 1024 * 1024)
    return {
        "valid": True,
        "provider_artifact_count": len(required),
        "schema": payload["evidence"]["schema"],
        "restoration_reason_code": restoration["restoration_reason_code"],
    }
