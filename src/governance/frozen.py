"""F4.1.1 immutable-baseline seal generation and verification."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from .canonical import canonical_hash, sha256_file
from .errors import GovernanceError

F4_FINAL = "f70c62af1e4f4eadc86d4eb3e8d99c2e33aa431c"
F4_START = "868693b2b060abd2c476bb575314dec2105a816a"
F4_MANIFEST = "docs/commissioning/phase-f4-counterfactual-lab/f4-authority-manifest.json"
HISTORICAL_EVIDENCE = {
    "b8fe9b7c-069f-46a5-bdfd-509170469ca9": "b59aaff7a2d9957800734e08ad53f174ebeb20cae16525fb68d6cf508eb51360",
    "df51e218-0017-4beb-b019-172e07b51aa1": "b97d291421f138054bb0576ba345dc6645d1cccd93d9ee48dd1cb2c81fcc0535",
    "0feb3af5-01da-430f-911f-9834add713d0": "e45aef3f9e5e1d729bfe83a7e3c5ff21323e4eac528bb706b98909a4823007f7",
}


def _git(root: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True).stdout.strip()


def protected_paths(root: Path) -> list[str]:
    explicit = set(_git(root, "ls-tree", "-r", "--name-only", F4_FINAL, "--", "src/lane_ii/lab", "tests/test_phase_f4_counterfactual_lab.py", "tests/test_phase_f4_anvil_semantic_state.py", "tests/test_phase_f4_anvil_real_regression.py", "docs/commissioning/phase-f4-counterfactual-lab").splitlines())
    explicit.update(_git(root, "diff", "--name-only", f"{F4_START}..{F4_FINAL}").splitlines())
    return sorted(item for item in explicit if item)


def build_baseline(root: Path, *, baseline_tests: int = 0) -> dict[str, Any]:
    paths = protected_paths(root)
    inventory: list[dict[str, str]] = []
    for relative in paths:
        blob = _git(root, "rev-parse", f"{F4_FINAL}:{relative}")
        content = subprocess.run(["git", "show", f"{F4_FINAL}:{relative}"], cwd=root, check=True, capture_output=True).stdout
        import hashlib
        inventory.append({"path": relative.replace("\\", "/"), "git_blob_sha": blob, "sha256": hashlib.sha256(content).hexdigest()})
    authority_hash = next(item["sha256"] for item in inventory if item["path"] == F4_MANIFEST)
    record: dict[str, Any] = {
        "schema": "BEELZEBUB_FROZEN_BASELINE_V1",
        "phase": "f4.1.1",
        "status": "READY_FROZEN",
        "branch": "codex/phase-f4-anvil-semantic-restoration",
        "starting_commit": F4_START,
        "final_commit": F4_FINAL,
        "final_git_tree_sha": _git(root, "rev-parse", f"{F4_FINAL}^{{tree}}"),
        "parent_commit": _git(root, "rev-parse", f"{F4_FINAL}^"),
        "protected_path_rules": ["src/lane_ii/lab/**", "tests/test_phase_f4_*.py", "docs/commissioning/phase-f4-counterfactual-lab/**", "f4.1.1 commit delta"],
        "protected_files": inventory,
        "f4_authority_manifest_sha256": authority_hash,
        "semantic_fingerprint_schema": "ANVIL_EXECUTION_STATE_V1",
        "pinned_anvil_identity": {"version": "1.8.1", "foundry_commit": "982849d3140c01fd3b72905759581a132df7aa98", "archive_sha256": "02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8", "executable_sha256": "c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f"},
        "known_provider_classification_rule": "ANVIL_V1_8_1_GENESIS_BLOCK_ENV_REANCHOR_V1",
        "evidence_packages": [{"package_id": key, "run_manifest_sha256": value} for key, value in HISTORICAL_EVIDENCE.items()],
        "baseline_test_count": baseline_tests,
        "closure_declaration": "F4.1.1 is sealed as an immutable baseline. F5 may verify and archive it but may not amend it.",
    }
    record["canonical_freeze_seal_sha256"] = canonical_hash(record)
    return record


def verify_baseline(root: Path, record: dict[str, Any]) -> None:
    if record.get("canonical_freeze_seal_sha256") != canonical_hash(record, omit={"canonical_freeze_seal_sha256"}):
        raise GovernanceError("FROZEN_BASELINE_DRIFT", "seal hash")
    if record.get("final_commit") != F4_FINAL:
        raise GovernanceError("FROZEN_BASELINE_DRIFT", "final commit")
    paths = [item["path"] for item in record.get("protected_files", [])]
    if paths != protected_paths(root):
        raise GovernanceError("FROZEN_BASELINE_DRIFT", "protected inventory")
    command = ["git", "diff", "--exit-code", F4_FINAL, "--", *paths]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
    if result.returncode:
        raise GovernanceError("FROZEN_BASELINE_DRIFT", "protected-path diff")
    for item in record["protected_files"]:
        import hashlib
        current = hashlib.sha256(subprocess.run(["git", "show", f"{F4_FINAL}:{item['path']}"], cwd=root, check=True, capture_output=True).stdout).hexdigest()
        if current != item["sha256"]:
            raise GovernanceError("FROZEN_BASELINE_DRIFT", item["path"])


def load_baseline(root: Path) -> dict[str, Any]:
    return json.loads((root / "governance" / "frozen-baselines" / "f4.1.1.json").read_text(encoding="utf-8"))
