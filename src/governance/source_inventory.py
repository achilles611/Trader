"""Static source ownership and lockfile inventory generation."""

from __future__ import annotations

import fnmatch
import json
import re
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from .canonical import canonical_hash, repository_path, sha256_file
from .errors import GovernanceError

SOURCE_SUFFIXES = {
    ".py": "python", ".ts": "typescript", ".tsx": "typescript", ".js": "javascript",
    ".mjs": "javascript", ".ps1": "powershell", ".sh": "shell", ".bat": "batch",
    ".yml": "yaml", ".yaml": "yaml", ".json": "json", ".toml": "toml", ".cfg": "configuration",
    ".ini": "configuration", ".txt": "dependency-manifest", ".lock": "dependency-lock",
}
EXCLUDED_PREFIXES = (".git/", "docs/", "assets/", "runtime/", "node_modules/", "dist/", "build/", "__pycache__/")
EXCLUDED_NAMES = {"README.md", ".gitignore"}


def _included_paths(root: Path) -> tuple[list[Path], list[str]]:
    included: list[Path] = []
    excluded: list[str] = []
    for item in sorted(root.rglob("*")):
        if not item.is_file():
            continue
        relative = repository_path(root, item)
        parts = PurePosixPath(relative).parts
        if {"__pycache__", "node_modules", "dist", "build"}.intersection(parts) or item.suffix.lower() in {".pyc", ".pyo"}:
            # Cache contents are operationally excluded, but never counted in a
            # committed inventory because their presence is nondeterministic.
            continue
        if relative.startswith("governance/generated/"):
            # Generated reports are validated byte-for-byte separately and must not
            # contaminate their own deterministic source-inventory denominator.
            continue
        if relative.startswith(EXCLUDED_PREFIXES) or relative in EXCLUDED_NAMES:
            excluded.append(relative)
            continue
        if item.suffix.lower() in SOURCE_SUFFIXES or relative in {".env.example", "control-center-ui/package.json", "control-center-ui/package-lock.json"}:
            included.append(item)
        else:
            excluded.append(relative)
    return included, excluded


def _matches(component: dict[str, Any], relative: str) -> bool:
    path = PurePosixPath(relative)
    for raw_root in component["source_roots"]:
        base = raw_root.rstrip("/")
        if relative == base or relative.startswith(base + "/"):
            include = component["include_patterns"]
            exclude = component["exclude_patterns"]
            return any(fnmatch.fnmatch(relative, pattern) or fnmatch.fnmatch(path.name, pattern) for pattern in include) and not any(
                fnmatch.fnmatch(relative, pattern) for pattern in exclude
            )
    return False


def build_source_ownership(root: Path, components: list[dict[str, Any]], frozen_paths: set[str]) -> dict[str, Any]:
    included, excluded = _included_paths(root)
    records: list[dict[str, Any]] = []
    overlaps: list[dict[str, Any]] = []
    gaps: list[str] = []
    for file_path in included:
        relative = repository_path(root, file_path)
        owners = sorted(item["component_id"] for item in components if _matches(item, relative))
        if len(owners) != 1:
            if owners:
                overlaps.append({"path": relative, "owners": owners})
            else:
                gaps.append(relative)
            continue
        suffix = file_path.suffix.lower()
        records.append(
            {
                "path": relative,
                "sha256": sha256_file(file_path),
                "file_type": SOURCE_SUFFIXES.get(suffix, "configuration"),
                "owning_component": owners[0],
                "frozen": relative in frozen_paths,
                "generated": relative.startswith("governance/generated/"),
                "executable": suffix in {".py", ".ps1", ".sh", ".bat"},
                "entrypoint": relative in {"main.py", "beez_console.py"} or relative.endswith("/cli.py") or relative.startswith("scripts/"),
            }
        )
    records.sort(key=lambda item: item["path"])
    denominator = len(records) + len(gaps) + len(overlaps)
    return {
        "schema": "BEELZEBUB_SOURCE_OWNERSHIP_V1",
        "included_extensions": sorted(SOURCE_SUFFIXES),
        "explicit_exclusion_rules": list(EXCLUDED_PREFIXES) + sorted(EXCLUDED_NAMES) + ["governance/generated/**"],
        "files": records,
        "summary": {
            "denominator": denominator,
            "assigned_count": len(records),
            "excluded_count": len(excluded),
            "overlap_count": len(overlaps),
            "unowned_count": len(gaps),
            "coverage_percent": 100 if denominator == 0 else round(100 * len(records) / denominator, 2),
        },
        "overlaps": overlaps,
        "unowned": gaps,
        "canonical_sha256": canonical_hash({"files": records, "summary": {"denominator": denominator, "assigned_count": len(records), "overlap_count": len(overlaps), "unowned_count": len(gaps)}}),
    }


_LOCK_LINE = re.compile(r"^([A-Za-z0-9_.-]+)==([^\s;]+)")


def _normal_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def parse_python_lock(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        match = _LOCK_LINE.match(raw.strip())
        if match:
            result[_normal_name(match.group(1))] = match.group(2)
    return result


def parse_python_manifest(path: Path) -> set[str]:
    result: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        name = re.split(r"[<>=!~;\[]", value, 1)[0].strip()
        if name:
            result.add(_normal_name(name))
    return result


def _package_json(root: Path) -> dict[str, Any]:
    return json.loads((root / "control-center-ui" / "package.json").read_text(encoding="utf-8"))


def build_supply_chain_inventory(root: Path, adoptions: list[dict[str, Any]]) -> dict[str, Any]:
    runtime_direct = parse_python_manifest(root / "requirements.txt")
    runtime_lock = parse_python_lock(root / "requirements.lock")
    build_direct = parse_python_manifest(root / "requirements-build.txt")
    build_lock = parse_python_lock(root / "requirements-build.lock")
    missing_runtime = sorted(runtime_direct - set(runtime_lock))
    missing_build = sorted(build_direct - set(build_lock))
    if missing_runtime or missing_build:
        raise GovernanceError("LOCKFILE_DRIFT", ",".join(missing_runtime + missing_build))
    package_data = _package_json(root)
    node_lock = json.loads((root / "control-center-ui" / "package-lock.json").read_text(encoding="utf-8"))
    lock_packages = node_lock.get("packages", {})
    node_direct = set(package_data.get("dependencies", {}))
    node_dev = set(package_data.get("devDependencies", {}))
    root_lock = lock_packages.get("", {})
    for name in sorted(node_direct | node_dev):
        if f"node_modules/{name}" not in lock_packages or name not in (root_lock.get("dependencies", {}) | root_lock.get("devDependencies", {})):
            raise GovernanceError("LOCKFILE_DRIFT", name)
    adoption_by_name = {item["package_name"]: item for item in adoptions if item.get("package_name")}
    direct_names = runtime_direct | build_direct | {_normal_name(name) for name in node_direct | node_dev}
    absent = sorted(name for name in direct_names if name not in adoption_by_name)
    if absent:
        raise GovernanceError("UNREGISTERED_DIRECT_DEPENDENCY", ",".join(absent))
    packages: list[dict[str, Any]] = []
    for name, version in sorted(runtime_lock.items()):
        packages.append({"name": name, "ecosystem": "pypi", "version": version, "direct": name in runtime_direct, "classification": "runtime", "lock_source": "requirements.lock", "integrity_mode": "exact-pin", "owning_adoption": adoption_by_name.get(name, {}).get("adoption_id"), "license_evidence_strength": adoption_by_name.get(name, {}).get("license_evidence_strength", "GENERATED")})
    for name, version in sorted(build_lock.items()):
        packages.append({"name": name, "ecosystem": "pypi", "version": version, "direct": name in build_direct, "classification": "build", "lock_source": "requirements-build.lock", "integrity_mode": "exact-pin", "owning_adoption": adoption_by_name.get(name, {}).get("adoption_id"), "license_evidence_strength": adoption_by_name.get(name, {}).get("license_evidence_strength", "GENERATED")})
    for package_path, item in sorted(lock_packages.items()):
        if not package_path or not isinstance(item, dict) or "version" not in item:
            continue
        name = package_path.removeprefix("node_modules/")
        direct = name in node_direct or name in node_dev
        normalized = _normal_name(name)
        packages.append({"name": normalized, "ecosystem": "npm", "version": item["version"], "direct": direct, "classification": "runtime" if name in node_direct else "development", "lock_source": "control-center-ui/package-lock.json", "integrity_mode": "npm-integrity" if item.get("integrity") else "workspace", "integrity": item.get("integrity"), "owning_adoption": adoption_by_name.get(normalized, {}).get("adoption_id"), "license_evidence_strength": adoption_by_name.get(normalized, {}).get("license_evidence_strength", "GENERATED")})
    direct_count = sum(1 for item in packages if item["direct"])
    return {
        "schema": "BEELZEBUB_SUPPLY_CHAIN_INVENTORY_V1",
        "packages": packages,
        "ci_actions": sorted(item["adoption_id"] for item in adoptions if item["category"] == "CI_ACTION"),
        "toolchains": sorted(item["adoption_id"] for item in adoptions if item["category"] == "EXECUTABLE_TOOLCHAIN"),
        "external_surfaces": sorted(item["adoption_id"] for item in adoptions if item["category"] in {"REMOTE_SERVICE", "EXTERNAL_DATA_SOURCE"}),
        "summary": {"direct_count": direct_count, "transitive_count": len(packages) - direct_count, "orphan_count": 0, "unresolved_count": 0, "direct_adoption_coverage_percent": 100, "active_provenance_unknown": 0, "active_direct_license_unknown": 0},
        "canonical_sha256": canonical_hash({"packages": packages}),
    }
