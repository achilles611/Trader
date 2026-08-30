"""Registry loading and semantic policy checks."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import jsonschema

from .canonical import safe_portable_path
from .errors import GovernanceError
from .yaml_loader import load_yaml

_IDENTIFIER = re.compile(r"^[a-z0-9]+(?:[a-z0-9-]*[a-z0-9])?$")
_ACTIVE = {"APPROVED", "INTEGRATED", "COMMISSIONED"}
_DISABLED = {"REFERENCE_ONLY", "REJECTED", "QUARANTINED", "RETIRED"}


def _schema(root: Path, name: str) -> dict[str, Any]:
    return json.loads((root / "governance" / "schemas" / name).read_text(encoding="utf-8"))


def _validate_schema(root: Path, record: dict[str, Any], schema_name: str, label: str) -> None:
    try:
        jsonschema.Draft202012Validator(_schema(root, schema_name)).validate(record)
    except jsonschema.ValidationError as exc:
        code = "UNKNOWN_SCHEMA_FIELD" if exc.validator == "additionalProperties" else "REGISTRY_SCHEMA_INVALID"
        raise GovernanceError(code, f"{label}: {exc.message}") from exc


def _records(root: Path, folder: str, schema_name: str, identifier: str) -> list[dict[str, Any]]:
    location = root / "governance" / "registry" / folder
    defaults_path = location / "_defaults.yaml"
    defaults = load_yaml(defaults_path) if defaults_path.exists() else {}
    if not isinstance(defaults, dict):
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", f"{folder} defaults")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(location.glob("*.yaml")):
        if path.name.startswith("_"):
            continue
        record = {**defaults, **load_yaml(path)}
        if not isinstance(record, dict):
            raise GovernanceError("REGISTRY_SCHEMA_INVALID", path.name)
        _validate_schema(root, record, schema_name, path.name)
        value = record[identifier]
        if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
            raise GovernanceError("REGISTRY_SCHEMA_INVALID", f"invalid {identifier}: {value}")
        if value in seen:
            raise GovernanceError("DUPLICATE_REGISTRY_ID", value)
        seen.add(value)
        records.append(record)
    if not records:
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", f"no {folder} records")
    return records


def load_components(root: Path) -> list[dict[str, Any]]:
    components = _records(root, "components", "architecture-component.schema.json", "component_id")
    component_ids = {item["component_id"] for item in components}
    for component in components:
        current = set(component["current_authority"])
        ceiling = set(component["authority_ceiling"])
        if not current.issubset(ceiling):
            raise GovernanceError("AUTHORITY_ESCALATION", component["component_id"])
        for value in component["source_roots"]:
            safe_portable_path(value)
        unknown = set(component["internal_dependencies"]) - component_ids
        if unknown:
            raise GovernanceError("UNRESOLVED_INTERNAL_EDGE", ",".join(sorted(unknown)))
        if component["component_id"] == "governance" and current.intersection(
            {"submit_paper_orders", "submit_testnet_orders", "submit_mainnet_orders", "sign_wallet_actions", "allocate_live_capital", "decide_strategy_actions", "generate_scientific_hypotheses", "evaluate_scientific_hypotheses"}
        ):
            raise GovernanceError("AUTHORITY_ESCALATION", "governance authority")
    return components


def load_adoptions(root: Path) -> list[dict[str, Any]]:
    records = _records(root, "adoptions", "third-party-adoption.schema.json", "adoption_id")
    for item in records:
        if item["lifecycle_status"] in _ACTIVE:
            if item["license_expression"] in {"UNKNOWN", ""}:
                raise GovernanceError("LICENSE_UNRESOLVED", item["adoption_id"])
            if item["immutable_provenance"] in {"UNKNOWN", ""}:
                raise GovernanceError("PROVENANCE_UNRESOLVED", item["adoption_id"])
        upstream = item["upstream_source"]
        if (item["category"] == "CI_ACTION" and "@v" in upstream) or upstream.endswith("/main") or upstream.endswith("/master"):
            raise GovernanceError("MUTABLE_PROVENANCE", item["adoption_id"])
        if item["category"] == "CI_ACTION" and len(item["immutable_provenance"].rsplit("@", 1)[-1]) != 40:
            raise GovernanceError("MUTABLE_PROVENANCE", item["adoption_id"])
        if item["category"] == "EXECUTABLE_TOOLCHAIN" and not item["artifact_hashes"]:
            raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", item["adoption_id"])
    return records


def validate_policies(root: Path) -> list[str]:
    names = ["authority-policy.yaml", "adoption-lifecycle.yaml", "provenance-policy.yaml", "retention-policy.yaml", "source-ownership-policy.yaml"]
    messages: list[str] = []
    schema = _schema(root, "governance-policy.schema.json")
    for name in names:
        item = load_yaml(root / "governance" / "policies" / name)
        try:
            jsonschema.Draft202012Validator(schema).validate(item)
        except jsonschema.ValidationError as exc:
            raise GovernanceError("REGISTRY_SCHEMA_INVALID", name) from exc
        messages.append(name)
    return messages
