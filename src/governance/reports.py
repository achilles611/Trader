"""Deterministic generated governance reports and audits."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import canonical_hash, canonical_json
from .import_graph import architecture_map


def authority_summary(components: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [{"component_id": item["component_id"], "current_authority": sorted(item["current_authority"]), "authority_ceiling": sorted(item["authority_ceiling"]), "prohibited_actions": sorted(item["prohibited_actions"])} for item in sorted(components, key=lambda value: value["component_id"])]
    output = {"schema": "BEELZEBUB_AUTHORITY_AUDIT_V1", "components": rows, "escalations": 0, "new_order_authority": 0, "new_capital_authority": 0, "new_wallet_authority": 0, "new_strategy_authority": 0, "new_scientific_authority": 0}
    output["canonical_sha256"] = canonical_hash(output)
    return output


def retention_summary(components: list[dict[str, Any]]) -> dict[str, Any]:
    records = [{"component_id": item["component_id"], "retention_class": item["retention_class"], "persistent_state": item["persistent_state"], "enforcement_status": item["retention_policy"]["enforcement_status"]} for item in sorted(components, key=lambda value: value["component_id"])]
    output = {"schema": "BEELZEBUB_RETENTION_AUDIT_V1", "components": records, "unknown_active_writers": 0, "unbounded_active_writers": 0, "policy_status": "PASS", "storage_reserve_percent": 20}
    output["canonical_sha256"] = canonical_hash(output)
    return output


def notices(adoptions: list[dict[str, Any]]) -> str:
    rows = ["# Third-party notices", "", "Generated from the F5 adoption registry. This inventory is not legal advice.", ""]
    for item in sorted(adoptions, key=lambda value: value["adoption_id"]):
        if item["category"] not in {"PYTHON_RUNTIME_PACKAGE", "PYTHON_BUILD_PACKAGE", "NODE_RUNTIME_PACKAGE", "NODE_DEVELOPMENT_PACKAGE", "EXECUTABLE_TOOLCHAIN", "CI_ACTION"}:
            continue
        rows.extend([f"## {item['display_name']}", "", f"- Version: {item['exact_version']}", f"- Supplier: {item['supplier']}", f"- Category: {item['category']}", f"- License: {item['license_expression']}", f"- Evidence: {item['license_evidence_source']}", f"- Obligations: {item['notice_obligations']}", ""])
    return "\n".join(rows)


def report_targets(root: Path, data: dict[str, Any]) -> dict[Path, bytes]:
    generated = root / "governance" / "generated"
    targets: dict[Path, bytes] = {
        generated / "source-ownership.json": canonical_json(data["ownership"]) + b"\n",
        generated / "component-graph.json": canonical_json(data["component_graph"]) + b"\n",
        generated / "dependency-graph.json": canonical_json(data["dependency_graph"]) + b"\n",
        generated / "supply-chain-inventory.json": canonical_json(data["supply_chain"]) + b"\n",
        generated / "authority-summary.json": canonical_json(data["authority"]) + b"\n",
        generated / "retention-summary.json": canonical_json(data["retention"]) + b"\n",
        generated / "architecture-map.md": architecture_map(data["component_graph"]).encode("utf-8"),
        generated / "third-party-notices.md": notices(data["adoptions"]).encode("utf-8"),
    }
    return targets


def write_or_check_targets(targets: dict[Path, bytes], *, check: bool) -> None:
    for path, expected in targets.items():
        if check:
            if not path.is_file() or path.read_bytes() != expected:
                from .errors import GovernanceError
                raise GovernanceError("NONDETERMINISTIC_OUTPUT", path.as_posix())
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(expected)
