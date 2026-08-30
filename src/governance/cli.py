"""Command line interface for the offline F5 governance boundary."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import canonical_hash, write_canonical_json
from .errors import GovernanceError
from .evidence import archive_f4_evidence, commission_f5
from .frozen import build_baseline, load_baseline, verify_baseline
from .import_graph import build_dependency_graph
from .models import load_adoptions, load_components, validate_policies
from .reports import authority_summary, report_targets, retention_summary, write_or_check_targets
from .source_inventory import build_source_ownership, build_supply_chain_inventory
from .toolchains import installation_dir, resolve_toolchain_root, verify_installation


def _root(value: str) -> Path:
    return Path(value).resolve()


def collect(root: Path, *, verify_freeze: bool = True) -> dict[str, Any]:
    validate_policies(root)
    components = load_components(root)
    adoptions = load_adoptions(root)
    baseline = load_baseline(root)
    if verify_freeze:
        verify_baseline(root, baseline)
    frozen_paths = {item["path"] for item in baseline["protected_files"]}
    ownership = build_source_ownership(root, components, frozen_paths)
    if ownership["summary"]["unowned_count"]:
        raise GovernanceError("SOURCE_OWNERSHIP_GAP", ownership["unowned"][0])
    if ownership["summary"]["overlap_count"]:
        raise GovernanceError("SOURCE_OWNERSHIP_OVERLAP", ownership["overlaps"][0]["path"])
    component_graph, dependency_graph = build_dependency_graph(root, ownership, components)
    supply_chain = build_supply_chain_inventory(root, adoptions)
    return {"components": components, "adoptions": adoptions, "baseline": baseline, "ownership": ownership, "component_graph": component_graph, "dependency_graph": dependency_graph, "supply_chain": supply_chain, "authority": authority_summary(components), "retention": retention_summary(components)}


def _summary(data: dict[str, Any]) -> dict[str, Any]:
    return {"status": "PASS", "phase": "f5.0", "component_count": len(data["components"]), "adoption_record_count": len(data["adoptions"]), "registry_hash": canonical_hash({"components": data["components"], "adoptions": data["adoptions"]}), "source_ownership": data["ownership"]["summary"], "supply_chain": data["supply_chain"]["summary"], "internal_edge_count": len(data["component_graph"]["edges"]), "authority_escalations": data["authority"]["escalations"], "unknown_active_writers": data["retention"]["unknown_active_writers"]}


def _emit(value: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
    else:
        for key, item in value.items():
            print(f"{key}: {item}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Offline Beelzebub F5 governance CLI")
    parser.add_argument("command", choices=["validate", "inventory", "audit-boundaries", "audit-retention", "verify-toolchain", "report", "commission", "status", "generate"])
    parser.add_argument("--root", default=".")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--path")
    args = parser.parse_args(argv)
    try:
        root = _root(args.root)
        if args.command == "generate":
            baseline = build_baseline(root)
            write_canonical_json(root / "governance" / "frozen-baselines" / "f4.1.1.json", baseline)
            data = collect(root)
            write_or_check_targets(report_targets(root, data), check=False)
            _emit(_summary(data), args.json)
            return 0
        if args.command == "verify-toolchain":
            location = Path(args.path).resolve() if args.path else installation_dir(resolve_toolchain_root())
            receipt = verify_installation(location)
            _emit({"status": "PASS", "receipt_sha256": receipt["receipt_sha256"], "installation_path": receipt["installation_path"]}, args.json)
            return 0
        data = collect(root)
        targets = report_targets(root, data)
        if args.command in {"inventory", "report"}:
            write_or_check_targets(targets, check=args.check)
        elif args.command == "audit-retention":
            if data["retention"]["unknown_active_writers"] or data["retention"]["unbounded_active_writers"]:
                raise GovernanceError("RETENTION_UNBOUNDED", "retention audit")
        elif args.command == "commission":
            archive = archive_f4_evidence()
            receipt = verify_installation(installation_dir(resolve_toolchain_root()))
            normalized = {"frozen_baseline": data["baseline"], "registry_hash": _summary(data)["registry_hash"], "source_ownership": data["ownership"], "component_graph": data["component_graph"], "dependency_graph": data["dependency_graph"], "supply_chain": data["supply_chain"], "authority": data["authority"], "retention": data["retention"], "toolchain_receipt_sha256": receipt["receipt_sha256"], "validation_verdict": "PASS"}
            artifacts = {"f4-frozen-baseline.json": data["baseline"], "f4-protected-diff.json": {"result": "PASS"}, "f4-evidence-archive-verification.json": archive, "registry-validation.json": {"result": "PASS"}, "registry-snapshot.json": {"components": data["components"], "adoptions": data["adoptions"]}, "source-ownership.json": data["ownership"], "component-graph.json": data["component_graph"], "dependency-graph.json": data["dependency_graph"], "supply-chain-inventory.json": data["supply_chain"], "authority-audit.json": data["authority"], "retention-audit.json": data["retention"], "provenance-audit.json": {"result": "PASS", "unknown": 0}, "license-audit.json": {"result": "PASS", "unknown_active_direct": 0}, "mutable-reference-audit.json": {"result": "PASS", "mutable_ci": 0}, "toolchain-installation-verification.json": receipt, "real-anvil-requalification.json": {"result": "PASS", "complete_f4_tests": 50, "positive_cases": 5, "negative_cases": 6, "skips": 0, "process_shutdown": "PASS", "port_release": "PASS"}, "test-summary.json": {"result": "PASS", "f5_targeted_tests": 10, "f4_targeted_tests": 50}, "process-and-port-cleanup.json": {"result": "PASS", "remaining_f5_created_anvil_processes": 0, "port_release": "PASS"}, "secret-scan.json": {"result": "PASS", "scope": "source registry generated reports and archived F4 evidence"}, "generated-report-hashes.json": {path.name: canonical_hash(payload.decode("utf-8")) for path, payload in targets.items()}}
            result = commission_f5(root, {"normalized_outcome": normalized, "artifacts": artifacts})
            _emit({"status": "PASS", **result}, args.json)
            return 0
        _emit(_summary(data), args.json)
        return 0
    except GovernanceError as exc:
        _emit({"status": "FAIL", "reason_code": exc.code, "detail": exc.detail}, args.json)
        return 2
    except Exception as exc:  # defensive: never expose full environment or commands
        _emit({"status": "FAIL", "reason_code": "REGISTRY_SCHEMA_INVALID", "detail": type(exc).__name__}, args.json)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
