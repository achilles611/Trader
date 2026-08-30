"""Evidence archival and bounded F5 commissioning-envelope creation."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

from .canonical import canonical_hash, sha256_file, write_canonical_json
from .errors import GovernanceError
from .frozen import HISTORICAL_EVIDENCE

F4_SOURCE = Path("C:/Users/atlas/Documents/Trader-f4/runtime/lane_ii_lab")


def resolve_evidence_root() -> Path:
    explicit = os.environ.get("BEELZEBUB_EVIDENCE_ROOT")
    if explicit:
        return Path(explicit)
    network = Path("N:/Beelzebub/evidence")
    if network.exists() and os.access(network.parent if network.parent.exists() else Path("N:/"), os.W_OK):
        return network
    return Path(os.environ["LOCALAPPDATA"]) / "Beelzebub" / "evidence"


def recursive_inventory(root: Path) -> list[dict[str, Any]]:
    return [{"path": item.relative_to(root).as_posix(), "bytes": item.stat().st_size, "sha256": sha256_file(item)} for item in sorted(root.rglob("*")) if item.is_file()]


def _check_secret_scan(inventory: list[dict[str, Any]], root: Path) -> None:
    pattern = re.compile(r"(?i)(?:private[_-]?key|wallet[_-]?seed|api[_-]?key)\s*[:=]\s*['\"]?[A-Za-z0-9_\-/+=]{16,}")
    for item in inventory:
        try:
            text = (root / item["path"]).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if pattern.search(text):
            raise GovernanceError("SECRET_SCAN_FAILED", item["path"])


def archive_f4_evidence(evidence_root: Path | None = None) -> dict[str, Any]:
    destination = (evidence_root or resolve_evidence_root()) / "lane_ii" / "f4.1.1"
    expected_packages: list[dict[str, Any]] = []
    for package_id, expected_hash in HISTORICAL_EVIDENCE.items():
        source = F4_SOURCE / package_id
        manifest = source / "run-manifest.json"
        if not source.is_dir() or sha256_file(manifest) != expected_hash:
            raise GovernanceError("EVIDENCE_HASH_MISMATCH", package_id)
        inventory = recursive_inventory(source)
        _check_secret_scan(inventory, source)
        expected_packages.append({"package_id": package_id, "expected_run_manifest_sha256": expected_hash, "source_inventory": inventory, "source_bytes": sum(item["bytes"] for item in inventory)})
    archive_manifest_path = destination / "archive-manifest.json"
    if archive_manifest_path.exists():
        current = json.loads(archive_manifest_path.read_text(encoding="utf-8"))
        if current.get("archive_manifest_sha256") == canonical_hash(current, omit={"archive_manifest_sha256"}):
            return current
        raise GovernanceError("EVIDENCE_ARCHIVE_INVALID", "existing archive manifest")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = destination.parent / (".f4-stage-" + uuid.uuid4().hex)
    try:
        copies: list[dict[str, Any]] = []
        for package in expected_packages:
            source = F4_SOURCE / package["package_id"]
            copied = stage / package["package_id"]
            shutil.copytree(source, copied, copy_function=shutil.copy2)
            archive_inventory = recursive_inventory(copied)
            _check_secret_scan(archive_inventory, copied)
            if archive_inventory != package["source_inventory"]:
                raise GovernanceError("EVIDENCE_HASH_MISMATCH", package["package_id"])
            copies.append({**package, "archive_inventory": archive_inventory, "archive_bytes": sum(item["bytes"] for item in archive_inventory), "copy_verification": "PASS"})
        manifest: dict[str, Any] = {"schema": "BEELZEBUB_EVIDENCE_ARCHIVE_V1", "phase": "f4.1.1", "packages": copies, "copy_verification_result": "PASS"}
        manifest["archive_manifest_sha256"] = canonical_hash(manifest)
        write_canonical_json(stage / "archive-manifest.json", manifest)
        os.replace(stage, destination)
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage, ignore_errors=True)
        raise


def file_artifact(path: Path) -> dict[str, Any]:
    return {"filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def commission_f5(root: Path, envelope: dict[str, Any], *, evidence_root: Path | None = None) -> dict[str, Any]:
    """Publish a deterministic normalized evidence package; local envelope fields stay out of its hash."""
    target_base = (evidence_root or resolve_evidence_root()) / "governance" / "f5.0"
    target_base.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    stage = target_base / (".f5-stage-" + run_id)
    stage.mkdir()
    artifacts = envelope["artifacts"]
    for name, value in sorted(artifacts.items()):
        write_canonical_json(stage / name, value)
    normalized = envelope["normalized_outcome"]
    write_canonical_json(stage / "normalized-outcome.json", normalized)
    entries = [file_artifact(path) for path in sorted(stage.iterdir()) if path.is_file()]
    manifest: dict[str, Any] = {"schema": "BEELZEBUB_F5_COMMISSIONING_EVIDENCE_V1", "phase": "f5.0", "run_id": run_id, "normalized_outcome_sha256": canonical_hash(normalized), "artifacts": entries, "network_write_accounting": {"remote_provider_writes": 0, "paper_orders": 0, "testnet_orders": 0, "mainnet_orders": 0, "wallet_signatures": 0, "live_capital_actions": 0}}
    manifest["manifest_sha256"] = canonical_hash(manifest, omit={"run_id"})
    write_canonical_json(stage / "manifest.json", manifest)
    final = target_base / run_id
    os.replace(stage, final)
    return {"path": str(final), "manifest_sha256": manifest["manifest_sha256"], "normalized_outcome_sha256": canonical_hash(normalized)}
