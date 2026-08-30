"""Hash-first, atomic provision and verification for the frozen Anvil toolchain."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

from .canonical import canonical_hash, is_reparse_point, sha256_file, write_canonical_json
from .errors import GovernanceError

PRODUCT = "Foundry Anvil"
VERSION = "1.8.1"
COMMIT = "982849d3140c01fd3b72905759581a132df7aa98"
ARCHIVE_SHA256 = "02d98fc2c573793960ee06b7f642487d483fe30572f7e248804c207334a418d8"
EXECUTABLE_SHA256 = "c6e29da1b010fe00bac6c0dc5c29484bd641deb5a84050aea10d13e9dc4fe26f"
EXPECTED_MEMBERS = frozenset({"anvil.exe", "cast.exe", "chisel.exe", "forge.exe", "solar.exe"})


def resolve_toolchain_root() -> Path:
    explicit = os.environ.get("BEELZEBUB_TOOLCHAIN_ROOT")
    if explicit:
        return Path(explicit)
    network = Path("N:/Beelzebub/toolchains")
    if network.exists() and os.access(network.parent if network.parent.exists() else Path("N:/"), os.W_OK):
        return network
    local = Path(os.environ["LOCALAPPDATA"]) / "Beelzebub" / "toolchains"
    return local


def installation_dir(root: Path) -> Path:
    return root / "foundry" / f"v{VERSION}" / COMMIT / "windows-amd64"


def _safe_archive_members(archive: Path) -> dict[str, zipfile.ZipInfo]:
    try:
        with zipfile.ZipFile(archive) as reader:
            members = {Path(info.filename).name: info for info in reader.infolist()}
            if set(members) != EXPECTED_MEMBERS:
                raise GovernanceError("TOOLCHAIN_ARCHIVE_INVALID", "unexpected archive members")
            for info in members.values():
                normalized = info.filename.replace("\\", "/")
                if normalized.startswith("/") or ":" in normalized or ".." in Path(normalized).parts:
                    raise GovernanceError("TOOLCHAIN_ARCHIVE_INVALID", "path traversal")
                if info.is_dir() or stat.S_ISLNK(info.external_attr >> 16):
                    raise GovernanceError("TOOLCHAIN_ARCHIVE_INVALID", "non-regular archive member")
            return members
    except zipfile.BadZipFile as exc:
        raise GovernanceError("TOOLCHAIN_ARCHIVE_INVALID", "invalid zip") from exc


def _under_temp(path: Path) -> bool:
    return path.resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())


def _receipt(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def provision_anvil(archive: Path, root: Path | None = None, *, mode: str = "explicit_archive") -> dict[str, Any]:
    archive = archive.resolve()
    if not archive.is_file() or is_reparse_point(archive):
        raise GovernanceError("TOOLCHAIN_ARCHIVE_INVALID", "archive is not a regular file")
    if sha256_file(archive) != ARCHIVE_SHA256:
        raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "archive")
    _safe_archive_members(archive)
    selected_root = (root or resolve_toolchain_root()).resolve()
    target = installation_dir(selected_root)
    if _under_temp(target):
        raise GovernanceError("TOOLCHAIN_EPHEMERAL_PATH", "final toolchain root")
    if target.exists():
        return verify_installation(target)
    selected_root.mkdir(parents=True, exist_ok=True)
    staging = selected_root / (".f5-stage-" + uuid.uuid4().hex)
    try:
        archive_dir = staging / "archive"
        bin_dir = staging / "bin"
        archive_dir.mkdir(parents=True)
        bin_dir.mkdir()
        copied_archive = archive_dir / archive.name
        shutil.copy2(archive, copied_archive)
        if sha256_file(copied_archive) != ARCHIVE_SHA256:
            raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "staged archive")
        with zipfile.ZipFile(copied_archive) as reader:
            with reader.open("anvil.exe") as source, (bin_dir / "anvil.exe").open("wb") as destination:
                shutil.copyfileobj(source, destination)
        executable = bin_dir / "anvil.exe"
        if is_reparse_point(executable) or sha256_file(executable) != EXECUTABLE_SHA256:
            raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "anvil.exe")
        result = subprocess.run([str(executable), "--version"], capture_output=True, text=True, timeout=20, check=False)
        version_output = (result.stdout + result.stderr).strip()
        if result.returncode or VERSION not in version_output or COMMIT not in version_output:
            raise GovernanceError("TOOLCHAIN_VERSION_MISMATCH", "anvil --version")
        receipt: dict[str, Any] = {
            "schema": "BEELZEBUB_TOOLCHAIN_INSTALLATION_V1",
            "product": PRODUCT,
            "version": VERSION,
            "upstream_commit": COMMIT,
            "release_provenance": "https://github.com/foundry-rs/foundry/releases/tag/v1.8.1",
            "platform": "windows", "architecture": "amd64", "archive_name": archive.name,
            "archive_sha256": ARCHIVE_SHA256, "executable_sha256": EXECUTABLE_SHA256,
            "version_command_result": version_output, "regular_file_checks": True,
            "reparse_point_checks": True, "installation_mode": mode,
            "installation_path": str(target), "verification_result": "PASS",
        }
        receipt["receipt_sha256"] = canonical_hash(receipt)
        write_canonical_json(staging / "installation-receipt.json", receipt)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, target)
        return verify_installation(target)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_installation(target: Path) -> dict[str, Any]:
    target = target.resolve()
    if _under_temp(target):
        raise GovernanceError("TOOLCHAIN_EPHEMERAL_PATH", str(target))
    if not target.is_dir() or is_reparse_point(target):
        raise GovernanceError("TOOLCHAIN_REPARSE_POINT", "installation root")
    executable = target / "bin" / "anvil.exe"
    receipt_path = target / "installation-receipt.json"
    if not executable.is_file() or is_reparse_point(executable):
        raise GovernanceError("TOOLCHAIN_REPARSE_POINT", "anvil.exe")
    if sha256_file(executable) != EXECUTABLE_SHA256:
        raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "anvil.exe")
    receipt = _receipt(receipt_path)
    if receipt.get("receipt_sha256") != canonical_hash(receipt, omit={"receipt_sha256"}):
        raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "receipt")
    if receipt.get("archive_sha256") != ARCHIVE_SHA256 or receipt.get("executable_sha256") != EXECUTABLE_SHA256:
        raise GovernanceError("TOOLCHAIN_HASH_MISMATCH", "receipt identity")
    if VERSION not in receipt.get("version_command_result", "") or COMMIT not in receipt.get("version_command_result", ""):
        raise GovernanceError("TOOLCHAIN_VERSION_MISMATCH", "receipt version")
    return receipt


def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provision", action="store_true")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--root", type=Path)
    args = parser.parse_args()
    if not args.provision:
        raise SystemExit("--provision is required")
    receipt = provision_anvil(args.archive, args.root)
    print(json.dumps({"status": "PASS", "receipt_sha256": receipt["receipt_sha256"], "installation_path": receipt["installation_path"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
