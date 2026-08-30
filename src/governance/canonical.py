"""Deterministic UTF-8 canonicalization and path-safe hashing helpers."""

from __future__ import annotations

import hashlib
import json
import os
import unicodedata
from pathlib import Path, PurePosixPath
from typing import Any

from .errors import GovernanceError


def normal_text(value: str) -> str:
    return unicodedata.normalize("NFC", value).replace("\r\n", "\n").replace("\r", "\n")


def canonical_json(value: Any) -> bytes:
    """Encode a canonical JSON value without locale or platform variation."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GovernanceError("NONDETERMINISTIC_OUTPUT", type(exc).__name__) from exc
    return normal_text(encoded).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def repository_path(root: Path, path: Path) -> str:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", "path escapes repository") from exc
    return PurePosixPath(relative).as_posix()


def safe_portable_path(value: str) -> str:
    candidate = value.replace("\\", "/")
    if not candidate or candidate.startswith("/") or ":" in candidate or ".." in PurePosixPath(candidate).parts:
        raise GovernanceError("REGISTRY_SCHEMA_INVALID", "non-portable committed path")
    return candidate


def canonical_hash(value: Any, *, omit: set[str] | None = None) -> str:
    if omit:
        value = {key: item for key, item in value.items() if key not in omit}
    return sha256_bytes(canonical_json(value))


def write_canonical_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json(value) + b"\n")


def is_reparse_point(path: Path) -> bool:
    """Use Windows file attributes when available; symlinks are always rejected."""
    if path.is_symlink():
        return True
    try:
        attributes = os.lstat(path).st_file_attributes  # type: ignore[attr-defined]
    except AttributeError:
        return False
    return bool(attributes & 0x400)  # FILE_ATTRIBUTE_REPARSE_POINT
