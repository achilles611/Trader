"""Hot/cold storage primitives for the scientific alpha engine.

The decision path owns only the hot root.  Cold storage is deliberately an
asynchronous archival concern: callers may enqueue a record locally even when
the removable archive drive is absent, but no hot-path method opens the cold
root.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _redact(value: Any) -> Any:
    """Keep archival evidence useful without allowing common secret fields."""
    forbidden = {"secret", "private_key", "seed", "seed_phrase", "mnemonic", "signer", "password", "token"}
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if str(key).lower() in forbidden else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return value


@dataclass(frozen=True)
class StorageRoots:
    """Portable roots with environment overrides for Windows deployment."""

    home: Path
    hot_root: Path
    cold_root: Path

    @classmethod
    def from_environment(cls, *, cwd: Path | None = None) -> "StorageRoots":
        base = Path(os.environ.get("BEELZEBUB_HOME") or cwd or Path.cwd())
        hot = Path(os.environ.get("BEELZEBUB_HOT_ROOT") or (base / "runtime" / "hot"))
        cold = Path(os.environ.get("BEELZEBUB_COLD_ROOT") or (base / "runtime" / "cold"))
        return cls(home=base, hot_root=hot, cold_root=cold)

    def ensure_hot(self) -> None:
        for path in (self.hot_root, self.hot_root / "spool", self.hot_root / "migration-snapshots"):
            path.mkdir(parents=True, exist_ok=True)

    def ensure_cold(self) -> None:
        for name in ("logs", "archives", "backups", "exports", "obsidian", "source-cache", "experiments", "graveyard", "snapshots"):
            (self.cold_root / name).mkdir(parents=True, exist_ok=True)

    def cold_status(self) -> dict[str, Any]:
        available = self.cold_root.exists() and os.access(self.cold_root, os.W_OK)
        return {
            "hot_root": str(self.hot_root),
            "cold_root": str(self.cold_root),
            "state": "READY" if available else "DEGRADED_ARCHIVAL",
            "cold_available": available,
        }


@dataclass(frozen=True)
class ArchiveManifest:
    archive_id: str
    content_class: str
    source_range: str
    destination: str
    checksum_sha256: str
    record_count: int
    created_at: str
    schema_version: int = 1


class ColdArchiveSpool:
    """Bounded hot spool and explicit, caller-scheduled cold-drive flush."""

    def __init__(self, roots: StorageRoots, *, max_bytes: int = 512 * 1024 * 1024, max_age_seconds: int = 7 * 24 * 3600) -> None:
        if max_bytes <= 0 or max_age_seconds <= 0:
            raise ValueError("Archive spool bounds must be positive.")
        self.roots = roots
        self.max_bytes = max_bytes
        self.max_age_seconds = max_age_seconds
        self.roots.ensure_hot()

    @property
    def spool_root(self) -> Path:
        return self.roots.hot_root / "spool"

    def enqueue(self, content_class: str, records: Iterable[dict[str, Any]]) -> Path:
        """Write only to the local hot spool; safe for an unavailable D: drive."""
        safe_class = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in content_class)
        path = self.spool_root / f"{_utc_now().replace(':', '').replace('-', '')}-{uuid4().hex}-{safe_class}.jsonl"
        count = 0
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(json.dumps(_redact(record), sort_keys=True, separators=(",", ":"), default=str))
                handle.write("\n")
                count += 1
        if count == 0:
            path.unlink(missing_ok=True)
        self.enforce_bounds()
        return path

    def backlog(self) -> dict[str, Any]:
        entries = [item for item in self.spool_root.glob("*.jsonl") if item.is_file()]
        return {"files": len(entries), "bytes": sum(item.stat().st_size for item in entries), "max_bytes": self.max_bytes}

    def enforce_bounds(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Bound archival backlog; callers can surface any eviction as health evidence."""
        now = now or datetime.now(timezone.utc)
        entries = sorted((item for item in self.spool_root.glob("*.jsonl") if item.is_file()), key=lambda item: item.stat().st_mtime)
        total = sum(item.stat().st_size for item in entries)
        evicted = 0
        for item in entries:
            age = now.timestamp() - item.stat().st_mtime
            if total <= self.max_bytes and age <= self.max_age_seconds:
                continue
            size = item.stat().st_size
            item.unlink(missing_ok=True)
            total -= size
            evicted += 1
        return {"evicted_files": evicted, "remaining_bytes": max(0, total), "max_bytes": self.max_bytes}

    def flush_once(self) -> dict[str, Any]:
        """Flush a bounded batch to cold storage.  Never call this from a decision path."""
        status = self.roots.cold_status()
        if not status["cold_available"]:
            return {**status, "flushed": 0, "backlog": self.backlog()}
        archive_root = self.roots.cold_root / "archives" / datetime.now(timezone.utc).strftime("%Y/%m/%d")
        archive_root.mkdir(parents=True, exist_ok=True)
        manifests: list[ArchiveManifest] = []
        for source in sorted(self.spool_root.glob("*.jsonl")):
            destination = archive_root / source.name
            shutil.copy2(source, destination)
            if _digest(source) != _digest(destination):
                destination.unlink(missing_ok=True)
                raise OSError(f"Archive checksum mismatch for {source.name}")
            with source.open("r", encoding="utf-8") as handle:
                record_count = sum(1 for _ in handle)
            manifests.append(ArchiveManifest(
                archive_id=uuid4().hex,
                content_class=source.stem.rsplit("-", 1)[-1],
                source_range=source.name,
                destination=str(destination),
                checksum_sha256=_digest(destination),
                record_count=record_count,
                created_at=_utc_now(),
            ))
            source.unlink()
        if manifests:
            manifest_path = self.roots.cold_root / "archives" / "manifest.jsonl"
            with manifest_path.open("a", encoding="utf-8", newline="\n") as handle:
                for manifest in manifests:
                    handle.write(json.dumps(asdict(manifest), sort_keys=True, separators=(",", ":")))
                    handle.write("\n")
        return {**status, "flushed": len(manifests), "manifests": [asdict(item) for item in manifests], "backlog": self.backlog()}


def migrate_sqlite_to_hot(*, source: Path, destination: Path, roots: StorageRoots) -> dict[str, Any]:
    """Copy an old SQLite database through SQLite's backup API with provenance.

    The source is never removed.  A newer existing destination is a hard stop,
    and both the snapshot and destination are integrity-checked before success.
    """
    source, destination = Path(source), Path(destination)
    roots.ensure_hot()
    if not source.exists():
        return {"state": "SOURCE_ABSENT", "source": str(source), "destination": str(destination)}
    if source.resolve() == destination.resolve():
        return {"state": "ALREADY_HOT", "source": str(source), "destination": str(destination)}
    if destination.exists() and destination.stat().st_mtime > source.stat().st_mtime:
        raise FileExistsError("Refusing to overwrite a newer hot SQLite destination.")
    snapshot = roots.hot_root / "migration-snapshots" / f"{source.stem}-{_utc_now().replace(':', '').replace('-', '')}.sqlite3"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(source)) as old, closing(sqlite3.connect(snapshot)) as snap:
        old.backup(snap)
    with closing(sqlite3.connect(snapshot)) as verification:
        if verification.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise sqlite3.DatabaseError("Legacy SQLite snapshot failed integrity verification.")
    with closing(sqlite3.connect(snapshot)) as snap, closing(sqlite3.connect(destination)) as hot:
        snap.backup(hot)
    with closing(sqlite3.connect(destination)) as verification:
        if verification.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise sqlite3.DatabaseError("Hot SQLite destination failed integrity verification.")
    provenance = {
        "state": "MIGRATED", "source": str(source), "snapshot": str(snapshot), "destination": str(destination),
        "source_checksum_sha256": _digest(source), "destination_checksum_sha256": _digest(destination), "migrated_at": _utc_now(),
    }
    (roots.hot_root / "migration-provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True), encoding="utf-8")
    return provenance
