"""L3H storage admission guard; canonical evidence is never auto-deleted."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil


DISARM_WATERMARK = 0.85


@dataclass(frozen=True)
class DiskStatus:
    root: str
    total_bytes: int
    free_bytes: int
    used_ratio: float
    admission_allowed: bool
    reason: str


def evaluate_disk(root: str | Path) -> DiskStatus:
    """Evaluate a rooted volume without performing cleanup or data deletion."""

    resolved = Path(root).expanduser().resolve()
    usage = shutil.disk_usage(resolved)
    used_ratio = (usage.total - usage.free) / usage.total if usage.total else 1.0
    allowed = usage.free > 0 and used_ratio <= DISARM_WATERMARK
    return DiskStatus(
        root=str(resolved), total_bytes=usage.total, free_bytes=usage.free, used_ratio=used_ratio,
        admission_allowed=allowed, reason="PASS" if allowed else "DISK_PRESSURE_DISARM",
    )
