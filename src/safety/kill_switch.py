from __future__ import annotations

from pathlib import Path


def global_kill_switch_reason(enabled: bool, path: Path) -> str | None:
    if enabled:
        return "global_kill_switch_env"
    if not path.exists():
        return None
    reason = path.read_text(encoding="utf-8").strip()
    return reason or f"global_kill_switch_file:{path}"
