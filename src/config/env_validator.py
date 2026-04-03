from __future__ import annotations

import os
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dotenv import dotenv_values


@dataclass(frozen=True)
class EnvCheck:
    key: str
    required_for: str
    present: bool
    message: str
    aliases: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EnvValidationReport:
    mode: str
    valid: bool
    missing_keys: list[str]
    checks: list[EnvCheck]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "valid": self.valid,
            "missing_keys": self.missing_keys,
            "warnings": list(self.warnings),
            "checks": [check.to_dict() for check in self.checks],
        }


def _value_for(key: str, env_values: dict[str, str | None], aliases: tuple[str, ...]) -> str | None:
    for candidate in (key, *aliases):
        current = os.getenv(candidate)
        if current not in (None, ""):
            return current
        file_value = env_values.get(candidate)
        if file_value not in (None, ""):
            return str(file_value)
    return None


def _check_key(key: str, env_values: dict[str, str | None], *, required_for: str, aliases: tuple[str, ...] = ()) -> EnvCheck:
    value = _value_for(key, env_values, aliases)
    present = value not in (None, "")
    alias_text = f" aliases={list(aliases)}" if aliases else ""
    return EnvCheck(
        key=key,
        aliases=aliases,
        required_for=required_for,
        present=present,
        message=f"{key} present" if present else f"{key} missing{alias_text}",
    )


def _writable_path_check(path: Path) -> tuple[bool, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    probe = path.parent / f".write-check-{uuid.uuid4().hex}"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True, "writable"
    except OSError as exc:
        return False, str(exc)


def validate_environment(settings, *, root_dir: Path) -> EnvValidationReport:
    env_path = root_dir / ".env"
    env_values = dotenv_values(env_path) if env_path.exists() else {}
    mode = "dry_run" if settings.dry_run else "live" if settings.live_trading_enabled else "paper"
    checks: list[EnvCheck] = [
        _check_key("DRY_RUN", env_values, required_for="dry-run", aliases=("SWARM_DRY_RUN",)),
        _check_key("LIVE_TRADING_ENABLED", env_values, required_for="all"),
        _check_key("PATCHING_ENABLED", env_values, required_for="all"),
        _check_key("SCHEDULER_TIMEZONE", env_values, required_for="all"),
        _check_key("TRADER_ARTIFACT_ROOT", env_values, required_for="all", aliases=("ARTIFACT_ROOT",)),
        _check_key("TRADER_DB_PATH", env_values, required_for="all", aliases=("SQLITE_DB_PATH",)),
        _check_key("RUN_LOCK_PATH", env_values, required_for="all"),
        _check_key("OPENAI_MODEL_ANALYSIS", env_values, required_for="analysis"),
    ]
    warnings: list[str] = []

    if settings.analysis.enabled:
        checks.append(_check_key("OPENAI_API_KEY", env_values, required_for=mode))
    if settings.patching.enabled:
        checks.append(_check_key("OPENAI_MODEL_PATCH", env_values, required_for="patching"))
        checks.append(_check_key("CODEX_PATCH_COMMAND", env_values, required_for="patching"))
    if not settings.dry_run and settings.live_trading_enabled:
        checks.append(_check_key("COINBASE_API_KEY", env_values, required_for="live"))
        checks.append(_check_key("COINBASE_API_SECRET", env_values, required_for="live"))
        checks.append(_check_key("BOT_MODE", env_values, required_for="live"))
    else:
        bot_mode = _value_for("BOT_MODE", env_values, ())
        if bot_mode and bot_mode.lower() == "live":
            warnings.append("BOT_MODE=live is set, but the wrapper will force paper mode because dry-run/live-trading is not active.")

    missing_keys = [check.key for check in checks if not check.present]
    valid = not missing_keys
    return EnvValidationReport(mode=mode, valid=valid, missing_keys=missing_keys, checks=checks, warnings=warnings)


def validate_writable_targets(settings) -> dict[str, dict[str, Any]]:
    artifact_ok, artifact_message = _writable_path_check(settings.artifact_root / ".probe")
    db_ok, db_message = _writable_path_check(settings.db_path)
    return {
        "artifact_root": {
            "ok": artifact_ok,
            "path": str(settings.artifact_root),
            "message": artifact_message,
        },
        "db_path": {
            "ok": db_ok,
            "path": str(settings.db_path),
            "message": db_message,
        },
    }
