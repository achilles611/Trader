"""One-shot clean-genesis creation for an explicitly retired paper ledger."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Mapping

from src.lane_iii.contracts import canonical_hash, normalized_utc

from .ledger import PaperLedger


_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")


def _exact_hex(value: str, pattern: re.Pattern[str], label: str) -> str:
    normalized = value.strip().lower()
    if not pattern.fullmatch(normalized):
        raise ValueError(f"{label} must be an exact lowercase hexadecimal identity.")
    return normalized


def create_clean_reset_genesis(
    ledger_path: str | Path,
    *,
    epoch_id: str,
    reset_receipt_path: str | Path,
    reset_timestamp: str,
    checkout_sha: str,
    build_sha: str,
    runtime_sha: str,
    addon_source_fingerprint: str,
    addon_build_fingerprint: str,
) -> dict[str, object]:
    """Create a blank ledger whose first durable record is the reset genesis.

    The function refuses every existing main/sidecar target.  It never imports,
    opens, or summarizes the retired operational ledger.
    """
    target = Path(ledger_path).expanduser().resolve()
    receipt = Path(reset_receipt_path).expanduser().resolve()
    timestamp = normalized_utc(reset_timestamp, "Lane III clean reset timestamp")
    # Validate parseability as well as the canonical UTC form used in hashes.
    datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if not receipt.is_file():
        raise FileNotFoundError(f"Clean-reset receipt does not exist: {receipt}")
    candidates = tuple(Path(str(target) + suffix) for suffix in ("", "-wal", "-shm", "-journal"))
    existing = [str(path) for path in candidates if path.exists()]
    if existing:
        raise FileExistsError(
            "Clean-reset genesis refuses an existing ledger or sidecar: " + ", ".join(existing)
        )

    provenance = {
        "checkout_sha": _exact_hex(checkout_sha, _GIT_SHA, "CHECKOUT_SHA"),
        "build_sha": _exact_hex(build_sha, _GIT_SHA, "BUILD_SHA"),
        "runtime_sha": _exact_hex(runtime_sha, _GIT_SHA, "RUNTIME_SHA"),
        "addon_source_fingerprint": _exact_hex(
            addon_source_fingerprint, _FINGERPRINT, "AddOn source fingerprint"
        ),
        "addon_build_fingerprint": _exact_hex(
            addon_build_fingerprint, _FINGERPRINT, "AddOn build fingerprint"
        ),
    }
    if len(set(provenance[key] for key in ("checkout_sha", "build_sha", "runtime_sha"))) != 1:
        raise ValueError("Clean-reset genesis requires CHECKOUT_SHA == BUILD_SHA == RUNTIME_SHA.")

    payload: dict[str, object] = {
        "reset_timestamp": timestamp,
        "reason": "AUTHORIZED_CLEAN_PAPER_ONLY_GENESIS",
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "position": "FLAT",
        "quantity": 0,
        "working_owned_orders": 0,
        "paper_execution": "DISARMED",
        "live_capital": "DENIED",
        "provenance": provenance,
        "reset_receipt_path": str(receipt),
    }
    identity = "l3g-clean-reset-genesis-" + canonical_hash(payload)
    ledger = PaperLedger(target, epoch_id=epoch_id, persist_high_frequency_records=False)
    try:
        before = ledger.health_status()
        if before["highest_sequence"] != 0:
            raise RuntimeError("Clean-reset target was not empty before genesis.")
        record_hash = ledger.append(
            "SESSION_CLEAN_RESET_GENESIS",
            payload,
            identity=identity,
            occurred_at=timestamp,
        )
        after = ledger.health_status()
        if after["highest_sequence"] != 1 or after["counts"] != {"SESSION": 1}:
            raise RuntimeError("Clean-reset genesis was not the first and only durable record.")
    finally:
        shutdown = ledger.close()

    return {
        "schema": "l3g-clean-reset-genesis-receipt-v1",
        "ledger_path": str(target),
        "ledger_epoch": epoch_id,
        "ledger_identity": ledger.ledger_identity,
        "genesis_sequence": 1,
        "genesis_identity": identity,
        "genesis_record_hash": record_hash,
        "reset_receipt_path": str(receipt),
        "provenance": provenance,
        "file_size_bytes": target.stat().st_size,
        "controlled_shutdown": shutdown,
    }
