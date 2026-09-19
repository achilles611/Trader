"""Emit a durable, topology-bound offline DEC26 risk-continuity receipt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.risk_continuity import (
    read_pong_risk_continuity_artifact,
    validate_pong_risk_continuity_snapshot,
    validate_risk_continuity_snapshot,
    write_pong_risk_continuity_artifact,
)
from src.l3g_paper.sessions import OFF_SESSION_PROFILE


PROFILE = "BEELZEBUB_PONG_DISCERNMENT_V1"
OPERATION = "pong-dec26-continuity-offline-20260919"


def snapshot() -> dict[str, object]:
    return {
        "schema": "lane-iii-paper-risk-continuity-snapshot-v2",
        "generated_at": "2026-09-19T16:00:00Z",
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ DEC26",
        "source_profile": PROFILE,
        "source_ledger": {
            "path": "N:\\Beelzebub\\offline\\dec26-ledger.sqlite",
            "ledger_identity": "offline-dec26-ledger",
            "ledger_epoch": "epoch-dec26-v1",
            "risk_boundary_sequence": 1,
            "risk_boundary_hash": "a" * 64,
            "coverage_complete": True,
        },
        "authority_lockout": {
            "locked_out": True,
            "lockout_reason": "OFFLINE_CONTINUITY_TEST_LOCKOUT",
            "lockout_trade_date": "2026-09-18",
        },
        "trade_dates": [{
            "trade_date": "2026-09-18",
            "realized_pnl": "5.00",
            "unrealized_pnl": "0",
            "entry_count": 1,
        }],
        "profile_trade_dates": [{
            "profile": PROFILE,
            "trade_date": "2026-09-18",
            "entry_count": 1,
            "consecutive_losses": 0,
        }],
        "sessions": [{
            "profile": PROFILE,
            "session_kind": "OFF_SESSION",
            "session_family": "OFF_SESSION",
            "session_id": "MNQZ6:OFF_SESSION:2026-09-18",
            "trade_date": "2026-09-18",
            "session_profile_hash": OFF_SESSION_PROFILE.profile_hash,
            "session_generation": 1,
            "entry_count": 1,
            "realized_pnl": "5.00",
        }],
        "entry_execution_ids": ["entry-dec26-1"],
        "exit_execution_ids": ["exit-dec26-1"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = snapshot()
    generic_refused = False
    try:
        validate_risk_continuity_snapshot(value, require_flat=True)
    except RuntimeError as error:
        generic_refused = str(error) == "RISK_CONTINUITY_ACCOUNT_IDENTITY_MISMATCH"
    if not generic_refused:
        raise RuntimeError("Expected generic SEP26 validator to refuse DEC26 identity")
    validated = validate_pong_risk_continuity_snapshot(
        value, topology=DEC26_TOPOLOGY, require_flat=True,
    )
    artifact_path = args.output.with_name("pong-risk-continuity-artifact.json")
    artifact = write_pong_risk_continuity_artifact(
        artifact_path,
        operation_id=OPERATION,
        source_profile=PROFILE,
        target_profile=PROFILE,
        snapshot=value,
        topology=DEC26_TOPOLOGY,
    )
    restored = read_pong_risk_continuity_artifact(
        artifact_path,
        topology=DEC26_TOPOLOGY,
        operation_id=OPERATION,
        target_profile=PROFILE,
    )
    result = {
        "schema": "beelzebub-pong-risk-continuity-proof-v1",
        "status": "PASS",
        "qualification": "OFFLINE_FIXTURE_ONLY",
        "authority": "DISARMED",
        "native_dispatches": 0,
        "topology": DEC26_TOPOLOGY.payload(),
        "generic_sep26_validator_refused": generic_refused,
        "validated_instrument": validated["instrument"],
        "entry_count": validated["trade_dates"][0]["entry_count"],
        "realized_pnl": validated["trade_dates"][0]["realized_pnl"],
        "lockout_preserved": validated["authority_lockout"]["locked_out"],
        "artifact_sha256": artifact["artifact_sha256"],
        "artifact_round_trip": restored["artifact_sha256"] == artifact["artifact_sha256"],
        "fresh_data_required": [
            "current Sim101 positions/orders/risk totals",
            "current DEC26 producer session and reconciliation",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
