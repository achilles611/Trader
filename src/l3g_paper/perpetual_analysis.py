"""Deterministic, verifier-bound analysis for one stopped Perpetual V2 run."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import os
from pathlib import Path
import sqlite3
import statistics
from typing import Iterable, Mapping

from .contracts import FIVE_MINUTE_PERPETUAL_POLICY


REPORT_SCHEMA = "beelzebub-perpetual-run-analysis-v1"
MANIFEST_SCHEMA = "beelzebub-perpetual-analysis-manifest-v1"


def _decimal(value: object) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _timestamp(value: object) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _payload(row: Mapping[str, object]) -> dict[str, object]:
    decoded = json.loads(str(row["payload_json"]))
    if not isinstance(decoded, dict) or not isinstance(decoded.get("payload"), dict):
        raise RuntimeError(
            f"Malformed immutable ledger envelope at sequence {row['ledger_sequence']}."
        )
    return decoded


def _stable_read(
    path: Path,
    *,
    verified_sequence: int,
    verified_hash: str,
) -> tuple[list[dict[str, object]], dict[str, str]]:
    if not path.is_file():
        raise RuntimeError(f"Paper ledger does not exist: {path}")
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=20.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN")
        fence = connection.execute(
            "SELECT ledger_sequence, record_hash, occurred_at "
            "FROM lane_iii_paper_audit WHERE ledger_sequence=?",
            (verified_sequence,),
        ).fetchone()
        if fence is None or str(fence["record_hash"]) != verified_hash:
            raise RuntimeError("VERIFIED_LEDGER_FENCE_MISMATCH")
        rows = connection.execute(
            "SELECT ledger_sequence, domain, kind, occurred_at, payload_json, record_hash "
            "FROM lane_iii_paper_audit WHERE ledger_sequence<=? ORDER BY ledger_sequence",
            (verified_sequence,),
        ).fetchall()
        metadata = {
            str(row["metadata_key"]): str(row["metadata_value"])
            for row in connection.execute(
                "SELECT metadata_key, metadata_value FROM lane_iii_paper_ledger_metadata"
            )
        }
        return [dict(row) for row in rows], metadata
    finally:
        connection.close()


def _select_run(
    records: Iterable[dict[str, object]], request_id: str | None,
) -> tuple[dict[str, object], dict[str, object]]:
    starts: list[dict[str, object]] = []
    stops: list[dict[str, object]] = []
    policy_hash = FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash
    for row in records:
        if row["domain"] != "SESSION":
            continue
        envelope = _payload(row)
        if envelope.get("paper_policy_hash") != policy_hash:
            continue
        payload = envelope["payload"]
        assert isinstance(payload, dict)
        if request_id is not None and payload.get("request_id") != request_id:
            continue
        item = {**row, "envelope": envelope}
        if row["kind"] == "SESSION_OPERATIONAL_PAPER_STARTED":
            starts.append(item)
        elif row["kind"] == "SESSION_OPERATIONAL_PAPER_STOPPED":
            stops.append(item)
    if not stops:
        raise RuntimeError("No stopped Perpetual V2 run matches the requested identity.")
    stop = stops[-1]
    stop_payload = stop["envelope"]["payload"]
    matching = [
        item for item in starts
        if int(item["ledger_sequence"]) < int(stop["ledger_sequence"])
        and item["envelope"]["payload"].get("request_id") == stop_payload.get("request_id")
    ]
    if not matching:
        raise RuntimeError("Stopped Perpetual V2 run has no matching start record.")
    return matching[-1], stop


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.floor((len(ordered) - 1) * fraction)]


def _summary(
    trades: list[dict[str, object]],
    decisions: list[dict[str, object]],
    *,
    started_at: str,
    stopped_at: str,
    command_counts: Mapping[str, int],
    order_rejections: int,
    duplicate_receipts: int,
) -> dict[str, object]:
    pnl = [_decimal(item.get("realized_pnl")) for item in trades]
    values = [value for value in pnl if value is not None]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    breakevens = [value for value in values if value == 0]
    fees = [
        value for item in trades
        if (value := _decimal(item.get("recorded_simulated_fees"))) is not None
    ]
    holds = [float(item["hold_seconds"]) for item in trades]
    running = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    loss_streak = 0
    maximum_loss_streak = 0
    for value in values:
        running += value
        peak = max(peak, running)
        drawdown = max(drawdown, peak - running)
        loss_streak = loss_streak + 1 if value < 0 else 0
        maximum_loss_streak = max(maximum_loss_streak, loss_streak)
    duration = (_timestamp(stopped_at) - _timestamp(started_at)).total_seconds()
    exposure = sum(holds)
    checkpoint_keys = [
        str(item["source_checkpoint_hash"])
        for item in trades if item.get("source_checkpoint_hash")
    ]
    key_counts: dict[str, int] = {}
    for key in checkpoint_keys:
        key_counts[key] = key_counts.get(key, 0) + 1
    return {
        "operational_duration_seconds": duration,
        "closed_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "breakevens": len(breakevens),
        "win_rate": str(Decimal(len(wins)) / Decimal(len(values))) if values else None,
        "gross_realized_pnl": str(sum(values, Decimal("0"))),
        "recorded_simulated_fees": str(sum(fees, Decimal("0"))),
        "net_pnl": None,
        "net_pnl_reason": "TRUE_COMMISSIONS_AND_FEES_NOT_RETAINED",
        "mean_trade": str(sum(values, Decimal("0")) / Decimal(len(values))) if values else None,
        "average_win": str(sum(wins, Decimal("0")) / Decimal(len(wins))) if wins else None,
        "average_loss": str(sum(losses, Decimal("0")) / Decimal(len(losses))) if losses else None,
        "payoff_ratio": (
            str(
                (sum(wins, Decimal("0")) / Decimal(len(wins)))
                / -(sum(losses, Decimal("0")) / Decimal(len(losses)))
            ) if wins and losses else None
        ),
        "profit_factor": (
            str(sum(wins, Decimal("0")) / -sum(losses, Decimal("0")))
            if losses and sum(losses, Decimal("0")) != 0 else None
        ),
        "maximum_realized_drawdown": str(drawdown),
        "maximum_loss_streak": maximum_loss_streak,
        "hold_seconds": {
            "minimum": min(holds) if holds else None,
            "median": statistics.median(holds) if holds else None,
            "mean": statistics.mean(holds) if holds else None,
            "p90": _percentile(holds, 0.9),
            "maximum": max(holds) if holds else None,
        },
        "exposure_seconds": exposure,
        "exposure_fraction": exposure / duration if duration > 0 else None,
        "flat_seconds": max(0.0, duration - exposure),
        "entries_per_hour": len(trades) * 3600 / duration if duration > 0 else None,
        "direction_counts": _counts(item.get("direction") for item in trades),
        "exit_reason_counts": _counts(item.get("exit_reason") for item in trades),
        "true_reversal_exit_count": sum(
            1 for item in trades
            if str(item.get("exit_reason", "")).startswith("FIVE_MINUTE_REVERSE_TO_")
        ),
        "freshness_emergency_exit_count": sum(
            1 for item in trades
            if item.get("exit_reason") in {"QUOTE_STALE", "CLASSIFIED_TRADE_STALE", "DEPTH_STALE"}
        ),
        "operator_stop_exit_count": sum(
            1 for item in trades if item.get("exit_reason") == "OPERATOR_STOP_TRADING"
        ),
        "boundary_decision_count": len(decisions),
        "unique_boundary_count": len({item.get("candle_close_utc") for item in decisions}),
        "decision_reason_counts": _counts(item.get("reason_code") for item in decisions),
        "unique_entry_checkpoint_count": len(key_counts),
        "same_checkpoint_reentry_count": sum(value - 1 for value in key_counts.values()),
        "command_action_counts": dict(command_counts),
        "order_rejection_count": order_rejections,
        "duplicate_receipt_count": duplicate_receipts,
    }


def _counts(values: Iterable[object]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value)
        result[key] = result.get(key, 0) + 1
    return result


def build_perpetual_analysis(
    ledger_path: str | Path,
    *,
    verification_id: str,
    verified_sequence: int,
    verified_hash: str,
    request_id: str | None = None,
) -> dict[str, object]:
    path = Path(ledger_path).expanduser().resolve()
    records, metadata = _stable_read(
        path, verified_sequence=verified_sequence, verified_hash=verified_hash,
    )
    if metadata.get("paper_policy_hash") != FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash:
        raise RuntimeError("PERPETUAL_POLICY_HASH_MISMATCH")
    start, stop = _select_run(records, request_id)
    start_sequence = int(start["ledger_sequence"])
    stop_sequence = int(stop["ledger_sequence"])
    selected = [
        row for row in records
        if start_sequence <= int(row["ledger_sequence"]) <= stop_sequence
    ]
    commands: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    decisions_by_id: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    entry_fills: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    realized_rows: list[tuple[dict[str, object], dict[str, object]]] = []
    decision_rows: list[dict[str, object]] = []
    equity_rows: list[dict[str, object]] = []
    command_counts: dict[str, int] = {}
    order_rejections = 0
    duplicate_receipts = 0
    for row in selected:
        envelope = _payload(row)
        payload = envelope["payload"]
        assert isinstance(payload, dict)
        if row["domain"] == "COMMAND":
            command_id = payload.get("command_id")
            if isinstance(command_id, str):
                commands[command_id] = (row, payload)
            action = str(payload.get("action"))
            command_counts[action] = command_counts.get(action, 0) + 1
        elif row["domain"] == "DECISION":
            decision_id = payload.get("paper_decision_id")
            if isinstance(decision_id, str):
                decisions_by_id[decision_id] = (row, payload)
            family = payload.get("family_summary")
            if isinstance(family, dict) and "candle_close_utc" in family:
                decision_rows.append({
                    "ledger_sequence": int(row["ledger_sequence"]),
                    "record_hash": str(row["record_hash"]),
                    "occurred_at": row["occurred_at"],
                    "decision_id": decision_id,
                    "decision": payload.get("decision"),
                    "reason_code": payload.get("reason_code"),
                    "relative_support": payload.get("relative_support"),
                    **family,
                })
        elif row["domain"] == "EXECUTION" and row["kind"] == "EXECUTION":
            if payload.get("order_role") == "ENTRY" and isinstance(
                payload.get("native_execution_id"), str,
            ):
                entry_fills[str(payload["native_execution_id"])] = (row, payload)
        elif row["domain"] == "EXECUTION" and row["kind"] == "EXECUTION_REALIZED_PNL":
            realized_rows.append((row, payload))
        elif row["domain"] == "ORDER_EVENT":
            if payload.get("native_error_code") not in {None, "", "NOERROR"}:
                order_rejections += 1
        elif row["domain"] == "COMMAND_RECEIPT" and payload.get("duplicate") is True:
            duplicate_receipts += 1

    trades: list[dict[str, object]] = []
    for row, payload in realized_rows:
        entry_command = commands.get(str(payload.get("entry_command_id")))
        exit_command = commands.get(str(payload.get("exit_command_id")))
        entry_decision = (
            decisions_by_id.get(str(entry_command[1].get("decision_id")))
            if entry_command is not None else None
        )
        family = (
            entry_decision[1].get("family_summary", {})
            if entry_decision is not None else {}
        )
        if not isinstance(family, dict):
            family = {}
        hold_seconds = (
            _timestamp(payload["exit_timestamp"]) - _timestamp(payload["entry_timestamp"])
        ).total_seconds()
        entry_fill = entry_fills.get(str(payload.get("entry_execution_id")))
        command_to_fill_ms = None
        if entry_command is not None and entry_fill is not None:
            command_to_fill_ms = (
                _timestamp(entry_fill[1]["timestamp"])
                - _timestamp(entry_command[1]["created_at"])
            ).total_seconds() * 1000
        direction = (
            str(entry_command[1].get("action", "")).replace("ENTER_", "")
            if entry_command is not None else "UNKNOWN"
        )
        trade = {
            "trade_sequence": len(trades) + 1,
            "ledger_sequence": int(row["ledger_sequence"]),
            "record_hash": str(row["record_hash"]),
            "trade_identity": payload.get("trade_identity"),
            "direction": direction,
            "quantity": payload.get("entry_quantity"),
            "entry_timestamp": payload.get("entry_timestamp"),
            "exit_timestamp": payload.get("exit_timestamp"),
            "hold_seconds": hold_seconds,
            "entry_price": payload.get("entry_price"),
            "exit_price": payload.get("exit_price"),
            "realized_pnl": payload.get("realized_pnl"),
            "recorded_simulated_fees": payload.get("simulated_fees"),
            "pnl_basis": payload.get("pnl_basis"),
            "entry_execution_id": payload.get("entry_execution_id"),
            "exit_execution_id": payload.get("exit_execution_id"),
            "entry_order_id": payload.get("entry_order_id"),
            "exit_order_id": payload.get("exit_order_id"),
            "entry_command_id": payload.get("entry_command_id"),
            "exit_command_id": payload.get("exit_command_id"),
            "entry_decision_id": payload.get("entry_decision_id"),
            "exit_reason": exit_command[1].get("reason_code") if exit_command else None,
            "exit_action": exit_command[1].get("action") if exit_command else None,
            "source_checkpoint_sequence": family.get("source_signal_ledger_sequence"),
            "source_checkpoint_hash": (
                family.get("source_signal_record_hash")
                or (entry_decision[0].get("record_hash") if entry_decision else None)
            ),
            "source_candle_close_utc": (
                family.get("continuity_tip_candle_close_utc")
                or family.get("candle_close_utc")
            ),
            "entry_trigger": family.get("perpetual_trigger"),
            "decision_to_command_ms": (
                (_timestamp(entry_command[1]["created_at"]) - _timestamp(entry_decision[1]["created_at"]))
                .total_seconds() * 1000
                if entry_command is not None and entry_decision is not None else None
            ),
            "command_to_fill_ms": command_to_fill_ms,
            "entry_reporting_session": payload.get("entry_reporting_session_label"),
            "exit_reporting_session": payload.get("exit_reporting_session_label"),
            "cross_reporting_session": payload.get("cross_reporting_session"),
            "trade_date": payload.get("trade_date"),
        }
        trades.append(trade)

    running = Decimal("0")
    peak = Decimal("0")
    for trade in trades:
        value = _decimal(trade.get("realized_pnl")) or Decimal("0")
        running += value
        peak = max(peak, running)
        equity_rows.append({
            "trade_sequence": trade["trade_sequence"],
            "exit_timestamp": trade["exit_timestamp"],
            "realized_pnl": str(value),
            "cumulative_realized_pnl": str(running),
            "realized_drawdown": str(peak - running),
        })

    start_payload = start["envelope"]["payload"]
    stop_payload = stop["envelope"]["payload"]
    assert isinstance(start_payload, dict) and isinstance(stop_payload, dict)
    started_at = str(stop_payload.get("started_at") or start["occurred_at"])
    stopped_at = str(stop["occurred_at"])
    manifest_identity = {
        "schema": MANIFEST_SCHEMA,
        "ledger_identity": metadata.get("ledger_uuid"),
        "ledger_epoch": metadata.get("ledger_epoch"),
        "verification_id": verification_id,
        "verified_sequence": verified_sequence,
        "verified_hash": verified_hash,
        "closure_sequence": stop_sequence,
        "closure_hash": stop["record_hash"],
    }
    analysis_id = "beelzebub-perpetual-analysis-" + hashlib.sha256(
        json.dumps(manifest_identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    summary = _summary(
        trades, decision_rows, started_at=started_at, stopped_at=stopped_at,
        command_counts=command_counts, order_rejections=order_rejections,
        duplicate_receipts=duplicate_receipts,
    )
    return {
        "schema": REPORT_SCHEMA,
        "analysis_id": analysis_id,
        "paper_only": True,
        "live_capital": "DENIED",
        "entry_profile_version": metadata.get("entry_profile_version"),
        "paper_policy_hash": metadata.get("paper_policy_hash"),
        "risk_profile_hash": metadata.get("risk_profile_hash"),
        "run": {
            "request_id": stop_payload.get("request_id"),
            "session_id": stop_payload.get("session_id"),
            "trade_date": stop_payload.get("trade_date"),
            "start_sequence": start_sequence,
            "started_at": started_at,
            "closure_sequence": stop_sequence,
            "closure_hash": stop["record_hash"],
            "stopped_at": stopped_at,
            "stop_reason": stop_payload.get("reason"),
            "final_position": stop_payload.get("final_position"),
            "final_quantity": stop_payload.get("final_quantity"),
            "final_working_order_count": stop_payload.get("final_working_order_count"),
        },
        "analysis_manifest": {
            **manifest_identity,
            "source_ledger_path": str(path),
            "source_snapshot_kind": "CONSISTENT_READ_TRANSACTION_AT_VERIFIED_FENCE",
            "source_snapshot_digest": hashlib.sha256(
                json.dumps(manifest_identity, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
        "summary": summary,
        "limitations": {
            "true_commissions": "NOT_RETAINED",
            "mae_mfe_mark_to_market": "UNAVAILABLE_RAW_PRICES_NOT_RETAINED_IN_LEDGER_ENVELOPES",
            "spread_slippage": "ALREADY_REFLECTED_IN_AUTHENTIC_FILL_TO_FILL_PNL; NOT_SUBTRACTED_AGAIN",
            "decision_marks": "30_SECOND_BOUNDARY_REFERENCE_MARKS_ONLY",
            "scientific_eligibility": False,
            "phase_e_block_eligibility": False,
        },
        "trades": trades,
        "decisions": decision_rows,
        "equity_curve": equity_rows,
    }


def _json_text(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _csv_text(rows: Iterable[Mapping[str, object]], fields: tuple[str, ...]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _markdown_text(report: Mapping[str, object]) -> str:
    run = report["run"]
    summary = report["summary"]
    limitations = report["limitations"]
    assert isinstance(run, Mapping) and isinstance(summary, Mapping)
    assert isinstance(limitations, Mapping)
    return "\n".join([
        "# Perpetual V2 stopped-run analysis",
        "",
        f"- Analysis: `{report['analysis_id']}`",
        f"- Request: `{run['request_id']}`",
        f"- Operational interval: `{run['started_at']}` to `{run['stopped_at']}` ({summary['operational_duration_seconds']} seconds)",
        f"- Closure: sequence `{run['closure_sequence']}`, hash `{run['closure_hash']}`; final `{run['final_position']} {run['final_quantity']}` with `{run['final_working_order_count']}` working orders",
        f"- Closed trades: {summary['closed_trades']} ({summary['wins']} wins / {summary['losses']} losses / {summary['breakevens']} breakeven)",
        f"- Gross realized fill P&L: `${summary['gross_realized_pnl']}`; net P&L unavailable because true commissions were not retained",
        f"- Profit factor: `{summary['profit_factor']}`; maximum realized drawdown: `${summary['maximum_realized_drawdown']}`",
        f"- Exits: `{summary['freshness_emergency_exit_count']}` freshness emergencies, `{summary['true_reversal_exit_count']}` logical reversals, `{summary['operator_stop_exit_count']}` operator stop",
        f"- Same-checkpoint reentries: `{summary['same_checkpoint_reentry_count']}` across `{summary['unique_entry_checkpoint_count']}` unique entry checkpoints",
        "",
        "## Evidence-supported finding",
        "",
        "Freshness emergency exits dominated the run, and multiple later entries reused a checkpoint that had already produced an entry. This is callback/recovery churn, not a new Perpetual switch. Preserve the consumed checkpoint across transient flat-state blockers; allow another entry only after a later durable boundary.",
        "",
        "## Limits",
        "",
        f"- True commissions: `{limitations['true_commissions']}`.",
        f"- MAE/MFE/mark-to-market drawdown: `{limitations['mae_mfe_mark_to_market']}`.",
        "- This is operational paper evidence, not Phase E scientific evidence.",
        "",
    ])


def _write_once(path: Path, content: str) -> dict[str, object]:
    encoded = content.encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise RuntimeError(f"Immutable analysis artifact differs: {path}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    return {"path": str(path), "sha256": hashlib.sha256(encoded).hexdigest()}


def export_perpetual_analysis(
    ledger_path: str | Path,
    output_directory: str | Path,
    *,
    verification_id: str,
    verified_sequence: int,
    verified_hash: str,
    request_id: str | None = None,
) -> dict[str, object]:
    report = build_perpetual_analysis(
        ledger_path,
        verification_id=verification_id,
        verified_sequence=verified_sequence,
        verified_hash=verified_hash,
        request_id=request_id,
    )
    root = Path(output_directory).expanduser().resolve() / str(report["analysis_id"])
    fields = {
        "trades.csv": (
            "trade_sequence", "direction", "quantity", "entry_timestamp", "exit_timestamp",
            "hold_seconds", "entry_price", "exit_price", "realized_pnl", "exit_reason",
            "entry_execution_id", "exit_execution_id", "source_checkpoint_sequence",
            "source_checkpoint_hash", "source_candle_close_utc", "entry_trigger",
            "decision_to_command_ms", "command_to_fill_ms", "ledger_sequence", "record_hash",
        ),
        "decisions.csv": (
            "ledger_sequence", "record_hash", "occurred_at", "candle_open_utc",
            "candle_close_utc", "decision_observed_at", "decision_latency_ms", "decision_id",
            "decision", "reason_code", "prior_position", "target_position", "bias",
            "bullish_support", "bearish_support", "score_delta", "decision_reference_price",
            "missed_boundary_count",
        ),
        "equity.csv": (
            "trade_sequence", "exit_timestamp", "realized_pnl",
            "cumulative_realized_pnl", "realized_drawdown",
        ),
    }
    contents = {
        "report.json": _json_text(report),
        "report.md": _markdown_text(report),
        "manifest.json": _json_text(report["analysis_manifest"]),
        "trades.csv": _csv_text(report["trades"], fields["trades.csv"]),
        "decisions.csv": _csv_text(report["decisions"], fields["decisions.csv"]),
        "equity.csv": _csv_text(report["equity_curve"], fields["equity.csv"]),
    }
    artifacts = {
        name: _write_once(root / name, content) for name, content in contents.items()
    }
    return {
        "analysis_id": report["analysis_id"],
        "output_directory": str(root),
        "summary": report["summary"],
        "artifacts": artifacts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--verification-id", required=True)
    parser.add_argument("--verified-sequence", required=True, type=int)
    parser.add_argument("--verified-hash", required=True)
    parser.add_argument("--request-id")
    args = parser.parse_args()
    result = export_perpetual_analysis(
        args.ledger,
        args.output,
        verification_id=args.verification_id,
        verified_sequence=args.verified_sequence,
        verified_hash=args.verified_hash,
        request_id=args.request_id,
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
