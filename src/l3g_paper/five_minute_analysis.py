"""Deterministic post-session export for the five-minute paper experiment."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Iterable, Mapping

from .contracts import FIVE_MINUTE_POLICY


REPORT_SCHEMA = "beelzebub-five-minute-session-analysis-v1"
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def _decimal(value: object) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _payload(row: Mapping[str, object]) -> dict[str, object]:
    decoded = json.loads(str(row["payload_json"]))
    if not isinstance(decoded, dict) or not isinstance(decoded.get("payload"), dict):
        raise RuntimeError(f"Malformed immutable ledger envelope at sequence {row['ledger_sequence']}.")
    return decoded


def _session_id(envelope: Mapping[str, object]) -> str | None:
    payload = envelope.get("payload")
    value = payload.get("session_id") if isinstance(payload, Mapping) else None
    if not isinstance(value, str):
        value = envelope.get("session_id")
    return value if isinstance(value, str) and value else None


def _read_snapshot(path: Path) -> tuple[list[dict[str, object]], dict[str, object]]:
    if not path.is_file():
        raise RuntimeError(f"Paper ledger does not exist: {path}")
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN")
        tip = connection.execute(
            "SELECT ledger_sequence, record_hash, occurred_at FROM lane_iii_paper_audit "
            "ORDER BY ledger_sequence DESC LIMIT 1"
        ).fetchone()
        if tip is None:
            raise RuntimeError("Paper ledger is empty.")
        rows = connection.execute(
            "SELECT ledger_sequence, domain, kind, occurred_at, payload_json, record_hash "
            "FROM lane_iii_paper_audit "
            "WHERE domain IN ('SESSION','DECISION','COMMAND','EXECUTION','INCIDENT') "
            "ORDER BY ledger_sequence"
        ).fetchall()
        result = [dict(row) for row in rows]
        return result, {
            "ledger_sequence": int(tip["ledger_sequence"]),
            "record_hash": str(tip["record_hash"]),
            "occurred_at": str(tip["occurred_at"]),
        }
    finally:
        connection.close()


def _choose_closed_session(
    records: Iterable[dict[str, object]], requested: str | None,
) -> tuple[str, dict[str, object]]:
    closed: list[tuple[str, dict[str, object]]] = []
    for row in records:
        if row["domain"] != "SESSION" or row["kind"] not in {
            "SESSION_CLOSED", "SESSION_OPERATIONAL_PAPER_STOPPED",
        }:
            continue
        envelope = _payload(row)
        if envelope.get("paper_policy_hash") != FIVE_MINUTE_POLICY.configuration_hash:
            continue
        identifier = _session_id(envelope)
        if identifier is not None:
            closed.append((identifier, {**dict(row), "envelope": envelope}))
    if requested is not None:
        matches = [item for item in closed if item[0] == requested]
        if not matches:
            raise RuntimeError(f"No closed five-minute session found for {requested!r}.")
        return matches[-1]
    if not closed:
        raise RuntimeError("No closed five-minute experimental session is available.")
    return closed[-1]


def build_session_analysis(
    ledger_path: str | Path, *, session_id: str | None = None,
) -> dict[str, object]:
    """Build one stable analysis from a consistent read-only ledger snapshot."""
    path = Path(ledger_path).expanduser().resolve()
    records, tip = _read_snapshot(path)
    selected_session, closure = _choose_closed_session(records, session_id)

    decisions: list[dict[str, object]] = []
    realized_by_entry: dict[str, list[dict[str, object]]] = {}
    reversal_entry_by_source: dict[str, str] = {}
    reversal_exit_by_source: dict[str, str] = {}
    command_ids_by_decision: dict[str, list[str]] = {}
    for row in records:
        if int(row["ledger_sequence"]) > int(closure["ledger_sequence"]):
            continue
        envelope = _payload(row)
        if _session_id(envelope) != selected_session:
            continue
        payload = envelope["payload"]
        assert isinstance(payload, dict)
        if row["domain"] == "COMMAND":
            decision_id = payload.get("decision_id")
            command_id = payload.get("command_id")
            if isinstance(decision_id, str) and isinstance(command_id, str):
                command_ids_by_decision.setdefault(decision_id, []).append(command_id)
        elif row["domain"] == "EXECUTION" and row["kind"] == "EXECUTION_REALIZED_PNL":
            entry_id = payload.get("entry_decision_id")
            if isinstance(entry_id, str):
                realized_by_entry.setdefault(entry_id, []).append(payload)
        elif row["domain"] == "DECISION" and row["kind"] == "DECISION":
            if payload.get("paper_policy_hash") != FIVE_MINUTE_POLICY.configuration_hash:
                continue
            family = payload.get("family_summary")
            if not isinstance(family, dict):
                continue
            source_reversal = family.get("source_reversal_decision_id")
            decision_id = payload.get("paper_decision_id")
            if isinstance(source_reversal, str) and isinstance(decision_id, str):
                reversal_entry_by_source[source_reversal] = decision_id
            sources = payload.get("source_observation_ids")
            if (
                family.get("risk_exit") in {"FIVE_MINUTE_REVERSE_TO_LONG", "FIVE_MINUTE_REVERSE_TO_SHORT"}
                and isinstance(decision_id, str)
                and isinstance(sources, list)
                and len(sources) == 1
                and isinstance(sources[0], str)
            ):
                reversal_exit_by_source[sources[0]] = decision_id
            if "candle_close_utc" not in family:
                continue
            decisions.append({
                "ledger_sequence": int(row["ledger_sequence"]),
                "record_hash": str(row["record_hash"]),
                "decision_id": decision_id,
                "decision": payload.get("decision"),
                "reason_code": payload.get("reason_code"),
                "relative_support": payload.get("relative_support"),
                "source_observation_ids": payload.get("source_observation_ids"),
                "source_local_sequences": payload.get("source_local_sequences"),
                **family,
            })

    decisions.sort(key=lambda item: (str(item["candle_close_utc"]), int(item["ledger_sequence"])))
    evaluated = 0
    wins = 0
    losses = 0
    flat_outcomes = 0
    endpoint_points = Decimal("0")
    realized_total = Decimal("0")
    for index, decision in enumerate(decisions):
        decision_id = decision.get("decision_id")
        staged_entry_id = reversal_entry_by_source.get(str(decision_id))
        staged_exit_id = reversal_exit_by_source.get(str(decision_id))
        related_ids = [
            value for value in (decision_id, staged_exit_id, staged_entry_id) if isinstance(value, str)
        ]
        entry_ids = [value for value in (decision_id, staged_entry_id) if isinstance(value, str)]
        realized = [item for entry_id in entry_ids for item in realized_by_entry.get(entry_id, [])]
        realized_values = [value for item in realized if (value := _decimal(item.get("realized_pnl"))) is not None]
        decision["command_ids"] = [
            command_id for related_id in related_ids for command_id in command_ids_by_decision.get(related_id, [])
        ]
        decision["staged_reversal_exit_decision_id"] = staged_exit_id
        decision["staged_reversal_entry_decision_id"] = staged_entry_id
        decision["realized_trade_pnl"] = str(sum(realized_values, Decimal("0"))) if realized_values else None
        realized_total += sum(realized_values, Decimal("0"))

        current_mark = _decimal(decision.get("decision_reference_price"))
        next_mark = (
            _decimal(decisions[index + 1].get("decision_reference_price"))
            if index + 1 < len(decisions) else None
        )
        target = decision.get("target_position")
        if current_mark is None or next_mark is None or target not in {"LONG", "SHORT"}:
            decision["next_boundary_mark_points"] = None
            decision["next_boundary_mark_dollars"] = None
            decision["outcome"] = "BLOCKED_OR_UNRESOLVED"
            continue
        raw = next_mark - current_mark
        signed = raw if target == "LONG" else -raw
        dollars = signed * Decimal("2")
        decision["next_boundary_reference_price"] = str(next_mark)
        decision["next_boundary_mark_points"] = str(signed)
        decision["next_boundary_mark_dollars"] = str(dollars)
        decision["outcome"] = "WORKED" if signed > 0 else "LOST" if signed < 0 else "FLAT"
        evaluated += 1
        endpoint_points += signed
        wins += int(signed > 0)
        losses += int(signed < 0)
        flat_outcomes += int(signed == 0)

    closure_envelope = closure["envelope"]
    assert isinstance(closure_envelope, dict)
    summary = {
        "decision_count": len(decisions),
        "evaluated_interval_count": evaluated,
        "worked_count": wins,
        "lost_count": losses,
        "flat_count": flat_outcomes,
        "unresolved_or_blocked_count": len(decisions) - evaluated,
        "worked_rate": None if not evaluated else str(Decimal(wins) / Decimal(evaluated)),
        "net_next_boundary_mark_points": str(endpoint_points),
        "net_next_boundary_mark_dollars": str(endpoint_points * Decimal("2")),
        "realized_trade_pnl": str(realized_total),
        "outcome_basis": "NEXT_BOUNDARY_REFERENCE_MARK_BEFORE_FEES_AND_SLIPPAGE",
        "realized_pnl_basis": "AUTHENTIC_ENTRY_AND_EXIT_FILLS_WHEN_AVAILABLE",
    }
    analysis_identity = {
        "schema": REPORT_SCHEMA,
        "session_id": selected_session,
        "paper_policy_hash": FIVE_MINUTE_POLICY.configuration_hash,
        "source_ledger_tip_sequence": closure["ledger_sequence"],
        "source_ledger_tip_hash": closure["record_hash"],
        "closure_record_hash": closure["record_hash"],
    }
    analysis_id = "beelzebub-5m-analysis-" + hashlib.sha256(
        json.dumps(analysis_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return {
        **analysis_identity,
        "analysis_id": analysis_id,
        "ledger_path": str(path),
        "source_ledger_tip_occurred_at": closure["occurred_at"],
        "closure_kind": closure["kind"],
        "closure_ledger_sequence": closure["ledger_sequence"],
        "closure_payload": closure_envelope["payload"],
        "entry_profile": FIVE_MINUTE_POLICY.entry_profile,
        "entry_profile_version": FIVE_MINUTE_POLICY.entry_profile_version,
        "scientific_eligibility": False,
        "paper_only": True,
        "live_capital": "DENIED",
        "summary": summary,
        "decisions": decisions,
    }


def _json_text(report: Mapping[str, object]) -> str:
    return json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _csv_text(report: Mapping[str, object]) -> str:
    fields = (
        "candle_open_utc", "candle_close_utc", "decision_observed_at", "decision_latency_ms",
        "decision_id", "prior_position", "bias", "target_position", "action", "decision",
        "reason_code", "bullish_support", "bearish_support", "score_delta",
        "decision_reference_price", "next_boundary_reference_price",
        "next_boundary_mark_points", "next_boundary_mark_dollars", "outcome",
        "realized_trade_pnl", "missed_boundary_count", "ledger_sequence", "record_hash",
    )
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(report.get("decisions", []))
    return buffer.getvalue()


def _markdown_text(report: Mapping[str, object]) -> str:
    summary = report["summary"]
    assert isinstance(summary, Mapping)
    lines = [
        "# Beelzebub five-minute session analysis",
        "",
        f"- Analysis: `{report['analysis_id']}`",
        f"- Session: `{report['session_id']}`",
        f"- Profile: `{report['entry_profile_version']}`",
        f"- Decisions: {summary['decision_count']}",
        f"- Worked / lost / flat: {summary['worked_count']} / {summary['lost_count']} / {summary['flat_count']}",
        f"- Net next-boundary mark: {summary['net_next_boundary_mark_points']} points (${summary['net_next_boundary_mark_dollars']})",
        f"- Realized fill P&L: ${summary['realized_trade_pnl']}",
        "- Outcome marks are pre-fee, pre-slippage diagnostics; realized P&L uses authenticated fills when available.",
        "",
        "| Candle close (UTC) | Action | Bias | From -> target | Bull / bear | Reason | Outcome | Mark P&L | Realized |",
        "|---|---|---|---|---|---|---|---:|---:|",
    ]
    for item in report.get("decisions", []):
        if not isinstance(item, Mapping):
            continue
        lines.append(
            "| {close} | {action} | {bias} | {prior} -> {target} | {bull} / {bear} | {reason} | {outcome} | {mark} | {realized} |".format(
                close=item.get("candle_close_utc", ""),
                action=item.get("action", ""),
                bias=item.get("bias", ""),
                prior=item.get("prior_position", ""),
                target=item.get("target_position", ""),
                bull=item.get("bullish_support", ""),
                bear=item.get("bearish_support", ""),
                reason=item.get("reason_code", ""),
                outcome=item.get("outcome", ""),
                mark=item.get("next_boundary_mark_dollars") or "",
                realized=item.get("realized_trade_pnl") or "",
            )
        )
    return "\n".join(lines) + "\n"


def _write_once(path: Path, content: str) -> None:
    encoded = content.encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise RuntimeError(f"Immutable analysis artifact already exists with different content: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def export_session_analysis(
    ledger_path: str | Path,
    output_directory: str | Path,
    *,
    session_id: str | None = None,
) -> dict[str, object]:
    report = build_session_analysis(ledger_path, session_id=session_id)
    root = Path(output_directory).expanduser().resolve()
    safe_session = _SAFE_NAME.sub("_", str(report["session_id"]))
    stem = f"{safe_session}-{report['analysis_id']}"
    contents = {
        "json": _json_text(report),
        "csv": _csv_text(report),
        "markdown": _markdown_text(report),
    }
    suffixes = {"json": ".json", "csv": ".csv", "markdown": ".md"}
    artifacts: dict[str, object] = {}
    for name, content in contents.items():
        path = root / f"{stem}{suffixes[name]}"
        _write_once(path, content)
        artifacts[name] = {
            "path": str(path),
            "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        }
    return {
        "analysis_id": report["analysis_id"],
        "session_id": report["session_id"],
        "artifacts": artifacts,
        "summary": report["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--session-id")
    args = parser.parse_args()
    result = export_session_analysis(args.ledger, args.output, session_id=args.session_id)
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
