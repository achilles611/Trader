from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import ArtifactLayout, NormalizedCycleBundle


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, content: str) -> None:
    _ensure_dir(path.parent)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def prepare_artifact_layout(artifact_root: Path, cycle_started_at: datetime, cycle_id: str) -> ArtifactLayout:
    day_root = artifact_root / "cycles" / cycle_started_at.strftime("%Y") / cycle_started_at.strftime("%m") / cycle_started_at.strftime("%d")
    cycle_root = day_root / cycle_id
    shared_log_dir = artifact_root / "logs"
    shared_analysis_dir = artifact_root / "analysis"
    shared_patch_dir = artifact_root / "patches"
    shared_report_dir = artifact_root / "reports"
    for directory in (
        cycle_root,
        cycle_root / "analysis",
        cycle_root / "patches",
        cycle_root / "reports",
        cycle_root / "validation",
        cycle_root / "logs",
        cycle_root / "bots",
        shared_log_dir,
        shared_analysis_dir,
        shared_patch_dir,
        shared_report_dir,
    ):
        _ensure_dir(directory)
    return ArtifactLayout(
        artifact_root=artifact_root,
        cycle_root=cycle_root,
        cycle_bundle_path=cycle_root / "cycle_bundle.json",
        cycle_summary_path=cycle_root / "cycle_summary.md",
        cycle_report_path=cycle_root / "reports" / "cycle_report.md",
        analysis_json_path=cycle_root / "analysis" / "analysis.json",
        analysis_summary_path=cycle_root / "analysis" / "analysis.md",
        analysis_request_path=cycle_root / "analysis" / "analysis_request.json",
        analysis_raw_response_path=cycle_root / "analysis" / "analysis_response.json",
        patch_request_path=cycle_root / "patches" / "patch_request.md",
        patch_diff_path=cycle_root / "patches" / "patch.diff",
        validation_report_path=cycle_root / "validation" / "validate_patch.json",
        combined_signals_path=cycle_root / "logs" / "signals.jsonl",
        combined_orders_path=cycle_root / "logs" / "orders.jsonl",
        shared_log_dir=shared_log_dir,
        shared_analysis_dir=shared_analysis_dir,
        shared_patch_dir=shared_patch_dir,
        shared_report_dir=shared_report_dir,
    )


def build_cycle_markdown(bundle: NormalizedCycleBundle) -> str:
    lines = [
        f"# Cycle {bundle.cycle_id}",
        "",
        f"- Status: `{bundle.status}`",
        f"- Dry run: `{bundle.dry_run}`",
        f"- Run mode: `{bundle.run_mode}`",
        f"- Git SHA: `{bundle.git_sha}`",
        f"- Market: `{bundle.market}`",
        f"- Total PnL: `{bundle.total_pnl:.2f}`",
        f"- Total drawdown: `{bundle.total_drawdown:.2%}`",
        f"- Total trades: `{bundle.total_trades}`",
        f"- Drift from boundary: `{bundle.timing.drift_seconds:.2f}s`",
        f"- Duration: `{bundle.timing.duration_seconds:.2f}s`",
        "",
        "## Bot Summary",
        "",
    ]
    for bot in bundle.bot_runs:
        lines.append(
            f"- `{bot.bot_id}` `{bot.profile_name}` pnl=`{bot.pnl:.2f}` trades=`{bot.trade_count}` "
            f"win_rate=`{bot.win_rate:.2%}` drawdown=`{bot.drawdown:.2%}`"
        )
    if bundle.top_events:
        lines.extend(["", "## Top Events", ""])
        for event in bundle.top_events[:10]:
            reason = event.get("reason") or event.get("block_reason") or "n/a"
            lines.append(
                f"- `{event.get('instance_id', 'unknown')}` `{event.get('action_candidate', 'hold')}` "
                f"`{reason}` quality=`{event.get('entry_quality_score', 0)}` executed=`{event.get('executed', False)}`"
            )
    return "\n".join(lines)


def build_analysis_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# AI Analysis",
        "",
        f"- Verdict: `{result.get('cycle_verdict', 'unknown')}`",
        f"- Summary: {result.get('summary', '')}",
        "",
        "## Findings",
        "",
    ]
    for finding in result.get("global_findings", []):
        lines.append(f"- {finding}")
    if result.get("risk_flags"):
        lines.extend(["", "## Risk Flags", ""])
        for flag in result["risk_flags"]:
            lines.append(f"- {flag}")
    if result.get("next_experiments"):
        lines.extend(["", "## Next Experiments", ""])
        for item in result["next_experiments"]:
            lines.append(
                f"- P{item.get('priority', '?')} `{item.get('scope', 'unknown')}` {item.get('description', '')}"
            )
    return "\n".join(lines)
