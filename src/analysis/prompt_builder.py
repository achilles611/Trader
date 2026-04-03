from __future__ import annotations

import json
from pathlib import Path


def load_system_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def build_analysis_input(bundle, *, max_signal_events: int, log_excerpts: dict[str, list[str]]) -> str:
    compact_payload = {
        "cycle_metadata": {
            "cycle_id": bundle.cycle_id,
            "status": bundle.status,
            "git_sha": bundle.git_sha,
            "repo_branch": bundle.repo_branch,
            "market": bundle.market,
            "generation": bundle.generation,
            "timing": bundle.timing.to_dict(),
            "totals": {
                "pnl": bundle.total_pnl,
                "drawdown": bundle.total_drawdown,
                "trades": bundle.total_trades,
            },
        },
        "guardrails": bundle.guardrails,
        "compile_test_result": bundle.compile_test_result,
        "rolling_comparison": bundle.rolling_comparison,
        "profile_deltas": bundle.profile_deltas,
        "bot_summaries": [bot.to_dict() for bot in bundle.bot_runs],
        "top_signals_events": bundle.top_events[:max_signal_events],
        "log_excerpts": log_excerpts,
    }
    return json.dumps(compact_payload, indent=2, sort_keys=True)
