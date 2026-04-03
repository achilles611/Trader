from __future__ import annotations


def build_patch_request(bundle, analysis_result: dict, *, branch_name: str, diff_line_limit: int) -> dict:
    return {
        "title": f"Trader swarm patch request for {bundle.cycle_id}",
        "base_sha": bundle.git_sha,
        "target_branch": branch_name,
        "summary": analysis_result.get("summary", ""),
        "cycle_verdict": analysis_result.get("cycle_verdict", "hold"),
        "global_findings": analysis_result.get("global_findings", []),
        "risk_flags": analysis_result.get("risk_flags", []),
        "next_experiments": analysis_result.get("next_experiments", []),
        "patch_requests": analysis_result.get("patch_requests", []),
        "constraints": [
            "Never modify main directly.",
            "Keep changes bounded to the files named in patch_requests whenever possible.",
            f"Keep the resulting diff under {diff_line_limit} changed lines unless validation evidence justifies more.",
            "Run compile, lint, tests, backtest, regression, risk, and secret-leak validation before asking for promotion.",
            "If validation fails, leave production deployment on main unchanged.",
        ],
    }


def build_patch_request_markdown(payload: dict) -> str:
    lines = [
        "# Codex Patch Request",
        "",
        f"- Target branch: `{payload['target_branch']}`",
        f"- Base SHA: `{payload['base_sha']}`",
        f"- Verdict: `{payload['cycle_verdict']}`",
        f"- Summary: {payload['summary']}",
        "",
        "## Constraints",
        "",
    ]
    for item in payload.get("constraints", []):
        lines.append(f"- {item}")
    if payload.get("next_experiments"):
        lines.extend(["", "## Next Experiments", ""])
        for experiment in payload["next_experiments"]:
            lines.append(
                f"- P{experiment.get('priority', '?')} `{experiment.get('scope', 'unknown')}` {experiment.get('description', '')}"
            )
    if payload.get("patch_requests"):
        lines.extend(["", "## Requested Changes", ""])
        for request in payload["patch_requests"]:
            lines.append(
                f"- `{request.get('target_file', 'unknown')}` `{request.get('change_type', 'config')}` {request.get('instruction', '')}"
            )
            for constraint in request.get("bounded_constraints", []):
                lines.append(f"  Constraint: {constraint}")
    return "\n".join(lines)
