from __future__ import annotations

import re
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

from ..eth_bot.backtest import run_backtest
from ..eth_bot.config import BotConfig
from ..safety.position_limits import validate_profile_limits


@dataclass(frozen=True)
class PatchValidationCheck:
    name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PatchValidationReport:
    overall_status: str
    checks: list[PatchValidationCheck]

    def to_dict(self) -> dict[str, Any]:
        return {"overall_status": self.overall_status, "checks": [check.to_dict() for check in self.checks]}


def _run_command(command: str, *, cwd) -> tuple[int, str]:
    result = subprocess.run(
        shlex.split(command),
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode, (result.stdout + result.stderr).strip()


def _git_diff_text(root_dir, base_ref: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--unified=0", base_ref],
        cwd=root_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _git_numstat(root_dir, base_ref: str) -> tuple[int, int]:
    result = subprocess.run(
        ["git", "diff", "--numstat", base_ref],
        cwd=root_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    added = 0
    deleted = 0
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        try:
            added += int(parts[0])
            deleted += int(parts[1])
        except ValueError:
            continue
    return added, deleted


def validate_patch(root_dir, settings, bot_definitions, *, base_ref: str) -> PatchValidationReport:
    checks: list[PatchValidationCheck] = []

    compile_command = f"{sys.executable} -m compileall src"
    compile_code, compile_output = _run_command(compile_command, cwd=root_dir)
    checks.append(
        PatchValidationCheck(
            name="compile",
            status="pass" if compile_code == 0 else "fail",
            message="compileall passed" if compile_code == 0 else "compileall failed",
            details={"output": compile_output[-4000:]},
        )
    )

    if settings.validation.lint_command:
        lint_code, lint_output = _run_command(settings.validation.lint_command, cwd=root_dir)
        checks.append(
            PatchValidationCheck(
                name="lint",
                status="pass" if lint_code == 0 else "fail",
                message="lint passed" if lint_code == 0 else "lint failed",
                details={"output": lint_output[-4000:]},
            )
        )
    else:
        checks.append(PatchValidationCheck(name="lint", status="skip", message="lint command not configured"))

    if settings.validation.test_command:
        test_code, test_output = _run_command(settings.validation.test_command, cwd=root_dir)
        checks.append(
            PatchValidationCheck(
                name="tests",
                status="pass" if test_code == 0 else "fail",
                message="tests passed" if test_code == 0 else "tests failed",
                details={"output": test_output[-4000:]},
            )
        )
    else:
        checks.append(PatchValidationCheck(name="tests", status="skip", message="test command not configured"))

    try:
        summary, _ = run_backtest(BotConfig.from_env(), candles=settings.validation.backtest_candles)
        backtest_details = {
            "ending_equity": summary.ending_equity,
            "total_return_pct": summary.total_return_pct,
            "win_rate_pct": summary.win_rate_pct,
            "max_drawdown_pct": summary.max_drawdown_pct,
        }
        backtest_ok = (
            summary.max_drawdown_pct <= settings.validation.regression_max_drawdown_pct
            and summary.total_return_pct >= settings.validation.regression_min_return_pct
            and summary.win_rate_pct >= settings.validation.regression_min_win_rate_pct
        )
        checks.append(
            PatchValidationCheck(
                name="backtest",
                status="pass" if backtest_ok else "fail",
                message="backtest met regression thresholds" if backtest_ok else "backtest failed regression thresholds",
                details=backtest_details,
            )
        )
    except Exception as exc:
        checks.append(PatchValidationCheck(name="backtest", status="fail", message=f"backtest failed: {exc}"))

    risk_issues = validate_profile_limits(settings, bot_definitions)
    checks.append(
        PatchValidationCheck(
            name="risk_limits",
            status="pass" if not risk_issues else "fail",
            message="profile guardrails valid" if not risk_issues else "; ".join(risk_issues),
        )
    )

    added, deleted = _git_numstat(root_dir, base_ref)
    changed_lines = added + deleted
    checks.append(
        PatchValidationCheck(
            name="diff_size",
            status="pass" if changed_lines <= settings.patching.diff_line_limit else "fail",
            message=f"changed_lines={changed_lines}",
            details={"added": added, "deleted": deleted, "limit": settings.patching.diff_line_limit},
        )
    )

    diff_text = _git_diff_text(root_dir, base_ref)
    leak_matches: list[str] = []
    for line in diff_text.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        for pattern in settings.validation.secret_patterns:
            if pattern in line and not line.rstrip().endswith("="):
                leak_matches.append(line[:200])
        if re.search(r"sk-[A-Za-z0-9]{20,}", line):
            leak_matches.append(line[:200])
    checks.append(
        PatchValidationCheck(
            name="secret_leakage",
            status="pass" if not leak_matches else "fail",
            message="no obvious secrets in diff" if not leak_matches else "secret-like values found in diff",
            details={"matches": leak_matches[:10]},
        )
    )

    has_non_pass = any(check.status != "pass" for check in checks)
    return PatchValidationReport(overall_status="fail" if has_non_pass else "pass", checks=checks)
