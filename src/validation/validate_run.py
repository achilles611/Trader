from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..eth_bot.market_data import CoinbasePublicClient
from ..safety.position_limits import validate_profile_limits


@dataclass(frozen=True)
class ValidationCheck:
    name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunValidationReport:
    overall_status: str
    can_run: bool
    should_skip: bool
    checks: list[ValidationCheck]

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "can_run": self.can_run,
            "should_skip": self.should_skip,
            "checks": [check.to_dict() for check in self.checks],
        }


def _calculate_recent_volatility_pct(client: CoinbasePublicClient, config) -> float:
    candles = client.get_candles(
        product_id=config.product_id,
        granularity=config.granularity,
        limit=min(24, config.lookback_candles),
    )
    if len(candles) < 2:
        return 0.0
    moves = []
    for previous, current in zip(candles, candles[1:]):
        if previous.close <= 0:
            continue
        moves.append(abs((current.close - previous.close) / previous.close) * 100)
    return max(moves, default=0.0)


def validate_cycle_preconditions(settings, bot_definitions, database, repo_state) -> RunValidationReport:
    checks: list[ValidationCheck] = []
    base_config = bot_definitions[0].base_config

    checks.append(
        ValidationCheck(
            name="repo_state",
            status="pass" if not repo_state.is_dirty and repo_state.branch == settings.git.production_branch else "fail",
            message="repo clean and on production branch" if not repo_state.is_dirty and repo_state.branch == settings.git.production_branch else "repo must be clean and on production branch",
            details={"branch": repo_state.branch, "dirty_files": repo_state.dirty_files},
        )
    )

    try:
        database.ping()
        checks.append(ValidationCheck(name="database", status="pass", message="sqlite reachable"))
    except Exception as exc:
        checks.append(ValidationCheck(name="database", status="fail", message=f"sqlite unreachable: {exc}"))

    if settings.live_trading_enabled and base_config.mode != "live":
        checks.append(
            ValidationCheck(
                name="live_mode",
                status="fail",
                message="LIVE_TRADING_ENABLED requires BOT_MODE=live",
            )
        )
    else:
        checks.append(
            ValidationCheck(
                name="live_mode",
                status="pass",
                message=f"runner mode is {base_config.mode}",
            )
        )

    if settings.analysis.enabled and not settings.analysis.api_key:
        checks.append(
            ValidationCheck(
                name="openai_credentials",
                status="warn",
                message="OPENAI_API_KEY not configured; cycle can continue in store-only mode",
            )
        )
    else:
        checks.append(
            ValidationCheck(
                name="openai_credentials",
                status="pass",
                message="analysis credentials ready" if settings.analysis.enabled else "analysis disabled",
            )
        )

    risk_issues = validate_profile_limits(settings, bot_definitions)
    checks.append(
        ValidationCheck(
            name="risk_limits",
            status="pass" if not risk_issues else "fail",
            message="profile guardrails valid" if not risk_issues else "; ".join(risk_issues),
        )
    )

    client = CoinbasePublicClient(
        timeout_seconds=base_config.market_data_timeout_seconds,
        max_retries=base_config.market_data_max_retries,
        retry_backoff_seconds=base_config.market_data_retry_backoff_seconds,
    )
    try:
        product = client.get_product_info(base_config.product_id)
        details = {"product_id": product.product_id, "trading_disabled": product.trading_disabled}
        checks.append(ValidationCheck(name="exchange_public_api", status="pass", message="public market data reachable", details=details))
        volatility_pct = _calculate_recent_volatility_pct(client, base_config)
        if settings.safety.skip_run_if_volatility_spike and volatility_pct >= settings.safety.volatility_spike_threshold_pct:
            checks.append(
                ValidationCheck(
                    name="volatility_guard",
                    status="skip",
                    message=f"recent volatility {volatility_pct:.2f}% exceeded threshold {settings.safety.volatility_spike_threshold_pct:.2f}%",
                )
            )
        else:
            checks.append(
                ValidationCheck(
                    name="volatility_guard",
                    status="pass",
                    message=f"recent volatility {volatility_pct:.2f}%",
                )
            )
    except Exception as exc:
        status = "skip" if settings.safety.skip_run_if_api_down else "fail"
        checks.append(
            ValidationCheck(
                name="exchange_public_api",
                status=status,
                message=f"market-data check failed: {exc}",
            )
        )

    now = datetime.now(timezone.utc)
    checks.append(
        ValidationCheck(
            name="clock",
            status="pass",
            message="clock is timezone-aware UTC",
            details={"utc_now": now.isoformat()},
        )
    )

    should_skip = any(check.status == "skip" for check in checks)
    has_failures = any(check.status == "fail" for check in checks)
    overall_status = "skip" if should_skip else "fail" if has_failures else "pass"
    can_run = overall_status == "pass"
    return RunValidationReport(overall_status=overall_status, can_run=can_run, should_skip=should_skip, checks=checks)
