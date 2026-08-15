from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from .models import jsonable


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _section(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"copytrade config section '{name}' must be a mapping.")
    return value


@dataclass(frozen=True)
class SourceConfig:
    venue: str = "hyperliquid"
    network: str = "mainnet"
    info_url: str = "https://api.hyperliquid.xyz/info"
    websocket_url: str = "wss://api.hyperliquid.xyz/ws"
    request_timeout_seconds: float = 20.0
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    stale_after_seconds: float = 60.0
    # userFills carries wallet attribution.  The shared userEvents and
    # orderUpdates feeds do not provide a safe uniform attribution contract.
    subscribe_order_updates: bool = False
    subscribe_position_state: bool = True
    subscribe_market_data: bool = True


@dataclass(frozen=True)
class ShadowObservationConfig:
    """Read-only D.4 account observation; it carries no trading authority."""

    enabled: bool = False
    venue: str = "hyperliquid"
    account_id: str = ""
    max_age_seconds: float = 60.0


@dataclass(frozen=True)
class CapitalConfig:
    initial_capital: float = 200.0
    currency: str = "USD"


@dataclass(frozen=True)
class SizingConfig:
    mode: str = "ratio"
    small_fraction: float = 0.05
    medium_fraction: float = 0.10
    large_fraction: float = 0.20
    small_ratio_max: float = 0.67
    large_ratio_min: float = 1.50
    min_history: int = 10
    fallback_fraction: float = 0.10
    quantiles: tuple[float, float] = (0.33, 0.67)
    copy_initial_entries: bool = True
    copy_target_adds: bool = False
    max_equity_age_seconds: float = 300.0
    accepted_equity_sources: tuple[str, ...] = ("source_fill", "exact", "recent_live_snapshot", "sampled_prior_proxy")


@dataclass(frozen=True)
class RiskConfig:
    max_total_committed_fraction: float = 0.80
    max_capital_per_target_fraction: float = 0.30
    max_capital_per_symbol_fraction: float = 0.40
    max_simultaneous_virtual_campaigns: int = 7
    daily_loss_stop_fraction: float = 0.10
    target_loss_stop_fraction: float = 0.15
    max_copy_drawdown_fraction: float = 0.20
    max_consecutive_losses: int = 5
    kill_switch_path: Path = Path("state/copytrade_kill_switch.txt")
    max_signal_age_seconds: float = 30.0
    max_price_deviation_bps: float = 50.0
    insufficient_capital_action: str = "scale"
    symbol_allowlist: tuple[str, ...] = ()
    symbol_blocklist: tuple[str, ...] = ()
    max_leverage: float | None = None
    risk_cap_base: str = "current_equity"


@dataclass(frozen=True)
class PaperExecutionConfig:
    fee_rate: float = 0.00045
    slippage_bps: float = 5.0
    min_order_notional: float = 10.0
    quantity_precision: int = 6
    detection_latency_ms: int = 250
    order_latency_ms: int = 100
    missed_trade_rate: float = 0.0
    random_seed: int = 7
    market_data_max_age_ms: int = 3_000
    mark_persist_interval_ms: int = 1_000
    stale_exit_market_policy: str = "target_fill_fallback"


@dataclass(frozen=True)
class BacktestConfig:
    detection_delays_ms: tuple[int, ...] = (100, 250, 500, 1000, 2000, 5000, 15000)
    slippage_scenarios_bps: tuple[float, ...] = (0.0, 1.0, 2.0, 5.0, 10.0, 25.0)
    training_windows_days: tuple[int, ...] = (30, 60, 90, 180)
    forward_windows_days: tuple[int, ...] = (7, 14, 30)
    default_training_days: int = 90
    default_forward_days: int = 30


@dataclass(frozen=True)
class CandidateConfig:
    history_days_min: int = 90
    history_days_preferred: int = 180
    closed_campaigns_min: int = 100
    max_drawdown_preferred: float = 0.15
    max_drawdown_hard: float = 0.25
    max_follower_drawdown_preferred: float = 0.15
    max_follower_drawdown_hard: float = 0.30
    liquidation_frequency_hard: float = 0.10
    minimum_copyability_hard: float = 0.05
    pnl_concentration_preferred: float = 0.50
    pnl_concentration_hard: float = 1.0  # 1.0 disables the optional hard gate
    profit_factor_preferred: float = 1.40
    require_positive_expectancy: bool = True
    require_positive_follower_expectancy: bool = True
    activity_max_age_days: int = 30
    require_proven_history: bool = False
    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "follower_performance": 18.0,
            "copyability": 14.0,
            "risk_adjusted_expectancy": 14.0,
            "drawdown_tail": 12.0,
            "follower_drawdown": 7.0,
            "walk_forward": 5.0,
            "consistency": 10.0,
            "latency_survivability": 5.0,
            "history_quality": 5.0,
            "position_size_stability": 5.0,
            "source_quality": 2.0,
            "friction_robustness": 5.0,
            "regime_robustness": 4.0,
        }
    )
    penalty_weights: dict[str, float] = field(
        default_factory=lambda: {
            "martingale": 15.0,
            "adverse_averaging": 10.0,
            "liquidation": 10.0,
            "negative_skew": 8.0,
            "concentration": 8.0,
            "small_sample": 10.0,
            "inactivity": 10.0,
            "latency_decay": 15.0,
            "follower_drawdown": 8.0,
            "jackpot_concentration": 12.0,
            "regime_dependency": 6.0,
            "friction_sensitivity": 8.0,
        }
    )


@dataclass(frozen=True)
class PrefilterConfig:
    """Cheap Stage A.2 gates applied before public backfill/simulation."""

    min_activity_observations: int = 0  # zero inherits analysis.min_discovery_activity
    min_distinct_active_days: int = 0
    min_distinct_active_hours: int = 0
    min_observation_span_hours: float = 0.0
    min_observed_notional: float = 0.0
    min_distinct_symbols: int = 0
    exclude_operator_managed: bool = True


@dataclass(frozen=True)
class ConfidenceConfig:
    """Evidence confidence is intentionally separate from suitability (0-100)."""

    closed_campaigns_reference: int = 100
    active_days_reference: int = 30
    history_days_reference: int = 180
    walk_forward_windows_reference: int = 4
    regimes_reference: int = 2


@dataclass(frozen=True)
class RegimeConfig:
    """Deterministic observed-price proxy segmentation; no ML classifier."""

    enabled: bool = True
    volatility_move_threshold: float = 0.03
    minimum_campaigns_per_regime: int = 3


@dataclass(frozen=True)
class FinalistRequirementsConfig:
    """Evidence policy for shadow-finalist recommendations, not raw scores."""

    minimum_confidence_score: float = 60.0
    require_copyability_evidence: bool = True
    require_walk_forward_evidence: bool = False
    require_latency_evidence: bool = False
    require_regime_evidence: bool = False


@dataclass(frozen=True)
class AnalysisConfig:
    """Bounded Phase B research-orchestration settings."""

    default_workers: int = 4
    retry_attempts: int = 3
    retry_initial_seconds: float = 0.25
    # One shared operating allowance for all public /info consumers in this
    # process.  It stays below Hyperliquid's documented IP ceiling.
    api_weight_budget_per_minute: int = 900
    rate_limit_backoff_initial_seconds: float = 2.0
    rate_limit_backoff_max_seconds: float = 30.0
    rate_limit_jitter_seconds: float = 0.5
    history_days: int = 180
    min_discovery_activity: int = 2
    shadow_finalist_count: int = 20
    walk_forward_min_windows: int = 2
    high_suitability_score: float = 70.0
    market_evidence_enabled: bool = True
    market_evidence_bucket_seconds: int = 60


@dataclass(frozen=True)
class ArtifactConfig:
    database_path: Path = Path("artifacts/copytrade.sqlite3")
    obsidian_root: Path = Path("artifacts/obsidian")
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8090


@dataclass(frozen=True)
class CopyTradeConfig:
    mode: str = "paper"
    live_enabled: bool = False
    source: SourceConfig = field(default_factory=SourceConfig)
    shadow_observation: ShadowObservationConfig = field(default_factory=ShadowObservationConfig)
    capital: CapitalConfig = field(default_factory=CapitalConfig)
    sizing: SizingConfig = field(default_factory=SizingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    paper_execution: PaperExecutionConfig = field(default_factory=PaperExecutionConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    candidates: CandidateConfig = field(default_factory=CandidateConfig)
    prefilter: PrefilterConfig = field(default_factory=PrefilterConfig)
    confidence: ConfidenceConfig = field(default_factory=ConfidenceConfig)
    regimes: RegimeConfig = field(default_factory=RegimeConfig)
    finalist_requirements: FinalistRequirementsConfig = field(default_factory=FinalistRequirementsConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    artifacts: ArtifactConfig = field(default_factory=ArtifactConfig)
    targets: tuple[dict[str, Any], ...] = ()
    config_path: Path = Path("config/copytrade.yaml")

    @classmethod
    def from_yaml(cls, path: str | Path = "config/copytrade.yaml") -> "CopyTradeConfig":
        load_dotenv()
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Copy-trading config not found: {config_path}")
        document = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if not isinstance(document, dict):
            raise ValueError("copytrade config must be a YAML mapping.")

        source = _section(document, "source")
        shadow_observation = _section(document, "shadow_observation")
        capital = _section(document, "capital")
        sizing = _section(document, "sizing")
        risk = _section(document, "risk")
        paper = _section(document, "paper_execution")
        fees = _section(document, "fees")
        slippage = _section(document, "slippage")
        latency = _section(document, "latency")
        backtest = _section(document, "backtest")
        candidates = _section(document, "candidates")
        prefilter = _section(document, "prefilter")
        confidence = _section(document, "confidence")
        regimes = _section(document, "regimes")
        finalist_requirements = _section(document, "finalist_requirements")
        analysis = _section(document, "analysis")
        artifacts = _section(document, "artifacts")
        database = _section(document, "database")
        obsidian = _section(document, "obsidian")
        dashboard = _section(document, "dashboard")
        future_live = _section(document, "future_live_execution")

        mode = os.getenv("COPYTRADE_MODE", str(document.get("mode", "paper"))).lower()
        live_setting = os.getenv("COPYTRADE_LIVE_ENABLED")
        live_enabled = _bool(live_setting, _bool(future_live.get("enabled"), False))
        config = cls(
            mode=mode,
            live_enabled=live_enabled,
            source=SourceConfig(**source),
            shadow_observation=ShadowObservationConfig(**shadow_observation),
            capital=CapitalConfig(**capital),
            sizing=SizingConfig(
                **{
                    **sizing,
                    "quantiles": tuple(sizing.get("quantiles", (0.33, 0.67))),
                    "accepted_equity_sources": tuple(sizing.get("accepted_equity_sources", SizingConfig().accepted_equity_sources)),
                }
            ),
            risk=RiskConfig(
                **{
                    **risk,
                    "kill_switch_path": Path(risk.get("kill_switch_path", "state/copytrade_kill_switch.txt")),
                    "symbol_allowlist": tuple(risk.get("symbol_allowlist", ())),
                    "symbol_blocklist": tuple(risk.get("symbol_blocklist", ())),
                }
            ),
            paper_execution=PaperExecutionConfig(
                **{
                    **paper,
                    "fee_rate": fees.get("paper_fee_rate", fees.get("fee_rate", paper.get("fee_rate", PaperExecutionConfig().fee_rate))),
                    "slippage_bps": slippage.get("paper_bps", slippage.get("slippage_bps", paper.get("slippage_bps", PaperExecutionConfig().slippage_bps))),
                    "detection_latency_ms": latency.get("detection_ms", latency.get("detection_latency_ms", paper.get("detection_latency_ms", PaperExecutionConfig().detection_latency_ms))),
                    "order_latency_ms": latency.get("order_ms", latency.get("order_latency_ms", paper.get("order_latency_ms", PaperExecutionConfig().order_latency_ms))),
                }
            ),
            backtest=BacktestConfig(
                **{
                    **backtest,
                    "detection_delays_ms": tuple(backtest.get("detection_delays_ms", BacktestConfig().detection_delays_ms)),
                    "slippage_scenarios_bps": tuple(backtest.get("slippage_scenarios_bps", BacktestConfig().slippage_scenarios_bps)),
                    "training_windows_days": tuple(backtest.get("training_windows_days", BacktestConfig().training_windows_days)),
                    "forward_windows_days": tuple(backtest.get("forward_windows_days", BacktestConfig().forward_windows_days)),
                }
            ),
            candidates=CandidateConfig(**candidates),
            prefilter=PrefilterConfig(**prefilter),
            confidence=ConfidenceConfig(**confidence),
            regimes=RegimeConfig(**regimes),
            finalist_requirements=FinalistRequirementsConfig(**finalist_requirements),
            analysis=AnalysisConfig(**analysis),
            artifacts=ArtifactConfig(
                **{
                    **artifacts,
                    "database_path": Path(database.get("path", artifacts.get("database_path", "artifacts/copytrade.sqlite3"))),
                    "obsidian_root": Path(obsidian.get("root", artifacts.get("obsidian_root", "artifacts/obsidian"))),
                    "dashboard_host": dashboard.get("host", artifacts.get("dashboard_host", ArtifactConfig().dashboard_host)),
                    "dashboard_port": dashboard.get("port", artifacts.get("dashboard_port", ArtifactConfig().dashboard_port)),
                }
            ),
            targets=tuple(document.get("targets", ())),
            config_path=config_path,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.mode not in {"paper", "live"}:
            raise ValueError("COPYTRADE_MODE must be 'paper' or 'live'.")
        # This alpha has no live execution adapter.  The double opt-in prevents
        # accidentally treating credentials or a copied config as permission.
        if self.mode == "live" and not self.live_enabled:
            raise ValueError("Live copy trading requires COPYTRADE_MODE=live and COPYTRADE_LIVE_ENABLED=true.")
        if self.mode == "live":
            raise ValueError("Live copy trading is not implemented in this alpha; use paper mode.")
        if self.shadow_observation.venue.lower() != "hyperliquid":
            raise ValueError("D.4 shadow observation currently supports only the Hyperliquid public read-only venue.")
        if self.shadow_observation.max_age_seconds <= 0:
            raise ValueError("shadow_observation.max_age_seconds must be positive.")
        if self.shadow_observation.enabled:
            account_id = self.shadow_observation.account_id.lower()
            if not (account_id.startswith("0x") and len(account_id) == 42
                    and all(character in "0123456789abcdef" for character in account_id[2:])):
                raise ValueError("shadow_observation.account_id must be a public 0x-prefixed 20-byte account address.")
        if self.capital.initial_capital <= 0:
            raise ValueError("capital.initial_capital must be positive.")
        if not 0 <= self.sizing.small_fraction <= self.sizing.medium_fraction <= self.sizing.large_fraction <= 1:
            raise ValueError("Sizing fractions must be ordered and between zero and one.")
        if self.sizing.small_ratio_max <= 0 or self.sizing.large_ratio_min < self.sizing.small_ratio_max:
            raise ValueError("Sizing ratio thresholds are invalid.")
        if self.sizing.min_history < 0:
            raise ValueError("sizing.min_history must be >= 0.")
        if not 0 <= self.paper_execution.missed_trade_rate <= 1:
            raise ValueError("paper_execution.missed_trade_rate must be between zero and one.")
        if self.paper_execution.quantity_precision < 0:
            raise ValueError("paper_execution.quantity_precision must be >= 0.")
        if self.paper_execution.market_data_max_age_ms < 0 or self.paper_execution.mark_persist_interval_ms < 0:
            raise ValueError("paper market-data ages and persistence intervals must be >= 0.")
        if self.paper_execution.stale_exit_market_policy not in {"target_fill_fallback", "skip"}:
            raise ValueError("paper_execution.stale_exit_market_policy must be target_fill_fallback or skip.")
        if not 0 < self.risk.max_total_committed_fraction <= 1:
            raise ValueError("risk.max_total_committed_fraction must be in (0, 1].")
        if self.risk.insufficient_capital_action not in {"scale", "skip"}:
            raise ValueError("risk.insufficient_capital_action must be scale or skip.")
        if self.risk.risk_cap_base not in {"initial_capital", "start_of_day_equity", "current_equity"}:
            raise ValueError("risk.risk_cap_base must be initial_capital, start_of_day_equity, or current_equity.")
        if self.analysis.default_workers <= 0 or self.analysis.retry_attempts <= 0:
            raise ValueError("analysis.default_workers and analysis.retry_attempts must be positive.")
        if self.analysis.retry_initial_seconds < 0 or self.analysis.history_days <= 0 or self.analysis.min_discovery_activity <= 0:
            raise ValueError("analysis retry delay, history days, and minimum discovery activity must be positive.")
        if not 1 <= self.analysis.api_weight_budget_per_minute <= 1_200:
            raise ValueError("analysis.api_weight_budget_per_minute must be between 1 and Hyperliquid's 1200 weight/minute limit.")
        if (self.analysis.rate_limit_backoff_initial_seconds <= 0
                or self.analysis.rate_limit_backoff_max_seconds < self.analysis.rate_limit_backoff_initial_seconds
                or self.analysis.rate_limit_jitter_seconds < 0):
            raise ValueError("analysis rate-limit backoff configuration is invalid.")
        if self.analysis.walk_forward_min_windows <= 0:
            raise ValueError("analysis.walk_forward_min_windows must be positive.")
        if not 0 <= self.analysis.high_suitability_score <= 100:
            raise ValueError("analysis.high_suitability_score must be in [0, 100].")
        if self.analysis.market_evidence_bucket_seconds <= 0:
            raise ValueError("analysis.market_evidence_bucket_seconds must be positive.")
        if not 0 <= self.candidates.max_follower_drawdown_preferred <= self.candidates.max_follower_drawdown_hard <= 1:
            raise ValueError("Follower drawdown thresholds must be ordered fractions in [0, 1].")
        if not 0 < self.candidates.liquidation_frequency_hard <= 1:
            raise ValueError("candidates.liquidation_frequency_hard must be in (0, 1].")
        if not 0 <= self.candidates.minimum_copyability_hard <= 1:
            raise ValueError("candidates.minimum_copyability_hard must be in [0, 1].")
        if not 0 <= self.candidates.pnl_concentration_preferred <= self.candidates.pnl_concentration_hard <= 1:
            raise ValueError("candidate P&L concentration thresholds must be ordered fractions in [0, 1].")
        if min(self.prefilter.min_activity_observations, self.prefilter.min_distinct_active_days,
               self.prefilter.min_distinct_active_hours, self.prefilter.min_distinct_symbols) < 0:
            raise ValueError("prefilter count thresholds must be >= 0.")
        if self.prefilter.min_observation_span_hours < 0 or self.prefilter.min_observed_notional < 0:
            raise ValueError("prefilter span/notional thresholds must be >= 0.")
        if min(self.confidence.closed_campaigns_reference, self.confidence.active_days_reference,
               self.confidence.history_days_reference, self.confidence.walk_forward_windows_reference,
               self.confidence.regimes_reference) <= 0:
            raise ValueError("confidence reference thresholds must be positive.")
        if self.regimes.volatility_move_threshold < 0 or self.regimes.minimum_campaigns_per_regime <= 0:
            raise ValueError("regime thresholds are invalid.")
        if not 0 <= self.finalist_requirements.minimum_confidence_score <= 100:
            raise ValueError("finalist_requirements.minimum_confidence_score must be in [0, 100].")

    def snapshot(self) -> dict[str, Any]:
        return jsonable(asdict(self))

    def research_snapshot(self) -> dict[str, Any]:
        """Return the Phase-B semantic configuration projection.

        The full snapshot remains durable run provenance. Public-request
        scheduling is operational rather than a research/scoring assumption,
        so changing its limiter settings must not stale otherwise identical
        Phase-B scores or finalist recommendations.
        """
        snapshot = self.snapshot()
        analysis = dict(snapshot.get("analysis") or {})
        for field_name in (
            "api_weight_budget_per_minute",
            "rate_limit_backoff_initial_seconds",
            "rate_limit_backoff_max_seconds",
            "rate_limit_jitter_seconds",
        ):
            analysis.pop(field_name, None)
        snapshot["analysis"] = analysis
        return snapshot
