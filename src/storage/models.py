from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ArtifactLayout:
    artifact_root: Path
    cycle_root: Path
    cycle_bundle_path: Path
    cycle_summary_path: Path
    cycle_report_path: Path
    analysis_json_path: Path
    analysis_summary_path: Path
    analysis_request_path: Path
    analysis_raw_response_path: Path
    patch_request_path: Path
    patch_diff_path: Path
    validation_report_path: Path
    combined_signals_path: Path
    combined_orders_path: Path
    shared_log_dir: Path
    shared_analysis_dir: Path
    shared_patch_dir: Path
    shared_report_dir: Path

    def bot_root(self, bot_id: str) -> Path:
        return self.cycle_root / "bots" / bot_id

    def bot_trade_log(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "trades.jsonl"

    def bot_signal_log(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "signals.jsonl"

    def bot_report_path(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "session_report.json"

    def bot_network_viz_path(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "network.svg"

    def bot_network_json_path(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "network.json"

    def bot_activations_path(self, bot_id: str) -> Path:
        return self.bot_root(bot_id) / "activations_latest.json"

    def bot_log_path(self, bot_id: str) -> Path:
        return self.shared_log_dir / f"bot_{bot_id}.log"


@dataclass(frozen=True)
class CycleTiming:
    cycle_id: str
    expected_trigger_at: str
    actual_trigger_at: str
    started_at: str
    finished_at: str | None
    drift_seconds: float
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NormalizedBotRun:
    bot_id: str
    profile_name: str
    family: str
    config_hash: str
    repo_sha: str
    market: str
    started_at: str
    ended_at: str
    pnl: float
    win_rate: float
    drawdown: float
    trade_count: int
    avg_hold_sec: float
    expectancy: float
    sharpe_like: float
    max_adverse_excursion: float
    max_favorable_excursion: float
    block_reason_counts: dict[str, int]
    signal_diagnostics: dict[str, Any]
    per_trade_summary: list[dict[str, Any]]
    artifact_path: str
    genome: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NormalizedCycleBundle:
    cycle_id: str
    status: str
    dry_run: bool
    run_mode: str
    git_sha: str
    repo_branch: str
    market: str
    generation: int
    timing: CycleTiming
    total_pnl: float
    total_drawdown: float
    total_trades: int
    guardrails: dict[str, Any]
    compile_test_result: dict[str, Any]
    top_events: list[dict[str, Any]]
    rolling_comparison: dict[str, Any]
    profile_deltas: list[dict[str, Any]]
    bot_runs: list[NormalizedBotRun]
    log_excerpts: dict[str, list[str]]
    dashboard_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["timing"] = self.timing.to_dict()
        payload["bot_runs"] = [bot.to_dict() for bot in self.bot_runs]
        return payload


@dataclass(frozen=True)
class RunCycleRecord:
    cycle_id: str
    started_at: str
    expected_trigger_at: str
    actual_trigger_at: str
    git_sha: str
    status: str
    dry_run: bool
    run_mode: str
    summary_artifact_path: str
    analysis_artifact_path: str = ""
    finished_at: str | None = None
    total_pnl: float = 0.0
    total_drawdown: float = 0.0
    total_trades: int = 0
    drift_seconds: float = 0.0
    duration_seconds: float = 0.0
    failure_code: str | None = None
    failure_details: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None


@dataclass(frozen=True)
class BotRunRecord:
    bot_id: str
    profile_name: str
    config_hash: str
    symbol: str
    pnl: float
    win_rate: float
    drawdown: float
    trade_count: int
    avg_hold_sec: float
    expectancy: float
    sharpe_like: float
    max_adverse_excursion: float
    max_favorable_excursion: float
    block_reason_counts: dict[str, int]
    artifact_path: str
    repo_sha: str
    started_at: str
    ended_at: str
    family: str
    signal_diagnostics: dict[str, Any]
    genome: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AIAnalysisRecord:
    model: str
    prompt_cache_key: str
    request_tokens_est: int
    response_tokens_est: int
    json_result: dict[str, Any]
    recommendation_grade: str
    patch_request_artifact_path: str
    request_size_bytes: int = 0
    response_size_bytes: int = 0
    latency_ms: float = 0.0
    schema_validation_result: str = "valid"
    analysis_status: str = "ok"
    response_id: str = ""
    request_id: str = ""


@dataclass(frozen=True)
class PatchAttemptRecord:
    branch_name: str
    diff_artifact_path: str
    validation_status: str
    merged_to_develop: bool
    promoted_to_main: bool
    notes: str = ""
