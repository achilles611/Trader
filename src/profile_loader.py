from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any, TypeVar

import yaml
from dotenv import load_dotenv

from .eth_bot.config import (
    BotConfig,
    BotInstanceConfig,
    InstancePaths,
    NetworkConfig,
    StrategyProfile,
    apply_instance_overrides,
)
from .eth_bot.profiles import SWARM_INSTANCE_IDS, default_strategy_profile


T = TypeVar("T")


def _resolve_path(root_dir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (root_dir / path)


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return None


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


def _replace_dataclass(instance: T, overrides: dict[str, Any]) -> T:
    allowed = {field.name for field in fields(type(instance))}
    filtered = {key: value for key, value in overrides.items() if key in allowed}
    return replace(instance, **filtered)


def _fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


@dataclass(frozen=True)
class SchedulerSettings:
    interval_minutes: int
    session_minutes: float
    dry_run_session_minutes: float
    timezone: str


@dataclass(frozen=True)
class GitSettings:
    remote: str
    production_branch: str
    develop_branch: str


@dataclass(frozen=True)
class SafetySettings:
    global_kill_switch: bool
    global_kill_switch_path: Path
    max_notional_per_bot: float
    max_portfolio_notional: float
    max_drawdown_per_cycle: float
    max_open_positions: int
    max_order_rate_per_minute: float
    max_hold_sec_per_profile: dict[str, int]
    skip_run_if_volatility_spike: bool
    volatility_spike_threshold_pct: float
    skip_run_if_api_down: bool
    require_validation_for_promotion: bool


@dataclass(frozen=True)
class AnalysisSettings:
    enabled: bool
    api_key: str | None
    model: str
    patch_model: str
    prompt_path: Path
    schema_path: Path
    prompt_cache_key: str
    prompt_cache_retention: str | None
    timeout_seconds: int
    max_retries: int
    backoff_seconds: float
    store_responses: bool
    rolling_cycles: int
    max_signal_events: int
    log_excerpt_lines: int
    use_previous_response: bool
    project_id: str | None


@dataclass(frozen=True)
class PatchingSettings:
    enabled: bool
    require_validation_for_promotion: bool
    diff_line_limit: int
    experiment_branch_prefix: str
    base_branch: str
    codex_patch_command: str
    auto_promote_to_develop: bool
    auto_promote_to_main: bool


@dataclass(frozen=True)
class ValidationSettings:
    lint_command: str
    test_command: str
    backtest_candles: int
    regression_max_drawdown_pct: float
    regression_min_return_pct: float
    regression_min_win_rate_pct: float
    secret_patterns: list[str]


@dataclass(frozen=True)
class NotificationSettings:
    discord_webhook_url: str | None


@dataclass(frozen=True)
class RuntimeSettings:
    root_dir: Path
    config_path: Path
    bots_dir: Path
    artifact_root: Path
    db_path: Path
    lock_path: Path
    generation: int
    dry_run: bool
    live_trading_enabled: bool
    scheduler: SchedulerSettings
    git: GitSettings
    safety: SafetySettings
    analysis: AnalysisSettings
    patching: PatchingSettings
    validation: ValidationSettings
    notifications: NotificationSettings


@dataclass(frozen=True)
class BotDefinition:
    bot_id: str
    family: str
    profile_name: str
    config_hash: str
    source_path: Path
    base_config: BotConfig
    strategy_profile: StrategyProfile
    network_config: NetworkConfig
    genome: dict[str, Any]

    def build_instance_config(self, root_dir: Path, artifact_layout, generation: int) -> BotInstanceConfig:
        paths = InstancePaths(
            state_path=root_dir / "state" / "instances" / self.bot_id / "state.json",
            trade_log_path=artifact_layout.bot_trade_log(self.bot_id),
            signal_log_path=artifact_layout.bot_signal_log(self.bot_id),
            report_path=artifact_layout.bot_report_path(self.bot_id),
            network_snapshot_path=root_dir / "models" / "instances" / self.bot_id / "network.json",
            network_viz_path=artifact_layout.bot_network_viz_path(self.bot_id),
            network_json_path=artifact_layout.bot_network_json_path(self.bot_id),
            activations_path=artifact_layout.bot_activations_path(self.bot_id),
        )
        effective_config = apply_instance_overrides(self.base_config, self.strategy_profile, paths)
        effective_config.validate()
        return BotInstanceConfig(
            instance_id=self.bot_id,
            family=self.family,
            generation=generation,
            profile_name=self.profile_name,
            base_config=effective_config,
            strategy_profile=self.strategy_profile,
            network_config=self.network_config,
            storage_paths=paths,
        )


def load_runtime_settings(root_dir: Path, config_path: Path | None = None) -> RuntimeSettings:
    root_dir = root_dir.resolve()
    config_path = (config_path or root_dir / "config" / "global.yaml").resolve()
    load_dotenv(root_dir / ".env", override=False)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    storage_payload = payload.get("storage", {})
    swarm_payload = payload.get("swarm", {})
    scheduler_payload = payload.get("scheduler", {})
    git_payload = payload.get("git", {})
    safety_payload = payload.get("safety", {})
    analysis_payload = payload.get("analysis", {})
    patching_payload = payload.get("patching", {})
    validation_payload = payload.get("validation", {})
    notification_payload = payload.get("notifications", {})

    artifact_root = _resolve_path(root_dir, _first_env("TRADER_ARTIFACT_ROOT", "ARTIFACT_ROOT") or storage_payload.get("artifact_root", "artifacts"))
    db_path = _resolve_path(root_dir, _first_env("TRADER_DB_PATH", "SQLITE_DB_PATH") or storage_payload.get("db_path", "artifacts/trader_swarm.sqlite3"))
    lock_path = _resolve_path(root_dir, _first_env("RUN_LOCK_PATH") or storage_payload.get("lock_path", "artifacts/run.lock"))
    bots_dir = _resolve_path(root_dir, swarm_payload.get("bot_config_dir", "config/bots"))
    dry_run = _get_bool_env("DRY_RUN", _get_bool_env("SWARM_DRY_RUN", bool(swarm_payload.get("dry_run", True))))
    live_trading_enabled = False if dry_run else _get_bool_env("LIVE_TRADING_ENABLED", bool(swarm_payload.get("live_trading_enabled", False)))

    return RuntimeSettings(
        root_dir=root_dir,
        config_path=config_path,
        bots_dir=bots_dir,
        artifact_root=artifact_root,
        db_path=db_path,
        lock_path=lock_path,
        generation=int(os.getenv("SWARM_GENERATION", swarm_payload.get("generation", 1))),
        dry_run=dry_run,
        live_trading_enabled=live_trading_enabled,
        scheduler=SchedulerSettings(
            interval_minutes=int(scheduler_payload.get("interval_minutes", 30)),
            session_minutes=float(scheduler_payload.get("session_minutes", 30)),
            dry_run_session_minutes=float(scheduler_payload.get("dry_run_session_minutes", 1.0)),
            timezone=str(os.getenv("SCHEDULER_TIMEZONE", scheduler_payload.get("timezone", "UTC"))),
        ),
        git=GitSettings(
            remote=str(git_payload.get("remote", "origin")),
            production_branch=str(git_payload.get("production_branch", "main")),
            develop_branch=str(git_payload.get("develop_branch", "develop")),
        ),
        safety=SafetySettings(
            global_kill_switch=_get_bool_env("GLOBAL_KILL_SWITCH", bool(safety_payload.get("global_kill_switch", False))),
            global_kill_switch_path=_resolve_path(root_dir, safety_payload.get("global_kill_switch_path", "state/global_kill_switch.txt")),
            max_notional_per_bot=float(safety_payload.get("max_notional_per_bot", 250.0)),
            max_portfolio_notional=float(safety_payload.get("max_portfolio_notional", 1500.0)),
            max_drawdown_per_cycle=float(safety_payload.get("max_drawdown_per_cycle", 0.05)),
            max_open_positions=int(safety_payload.get("max_open_positions", 10)),
            max_order_rate_per_minute=float(safety_payload.get("max_order_rate_per_minute", 10.0)),
            max_hold_sec_per_profile={str(key): int(value) for key, value in safety_payload.get("max_hold_sec_per_profile", {}).items()},
            skip_run_if_volatility_spike=bool(safety_payload.get("skip_run_if_volatility_spike", False)),
            volatility_spike_threshold_pct=float(safety_payload.get("volatility_spike_threshold_pct", 3.0)),
            skip_run_if_api_down=bool(safety_payload.get("skip_run_if_api_down", True)),
            require_validation_for_promotion=bool(safety_payload.get("require_validation_for_promotion", True)),
        ),
        analysis=AnalysisSettings(
            enabled=_get_bool_env("AI_ANALYSIS_ENABLED", bool(analysis_payload.get("enabled", True))),
            api_key=os.getenv("OPENAI_API_KEY") or None,
            model=str(os.getenv("OPENAI_MODEL_ANALYSIS", analysis_payload.get("model", "gpt-5.4-mini"))),
            patch_model=str(os.getenv("OPENAI_MODEL_PATCH", analysis_payload.get("patch_model", "gpt-5.4-mini"))),
            prompt_path=_resolve_path(root_dir, analysis_payload.get("prompt_path", "config/analysis_prompt.md")),
            schema_path=_resolve_path(root_dir, analysis_payload.get("schema_path", "config/analysis_schema.json")),
            prompt_cache_key=str(analysis_payload.get("prompt_cache_key", "trader-swarm-analysis-v1")),
            prompt_cache_retention=analysis_payload.get("prompt_cache_retention", "in_memory"),
            timeout_seconds=int(analysis_payload.get("timeout_seconds", 45)),
            max_retries=int(analysis_payload.get("max_retries", 3)),
            backoff_seconds=float(analysis_payload.get("backoff_seconds", 2.0)),
            store_responses=bool(analysis_payload.get("store_responses", True)),
            rolling_cycles=int(analysis_payload.get("rolling_cycles", 5)),
            max_signal_events=int(analysis_payload.get("max_signal_events", 20)),
            log_excerpt_lines=int(analysis_payload.get("log_excerpt_lines", 40)),
            use_previous_response=bool(analysis_payload.get("use_previous_response", False)),
            project_id=os.getenv("OPENAI_PROJECT_ID") or None,
        ),
        patching=PatchingSettings(
            enabled=_get_bool_env("PATCHING_ENABLED", bool(patching_payload.get("enabled", False))),
            require_validation_for_promotion=bool(patching_payload.get("require_validation_for_promotion", True)),
            diff_line_limit=int(patching_payload.get("diff_line_limit", 400)),
            experiment_branch_prefix=str(patching_payload.get("experiment_branch_prefix", "exp")),
            base_branch=str(patching_payload.get("base_branch", "main")),
            codex_patch_command=str(os.getenv("CODEX_PATCH_COMMAND", patching_payload.get("codex_patch_command", ""))),
            auto_promote_to_develop=bool(patching_payload.get("auto_promote_to_develop", False)),
            auto_promote_to_main=bool(patching_payload.get("auto_promote_to_main", False)),
        ),
        validation=ValidationSettings(
            lint_command=str(validation_payload.get("lint_command", "")),
            test_command=str(validation_payload.get("test_command", "python -m unittest discover -s tests")),
            backtest_candles=int(validation_payload.get("backtest_candles", 400)),
            regression_max_drawdown_pct=float(validation_payload.get("regression_max_drawdown_pct", 8.0)),
            regression_min_return_pct=float(validation_payload.get("regression_min_return_pct", -5.0)),
            regression_min_win_rate_pct=float(validation_payload.get("regression_min_win_rate_pct", 20.0)),
            secret_patterns=[str(item) for item in validation_payload.get("secret_patterns", [])],
        ),
        notifications=NotificationSettings(
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL") or notification_payload.get("discord_webhook_url") or None,
        ),
    )


def load_bot_definitions(settings: RuntimeSettings) -> list[BotDefinition]:
    base_config = BotConfig.from_env()
    baseline_profile = default_strategy_profile(base_config)
    bot_files = sorted(settings.bots_dir.glob("*.yaml"))
    definitions: list[BotDefinition] = []

    for index, path in enumerate(bot_files, start=1):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        bot_id = str(payload["bot_id"])
        family = str(payload["family"])
        profile_name = str(payload["profile_name"])
        strategy_profile = _replace_dataclass(baseline_profile, payload.get("strategy_profile", {}))
        strategy_profile.validate()

        effective_base = _replace_dataclass(base_config, payload.get("bot_config_overrides", {}))
        if settings.dry_run or not settings.live_trading_enabled:
            effective_base = replace(
                effective_base,
                mode="paper",
                coinbase_api_key=None,
                coinbase_api_secret=None,
            )
        effective_base.validate()

        network_payload = payload.get("network", {})
        mutation_scale = float(network_payload.get("mutation_scale", 0.08 if family == "zerk" else 0.03))
        version_tag = str(network_payload.get("version_tag", "swarm-v1"))
        network_config = NetworkConfig(
            layer_sizes=tuple(network_payload.get("layer_sizes", (24, 32, 24, 16, 8, 2))),
            learning_rate=float(network_payload.get("learning_rate", 0.01)),
            seed=int(network_payload.get("seed", 7 + settings.generation * 100 + index)),
            mutation_scale=mutation_scale,
            version=f"{version_tag}-g{settings.generation:03d}",
        )
        network_config.validate()

        hash_payload = {
            "bot_id": bot_id,
            "profile_name": profile_name,
            "family": family,
            "strategy_profile": asdict(strategy_profile),
            "bot_config_overrides": payload.get("bot_config_overrides", {}),
            "network": asdict(network_config),
            "generation": settings.generation,
        }
        definitions.append(
            BotDefinition(
                bot_id=bot_id,
                family=family,
                profile_name=profile_name,
                config_hash=_fingerprint(hash_payload),
                source_path=path,
                base_config=effective_base,
                strategy_profile=strategy_profile,
                network_config=network_config,
                genome=payload.get("genome", {}),
            )
        )

    actual_ids = {item.bot_id for item in definitions}
    expected_ids = set(SWARM_INSTANCE_IDS)
    missing_ids = sorted(expected_ids - actual_ids)
    extra_ids = sorted(actual_ids - expected_ids)
    if missing_ids or extra_ids:
        raise ValueError(f"Bot profile mismatch. Missing={missing_ids} extra={extra_ids}")
    return definitions
