from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .bot import SessionTracker, TradingBot
from .config import BotConfig, BotInstanceConfig
from .dashboard import write_swarm_dashboard
from .evolution import load_generation_reports, propose_next_generation
from .market_data import CoinbasePublicClient, TransientMarketDataError
from .network import NeuralNetwork, load_training_samples
from .profiles import SWARM_INSTANCE_IDS, build_swarm_instance_configs
from .storage import save_json


class SwarmOrchestrator:
    def __init__(
        self,
        base_config: BotConfig,
        *,
        generation: int,
        root_dir: Path | None = None,
    ) -> None:
        self.base_config = base_config
        self.generation = generation
        self.root_dir = root_dir or Path(".")
        self.market_data = CoinbasePublicClient(
            timeout_seconds=base_config.market_data_timeout_seconds,
            max_retries=base_config.market_data_max_retries,
            retry_backoff_seconds=base_config.market_data_retry_backoff_seconds,
        )
        self.instances = build_swarm_instance_configs(base_config, generation=generation, root_dir=self.root_dir)
        self.baseline_network = NeuralNetwork.load_or_create(
            base_config.baseline_network_path,
            self.instances[0].network_config,
        )
        self.bots = self._build_bots()

    def _build_bots(self) -> list[TradingBot]:
        bots: list[TradingBot] = []
        for index, instance in enumerate(self.instances):
            mutation_scale = 0.0 if instance.instance_id == "tr1" else (0.08 if instance.family == "zerk" else 0.03)
            network = NeuralNetwork.load_or_create(
                instance.storage_paths.network_snapshot_path,
                instance.network_config,
                baseline=self.baseline_network,
                mutation_scale=mutation_scale,
                seed_offset=index + 1,
            )
            bots.append(
                TradingBot(
                    instance.base_config,
                    instance_config=instance,
                    market_data=self.market_data,
                    network=network,
                )
            )
        return bots

    @property
    def dashboard_path(self) -> Path:
        return self.root_dir / "viz" / "dashboard" / f"generation_{self.generation:03d}" / "dashboard.html"

    def write_dashboard(self) -> Path:
        return write_swarm_dashboard(
            self.root_dir,
            self.generation,
            [instance.instance_id for instance in self.instances],
        )

    def run_session(self, minutes: float) -> dict[str, Any]:
        if minutes <= 0:
            raise ValueError("Session duration must be greater than 0 minutes.")

        dashboard_path = self.write_dashboard()
        initial_frame = self.market_data.get_market_frame(
            product_id=self.base_config.product_id,
            granularity=self.base_config.granularity,
            limit=self.base_config.lookback_candles,
        )
        trackers: dict[str, SessionTracker] = {
            bot.instance.instance_id: bot.start_session_tracker(minutes=minutes, frame=initial_frame)
            for bot in self.bots
        }
        for bot in self.bots:
            bot.write_visual_snapshot(initial_frame)
        deadline = time.monotonic() + (minutes * 60)
        last_frame = initial_frame
        transient_market_data_errors = 0

        while True:
            try:
                frame = self.market_data.get_market_frame(
                    product_id=self.base_config.product_id,
                    granularity=self.base_config.granularity,
                    limit=self.base_config.lookback_candles,
                )
                last_frame = frame
            except TransientMarketDataError as exc:
                transient_market_data_errors += 1
                for bot in self.bots:
                    trackers[bot.instance.instance_id].transient_market_data_errors += 1
                    bot.logger.warning("%s Shared transient market-data failure: %s", bot.log_prefix, exc)
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    break
                time.sleep(min(self.base_config.loop_seconds, remaining_seconds))
                continue

            for bot in self.bots:
                tracker = trackers[bot.instance.instance_id]
                try:
                    cycle = bot.run_once_with_frame(frame)
                    bot.update_session_tracker(tracker, cycle)
                except Exception as exc:
                    tracker.errors += 1
                    bot._handle_system_error(exc)
                    bot.logger.exception("%s Swarm cycle failed.", bot.log_prefix)

            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                break
            time.sleep(min(self.base_config.loop_seconds, remaining_seconds))

        reports: dict[str, dict[str, Any]] = {}
        for bot in self.bots:
            report = bot.build_session_report(trackers[bot.instance.instance_id], ending_frame=last_frame)
            report["shared_transient_market_data_errors"] = transient_market_data_errors
            reports[bot.instance.instance_id] = report
            save_json(bot.instance.storage_paths.report_path, report)
            bot.write_visual_snapshot(last_frame)

        summary = {
            "generation": self.generation,
            "started_at": min(tracker.started_at for tracker in trackers.values()).isoformat(),
            "ended_at": datetime.utcnow().isoformat(),
            "minutes": minutes,
            "dashboard_path": str(dashboard_path),
            "instance_reports": reports,
            "next_generation_proposals": propose_next_generation(self.instances, reports, to_generation=self.generation + 1),
        }
        save_json(self.generation_root / "swarm_session_report.json", summary)
        save_json(
            self.generation_root / "next_generation_proposals.json",
            summary["next_generation_proposals"],
        )
        self.write_dashboard()
        return summary

    @property
    def generation_root(self) -> Path:
        return self.root_dir / "reports" / f"generation_{self.generation:03d}"


def train_baseline_network(config: BotConfig, input_path: Path, *, epochs: int = 12) -> dict[str, Any]:
    samples = load_training_samples(input_path)
    network = NeuralNetwork.load_or_create(config.baseline_network_path, build_swarm_instance_configs(config, generation=1)[0].network_config)
    summary = network.train(samples, epochs=epochs)
    network.save(config.baseline_network_path)
    return {
        "input_path": str(input_path),
        "output_path": str(config.baseline_network_path),
        **summary,
    }


def profile_dump(config: BotConfig, generation: int, *, root_dir: Path | None = None) -> dict[str, Any]:
    instances = build_swarm_instance_configs(config, generation=generation, root_dir=root_dir or Path("."))
    return {
        "generation": generation,
        "instances": [
            {
                "instance_id": instance.instance_id,
                "family": instance.family,
                "profile_name": instance.profile_name,
                "strategy_profile": instance.strategy_profile.__dict__,
                "config": {
                    "granularity": instance.base_config.granularity,
                    "loop_seconds": instance.base_config.loop_seconds,
                    "fast_ema_period": instance.base_config.fast_ema_period,
                    "slow_ema_period": instance.base_config.slow_ema_period,
                    "pullback_min_pct": instance.base_config.pullback_min_pct,
                    "rsi_entry_floor": instance.base_config.rsi_entry_floor,
                    "rsi_entry_ceiling": instance.base_config.rsi_entry_ceiling,
                },
            }
            for instance in instances
        ],
    }


def evolve_generation(config: BotConfig, from_generation: int, to_generation: int, *, root_dir: Path | None = None) -> dict[str, Any]:
    root = root_dir or Path(".")
    instances = build_swarm_instance_configs(config, generation=from_generation, root_dir=root)
    report_paths = [instance.storage_paths.report_path for instance in instances]
    reports = load_generation_reports(report_paths)
    proposals = propose_next_generation(instances, reports, to_generation=to_generation)
    save_json(root / "reports" / f"generation_{to_generation:03d}" / "next_generation_proposals.json", proposals)
    return proposals


def render_instance_visual(config: BotConfig, instance_id: str, generation: int, *, root_dir: Path | None = None) -> dict[str, Any]:
    root = root_dir or Path(".")
    instances = {instance.instance_id: instance for instance in build_swarm_instance_configs(config, generation=generation, root_dir=root)}
    if instance_id not in instances:
        raise ValueError(f"Unknown instance_id={instance_id}. Expected one of: {', '.join(SWARM_INSTANCE_IDS)}")
    instance = instances[instance_id]
    network = NeuralNetwork.load_or_create(instance.storage_paths.network_snapshot_path, instance.network_config)
    bot = TradingBot(
        instance.base_config,
        instance_config=instance,
        market_data=CoinbasePublicClient(
            timeout_seconds=config.market_data_timeout_seconds,
            max_retries=config.market_data_max_retries,
            retry_backoff_seconds=config.market_data_retry_backoff_seconds,
        ),
        network=network,
    )
    bot.write_visual_snapshot()
    return {
        "instance_id": instance_id,
        "network_snapshot_path": str(instance.storage_paths.network_snapshot_path),
        "network_viz_path": str(instance.storage_paths.network_viz_path),
        "network_json_path": str(instance.storage_paths.network_json_path),
        "activations_path": str(instance.storage_paths.activations_path),
    }
