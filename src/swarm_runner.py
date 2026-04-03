from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .eth_bot.bot import SessionTracker, TradingBot
from .eth_bot.dashboard import write_swarm_dashboard
from .eth_bot.market_data import CoinbasePublicClient, TransientMarketDataError
from .eth_bot.network import NeuralNetwork
from .eth_bot.storage import save_json


@dataclass(frozen=True)
class SwarmRunResult:
    generation: int
    started_at: str
    ended_at: str
    minutes: float
    dashboard_path: str
    instance_reports: dict[str, dict[str, Any]]
    bot_artifact_paths: dict[str, dict[str, str]]


class SwarmRunner:
    def __init__(self, *, settings, bot_definitions, artifact_layout) -> None:
        self.settings = settings
        self.bot_definitions = list(bot_definitions)
        self.artifact_layout = artifact_layout
        self.logger = logging.getLogger("trader_swarm.swarm_runner")
        self.base_config = self.bot_definitions[0].base_config
        self.market_data = CoinbasePublicClient(
            timeout_seconds=self.base_config.market_data_timeout_seconds,
            max_retries=self.base_config.market_data_max_retries,
            retry_backoff_seconds=self.base_config.market_data_retry_backoff_seconds,
        )
        self.instance_configs = {
            definition.bot_id: definition.build_instance_config(settings.root_dir, artifact_layout, settings.generation)
            for definition in self.bot_definitions
        }
        self.baseline_network = NeuralNetwork.load_or_create(
            self.base_config.baseline_network_path,
            self.bot_definitions[0].network_config,
        )
        self.bots = self._build_bots()

    def _build_bots(self) -> list[TradingBot]:
        bots: list[TradingBot] = []
        for index, definition in enumerate(self.bot_definitions, start=1):
            instance_config = self.instance_configs[definition.bot_id]
            network = NeuralNetwork.load_or_create(
                instance_config.storage_paths.network_snapshot_path,
                definition.network_config,
                baseline=self.baseline_network,
                mutation_scale=definition.network_config.mutation_scale,
                seed_offset=index,
            )
            bots.append(
                TradingBot(
                    instance_config.base_config,
                    instance_config=instance_config,
                    market_data=self.market_data,
                    network=network,
                )
            )
        return bots

    def write_dashboard(self) -> Path:
        return write_swarm_dashboard(
            self.settings.root_dir,
            self.settings.generation,
            [definition.bot_id for definition in self.bot_definitions],
        )

    def run_session(self, minutes: float) -> SwarmRunResult:
        if minutes <= 0:
            raise ValueError("Session duration must be greater than 0 minutes.")

        started_at = datetime.now(timezone.utc)
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
                self.logger.warning("Shared transient market-data failure: %s", exc)
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
                    bot.logger.exception("%s Swarm cycle failed: %s", bot.log_prefix, exc)

            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                break
            time.sleep(min(self.base_config.loop_seconds, remaining_seconds))

        reports: dict[str, dict[str, Any]] = {}
        bot_artifact_paths: dict[str, dict[str, str]] = {}
        for bot in self.bots:
            report = bot.build_session_report(trackers[bot.instance.instance_id], ending_frame=last_frame)
            report["shared_transient_market_data_errors"] = transient_market_data_errors
            reports[bot.instance.instance_id] = report
            save_json(bot.instance.storage_paths.report_path, report)
            bot.write_visual_snapshot(last_frame)
            bot_artifact_paths[bot.instance.instance_id] = {
                "report_path": str(bot.instance.storage_paths.report_path),
                "trade_log_path": str(bot.instance.storage_paths.trade_log_path),
                "signal_log_path": str(bot.instance.storage_paths.signal_log_path),
                "network_snapshot_path": str(bot.instance.storage_paths.network_snapshot_path),
            }

        ended_at = datetime.now(timezone.utc)
        return SwarmRunResult(
            generation=self.settings.generation,
            started_at=started_at.isoformat(),
            ended_at=ended_at.isoformat(),
            minutes=minutes,
            dashboard_path=str(dashboard_path),
            instance_reports=reports,
            bot_artifact_paths=bot_artifact_paths,
        )
