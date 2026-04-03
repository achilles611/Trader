from __future__ import annotations

import argparse
import logging
from json import dumps
from pathlib import Path

from .backtest import run_backtest
from .bot import TradingBot
from .config import BotConfig
from .orchestrator import SwarmOrchestrator, evolve_generation, profile_dump, render_instance_visual, train_baseline_network


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Small ETH trading bot starter.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("run", help="Run the bot loop continuously.")
    subparsers.add_parser("once", help="Run a single decision cycle.")

    session_parser = subparsers.add_parser("session", help="Run a timed forward paper session.")
    session_parser.add_argument(
        "--minutes",
        type=float,
        default=60.0,
        help="How long to run the timed session.",
    )
    session_parser.add_argument(
        "--report-file",
        type=str,
        default="",
        help="Optional path to write the JSON session report.",
    )

    backtest_parser = subparsers.add_parser("backtest", help="Run a historical paper backtest.")
    backtest_parser.add_argument(
        "--candles",
        type=int,
        default=1000,
        help="How many candles to pull from Coinbase public data.",
    )

    swarm_parser = subparsers.add_parser("swarm-session", help="Run the 10-bot paper swarm.")
    swarm_parser.add_argument("--minutes", type=float, default=60.0, help="How long to run the swarm session.")
    swarm_parser.add_argument("--generation", type=int, default=1, help="Generation number for reports and profiles.")

    evolve_parser = subparsers.add_parser("evolve", help="Score one generation and emit next-generation proposals.")
    evolve_parser.add_argument("--from-generation", type=int, required=True, help="Generation to read reports from.")
    evolve_parser.add_argument("--to-generation", type=int, required=True, help="Generation number to emit proposals for.")

    train_parser = subparsers.add_parser("train-network", help="Train the baseline neural scorer from trade samples.")
    train_parser.add_argument("--input", type=str, default="", help="Training sample JSONL path.")
    train_parser.add_argument("--epochs", type=int, default=12, help="Training epochs.")

    viz_parser = subparsers.add_parser("viz-network", help="Render one instance network bundle.")
    viz_parser.add_argument("--instance", type=str, required=True, help="Instance id, for example tr4.")
    viz_parser.add_argument("--generation", type=int, default=1, help="Generation number for instance paths.")

    profile_parser = subparsers.add_parser("profile-dump", help="Print the configured swarm profiles for a generation.")
    profile_parser.add_argument("--generation", type=int, default=1, help="Generation number to inspect.")
    return parser


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    config = BotConfig.from_env()
    bot = TradingBot(config)

    if args.command == "run":
        bot.run_forever()
        return 0

    if args.command == "once":
        bot.run_once()
        return 0

    if args.command == "backtest":
        summary, trades = run_backtest(config, candles=args.candles)
        print(
            dumps(
                {
                    "summary": summary.__dict__,
                    "trades": [trade.to_json() for trade in trades[-10:]],
                },
                indent=2,
            )
        )
        return 0

    if args.command == "session":
        report = bot.run_session(minutes=args.minutes)
        if args.report_file:
            report_path = Path(args.report_file)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(dumps(report, indent=2), encoding="utf-8")
        print(dumps(report, indent=2))
        return 0

    if args.command == "swarm-session":
        orchestrator = SwarmOrchestrator(config, generation=args.generation)
        report = orchestrator.run_session(minutes=args.minutes)
        print(dumps(report, indent=2))
        return 0

    if args.command == "evolve":
        report = evolve_generation(
            config,
            from_generation=args.from_generation,
            to_generation=args.to_generation,
        )
        print(dumps(report, indent=2))
        return 0

    if args.command == "train-network":
        input_path = Path(args.input) if args.input else config.training_sample_log_path
        report = train_baseline_network(config, input_path, epochs=args.epochs)
        print(dumps(report, indent=2))
        return 0

    if args.command == "viz-network":
        report = render_instance_visual(config, args.instance, generation=args.generation)
        print(dumps(report, indent=2))
        return 0

    if args.command == "profile-dump":
        report = profile_dump(config, generation=args.generation)
        print(dumps(report, indent=2))
        return 0

    return 1
