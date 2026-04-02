from __future__ import annotations

import argparse
import logging
from json import dumps
from pathlib import Path

from .backtest import run_backtest
from .bot import TradingBot
from .config import BotConfig


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

    return 1
