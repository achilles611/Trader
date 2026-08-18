"""Beelzebub's paper-only scientific copy-trade entry point."""

import sys

from src.copytrade.cli import main as copytrade_main


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "materialization":
        from src.phase_e.cli import main as materialization_main
        raise SystemExit(materialization_main(sys.argv[2:]))
    raise SystemExit(copytrade_main())
