"""Beelzebub's paper-only scientific copy-trade entry point."""

import sys

from src.copytrade.cli import main as copytrade_main


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "materialization":
        from src.phase_e.cli import main as materialization_main
        raise SystemExit(materialization_main(sys.argv[2:]))
    if len(sys.argv) > 1 and sys.argv[1] == "hypothesis-family":
        from src.phase_e.generation_cli import family_main
        raise SystemExit(family_main(sys.argv[2:]))
    if len(sys.argv) > 1 and sys.argv[1] == "hypothesis-generation":
        from src.phase_e.generation_cli import generation_main
        raise SystemExit(generation_main(sys.argv[2:]))
    if len(sys.argv) > 1 and sys.argv[1] == "hypothesis-evaluation":
        from src.phase_e.evaluation_cli import main as evaluation_main
        raise SystemExit(evaluation_main(sys.argv[2:]))
    raise SystemExit(copytrade_main())
