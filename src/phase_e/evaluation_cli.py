"""Conservative operator CLI for Phase E.4 scientific evaluation."""

from __future__ import annotations

import argparse
import json
from typing import Any, Sequence

from .evaluation import EvaluationSettings, PhaseEEvaluator


def _print(value: dict[str, Any] | list[dict[str, Any]]) -> int:
    print(json.dumps(value, sort_keys=True, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="beelzebub hypothesis-evaluation",
        description="Phase E.4 preregistered, validation-only scientific evaluation controls.",
    )
    parser.add_argument("--database", required=True, help="Existing hot scientific SQLite database path.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("eligible", help="List COMPLETE E.3 universes and protocol-registration state.")
    preregister = commands.add_parser("preregister", help="Seal one E.4 protocol before outcome access.")
    preregister.add_argument("--generation-run", required=True)
    preregister.add_argument("--minimum-independent-components", type=int, default=8)
    preregister.add_argument("--minimum-components-per-arm", type=int, default=2)
    preregister.add_argument("--minimum-valid-resample-fraction", type=float, default=0.90)
    preregister.add_argument("--minimum-practical-effect", type=float, default=0.001)
    preregister.add_argument("--maximum-absolute-outcome", type=float, default=10.0)
    preregister.add_argument("--maximum-sampling-weight", type=float, default=1_000_000_000.0)
    commands.add_parser("protocols", help="List sealed E.4 protocols.")
    protocol = commands.add_parser("protocol", help="Inspect one sealed E.4 protocol.")
    protocol.add_argument("--protocol", required=True)
    evaluate = commands.add_parser("evaluate", help="Evaluate one sealed family atomically on validation evidence.")
    evaluate.add_argument("--protocol", required=True)
    results = commands.add_parser("results", help="Inspect results or pending/inconclusive reason codes.")
    results.add_argument("--protocol", required=True)
    run = commands.add_parser("run", help="Inspect one immutable E.4 evaluation run and manifest.")
    run.add_argument("--evaluation-run", required=True)
    verify = commands.add_parser("verify", help="Recompute and verify one E.4 run deterministically.")
    verify.add_argument("--evaluation-run", required=True)
    reproduce = commands.add_parser("reproduce", help="Read-only deterministic replay of one E.4 run.")
    reproduce.add_argument("--evaluation-run", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    evaluator = PhaseEEvaluator(args.database)
    if args.command == "eligible":
        return _print({"items": evaluator.eligible_families(), "test_partition_queries": 0, "trading_authority": False})
    if args.command == "preregister":
        settings = EvaluationSettings(
            minimum_independent_components=args.minimum_independent_components,
            minimum_components_per_arm=args.minimum_components_per_arm,
            minimum_valid_resample_fraction=args.minimum_valid_resample_fraction,
            minimum_practical_effect=args.minimum_practical_effect,
            maximum_absolute_outcome=args.maximum_absolute_outcome,
            maximum_sampling_weight=args.maximum_sampling_weight,
        )
        return _print(evaluator.preregister(args.generation_run, settings=settings))
    if args.command == "protocols":
        return _print({"items": evaluator.list_protocols(), "trading_authority": False})
    if args.command == "protocol":
        return _print(evaluator.get_protocol(args.protocol))
    if args.command == "evaluate":
        return _print(evaluator.evaluate(args.protocol))
    if args.command == "results":
        return _print({"items": evaluator.results(args.protocol), "test_partition_queries": 0, "trading_authority": False})
    if args.command == "run":
        return _print(evaluator.get_run(args.evaluation_run))
    if args.command == "verify":
        return _print(evaluator.verify(args.evaluation_run))
    return _print(evaluator.reproduce(args.evaluation_run))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
