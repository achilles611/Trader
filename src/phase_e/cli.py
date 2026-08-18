"""Minimal operator CLI for Phase E.2 materializations.

It is deliberately independent of ``src.copytrade`` so no Phase E object can
enter a production decision path.  Invoke through ``main.py materialization``
or ``python -m src.phase_e.cli``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from .materialization import (
    ALL_ELIGIBLE_V1,
    DETERMINISTIC_HASH_V1,
    TIME_STRATIFIED_HASH_V2,
    EligibilitySpec,
    MaterializationSpec,
    OutcomeResolutionSpec,
    PhaseEMaterializer,
    StratificationSpec,
)
from .types import FeatureReference, OutcomeHorizon, PartitionIdentity


def _partition(raw: str) -> PartitionIdentity:
    source = Path(raw)
    value = json.loads(source.read_text(encoding="utf-8") if source.exists() else raw)
    if not isinstance(value, dict):
        raise ValueError("--partition-json must be a JSON object or path to one.")
    horizon = value.get("outcome_horizon", value.get("horizon_seconds"))
    if isinstance(horizon, dict):
        horizon = horizon.get("seconds")
    return PartitionIdentity(
        partition_id=value["partition_id"], train_start=value["train_start"], train_end=value["train_end"],
        validation_start=value["validation_start"], validation_end=value["validation_end"],
        test_start=value["test_start"], test_end=value["test_end"], purge_seconds=value.get("purge_seconds", 0),
        embargo_seconds=value.get("embargo_seconds", 0), random_seed=value["random_seed"], horizon=OutcomeHorizon(horizon),
        feature_lookback_seconds=value.get("feature_lookback_seconds", 0),
        sampling_algorithm=value.get("sampling_algorithm", "NONE_V1"),
        outcome_boundary_policy=value.get("outcome_boundary_policy", "END_EXCLUSIVE_OUTCOME_CONTAINED"),
    )


def _feature(raw: str) -> FeatureReference:
    """Parse ``feature_id@version[:lookback_seconds]`` without ambiguity."""
    try:
        identifier, rest = raw.rsplit("@", 1)
        version_text, *lookback = rest.split(":")
        return FeatureReference(identifier, int(version_text), int(lookback[0]) if lookback else 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("--feature must use feature_id@version[:lookback_seconds].") from exc


def _spec(args: argparse.Namespace, materializer: PhaseEMaterializer) -> MaterializationSpec:
    partition = _partition(args.partition_json)
    features = tuple(sorted((_feature(value) for value in args.feature), key=lambda item: (item.feature_id, item.version)))
    eligibility = EligibilitySpec(source=args.source, kinds=tuple(sorted(args.kind or ["WALLET_FILL"])))
    universe = materializer.bind_source_universe(corpus_fingerprint=args.corpus, eligibility=eligibility)
    algorithm = args.sampling_algorithm
    sample_size = None if algorithm == ALL_ELIGIBLE_V1 else args.sample_size
    stratification = (StratificationSpec("UTC_TIME_BUCKET", args.stratify_bucket_seconds)
                      if algorithm == TIME_STRATIFIED_HASH_V2 else StratificationSpec())
    return MaterializationSpec(
        source_universe=universe, partition=partition, eligibility=eligibility, required_features=features,
        outcome_horizon=OutcomeHorizon(args.horizon_seconds), sampling_algorithm=algorithm,
        sampling_seed=args.seed, target_count=sample_size, tier=args.tier, purpose=args.purpose,
        outcome_resolution=OutcomeResolutionSpec(maximum_lag_seconds=args.outcome_maximum_lag_seconds),
        stratification=stratification,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="beelzebub materialization", description="Phase E.2 reproducible scientific materialization controls.")
    parser.add_argument("--database", required=True, help="Existing hot scientific SQLite database path.")
    commands = parser.add_subparsers(dest="command", required=True)

    def specification_command(name: str, help_text: str) -> argparse.ArgumentParser:
        command = commands.add_parser(name, help=help_text, description=help_text)
        command.add_argument("--corpus", required=True, help="Frozen D.7 corpus fingerprint.")
        command.add_argument("--partition-json", required=True, help="Partition JSON text or a JSON file path.")
        command.add_argument("--source", default="HISTORICAL_OFFICIAL_ARCHIVE", help="Immutable D source name.")
        command.add_argument("--kind", action="append", default=None, help="Eligible anchor kind; repeatable (default: WALLET_FILL).")
        command.add_argument("--feature", action="append", default=[], help="Required D feature: feature_id@version[:lookback_seconds].")
        command.add_argument("--horizon-seconds", type=int, default=5, help="Declared E.1 outcome horizon in seconds.")
        command.add_argument("--sampling-algorithm", choices=(ALL_ELIGIBLE_V1, DETERMINISTIC_HASH_V1, TIME_STRATIFIED_HASH_V2), default=DETERMINISTIC_HASH_V1)
        command.add_argument("--sample-size", type=int, default=10_000, help="Requested count; ignored by ALL_ELIGIBLE_V1.")
        command.add_argument("--seed", type=int, default=0, help="Immutable deterministic sampling seed.")
        command.add_argument("--tier", default="PILOT", help="Predeclared infrastructure tier label.")
        command.add_argument("--purpose", default="operator_requested_materialization", help="Predeclared non-evaluative purpose label.")
        command.add_argument("--stratify-bucket-seconds", type=int, help="Required only for TIME_STRATIFIED_HASH_V2.")
        command.add_argument("--outcome-maximum-lag-seconds", type=int, default=5,
                             help="Maximum allowed delay after the declared horizon for the first same-symbol trade print.")
        return command

    specification_command("plan", "Inspect the full source universe and outcome-blind materialization estimate.")
    specification_command("build", "Register and build a deterministic materialization.")
    for name, help_text in (("status", "Show one materialization's durable status."), ("verify", "Reconcile one completed materialization."),
                            ("reproduce", "Revalidate immutable source and artifact fingerprints.")):
        command = commands.add_parser(name, help=help_text, description=help_text)
        command.add_argument("--materialization", required=True)
    commands.add_parser("list", help="List durable E.2 materializations.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    materializer = PhaseEMaterializer(args.database)
    if args.command in {"plan", "build"}:
        spec = _spec(args, materializer)
        result: dict[str, Any] = materializer.plan(spec) if args.command == "plan" else materializer.build(spec)
    elif args.command == "status":
        result = materializer.get(args.materialization)
    elif args.command == "verify":
        result = materializer.verify(args.materialization)
    elif args.command == "reproduce":
        result = materializer.reproduce(args.materialization)
    else:
        result = {"items": materializer.list(), "trading_authority": False}
    print(json.dumps(result, sort_keys=True, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
