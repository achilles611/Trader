"""Minimal read-only/operator CLI for Phase E.3 hypothesis generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from .generation import HypothesisFamilySpec, PhaseEHypothesisGenerator, wallet_action_sign_family


def _family(raw: str) -> HypothesisFamilySpec:
    source = Path(raw)
    value = json.loads(source.read_text(encoding="utf-8") if source.exists() else raw)
    if not isinstance(value, dict):
        raise ValueError("--family-json must be a hypothesis-family JSON object or path.")
    return HypothesisFamilySpec.from_payload(value)


def _result(value: MappingLike) -> int:
    print(json.dumps(value, sort_keys=True, indent=2, default=str))
    return 0


MappingLike = dict[str, Any] | list[dict[str, Any]]


def family_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="beelzebub hypothesis-family", description="Phase E.3 immutable hypothesis-family controls.")
    parser.add_argument("--database", required=True, help="Existing hot scientific SQLite database path.")
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("register", help="Register one immutable predeclared family.")
    register.add_argument("--family-json", required=True)
    commands.add_parser("list", help="List registered immutable families.")
    control = commands.add_parser("wallet-action-sign-control", help="Register the small E.3 wallet-action sign control family.")
    control.add_argument("--minimum-training-support", type=int, default=20)
    control.add_argument("--maximum-candidates", type=int, default=2)
    args = parser.parse_args(argv)
    generator = PhaseEHypothesisGenerator(args.database)
    if args.command == "list":
        return _result({"items": generator.list_families(), "trading_authority": False})
    family = (_family(args.family_json) if args.command == "register" else wallet_action_sign_family(
        minimum_training_support=args.minimum_training_support, maximum_candidates=args.maximum_candidates,
    ))
    return _result(generator.register_family(family))


def generation_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="beelzebub hypothesis-generation", description="Phase E.3 outcome-blind hypothesis generation controls.")
    parser.add_argument("--database", required=True, help="Existing hot scientific SQLite database path.")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (("plan", "Preview a sealed predictor-only generation run."),
                            ("run", "Generate and freeze a hypothesis universe, then map it to E.1.")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--materialization", required=True)
        command.add_argument("--family-json", required=True)
    for name, help_text in (("status", "Show one durable E.3 generation run."),
                            ("verify", "Reconcile one complete E.3 run and its E.2 trust boundary.")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--generation-run", required=True)
    commands.add_parser("list", help="List E.3 generation runs.")
    args = parser.parse_args(argv)
    generator = PhaseEHypothesisGenerator(args.database)
    if args.command == "plan":
        return _result(generator.plan(args.materialization, _family(args.family_json)))
    if args.command == "run":
        return _result(generator.run(args.materialization, _family(args.family_json)))
    if args.command == "status":
        return _result(generator.get(args.generation_run))
    if args.command == "verify":
        return _result(generator.verify(args.generation_run))
    return _result({"items": generator.list(), "trading_authority": False})
