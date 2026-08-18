"""Outcome-isolated Monte Carlo validation for the frozen E.5 method."""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase_e.prospective import (
    DesignObservation,
    E5_OBSERVATION_SCHEMA,
    OutcomeRecord,
    load_frozen_protocol,
    scheduled_blocks,
    synthetic_wild_cluster_bootstrap_t,
)
from src.phase_e.types import canonical_hash


PROTOCOL = ROOT / "docs" / "commissioning" / "phase-e5-prospective-experiment" / "e5-protocol-v1.json"
FIXTURE_NAMESPACE = "SYNTHETIC_E5_ONLY_NEVER_PRODUCTION"


def observations(protocol: dict[str, object]) -> list[DesignObservation]:
    output: list[DesignObservation] = []
    protocol_hash = str(protocol["identity"]["protocol_hash"])  # type: ignore[index]
    salt = protocol["sampling"]["wallet_cohort_salt"]  # type: ignore[index]
    cohort_count = int(protocol["sampling"]["wallet_cohort_count"])  # type: ignore[index]
    for block in scheduled_blocks(protocol):
        start = datetime.fromisoformat(block.sample_start.replace("Z", "+00:00"))
        for offset in range(10):
            ordinal = block.ordinal * 10 + offset
            nonce = 0
            while True:
                wallet = f"synthetic-wallet-{ordinal}-{nonce}"
                digest = canonical_hash({
                    "algorithm": "SALTED_WALLET_COHORT_SHA256_V1", "salt": salt, "wallet_id": wallet,
                })
                if int(digest[:16], 16) % cohort_count == block.cohort:
                    break
                nonce += 1
            anchor = start + timedelta(seconds=offset * 12)
            output.append(DesignObservation(
                observation_id=f"synthetic-{ordinal:05}", source_schema=E5_OBSERVATION_SCHEMA,
                protocol_hash=protocol_hash, block_id=block.block_id,
                anchor_at=anchor.isoformat().replace("+00:00", "Z"),
                exposure_end_at=(anchor + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                wallet_id=wallet, symbol=f"SYN-{ordinal % 12:02}", source_event_id=f"synthetic-event-{ordinal}",
                sampling_weight=1.0, predicate=offset % 2 == 0, liquidity_stratum=f"liq-{ordinal % 4}",
                graph_density_stratum=f"density-{ordinal % 4}", time_stratum=f"utc-{anchor.hour:02}",
                eligibility_snapshot_hash=f"synthetic-eligibility-{ordinal}", symbol_liquidity_eligible=True,
                transaction_id=f"synthetic-tx-{ordinal}", endpoint_family_id=f"synthetic-endpoint-{ordinal}",
                campaign_id=f"synthetic-campaign-{ordinal}",
            ))
    return output


def draw_outcomes(
    design: list[DesignObservation], *, seed: int, effect: float,
    heavy_tail: bool = False, heterogeneous: bool = False,
) -> list[OutcomeRecord]:
    rng = random.Random(seed)
    block_scale: dict[str, float] = {}
    output: list[OutcomeRecord] = []
    for item in design:
        if item.block_id not in block_scale:
            block_scale[item.block_id] = 0.5 + 1.5 * rng.random() if heterogeneous else 1.0
        if heavy_tail:
            chi_square = sum(rng.gauss(0.0, 1.0) ** 2 for _ in range(3))
            noise = rng.gauss(0.0, 1.0) / math.sqrt(chi_square / 3.0)
        else:
            noise = rng.gauss(0.0, 1.0)
        signed_effect = effect / 2.0 if item.predicate else -effect / 2.0
        output.append(OutcomeRecord(item.observation_id, signed_effect + block_scale[item.block_id] * noise))
    return output


def run_scenario(
    protocol: dict[str, object], design: list[DesignObservation], *, name: str,
    trials: int, replications: int, effect: float, heavy_tail: bool = False,
    heterogeneous: bool = False,
) -> dict[str, object]:
    rejections = 0
    coverage = 0
    for trial in range(trials):
        result = synthetic_wild_cluster_bootstrap_t(
            protocol,
            design,
            draw_outcomes(
                design, seed=int(canonical_hash({"scenario": name, "trial": trial})[:16], 16),
                effect=effect, heavy_tail=heavy_tail, heterogeneous=heterogeneous,
            ),
            hypothesis_id=f"{name}-{trial}", fixture_namespace=FIXTURE_NAMESPACE,
            replications=replications,
        )
        rejections += result.raw_p_value <= 0.05
        coverage += result.confidence_interval[0] <= effect <= result.confidence_interval[1]
    rate = rejections / trials
    return {
        "scenario": name, "trials": trials, "replications": replications, "true_effect": effect,
        "rejections": rejections, "rejection_rate": rate,
        "confidence_interval_coverage": coverage / trials,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=500)
    parser.add_argument("--replications", type=int, default=999)
    args = parser.parse_args()
    if args.trials < 100 or args.replications < 199:
        parser.error("commissioning validation requires at least 100 trials and 199 replications")
    protocol = load_frozen_protocol(PROTOCOL)
    design = observations(protocol)
    block_order = {block_id: ordinal for ordinal, block_id in enumerate(sorted({item.block_id for item in design}))}
    unequal = [replace(item, sampling_weight=float(1 + (block_order[item.block_id] % 5))) for item in design]
    scenarios = [
        run_scenario(protocol, design, name="independent-null", trials=args.trials,
                     replications=args.replications, effect=0.0),
        run_scenario(protocol, unequal, name="heterogeneous-heavy-tail-null", trials=args.trials,
                     replications=args.replications, effect=0.0, heavy_tail=True, heterogeneous=True),
        run_scenario(protocol, design, name="independent-true-effect", trials=args.trials,
                     replications=args.replications, effect=0.5),
    ]
    allowance = 0.05 + 2.0 * math.sqrt(0.05 * 0.95 / args.trials)
    accepted = all(item["rejection_rate"] <= allowance for item in scenarios if item["true_effect"] == 0.0)
    payload = {
        "schema": "phase-e5-synthetic-validation-v1",
        "protocol_id": protocol["identity"]["protocol_id"],
        "protocol_hash": protocol["identity"]["protocol_hash"],
        "fixture_namespace": FIXTURE_NAMESPACE,
        "null_acceptance_limit": allowance,
        "scenarios": scenarios,
        "accepted": accepted,
    }
    payload["validation_hash"] = canonical_hash(payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
