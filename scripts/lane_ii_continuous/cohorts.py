"""Canonical wallet lineage and explicit research eligibility; no venue actions."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

from .hypotheses import canonical, document_hash

REQUIRED_EVIDENCE = (
    "attributable_activity", "causal_availability", "reproducible_ids", "deduplication_verified",
    "contract_mapping_verified", "destination_executable_prices", "realistic_delay",
    "fees_verified", "size_depth_verified", "venue_eligibility_verified",
)


def freeze_wallet_registry(roster_path, expected_sha256):
    raw = Path(roster_path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("CANONICAL_ROSTER_HASH_MISMATCH")
    source = json.loads(raw)
    rows = source.get("wallets", source.get("members", []))
    identities = [row["wallet"] for row in rows]
    if (len(rows) != 10 or len(set(identities)) != 10
            or any(not re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet) for wallet in identities)
            or Counter(row["cohort"] for row in rows) != {"PRIMARY": 8, "PROBATION": 2}
            or not {"bossgirl", "bonereaper"}.issubset({row["name"].casefold() for row in rows})):
        raise ValueError("CANONICAL_TEN_WALLET_LINEAGE_REQUIRED")
    members = []
    for row in rows:
        members.append({"wallet": row["wallet"], "name": row["name"], "cohort": row["cohort"],
            "historical_admission": row.get("admission"), "source_record_sha256": document_hash(row),
            "historical_active": row.get("active"), "observation_state": "SOURCE_UNOBSERVED",
            "copying_eligible": False, "eligibility_reason": "CURRENT_COPY_EVIDENCE_NOT_SUPPLIED"})
    result = {"schema": "LANE_II_WALLET_QUALITY_REGISTRY_V1", "source_sha256": expected_sha256,
              "members": members, "lineage": {"PRIMARY": 8, "PROBATION": 2},
              "survivorship_filter_applied": False, "source_scope": source.get("scope"),
              "funded_authority": False}
    result["registry_sha256"] = document_hash(result)
    return result


def evaluate_wallet_eligibility(evidence):
    missing = [key for key in REQUIRED_EVIDENCE if evidence.get(key) is not True]
    if evidence.get("cross_venue") is True and evidence.get("exact_equivalence_verified") is not True:
        missing.append("exact_equivalence_verified")
    state = evidence.get("observation_state", "SOURCE_UNOBSERVED")
    if state not in {"ACTIVE", "INACTIVE", "SOURCE_UNAVAILABLE", "SOURCE_UNOBSERVED"}:
        raise ValueError("UNKNOWN_WALLET_OBSERVATION_STATE")
    if state != "ACTIVE":
        missing.append("current_active_source")
    evaluated_at, valid_until = None, None
    try:
        observed = datetime.fromisoformat(evidence["observed_at_utc"].replace("Z", "+00:00"))
        evaluated = datetime.fromisoformat(evidence["evaluated_at_utc"].replace("Z", "+00:00"))
        maximum_age = evidence["maximum_source_age_seconds"]
        if (observed.tzinfo is None or evaluated.tzinfo is None or type(maximum_age) not in {int, float}
                or not 0 < maximum_age <= 300 or not 0 <= (evaluated - observed).total_seconds() <= maximum_age):
            missing.append("current_observation_clock")
        else:
            evaluated_at = evaluated.astimezone(timezone.utc).isoformat()
            valid_until = (observed + timedelta(seconds=maximum_age)).astimezone(timezone.utc).isoformat()
    except (AttributeError, KeyError, TypeError, ValueError):
        missing.append("current_observation_clock")
    if not isinstance(evidence.get("source_manifest_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", evidence["source_manifest_sha256"]):
        missing.append("bound_source_manifest")
    return {"schema": "LANE_II_WALLET_ELIGIBILITY_V1", "observation_state": state,
            "paper_copying_eligible": not missing, "missing_requirements": missing,
            "evaluated_at_utc": evaluated_at, "valid_until_utc": valid_until,
            "eligibility_requires_current_clock_recheck": True,
            "research_observation_permitted": True, "funded_authority": False,
            "leaderboard_profit_is_fill_evidence": False}


def update_wallet_observation(registry, wallet, evidence):
    """Keep inactive and unsuccessful members; only update observed quality facts."""
    result = deepcopy(registry)
    matches = [row for row in result["members"] if row["wallet"] == wallet]
    if len(matches) != 1:
        raise ValueError("WALLET_OUTSIDE_FROZEN_ROSTER")
    assessment = evaluate_wallet_eligibility(evidence)
    matches[0].update(observation_state=assessment["observation_state"],
                      copying_eligible=assessment["paper_copying_eligible"],
                      eligibility=assessment, observation_evidence_sha256=document_hash(evidence))
    result.pop("registry_sha256", None)
    result["registry_sha256"] = document_hash(result)
    return result
