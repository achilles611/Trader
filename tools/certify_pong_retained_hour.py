#!/usr/bin/env python3
"""Create the production Pong hour certificate from a stopped collector.

This is an offline durability verifier, not a market-history generator.  It
accepts only an already-eligible authentic collector interval, replays the
entire retained hash chain through the collector's durable tip, and derives
DailyBias from the two actual live hourly bars plus classified native trades.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from src.l3f_provider.ninjatrader_observation import (  # noqa: E402
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    NinjaTraderObservation,
)
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider  # noqa: E402
from src.l3g_paper.daily_bias import ClassifiedSide, ClassifiedTradeInput  # noqa: E402
from src.l3g_paper.pong_experiment import CompletedPongHour  # noqa: E402
from src.l3g_paper.pong_inputs import (  # noqa: E402
    PONG_HOURLY_WARM_START_POLICY_VERSION,
    hourly_warm_start_policy_hash,
)
from src.l3g_paper.pong_contract import (  # noqa: E402
    PongContractTopology,
    topology_for_version,
)
from src.l3g_paper.pong_production import (  # noqa: E402
    PONG_QUALIFYING_HOUR_SCHEMA,
    PongProductionHost,
    _atomic_write,
)
from src.lane_iii.contracts import canonical_hash, normalized_utc  # noqa: E402


ZERO_HASH = "0" * 64


def utc(value: object) -> datetime:
    return datetime.fromisoformat(
        normalized_utc(str(value), "Retained Pong time").replace("Z", "+00:00")
    )


def wire(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def eligible_interval(status: dict[str, object]) -> dict[str, object]:
    values = [status.get("required_interval")]
    recent = status.get("recent_completed_intervals")
    if isinstance(recent, list):
        values.extend(recent)
    candidates = [
        dict(value) for value in values
        if isinstance(value, dict) and value.get("eligible") is True
    ]
    if not candidates:
        raise RuntimeError("PONG_RETAINED_ELIGIBLE_INTERVAL_MISSING")
    return max(candidates, key=lambda value: utc(value["end_utc"]))


def completed_hour(
    observation: dict[str, object], *, continuity_domain: str,
    topology: PongContractTopology,
) -> CompletedPongHour:
    payload = observation["payload"]
    if not isinstance(payload, dict):
        raise RuntimeError("PONG_RETAINED_HOURLY_PAYLOAD_INVALID")
    start = utc(payload.get("start"))
    end = utc(payload.get("end"))
    observed = utc(observation.get("ninja_receipt_time"))
    if (
        end != start + timedelta(hours=1)
        or end > observed
        or observed > end + timedelta(minutes=5)
        or payload.get("contract_id") != topology.native_instrument
        or payload.get("source") != "NINJATRADER_BARS_REQUEST_PROVIDER"
        or payload.get("policy_version") != PONG_HOURLY_WARM_START_POLICY_VERSION
        or payload.get("policy_hash") != hourly_warm_start_policy_hash(topology)
        or payload.get("publication_policy") != "CAUSAL_LIVE_COMPLETED_HOUR_V1"
    ):
        raise RuntimeError("PONG_RETAINED_HOURLY_PROVENANCE_INVALID")
    bar_core = {
        "start": wire(start), "end": wire(end),
        "open": str(payload.get("open")), "high": str(payload.get("high")),
        "low": str(payload.get("low")), "close": str(payload.get("close")),
        "volume": payload.get("volume"),
    }
    return CompletedPongHour(
        start=wire(start), end=wire(end), available_at=wire(observed),
        open=Decimal(str(payload["open"])),
        high=Decimal(str(payload["high"])),
        low=Decimal(str(payload["low"])),
        close=Decimal(str(payload["close"])),
        source="NINJATRADER_BARS_REQUEST_PROVIDER",
        continuity_domain=continuity_domain,
        source_ids=(str(observation["observation_id"]),),
        source_hashes=(canonical_hash(bar_core),),
        complete=True,
        instrument=topology.native_instrument,
    )


def retained_topology(status: dict[str, object]) -> PongContractTopology:
    instrument = status.get("instrument")
    if not isinstance(instrument, dict):
        raise RuntimeError("PONG_RETAINED_TOPOLOGY_MISSING")
    topology = topology_for_version(instrument.get("topology_version"))
    if topology is None or any((
        instrument.get("native") != topology.native_instrument,
        instrument.get("canonical") != topology.canonical_contract,
        instrument.get("topology_hash") != topology.binding_hash,
        instrument.get("state_namespace") != topology.state_namespace,
    )):
        raise RuntimeError("PONG_RETAINED_TOPOLOGY_INVALID")
    return topology


def select_status(collection_root: Path) -> tuple[Path, dict[str, object], dict[str, object]]:
    matches: list[tuple[datetime, Path, dict[str, object], dict[str, object]]] = []
    for path in collection_root.glob("collectors/*/status.json"):
        try:
            status = json.loads(path.read_text(encoding="utf-8"))
            interval = eligible_interval(status)
            matches.append((utc(interval["end_utc"]), path, status, interval))
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, RuntimeError):
            continue
    if not matches:
        raise RuntimeError("PONG_RETAINED_ELIGIBLE_COLLECTOR_MISSING")
    _, path, status, interval = max(matches, key=lambda item: item[0])
    return path, status, interval


def certify(
    collection_root: Path,
    state_root: Path,
    *,
    root_key: bytes | None = None,
    test_only: bool = False,
) -> dict[str, object]:
    status_path, status, interval = select_status(collection_root)
    topology = retained_topology(status)
    if status.get("state") != "STOPPED":
        raise RuntimeError("PONG_RETAINED_COLLECTOR_NOT_STOPPED")
    durability = status.get("durability")
    producer = interval.get("producer")
    streams = interval.get("streams")
    if not isinstance(durability, dict) or not isinstance(producer, dict) or not isinstance(streams, dict):
        raise RuntimeError("PONG_RETAINED_STATUS_SHAPE_INVALID")
    if (
        producer.get("source_fingerprint") != EXPECTED_OBSERVER_SOURCE_FINGERPRINT
        or producer.get("signature_verified") is not True
        or interval.get("continuity_valid") is not True
        or interval.get("blockers") not in ([], ())
        or int(durability.get("queue_overflow_records") or 0) != 0
        or int(durability.get("rejected_records") or 0) != 0
        or int(durability.get("duplicate_records") or 0) != 0
    ):
        raise RuntimeError("PONG_RETAINED_STATUS_NOT_QUALIFYING")
    for name in ("DEPTH", "QUOTE", "TRADE"):
        stream = streams.get(name)
        if (
            not isinstance(stream, dict)
            or int(stream.get("records") or 0) <= 0
            or int(stream.get("source_sequence_gaps") or 0) != 0
            or int(stream.get("duplicate_or_out_of_order") or 0) != 0
        ):
            raise RuntimeError("PONG_RETAINED_STREAM_NOT_QUALIFYING:" + name)

    journal = Path(str(durability["journal_path"])).resolve()
    if not journal.is_file():
        raise RuntimeError("PONG_RETAINED_JOURNAL_MISSING")
    expected_records = int(durability["durable_records"])
    if expected_records != int(durability["written_records"]):
        raise RuntimeError("PONG_RETAINED_JOURNAL_NOT_FULLY_DURABLE")
    start, end = utc(interval["start_utc"]), utc(interval["end_utc"])
    previous_start = start - timedelta(hours=1)
    continuity_domain = (
        "NINJATRADER_HOURLY:" + str(producer["session_id"]) + ":"
        + str(producer["source_fingerprint"]) + ":"
        + str(producer["build_fingerprint"])
    )
    prior_hash = ZERO_HASH
    rows = 0
    interval_counts = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
    hours: dict[str, CompletedPongHour] = {}
    trades: list[ClassifiedTradeInput] = []
    attestation_key = root_key or LocalPaperSecretProvider().load_key()
    with journal.open("r", encoding="utf-8", newline="") as handle:
        for raw in handle:
            if not raw.strip():
                continue
            row = json.loads(raw)
            rows += 1
            if rows > expected_records:
                break
            digest = row.pop("record_hash", None)
            if (
                row.get("journal_sequence") != rows
                or row.get("previous_record_hash") != prior_hash
                or digest != canonical_hash(row)
                or row.get("signature_verified") is not True
            ):
                raise RuntimeError(f"PONG_RETAINED_JOURNAL_CHAIN_INVALID:{rows}")
            prior_hash = str(digest)
            observation = row.get("observation")
            if not isinstance(observation, dict):
                raise RuntimeError(f"PONG_RETAINED_OBSERVATION_INVALID:{rows}")
            verified = NinjaTraderObservation.from_wire(
                json.dumps(observation, sort_keys=True, separators=(",", ":")),
            ).verify_attestation(
                attestation_key,
                expected_source_fingerprint=EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
            )
            if row.get("wire_hash") != verified.wire_hash():
                raise RuntimeError(f"PONG_RETAINED_WIRE_HASH_INVALID:{rows}")
            observation_type = str(observation.get("observation_type"))
            if observation_type == "HOURLY_BAR":
                hour = completed_hour(
                    observation,
                    continuity_domain=continuity_domain,
                    topology=topology,
                )
                hour_start = utc(hour.start)
                if hour_start in {previous_start, start}:
                    hours[hour.start] = hour
                continue
            if observation_type not in interval_counts:
                continue
            payload = observation.get("payload")
            if (
                not isinstance(payload, dict)
                or payload.get("contract_id") != topology.native_instrument
            ):
                raise RuntimeError("PONG_RETAINED_MARKET_TOPOLOGY_INVALID")
            event_at = utc(
                observation.get("provider_timestamp")
                or observation.get("ninja_receipt_time")
            )
            if not start <= event_at < end:
                continue
            interval_counts[observation_type] += 1
            if observation_type != "TRADE":
                continue
            classification = row.get("aggressor_classification")
            if not isinstance(classification, dict) or not isinstance(payload, dict):
                raise RuntimeError("PONG_RETAINED_TRADE_CLASSIFICATION_INVALID")
            side = str(classification.get("side"))
            if side not in {"BUY", "SELL"}:
                continue
            trades.append(ClassifiedTradeInput(
                event_id=str(observation["observation_id"]),
                payload_hash=str(observation["payload_hash"]),
                source="NINJATRADER_NATIVE_AGGRESSOR:"
                + str(classification.get("provenance") or "UNAVAILABLE"),
                instrument=topology.native_instrument,
                side=ClassifiedSide(side),
                size=Decimal(str(payload["size"])),
                event_at=wire(event_at),
                observed_at=normalized_utc(
                    str(observation["ninja_receipt_time"]), "Trade receipt time",
                ),
                available_at=normalized_utc(
                    str(observation["ninja_receipt_time"]), "Trade availability time",
                ),
                authenticated=True,
            ))
    if rows != expected_records or prior_hash != durability.get("journal_tip_sha256"):
        raise RuntimeError("PONG_RETAINED_JOURNAL_TIP_MISMATCH")
    for name, count in interval_counts.items():
        if count != int(streams[name]["records"]):
            raise RuntimeError("PONG_RETAINED_STREAM_COUNT_MISMATCH:" + name)
    expected_starts = (wire(previous_start), wire(start))
    if tuple(sorted(hours)) != expected_starts:
        raise RuntimeError("PONG_RETAINED_TWO_LIVE_HOURS_MISSING")
    pair = (hours[expected_starts[0]], hours[expected_starts[1]])
    published_at = pair[1].available_at
    bias = PongProductionHost._bias_from_inputs(
        pair[0], pair[1], tuple(trades), published_at=published_at,
        instrument=topology.native_instrument,
    )
    if not bias.configured:
        raise RuntimeError("PONG_RETAINED_DAILY_BIAS_UNCONFIGURED")
    body: dict[str, object] = {
        "schema": PONG_QUALIFYING_HOUR_SCHEMA,
        "created_at": wire(datetime.now(timezone.utc)),
        "test_only": test_only,
        "eligible": True,
        "collector_id": status.get("collector", {}).get("collector_id"),
        "producer": dict(producer),
        "required_interval": dict(interval),
        "journal_path": str(journal),
        "journal_tip_sha256": prior_hash,
        "journal_verified_records": rows,
        "collector_status_path": str(status_path.resolve()),
        "collector_status_hash": canonical_hash(status),
        "topology": topology.payload() | {"binding_hash": topology.binding_hash},
        "hour_pair": [item.payload() for item in pair],
        "classified_trades": [
            {
                **item.__dict__, "side": item.side.value, "size": str(item.size),
            }
            for item in trades
        ],
        "published_at": published_at,
        "daily_bias": bias.payload(),
        "daily_bias_result_hash": bias.result_hash,
    }
    body["certificate_hash"] = canonical_hash(body)
    destination = state_root.resolve() / "qualified-hour.json"
    _atomic_write(destination, body)
    return {
        "schema": "beelzebub-pong-retained-hour-certification-result-v1",
        "result": "TEST_ONLY_CERTIFIED" if test_only else "CERTIFIED",
        "certificate_path": str(destination),
        "certificate_hash": body["certificate_hash"],
        "interval": {"start_utc": interval["start_utc"], "end_utc": interval["end_utc"]},
        "producer": producer,
        "journal_verified_records": rows,
        "classified_trades": len(trades),
        "daily_bias_result_hash": bias.result_hash,
        "topology": topology.payload() | {"binding_hash": topology.binding_hash},
        "test_only": test_only,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection-root", required=True, type=Path)
    parser.add_argument("--state-root", required=True, type=Path)
    parser.add_argument("--test-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(
        certify(args.collection_root, args.state_root, test_only=args.test_only),
        sort_keys=True, separators=(",", ":"),
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
