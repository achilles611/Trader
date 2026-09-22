#!/usr/bin/env python3
"""Build paired legacy/LSC corpora from an authenticated Pong collector journal.

This offline tool verifies the collector hash chain and every native HMAC before
encoding.  Its output proves observation-level parity only; it does not add or
infer decision, hypothesis, evidence-family, qualification, or continuity
claims that are absent from the source journal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Mapping

from src.l3f_provider.ninjatrader_observation import (
    EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
    NinjaTraderObservation,
)
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
from src.lsc.codec import LSCContext, encode_context, encode_observation


GENESIS = "0" * 64


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _write_new(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        handle.write("\n")


def _legacy_row(observation: NinjaTraderObservation, context: LSCContext) -> dict[str, object]:
    producer = observation.producer
    account_alias = observation.account_alias
    account_class = observation.account_class
    return {
        "schema": observation.schema,
        "observation_id": observation.observation_id,
        "session_id": observation.session_id,
        "observation_type": observation.observation_type,
        "ninja_receipt_time": observation.ninja_receipt_time,
        "local_monotonic_sequence": observation.local_monotonic_sequence,
        "provider_timestamp": observation.provider_timestamp,
        "provider_sequence": observation.provider_sequence,
        "exchange_timestamp": observation.exchange_timestamp,
        "account": None if account_alias is None else {
            "alias": account_alias,
            "class": account_class.value if account_class is not None else None,
        },
        "payload": dict(observation.payload),
        "payload_hash": observation.payload_hash,
        "producer": None if producer is None else producer.payload(),
        "signature": observation.signature,
        "signature_verified": observation.signature_verified,
        "wire_hash": observation.wire_hash(),
        "source_sequence": observation.payload["source_event_sequence"],
        "source_generation": observation.payload["source_generation"],
        "source": observation.source,
        "provider": observation.provider,
        "environment": observation.environment,
        "producer_protocol_version": context.producer_protocol_version,
        "producer_source_fingerprint": context.producer_source_fingerprint,
        "producer_build_fingerprint": context.producer_build_fingerprint,
        "producer_build_timestamp": context.producer_build_timestamp,
        "native_contract_id": context.native_contract_id,
        "canonical_contract_id": context.canonical_contract_id,
        "generation": context.generation,
    }


def build(
    source: Path,
    legacy_output: Path,
    lsc_output: Path,
    manifest_output: Path,
    *,
    canonical_contract_id: str,
    tick_size: str,
    maximum_records: int | None,
) -> dict[str, object]:
    key = LocalPaperSecretProvider().load_key()
    prior = GENESIS
    collector_id: str | None = None
    context: LSCContext | None = None
    context_bytes: bytes | None = None
    legacy_rows: list[dict[str, object]] = []
    lsc_rows: list[dict[str, object]] = []
    source_hasher = hashlib.sha256()

    with source.open("rb") as handle:
        for line_number, raw in enumerate(handle, 1):
            if maximum_records is not None and len(legacy_rows) >= maximum_records:
                break
            source_hasher.update(raw)
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError(f"collector line {line_number} is not an object")
            supplied_hash = value.get("record_hash")
            body = {key_name: item for key_name, item in value.items() if key_name != "record_hash"}
            expected_hash = hashlib.sha256(_canonical_json(body)).hexdigest()
            if supplied_hash != expected_hash:
                raise ValueError(f"collector line {line_number} hash mismatch")
            if value.get("previous_record_hash") != prior:
                raise ValueError(f"collector line {line_number} chain mismatch")
            if value.get("journal_sequence") != line_number:
                raise ValueError(f"collector line {line_number} sequence mismatch")
            if value.get("signature_verified") is not True:
                raise ValueError(f"collector line {line_number} lacks verified signature claim")
            current_collector = value.get("collector_id")
            if not isinstance(current_collector, str) or not current_collector:
                raise ValueError(f"collector line {line_number} collector identity invalid")
            if collector_id is None:
                collector_id = current_collector
            elif collector_id != current_collector:
                raise ValueError(f"collector line {line_number} collector identity changed")

            raw_observation = value.get("observation")
            if not isinstance(raw_observation, dict):
                raise ValueError(f"collector line {line_number} observation invalid")
            wire = _canonical_json(raw_observation).decode("utf-8")
            observation = NinjaTraderObservation.from_wire(wire).verify_attestation(
                key,
                expected_source_fingerprint=EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
            )
            if observation.wire_hash() != value.get("wire_hash"):
                raise ValueError(f"collector line {line_number} wire hash mismatch")
            producer = observation.producer
            if producer is None:
                raise ValueError(f"collector line {line_number} producer identity missing")
            generation = observation.payload.get("source_generation")
            native_contract = observation.payload.get("contract_id")
            if type(generation) is not int or not isinstance(native_contract, str):
                raise ValueError(f"collector line {line_number} source identity invalid")
            if context is None:
                context = LSCContext(
                    observation_schema=observation.schema,
                    source=observation.source,
                    provider=observation.provider,
                    environment=observation.environment,
                    producer_protocol_version=producer.protocol_version,
                    producer_source_fingerprint=producer.source_fingerprint,
                    producer_build_fingerprint=producer.build_fingerprint,
                    producer_build_timestamp=producer.build_timestamp,
                    native_contract_id=native_contract,
                    canonical_contract_id=canonical_contract_id,
                    tick_size=tick_size,
                    session_id=observation.session_id,
                    generation=generation,
                    timestamp_origin=observation.ninja_receipt_time,
                )
                context_bytes = encode_context(context)
            assert context_bytes is not None
            encoded = encode_observation(observation, context_bytes)
            legacy_rows.append(_legacy_row(observation, context))
            lsc_rows.append({
                "context_hex": context_bytes.hex(),
                "record_hex": encoded.record_bytes.hex(),
            })
            prior = expected_hash

    if not legacy_rows or context is None or context_bytes is None or collector_id is None:
        raise ValueError("collector journal produced no verified observations")

    _write_new(legacy_output, {"events": legacy_rows})
    _write_new(lsc_output, {"events": lsc_rows})
    manifest = {
        "schema": "beelzebub-lsc-parity-corpus-manifest-v1",
        "scope": "OBSERVATION_LEVEL_ONLY",
        "qualification_authority": False,
        "decision_evidence_parity_included": False,
        "source_journal": str(source.resolve()),
        "source_prefix_sha256": source_hasher.hexdigest(),
        "source_prefix_records": len(legacy_rows),
        "collector_id": collector_id,
        "collector_tip_for_prefix": prior,
        "expected_source_fingerprint": EXPECTED_OBSERVER_SOURCE_FINGERPRINT,
        "context_digest": context_bytes[-32:].hex(),
        "legacy_output": str(legacy_output.resolve()),
        "legacy_sha256": hashlib.sha256(legacy_output.read_bytes()).hexdigest(),
        "lsc_output": str(lsc_output.resolve()),
        "lsc_sha256": hashlib.sha256(lsc_output.read_bytes()).hexdigest(),
    }
    _write_new(manifest_output, manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--legacy-output", type=Path, required=True)
    parser.add_argument("--lsc-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument("--canonical-contract", default="MNQZ6")
    parser.add_argument("--tick-size", default="0.25")
    parser.add_argument("--maximum-records", type=int)
    args = parser.parse_args()
    try:
        result = build(
            args.source,
            args.legacy_output,
            args.lsc_output,
            args.manifest_output,
            canonical_contract_id=args.canonical_contract,
            tick_size=args.tick_size,
            maximum_records=args.maximum_records,
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({
            "result": "ERROR",
            "error": type(error).__name__,
            "detail": str(error),
        }, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
