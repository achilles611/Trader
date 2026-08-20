"""Append-only raw/normalized capture and deterministic L3-B replay.

Storage locations are supplied by callers.  This module does not select a
machine path, connect to a provider, or talk to an execution system.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from .contracts import canonical_hash
from .market_data import (
    L3B_SCHEMA,
    AggressorProvenance,
    AggressorSide,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    CanonicalMarketEvent,
    DepthOperation,
    DerivativesContextEvent,
    EventHeader,
    EventTimestamps,
    MarketDataPipeline,
    MarketDataRefused,
    MarketDataSource,
    MarketStream,
    MNQContract,
    OptionRight,
    PipelineResult,
    RawProviderEvent,
    QuoteEvent,
    TradeEvent,
)


def _header_from_payload(value: object) -> EventHeader:
    if not isinstance(value, Mapping):
        raise ValueError("Canonical record header must be a mapping.")
    source = value.get("source")
    instrument = value.get("instrument")
    timestamps = value.get("timestamps")
    if not isinstance(source, Mapping) or not isinstance(instrument, Mapping) or not isinstance(timestamps, Mapping):
        raise ValueError("Canonical record header has malformed provenance.")
    return EventHeader(
        event_id=value["event_id"],
        source=MarketDataSource(source["provider"], source["feed"]),
        instrument=MNQContract(
            instrument["contract_symbol"], instrument["contract_expiry"], instrument.get("exchange", "CME"),
        ),
        timestamps=EventTimestamps(
            timestamps["local_receipt_time"], timestamps.get("exchange_time"), timestamps.get("provider_time"),
        ),
        stream=MarketStream(value["stream"]),
        raw_event_id=value["raw_event_id"],
        raw_payload_hash=value["raw_payload_hash"],
        provider_sequence=value.get("provider_sequence"),
        provider_event_id=value.get("provider_event_id"),
    )


def event_from_payload(value: object) -> CanonicalMarketEvent:
    """Rehydrate a capture through the same strict constructors used live."""
    if not isinstance(value, Mapping):
        raise ValueError("Canonical event record must be a mapping.")
    kind = value.get("kind")
    header = _header_from_payload(value.get("header"))
    if kind == "TRADE":
        return TradeEvent(
            header, Decimal(str(value["price"])), value["size"], AggressorSide(value["aggressor_side"]),
            AggressorProvenance(value["aggressor_provenance"]), value.get("derivation_quote_event_id"),
        )
    if kind == "QUOTE":
        return QuoteEvent(header, Decimal(str(value["bid_price"])), Decimal(str(value["ask_price"])), value["bid_quantity"], value["ask_quantity"])
    if kind == "BOOK_SNAPSHOT":
        bids = tuple(BookLevel(Decimal(str(level["price"])), level["quantity"]) for level in value["bids"])
        asks = tuple(BookLevel(Decimal(str(level["price"])), level["quantity"]) for level in value["asks"])
        return BookSnapshotEvent(header, bids, asks)
    if kind == "BOOK_DELTA":
        return BookDeltaEvent(
            header, BookSide(value["side"]), DepthOperation(value["operation"]), Decimal(str(value["price"])), value.get("quantity"),
        )
    if kind == "DERIVATIVES_CONTEXT":
        return DerivativesContextEvent(
            header, value["underlying"], value["expiry"], Decimal(str(value["strike"])), OptionRight(value["right"]),
            value.get("open_interest"), value.get("volume"), value["data_vintage_time"],
        )
    raise ValueError("Canonical event record has an unknown kind.")


@dataclass(frozen=True)
class CaptureStats:
    raw_events: int
    normalized_events: int
    rejected_records: int


class AppendOnlyMarketCapture:
    """Two JSONL streams retaining raw provider fidelity and normalized provenance links."""

    def __init__(self, directory: Path | str) -> None:
        self.directory = Path(directory)
        self.raw_path = self.directory / "raw-events.jsonl"
        self.normalized_path = self.directory / "normalized-events.jsonl"
        self.rejected_path = self.directory / "rejected-events.jsonl"
        self.directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _append(path: Path, record: Mapping[str, object]) -> None:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded + "\n")
            handle.flush()

    def record_raw(self, event: RawProviderEvent) -> str:
        record = {"schema": L3B_SCHEMA, "record_type": "RAW_PROVIDER_EVENT", **event.payload_record()}
        record["record_hash"] = canonical_hash(record)
        self._append(self.raw_path, record)
        return record["record_hash"]  # type: ignore[return-value]

    def record_normalized(self, event: CanonicalMarketEvent) -> str:
        record = {"schema": L3B_SCHEMA, "record_type": "CANONICAL_MARKET_EVENT", "event": event.payload()}
        record["record_hash"] = canonical_hash(record)
        self._append(self.normalized_path, record)
        return record["record_hash"]  # type: ignore[return-value]

    def record_rejection(self, raw_event_id: str, reason: str) -> None:
        record = {"schema": L3B_SCHEMA, "record_type": "REJECTED_PROVIDER_EVENT", "raw_event_id": raw_event_id, "reason": reason}
        record["record_hash"] = canonical_hash(record)
        self._append(self.rejected_path, record)

    @staticmethod
    def _records(path: Path) -> Iterator[Mapping[str, object]]:
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise MarketDataRefused(f"Capture record at {path}:{line_number} is not valid JSON.") from exc
                if not isinstance(record, Mapping):
                    raise MarketDataRefused(f"Capture record at {path}:{line_number} is not an object.")
                expected = record.get("record_hash")
                without_hash = {key: value for key, value in record.items() if key != "record_hash"}
                if expected != canonical_hash(without_hash):
                    raise MarketDataRefused(f"Capture record at {path}:{line_number} failed integrity verification.")
                yield record

    def normalized_events(self) -> Iterator[CanonicalMarketEvent]:
        for record in self._records(self.normalized_path):
            if record.get("schema") != L3B_SCHEMA or record.get("record_type") != "CANONICAL_MARKET_EVENT":
                raise MarketDataRefused("Normalized capture contains an incompatible record.")
            yield event_from_payload(record.get("event"))

    def raw_events(self) -> Iterator[RawProviderEvent]:
        for record in self._records(self.raw_path):
            if record.get("schema") != L3B_SCHEMA or record.get("record_type") != "RAW_PROVIDER_EVENT":
                raise MarketDataRefused("Raw capture contains an incompatible record.")
            source = record.get("source")
            if not isinstance(source, Mapping):
                raise MarketDataRefused("Raw capture contains malformed source identity.")
            raw = RawProviderEvent(
                record["raw_event_id"], MarketDataSource(source["provider"], source["feed"]), record["received_at"],
                record["payload"], record.get("provider_event_id"),
            )
            if record.get("payload_hash") != raw.payload_hash:
                raise MarketDataRefused("Raw capture payload hash does not match its preserved payload.")
            yield raw

    def stats(self) -> CaptureStats:
        return CaptureStats(
            raw_events=sum(1 for _ in self._records(self.raw_path)),
            normalized_events=sum(1 for _ in self._records(self.normalized_path)),
            rejected_records=sum(1 for _ in self._records(self.rejected_path)),
        )


@dataclass(frozen=True)
class ReplayReport:
    events_processed: int
    results: tuple[PipelineResult, ...]
    final_book_hash: str
    final_book_quality: str


class DeterministicReplay:
    """Feeds recorded canonical events into the identical synchronous pipeline path."""

    def __init__(self, pipeline: MarketDataPipeline) -> None:
        self._pipeline = pipeline

    def replay(self, events: Iterable[CanonicalMarketEvent]) -> ReplayReport:
        results = tuple(self._pipeline.apply(event) for event in events)
        state = self._pipeline.book._state()
        return ReplayReport(len(results), results, state.state_hash, state.quality.value)

    def replay_capture(self, capture: AppendOnlyMarketCapture) -> ReplayReport:
        return self.replay(capture.normalized_events())
