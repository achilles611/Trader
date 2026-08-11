from __future__ import annotations

import io
import json
import re
from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol, TextIO
from urllib.parse import urlparse

from .models import DiscoveryObservation, DiscoveryRun, DiscoverySummary, as_utc, new_run_id, stable_id, utc_now
from .storage import CopyTradeDatabase


class DiscoveryProviderError(RuntimeError):
    """Raised when a source cannot be acquired safely or interpreted reliably."""


class CandidateDiscoveryAdapter(Protocol):
    """Transport-independent extension point for structured discovery sources."""

    @property
    def source_name(self) -> str: ...
    def discover(self, *, refresh: bool = False) -> Iterable[DiscoveryObservation]: ...


class HyperCoreNodeTradeTransport(Protocol):
    """Acquisition seam for downloaded S3 data, a local node, fixtures, or an indexer."""

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]: ...


class IterableNodeTradeTransport:
    """Fixture/local-node transport that does not materialize a supplied stream."""

    def __init__(self, trades: Iterable[Mapping[str, Any]], name: str = "iterable") -> None:
        self.trades = trades
        self.name = name

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]:
        yield from self.trades


class LocalNodeTradeFileTransport:
    """Streams downloaded HyperCore JSON/JSONL/LZ4 records without a remote API."""

    def __init__(self, paths: Iterable[str | Path]) -> None:
        self.paths = tuple(Path(path) for path in paths)
        if not self.paths:
            raise DiscoveryProviderError("hypercore-file discovery requires at least one --input node-trade or node-fill file.")

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]:
        for path in self.paths:
            if not path.exists():
                raise DiscoveryProviderError(f"HyperCore discovery input does not exist: {path}")
            if path.suffix.lower() == ".lz4":
                try:
                    import lz4.frame
                except ImportError as exc:
                    raise DiscoveryProviderError(f"Reading LZ4 HyperCore data ({path}) requires optional lz4.") from exc
                with lz4.frame.open(path, mode="rt", encoding="utf-8") as stream:
                    yield from _iter_json_records(stream, str(path))
            else:
                with path.open("r", encoding="utf-8") as stream:
                    yield from _iter_json_records(stream, str(path))


class RequesterPaysS3NodeTradeTransport:
    """Streams exact historical-node S3 objects with AWS requester-pays enabled."""

    def __init__(self, object_uris: Iterable[str]) -> None:
        self.object_uris = tuple(object_uris)
        if not self.object_uris:
            raise DiscoveryProviderError(
                "hypercore-s3 discovery requires exact s3://bucket/key --input objects; prefix discovery is intentionally not guessed."
            )

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]:
        try:
            import boto3
        except ImportError as exc:
            raise DiscoveryProviderError(
                "Direct Hyperliquid S3 discovery requires optional boto3 plus configured AWS requester-pays credentials; "
                "download the node data first and use --source hypercore-file instead."
            ) from exc
        client = boto3.client("s3")
        for uri in self.object_uris:
            bucket, key = _s3_location(uri)
            try:
                response = client.get_object(Bucket=bucket, Key=key, RequestPayer="requester")
                body = response["Body"]
            except Exception as exc:
                raise DiscoveryProviderError(
                    f"Could not fetch {uri} with RequestPayer=requester. Hyperliquid historical-node S3 transfer is requester-pays; "
                    "configure AWS billing/credentials or download the object locally."
                ) from exc
            try:
                if key.lower().endswith(".lz4"):
                    try:
                        import lz4.frame
                    except ImportError as exc:
                        raise DiscoveryProviderError(f"Reading LZ4 HyperCore data ({uri}) requires optional lz4.") from exc
                    with lz4.frame.open(body, mode="rt", encoding="utf-8") as stream:
                        yield from _iter_json_records(stream, uri)
                else:
                    with io.TextIOWrapper(body, encoding="utf-8") as stream:
                        yield from _iter_json_records(stream, uri)
            finally:
                body.close()


class HyperCoreNodeTradeDiscoveryProvider:
    """Normalizes documented node_trades, node_fills, and batched node_fills_by_block."""

    # Retain the Phase A source key so stored candidate queries and downstream
    # consumers remain compatible even though the provider now handles all
    # documented HyperCore node-data layouts.
    source_name = "hyperliquid_hypercore_node_trades"

    def __init__(self, transport: HyperCoreNodeTradeTransport) -> None:
        self.transport = transport

    def discover(self, *, refresh: bool = False) -> Iterable[DiscoveryObservation]:
        observed_at = utc_now()
        saw_record = False
        for record in self.transport.iter_trades(refresh=refresh):
            saw_record = True
            yield from _normalize_hypercore_record(record, observed_at, type(self.transport).__name__)
        if not saw_record:
            raise DiscoveryProviderError("HyperCore discovery input contained no JSON records.")


class DiscoveryPipeline:
    """Cheap, idempotent registration of normalized discovery evidence; never scores or executes."""

    def __init__(self, database: CopyTradeDatabase, *, batch_size: int = 500) -> None:
        self.database = database
        self.batch_size = batch_size

    def run(
        self, provider: CandidateDiscoveryAdapter, *, limit: int, min_activity: int, refresh: bool = False,
        max_activity_age: timedelta | None = timedelta(days=30), configuration: dict[str, Any] | None = None,
    ) -> DiscoverySummary:
        if limit <= 0:
            raise ValueError("Discovery limit must be positive.")
        if min_activity <= 0:
            raise ValueError("--min-activity must be positive.")
        if self.batch_size <= 0:
            raise ValueError("Discovery batch size must be positive.")
        age_seconds = max_activity_age.total_seconds() if max_activity_age is not None else None
        run = DiscoveryRun(
            run_id=new_run_id("discover"), started_at=utc_now(), sources=(provider.source_name,),
            configuration={
                "limit": limit, "min_activity": min_activity, "refresh": refresh,
                "max_activity_age_seconds": age_seconds, **(configuration or {}),
            },
        )
        self.database.start_discovery_run(run)
        try:
            invalid_wallets = self.database.stage_discovery_observations(
                run.run_id, provider.discover(refresh=refresh), batch_size=self.batch_size,
            )
            return self.database.complete_discovery_run(
                run, limit=limit, min_activity=min_activity, max_activity_age_seconds=age_seconds,
                invalid_wallets=invalid_wallets,
            )
        except Exception as exc:
            # Candidate state is only changed during completion. Remove staged
            # partial evidence so a malformed/provider-failed run stays auditable
            # as failed without looking like a completed source observation.
            self.database.discard_discovery_observations(run.run_id)
            self.database.finish_discovery_run(run.run_id, status="failed", errors=(str(exc),))
            if isinstance(exc, DiscoveryProviderError):
                raise
            raise DiscoveryProviderError(f"Discovery provider {provider.source_name} failed: {exc}") from exc


def build_discovery_provider(source: str, inputs: Iterable[str]) -> CandidateDiscoveryAdapter:
    normalized = source.strip().lower()
    if normalized == "hypercore-file":
        return HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport(inputs))
    if normalized == "hypercore-s3":
        return HyperCoreNodeTradeDiscoveryProvider(RequesterPaysS3NodeTradeTransport(inputs))
    raise ValueError(f"Unsupported discovery source: {source}. Supported sources: hypercore-file, hypercore-s3.")


def parse_activity_age(value: str | None) -> timedelta | None:
    """Parse `30d`, `24h`, or `none` for the cheap candidate-recency gate."""
    normalized = (value or "").strip().lower()
    if normalized in {"none", "off", "disabled"}:
        return None
    match = re.fullmatch(r"(\d+)\s*([smhdw])", normalized)
    if not match or int(match.group(1)) <= 0:
        raise ValueError("--max-activity-age must be a positive value such as 24h, 7d, or 'none'.")
    quantity, unit = int(match.group(1)), match.group(2)
    seconds = {"s": 1, "m": 60, "h": 3600, "d": 86_400, "w": 604_800}[unit]
    return timedelta(seconds=quantity * seconds)


def _normalize_hypercore_record(
    record: Mapping[str, Any], observed_at: datetime, transport_name: str,
) -> Iterator[DiscoveryObservation]:
    if not isinstance(record, Mapping):
        raise DiscoveryProviderError("HyperCore input record must be a JSON object.")
    if "side_info" in record:
        yield from _normalize_node_trade(record, observed_at, transport_name)
        return
    if "events" in record:
        events = record.get("events")
        if not isinstance(events, list) or not any(key in record for key in ("block_number", "block_time", "local_time")):
            raise DiscoveryProviderError("Unsupported HyperCore block record: expected node_fills_by_block {block_number, events:[fill...] }.")
        block_meta = {key: record.get(key) for key in ("local_time", "block_time", "block_number") if record.get(key) is not None}
        for event in events:
            if not isinstance(event, Mapping):
                raise DiscoveryProviderError("Unsupported node_fills_by_block event: expected a fill object.")
            yield from _normalize_node_fill(event, observed_at, transport_name, "node_fills_by_block", block_meta)
        return
    if _looks_like_fill(record):
        yield from _normalize_node_fill(record, observed_at, transport_name, "node_fills", {})
        return
    raise DiscoveryProviderError(
        "Unsupported HyperCore discovery schema. Supported formats are node_trades (side_info), "
        "node_fills (user plus API fill fields), and node_fills_by_block (block metadata plus events)."
    )


def _normalize_node_trade(record: Mapping[str, Any], observed_at: datetime, transport_name: str) -> Iterator[DiscoveryObservation]:
    sides = record.get("side_info")
    if not isinstance(sides, list) or len(sides) != 2 or record.get("time") in (None, ""):
        raise DiscoveryProviderError("Unsupported node_trades record: expected two side_info users plus time, coin, px, and sz.")
    try:
        activity_at = as_utc(record["time"])
        price, size = float(record.get("px") or 0), abs(float(record.get("sz") or 0))
    except (TypeError, ValueError) as exc:
        raise DiscoveryProviderError("Malformed node_trades price, size, or time.") from exc
    if not str(record.get("coin") or record.get("symbol") or ""):
        raise DiscoveryProviderError("Malformed node_trades record: missing coin/symbol.")
    for index, side in enumerate(sides):
        if not isinstance(side, Mapping):
            raise DiscoveryProviderError("Malformed node_trades side_info entry.")
        wallet = str(side.get("user") or "").lower()
        evidence_id = _hypercore_event_id(record, wallet)
        yield DiscoveryObservation(
            wallet=wallet, source=HyperCoreNodeTradeDiscoveryProvider.source_name, observed_at=as_utc(observed_at),
            recent_activity_at=activity_at, source_score=price * size if price > 0 and size > 0 else None,
            metadata={"format": "node_trades", "coin": str(record.get("coin") or record.get("symbol") or ""),
                      "role": "buyer" if index == 0 else "seller", "transport": transport_name},
            raw_evidence=dict(record), evidence_id=evidence_id,
        )


def _normalize_node_fill(
    record: Mapping[str, Any], observed_at: datetime, transport_name: str, format_name: str,
    block_meta: Mapping[str, Any], user_override: str | None = None,
) -> Iterator[DiscoveryObservation]:
    # Some node writers wrap API-format fills as {user, fill} or {user, fills}.
    if isinstance(record.get("fills"), list):
        outer_user = str(record.get("user") or user_override or "")
        for item in record["fills"]:
            if not isinstance(item, Mapping):
                raise DiscoveryProviderError(f"Malformed {format_name} fills wrapper.")
            yield from _normalize_node_fill(item, observed_at, transport_name, format_name, block_meta, outer_user)
        return
    fill = record.get("fill") if isinstance(record.get("fill"), Mapping) else record
    if not isinstance(fill, Mapping):
        raise DiscoveryProviderError(f"Malformed {format_name} fill record.")
    wallet = str(fill.get("user") or record.get("user") or user_override or "").lower()
    timestamp = fill.get("time") or fill.get("timestamp") or block_meta.get("block_time") or block_meta.get("local_time")
    symbol = str(fill.get("coin") or fill.get("symbol") or "")
    if not wallet or timestamp in (None, "") or not symbol or (fill.get("px") is None and fill.get("price") is None) or (fill.get("sz") is None and fill.get("size") is None):
        raise DiscoveryProviderError(
            f"Unsupported {format_name} fill: expected user, time, coin/symbol, px/price, and sz/size fields."
        )
    try:
        activity_at = as_utc(timestamp)
        price, size = float(fill.get("px") or fill.get("price")), abs(float(fill.get("sz") or fill.get("size")))
    except (TypeError, ValueError) as exc:
        raise DiscoveryProviderError(f"Malformed {format_name} price, size, or time.") from exc
    evidence_id = _hypercore_event_id(fill, wallet, block_meta)
    yield DiscoveryObservation(
        wallet=wallet, source=HyperCoreNodeTradeDiscoveryProvider.source_name, observed_at=as_utc(observed_at),
        recent_activity_at=activity_at, source_score=price * size if price > 0 and size > 0 else None,
        metadata={"format": format_name, "coin": symbol, "transport": transport_name, **dict(block_meta)},
        raw_evidence=dict(fill), evidence_id=evidence_id,
    )


def _looks_like_fill(record: Mapping[str, Any]) -> bool:
    return "user" in record and any(key in record for key in ("coin", "symbol", "px", "price", "sz", "size"))


def _hypercore_event_id(payload: Mapping[str, Any], wallet: str, block_meta: Mapping[str, Any] | None = None) -> str:
    """Prefer fill/trade IDs; preserve extra fields for multi-fill transactions."""
    primary = next((payload.get(key) for key in ("tid", "tradeId", "fillId", "id") if payload.get(key) not in (None, "")), None)
    if primary is not None:
        return stable_id("hypercore_discovery_event", wallet, "id", str(primary))
    transaction_hash = str(payload.get("hash") or payload.get("transactionHash") or "")
    parts: list[Any] = [wallet, "hash", transaction_hash,
        str(payload.get("oid") or payload.get("orderId") or ""), str(payload.get("coin") or payload.get("symbol") or ""),
        str(payload.get("time") or payload.get("timestamp") or (block_meta or {}).get("block_time") or ""),
        str(payload.get("px") or payload.get("price") or ""), str(payload.get("sz") or payload.get("size") or ""),
        str((block_meta or {}).get("block_number") or ""),
    ]
    return stable_id("hypercore_discovery_event", *parts)


def _iter_json_records(stream: TextIO, label: str) -> Iterator[Mapping[str, Any]]:
    first = stream.readline()
    while first and not first.strip():
        first = stream.readline()
    if not first:
        return
    if first.lstrip().startswith("["):
        yield from _iter_json_array(stream, first, label)
        return
    yield from _iter_json_lines(stream, first, label)


def _iter_json_lines(stream: TextIO, first_line: str, label: str) -> Iterator[Mapping[str, Any]]:
    for number, line in enumerate(chain((first_line,), stream), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DiscoveryProviderError(f"Malformed JSONL in HyperCore input {label} line {number}: {exc}") from exc
        if not isinstance(record, Mapping):
            raise DiscoveryProviderError(f"HyperCore input {label} line {number} must be a JSON object.")
        yield record


def _iter_json_array(stream: TextIO, initial: str, label: str) -> Iterator[Mapping[str, Any]]:
    decoder = json.JSONDecoder()
    buffer, index, ended = initial, initial.index("[") + 1, False
    while not ended:
        while True:
            while index < len(buffer) and buffer[index] in " \t\r\n,":
                index += 1
            if index < len(buffer):
                break
            chunk = stream.read(64 * 1024)
            if not chunk:
                raise DiscoveryProviderError(f"Unterminated JSON array in HyperCore input {label}.")
            buffer, index = buffer[index:] + chunk, 0
        if buffer[index] == "]":
            return
        try:
            record, end = decoder.raw_decode(buffer, index)
        except json.JSONDecodeError:
            chunk = stream.read(64 * 1024)
            if not chunk:
                raise DiscoveryProviderError(f"Malformed JSON array in HyperCore input {label}.")
            buffer += chunk
            continue
        if not isinstance(record, Mapping):
            raise DiscoveryProviderError(f"HyperCore JSON array input {label} must contain JSON objects.")
        yield record
        buffer, index = buffer[end:], 0


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise DiscoveryProviderError(f"Expected an exact s3://bucket/key HyperCore object, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")
