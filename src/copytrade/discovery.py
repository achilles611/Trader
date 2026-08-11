from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol
from urllib.parse import urlparse

from .models import DiscoveryObservation, DiscoveryRun, DiscoverySummary, as_utc, new_run_id, utc_now
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
    """Fixture/local-node transport; values are normalized without network access."""

    def __init__(self, trades: Iterable[Mapping[str, Any]], name: str = "iterable") -> None:
        self.trades = tuple(dict(trade) for trade in trades)
        self.name = name

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]:
        return iter(self.trades)


class LocalNodeTradeFileTransport:
    """Reads downloaded node-trade JSON or JSON-lines files without guessing a remote API."""

    def __init__(self, paths: Iterable[str | Path]) -> None:
        self.paths = tuple(Path(path) for path in paths)
        if not self.paths:
            raise DiscoveryProviderError("hypercore-file discovery requires at least one --input node-trade file.")

    def iter_trades(self, *, refresh: bool = False) -> Iterable[Mapping[str, Any]]:
        for path in self.paths:
            if not path.exists():
                raise DiscoveryProviderError(f"HyperCore node-trade input does not exist: {path}")
            yield from _decode_node_trade_bytes(path.read_bytes(), str(path))


class RequesterPaysS3NodeTradeTransport:
    """Downloads explicit historical-node objects with AWS requester-pays enabled."""

    def __init__(self, object_uris: Iterable[str]) -> None:
        self.object_uris = tuple(object_uris)
        if not self.object_uris:
            raise DiscoveryProviderError(
                "hypercore-s3 discovery requires one or more exact s3://bucket/key --input objects; prefix discovery is intentionally not guessed."
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
                payload = response["Body"].read()
            except Exception as exc:
                raise DiscoveryProviderError(
                    f"Could not fetch {uri} with RequestPayer=requester. Hyperliquid historical-node S3 transfer is requester-pays; "
                    "configure AWS billing/credentials or download the object locally."
                ) from exc
            yield from _decode_node_trade_bytes(payload, uri)


class HyperCoreNodeTradeDiscoveryProvider:
    """Official HyperCore node-trade discovery from the documented `side_info[].user` schema."""

    source_name = "hyperliquid_hypercore_node_trades"

    def __init__(self, transport: HyperCoreNodeTradeTransport) -> None:
        self.transport = transport

    def discover(self, *, refresh: bool = False) -> Iterable[DiscoveryObservation]:
        observed_at = utc_now()
        for trade in self.transport.iter_trades(refresh=refresh):
            if not isinstance(trade, Mapping):
                continue
            side_info = trade.get("side_info")
            if not isinstance(side_info, list) or len(side_info) != 2 or trade.get("time") in (None, ""):
                continue
            try:
                activity_at = as_utc(trade.get("time"))
                price = float(trade.get("px") or 0)
                size = abs(float(trade.get("sz") or 0))
            except (TypeError, ValueError):
                continue
            for index, side in enumerate(side_info):
                if not isinstance(side, Mapping):
                    continue
                wallet = str(side.get("user") or "").lower()
                role = "buyer" if index == 0 else "seller"
                evidence_id = str(trade.get("hash") or trade.get("tid") or f"{trade.get('time')}:{trade.get('coin')}:{index}")
                yield DiscoveryObservation(
                    wallet=wallet,
                    source=self.source_name,
                    observed_at=observed_at,
                    recent_activity_at=activity_at,
                    source_score=price * size if price > 0 and size > 0 else None,
                    metadata={"coin": str(trade.get("coin") or ""), "role": role, "transport": type(self.transport).__name__},
                    raw_evidence=dict(trade),
                    evidence_id=evidence_id,
                )


class DiscoveryPipeline:
    """Cheap, idempotent registration of normalized discovery evidence; never scores or executes."""

    def __init__(self, database: CopyTradeDatabase) -> None:
        self.database = database

    def run(
        self, provider: CandidateDiscoveryAdapter, *, limit: int, min_activity: int, refresh: bool = False,
        configuration: dict[str, Any] | None = None,
    ) -> DiscoverySummary:
        if limit <= 0:
            raise ValueError("Discovery limit must be positive.")
        if min_activity <= 0:
            raise ValueError("--min-activity must be positive.")
        run = DiscoveryRun(
            run_id=new_run_id("discover"), started_at=utc_now(), sources=(provider.source_name,),
            configuration={"limit": limit, "min_activity": min_activity, "refresh": refresh, **(configuration or {})},
        )
        self.database.start_discovery_run(run)
        try:
            observations = tuple(provider.discover(refresh=refresh))
            summary = self.database.persist_discovery_observations(run, observations, limit=limit, min_activity=min_activity)
        except Exception as exc:
            self.database.finish_discovery_run(run.run_id, status="failed", errors=(str(exc),))
            if isinstance(exc, DiscoveryProviderError):
                raise
            raise DiscoveryProviderError(f"Discovery provider {provider.source_name} failed: {exc}") from exc
        return summary


def build_discovery_provider(source: str, inputs: Iterable[str]) -> CandidateDiscoveryAdapter:
    normalized = source.strip().lower()
    if normalized == "hypercore-file":
        return HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport(inputs))
    if normalized == "hypercore-s3":
        return HyperCoreNodeTradeDiscoveryProvider(RequesterPaysS3NodeTradeTransport(inputs))
    raise ValueError(f"Unsupported discovery source: {source}. Supported sources: hypercore-file, hypercore-s3.")


def _decode_node_trade_bytes(payload: bytes, label: str) -> Iterator[Mapping[str, Any]]:
    if label.lower().endswith(".lz4"):
        try:
            import lz4.frame
        except ImportError as exc:
            raise DiscoveryProviderError(f"Reading LZ4 node-trade data ({label}) requires optional lz4.") from exc
        payload = lz4.frame.decompress(payload)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DiscoveryProviderError(f"Node-trade input {label} is not UTF-8 JSON/JSONL data.") from exc
    stripped = text.lstrip()
    if stripped.startswith("["):
        try:
            records = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DiscoveryProviderError(f"Malformed JSON array in node-trade input {label}: {exc}") from exc
        if not isinstance(records, list):
            raise DiscoveryProviderError(f"Node-trade input {label} must contain a JSON array or JSON Lines records.")
        for record in records:
            if isinstance(record, Mapping):
                yield record
        return
    for number, line in enumerate(io.StringIO(text), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DiscoveryProviderError(f"Malformed JSONL in node-trade input {label} line {number}: {exc}") from exc
        if isinstance(record, Mapping):
            yield record


def _s3_location(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise DiscoveryProviderError(f"Expected an exact s3://bucket/key node-trade object, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")
