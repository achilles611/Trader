"""Official HyperCore node-data acquisition for Phase C orchestration.

This module deliberately stops at obtaining verified local objects.  Frozen
Phase A continues to parse and normalize them through ``hypercore-file``.
The acquisition plan is based on the UTC hour encoded in the official node
output path, never on S3 ``LastModified`` storage metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .models import as_utc, iso, utc_now


OFFICIAL_BUCKET = "hl-mainnet-node-data"
OFFICIAL_PREFIX = "node_fills_by_block/"
OFFICIAL_HOURLY_PREFIX = f"{OFFICIAL_PREFIX}hourly/"
OFFICIAL_DOCUMENTATION_URL = "https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data"
CACHE_MAX_BYTES = 5 * 1024 * 1024 * 1024
MAX_HOURLY_LOOKBACK = 48
PUBLICATION_LAG_HOURS = 1
DISK_RESERVE_BYTES = 50 * 1024 * 1024

# The official node documentation establishes ``hourly/YYYYMMDD/HOUR``.  The
# final suffix is intentionally not guessed: a narrow S3 listing supplies the
# exact official object key and this parser rejects all other paths.
_HOURLY_KEY = re.compile(
    r"^node_fills_by_block/hourly/(?P<date>\d{8})/"
    r"(?P<hour>(?:[0-9]|0[0-9]|1[0-9]|2[0-3]))"
    r"(?P<suffix>(?:\.[A-Za-z0-9][A-Za-z0-9._-]*)?)$"
)


class HyperCoreSourceError(RuntimeError):
    """A safe, operator-facing acquisition/access failure."""


@dataclass(frozen=True)
class HyperCoreObject:
    bucket: str
    key: str
    size: int
    last_modified: str | None
    etag: str | None = None
    data_hour_start: str | None = None
    data_hour_end: str | None = None
    date: str | None = None
    hour: int | None = None

    @property
    def identifier(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(frozen=True)
class HistoricalHourPlan:
    """One explicit UTC slot in a bounded historical acquisition request."""

    start: str
    end: str


def historical_hour_slots(start: str | datetime, end: str | datetime, *, maximum_hours: int) -> tuple[HistoricalHourPlan, ...]:
    """Return deterministic, end-exclusive UTC-hour slots for D.7.

    This intentionally has no relationship to the short recent-discovery
    lookback.  A caller must name both endpoints and the requested range is
    bounded before any source listing or download occurs.
    """
    if maximum_hours <= 0:
        raise ValueError("Historical acquisition maximum_hours must be positive.")
    def utc_boundary(value: str | datetime) -> datetime:
        if isinstance(value, str) and not value.strip().endswith("Z"):
            raise ValueError("Historical acquisition boundaries must use an explicit Z UTC offset.")
        parsed = as_utc(value)
        if isinstance(value, datetime) and (value.tzinfo is None or value.utcoffset() != timedelta(0)):
            raise ValueError("Historical acquisition datetime boundaries must be UTC.")
        return HyperCoreSourceAcquisition._floor_to_hour(parsed)
    try:
        first, last = utc_boundary(start), utc_boundary(end)
    except (TypeError, ValueError) as exc:
        raise ValueError("Historical acquisition start/end must be ISO-8601 UTC timestamps.") from exc
    if first.tzinfo != timezone.utc or last.tzinfo != timezone.utc:
        raise ValueError("Historical acquisition boundaries must be UTC.")
    if last <= first:
        raise ValueError("Historical acquisition end must be after start.")
    count = int((last - first).total_seconds() // 3600)
    if count <= 0 or count > maximum_hours:
        raise ValueError(f"Historical acquisition range contains {count} hourly slots; configured maximum is {maximum_hours}.")
    return tuple(HistoricalHourPlan(
        iso(first + timedelta(hours=index)).replace("+00:00", "Z"),
        iso(first + timedelta(hours=index + 1)).replace("+00:00", "Z"),
    ) for index in range(count))


PRESETS: dict[str, dict[str, Any]] = {
    "quick": {"hourly_object_count": 1, "candidate_limit": 1_000, "min_activity": 2, "max_activity_age": "30d"},
    "standard": {"hourly_object_count": 6, "candidate_limit": 2_500, "min_activity": 2, "max_activity_age": "30d"},
    "deep": {"hourly_object_count": 24, "candidate_limit": 5_000, "min_activity": 2, "max_activity_age": "30d"},
}


def cache_directory(database_path: str | Path) -> Path:
    """Keep downloaded public data beside other runtime artifacts, never source."""
    return Path(database_path).parent / "hypercore-cache"


def discovery_preset(name: str) -> dict[str, Any]:
    preset = PRESETS.get(str(name).strip().lower())
    if not preset:
        raise ValueError("Discovery preset must be one of: quick, standard, deep.")
    return {**preset, "window_hours": preset["hourly_object_count"], "preset": str(name).strip().lower()}


class HyperCoreSourceAcquisition:
    """Resolve, cache, and verify only official requester-pays source objects.

    The resolver issues bounded listings below each expected UTC-hour prefix.
    It never paginates or filters the historical archive root, and regards S3
    ``LastModified`` only as provenance for the exact object obtained.
    """

    def __init__(
        self, cache_root: str | Path, *, s3_client_factory: Callable[[], Any] | None = None,
        now: Callable[[], datetime] = utc_now, max_cache_bytes: int = CACHE_MAX_BYTES,
        publication_lag_hours: int = PUBLICATION_LAG_HOURS, max_hourly_lookback: int = MAX_HOURLY_LOOKBACK,
    ) -> None:
        self.cache_root = Path(cache_root).resolve()
        self.s3_client_factory = s3_client_factory
        self.now = now
        self.max_cache_bytes = max_cache_bytes
        self.publication_lag_hours = max(1, int(publication_lag_hours))
        self.max_hourly_lookback = max(1, int(max_hourly_lookback))
        self._last_probe: dict[str, Any] | None = None

    def _client(self) -> Any:
        if self.s3_client_factory is not None:
            return self.s3_client_factory()
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise HyperCoreSourceError(
                "Official HyperCore node-data access requires boto3 and standard AWS requester-pays credentials. "
                "Install the project dependencies, configure AWS outside Trader, then test source access."
            ) from exc
        session = boto3.session.Session()
        if session.get_credentials() is None:
            raise HyperCoreSourceError(
                "Official HyperCore node-data access currently uses an AWS requester-pays source. "
                "No usable AWS credentials were detected on this machine. Configure standard AWS credentials, then click Test Source Access. "
                "No credentials are stored by Trader."
            )
        return session.client("s3", config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3, "mode": "standard"}))

    @staticmethod
    def _is_access_denied(exc: Exception) -> bool:
        text = str(exc).lower()
        return "accessdenied" in text or "access denied" in text or "requester" in text or "forbidden" in text

    @classmethod
    def _access_error(cls, exc: Exception) -> HyperCoreSourceError:
        text = str(exc)
        lowered = text.lower()
        if cls._is_access_denied(exc):
            return HyperCoreSourceError(
                "Failed to access official HyperCore source: AWS requester-pays authorization was denied. "
                "Configure AWS billing/credentials with requester-pays access, then test again."
            )
        if "nosuchbucket" in lowered or "not found" in lowered:
            return HyperCoreSourceError("Failed to access official HyperCore source: the documented source bucket or prefix was unavailable.")
        return HyperCoreSourceError(f"Failed to access official HyperCore source: {text}")

    def credentials_detected(self) -> bool:
        if self.s3_client_factory is not None:
            return True
        try:
            import boto3
            return boto3.session.Session().get_credentials() is not None
        except ImportError:
            return False

    def _probe_prefix(self) -> str:
        reference = self._floor_to_hour(as_utc(self.now())) - timedelta(hours=self.publication_lag_hours)
        return f"{OFFICIAL_HOURLY_PREFIX}{reference.strftime('%Y%m%d')}/"

    def source_status(self, *, test_access: bool = False) -> dict[str, Any]:
        """Return safe credential/probe state without enumerating the archive."""
        cache = self.cache_status()
        newest = cache.get("newest_object") if isinstance(cache.get("newest_object"), dict) else {}
        credentials = self.credentials_detected()
        prior = self._last_probe if credentials else None
        result: dict[str, Any] = {
            "source": "Official HyperCore node data",
            "official_documentation": OFFICIAL_DOCUMENTATION_URL,
            "bucket": OFFICIAL_BUCKET,
            "prefix": OFFICIAL_HOURLY_PREFIX,
            "transport": "aws_s3_requester_pays",
            "aws_credentials_detected": credentials,
            "aws_profile": os.environ.get("AWS_PROFILE") or None,
            "requester_pays_access": prior.get("requester_pays_access") if prior else "UNTESTED",
            "connection_state": prior.get("connection_state") if prior else ("UNTESTED" if credentials else "SETUP_REQUIRED"),
            "probe_prefix": prior.get("probe_prefix") if prior else self._probe_prefix(),
            "probe_object_count": prior.get("probe_object_count") if prior else None,
            "message": prior.get("message") if prior else "Test the official requester-pays source before starting discovery.",
            "cache": cache,
            "newest_available_data": newest.get("data_hour_start") or None,
            "newest_available_data_scope": "latest cached official source hour" if newest else "not resolved yet",
        }
        if not credentials:
            result["message"] = (
                "Official HyperCore node-data access currently uses an AWS requester-pays source. "
                "No usable AWS credentials were detected on this machine. Configure standard AWS credentials, then click Test Source Access. "
                "No credentials are stored by Trader."
            )
        if not test_access or not credentials:
            return result
        try:
            probe_prefix = self._probe_prefix()
            response = self._client().list_objects_v2(
                Bucket=OFFICIAL_BUCKET, Prefix=probe_prefix, MaxKeys=1, RequestPayer="requester",
            )
            result.update({
                "requester_pays_access": "READY", "connection_state": "READY", "probe_prefix": probe_prefix,
                "probe_object_count": len(response.get("Contents", [])),
                "message": "Official requester-pays source access is ready.",
            })
        except Exception as exc:
            message = str(exc) if isinstance(exc, HyperCoreSourceError) else str(self._access_error(exc))
            result.update({
                "requester_pays_access": "FAILED",
                "connection_state": "SETUP_REQUIRED" if self._is_access_denied(exc) else "UNAVAILABLE",
                "message": message,
            })
        self._last_probe = {key: result[key] for key in ("requester_pays_access", "connection_state", "probe_prefix", "probe_object_count", "message")}
        return result

    @staticmethod
    def _floor_to_hour(value: datetime) -> datetime:
        return as_utc(value).replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def _key_components(key: str) -> tuple[datetime, str, int] | None:
        match = _HOURLY_KEY.fullmatch(key)
        if not match:
            return None
        date, hour = match.group("date"), int(match.group("hour"))
        try:
            start = datetime.strptime(f"{date}{hour:02d}", "%Y%m%d%H").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return start, date, hour

    @classmethod
    def _object_from_item(cls, item: dict[str, Any]) -> HyperCoreObject | None:
        key = str(item.get("Key") or "")
        components = cls._key_components(key)
        size = int(item.get("Size") or 0)
        if components is None or size <= 0:
            return None
        hour_start, date, hour = components
        modified = item.get("LastModified")
        return HyperCoreObject(
            bucket=OFFICIAL_BUCKET, key=key, size=size,
            last_modified=iso(as_utc(modified)) if isinstance(modified, datetime) else None,
            etag=str(item.get("ETag") or "").strip('"') or None,
            data_hour_start=iso(hour_start), data_hour_end=iso(hour_start + timedelta(hours=1)), date=date, hour=hour,
        )

    @staticmethod
    def _slot_prefixes(hour_start: datetime) -> tuple[str, ...]:
        date, hour = hour_start.strftime("%Y%m%d"), hour_start.hour
        raw, padded = str(hour), f"{hour:02d}"
        return tuple(dict.fromkeys(f"{OFFICIAL_HOURLY_PREFIX}{date}/{value}" for value in (raw, padded)))

    def _resolve_slot(self, client: Any, hour_start: datetime) -> HyperCoreObject | None:
        matches: dict[str, HyperCoreObject] = {}
        try:
            for prefix in self._slot_prefixes(hour_start):
                response = client.list_objects_v2(
                    Bucket=OFFICIAL_BUCKET, Prefix=prefix, MaxKeys=4, RequestPayer="requester",
                )
                for item in response.get("Contents", []):
                    if not isinstance(item, dict):
                        continue
                    source = self._object_from_item(item)
                    if source is not None and source.data_hour_start == iso(hour_start):
                        matches[source.key] = source
        except HyperCoreSourceError:
            raise
        except Exception as exc:
            raise self._access_error(exc) from exc
        if len(matches) > 1:
            names = ", ".join(sorted(matches))
            raise HyperCoreSourceError(
                f"Official HyperCore hourly slot {iso(hour_start)} returned multiple data objects ({names}). "
                "Refusing to guess which object is the production fill stream."
            )
        return next(iter(matches.values()), None)

    def resolve_hourly_objects(self, required_hours: int, *, lookback_hours: int | None = None) -> list[HyperCoreObject]:
        """Resolve the exact recent UTC-hour objects without archive-root scans."""
        required = int(required_hours)
        lookback = self.max_hourly_lookback if lookback_hours is None else int(lookback_hours)
        if required < 1:
            raise ValueError("Discovery must request at least one HyperCore hourly object.")
        if lookback < required:
            raise ValueError("Hourly source lookback must be at least the requested object count.")
        client = self._client()
        latest = self._floor_to_hour(as_utc(self.now())) - timedelta(hours=self.publication_lag_hours)
        found: list[HyperCoreObject] = []
        for offset in range(lookback):
            source = self._resolve_slot(client, latest - timedelta(hours=offset))
            if source is not None:
                found.append(source)
                if len(found) == required:
                    break
        if len(found) != required:
            raise HyperCoreSourceError(
                f"Requested {required} recent HyperCore hourly objects but only {len(found)} were available "
                f"within the bounded {lookback}-hour source lookback."
            )
        return sorted(found, key=lambda item: (item.data_hour_start or "", item.key))

    def resolve_historical_slots(
        self, slots: Iterable[HistoricalHourPlan], *, cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, HyperCoreObject | None]:
        """Resolve exactly named historical hours without a root archive scan.

        Results contain ``None`` for an absent official hour.  That absence is
        evidence for the D.7 coverage authority, not a reason to pretend the
        surrounding downloaded files provide complete coverage.
        """
        selected = tuple(slots)
        if not selected:
            raise ValueError("Historical acquisition requires at least one UTC-hour slot.")
        client = self._client()
        resolved: dict[str, HyperCoreObject | None] = {}
        for slot in selected:
            if cancelled and cancelled():
                break
            hour = self._floor_to_hour(as_utc(slot.start))
            resolved[iso(hour).replace("+00:00", "Z")] = self._resolve_slot(client, hour)
        return resolved

    def resolve_recent(self, window: timedelta, *, maximum_objects: int | None = None) -> list[HyperCoreObject]:
        """Compatibility wrapper: a window now means a UTC-hour object count."""
        if window.total_seconds() <= 0:
            raise ValueError("Discovery source window must be positive.")
        requested = max(1, math.ceil(window.total_seconds() / 3600))
        return self.resolve_hourly_objects(maximum_objects or requested)

    def preflight(self, objects: Iterable[HyperCoreObject]) -> dict[str, Any]:
        """Validate the complete job plan and reserve space before any GET."""
        plan = list(objects)
        if not plan:
            raise HyperCoreSourceError("The HyperCore acquisition plan contained no official hourly objects.")
        if len({item.identifier for item in plan}) != len(plan):
            raise HyperCoreSourceError("The HyperCore acquisition plan contained duplicate official objects.")
        for item in plan:
            self._validate_source(item)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        cached: list[HyperCoreObject] = []
        protected_paths: set[Path] = set()
        for item in plan:
            data_path, metadata_path = self._paths_for(item)
            protected_paths.add(data_path)
            if self._cached_metadata(item, data_path, metadata_path) is not None:
                cached.append(item)
        bytes_total = sum(item.size for item in plan)
        bytes_cached = sum(item.size for item in cached)
        bytes_to_download = bytes_total - bytes_cached
        if bytes_total > self.max_cache_bytes:
            raise HyperCoreSourceError(
                f"This discovery scan requires approximately {self._bytes(bytes_total)} of staging space, "
                f"but the HyperCore cache limit is {self._bytes(self.max_cache_bytes)}. "
                "Choose a smaller scan or increase the configured cache allowance."
            )
        entries = self._cache_files()
        unrelated_bytes = sum(path.stat().st_size for _, path in entries if path not in protected_paths and path.exists())
        available_before = shutil.disk_usage(self.cache_root).free
        required_free_bytes = bytes_to_download + (DISK_RESERVE_BYTES if bytes_to_download else 0)
        if available_before + unrelated_bytes < required_free_bytes:
            raise HyperCoreSourceError(
                "Insufficient free disk space to stage the complete HyperCore discovery plan before download. "
                f"Need {self._bytes(required_free_bytes)} and only "
                f"{self._bytes(available_before + unrelated_bytes)} is available after eligible cache cleanup."
            )
        self._prune_for(bytes_to_download, protected_paths=protected_paths)
        cache_after = self.cache_status()["size_bytes"]
        if cache_after + bytes_to_download > self.max_cache_bytes:
            raise HyperCoreSourceError("Unable to reserve the bounded HyperCore cache for the complete discovery plan.")
        available_after = shutil.disk_usage(self.cache_root).free
        if available_after < required_free_bytes:
            raise HyperCoreSourceError("Insufficient free disk space to stage the complete HyperCore discovery plan before download.")
        return {
            "objects_planned": len(plan), "objects_cached": len(cached), "bytes_total": bytes_total,
            "bytes_cached": bytes_cached, "bytes_to_download": bytes_to_download,
            "available_disk_bytes": available_after, "cache_limit_bytes": self.max_cache_bytes,
            "cached_source_identifiers": [item.identifier for item in cached],
            "protected_paths": [str(path) for path in sorted(protected_paths)],
        }

    def acquire(self, source: HyperCoreObject, *, protected_paths: Iterable[str | Path] = ()) -> tuple[Path, dict[str, Any]]:
        self._validate_source(source)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        data_path, metadata_path = self._paths_for(source)
        cached = self._cached_metadata(source, data_path, metadata_path)
        if cached is not None:
            return data_path, cached
        if source.size > self.max_cache_bytes:
            raise HyperCoreSourceError("Official HyperCore source object exceeds the configured bounded cache size.")
        protected = {Path(value).resolve() for value in protected_paths}
        protected.add(data_path)
        self._prune_for(source.size, protected_paths=protected)
        free = shutil.disk_usage(self.cache_root).free
        if source.size > 0 and free < source.size + DISK_RESERVE_BYTES:
            raise HyperCoreSourceError("Insufficient free disk space to cache the requested official HyperCore source object.")
        partial = data_path.with_suffix(data_path.suffix + ".partial")
        partial.unlink(missing_ok=True)
        digest = hashlib.sha256()
        try:
            response = self._client().get_object(Bucket=source.bucket, Key=source.key, RequestPayer="requester")
            body = response["Body"]
            try:
                with partial.open("wb") as stream:
                    while True:
                        chunk = body.read(1024 * 1024)
                        if not chunk:
                            break
                        stream.write(chunk)
                        digest.update(chunk)
            finally:
                body.close()
            actual_size = partial.stat().st_size if partial.exists() else 0
            expected_size = int(response.get("ContentLength") or source.size or 0)
            if actual_size <= 0 or (expected_size and actual_size != expected_size):
                raise HyperCoreSourceError("Official HyperCore source download was incomplete; the partial object was not accepted.")
            os.replace(partial, data_path)
        except HyperCoreSourceError:
            partial.unlink(missing_ok=True)
            raise
        except Exception as exc:
            partial.unlink(missing_ok=True)
            raise self._access_error(exc) from exc
        metadata = {
            **asdict(source), "identifier": source.identifier, "local_cache_path": str(data_path),
            "acquired_at": iso(self.now()), "source_transport": "aws_s3_requester_pays",
            "official_source_identifier": source.identifier, "object_checksum": source.etag,
            # ETags are provenance only: multipart S3 ETags are not an MD5
            # contract.  This digest covers the exact bytes accepted locally.
            "sha256": digest.hexdigest(),
        }
        self._write_metadata(metadata_path, metadata)
        return data_path, metadata

    def cache_status(self) -> dict[str, Any]:
        if not self.cache_root.exists():
            return {"path": str(self.cache_root), "size_bytes": 0, "object_count": 0, "newest_object": None,
                    "cache_limit_bytes": self.max_cache_bytes}
        entries = self._metadata_entries()
        valid = [item for item in entries if self._contained_file(item.get("local_cache_path"))]
        files = [Path(str(item["local_cache_path"])) for item in valid]
        size = sum(path.stat().st_size for path in files if path.exists() and path.is_file())
        newest = max(valid, key=lambda item: (str(item.get("data_hour_start") or ""), str(item.get("key") or "")), default=None)
        return {"path": str(self.cache_root), "size_bytes": size, "object_count": len(files), "newest_object": newest,
                "cache_limit_bytes": self.max_cache_bytes}

    def prune(self) -> dict[str, int]:
        return self._prune_for(0)

    def _validate_source(self, source: HyperCoreObject) -> None:
        components = self._key_components(source.key)
        if source.bucket != OFFICIAL_BUCKET or components is None:
            raise HyperCoreSourceError("Only documented official HyperCore hourly node_fills_by_block objects may be acquired.")
        hour_start, date, hour = components
        if source.data_hour_start and source.data_hour_start != iso(hour_start):
            raise HyperCoreSourceError("Official HyperCore source data-hour provenance did not match its object key.")
        if (source.date and source.date != date) or (source.hour is not None and source.hour != hour):
            raise HyperCoreSourceError("Official HyperCore source date/hour provenance did not match its object key.")

    def _paths_for(self, source: HyperCoreObject) -> tuple[Path, Path]:
        digest = hashlib.sha256(f"{source.bucket}/{source.key}".encode("utf-8")).hexdigest()[:24]
        suffix = Path(source.key).suffix if Path(source.key).suffix else ".jsonl"
        data_path = (self.cache_root / f"hypercore_{digest}{suffix}").resolve()
        if self.cache_root not in data_path.parents:
            raise HyperCoreSourceError("Resolved cache path escaped the configured HyperCore cache directory.")
        return data_path, data_path.with_suffix(data_path.suffix + ".metadata.json")

    def _cached_metadata(self, source: HyperCoreObject, data_path: Path, metadata_path: Path) -> dict[str, Any] | None:
        if not data_path.exists() or data_path.stat().st_size <= 0 or not metadata_path.exists():
            return None
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(metadata, dict) or metadata.get("bucket") != source.bucket or metadata.get("key") != source.key:
            return None
        if source.size and data_path.stat().st_size != source.size:
            return None
        if source.etag and metadata.get("etag") and metadata.get("etag") != source.etag:
            return None
        if source.data_hour_start and metadata.get("data_hour_start") != source.data_hour_start:
            return None
        expected_digest = metadata.get("sha256")
        # A cache entry written before digest hardening is deliberately
        # re-fetched.  Size and ETag cannot prove same-size local tampering.
        if not isinstance(expected_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
            return None
        if self._file_sha256(data_path) != expected_digest:
            return None
        return metadata

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def _metadata_entries(self) -> list[dict[str, Any]]:
        if not self.cache_root.exists():
            return []
        entries: list[dict[str, Any]] = []
        for path in self.cache_root.glob("*.metadata.json"):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    entries.append(value)
            except (OSError, json.JSONDecodeError):
                continue
        return entries

    def _cache_files(self) -> list[tuple[dict[str, Any], Path]]:
        return [(item, Path(str(item.get("local_cache_path") or "")).resolve()) for item in self._metadata_entries()
                if self._contained_file(item.get("local_cache_path"))]

    def _contained_file(self, value: object) -> bool:
        try:
            path = Path(str(value)).resolve()
            return self.cache_root in path.parents and path.is_file()
        except OSError:
            return False

    def _prune_for(self, incoming_bytes: int, *, protected_paths: Iterable[Path] = ()) -> dict[str, int]:
        protected = {Path(path).resolve() for path in protected_paths}
        files = sorted(self._cache_files(), key=lambda item: str(item[0].get("acquired_at") or ""))
        total = sum(path.stat().st_size for _, path in files if path.exists())
        removed = 0
        for _, path in files:
            if total + max(0, incoming_bytes) <= self.max_cache_bytes:
                break
            if path in protected or not self._contained_file(path):
                continue
            size = path.stat().st_size
            metadata = path.with_suffix(path.suffix + ".metadata.json")
            path.unlink(missing_ok=True)
            metadata.unlink(missing_ok=True)
            total -= size
            removed += 1
        return {"removed_objects": removed, "size_bytes": max(total, 0)}

    @staticmethod
    def _write_metadata(path: Path, metadata: dict[str, Any]) -> None:
        partial = path.with_suffix(path.suffix + ".partial")
        partial.write_text(json.dumps(metadata, sort_keys=True, default=str), encoding="utf-8")
        os.replace(partial, path)

    @staticmethod
    def _bytes(value: int) -> str:
        return f"{value / (1024 ** 3):.1f} GiB" if value >= 1024 ** 3 else f"{value / (1024 ** 2):.1f} MiB"
