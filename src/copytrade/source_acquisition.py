"""Official HyperCore node-data acquisition for Phase C orchestration.

This module deliberately stops at obtaining verified local objects.  Frozen
Phase A continues to parse and normalize them through ``hypercore-file``.
Hyperliquid documents the public, requester-pays S3 prefix used here:
https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

from .models import as_utc, iso, utc_now


OFFICIAL_BUCKET = "hl-mainnet-node-data"
OFFICIAL_PREFIX = "node_fills_by_block/"
OFFICIAL_DOCUMENTATION_URL = "https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data"
CACHE_MAX_BYTES = 5 * 1024 * 1024 * 1024


class HyperCoreSourceError(RuntimeError):
    """A safe, operator-facing acquisition/access failure."""


@dataclass(frozen=True)
class HyperCoreObject:
    bucket: str
    key: str
    size: int
    last_modified: str | None
    etag: str | None = None

    @property
    def identifier(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


PRESETS: dict[str, dict[str, Any]] = {
    "quick": {"window": timedelta(hours=1), "candidate_limit": 1_000, "min_activity": 2, "max_activity_age": "30d"},
    "standard": {"window": timedelta(hours=6), "candidate_limit": 2_500, "min_activity": 2, "max_activity_age": "30d"},
    "deep": {"window": timedelta(hours=24), "candidate_limit": 5_000, "min_activity": 2, "max_activity_age": "30d"},
}


def cache_directory(database_path: str | Path) -> Path:
    """Keep downloaded public data beside other runtime artifacts, never source."""
    return Path(database_path).parent / "hypercore-cache"


def discovery_preset(name: str) -> dict[str, Any]:
    preset = PRESETS.get(str(name).strip().lower())
    if not preset:
        raise ValueError("Discovery preset must be one of: quick, standard, deep.")
    return {**preset, "preset": str(name).strip().lower()}


class HyperCoreSourceAcquisition:
    """Resolve, cache, and verify only official requester-pays source objects.

    Object keys are never supplied by a browser.  The resolver lists the
    documented source prefix with ``RequestPayer=requester`` and selects objects
    by S3's authoritative LastModified timestamps for the requested window.
    """

    def __init__(
        self, cache_root: str | Path, *, s3_client_factory: Callable[[], Any] | None = None,
        now: Callable[[], datetime] = utc_now, max_cache_bytes: int = CACHE_MAX_BYTES,
    ) -> None:
        self.cache_root = Path(cache_root).resolve()
        self.s3_client_factory = s3_client_factory
        self.now = now
        self.max_cache_bytes = max_cache_bytes

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
    def _access_error(exc: Exception) -> HyperCoreSourceError:
        text = str(exc)
        lowered = text.lower()
        if "accessdenied" in lowered or "access denied" in lowered or "requester" in lowered:
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

    def source_status(self, *, test_access: bool = False) -> dict[str, Any]:
        cache = self.cache_status()
        newest = cache.get("newest_object") if isinstance(cache.get("newest_object"), dict) else {}
        result: dict[str, Any] = {
            "source": "Official HyperCore node data",
            "official_documentation": OFFICIAL_DOCUMENTATION_URL,
            "bucket": OFFICIAL_BUCKET,
            "prefix": OFFICIAL_PREFIX,
            "transport": "aws_s3_requester_pays",
            "aws_credentials_detected": self.credentials_detected(),
            "requester_pays_access": "not_tested",
            "connection_state": "READY" if self.credentials_detected() else "SETUP_REQUIRED",
            "message": "Official public HyperCore node data is distributed through AWS requester-pays S3.",
            "cache": cache,
            "newest_available_data": newest.get("last_modified") or None,
            "newest_available_data_scope": "most recent cached/resolved official object" if newest else "not resolved yet",
        }
        if not result["aws_credentials_detected"]:
            result["message"] = (
                "Official HyperCore node-data access currently uses an AWS requester-pays source. "
                "No usable AWS credentials were detected on this machine. Configure standard AWS credentials, then click Test Source Access. "
                "No credentials are stored by Trader."
            )
        if test_access:
            try:
                self._client().list_objects_v2(Bucket=OFFICIAL_BUCKET, Prefix=OFFICIAL_PREFIX, MaxKeys=1, RequestPayer="requester")
                result.update({"requester_pays_access": "ready", "connection_state": "READY", "message": "Official requester-pays source access is ready."})
            except HyperCoreSourceError as exc:
                result.update({"requester_pays_access": "failed", "connection_state": "SETUP_REQUIRED", "message": str(exc)})
            except Exception as exc:
                message = str(self._access_error(exc))
                result.update({"requester_pays_access": "failed", "connection_state": "SETUP_REQUIRED" if "authorization was denied" in message else "UNAVAILABLE", "message": message})
        return result

    def resolve_recent(self, window: timedelta, *, maximum_objects: int | None = None) -> list[HyperCoreObject]:
        if window.total_seconds() <= 0:
            raise ValueError("Discovery source window must be positive.")
        client = self._client()
        cutoff, now = as_utc(self.now() - window), as_utc(self.now())
        token: str | None = None
        matches: list[HyperCoreObject] = []
        try:
            while True:
                request: dict[str, Any] = {"Bucket": OFFICIAL_BUCKET, "Prefix": OFFICIAL_PREFIX, "MaxKeys": 1000, "RequestPayer": "requester"}
                if token:
                    request["ContinuationToken"] = token
                response = client.list_objects_v2(**request)
                for item in response.get("Contents", []):
                    key = str(item.get("Key") or "")
                    if not key.startswith(OFFICIAL_PREFIX) or not key or key.endswith("/"):
                        continue
                    modified = item.get("LastModified")
                    if not isinstance(modified, datetime):
                        continue
                    modified_at = as_utc(modified)
                    if cutoff <= modified_at <= now and int(item.get("Size") or 0) > 0:
                        matches.append(HyperCoreObject(
                            bucket=OFFICIAL_BUCKET, key=key, size=int(item.get("Size") or 0),
                            last_modified=iso(modified_at), etag=str(item.get("ETag") or "").strip('"') or None,
                        ))
                if not response.get("IsTruncated"):
                    break
                token = str(response.get("NextContinuationToken") or "") or None
                if token is None:
                    raise HyperCoreSourceError("Official HyperCore source enumeration ended without a continuation token.")
        except HyperCoreSourceError:
            raise
        except Exception as exc:
            raise self._access_error(exc) from exc
        ordered = sorted(matches, key=lambda item: (item.last_modified or "", item.key))
        if maximum_objects is not None:
            ordered = ordered[-max(1, int(maximum_objects)):]
        if not ordered:
            raise HyperCoreSourceError(
                f"Official HyperCore source object unavailable for the requested {int(window.total_seconds() // 3600) or 1}-hour window. "
                "Try a wider scan window."
            )
        return ordered

    def acquire(self, source: HyperCoreObject) -> tuple[Path, dict[str, Any]]:
        if source.bucket != OFFICIAL_BUCKET or not source.key.startswith(OFFICIAL_PREFIX):
            raise HyperCoreSourceError("Only the documented official HyperCore node_fills_by_block source may be acquired.")
        self.cache_root.mkdir(parents=True, exist_ok=True)
        data_path, metadata_path = self._paths_for(source)
        cached = self._cached_metadata(source, data_path, metadata_path)
        if cached is not None:
            return data_path, cached
        if source.size > self.max_cache_bytes:
            raise HyperCoreSourceError("Official HyperCore source object exceeds the configured bounded cache size.")
        self._prune_for(source.size)
        free = shutil.disk_usage(self.cache_root).free
        if source.size > 0 and free < source.size + 50 * 1024 * 1024:
            raise HyperCoreSourceError("Insufficient free disk space to cache the requested official HyperCore source object.")
        partial = data_path.with_suffix(data_path.suffix + ".partial")
        partial.unlink(missing_ok=True)
        try:
            response = self._client().get_object(Bucket=source.bucket, Key=source.key, RequestPayer="requester")
            body = response["Body"]
            with partial.open("wb") as stream:
                while True:
                    chunk = body.read(1024 * 1024)
                    if not chunk:
                        break
                    stream.write(chunk)
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
        }
        self._write_metadata(metadata_path, metadata)
        return data_path, metadata

    def cache_status(self) -> dict[str, Any]:
        if not self.cache_root.exists():
            return {"path": str(self.cache_root), "size_bytes": 0, "object_count": 0, "newest_object": None}
        entries = self._metadata_entries()
        valid = [item for item in entries if self._contained_file(item.get("local_cache_path"))]
        files = [Path(str(item["local_cache_path"])) for item in valid]
        size = sum(path.stat().st_size for path in files if path.exists() and path.is_file())
        newest = max(valid, key=lambda item: str(item.get("acquired_at") or item.get("last_modified") or ""), default=None)
        return {"path": str(self.cache_root), "size_bytes": size, "object_count": len(files), "newest_object": newest}

    def prune(self) -> dict[str, int]:
        return self._prune_for(0)

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
        return metadata

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

    def _contained_file(self, value: object) -> bool:
        try:
            path = Path(str(value)).resolve()
            return self.cache_root in path.parents and path.is_file()
        except OSError:
            return False

    def _prune_for(self, incoming_bytes: int) -> dict[str, int]:
        entries = sorted(self._metadata_entries(), key=lambda item: str(item.get("acquired_at") or ""))
        files = [(item, Path(str(item.get("local_cache_path") or ""))) for item in entries]
        total = sum(path.stat().st_size for _, path in files if self._contained_file(path))
        removed = 0
        for item, path in files:
            if total + max(0, incoming_bytes) <= self.max_cache_bytes:
                break
            if not self._contained_file(path):
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
