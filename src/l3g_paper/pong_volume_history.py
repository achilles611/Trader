"""Provider-only, restart-verifiable volume references. Never order-flow evidence."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import json
import os
from pathlib import Path
from uuid import uuid4

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.lane_iii.contracts import canonical_hash, normalized_utc
from .pong_microstructure import VolumeWindow, VolumeWindowKind
from .pong_contract import SEP26_TOPOLOGY, PongContractTopology

HISTORY_TYPE = "VOLUME_REFERENCE_HISTORY"
HISTORY_POLICY = "PROVIDER_COMPLETED_UTC_5M_VOLUME_V1"
HISTORY_BARS_BACK = 2016
HISTORY_CHUNK_SIZE = 128
_CONTEXT = b"PONG_VOLUME_REFERENCE_RECEIPT_V1"


def _time(value: object) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, "Volume reference time").replace("Z", "+00:00"))


def _wire(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


class PongVolumeReferenceHistory:
    """Admit one complete signed provider response; enforce availability at use."""

    def __init__(
        self,
        path: Path,
        *,
        root_key: bytes,
        expected_source: str,
        topology: PongContractTopology = SEP26_TOPOLOGY,
    ) -> None:
        self.path = path
        self._key = bytes(root_key)
        self.expected_source = expected_source
        self.topology = topology
        self._pending: list[dict] = []
        self._pending_id: str | None = None
        self.windows: tuple[VolumeWindow, ...] = ()
        self._prior_index_source: tuple[VolumeWindow, ...] | None = None
        self._prior_by_slot: dict[str, tuple[VolumeWindow, ...]] = {}
        self.error: str | None = None
        self.receipt_hash: str | None = None
        if path.exists():
            try:
                value = json.loads(path.read_bytes())
                seal = value.pop("receipt_hmac")
                if not hmac.compare_digest(seal, self._seal(value)):
                    raise ValueError("RECEIPT_AUTHENTICATION")
                self.windows = self._decode(value["records"], value["received_at"])
                self.receipt_hash = canonical_hash(value)
            except (ValueError, TypeError, KeyError, OSError) as error:
                self.error = "PONG_VOLUME_HISTORY_RESTORE_REFUSED:" + str(error)

    def _seal(self, value: object) -> str:
        key = hmac.new(self._key, _CONTEXT, hashlib.sha256).digest()
        return hmac.new(key, _bytes(value), hashlib.sha256).hexdigest()

    def _verified(self, raw: dict) -> NinjaTraderObservation:
        return NinjaTraderObservation.from_wire(json.dumps(raw)).verify_attestation(
            self._key, expected_source_fingerprint=self.expected_source,
        )

    def _decode(self, records: list[dict], received_at: str) -> tuple[VolumeWindow, ...]:
        if not records or len(records) > 16:
            raise ValueError("CHUNK_COUNT")
        observed = _time(received_at)
        identity = None
        windows = []
        last_end = None
        for index, raw in enumerate(records):
            record = self._verified(raw)
            p = record.payload
            if (record.observation_type != HISTORY_TYPE or record.account_alias is not None
                    or record.producer.source_stream != "HISTORICAL_BARS"
                    or record.producer.publication_policy != HISTORY_POLICY
                    or p.get("publication_policy") != HISTORY_POLICY
                    or p.get("contract_id") != self.topology.native_instrument
                    or p.get("canonical_contract") != self.topology.canonical_contract
                    or p.get("source") != "NINJATRADER_BARS_REQUEST_PROVIDER"
                    or p.get("merge_policy") != "DO_NOT_MERGE"
                    or p.get("trading_hours") != "Default 24 x 7" or p.get("timezone") != "UTC"
                    or p.get("bar_period_minutes") != 5 or p.get("requested_bars_back") != HISTORY_BARS_BACK
                    or p.get("status") != "COMPLETE" or p.get("chunk_index") != index
                    or p.get("chunk_count") != len(records)):
                raise ValueError("PROVIDER_CONTRACT")
            requested = _time(p["requested_at"])
            if requested > _time(record.ninja_receipt_time) or _time(record.ninja_receipt_time) > observed:
                raise ValueError("FUTURE_AVAILABILITY")
            current_identity = (record.session_id, record.producer.source_fingerprint,
                                record.producer.build_fingerprint, p["request_id"], p["requested_at"], p["total_bars"])
            if identity is not None and current_identity != identity:
                raise ValueError("MIXED_REQUEST")
            identity = current_identity
            bars = p.get("bars")
            if not isinstance(bars, list) or not 1 <= len(bars) <= HISTORY_CHUNK_SIZE:
                raise ValueError("BARS_SHAPE")
            if index < len(records) - 1 and len(bars) != HISTORY_CHUNK_SIZE:
                raise ValueError("CHUNK_INCOMPLETE")
            for bar in bars:
                start, end = _time(bar["start"]), _time(bar["end"])
                if (start.minute % 5 or start.second or start.microsecond
                        or end != start + timedelta(minutes=5) or end > requested
                        or (last_end is not None and start < last_end)):
                    raise ValueError("BAR_ALIGNMENT_OR_OVERLAP")
                # Session closures and absent provider bars remain absent. Never
                # interpolate zero volume or pretend a missing slot was loaded.
                volume = bar.get("volume")
                if type(volume) is not int or volume < 0:
                    raise ValueError("BAR_VOLUME")
                last_end = end
                digest = canonical_hash({"bar": bar, "observation": record.wire_hash()})
                windows.append(VolumeWindow(
                    "pong-provider-volume-" + digest[:32], digest, self.topology.native_instrument,
                    f"UTC-{start.hour:02d}:{start.minute:02d}-5M", _wire(start), _wire(end),
                    _wire(observed), Decimal(volume), VolumeWindowKind.COMPLETED, True, True,
                ))
        if type(identity[-1]) is not int or identity[-1] != len(windows) or len(windows) > HISTORY_BARS_BACK:
            raise ValueError("TOTAL_BARS_INCOMPLETE")
        return tuple(windows)

    def admit(self, record: NinjaTraderObservation, *, received_at: str) -> None:
        """Called by the worker, never by a market callback or native dispatcher."""
        try:
            raw = {**record.unsigned_wire_payload(), "signature": record.signature}
            verified = self._verified(raw)
            p = verified.payload
            if p.get("chunk_index") == 0:
                self._pending = []
                self._pending_id = str(p.get("request_id"))
            if (str(p.get("request_id")) != self._pending_id
                    or p.get("chunk_index") != len(self._pending)
                    or type(p.get("chunk_count")) is not int or not 1 <= p["chunk_count"] <= 16):
                raise ValueError("CHUNK_SEQUENCE")
            self._pending.append(raw)
            if len(self._pending) != p["chunk_count"]:
                return
            windows = self._decode(self._pending, received_at)
            body = {"schema": "pong-volume-reference-receipt-v1", "records": self._pending,
                    "received_at": received_at, "authentic_order_flow_authority": "DENIED"}
            value = dict(body, receipt_hmac=self._seal(body))
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name("." + self.path.name + "." + uuid4().hex + ".tmp")
            try:
                with temporary.open("xb") as handle:
                    handle.write(_bytes(value))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
            finally:
                temporary.unlink(missing_ok=True)
            self.windows, self.error, self.receipt_hash = windows, None, canonical_hash(body)
            self._pending = []
            self._pending_id = None
        except (ValueError, TypeError, KeyError, OSError) as error:
            self.error = "PONG_VOLUME_HISTORY_ADMISSION_REFUSED:" + str(error)
            self._pending = []
            self._pending_id = None

    def prior(self, bucket: datetime) -> tuple[VolumeWindow, ...]:
        key = f"UTC-{bucket.hour:02d}:{bucket.minute:02d}-5M"
        if self.error:
            return ()
        # Index immutable provider windows; receipt replacement invalidates the
        # index. Availability and strict-prior checks still run at every use.
        if self._prior_index_source is not self.windows:
            slots: dict[str, list[VolumeWindow]] = {}
            for window in self.windows:
                slots.setdefault(window.normalization_key, []).append(window)
            self._prior_by_slot = {slot: tuple(values) for slot, values in slots.items()}
            self._prior_index_source = self.windows
        return tuple(w for w in self._prior_by_slot.get(key, ())
                     if _time(w.window_end) < bucket and _time(w.available_at) <= bucket)

    def status(self, at: str) -> dict:
        moment = _time(at)
        bucket = moment.replace(minute=moment.minute // 5 * 5, second=0, microsecond=0)
        prior = self.prior(bucket)
        return {"required_prior_windows_per_utc_slot": 3, "loaded_windows": len(self.windows),
                "current_slot_prior_windows": len(prior), "ready_for_current_slot": len(prior) >= 3,
                "first_start": self.windows[0].window_start if self.windows else None,
                "last_end": self.windows[-1].window_end if self.windows else None,
                "available_at": self.windows[0].available_at if self.windows else None,
                "receipt_hash": self.receipt_hash, "error": self.error,
                "instrument": self.topology.native_instrument,
                "canonical_contract": self.topology.canonical_contract,
                "topology_version": self.topology.version,
                "topology_hash": self.topology.binding_hash,
                "pending_chunks": len(self._pending), "authentic_order_flow_authority": "DENIED"}
