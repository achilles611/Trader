"""Explicit local operator authorization for one dated Sim101 trial.

This file binds scheduling authority, not strategy policy or native evidence.
It does not arm, clear a lockout, or start a data-wait supervisor.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from src.lane_iii.contracts import canonical_hash


PONG_AUTHORIZATION_ENV = "BEELZEBUB_PONG_COMMISSIONING_AUTHORIZATION"
_FIXED = {
    "schema": "pong-single-trial-authorization-v1",
    "account": "Sim101",
    "account_class": "LOCAL_SIMULATION",
    "instrument": "MNQ SEP26",
    "canonical_contract": "MNQU6",
    "maximum_quantity": 1,
    "run_duration_seconds": 3600,
    "maximum_runs": 1,
    "live_authority": "DISARMED",
    "live_capital": "DENIED",
}


@dataclass(frozen=True)
class PongLaunchAuthorization:
    experiment_id: str
    activation_date_local: date
    ny_open_blackout: bool
    manifest_hash: str
    source_sha256: str
    operator_acknowledged_at_utc: str | None = None
    expires_at_utc: str | None = None

    @classmethod
    def load(cls, path: str | Path) -> "PongLaunchAuthorization":
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        fields = set(_FIXED) | {
            "activation_date_local", "ny_open_blackout", "source_path", "source_sha256",
        }
        version_two = isinstance(value, dict) and value.get('schema') == 'pong-single-trial-authorization-v2'
        fixed = dict(_FIXED)
        if version_two:
            fields |= {'operator_acknowledged_at_utc', 'operator_acknowledged_at_denver', 'expires_at_utc'}
            fixed['schema'] = 'pong-single-trial-authorization-v2'
        if type(value) is not dict or set(value) != fields:
            raise ValueError("PONG_LAUNCH_AUTHORIZATION_FIELDS_INVALID")
        if any(type(value[key]) is not type(expected) or value[key] != expected
               for key, expected in fixed.items()):
            raise ValueError("PONG_LAUNCH_AUTHORIZATION_SCOPE_INVALID")
        day = date.fromisoformat(value["activation_date_local"])
        if version_two:
            denver = ZoneInfo('America/Denver')
            acknowledged = datetime.fromisoformat(value['operator_acknowledged_at_utc'].replace('Z', '+00:00'))
            expires = datetime.fromisoformat(value['expires_at_utc'].replace('Z', '+00:00'))
            expected_end = datetime.combine(day + timedelta(days=1), datetime.min.time(), denver)
            if (acknowledged.utcoffset() != timedelta(0) or expires.utcoffset() != timedelta(0)
                    or acknowledged >= expires or expires != expected_end
                    or acknowledged.astimezone(denver).date() != day
                    or value['operator_acknowledged_at_denver'] != acknowledged.astimezone(denver).isoformat()):
                raise ValueError('PONG_LAUNCH_AUTHORIZATION_TIME_SCOPE_INVALID')
        if type(value["ny_open_blackout"]) is not bool:
            raise ValueError("PONG_LAUNCH_AUTHORIZATION_BLACKOUT_INVALID")
        source = Path(value["source_path"])
        if not source.is_absolute() or hashlib.sha256(source.read_bytes()).hexdigest() != value["source_sha256"]:
            raise ValueError("PONG_LAUNCH_AUTHORIZATION_SOURCE_MISMATCH")
        digest = canonical_hash(value)
        return cls(
            "pong-single-trial-" + day.strftime("%Y%m%d") + "-" + digest[:16],
            day, value["ny_open_blackout"], digest, value["source_sha256"],
            value.get('operator_acknowledged_at_utc'), value.get('expires_at_utc'),
        )
