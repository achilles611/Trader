"""Run the disarmed, authenticated L3H loopback gateway as a local service.

The service never dispatches a command, sends a heartbeat, constructs a live
runtime, or reads broker credentials.  Its only side effects are opening the
loopback listener and writing a compact, non-secret health snapshot for the
local dashboard.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time


REPOSITORY = Path(__file__).resolve().parents[1]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from src.l3h_live.contracts import AccountClass, canonical_json, load_verified_capability, utc_now
from src.l3h_live.gateway import AuthenticatedLoopbackGateway


def _root(value: str | None) -> Path:
    return Path(value).expanduser().resolve() if value else Path(os.environ["LOCALAPPDATA"]) / "Beelzebub" / "authority" / "l3h"


def _capability(root: Path):
    candidates = [path for path in (root / "capabilities").glob("l3h-cap-sim101-*.json") if not path.name.endswith(".attestation.json")]
    if not candidates:
        raise RuntimeError("No local Sim101 mechanical capability exists.")
    capability = load_verified_capability(max(candidates, key=lambda path: path.stat().st_mtime), (root / "keys" / "l3h.capability.hmac.key").read_bytes())
    if capability.account_class is not AccountClass.LOCAL_SIMULATION or capability.live_capital:
        raise RuntimeError("Gateway service refuses non-Sim101 or live-capital capability material.")
    return capability


def _write(root: Path, capability, gateway: AuthenticatedLoopbackGateway) -> None:
    path = root / "events" / "l3h-gateway-status.json"
    reports = gateway.reconciliations()
    payload = {
        "schema": "l3h-gateway-status-v1", "updated_at": utc_now(),
        "account_class": capability.account_class.value, "live_capital": "DENIED", "live_armed": False,
        "capability_hash": capability.capability_hash, "source_fingerprint": capability.source_fingerprint,
        "gateway": dict(gateway.status), "reconciliation": None if not reports else dict(reports[-1]),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(canonical_json(payload))
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run only the disarmed L3H Sim101 gateway.")
    parser.add_argument("--authority-root")
    parser.add_argument("--run-once", action="store_true")
    args = parser.parse_args()
    root = _root(args.authority_root)
    capability = _capability(root)
    gateway = AuthenticatedLoopbackGateway(
        (root / "keys" / "l3h.execution.local.key").read_bytes(),
        expected_addon_fingerprint=capability.source_fingerprint,
        expected_capability_hash=capability.capability_hash,
    )
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    gateway.start()
    try:
        while not stopping:
            _write(root, capability, gateway)
            if args.run_once:
                break
            time.sleep(1)
    finally:
        gateway.stop()
        _write(root, capability, gateway)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
