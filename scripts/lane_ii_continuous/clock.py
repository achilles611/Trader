"""Measured process clock and renewable decision-admission gate.

The coordinator publishes CLOCK_CONTROL.json.  Workers never change the Windows
clock and never query NTP.  A held clock disables decisions, not local analysis.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import threading
import time

RAW_TIME = time.time
MONOTONIC = time.monotonic
DEFAULT_BOUNDS = {"sample_count": 3, "peer_dispersion_seconds": .2, "sample_span_seconds": .2,
                  "maximum_correction_seconds": 60., "host_jump_seconds": .15,
                  "continuity_seconds": .2, "maximum_age_seconds": 600.}


def project(anchor: dict, monotonic: float | None = None) -> float:
    return anchor["utc_epoch"] + (MONOTONIC() if monotonic is None else monotonic) - anchor["monotonic"]


def assess_calibration(samples: dict, *, host_epoch: float, monotonic: float, boot_id,
                       prior_anchor: dict | None = None, host_elapsed: float | None = None,
                       monotonic_elapsed: float | None = None, bounds: dict | None = None) -> dict:
    bounds = {**DEFAULT_BOUNDS, **(bounds or {})}
    result = {"schema": "LANE_II_CLOCK_CONTROL_V1", "state": "held", "reason": None,
              "peer_samples_seconds": samples, "bounds": bounds, "observed_host_epoch": host_epoch,
              "observed_monotonic": monotonic, "boot_id": boot_id, "host_clock_modified": False}
    reason = None
    if (not isinstance(samples, dict) or len(samples) < 2 or
            any(len(v) != bounds["sample_count"] or any(not isinstance(x, (int, float)) or isinstance(x, bool) or not math.isfinite(x) for x in v) for v in samples.values())):
        reason = "CLOCK_SAMPLES_INCOMPLETE"
    elif any(max(v) - min(v) > bounds["sample_span_seconds"] for v in samples.values()):
        reason = "CLOCK_SAMPLE_INSTABILITY"
    else:
        medians = [statistics.median(v) for v in samples.values()]
        correction = statistics.mean(medians)
        result["peer_dispersion_seconds"] = max(medians) - min(medians)
        result["correction_seconds"] = correction
        if result["peer_dispersion_seconds"] > bounds["peer_dispersion_seconds"]:
            reason = "CLOCK_PEER_DISAGREEMENT"
        elif abs(correction) > bounds["maximum_correction_seconds"]:
            reason = "CLOCK_CORRECTION_LIMIT"
        elif host_elapsed is not None and monotonic_elapsed is not None and abs(host_elapsed - monotonic_elapsed) > bounds["host_jump_seconds"]:
            reason = "HOST_CLOCK_CHANGED_DURING_CALIBRATION"
        else:
            anchor = {"monotonic": monotonic, "utc_epoch": host_epoch + correction,
                      "host_epoch": host_epoch, "correction_seconds": correction, "boot_id": boot_id}
            if prior_anchor and (str(prior_anchor["boot_id"]) != str(boot_id) or monotonic < prior_anchor["monotonic"]):
                reason = "CLOCK_BOOT_OR_MONOTONIC_MISMATCH"
            elif prior_anchor and abs(project(prior_anchor, monotonic) - anchor["utc_epoch"]) > bounds["continuity_seconds"]:
                reason = "CLOCK_REFRESH_CONTINUITY_HOLD"
            else:
                result.update(state="accepted", anchor=anchor)
    result["reason"] = reason
    return result


def _windows_samples(peer: str) -> list[float]:
    command = [str(Path(os.environ["SystemRoot"]) / "System32/w32tm.exe"), "/stripchart",
               "/computer:" + peer, "/samples:3", "/period:1", "/dataonly"]
    result = subprocess.run(command, capture_output=True, text=True, errors="replace", timeout=12,
                            check=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return [float(x.replace(",", ".")) for x in re.findall(r",\s*([+-]\d+[.,]\d+)s\s*$", result.stdout, re.MULTILINE)]


def calibrate_windows(*, boot_id, prior_anchor: dict | None = None,
                      peers=("time.windows.com", "time.cloudflare.com")) -> dict:
    """Coordinator-only bounded measurements; no model or venue requests."""
    raw, mono = RAW_TIME(), MONOTONIC()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            samples = dict(zip(peers, pool.map(_windows_samples, peers)))
        end_raw, end_mono = RAW_TIME(), MONOTONIC()
        return assess_calibration(samples, host_epoch=end_raw, monotonic=end_mono, boot_id=boot_id,
                                  prior_anchor=prior_anchor, host_elapsed=end_raw - raw,
                                  monotonic_elapsed=end_mono - mono)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return {"schema": "LANE_II_CLOCK_CONTROL_V1", "state": "held", "reason": "CLOCK_MEASUREMENT_FAILED",
                "error_type": type(exc).__name__, "observed_monotonic": MONOTONIC(), "boot_id": boot_id,
                "host_clock_modified": False, "bounds": dict(DEFAULT_BOUNDS), "peer_samples_seconds": {}}


class ClockGate:
    def __init__(self, control_path, *, monotonic=MONOTONIC, raw_time=RAW_TIME, expected_boot=None):
        self.path = Path(control_path)
        self.monotonic, self.raw_time = monotonic, raw_time
        self.expected_boot = expected_boot
        self.anchor = None
        self.reason = "CLOCK_NOT_LOADED"
        self.last_read = float("-inf")
        self.last_now = float("-inf")
        self.bounds = dict(DEFAULT_BOUNDS)
        self.revisions = 0
        self._lock = threading.RLock()

    def refresh(self, force=False):
        with self._lock:
            now = self.monotonic()
            if not force and now - self.last_read < .5:
                return
            self.last_read = now
            try:
                if self.path.stat().st_size > 65536:
                    raise ValueError("CLOCK_CONTROL_SIZE")
                state = json.loads(self.path.read_bytes())
                if state.get("schema") != "LANE_II_CLOCK_CONTROL_V1" or state.get("state") != "accepted":
                    self.reason = state.get("reason") or "CLOCK_CONTROL_HELD"
                    return
                anchor = state["anchor"]
                for key in ("monotonic", "utc_epoch", "host_epoch", "correction_seconds"):
                    if not isinstance(anchor[key], (int, float)) or isinstance(anchor[key], bool) or not math.isfinite(anchor[key]):
                        raise ValueError("CLOCK_ANCHOR_NONFINITE")
                if self.expected_boot is not None and str(anchor.get("boot_id")) != str(self.expected_boot):
                    raise ValueError("CLOCK_BOOT_MISMATCH")
                if not 0 <= now - anchor["monotonic"] <= self.bounds["maximum_age_seconds"]:
                    raise ValueError("CLOCK_ANCHOR_STALE")
                if abs(anchor["correction_seconds"]) > self.bounds["maximum_correction_seconds"] or abs(anchor["utc_epoch"] - anchor["host_epoch"] - anchor["correction_seconds"]) > 1e-6:
                    raise ValueError("CLOCK_ANCHOR_CORRECTION_INVALID")
                if self.anchor and anchor != self.anchor:
                    if anchor["boot_id"] != self.anchor["boot_id"] or anchor["monotonic"] < self.anchor["monotonic"]:
                        raise ValueError("CLOCK_REFRESH_IDENTITY")
                    if abs(project(anchor, now) - project(self.anchor, now)) > self.bounds["continuity_seconds"]:
                        raise ValueError("CLOCK_REFRESH_CONTINUITY_HOLD")
                if anchor != self.anchor:
                    self.revisions += 1
                self.anchor = anchor
                if self.expected_boot is None:
                    self.expected_boot = anchor["boot_id"]
                self.reason = None
            except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as exc:
                self.reason = str(exc) if isinstance(exc, ValueError) else "CLOCK_CONTROL_UNAVAILABLE"

    def decision_allowed(self) -> bool:
        self.refresh()
        return self.anchor is not None and self.reason is None and 0 <= self.monotonic() - self.anchor["monotonic"] <= self.bounds["maximum_age_seconds"]

    def now(self) -> float:
        self.refresh()
        with self._lock:
            value = project(self.anchor, self.monotonic()) if self.anchor else self.raw_time()
            # An accepted sub-200 ms correction cannot reverse event timestamps.
            self.last_now = max(self.last_now, value)
            return self.last_now

    def status(self) -> dict:
        allowed = self.decision_allowed()
        return {"state": "accepted" if allowed else "held", "reason": self.reason if self.reason or allowed else "CLOCK_ANCHOR_STALE",
                "anchor_age_seconds": None if not self.anchor else self.monotonic() - self.anchor["monotonic"],
                "anchor": self.anchor, "accepted_revisions": self.revisions, "host_clock_modified": False}


_GATE = None


def install(control_path=None) -> ClockGate:
    global _GATE
    if _GATE is not None:
        return _GATE
    path = control_path or os.environ.get("LANE_II_CLOCK_CONTROL")
    if not path:
        raise ValueError("CLOCK_CONTROL_REQUIRED")
    gate = ClockGate(path, expected_boot=os.environ.get("LANE_II_BOOT_ID"))
    gate.refresh(force=True)
    if not gate.decision_allowed():
        raise ValueError("CLOCK_STARTUP_HOLD:" + str(gate.reason))
    _GATE = gate
    time.time = gate.now
    time.time_ns = lambda: round(gate.now() * 1_000_000_000)
    return gate


def decision_allowed() -> bool:
    return _GATE is not None and _GATE.decision_allowed()


def status() -> dict:
    return {"state": "held", "reason": "CLOCK_NOT_INSTALLED"} if _GATE is None else _GATE.status()
