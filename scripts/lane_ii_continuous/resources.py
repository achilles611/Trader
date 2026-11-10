"""Measured admission, persistent resource guards, and explicit storage reserve."""
from __future__ import annotations

import math
import os
from pathlib import Path
import shutil
import time

GIB = 2**30
DEFAULT_LIMITS = {
    "system_free_floor_bytes": GIB,
    "runtime_free_floor_bytes": 80*GIB,
    "shutdown_closeout_reserve_bytes": 4*GIB,
    "wal_reserve_bytes": GIB,
    "analysis_scratch_bytes": 4*GIB,
    "ingest_upper_bytes_per_second": 256*1024,
    "retention_copy_factor": 2,
    "admission_ram_bytes": 4*GIB + GIB//2,
    "soft_ram_bytes": 4*GIB,
    "hard_ram_bytes": 2*GIB,
    "admission_cpu_p95": 70,
    "runtime_cpu_ceiling": 85,
    "runtime_cpu_persistence_seconds": 5,
    "soft_ram_persistence_seconds": 60,
    "owned_rss_bytes": GIB,
    "owned_processes": 8,
    "release_stable_seconds": 300,
}


def storage_required(remaining_seconds, limits=DEFAULT_LIMITS, observed_ingest_rate=None):
    if not math.isfinite(remaining_seconds) or remaining_seconds < 0:
        raise ValueError("Invalid remaining duration")
    rate = max(limits["ingest_upper_bytes_per_second"], observed_ingest_rate or 0)
    capture = math.ceil(rate * remaining_seconds * limits["retention_copy_factor"])
    return {"required_free_bytes": capture + sum(limits[k] for k in (
        "runtime_free_floor_bytes","shutdown_closeout_reserve_bytes","wal_reserve_bytes","analysis_scratch_bytes")),
        "capture_retention_bytes":capture,"ingest_bytes_per_second":rate,
        "remaining_seconds":remaining_seconds,"rate_basis":"max(recorded conservative bound, measured ingest)",
        "floor_unchanged":True}


def sample(runtime_root, *, cpu_interval=None, include_owned=True):
    import psutil
    if os.name == 'nt':
        drive = os.environ.get('SystemDrive')
        system = Path(drive + '\\' if drive else os.environ.get('SystemRoot', '')).anchor
        if not system or not Path(system).drive:
            raise ValueError('WINDOWS_SYSTEM_VOLUME_UNRESOLVED')
    else:
        system = '/'
    proc = psutil.Process()
    processes = [proc]
    if include_owned:
        processes += proc.children(recursive=True)
    rss=0
    identities=[]
    for process in processes:
        try:
            rss += process.memory_info().rss
            identities.append({"pid":process.pid,"creation_epoch":process.create_time()})
        except (psutil.NoSuchProcess,psutil.AccessDenied):
            pass
    return {"monotonic":time.monotonic(),"wall_epoch":time.time(),
            "runtime_root":str(Path(runtime_root).resolve()),
            "runtime_free_bytes":shutil.disk_usage(runtime_root).free,
            "system_free_bytes":shutil.disk_usage(system).free,
            "available_ram_bytes":psutil.virtual_memory().available,
            "cpu_percent":psutil.cpu_percent(interval=cpu_interval),
            "owned_rss_bytes":rss,"owned_process_count":len(identities),"owned_processes":identities}


def admission(samples, remaining_seconds, *, limits=DEFAULT_LIMITS, observed_ingest_rate=None, heavy=False):
    if not samples:
        return {"admitted":False,"reasons":["RESOURCE_MEASUREMENT_MISSING"]}
    required=storage_required(remaining_seconds,limits,observed_ingest_rate)
    cpus=sorted(s["cpu_percent"] for s in samples)
    cpu=cpus[max(0,math.ceil(.95*len(cpus))-1)]
    reasons=[]
    if min(s["system_free_bytes"] for s in samples) < limits["system_free_floor_bytes"]:
        reasons.append("SYSTEM_DISK_FLOOR")
    if min(s["runtime_free_bytes"] for s in samples) < required["required_free_bytes"]:
        reasons.append("RUNTIME_DISK_RESERVE")
    if min(s["available_ram_bytes"] for s in samples) < limits["admission_ram_bytes"]:
        reasons.append("RAM_ADMISSION")
    if cpu > limits["admission_cpu_p95"]:
        reasons.append("CPU_ADMISSION")
    if max(s["owned_rss_bytes"] for s in samples) > limits["owned_rss_bytes"]*.75:
        reasons.append("OWNED_RSS_HEADROOM")
    if max(s["owned_process_count"] for s in samples) >= limits["owned_processes"]:
        reasons.append("OWNED_PROCESS_HEADROOM")
    return {"admitted":not reasons,"reasons":reasons,"storage":required,"cpu_p95":cpu,
            "minimum_available_ram_bytes":min(s["available_ram_bytes"] for s in samples),
            "minimum_system_free_bytes":min(s["system_free_bytes"] for s in samples),
            "minimum_runtime_free_bytes":min(s["runtime_free_bytes"] for s in samples),
            "heavy":heavy,"limits":limits}


class Guard:
    def __init__(self,limits=None):
        self.limits=dict(DEFAULT_LIMITS if limits is None else limits)
        self.high_since=None
        self.low_since=None
        self.observed_ingest_high_water=0

    def check(self,observation,remaining_seconds,observed_ingest_rate=None):
        l=self.limits
        now=observation["monotonic"]
        self.observed_ingest_high_water=max(self.observed_ingest_high_water,observed_ingest_rate or 0)
        high=observation["cpu_percent"] > l["runtime_cpu_ceiling"]
        low=observation["available_ram_bytes"] < l["soft_ram_bytes"]
        self.high_since=(now if self.high_since is None else self.high_since) if high else None
        self.low_since=(now if self.low_since is None else self.low_since) if low else None
        if observation["available_ram_bytes"] < l["hard_ram_bytes"]: return "RAM_HARD_FLOOR"
        if observation["system_free_bytes"] < l["system_free_floor_bytes"]: return "SYSTEM_DISK_FLOOR"
        if observation["runtime_free_bytes"] < storage_required(remaining_seconds,l,self.observed_ingest_high_water)["required_free_bytes"]: return "RUNTIME_DISK_RESERVE"
        if observation["owned_rss_bytes"] > l["owned_rss_bytes"]: return "OWNED_RSS_CEILING"
        if observation["owned_process_count"] > l["owned_processes"]: return "OWNED_PROCESS_CEILING"
        if self.high_since is not None and now-self.high_since >= l["runtime_cpu_persistence_seconds"]: return "CPU_PERSISTENT"
        if self.low_since is not None and now-self.low_since >= l["soft_ram_persistence_seconds"]: return "RAM_PERSISTENT"
        return None
