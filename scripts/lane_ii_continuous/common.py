"""Small durable-file and identity utilities shared by the coordinator."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid


def utc(epoch=None):
    return datetime.fromtimestamp(time.time() if epoch is None else epoch, timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def binding(path):
    p = Path(path).resolve()
    return {"path": str(p), "bytes": p.stat().st_size, "sha256": file_hash(p)}


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def atomic_bytes(path, content, *, retries=6):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + "." + uuid.uuid4().hex + ".pending")
    with tmp.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(retries + 1):
        try:
            os.replace(tmp, p)
            return
        except PermissionError:
            if attempt == retries:
                # Retain the durable pending file for diagnosis/recovery.
                raise
            time.sleep(min(.025 * 2**attempt, .4))


def atomic_json(path, value):
    atomic_bytes(path, canonical(value) + b"\n")


def process_identity(pid=None):
    import psutil
    proc = psutil.Process(os.getpid() if pid is None else pid)
    executable = Path(proc.exe())
    return {"pid": proc.pid, "creation_epoch": proc.create_time(), "observed_utc":utc(),
            "observed_monotonic":time.monotonic(),
            "boot_epoch": psutil.boot_time(), "executable": str(executable),
            "executable_sha256": file_hash(executable)}


BOOT_EPOCH_TOLERANCE_SECONDS = 2.0


def fresh_coordinator_clock(clock, *, boot_epoch=None, monotonic=None):
    """Check a serialized coordinator anchor in a separate worker process."""
    import math
    import psutil
    boot_epoch = psutil.boot_time() if boot_epoch is None else boot_epoch
    monotonic = time.monotonic() if monotonic is None else monotonic
    try:
        anchor = clock['anchor']
        recorded_boot = clock['boot_id']
        values = (recorded_boot, boot_epoch, anchor['monotonic'], monotonic, anchor['utc_epoch'])
        return (clock['schema'] == 'LANE_II_CLOCK_CONTROL_V1' and clock['state'] == 'accepted'
                and all(type(v) in {int, float} and math.isfinite(v) for v in values)
                and anchor['boot_id'] == recorded_boot
                and abs(recorded_boot - boot_epoch) <= BOOT_EPOCH_TOLERANCE_SECONDS
                and 0 <= monotonic - anchor['monotonic'] <= 600)
    except (KeyError, TypeError):
        return False


def same_process(identity):
    import psutil
    try:
        if abs(psutil.boot_time() - identity["boot_epoch"]) > BOOT_EPOCH_TOLERANCE_SECONDS:
            return False
        proc = psutil.Process(identity["pid"])
        return (abs(proc.create_time() - identity["creation_epoch"]) < .01
                and os.path.normcase(proc.exe()) == os.path.normcase(identity["executable"]))
    except psutil.NoSuchProcess:
        return False
    except (KeyError, TypeError, psutil.AccessDenied):
        return None


@contextmanager
def application_lock(path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a+b") as stream:
        if p.stat().st_size == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            stream.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)
