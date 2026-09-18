"""Offline-only pacing and host-wide exclusion. Never imports trading authority."""
from __future__ import annotations

import ctypes
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import struct
import tempfile
import threading
import time


LEASE_NAME = r"Global\BeezPongCanonicalPerformanceV2"
FRAME_HEADER = struct.Struct('<Q')
_FAILED_LEASES = []
_PROCESS_LEASE = threading.Lock()
RELEASE_NEW = 0
RELEASE_READY = 1
RELEASE_RUNNING = 2
RELEASE_COMPLETE = 3
RELEASE_ABORTED = 4
RELEASE_CAPACITY_EXHAUSTED = 5
RELEASE_FAILED = 6
RELEASE_PROGRESS_LOCK_TIMEOUT_SECONDS = 1.0


@contextmanager
def release_progress_guard(lock):
    """A dead publisher cannot turn a non-robust IPC lock into an endless wait."""
    if not lock.acquire(timeout=RELEASE_PROGRESS_LOCK_TIMEOUT_SECONDS):
        raise RuntimeError('RELEASE_PROGRESS_LOCK_TIMEOUT')
    try:
        yield
    finally:
        lock.release()


class OfflineWorkerShutdownUnproven(RuntimeError):
    retain_performance_lease = True


def process_identity():
    """Bind a PID to OS process creation time, not a wall-clock guess."""
    if os.name == 'nt':
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
        stamps = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(kernel.GetCurrentProcess(), *(ctypes.byref(x) for x in stamps)):
            raise ctypes.WinError(ctypes.get_last_error())
        creation = (stamps[0].dwHighDateTime << 32) | stamps[0].dwLowDateTime
        return dict(pid=os.getpid(), process_creation_filetime_100ns=creation)
    # The tested deployment is Windows. Linux supports the same exclusion in CI.
    creation = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
    return dict(pid=os.getpid(), process_creation_boot_ticks=int(creation))


def system_cpu_times():
    """Cheap aggregate counters for one-second load samples, no blocking probe."""
    if os.name == 'nt':
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.GetSystemTimes.argtypes = [ctypes.POINTER(wintypes.FILETIME)] * 3
        stamps = [wintypes.FILETIME() for _ in range(3)]
        if not kernel.GetSystemTimes(*(ctypes.byref(x) for x in stamps)):
            raise ctypes.WinError(ctypes.get_last_error())
        idle, kernel_time, user = [((x.dwHighDateTime << 32) | x.dwLowDateTime)/1e7 for x in stamps]
        return idle, kernel_time+user
    values = [int(x) for x in Path('/proc/stat').read_text().splitlines()[0].split()[1:]]
    return values[3]+values[4], sum(values[:8])


class BenchmarkLease:
    """Fail immediately on contention; OS releases ownership after process exit."""
    def __init__(self, run_id):
        self.run_id = str(run_id)
        self.handle = None
        self.file = None
        self.metadata = None
        self.process_owned = False

    def __enter__(self):
        if not _PROCESS_LEASE.acquire(blocking=False):
            raise RuntimeError('HOST_PERFORMANCE_LEASE_BUSY')
        self.process_owned = True
        try:
            return self._acquire()
        except BaseException:
            if self.process_owned:
                _PROCESS_LEASE.release()
                self.process_owned = False
            raise

    def _acquire(self):
        if os.name == 'nt':
            from ctypes import wintypes
            self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            self.kernel.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
            self.kernel.CreateMutexW.restype = wintypes.HANDLE
            self.kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            self.kernel.ReleaseMutex.argtypes = [wintypes.HANDLE]
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            self.handle = self.kernel.CreateMutexW(None, False, LEASE_NAME)
            if not self.handle:
                raise ctypes.WinError(ctypes.get_last_error())
            status = self.kernel.WaitForSingleObject(self.handle, 0)
            if status not in (0, 0x80):  # WAIT_OBJECT_0 / abandoned prior owner
                self.kernel.CloseHandle(self.handle)
                self.handle = None
                raise RuntimeError('HOST_PERFORMANCE_LEASE_BUSY')
            abandoned = status == 0x80
        else:
            import fcntl
            self.file = open(Path(tempfile.gettempdir())/'beez-pong-canonical-performance-v2.lock', 'a+b')
            try:
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.file.close()
                self.file = None
                raise RuntimeError('HOST_PERFORMANCE_LEASE_BUSY') from None
            abandoned = False
        try:
            self.metadata = dict(schema='pong-host-performance-lease-v2', name=LEASE_NAME,
                run_id=self.run_id, **process_identity(), recovered_abandoned_owner=abandoned,
                acquired_at_utc=datetime.now(timezone.utc).isoformat())
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc, traceback):
        if getattr(exc, 'retain_performance_lease', False):
            # A failed offline process must not let another benchmark start while
            # its non-daemon workers are still draining. OS exit owns final release.
            _FAILED_LEASES.append(self)
            return
        if self.handle is not None:
            self.kernel.ReleaseMutex(self.handle)
            self.kernel.CloseHandle(self.handle)
            self.handle = None
        if self.file is not None:
            self.file.close()
            self.file = None
        if self.process_owned:
            _PROCESS_LEASE.release()
            self.process_owned = False


def independent_sender(path, expected_hash, dues, connection, go, abort, epoch,
                       send_started, send_completed, delivered, cpu_ns):
    """Spawn-owned preloaded sender. Blocking IPC never masquerades as delivery."""
    cpu_start = time.process_time_ns()
    try:
        raw = Path(path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected_hash:
            raise ValueError('SENDER_CORPUS_DIGEST_CHANGED')
        frames = raw.splitlines()
        if len(frames) != len(dues):
            raise ValueError('SENDER_CORPUS_COUNT_CHANGED')
        connection.send_bytes(json.dumps(dict(status='READY', **process_identity(),
            preloaded_wire_bytes=sum(map(len, frames)), file_bytes=len(raw),
            preload_resident_bytes_lower_bound=len(raw)+sum(map(len, frames)))).encode())
        del raw
        while not go.wait(.05):
            if abort.is_set():
                return
        for index, frame in enumerate(frames):
            remaining = epoch.value + dues[index] - time.perf_counter()
            if remaining > 0 and abort.wait(remaining):
                break
            if abort.is_set():
                break
            send_started[index] = time.perf_counter()
            connection.send_bytes(FRAME_HEADER.pack(index) + frame)
            send_completed[index] = time.perf_counter()
            delivered.value = index + 1
        connection.send_bytes(b'')
    except (BrokenPipeError, EOFError, OSError):
        # Closing the owning receiver is the bounded graceful abort for a blocked send.
        if not abort.is_set():
            raise
    except BaseException as error:
        # Only startup errors use this message; after READY, process exit is failure.
        if not go.is_set():
            connection.send_bytes(json.dumps(dict(status='ERROR', error=type(error).__name__)).encode())
        raise
    finally:
        cpu_ns.value = time.process_time_ns() - cpu_start
        connection.close()


def independent_release_sender(path, expected_hash, dues, connection, go, abort,
                               epoch, release_started, release_completed, released,
                               admitted, state, progress_lock, cpu_ns,
                               outstanding_peaks, record_capacity, wire_byte_capacity):
    """Publish sealed preloaded inputs at deadlines without per-record IPC bytes.

    Both processes independently hash the same pinned file. Index i always means
    original frame i; there is no renumbering, recycling or forged wire content.
    The shared release high-watermark is published only after its timestamp.
    Outstanding bounds use authenticated admission, not just parent polling.
    """
    cpu_start = time.process_time_ns()
    ready_sent = False
    parent = multiprocessing.parent_process()
    def owner_stopped():
        return abort.is_set() or (parent is not None and not parent.is_alive())
    try:
        raw = Path(path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected_hash:
            raise ValueError('SENDER_CORPUS_DIGEST_CHANGED')
        frames = tuple(raw.splitlines())
        if len(frames) != len(dues):
            raise ValueError('SENDER_CORPUS_COUNT_CHANGED')
        if type(record_capacity) is not int or record_capacity <= 0:
            raise ValueError('RELEASE_RECORD_CAPACITY_INVALID')
        if type(wire_byte_capacity) is not int or wire_byte_capacity <= 0:
            raise ValueError('RELEASE_BYTE_CAPACITY_INVALID')
        prefix_bytes = [0]
        for frame in frames:
            prefix_bytes.append(prefix_bytes[-1]+len(frame))
        state.value = RELEASE_READY
        connection.send_bytes(json.dumps(dict(status='READY', **process_identity(),
            transport='PRELOADED_SEALED_CORPUS_INDEX_RELEASE_V3',
            frames_sha256=expected_hash, frame_count=len(frames),
            preloaded_wire_bytes=prefix_bytes[-1], file_bytes=len(raw),
            preload_resident_bytes_lower_bound=len(raw)+prefix_bytes[-1],
            outstanding_record_capacity=record_capacity,
            outstanding_wire_byte_capacity=wire_byte_capacity)).encode())
        ready_sent = True
        connection.close()  # No data or backpressure travels on this handshake pipe.
        del raw
        while not go.wait(.05):
            if owner_stopped():
                state.value = RELEASE_ABORTED
                return
        state.value = RELEASE_RUNNING
        for index in range(len(frames)):
            remaining = epoch.value+dues[index]-time.perf_counter()
            while remaining > 0:
                if owner_stopped():
                    state.value = RELEASE_ABORTED
                    return
                # Windows multiprocessing semaphore waits round sub-.5ms to
                # zero. A 2400/s deadline would busy-poll an entire CPU core.
                # Python3.12 time.sleep uses a high-resolution waitable timer;
                # bounded slices retain <=50ms owner/cancel polling.
                time.sleep(min(remaining, .05))
                remaining = epoch.value+dues[index]-time.perf_counter()
            if owner_stopped():
                state.value = RELEASE_ABORTED
                return
            release_started[index] = time.perf_counter()
            with release_progress_guard(progress_lock):
                consumed = admitted.value
                if not 0 <= consumed <= index:
                    raise ValueError('RELEASE_ADMISSION_WATERMARK_INVALID')
                if (index+1-consumed > record_capacity
                        or prefix_bytes[index+1]-prefix_bytes[consumed] > wire_byte_capacity):
                    # The next input was not released. Retain the exact prefix;
                    # no wait-for-capacity loop, drop, renumber or skipped frame.
                    state.value = RELEASE_CAPACITY_EXHAUSTED
                    return
                outstanding_peaks[0] = max(outstanding_peaks[0], index+1-consumed)
                outstanding_peaks[1] = max(outstanding_peaks[1], prefix_bytes[index+1]-prefix_bytes[consumed])
                release_completed[index] = time.perf_counter()
                released.value = index+1
        with release_progress_guard(progress_lock):
            state.value = RELEASE_COMPLETE
    except BaseException as error:
        state.value = RELEASE_FAILED
        if not ready_sent:
            connection.send_bytes(json.dumps(dict(status='ERROR', error=str(error))).encode())
        raise
    finally:
        cpu_ns.value = time.process_time_ns()-cpu_start
        connection.close()
