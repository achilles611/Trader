"""Bounded public observation transport; no authentication or model endpoints.

The frozen runner imports this file as continuous_transport.  Its retry engine
retains every attempt and pauses simulated decisions during transient outages.
"""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import queue
import random
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

PUBLIC_HTTP_HOSTS = frozenset(("gamma-api.polymarket.com", "clob.polymarket.com", "api.exchange.coinbase.com"))
PUBLIC_WS_HOST = "ws-feed.exchange.coinbase.com"
PUBLIC_WS_URL = "wss://" + PUBLIC_WS_HOST
TRANSIENT_STATUS = frozenset((0, 408, 425, 429, 500, 502, 503, 504))
RAW_HOST_TIME = time.time


def allowed(url: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.fragment or parsed.netloc not in PUBLIC_HTTP_HOSTS:
            return False
        if parsed.hostname == "gamma-api.polymarket.com":
            return not parsed.query and re.fullmatch(r"/markets/slug/btc-updown-(?:5|15)m-\d+", parsed.path) is not None
        if parsed.hostname == "clob.polymarket.com":
            return (parsed.path == "/book" and set(query) == {"token_id"} and len(query["token_id"]) == 1 and query["token_id"][0].isascii() and query["token_id"][0].isdigit()) or (not parsed.query and re.fullmatch(r"/markets/0x[0-9a-fA-F]{64}", parsed.path) is not None)
        return parsed.path == "/products/BTC-USD/candles" and set(query) == {"granularity", "start", "end"} and query["granularity"] == ["60"] and all(len(query[key]) == 1 and 0 < len(query[key][0]) <= 40 for key in ("start", "end"))
    except (ValueError, TypeError, AttributeError):
        return False


class NetworkBoundary:
    """Allow only the frozen GET helper or exact Coinbase observation socket.

    DNS, socket connects, urllib requests and descendant starts are constrained.
    Redirects are disabled separately by the helper.  Proxy environment variables
    cannot extend this boundary.  A source verifier binds the code using it.
    """
    def __init__(self, role, helper_path=None):
        if role not in ("worker", "helper"):
            raise ValueError("NETWORK_ROLE")
        self.role = role
        self.helper_path = str(Path(helper_path).resolve()) if helper_path else None
        self.hosts = PUBLIC_HTTP_HOSTS if role == "helper" else frozenset((PUBLIC_WS_HOST,))
        self.addresses = set()
        self.counters = {"public_get_requests": 0, "public_socket_connects": 0, "owned_http_helper_starts": 0,
                         "denied_network_actions": 0, "model_api_calls": 0, "authenticated_account_calls": 0, "real_orders": 0}
        self.lock = threading.Lock()

    def deny(self, reason):
        with self.lock:
            self.counters["denied_network_actions"] += 1
        raise PermissionError("PUBLIC_OBSERVATION_BOUNDARY:" + reason)

    def validate_host(self, host, port):
        if host not in self.hosts or port not in (443, "443"):
            self.deny("DNS_HOST_OR_PORT")

    def authorize_request(self, url, method, headers=None):
        if self.role != "helper" or method != "GET" or not allowed(url):
            self.deny("HTTP_METHOD_OR_URL")
        if any(str(key).lower() not in ("user-agent", "accept", "host", "connection", "accept-encoding") for key in (headers or {})):
            self.deny("HTTP_HEADER")
        self.counters["public_get_requests"] += 1

    def audit(self, event, args):
        if event == "socket.getaddrinfo":
            self.validate_host(args[0], args[1])
        elif event in ("socket.gethostbyname", "socket.gethostbyaddr"):
            self.deny("UNBOUND_DNS")
        elif event == "socket.connect":
            address = args[1]
            if not isinstance(address, tuple) or len(address) < 2 or address[1] != 443 or address[0] not in self.addresses:
                self.deny("UNBOUND_SOCKET")
            self.counters["public_socket_connects"] += 1
        elif event == "socket.sendto":
            self.deny("DATAGRAMS_FORBIDDEN")
        elif event == "urllib.Request":
            self.authorize_request(args[0], args[3], args[2])
        elif event == "subprocess.Popen":
            if self.role != "worker" or not self.helper_path or len(args) != 4:
                self.deny("UNBOUND_CHILD")
            executable, argv, cwd, env = args
            expected = [sys.executable, "-I", "-S", "-B", self.helper_path, "net"]
            # CPython audits the serialized command line on Windows, but the
            # argument sequence on POSIX. Match its exact representation; do
            # not parse or accept alternative quoting or execution overrides.
            if os.name == "nt":
                bound_command = executable is None and argv == subprocess.list2cmdline(expected)
            else:
                bound_command = executable == sys.executable and isinstance(argv, (list, tuple)) and list(argv) == expected
            if not bound_command or cwd is not None or env is not None:
                self.deny("UNBOUND_CHILD")
            self.counters["owned_http_helper_starts"] += 1
        elif event in ("os.system", "os.posix_spawn", "os.spawn"):
            self.deny("UNBOUND_CHILD")

    def install(self):
        original = socket.getaddrinfo
        def bounded_resolution(host, port, *args, **kwargs):
            self.validate_host(host, port)
            result = original(host, port, *args, **kwargs)
            for row in result:
                address = row[4][0]
                if not ipaddress.ip_address(address).is_global:
                    self.deny("NONPUBLIC_RESOLUTION")
                self.addresses.add(address)
            return result
        socket.getaddrinfo = bounded_resolution
        sys.addaudithook(self.audit)
        return self

    def status(self):
        return {"schema": "LANE_II_NETWORK_BOUNDARY_V1", "role": self.role, "enforced": True,
                "http_method": "GET", "redirects": "DENIED", "permitted_hosts": sorted(self.hosts), **self.counters}


_BOUNDARY = None
_WORKER_LIVE = None


def install_boundary(role, helper_path=None):
    global _BOUNDARY
    if _BOUNDARY is None:
        _BOUNDARY = NetworkBoundary(role, helper_path).install()
    elif _BOUNDARY.role != role:
        raise ValueError("NETWORK_ROLE_ALREADY_INSTALLED")
    return _BOUNDARY


def boundary_status():
    return {"enforced": False, "reason": "NOT_INSTALLED"} if _BOUNDARY is None else _BOUNDARY.status()


def communicate_bounded(process, payload, *, cutoff, deadline, stop_path, job, maximum_seconds=8.,
                        monotonic=time.monotonic, wall=None, live_check=None):
    """Retain the Popen handle and stop only its owned helper on cancellation."""
    wall = wall or (lambda: time.time())
    live_check = live_check or _WORKER_LIVE or (lambda: True)
    finish = min(deadline, monotonic() + maximum_seconds)
    first = True
    while True:
        if not live_check() or Path(stop_path).exists() or wall() >= cutoff or monotonic() >= deadline:
            job.close()
            process.wait(timeout=2)
            raise InterruptedError("HTTP_STOP_OR_CUTOFF")
        remaining = finish - monotonic()
        if remaining <= 0:
            job.close()
            process.wait(timeout=2)
            raise TimeoutError("OWNED_HTTP_HELPER_EXCEEDED_TOTAL_BOUND")
        try:
            return process.communicate(payload if first else None, timeout=min(.1, remaining))
        except subprocess.TimeoutExpired:
            first = False


class RetryController:
    def __init__(self, *, attempts=3, total_seconds=30., spacing=.501, monotonic=time.monotonic,
                 wall=None, jitter=None):
        if not 1 <= attempts <= 3 or not 0 < total_seconds <= 30 or spacing < .501:
            raise ValueError("RETRY_BOUNDS")
        self.attempts, self.total_seconds, self.spacing = attempts, total_seconds, spacing
        self.monotonic, self.wall = monotonic, wall or (lambda: time.time())
        self.jitter = jitter or (lambda: random.SystemRandom().uniform(0, .25))
        self.next_start = 0.
        self.healthy = True
        self.attempt_count = self.retry_count = self.exhaustions = 0
        self.last_error = None

    def status(self):
        return {"state": "healthy" if self.healthy else "held", "attempts": self.attempt_count,
                "retries": self.retry_count, "exhaustions": self.exhaustions, "last_error": self.last_error,
                "maximum_attempts_per_request": self.attempts, "total_request_seconds": self.total_seconds,
                "minimum_entry_spacing_seconds": self.spacing}

    def observe(self, job, fetch, *, live, wait, emit, reserve_attempt, cutoff, deadline, stop_path):
        finish = min(deadline, self.monotonic() + self.total_seconds)
        for attempt in range(1, self.attempts + 1):
            while live() and self.monotonic() < self.next_start and self.monotonic() < finish:
                wait(min(.05, self.next_start - self.monotonic(), finish - self.monotonic()))
            if not live() or self.monotonic() >= finish:
                if attempt > 1:
                    self.exhaustions += 1
                    self.healthy = False
                return "CANCELLED" if not live() else "RETRY_BUDGET_EXHAUSTED"
            reserve_attempt()
            self.attempt_count += 1
            self.retry_count += int(attempt > 1)
            attempt_job = {**job, "id": job["id"] * 10 + attempt, "base_job_id": job["id"], "attempt": attempt}
            received = None
            try:
                response = fetch(job["url"], cutoff, finish, stop_path)
                entered = response.get("entry_mono")
                self.next_start = max(self.monotonic(), (entered if entered is not None else self.monotonic()) + self.spacing)
                transient = response["status"] in TRANSIENT_STATUS
                # Helper permission/validation failures are terminal, never retried.
                deterministic = response.get("error", "").startswith(("PermissionError:", "ValueError:"))
                if deterministic:
                    transient = False
                received = {"job": attempt_job, "response": response, "received_wall": self.wall(), "received_mono": self.monotonic(), "received_host_wall": RAW_HOST_TIME()}
                event = "HTTP"
                self.last_error = response.get("error") or ("HTTP_" + str(response["status"]) if transient else None)
                if response.get("error", "").startswith("ValueError:STOP_OR_CUTOFF_BEFORE_HTTP"):
                    emit(event, {**received, "attempt_terminal": True})
                    return "CANCELLED"
            except InterruptedError as exc:
                emit("HTTP_FAILED", {"job": attempt_job, "error": type(exc).__name__ + ":" + str(exc), "attempt_terminal": True})
                return "CANCELLED"
            except Exception as exc:
                if "CLOCK_STARTUP_HOLD:" in str(exc):
                    self.healthy = False
                    self.last_error = "CLOCK_ADMISSION_HELD"
                    emit("HTTP_FAILED", {"job": attempt_job, "error": self.last_error, "attempt_terminal": True})
                    return "CLOCK_ADMISSION_HELD"
                transient = isinstance(exc, (TimeoutError, ConnectionError, OSError)) and not isinstance(exc, PermissionError)
                deterministic = not transient
                self.next_start = self.monotonic() + self.spacing
                self.last_error = type(exc).__name__ + ":" + str(exc)
                received = {"job": attempt_job, "error": self.last_error}
                event = "HTTP_FAILED"
            terminal = not transient or attempt == self.attempts or self.monotonic() >= finish
            self.healthy = not transient and not deterministic
            emit(event, {**received, "attempt_terminal": terminal})
            if deterministic:
                return "DETERMINISTIC_TRANSPORT_FAILURE"
            if not transient:
                return "OBSERVED"
            if terminal:
                self.exhaustions += 1
                return "TRANSIENT_TRANSPORT_RETRIES_EXHAUSTED"
            delay = min(4., 2 ** (attempt - 1) + max(0., min(.25, self.jitter())))
            if received.get("response", {}).get("status") == 429:
                retry_after = received["response"].get("headers", {}).get("retry-after")
                try:
                    delay = max(delay, float(retry_after))
                except (ValueError, TypeError):
                    delay = max(delay, 5.)
                # If the venue asks for longer than this request's bounded lifetime,
                # stop the collector instead of violating its Retry-After pacing.
                if delay >= finish - self.monotonic():
                    self.exhaustions += 1
                    return "VENUE_RETRY_AFTER_EXCEEDS_BUDGET"
            self.next_start = max(self.next_start, self.monotonic() + delay)
        return "TRANSIENT_TRANSPORT_RETRIES_EXHAUSTED"


def http_receiver(study, fetch, policy):
    global _WORKER_LIVE
    _WORKER_LIVE = study.live
    retry = study.transport_control
    def reserve():
        if study.http_count >= policy["public_http_requests"]:
            raise RuntimeError("HTTP_REQUEST_BUDGET")
        study.http_count += 1
    while study.live():
        if not study.clock_ready():
            study.stop.wait(.1)
            continue
        try:
            _, _, job = study.requests.get(timeout=.1)
        except queue.Empty:
            continue
        result = retry.observe(job, fetch, live=study.live, wait=study.stop.wait, emit=study.put,
                               reserve_attempt=reserve, cutoff=study.cutoff, deadline=study.deadline,
                               stop_path=study.root / "STOP")
        if result not in ("OBSERVED", "CANCELLED", "CLOCK_ADMISSION_HELD"):
            study.fatal = result
            study.stop.set()
            return
