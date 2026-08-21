"""Reproducible, read-only L3-F2 NinjaTrader commissioning receiver.

Run ``python -m src.l3f_provider.ninjatrader_commission --duration-seconds 60``
before activating the already-installed read-only AddOn/market observer.  The
program only accepts local observation frames; it never sends a byte to
NinjaTrader and prints no payload, identifier, or secret.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import select
import socket
import sys
import time
from typing import Iterable

from .ninjatrader_observation import (
    AccountClass, LOOPBACK_HOST, LoopbackBridgeConfig, LoopbackNinjaTraderBridge,
    NinjaTraderHealthStream, NinjaTraderHealthTracker, NinjaTraderObservation,
    NinjaTraderObservationError,
)
from .tradovate_observation import StreamHealth


@dataclass
class CommissioningSummary:
    listener_ready: bool = False
    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    observation_types: dict[str, int] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)
    error_codes: dict[str, int] = field(default_factory=dict)
    error_details: dict[str, int] = field(default_factory=dict)
    snapshot_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    health: dict[str, str] = field(default_factory=dict)
    local_bridge_transitions: list[str] = field(default_factory=list)

    def accept(self, observation: NinjaTraderObservation | None) -> None:
        if observation is None:
            self.duplicates += 1
            return
        self.accepted += 1
        self.observation_types[observation.observation_type] = self.observation_types.get(observation.observation_type, 0) + 1
        if observation.account_alias is not None and observation.account_class is not None:
            self.aliases[observation.account_alias] = observation.account_class.value
        if observation.observation_type == "SNAPSHOT_COMPLETE" and observation.account_alias is not None:
            scope = observation.payload.get("scope")
            count_key = "open_count" if scope == "POSITION" else "working_count" if scope == "ORDER" else None
            count = None if count_key is None else observation.payload.get(count_key)
            if type(count) is int and count >= 0:
                self.snapshot_counts.setdefault(observation.account_alias, {})[str(scope)] = count

    def reject(self, error: NinjaTraderObservationError) -> None:
        self.rejected += 1
        self.error_codes[error.code.value] = self.error_codes.get(error.code.value, 0) + 1
        detail = error.detail or "unspecified"
        self.error_details[detail] = self.error_details.get(detail, 0) + 1

    def set_health(self, tracker: NinjaTraderHealthTracker) -> None:
        snapshot = tracker.snapshot()
        self.health = {
            stream.value: snapshot.streams[stream].value
            for stream in NinjaTraderHealthStream
        }
        local_bridge = self.health[NinjaTraderHealthStream.LOCAL_BRIDGE.value]
        if not self.local_bridge_transitions or self.local_bridge_transitions[-1] != local_bridge:
            self.local_bridge_transitions.append(local_bridge)

    def safe_report(self) -> dict[str, object]:
        types = set(self.observation_types)
        lucid = self.aliases.get("Lucid25kflex01") == AccountClass.PROVIDER_EVALUATION.value
        sim = self.aliases.get("Sim101") == AccountClass.LOCAL_SIMULATION.value
        lucid_snapshots = self.snapshot_counts.get("Lucid25kflex01", {})
        position_count = lucid_snapshots.get("POSITION")
        order_count = lucid_snapshots.get("ORDER")
        return {
            "schema": "lane-iii-phase-f3-ninjatrader-commissioning-v1",
            "listener": {"host": LOOPBACK_HOST, "ready": self.listener_ready},
            "accepted_observations": self.accepted,
            "rejected_observations": self.rejected,
            "duplicate_observations": self.duplicates,
            "observation_types": dict(sorted(self.observation_types.items())),
            "account_binding": {"lucid_alias_identified": lucid, "sim101_identified_separately": sim},
            "market_data": {"quotes": "QUOTE" in types, "trades": "TRADE" in types, "depth": "DEPTH" in types},
            "state": {
                "account": "ACCOUNT" in types,
                "position": "POSITION" in types,
                "orders": "ORDER" in types,
                "executions": "EXECUTION" in types,
                "lucid_position_truth": "UNAVAILABLE" if position_count is None else "FLAT_CONFIRMED" if position_count == 0 else "OPEN_POSITION_OBSERVED",
                "lucid_working_order_truth": "UNAVAILABLE" if order_count is None else "NONE_WORKING_CONFIRMED" if order_count == 0 else "WORKING_ORDER_OBSERVED",
            },
            # This is provider transport health, never strategy or position
            # state. In particular, healthy quotes cannot make account state
            # healthy, and a closed local socket cannot imply FLAT.
            "provider_health": {
                "streams": dict(sorted(self.health.items())),
                "local_bridge_transitions": list(self.local_bridge_transitions),
            },
            "errors": dict(sorted(self.error_codes.items())),
            "error_details": dict(sorted(self.error_details.items())),
            "authority": "OBSERVE_ONLY",
        }


class NinjaTraderCommissioningHarness:
    def __init__(self, config: LoopbackBridgeConfig = LoopbackBridgeConfig()) -> None:
        self.bridge = LoopbackNinjaTraderBridge(config)
        self.summary = CommissioningSummary()
        self.health = NinjaTraderHealthTracker()
        self.summary.set_health(self.health)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _mark(self, stream: NinjaTraderHealthStream, state: StreamHealth, at: str | None = None) -> None:
        self.health.mark(stream, state, at or self._now())
        self.summary.set_health(self.health)

    def _record_observation_health(self, observation: NinjaTraderObservation) -> None:
        at = observation.ninja_receipt_time
        # A decoded observation proves only the stream that carried it. These
        # independent marks deliberately do not infer account truth from MNQ.
        self._mark(NinjaTraderHealthStream.NINJATRADER_PROCESS, StreamHealth.HEALTHY, at)
        if observation.observation_type in {"TRADE", "QUOTE"}:
            self._mark(NinjaTraderHealthStream.MARKET_DATA_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "DEPTH":
            self._mark(NinjaTraderHealthStream.DEPTH_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "ACCOUNT":
            self._mark(NinjaTraderHealthStream.ACCOUNT_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "POSITION":
            self._mark(NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "ORDER":
            self._mark(NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "SNAPSHOT_COMPLETE":
            scope = observation.payload.get("scope")
            if scope == "POSITION":
                self._mark(NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY, at)
            elif scope == "ORDER":
                self._mark(NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY, at)
        elif (
            observation.observation_type == "CONNECTION"
            and observation.account_alias == "Lucid25kflex01"
            and observation.payload.get("state") == "ACCOUNT_BOUND"
        ):
            self._mark(NinjaTraderHealthStream.LUCID_CONNECTION, StreamHealth.HEALTHY, at)

    def ingest(self, frames: Iterable[bytes]) -> CommissioningSummary:
        self.summary.listener_ready = True
        for frame in frames:
            try:
                observation = self.bridge.accept_observation(frame)
                self.summary.accept(observation)
                if observation is not None:
                    self._record_observation_health(observation)
            except NinjaTraderObservationError as error:
                self.summary.reject(error)
        return self.summary

    def run(self, duration_seconds: float) -> CommissioningSummary:
        if duration_seconds <= 0:
            raise ValueError("Commissioning duration must be positive.")
        listener = self.bridge.open_listener()
        listener.setblocking(False)
        self.summary.listener_ready = True
        self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.CONNECTING)
        deadline = time.monotonic() + duration_seconds
        connections: dict[socket.socket, bytearray] = {}

        def close_connection(connection: socket.socket) -> None:
            data = connections.pop(connection, bytearray())
            if data:
                self.ingest((bytes(data),))
            try:
                connection.close()
            finally:
                # A peer disconnect is an explicit transport condition. It
                # does not mutate account/position/order reconciliation.
                self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)

        try:
            while time.monotonic() < deadline:
                readable = [listener, *connections]
                ready, _, _ = select.select(readable, [], [], min(0.25, deadline - time.monotonic()))
                if not ready:
                    continue
                for source in ready:
                    if source is listener:
                        while True:
                            try:
                                connection, remote = listener.accept()
                            except BlockingIOError:
                                # A local client may disconnect between select()
                                # and accept(). Keep the listener available.
                                break
                            if remote[0] != LOOPBACK_HOST:
                                connection.close()
                                continue
                            connection.setblocking(False)
                            connections[connection] = bytearray()
                            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.HEALTHY)
                        continue
                    try:
                        buffered = len(connections[source])
                        # Read at most the configured frame allowance plus one
                        # byte. This bounds even newline-free hostile input.
                        chunk = source.recv(min(4096, self.bridge.config.maximum_frame_bytes + 1 - buffered))
                    except BlockingIOError:
                        continue
                    if not chunk:
                        close_connection(source)
                        continue
                    data = connections[source]
                    data.extend(chunk)
                    while b"\n" in data:
                        frame, _, remainder = data.partition(b"\n")
                        connections[source] = data = bytearray(remainder)
                        self.ingest((bytes(frame),))
                    if len(data) > self.bridge.config.maximum_frame_bytes:
                        self.ingest((bytes(data),))
                        connections[source] = bytearray()
                        close_connection(source)
        finally:
            for connection in tuple(connections):
                close_connection(connection)
            listener.close()
            # Stopping the listener ends the observation transport even when
            # no client ever connected. It must not remain CONNECTING in a
            # persisted commissioning report.
            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)
        return self.summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only L3-F2 NinjaTrader commissioning receiver")
    parser.add_argument("--port", type=int, default=48135)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    try:
        harness = NinjaTraderCommissioningHarness(LoopbackBridgeConfig(port=args.port))
        summary = harness.run(args.duration_seconds)
    except (OSError, ValueError) as error:
        print(json.dumps({"schema": "lane-iii-phase-f3-ninjatrader-commissioning-v1", "listener": {"host": LOOPBACK_HOST, "ready": False}, "error": type(error).__name__}, sort_keys=True))
        return 2
    print(json.dumps(summary.safe_report(), sort_keys=True))
    return 0 if summary.accepted else 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
