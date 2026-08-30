from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness, NinjaTraderListenerWorker
from src.l3f_provider.ninjatrader_observation import (
    LoopbackBridgeConfig, LoopbackNinjaTraderBridge, NinjaTraderObservation,
    NinjaTraderObservationError,
)


TIME = "2026-08-20T15:00:00Z"


def frame(kind: str, number: int, account=None, payload=None) -> bytes:
    return json.dumps({"schema": "lane-iii-phase-f2-ninjatrader-observation-v1", "observation_id": f"nt-{number}", "session_id": "session", "observation_type": kind, "ninja_receipt_time": TIME, "local_monotonic_sequence": number, "provider_timestamp": TIME, "provider_sequence": None, "exchange_timestamp": None, "account": account, "payload": payload or {"contract_id": "MNQ SEPT26"}}).encode()


class LaneIIIPhaseF3Tests(unittest.TestCase):
    @staticmethod
    def _ephemeral_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
            reservation.bind(("127.0.0.1", 0))
            return reservation.getsockname()[1]

    def _start_receiver(self, *, duration: float = 0.35, maximum_frame_bytes: int = 65536):
        port = self._ephemeral_port()
        harness = NinjaTraderCommissioningHarness(
            LoopbackBridgeConfig(port=port, maximum_frame_bytes=maximum_frame_bytes)
        )
        result: list = []
        failures: list[BaseException] = []

        def receive() -> None:
            try:
                result.append(harness.run(duration))
            except Exception as error:  # pragma: no cover - asserted below
                failures.append(error)

        thread = threading.Thread(target=receive, daemon=True)
        thread.start()
        deadline = time.monotonic() + 0.75
        while True:
            try:
                client = socket.create_connection(("127.0.0.1", port), timeout=0.25)
                return harness, result, failures, thread, client
            except (ConnectionRefusedError, TimeoutError):
                if time.monotonic() >= deadline:
                    self.fail("commissioning listener did not start")
                time.sleep(0.01)

    def _finish_receiver(self, result, failures, thread) -> None:
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())
        self.assertFalse(failures)
        self.assertEqual(len(result), 1)

    def test_harness_summarizes_native_schema_without_payload_or_account_id(self):
        harness = NinjaTraderCommissioningHarness()
        lucid = {"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"}
        result = harness.ingest((
            frame("ACCOUNT", 1, lucid),
            frame("ACCOUNT", 2, {"alias": "Sim101", "class": "LOCAL_SIMULATION"}),
            frame("QUOTE", 3), frame("TRADE", 4), frame("DEPTH", 5), frame("POSITION", 6, lucid), frame("ORDER", 7, lucid),
        ))
        report = result.safe_report()
        self.assertEqual(report["accepted_observations"], 7)
        self.assertTrue(report["account_binding"]["lucid_alias_identified"])
        self.assertTrue(report["account_binding"]["sim101_identified_separately"])
        self.assertTrue(report["market_data"]["quotes"])
        self.assertNotIn("MNQ SEPT26", json.dumps(report))

    def test_harness_counts_rejection_and_duplicate_without_raising(self):
        harness = NinjaTraderCommissioningHarness()
        first = frame("HEALTH", 1)
        result = harness.ingest((first, first, b'{"schema":"wrong"}'))
        self.assertEqual(result.accepted, 1)
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.rejected, 1)
        self.assertEqual(result.safe_report()["error_details"], {"bridge_schema": 1})

    def test_listener_status_tracks_latest_accepted_authentic_observation(self):
        worker = NinjaTraderListenerWorker()
        observation = LoopbackNinjaTraderBridge().accept_observation(frame("QUOTE", 1))
        self.assertIsNotNone(observation)
        worker._record_and_forward_observation(observation)
        status = worker.status().as_dict()
        self.assertEqual(status["accepted_observations"], 1)
        self.assertEqual(status["last_observation_at"], TIME)
        self.assertEqual(status["market_observer_state"], "ACTIVE")
        self.assertTrue(status["market_observer_active"])
        self.assertTrue(status["market_observer_level_one_received"])
        self.assertFalse(status["market_observer_depth_received"])
        self.assertEqual(status["observation_types"], {"QUOTE": 1})

    def test_listener_distinguishes_connection_events_from_active_market_observer(self):
        worker = NinjaTraderListenerWorker()
        connection = LoopbackNinjaTraderBridge().accept_observation(
            frame("ACCOUNT", 1, {"alias": "Sim101", "class": "LOCAL_SIMULATION"})
        )
        self.assertIsNotNone(connection)
        worker._record_and_forward_observation(connection)
        status = worker.status().as_dict()
        self.assertEqual(status["accepted_observations"], 1)
        self.assertEqual(status["market_observer_state"], "NOT_ACTIVE")
        self.assertFalse(status["market_observer_active"])
        self.assertFalse(status["market_observer_level_one_received"])

    def test_listener_surfaces_latest_read_only_cash_values_for_sim_and_lucid(self):
        worker = NinjaTraderListenerWorker()
        bridge = LoopbackNinjaTraderBridge()
        sim = bridge.accept_observation(frame(
            "ACCOUNT", 1, {"alias": "Sim101", "class": "LOCAL_SIMULATION"},
            {"item": "CashValue", "value": 100_123.45},
        ))
        lucid = bridge.accept_observation(frame(
            "ACCOUNT", 2, {"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"},
            {"item": "CashValue", "value": 24_987.65},
        ))
        assert sim is not None and lucid is not None
        worker._record_and_forward_observation(sim)
        worker._record_and_forward_observation(lucid)

        balances = worker.status().as_dict()["account_balances"]
        self.assertEqual(balances["Sim101"]["cash_value"], 100_123.45)
        self.assertEqual(balances["Lucid25kflex01"]["cash_value"], 24_987.65)
        self.assertEqual(balances["Sim101"]["cash_value_observed_at"], TIME)
        self.assertEqual(balances["Lucid25kflex01"]["account_class"], "PROVIDER_EVALUATION")

    def test_harness_reports_authoritative_empty_position_and_order_snapshots(self):
        account = {"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"}
        harness = NinjaTraderCommissioningHarness()
        report = harness.ingest((
            frame("SNAPSHOT_COMPLETE", 1, account, {"scope": "POSITION", "open_count": 0}),
            frame("SNAPSHOT_COMPLETE", 2, account, {"scope": "ORDER", "working_count": 0}),
        )).safe_report()
        self.assertEqual(report["state"]["lucid_position_truth"], "FLAT_CONFIRMED")
        self.assertEqual(report["state"]["lucid_working_order_truth"], "NONE_WORKING_CONFIRMED")

    def test_nonblocking_accept_race_does_not_end_commissioning(self):
        class Listener:
            closed = False

            def setblocking(self, value):
                self.nonblocking = value

            def accept(self):
                raise BlockingIOError()

            def close(self):
                self.closed = True

        listener = Listener()
        harness = NinjaTraderCommissioningHarness()
        with patch.object(harness.bridge, "open_listener", return_value=listener), \
             patch("src.l3f_provider.ninjatrader_commission.select.select", return_value=([listener], [], [])), \
             patch("src.l3f_provider.ninjatrader_commission.time.monotonic", side_effect=(0.0, 0.0, 0.0, 1.0)):
            result = harness.run(1.0)
        self.assertTrue(result.listener_ready)
        self.assertTrue(listener.closed)

    def test_nonblocking_receive_race_does_not_end_commissioning(self):
        class Connection:
            closed = False

            def setblocking(self, value):
                self.nonblocking = value

            def recv(self, _):
                raise BlockingIOError()

            def close(self):
                self.closed = True

        class Listener:
            closed = False

            def __init__(self):
                self.accepted = False

            def setblocking(self, value):
                self.nonblocking = value

            def accept(self):
                if self.accepted:
                    raise BlockingIOError()
                self.accepted = True
                return connection, ("127.0.0.1", 48135)

            def close(self):
                self.closed = True

        connection = Connection()
        listener = Listener()
        harness = NinjaTraderCommissioningHarness()
        with patch.object(harness.bridge, "open_listener", return_value=listener), \
             patch("src.l3f_provider.ninjatrader_commission.select.select", side_effect=(([listener], [], []), ([connection], [], []))), \
             patch("src.l3f_provider.ninjatrader_commission.time.monotonic", side_effect=(0.0, 0.0, 0.0, 0.0, 0.0, 1.0)):
            result = harness.run(1.0)
        self.assertTrue(result.listener_ready)
        self.assertTrue(connection.closed)

    def test_abrupt_peer_reset_is_local_and_does_not_end_listener(self):
        class Connection:
            closed = False

            def setblocking(self, value):
                self.nonblocking = value

            def recv(self, _):
                raise ConnectionResetError("peer reset")

            def close(self):
                self.closed = True

        class Listener:
            closed = False

            def __init__(self):
                self.accepted = False

            def setblocking(self, value):
                self.nonblocking = value

            def accept(self):
                if self.accepted:
                    raise BlockingIOError()
                self.accepted = True
                return connection, ("127.0.0.1", 48135)

            def close(self):
                self.closed = True

        connection = Connection()
        listener = Listener()
        harness = NinjaTraderCommissioningHarness()
        with patch.object(harness.bridge, "open_listener", return_value=listener), \
             patch("src.l3f_provider.ninjatrader_commission.select.select", side_effect=(([listener], [], []), ([connection], [], []))), \
             patch("src.l3f_provider.ninjatrader_commission.time.monotonic", side_effect=(0.0, 0.0, 0.0, 0.0, 0.0, 1.0)):
            result = harness.run(1.0)
        self.assertTrue(result.listener_ready)
        self.assertTrue(connection.closed)
        self.assertTrue(listener.closed)
        self.assertEqual(result.safe_report()["provider_health"]["streams"]["LOCAL_BRIDGE"], "DISCONNECTED")

    def test_receiver_ingests_multiple_frames_from_one_local_connection(self):
        _, result, failures, thread, client = self._start_receiver()
        try:
            client.sendall(frame("HEALTH", 1) + b"\n" + frame("HEALTH", 2) + b"\n")
        finally:
            client.close()
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 2)

    def test_receiver_ingests_account_channel_while_market_channel_remains_open(self):
        harness, result, failures, thread, market = self._start_receiver()
        try:
            market.sendall(frame("QUOTE", 1) + b"\n")
            account = {"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"}
            with socket.create_connection(("127.0.0.1", harness.bridge.config.port), timeout=0.25) as state:
                state.sendall(frame("ACCOUNT", 1, account) + b"\n")
            time.sleep(0.05)
        finally:
            market.close()
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 2)

    def test_receiver_binds_exact_loopback_and_emits_no_response(self):
        bridge = LoopbackNinjaTraderBridge(LoopbackBridgeConfig(port=self._ephemeral_port()))
        listener = bridge.open_listener()
        try:
            self.assertEqual(listener.getsockname()[0], "127.0.0.1")
        finally:
            listener.close()

        harness, result, failures, thread, client = self._start_receiver()
        try:
            client.sendall(frame("HEALTH", 1) + b"\n")
            client.shutdown(socket.SHUT_WR)
            client.settimeout(0.5)
            self.assertEqual(client.recv(1), b"")
        finally:
            client.close()
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 1)
        self.assertEqual(result[0].safe_report()["authority"], "OBSERVE_ONLY")

    def test_receiver_assembles_fragmented_frame_before_admission(self):
        _, result, failures, thread, client = self._start_receiver()
        try:
            encoded = frame("HEALTH", 1)
            client.sendall(encoded[:11])
            time.sleep(0.03)
            client.sendall(encoded[11:] + b"\n")
        finally:
            client.close()
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 1)
        self.assertEqual(result[0].rejected, 0)

    def test_receiver_fails_closed_for_bad_encoding_json_and_oversized_frame(self):
        _, result, failures, thread, client = self._start_receiver(maximum_frame_bytes=1024)
        try:
            client.sendall(b"\xff\n{\n" + frame("HEALTH", 1) + b"\n")
            time.sleep(0.03)
            # The bounded reader rejects immediately once a newline-free frame
            # exceeds the configured maximum; it does not retain the tail.
            client.sendall(b"x" * 1025 + b"\n")
        finally:
            client.close()
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 1)
        self.assertGreaterEqual(result[0].rejected, 3)
        self.assertIn("bridge_encoding", result[0].error_details)
        self.assertIn("bridge_json", result[0].error_details)
        self.assertIn("bridge_frame", result[0].error_details)

    def test_disconnect_mid_frame_is_rejected_and_only_transport_health_changes(self):
        harness, result, failures, thread, client = self._start_receiver()
        try:
            client.sendall(frame("HEALTH", 1)[:12])
        finally:
            client.close()
        self._finish_receiver(result, failures, thread)
        report = result[0].safe_report()
        self.assertEqual(result[0].accepted, 0)
        self.assertEqual(result[0].rejected, 1)
        self.assertEqual(report["provider_health"]["streams"]["LOCAL_BRIDGE"], "DISCONNECTED")
        self.assertEqual(report["provider_health"]["streams"]["ACCOUNT_STREAM"], "UNKNOWN")
        self.assertEqual(harness.summary.local_bridge_transitions[-1], "DISCONNECTED")

    def test_reconnect_returns_only_local_bridge_health_to_healthy(self):
        harness, result, failures, thread, first = self._start_receiver(duration=0.45)
        try:
            first.sendall(frame("HEALTH", 1) + b"\n")
        finally:
            first.close()
        time.sleep(0.04)
        with socket.create_connection(("127.0.0.1", harness.bridge.config.port), timeout=0.25) as second:
            second.sendall(frame("HEALTH", 2) + b"\n")
        self._finish_receiver(result, failures, thread)
        self.assertEqual(result[0].accepted, 2)
        transitions = result[0].safe_report()["provider_health"]["local_bridge_transitions"]
        self.assertEqual(transitions, ["UNKNOWN", "CONNECTING", "HEALTHY", "DISCONNECTED", "HEALTHY", "DISCONNECTED"])

    def test_receiver_shutdown_closes_open_client_and_marks_transport_disconnected(self):
        harness, result, failures, thread, client = self._start_receiver(duration=0.35)
        try:
            client.sendall(frame("HEALTH", 1) + b"\n")
            self._finish_receiver(result, failures, thread)
            client.settimeout(0.25)
            self.assertEqual(client.recv(1), b"")
        finally:
            client.close()
        self.assertEqual(result[0].safe_report()["provider_health"]["streams"]["LOCAL_BRIDGE"], "DISCONNECTED")

    def test_receiver_shutdown_without_client_is_disconnected_not_connecting(self):
        harness = NinjaTraderCommissioningHarness(LoopbackBridgeConfig(port=self._ephemeral_port()))
        result = harness.run(0.01)
        report = result.safe_report()
        self.assertEqual(report["provider_health"]["streams"]["LOCAL_BRIDGE"], "DISCONNECTED")
        self.assertEqual(
            report["provider_health"]["local_bridge_transitions"],
            ["UNKNOWN", "CONNECTING", "DISCONNECTED"],
        )

    def test_fastapi_lifespan_is_the_only_production_listener_entrypoint(self):
        root = Path(__file__).parents[1]
        self.assertNotIn("ninjatrader-observe", (root / "main.py").read_text(encoding="utf-8"))
        commission_source = (root / "src" / "l3f_provider" / "ninjatrader_commission.py").read_text(encoding="utf-8")
        self.assertNotIn("if __name__ ==", commission_source)
        self.assertNotIn("ArgumentParser", commission_source)

    def test_gui_runtime_starts_exactly_one_listener_and_releases_then_reacquires_port(self):
        port = 48135
        worker = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
        with tempfile.TemporaryDirectory() as directory:
            config = replace(CopyTradeConfig(), artifacts=replace(
                CopyTradeConfig().artifacts, database_path=Path(directory) / "copytrade.sqlite3",
            ))
            app = create_control_center_app(config, ninjatrader_listener_factory=lambda: worker)

            async def exercise_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    self.assertIs(app.state.ninjatrader_observer, worker)
                    self.assertEqual(worker.status().state, "LISTENING")
                    self.assertEqual(worker.status().port, port)
                    self.assertEqual(worker.status().start_attempts, 1)
                    with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                        pass
                    netstat = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, check=True)
                    self.assertRegex(
                        netstat.stdout,
                        r"127\.0\.0\.1:48135\s+\S+\s+LISTENING\s+\d+",
                    )
                    duplicate = worker.start()
                    self.assertEqual(duplicate.state, "LISTENING")
                    self.assertEqual(duplicate.start_attempts, 1)
                    self.assertEqual(
                        sum(thread.name == "NINJATRADER_OBSERVER" and thread.is_alive() for thread in threading.enumerate()),
                        1,
                    )

            asyncio.run(exercise_lifespan())
        self.assertEqual(worker.status().state, "STOPPED")
        self.assertFalse(any(thread.name == "NINJATRADER_OBSERVER" and thread.is_alive() for thread in threading.enumerate()))
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=0.1)
        netstat = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, check=True)
        self.assertNotRegex(netstat.stdout, r"127\.0\.0\.1:48135\s+\S+\s+LISTENING\s+\d+")

        restarted = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
        try:
            self.assertEqual(restarted.start().state, "LISTENING")
        finally:
            self.assertEqual(restarted.stop().state, "STOPPED")

    def test_duplicate_fastapi_lifespan_is_refused_without_second_worker(self):
        port = self._ephemeral_port()
        workers: list[NinjaTraderListenerWorker] = []

        def factory() -> NinjaTraderListenerWorker:
            worker = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
            workers.append(worker)
            return worker

        with tempfile.TemporaryDirectory() as directory:
            config = replace(CopyTradeConfig(), artifacts=replace(
                CopyTradeConfig().artifacts, database_path=Path(directory) / "copytrade.sqlite3",
            ))
            app = create_control_center_app(config, ninjatrader_listener_factory=factory)

            async def exercise_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    with self.assertRaisesRegex(RuntimeError, "duplicate FastAPI lifespan refused"):
                        async with app.router.lifespan_context(app):
                            pass
                    self.assertEqual(len(workers), 1)
                    self.assertEqual(workers[0].status().state, "LISTENING")

            asyncio.run(exercise_lifespan())
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0].status().state, "STOPPED")

    def test_ui_refreshes_and_websocket_reconnects_keep_one_listener_owner(self):
        port = self._ephemeral_port()
        workers: list[NinjaTraderListenerWorker] = []

        def factory() -> NinjaTraderListenerWorker:
            worker = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
            workers.append(worker)
            return worker

        with tempfile.TemporaryDirectory() as directory:
            config = replace(CopyTradeConfig(), artifacts=replace(
                CopyTradeConfig().artifacts, database_path=Path(directory) / "copytrade.sqlite3",
            ))
            app = create_control_center_app(config, ninjatrader_listener_factory=factory)

            class DisconnectingWebSocket:
                sent = 0

                async def accept(self) -> None:
                    return None

                async def send_json(self, _payload) -> None:
                    self.sent += 1

                async def receive_text(self) -> str:
                    from fastapi import WebSocketDisconnect
                    raise WebSocketDisconnect()

            async def exercise_ui_churn() -> None:
                health_endpoint = next(route.endpoint for route in app.routes if route.path == "/api/health")
                websocket_endpoint = next(route.endpoint for route in app.routes if route.path == "/ws")
                async with app.router.lifespan_context(app):
                    for _ in range(3):
                        response = await health_endpoint()
                        self.assertEqual(response["ninjatrader_observer"]["start_attempts"], 1)
                    for _ in range(2):
                        await websocket_endpoint(DisconnectingWebSocket())
                    self.assertEqual(len(workers), 1)
                    self.assertEqual(workers[0].status().state, "LISTENING")
                    self.assertEqual(workers[0].status().start_attempts, 1)

            asyncio.run(exercise_ui_churn())
        self.assertEqual(workers[0].status().state, "STOPPED")

    def test_gui_listener_never_reads_stdin_and_bind_collision_aborts_app_startup(self):
        port = 48135
        blocker = LoopbackNinjaTraderBridge(LoopbackBridgeConfig(port=port)).open_listener()
        worker = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port))
        commission_source = (Path(__file__).parents[1] / "src" / "l3f_provider" / "ninjatrader_commission.py").read_text(encoding="utf-8")
        self.assertNotIn("stdin", commission_source)

        with tempfile.TemporaryDirectory() as directory:
            config = replace(CopyTradeConfig(), artifacts=replace(
                CopyTradeConfig().artifacts, database_path=Path(directory) / "copytrade.sqlite3",
            ))
            app = create_control_center_app(config, ninjatrader_listener_factory=lambda: worker)

            async def attempt_startup() -> None:
                async with app.router.lifespan_context(app):
                    pass

            try:
                with self.assertLogs("src.l3f_provider.ninjatrader_commission", level="ERROR") as logs:
                    with self.assertRaisesRegex(RuntimeError, "NINJATRADER_OBSERVER startup failed at 127.0.0.1:48135"):
                        asyncio.run(attempt_startup())
                status = worker.status()
                self.assertIsNone(app.state.ninjatrader_observer)
                self.assertEqual(status.state, "FAILED")
                self.assertEqual(status.host, "127.0.0.1")
                self.assertEqual(status.port, port)
                self.assertEqual(status.start_attempts, 1)
                self.assertIsNotNone(status.error)
                self.assertIn("NINJATRADER_OBSERVER FAILED 127.0.0.1:48135", logs.output[0])
                self.assertEqual(blocker.getsockname(), ("127.0.0.1", port))
                self.assertFalse(any(thread.name == "NINJATRADER_OBSERVER" and thread.is_alive() for thread in threading.enumerate()))
            finally:
                worker.stop()
                blocker.close()

    def test_strict_wire_rejects_unknown_fields_duplicate_keys_bool_sequence_and_unknown_alias(self):
        base = json.loads(frame("HEALTH", 1))
        base["extra"] = "control"
        with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
            NinjaTraderObservation.from_wire(json.dumps(base))

        duplicate = frame("HEALTH", 1).decode().replace('"observation_id"', '"observation_id":"first","observation_id"', 1)
        with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
            NinjaTraderObservation.from_wire(duplicate)

        base = json.loads(frame("HEALTH", 1))
        base["local_monotonic_sequence"] = True
        with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
            NinjaTraderObservation.from_wire(json.dumps(base))

        base = json.loads(frame("ACCOUNT", 1, {"alias": "actual-lucid-id", "class": "PROVIDER_EVALUATION"}))
        with self.assertRaisesRegex(NinjaTraderObservationError, "ACCOUNT_NOT_FOUND"):
            NinjaTraderObservation.from_wire(json.dumps(base))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
