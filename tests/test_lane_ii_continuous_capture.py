from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.lane_ii_continuous.capture import STARTUP_RELEASE, build_forward_command, prepare_candidate, read_terminal, request_stop, verify_candidate
from scripts.lane_ii_continuous.clock import ClockGate, assess_calibration
from scripts.lane_ii_continuous.transport import NetworkBoundary, RetryController, allowed, communicate_bounded


class FakeTime:
    def __init__(self):
        self.value = 100.
    def now(self):
        return self.value
    def wait(self, seconds):
        self.value += seconds


class CaptureTests(unittest.TestCase):
    @unittest.skipUnless(Path("N:/Beelzebub/runtime/lane-ii-prediction-markets/short-day-slim-3h-preparation-20261105/candidate-v2/study/SOURCE_MANIFEST.json").is_file(), "retained source fixture is local to Atlas")
    def test_actual_bound_roster_generation_stale_quotes_and_source_tamper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = prepare_candidate("N:/Beelzebub/runtime/lane-ii-prediction-markets/short-day-slim-3h-preparation-20261105/candidate-v2/study", root / "candidate")
            command = build_forward_command(manifest, root / "run", "2026-11-08T00:00:00Z", "2026-11-08T00:10:00Z")
            self.assertEqual(command[-1], "--startup-gate")
            # Import and construct local model objects only. Never release/start
            # the Study; no collector threads, sockets or helper requests run.
            code = "import sys;sys.path.insert(0," + repr(manifest["candidate_dir"]) + ");import study;from roster import profile_specs,make_profile,qualified_quotes;profiles=[make_profile(s,lambda *a:None) for s in profile_specs()];assert [(p.id,p.model['budget'],p.initial_cash) for p in profiles]==[('5m-P13',5.,3000.),('5m-P2',6.,3000.)];assert profiles[0].positions is not profiles[1].positions;assert qualified_quotes({'Up':{'received':90.,'source_timestamp':99.,'request_entry_wall':89.}},100.)=={};assert qualified_quotes({'Up':{'received':99.,'source_timestamp':90.,'request_entry_wall':98.}},100.)=={};print(study.verify_sources())"
            result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", code], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), manifest["source_manifest_sha256"])
            with (root / "candidate/optimized_controls.py").open("ab") as stream:
                stream.write(b"\n# changed\n")
            with self.assertRaisesRegex(ValueError, "DERIVED_SOURCE_CHANGED"):
                verify_candidate(manifest)

    def test_public_get_boundary_rejects_auth_models_mutations_and_redirect_targets(self):
        good = "https://clob.polymarket.com/book?token_id=123"
        self.assertTrue(allowed(good))
        for url in ("https://api.openai.com/v1/responses", "https://api.anthropic.com/v1/messages", "https://clob.polymarket.com/order", "https://clob.polymarket.com:443/book?token_id=123", "http://clob.polymarket.com/book?token_id=123", "https://key@clob.polymarket.com/book?token_id=123", good + "&token_id=4", good + "#fragment"):
            self.assertFalse(allowed(url), url)
        boundary = NetworkBoundary("helper")
        boundary.authorize_request(good, "GET", {"Accept": "application/json"})
        for url, method, headers in ((good, "POST", {}), (good, "GET", {"Authorization": "secret"}), ("https://api.openai.com/v1/models", "GET", {})):
            with self.assertRaises(PermissionError):
                boundary.authorize_request(url, method, headers)
        with self.assertRaises(PermissionError):
            boundary.audit("socket.connect", (None, ("1.1.1.1", 443)))
        with self.assertRaises(PermissionError):
            boundary.audit("socket.getaddrinfo", ("api.openai.com", 443))
        with self.assertRaises(PermissionError):
            boundary.audit("subprocess.Popen", (sys.executable, [sys.executable, "-c", "pass"]))
        self.assertEqual(boundary.status()["model_api_calls"], 0)
        self.assertEqual(boundary.status()["denied_network_actions"], 6)

    def test_actual_audit_hook_denies_provider_socket_without_network(self):
        # An isolated fixture owns the permanent Python audit hook; DNS is denied
        # before the OS resolver, so this cannot contact a provider.
        code = "from scripts.lane_ii_continuous.transport import install_boundary; import socket; b=install_boundary('helper');\ntry: socket.getaddrinfo('api.openai.com',443)\nexcept PermissionError: print(b.status()['denied_network_actions'])\nelse: raise SystemExit(3)"
        result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "1")

    def test_transient_attempts_preserve_clocks_raw_results_and_spacing(self):
        clock, events, begins = FakeTime(), [], []
        control = RetryController(monotonic=clock.now, wall=clock.now, jitter=lambda: .2)
        results = iter((503, 200))
        def fetch(*_):
            begins.append(clock.now())
            return {"status": next(results), "entry_mono": clock.now(), "entry_wall": 700., "end_wall": 701., "body_b64": "", "headers": {}}
        result = control.observe({"id": 1, "url": "fixture"}, fetch, live=lambda: True, wait=clock.wait,
                                 emit=lambda kind, data: events.append((kind, data, control.healthy)),
                                 reserve_attempt=lambda: None, cutoff=1000, deadline=1000, stop_path="unused")
        self.assertEqual(result, "OBSERVED")
        self.assertEqual([event[1]["job"]["id"] for event in events], [11, 12])
        self.assertEqual([event[1]["attempt_terminal"] for event in events], [False, True])
        self.assertFalse(events[0][2])
        self.assertTrue(events[1][2])
        self.assertGreaterEqual(begins[1] - begins[0], 1.2 - 1e-8)
        self.assertEqual(events[0][1]["response"]["entry_wall"], 700.)
        self.assertEqual(control.retry_count, 1)

    def test_retries_are_finite_and_cancelled_without_new_request(self):
        clock, events = FakeTime(), []
        control = RetryController(monotonic=clock.now, wall=clock.now, jitter=lambda: 0)
        def fail(*_):
            raise TimeoutError("fixture timeout")
        args = dict(live=lambda: True, wait=clock.wait, emit=lambda *x: events.append(x), reserve_attempt=lambda: None, cutoff=1000, deadline=1000, stop_path="unused")
        result = control.observe({"id": 3, "url": "fixture"}, fail, **args)
        self.assertEqual(result, "TRANSIENT_TRANSPORT_RETRIES_EXHAUSTED")
        self.assertEqual(len(events), 3)
        self.assertFalse(control.healthy)
        self.assertEqual(control.exhaustions, 1)
        args["live"] = lambda: False
        result = control.observe({"id": 4, "url": "fixture"}, fail, **args)
        self.assertEqual(result, "CANCELLED")
        self.assertEqual(len(events), 3)

    def test_venue_retry_after_is_not_shortened_to_retry_budget(self):
        clock = FakeTime()
        control = RetryController(monotonic=clock.now, wall=clock.now)
        result = control.observe({"id": 1, "url": "fixture"}, lambda *_: {"status": 429, "headers": {"retry-after": "120"}},
                                 live=lambda: True, wait=clock.wait, emit=lambda *_: None,
                                 reserve_attempt=lambda: None, cutoff=1000, deadline=1000, stop_path="unused")
        self.assertEqual(result, "VENUE_RETRY_AFTER_EXCEEDS_BUDGET")
        self.assertEqual(control.attempt_count, 1)

    def test_clock_hold_between_request_and_helper_start_holds_without_retry_storm(self):
        clock = FakeTime()
        control = RetryController(monotonic=clock.now, wall=clock.now)
        events = []
        def failed_start(*_):
            raise RuntimeError("HTTP_HELPER_EXIT:1:ValueError:CLOCK_STARTUP_HOLD:CLOCK_ANCHOR_STALE")
        result = control.observe({"id": 1, "url": "fixture"}, failed_start, live=lambda: True,
                                 wait=clock.wait, emit=lambda *event: events.append(event), reserve_attempt=lambda: None,
                                 cutoff=1000, deadline=1000, stop_path="unused")
        self.assertEqual(result, "CLOCK_ADMISSION_HELD")
        self.assertEqual(control.attempt_count, 1)
        self.assertTrue(events[0][1]["attempt_terminal"])
        self.assertFalse(control.healthy)

    def test_owned_helper_stop_is_polled_and_closes_only_its_job(self):
        clock = FakeTime()
        class Job:
            closed = 0
            def close(self): self.closed += 1
        class Process:
            calls = 0
            waited = False
            def communicate(self, payload, timeout):
                self.calls += 1
                clock.wait(timeout)
                raise subprocess.TimeoutExpired("fixture", timeout)
            def wait(self, timeout): self.waited = True
        process, job = Process(), Job()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TimeoutError):
                communicate_bounded(process, b"payload", cutoff=1000, deadline=1000, stop_path=Path(tmp) / "STOP", job=job, maximum_seconds=.3, monotonic=clock.now, wall=clock.now)
        self.assertEqual(job.closed, 1)
        self.assertTrue(process.waited)
        self.assertLessEqual(process.calls, 4)
        process, job = Process(), Job()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(InterruptedError):
                communicate_bounded(process, b"payload", cutoff=1000, deadline=1000, stop_path=Path(tmp) / "STOP", job=job, monotonic=clock.now, wall=clock.now, live_check=lambda: False)
        self.assertEqual(process.calls, 0)
        self.assertEqual(job.closed, 1)

    def test_observation_cutoff_is_cancellation_not_fresh_transport_failure(self):
        clock = FakeTime()
        control = RetryController(monotonic=clock.now, wall=clock.now)
        events = []
        result = control.observe({"id": 1, "url": "fixture"}, lambda *_: {"status": 0, "headers": {}, "error": "ValueError:STOP_OR_CUTOFF_BEFORE_HTTP"},
                                 live=lambda: True, wait=clock.wait, emit=lambda *event: events.append(event), reserve_attempt=lambda: None,
                                 cutoff=1000, deadline=1000, stop_path="unused")
        self.assertEqual(result, "CANCELLED")
        self.assertEqual(control.attempt_count, 1)
        self.assertEqual(events[0][0], "HTTP")
        self.assertTrue(events[0][1]["attempt_terminal"])

    def test_clock_refresh_preserves_projection_and_stale_hold(self):
        clock = FakeTime()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "CLOCK_CONTROL.json"
            state = assess_calibration({"one": [1., 1.01, .99], "two": [1., 1., 1.]}, host_epoch=1000., monotonic=100., boot_id="boot")
            path.write_text(json.dumps(state))
            gate = ClockGate(path, monotonic=clock.now, raw_time=lambda: 0, expected_boot="boot")
            self.assertTrue(gate.decision_allowed())
            first = gate.now()
            clock.wait(300.)
            refreshed = assess_calibration({"one": [1., 1., 1.], "two": [1., 1., 1.]}, host_epoch=1300.1, monotonic=400., boot_id="boot", prior_anchor=state["anchor"])
            path.write_text(json.dumps(refreshed))
            self.assertTrue(gate.decision_allowed())
            self.assertAlmostEqual(gate.now() - first, 300.1)
            clock.wait(601.)
            self.assertFalse(gate.decision_allowed())
            self.assertEqual(gate.status()["reason"], "CLOCK_ANCHOR_STALE")
            self.assertGreater(gate.now(), first)

    def test_clock_instability_and_discontinuous_or_reboot_refresh_hold(self):
        state = assess_calibration({"one": [0., .3, 0.], "two": [0., 0., 0.]}, host_epoch=1000., monotonic=100., boot_id="b")
        self.assertEqual(state["reason"], "CLOCK_SAMPLE_INSTABILITY")
        state = assess_calibration({"one": [1., 1., 1.], "two": [2., 2., 2.]}, host_epoch=1000., monotonic=100., boot_id="b")
        self.assertEqual(state["reason"], "CLOCK_PEER_DISAGREEMENT")
        prior = {"utc_epoch": 1001., "host_epoch": 1000., "correction_seconds": 1., "monotonic": 100., "boot_id": "b"}
        state = assess_calibration({"one": [2., 2., 2.], "two": [2., 2., 2.]}, host_epoch=1300., monotonic=400., boot_id="b", prior_anchor=prior)
        self.assertEqual(state["reason"], "CLOCK_REFRESH_CONTINUITY_HOLD")
        state = assess_calibration({"one": [1., 1., 1.], "two": [1., 1., 1.]}, host_epoch=1300., monotonic=400., boot_id="new", prior_anchor=prior)
        self.assertEqual(state["reason"], "CLOCK_BOOT_OR_MONOTONIC_MISMATCH")

    def test_terminal_adapter_never_turns_failed_capture_into_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = root / "capture"
            capture.mkdir()
            receipt = {"pid": 50, "source_manifest_sha256": "source", "state": "FAILED", "reason": "TRANSIENT_TRANSPORT_RETRIES_EXHAUSTED", "paper_start_utc": "2026-11-08T00:00:00Z", "actual_end_utc": "2026-11-08T00:15:00Z", "cutoff_utc": "2026-11-08T01:00:00Z", "profiles": []}
            (capture / "receipt.json").write_text(json.dumps(receipt))
            terminal = read_terminal(root)
            self.assertEqual(terminal["execution"]["state"], "failed")
            self.assertEqual(terminal["coverage"]["observed_seconds"], 900)
            self.assertFalse(terminal["closure"]["capture_sealed"])
            self.assertFalse(terminal["coverage"]["full_duration_qualified"])
            with self.assertRaises(ValueError):
                request_stop(root, "fixture")
            (capture / "preregistration.json").write_text("{}")
            stop = request_stop(root, "fixture")
            first = Path(stop).read_bytes()
            request_stop(root, "changed reason")
            self.assertEqual(Path(stop).read_bytes(), first)
        self.assertEqual(STARTUP_RELEASE, b"BTC_NEXT12_PAPER_START\n")


if __name__ == "__main__":
    unittest.main()
