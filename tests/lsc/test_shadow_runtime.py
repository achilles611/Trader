from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.lsc.codec import decode_observation, encode_context, encode_observation
from src.lsc.segment_store import SegmentLimits, SegmentStore
from src.lsc.shadow_runtime import (
    LSCShadowConfig,
    LSCShadowRuntime,
    LSCShadowRuntimeError,
)
from tests.lsc.test_codec import CANONICAL_CONTRACT, _payload, context, observation


class LSCShadowRuntimeTests(unittest.TestCase):
    def _config(self, root: Path, **changes: object) -> LSCShadowConfig:
        values: dict[str, object] = {
            "root": root,
            "journal_id": "lsc-shadow-test",
            "profile_id": "beelzebub_pong_discernment_v1",
            "native_contract_id": "MNQ DEC26",
            "canonical_contract_id": CANONICAL_CONTRACT,
            "queue_capacity": 16,
            "segment_limits": SegmentLimits(max_records=2),
            "maximum_queue_residence_seconds": 1.0,
        }
        values.update(changes)
        return LSCShadowConfig(**values)  # type: ignore[arg-type]

    def test_clean_mixed_stream_seals_commits_and_replays_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(root))
            self.assertFalse(runtime.start()["proof_valid"])
            expected = [observation(kind, sequence) for sequence, kind in enumerate(
                ("QUOTE", "TRADE", "DEPTH"), 1,
            )]
            self.assertEqual([runtime.submit(item) for item in expected], [True] * 3)

            status = runtime.close()

            self.assertTrue(status["proof_valid"])
            self.assertEqual(status["submitted"], 3)
            self.assertEqual(status["processed"], 3)
            self.assertEqual(status["sealed_segments"], 2)
            self.assertEqual(status["committed_segments"], 2)
            self.assertFalse((root / "runtime-state" / "active.json").exists())
            self.assertEqual(len(list((root / "runtime-state" / "completed").glob("*.json"))), 1)
            completion = json.loads(Path(str(status["completion_receipt"])).read_text("utf-8"))
            self.assertEqual(completion["state"], "COMPLETE")
            self.assertTrue(completion["proof_valid"])
            self.assertEqual(completion["segment_head"], status["segment_head"])
            self.assertEqual(completion["commitment_head"], status["commitment_head"])
            self.assertFalse(any(root.rglob("*.sqlite*")))

            store = SegmentStore(root, limits=self._config(root).segment_limits)
            chain = store.verify_chain()
            decoded = []
            for segment in chain:
                context_bytes = store.read_context(segment.segment_id)
                for event_index in range(segment.record_count):
                    stored = store.lookup(
                        segment.segment_id,
                        event_index,
                        segment.first_local_sequence + event_index,
                    )
                    decoded.append(decode_observation(stored.record_bytes, context_bytes))
            self.assertEqual(decoded, expected)

    def test_restart_recovers_committed_prefix_and_continues_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = LSCShadowRuntime(self._config(root))
            first.start()
            self.assertTrue(first.submit(observation("QUOTE", 1)))
            self.assertTrue(first.close()["proof_valid"])

            second = LSCShadowRuntime(self._config(root))
            start = second.start()
            self.assertEqual(start["recovered_segments"], 1)
            self.assertTrue(second.submit(observation("TRADE", 2)))
            status = second.close()
            self.assertTrue(status["proof_valid"])
            self.assertEqual(status["last_local_sequence"], 2)
            self.assertEqual(status["sealed_segments"], 2)

    def test_unclean_previous_process_is_held_before_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "runtime-state" / "active.json"
            marker.parent.mkdir(parents=True)
            marker.write_text('{"schema":"test","state":"ACTIVE"}\n', encoding="utf-8")

            runtime = LSCShadowRuntime(self._config(root))
            with self.assertRaisesRegex(
                LSCShadowRuntimeError, "LSC_SHADOW_UNCLEAN_PREVIOUS_RUN",
            ):
                runtime.start()
            self.assertTrue(marker.exists())

    def test_supported_unclean_recovery_preserves_failed_run_and_commits_sealed_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self._config(root)
            interrupted = LSCShadowRuntime(config)
            interrupted._started_at = "2026-09-23T00:00:00.000000Z"
            root.mkdir(parents=True, exist_ok=True)
            interrupted._write_active_run_marker()
            source = observation("QUOTE", 1)
            source_context = context(source)
            context_bytes = encode_context(source_context)
            store = SegmentStore(root, limits=config.segment_limits)
            builder = store.new_segment(context_bytes)
            builder.append(encode_observation(source, context_bytes))
            sealed = builder.seal()
            self.assertFalse((root / "shadow-commitments").exists())

            with patch("src.lsc.shadow_runtime._process_liveness", return_value=False):
                recovery = LSCShadowRuntime.recover_unclean(config)

            self.assertFalse(recovery["proof_valid"])
            self.assertTrue(recovery["possible_unsealed_tail_loss"])
            self.assertEqual(recovery["sealed_segments"], 1)
            self.assertEqual(recovery["committed_segments_before"], 0)
            self.assertEqual(recovery["recovered_commitment_segment_ids"], [sealed.segment_id])
            self.assertEqual(recovery["committed_segments_after"], 1)
            self.assertFalse((root / "runtime-state" / "active.json").exists())
            self.assertTrue(Path(str(recovery["recovery_receipt"])).is_file())
            self.assertTrue(Path(str(recovery["archived_active_marker"])).is_file())

            continued = LSCShadowRuntime(config)
            self.assertEqual(continued.start()["recovered_segments"], 1)
            self.assertTrue(continued.submit(observation("TRADE", 2)))
            status = continued.close()
            self.assertTrue(status["proof_valid"])
            self.assertEqual(status["last_local_sequence"], 2)

    def test_unclean_recovery_refuses_live_writer_process_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self._config(root)
            runtime = LSCShadowRuntime(config)
            runtime._started_at = "2026-09-23T00:00:00.000000Z"
            root.mkdir(parents=True, exist_ok=True)
            runtime._write_active_run_marker()
            marker = root / "runtime-state" / "active.json"
            before = marker.read_bytes()

            with self.assertRaisesRegex(
                LSCShadowRuntimeError, "LSC_SHADOW_ACTIVE_WRITER_PROCESS_ALIVE",
            ):
                LSCShadowRuntime.recover_unclean(config)

            self.assertEqual(marker.read_bytes(), before)
            self.assertFalse((root / "runtime-state" / "unclean-recovery").exists())

    def test_queue_overflow_latches_without_blocking_submitter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(root, queue_capacity=1))
            entered = threading.Event()
            release = threading.Event()
            original_append = runtime._append

            def blocked_append(item: object) -> None:
                entered.set()
                self.assertTrue(release.wait(2.0))
                original_append(item)  # type: ignore[arg-type]

            runtime._append = blocked_append  # type: ignore[method-assign]
            runtime.start()
            self.assertTrue(runtime.submit(observation("QUOTE", 1)))
            self.assertTrue(entered.wait(1.0))
            self.assertTrue(runtime.submit(observation("TRADE", 2)))
            started = time.perf_counter()
            self.assertFalse(runtime.submit(observation("DEPTH", 3)))
            elapsed = time.perf_counter() - started
            self.assertLess(elapsed, 0.05)
            release.set()

            status = runtime.close(timeout_seconds=2.0)
            self.assertFalse(status["proof_valid"])
            self.assertEqual(status["failure_reason"], "LSC_SHADOW_QUEUE_FULL")
            self.assertEqual(status["overflow"], 1)
            self.assertTrue(Path(str(status["failure_receipt"])).is_file())
            receipt = json.loads(Path(str(status["failure_receipt"])).read_text("utf-8"))
            self.assertFalse(receipt["proof_valid"])

    def test_worker_rejects_invalid_generation_without_raising_to_submitter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = _payload("QUOTE", 1)
            payload["source_generation"] = "7"
            invalid = observation("QUOTE", 1, payload=payload)
            runtime = LSCShadowRuntime(self._config(root))
            runtime.start()
            self.assertTrue(runtime.submit(invalid))
            status = runtime.close()
            self.assertFalse(status["proof_valid"])
            self.assertEqual(status["failure_reason"], "LSC_SHADOW_WORKER_FAILED")
            self.assertIn("SOURCE_GENERATION_INVALID", str(status["failure_detail"]))

    def test_queue_residence_limit_invalidates_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(
                root, maximum_queue_residence_seconds=0.01,
            ))
            original_append = runtime._append

            def delayed_append(item: object) -> None:
                time.sleep(0.03)
                original_append(item)  # type: ignore[arg-type]

            runtime._append = delayed_append  # type: ignore[method-assign]
            runtime.start()
            self.assertTrue(runtime.submit(observation("QUOTE", 1)))
            self.assertTrue(runtime.submit(observation("TRADE", 2)))
            status = runtime.close()
            self.assertFalse(status["proof_valid"])
            self.assertEqual(status["failure_reason"], "LSC_SHADOW_WORKER_FAILED")
            self.assertIn("QUEUE_RESIDENCE_EXCEEDED", str(status["failure_detail"]))

    def test_native_contract_substitution_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = _payload("QUOTE", 1)
            payload["contract_id"] = "MNQ SEP26"
            substituted = observation("QUOTE", 1, payload=payload)
            runtime = LSCShadowRuntime(self._config(root))
            runtime.start()
            self.assertTrue(runtime.submit(substituted))
            status = runtime.close()
            self.assertFalse(status["proof_valid"])
            self.assertIn("NATIVE_CONTRACT_MISMATCH", str(status["failure_detail"]))

    def test_close_is_idempotent_and_post_close_offer_does_not_change_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(root))
            runtime.start()
            self.assertTrue(runtime.submit(observation("QUOTE", 1)))
            first = runtime.close()
            self.assertTrue(first["proof_valid"])
            self.assertFalse(runtime.submit(observation("TRADE", 2)))
            second = runtime.close()
            self.assertEqual(second, first)
            self.assertTrue(second["proof_valid"])
            self.assertIsNone(second["failure_reason"])

    def test_submit_close_interlock_cannot_strand_an_accepted_item(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(root))
            runtime.start()
            entered = threading.Event()
            release = threading.Event()
            original_put = runtime._queue.put_nowait

            def gated_put(item: object) -> None:
                entered.set()
                self.assertTrue(release.wait(2.0))
                original_put(item)  # type: ignore[arg-type]

            runtime._queue.put_nowait = gated_put  # type: ignore[method-assign]
            accepted: list[bool] = []
            submitter = threading.Thread(
                target=lambda: accepted.append(runtime.submit(observation("QUOTE", 1))),
            )
            submitter.start()
            self.assertTrue(entered.wait(1.0))
            closed: list[dict[str, object]] = []
            closer = threading.Thread(target=lambda: closed.append(runtime.close()))
            closer.start()
            time.sleep(0.02)
            self.assertTrue(closer.is_alive())
            release.set()
            submitter.join(2.0)
            closer.join(2.0)
            self.assertEqual(accepted, [True])
            self.assertEqual(len(closed), 1)
            self.assertTrue(closed[0]["proof_valid"])
            self.assertEqual(closed[0]["submitted"], 1)
            self.assertEqual(closed[0]["processed"], 1)

    def test_non_passive_and_unverified_observations_never_enter_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = LSCShadowRuntime(self._config(root))
            runtime.start()
            non_passive = replace(observation("QUOTE", 1), observation_type="HEALTH")
            self.assertFalse(runtime.submit(non_passive))
            self.assertTrue(runtime.submit(replace(
                observation("QUOTE", 2), signature_verified=None,
            )))
            status = runtime.close()
            self.assertFalse(status["proof_valid"])
            self.assertEqual(status["non_passive_ignored"], 1)
            self.assertIn("verified", str(status["failure_detail"]).lower())


if __name__ == "__main__":
    unittest.main()
