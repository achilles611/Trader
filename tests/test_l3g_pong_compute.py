"""TEST ONLY: deterministic real host ordering and isolated spawned computation.

No synthetic inputs or certificates from these tests qualify market history.
"""
from concurrent.futures import Future
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from src.l3g_paper.pong_compute import (
    COMPUTE_BUILD_HASH, PongWindowRequest, PongWindowResult,
    evaluate_window, open_compute_pool,
)
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_microstructure import (
    ClassifiedFlowPolicy, LiquidityClusterPolicy, VolumeRegimePolicy,
    MicrostructureEvidenceMappingPolicy, MicrostructureEvidenceMapper,
)
from tests.test_l3g_pong_microstructure import (
    BASE, stamp, trade, depth, volume_window, AggressorSide, BookSide,
    PriceResponse, VolumeWindowKind, MicrostructureEvidenceMapperTests,
)
from tests.test_l3g_pong_production import runtime_snapshot, pending_collection


def request(ordinal=1):
    return PongWindowRequest('TEST_ONLY_GENERATION', ordinal, ('session', 'source', 'build'),
        stamp(300), stamp(296),
        tuple(trade(s, AggressorSide.BUY, p, namespace='long-flow')
              for s, p in zip(range(297, 301), ('100', '100', '100.25', '100.25'))),
        (depth(299, BookSide.BID, '100', namespace='long-absorb'),
         depth(300, BookSide.BID, '100', interaction='30', response=PriceResponse.REJECTION,
               namespace='long-absorb')),
        (depth(299, BookSide.ASK, '100', namespace='long-consume'),
         depth(300, BookSide.ASK, '70', matched='30', interaction='30',
               response=PriceResponse.ACCEPTANCE, namespace='long-consume')),
        volume_window('current-long', BASE, '100', kind=VolumeWindowKind.AS_OF),
        tuple(volume_window(f'prior-long-{days}', BASE-timedelta(days=days), '100')
              for days in (3, 2, 1)), True, ClassifiedFlowPolicy(), LiquidityClusterPolicy(),
        VolumeRegimePolicy(), MicrostructureEvidenceMappingPolicy())


class PongComputeTests(unittest.TestCase):
    def host(self, root):
        host = PongProductionHost(Path(root), selected_profile='BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2',
            expected_market_source_fingerprint='a'*64, runtime_snapshot=runtime_snapshot,
            collector_status=pending_collection, controller_dispatch=lambda *a: False,
            native_dispatch=lambda *a: self.fail('TEST ONLY cannot dispatch'))
        host._producer = request().producer
        host._compute_generation = request().generation
        return host

    def test_actual_spawned_workers_preserve_frozen_valid_result_and_local_hashes(self):
        job = request()
        # Populate process-salted caches before serialization; they must not travel.
        hash(job.trades[0]); hash(job.bids[0]); hash(job.trades[0].event)
        recovered = pickle.loads(pickle.dumps(job))
        self.assertNotIn('_immutable_hash', recovered.trades[0].__dict__)
        self.assertNotIn('_immutable_hash', recovered.trades[0].event.__dict__)
        self.assertNotIn('_immutable_hash', recovered.bids[0].__dict__)
        expected = evaluate_window(job)
        self.assertFalse(expected.blockers)
        baseline = MicrostructureEvidenceMapperTests.frame(BASE, long=True, revision=1)[1]
        self.assertEqual(MicrostructureEvidenceMapper().map_frame(
            *expected.frame, first_evidence_revision=1), baseline)
        pool = open_compute_pool(2)
        try:
            outputs = [pool.submit(evaluate_window, replace(job, ordinal=i)) for i in range(1, 7)]
            for i, future in enumerate(outputs, 1):
                actual = future.result(timeout=10)
                self.assertEqual(actual, replace(expected, ordinal=i))
                self.assertEqual(tuple(x.result_hash for x in actual.frame),
                                 tuple(x.result_hash for x in expected.frame))
        finally:
            pool.shutdown(wait=True)

    def test_paused_a_cannot_be_overtaken_by_completed_b_or_blocked_c(self):
        with TemporaryDirectory() as root:
            host = self.host(root)
            a, b = Future(), Future()
            host._compute_pool = Mock()
            host._compute_batch_limit = 1
            host._compute_pool.submit.side_effect = [a, b]
            at = BASE+timedelta(seconds=300)
            host._queue_compute(request(1), at)
            host._queue_compute(request(2), at)
            host._queue_compute(None, at, ('TEST_ONLY_C_BLOCKED',))
            b.set_result((evaluate_window(request(2)),))
            host._drain_compute()
            self.assertEqual(host._compute_applied, 0)
            self.assertIsNone(host._last_microstructure_at)
            a.set_result((evaluate_window(request(1)),))
            applied = []
            original = host._mapper.map_frame
            def apply(*args, **kwargs):
                applied.append(kwargs['first_evidence_revision'])
                return original(*args, **kwargs)
            with patch.object(host._mapper, 'map_frame', side_effect=apply):
                host._drain_compute()
            self.assertEqual(host._compute_applied, 3)
            self.assertEqual(len(applied), 2)
            self.assertGreater(applied[1], applied[0])
            self.assertEqual(host._microstructure_blockers, ('TEST_ONLY_C_BLOCKED',))
            self.assertFalse(host._compute_pending)

    def test_identity_stale_generation_worker_failure_timeout_and_overflow_deny(self):
        cases = ('ordinal', 'generation', 'producer', 'build_hash', 'as_of', 'failure', 'timeout')
        for case in cases:
            with self.subTest(case=case), TemporaryDirectory() as root:
                host = self.host(root)
                future = Future()
                host._compute_pool = Mock()
                host._compute_batch_limit = 1
                host._compute_pool.submit.return_value = future
                host._queue_compute(request(), BASE+timedelta(seconds=300))
                value = evaluate_window(request())
                if case == 'failure':
                    future.set_exception(ValueError('TEST ONLY worker exception'))
                elif case == 'timeout':
                    with patch('src.l3g_paper.pong_production.time.monotonic', return_value=time.monotonic()+16):
                        host._drain_compute()
                    future.set_result((value,))
                else:
                    bad = {'ordinal': 8, 'generation': 'STALE', 'producer': ('REPLACED',),
                           'build_hash': '0'*64, 'as_of': stamp(299)}[case]
                    future.set_result((replace(value, **{case: bad}),))
                host._drain_compute()
                self.assertTrue(host._input_error)
                self.assertEqual(host._compute_applied, 0)
                self.assertFalse(host.authority_status()['entry_authority_ready'])
        with TemporaryDirectory() as root:
            host = self.host(root)
            host._compute_limit = 2
            host._queue_compute(None, BASE, ('TEST_ONLY_A',))
            host._queue_compute(None, BASE, ('TEST_ONLY_B',))
            with self.assertRaisesRegex(RuntimeError, 'QUEUE_OVERFLOW'):
                host._queue_compute(None, BASE)
            self.assertEqual(len(host._compute_pending), 2)
            self.assertEqual(host._compute_submitted, 2)
            self.assertEqual(host._compute_refused, 1)

    def test_submission_failure_retains_prior_order_and_stopping_fences_activation(self):
        with TemporaryDirectory() as root:
            host = self.host(root)
            host._compute_pool = Mock()
            host._compute_batch_limit = 1
            host._compute_pool.submit.side_effect = RuntimeError('TEST ONLY unavailable pool')
            host._queue_compute(request(), BASE+timedelta(seconds=300))
            host._drain_compute()
            self.assertEqual(host._compute_submitted, 1)
            self.assertEqual(host._compute_refused, 1)
            self.assertEqual(host._input_error, 'PONG_COMPUTE_SUBMISSION_FAILED')
            host._stop.set()
            self.assertIn('PONG_HOST_STOPPING', host.authority_status()['blocking_reasons'])

    def test_stop_drains_accepted_work_without_starting_paper(self):
        with TemporaryDirectory() as root:
            host = self.host(root)
            host._queue_compute(None, BASE, ('TEST_ONLY_INVALID',))
            host.start()
            host.stop()
            self.assertFalse(host._compute_pending)
            self.assertEqual(host._compute_applied, 1)
            self.assertFalse(host.authority_status()['activation_candidate'])


if __name__ == '__main__':
    unittest.main()
