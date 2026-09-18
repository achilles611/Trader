"""Bounded pure-Pong CPU workers. No files written, transports or authority.

The host captures causal input state and alone applies ordered results. Workers
can only evaluate immutable already-authenticated inputs; they cannot dispatch
orders, advance risk, own a publisher, or certify an authentic hour.
"""
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import hashlib
import multiprocessing
from pathlib import Path

from src.lane_iii.contracts import canonical_hash
from .pong_microstructure import (
    ClassifiedFlowAdapter, ClassifiedFlowPolicy, ClassifiedTrade,
    DepthObservation, LiquidityClusterAdapter, LiquidityClusterPolicy,
    MicrostructureEvidenceMapper, MicrostructureEvidenceMappingPolicy,
    VolumeRegimeAdapter, VolumeRegimePolicy, VolumeWindow,
)

_ROOT = Path(__file__).parent
COMPUTE_BUILD_HASH = canonical_hash({
    name: hashlib.sha256((_ROOT/name).read_bytes()).hexdigest()
    for name in ('pong_compute.py', 'pong_microstructure.py', 'discernment.py')
})


@dataclass(frozen=True)
class PongWindowRequest:
    generation: str
    ordinal: int
    producer: tuple
    as_of: str
    window_start: str
    trades: tuple[ClassifiedTrade, ...]
    bids: tuple[DepthObservation, ...]
    asks: tuple[DepthObservation, ...]
    volume: VolumeWindow
    prior_volume: tuple[VolumeWindow, ...]
    continuity_valid: bool
    flow_policy: ClassifiedFlowPolicy
    liquidity_policy: LiquidityClusterPolicy
    volume_policy: VolumeRegimePolicy
    mapping_policy: MicrostructureEvidenceMappingPolicy


@dataclass(frozen=True)
class PongWindowResult:
    generation: str
    ordinal: int
    producer: tuple
    as_of: str
    build_hash: str
    blockers: tuple[str, ...]
    frame: tuple | None


def compute_identity() -> str:
    return COMPUTE_BUILD_HASH


def evaluate_window_batch(requests: tuple[PongWindowRequest, ...]) -> tuple[PongWindowResult, ...]:
    if type(requests) is not tuple or not 1 <= len(requests) <= 32:
        raise ValueError('PONG_COMPUTE_BATCH_BOUND_INVALID')
    # Pickle memoizes shared immutable event objects across these causal frames.
    # No frame is coalesced, sampled or evaluated at a later frame's as-of time.
    return tuple(evaluate_window(request) for request in requests)


def evaluate_window(request: PongWindowRequest) -> PongWindowResult:
    if type(request) is not PongWindowRequest:
        raise TypeError('PONG_COMPUTE_REQUEST_TYPE_INVALID')
    try:
        return _evaluate_window(request)
    except ValueError:
        # Preserve the synchronous adapter's explicit invalid-input outcome;
        # unexpected infrastructure failures still fail the Future and latch.
        return PongWindowResult(request.generation, request.ordinal, request.producer,
            request.as_of, COMPUTE_BUILD_HASH,
            ('PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:ValueError',), None)


def _evaluate_window(request: PongWindowRequest) -> PongWindowResult:
    flow = ClassifiedFlowAdapter(request.flow_policy).evaluate(request.trades,
        window_start=request.window_start, as_of=request.as_of,
        feed_continuity_valid=request.continuity_valid)
    liquidity = LiquidityClusterAdapter(request.liquidity_policy)
    bid = liquidity.evaluate(request.bids, as_of=request.as_of, feed_continuity_valid=request.continuity_valid)
    ask = liquidity.evaluate(request.asks, as_of=request.as_of, feed_continuity_valid=request.continuity_valid)
    volume = VolumeRegimeAdapter(request.volume_policy).evaluate(
        request.volume, request.prior_volume, as_of=request.as_of)
    frame = (flow, bid, ask, volume)
    # Invalid frames emit only their complete validation reasons. For valid
    # frames immutable artifact hashes are evaluated here and travel with the
    # results; the host assigns the actual serial Discernment revisions.
    mapped = MicrostructureEvidenceMapper(request.mapping_policy).map_frame(
        *frame, first_evidence_revision=1, include_blocked_artifacts=False)
    return PongWindowResult(request.generation, request.ordinal, request.producer,
        request.as_of, COMPUTE_BUILD_HASH, mapped.blockers, None if mapped.blockers else frame)


def open_compute_pool(workers: int) -> ProcessPoolExecutor:
    if type(workers) is not int or not 1 <= workers <= 3:
        raise ValueError('PONG_COMPUTE_WORKER_BOUND_INVALID')
    pool = ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn'))
    try:
        probes = [pool.submit(compute_identity) for _ in range(workers)]
        for probe in probes:
            if probe.result(timeout=20) != COMPUTE_BUILD_HASH:
                raise RuntimeError('PONG_COMPUTE_LOADED_IDENTITY_MISMATCH')
        return pool
    except BaseException:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
