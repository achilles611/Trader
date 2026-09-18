# Pong serial throughput profile — 2026-09-14

**Result: throughput remains blocking.** No deployment, NinjaTrader compilation,
profile switch, paper arm, order, or production ledger mutation was performed.
The candidate was restored after two exploratory microstructure edits failed to
produce a repeatable end-to-end gain.

## Reproducible scope

- The existing `benchmark_durable_pong.py` replayed 10,000 original signed
  observations from the retained September 14 failed-hour journal through the
  receiver's authentication/validation, the append-only collector, and the
  serial production host (`compute_workers=0`). Its synthetic 2,000 events/s
  admission schedule and frozen historical clock are **test only**; they do not
  create qualifying market evidence.
- The retained source prefix SHA-256 was
  `6b59a1e816490656174389c1bbae7ef753e1de5d8924cd6f5ac7b354d177a138`.
  The original [baseline result](N:/Beelzebub/runtime/audit/pong-market-open-20260914/TEST-ONLY-durable-replay-1789406019123373400/result.json)
  was **780.46 events/s**. A fresh run of the restored candidate yielded
  [781.49 events/s](N:/Beelzebub/runtime/audit/pong-market-open-20260914/TEST-ONLY-durable-replay-1789411425140196600/result.json)
  (10,000 / 12.796 s). This is within ordinary run-to-run variation.
- The fresh run admitted and durably retained all 10,000 records, with zero
  receiver rejects/duplicates, host input errors, or host overflows. Independent
  read-only verification of every journal sequence, previous hash, canonical
  record hash, wire hash, final tip, and source-prefix digest passed; its
  [test-only verification receipt](N:/Beelzebub/runtime/audit/pong-market-open-20260914/TEST-ONLY-durable-replay-1789411425140196600/independent-journal-verification.json)
  is separate from the journal. The new
  test journal tip is
  `35c6df0eec9c6dd2f293c853c17de74d37b26dea1276fcfcceafd478d0de454d`.
  This tip is run-specific because the collector ID is in the hash chain.

## Measured cost, retained-prefix replay

The full-run harness samples Windows `thread_time` for the named functions.
Values below are CPU seconds per 10,000 records, rounded; timer granularity and
nested callbacks make them approximate and **not an additive CPU ledger**.

| Stage | CPU seconds | Evidence/interpretation |
| --- | ---: | --- |
| Receiver decode, validation, signature check, admission and health | 4.33 | Authentication alone is not this entire cost. |
| Serial host admission and causal evaluation | 5.63 | Includes every host substage below. |
| Collector interval/sequence validation | 0.63 | Same retained signed records. |
| Collector canonical serialization and chain hashing | 0.50 | Excludes journal write/fsync waiting. |

The [receiver-only profile](N:/Beelzebub/runtime/audit/pong-market-open-20260914/TEST-ONLY-receiver-profile-1789411390980204400.txt)
shows 4.47 profiled seconds in frame decode, including 2.29 in observation
construction and 1.79 in deep immutable-payload freezing, and 1.73 in
`verify_attestation`. Those are inclusive profiling times, not additional cost
to add to the table. The HMAC primitive is only a small part of verification;
weakening or bypassing attestation would target the wrong work.

The isolated [host cProfile](N:/Beelzebub/runtime/audit/pong-market-open-20260914/host-profile-1789411477917931500.txt)
reports these inclusive profiled seconds over 10,000 retained records:

| Host substage | Profiled seconds | Calls |
| --- | ---: | ---: |
| Entire host consume | 10.05 | 10,000 |
| Causal frame selection, gates, mapping | 8.73 | 5,007 |
| Flow adapter | 2.86 | 4,974 |
| Bid/ask liquidity adapters combined | 2.86 | 9,948 |
| Ordered-reference checking | 1.33 | 14,922 |
| Canonical encoding/hash (shared across adapters) | 1.47 | 31,217 |
| Trade-window filter expression | 0.54 | 802,041 iterations |
| Volume-history prior selection | 0.52 | 4,974 |
| Volume regime adapter | 0.37 | 4,974 |
| Native record admission | 1.07 | 10,000 |

The host substage timings overlap by design and cProfile adds substantial
overhead. They identify hotspots, not unprofiled CPU totals. The collector's
serialization wrapper accumulated 7.55 s of concurrent wall time but only
about 0.50 s of sampled thread CPU; this measurement **does not isolate**
journal write/fsync or network-storage latency. Controller/Discernment mapping
was not meaningfully stressed: the retained prefix had bid/ask relative-size
and prior-volume blockers, so no valid mapped evidence reached that path.

## A/B findings and qualification gap

Removing three whole-window LRU caches and replacing flow/liquidity passes with
a single Python loop were tried only in the working tree, then reverted. The
end-to-end results were 795.04 and 766.46 events/s respectively, against the
780.46 baseline; host-only timings were variable and the one-loop version was
slower in a direct comparison. No performance claim or candidate code change
is justified by those results.

The observed market arrival range is 1,345–1,689 events/s. The restored
781.49 events/s serial replay needs about **1.72–2.16×** its current capacity
merely to match that range, before any headroom. A 25% peak-rate engineering
target, if adopted, would be about 2,111 events/s and require about 2.70×
current capacity. The existing 10,000-record prefix is not a valid-evidence
benchmark: its last blockers were
`BID_LIQUIDITY:LIQUIDITY_CLUSTER_BELOW_RELATIVE_SIZE`,
`ASK_LIQUIDITY:LIQUIDITY_CLUSTER_BELOW_RELATIVE_SIZE`, and
`VOLUME_REGIME:VOLUME_REGIME_INSUFFICIENT_PRIOR_HISTORY`. Neither it nor a
synthetic valid-frame fixture can certify a real qualifying hour.

## Narrow next engineering pass

Primary code surfaces are `src/l3g_paper/pong_production.py`
(`_evaluate_microstructure`, `_admit_trade`, `_admit_depth`),
`src/l3g_paper/pong_microstructure.py` (ordered refs, flow and liquidity
statistics), `src/l3g_paper/pong_volume_history.py` (`prior`),
`src/l3f_provider/ninjatrader_observation.py` (decode/freeze/attestation), and
`src/l3g_paper/pong_collection.py` (journal/interval validation). Include
`src/l3g_paper/pong_controller.py` only for measurement of durable-control
latency. Do not change any safety gate to meet the rate target.

1. Keep `compute_workers=0`. Capture an exact build/source manifest and a
   representative signed replay corpus with sustained and peak windows,
   including frames where flow, both liquidity gates, volume regime, and
   Discernment mapping all run. Record why each window is representative and
   keep invalid periods as negative controls. Never relabel unavailable data
   as valid to create the fixture.
2. Instrument **exclusive** stage CPU and wall time with low-overhead counters:
   receiver decode/freeze, attestation, host admission, trade/depth window
   selection, flow statistics, bid/ask liquidity statistics, volume history,
   canonical encoding/hash, mapper/Discernment, journal write, fsync, and
   controller durable writes. Report queue depth and drain time under paced
   sustained and burst ingress; avoid adding nested inclusive times.
3. Optimize the measured dominant serial component while preserving ordered
   source IDs/hashes, exact as-of admission, normalized timestamps, policy
   identities, result hashes, blocker order, signed wire, and chain format.
   Candidate experiments are incremental causal-window state with exact
   eviction, prevalidated immutable metadata, and fewer redundant copies.
   Keep a reference implementation for per-frame byte/hash/decision parity.
4. Re-run independent signed-input and journal-chain verification plus the
   focused safety suite after each accepted change. Claim throughput
   qualification only after repeated end-to-end sustained and burst runs on
   valid evidence exceed the documented arrival envelope with chosen headroom,
   with zero gaps/overflows, bounded queues, complete drain, and exact
   evidence parity. Deployment and fresh Sim101 reconciliation remain later
   operations.

The parity suite must retain the existing fail-closed outcomes for unsigned or
source-mismatched input, changed producer/generation, duplicate/out-of-order
and gapped stream sequences, future-available evidence, incomplete causal
windows, insufficient prior volume, stale flow/liquidity, queue overflow, and
host/compute failure. Record nested blocker codes at each first failing event,
not only the last host status. Preserve exact `Sim101 / LOCAL_SIMULATION /
MNQ SEP26`, one-contract policy, and live-capital denial throughout.
