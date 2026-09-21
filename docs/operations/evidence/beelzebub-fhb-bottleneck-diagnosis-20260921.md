# Beelzebub FHB bottleneck diagnosis and implemented fixes — 2026-09-21

## Outcome

The repaired FHB now reaches the intended valid data path, but it does not improve whole-path throughput in the current configuration. On the fresh 10,000-record same-capture periodic harness, FHB reduces analytical evaluations and peak traced memory but adds 13.234 seconds wall and 13.078 seconds CPU. A subsequent bounded allocation repair at `ce7f64d669284aadada4344af68fb53f159c7070` reduces the isolated FHB-maintenance span by 2.45–4.55% across two repeats with exact semantic parity, but total FHB wall time remains within noise of the pre-change run (-0.49% and +0.57%). On the live disarmed host, the configured 20 ms periodic path is not active; fail-closed `EVENT_DRIVEN_PROTECTION` evaluates canonical changes because the runtime bridge is unavailable. The host eventually exceeds the existing one-second freshness limits and correctly makes FHB not-ready.

The measured performance verdict is therefore **FAIL / HOLD**, not an optimization success. No queue, latency, durability, qualification, or authority threshold was raised or relaxed.

## Receipts and workload

Fresh authentic journal:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T033216734509Z-468d687551cf\authenticated-observations.jsonl`

- 107,537 durable records and 306,098,341 bytes.
- 99,081 DEPTH, 3,728 QUOTE and 4,728 TRADE.
- 94,779 available DEPTH and 4,302 explicit unavailable DEPTH.
- zero journal stream gaps and duplicate/out-of-order events.
- SHA-256 `b8c95b3ce4b84c1e16d14d7e218f7771e9aaef158de3cdd6304f36049e5dcc92`.

Comparable fresh-prefix receipts:

- `profile-d5544ae-fresh-10000-r1.json`, SHA-256 `2c85d8009b43841b733964740a0d46da58fae688507c34a97f566c2031a2dc2e`.
- `profile-d5544ae-fresh-10000-r1.pstats`, SHA-256 `b1e0d9f144fe5372178322fdbde36f486785bcac210294aa195dd959e18601a8`.
- `wall-d5544ae-fresh-10000-r1.json`, SHA-256 `69d3375c7cf3b8591db2f1f7c23338b0b38410bf59e9417d9b9e89cdb357d159`.
- `wall-fhb-snapshot-opt-fresh-10000-r1.json`, SHA-256 `c0c3173e95dd2399ada742dff357500f4d48a17cec6d92f546635f73f32c8a20`.
- `wall-fhb-snapshot-opt-fresh-10000-r2.json`, SHA-256 `e82ebdb3ef1e832ee993020779f34cf4571e76e80a6a7c58c781ef03179fdb8a`.

The harness authenticates the same immutable prefix in both passes, uses the same deterministic QPC grid, denies native dispatch, uses zero compute helpers, and removes temporary state after status capture. It excludes collector disk writes, the paper ledger, real queue residence, runtime-bridge behavior and native callback/sender CPU; it is comparison evidence, not qualification.

## Same-capture comparison

Fresh 10,000-record prefix: 8,984 DEPTH, 412 QUOTE, 604 TRADE; no source gaps or out-of-order events.

| Metric | Periodic baseline | Periodic FHB | Delta / interpretation |
|---|---:|---:|---|
| Wall seconds | 35.734431 | 48.968338 | +13.233906 (+37.0%) |
| CPU seconds | 35.406250 | 48.484375 | +13.078125 (+36.9%) |
| Diagnostic records/s | 279.842 | 204.214 | -27.0% |
| Peak traced bytes | 15,410,156 | 9,693,623 | -5,716,533 (-37.1%) |
| Microstructure evaluations | 612 | 270 | 342 fewer (-55.9%) |
| FHB eligible evaluations | n/a | 1,308 | valid path exercised |
| FHB out-of-core skips | n/a | 2,782 | intentional skips |
| FHB rewarm-required events | n/a | 4,894 | includes startup and natural seams |
| Completed rewarms | n/a | 319 | nonzero recovery proof |
| Native dispatches | 0 | 0 | unchanged/disarmed |

The lower evaluation count and lower traced-memory peak do not compensate for FHB maintenance in this Python replay. The harness status `PASS` means only that its narrow parity predicates and nonzero eligible path passed; its qualification field is `NOT_QUALIFIED_HISTORICAL_RETAINED_REPLAY`.

## Ranked stage costs

The fresh 10,000-record cProfile contains both passes, 20,000 total envelope decodes, and profiling overhead. Cumulative spans overlap and must not be summed as wall time.

| Rank | Stage | Cumulative evidence | Scope |
|---|---|---:|---|
| 1 | Canonical JSON encoding | 38.980 s internal / 40.309 s cumulative, 85,162 encodes | shared authentication, journal-shape and harness work |
| 2 | Wire parse | 39.750 s cumulative, 20,000 records | shared |
| 3 | Attestation verification | 31.709 s cumulative | shared, mandatory |
| 4 | FHB admission | 40.213 s cumulative, 8,984 DEPTH | FHB-specific owner boundary |
| 5 | `FocusedHorizonBand.process_depth()` | 38.948 s cumulative; 14.088 s internal | FHB-specific |
| 6 | Signed top-10 snapshots | 8.177 s cumulative, 17,968 sides | FHB-specific |
| 7 | Sparse position snapshots | 5.698 s cumulative | FHB-specific seam validation |
| 8 | Ordinary depth admission | 2.730 s cumulative | baseline comparator |

The unprofiled receipt is the authority for wall/CPU comparison. The profile is only a ranking aid because cProfile and `tracemalloc` magnify allocation-heavy paths.

## Live full-host result

The authentic brief ran from collector start `2026-09-21T03:32:16.734509Z` until final durable stop `2026-09-21T03:37:23.019458Z`, then drained. It is a brief initialization/fault proof, not the requested 20-minute acceptance window.

- Durable average: approximately 351 records/s over collector elapsed time; natural traffic only.
- Collector queue high-water 754/262,144; maximum queue age 2,016 ms; final depth zero.
- Host sample queue depth 739 and high-water 754 at the fault point.
- FHB source age reached 11,158.297 ms and `persistent_breach=true` with `FHB_HOST_QUEUE_AGE_EXCEEDED`.
- Before the fault, FHB was ready and genuinely consumed: at a representative snapshot it had 51,185 maintenance ticks, 12,036 output batches, 22,636 real consumer uses, fallback `NONE`, and zero helper workers.
- The live schedule remained `EVENT_DRIVEN_PROTECTION`; periodic-flat dispatch/evaluation stayed zero because `RUNTIME_BRIDGE_UNAVAILABLE` is deliberately fail-closed.
- Producer transport failure rose from baseline 0 to 209 during startup. Producer and collector overflow, rejects, duplicates, and journal stream gaps were zero.
- Host shutdown drained the queue and exited without force. The ledger verifier passed through sequence 164,814.

This demonstrates that the first limiting production stage is the single serial Pong host under event-driven protection, not native Level II availability, collector capacity, or ledger drain. Exact attribution between authentication, FHB maintenance and event-driven analytics requires lower-overhead per-stage production timers in a future pass; unavailable p50/p95/p99 stage latencies, allocation/GC and storage device counters are not invented here.

## Implemented repairs and their measured disposition

### Native continuity and disclosure

The native source now advances one sparse positional working cursor through crossed/partial transitions, emits exact removed-price identity where known, and conditionally discloses full sparse rows only at unavailable seams and the first coherent reconciliation. This eliminates the stale-cursor correctness defect. It is not claimed as a transport-rate reduction; 18,862 of 99,081 DEPTH records in the fresh journal carried the expanded disclosure contract.

### FHB maintenance/consumer wiring

FHB now performs exact 10+10 selection, five-second exact-price shadow maintenance, sparse-position validation, explicit invalidation/rewarm, bounded output, and real consumer accounting. The former disabled/excluded path is fixed. The live proof establishes nonzero maintenance, output and consumer watermarks with fallback `NONE`.

### Exact price-to-tick cache

The FHB repair includes a bounded 8,192-entry cache for immutable MNQ price-string to tick conversion. A helper-only QPC microcheck over 200,000 calls/20 prices improved 0.3516025 s to 0.0234068 s (15.021x) with identical checksum. This is a local operation improvement only. The integrated same-capture and live results above prevent any claim that it creates a net full-path performance win.

### Bounded snapshot-allocation repair

Commit `ce7f64d669284aadada4344af68fb53f159c7070` removes repeated sort/tuple/set construction during signed-side validation, copies only the mutated sparse-cursor side, and captures prior shadow quantities only for the removed-best touch case. It does not change the signed ordering, duplicate-price, source-level, sparse-cursor, shadow-history, startup-ambiguity or fail-closed rules.

Both post-change repeats preserve the exact 8,984 DEPTH / 412 QUOTE / 604 TRADE input and outcome counts: 1,308 eligible evaluations, 2,782 out-of-core skips, 4,894 rewarm-required events, 319 completed rewarms, 567 reconciliations, six coherence failures and seven resets. Repeat 1 reduces reported FHB maintenance from 14.1368282 to 13.4942676 seconds (-4.545%) while total FHB wall changes from 48.968338 to 48.728461 seconds (-0.490%). Repeat 2 reports 13.7908171 seconds maintenance (-2.448%) and 49.248005 seconds total wall (+0.571%). The focused and integrated suites pass 38/38 and 169/169 respectively.

This is an accepted local allocation reduction, not a whole-path throughput pass. The current expected Python/Pong loaded-artifact hash is `bd7826edd2d955bbb8ea59c2cc9160ead87532f36f976abba0b028865f575f7b`; it was not loaded into a new live host after the failed brief, so the earlier runtime proof remains attributable only to the `d5544ae` hash `a7f0f587da089c903b19c3b253d20c15d0bc1c95f6ca0f280350296e0e54a717`.

### Launcher and counter identity

The launcher now seals FHB profile/fingerprint and scheduler identity. The collector reports producer failure baseline/current/delta, accepts stable inherited nonzero totals, rejects same-producer increases and regressions, and treats a replacement producer as a separate identity boundary. The new natural epoch correctly reports a genuine +209 delta; the repair does not erase it.

## 2,111 records/s disposition

The 2,111/s figure remains unchanged. Its derivation is the prior observed 1,689/s peak plus 25% headroom (`1,689 × 1.25 = 2,111.25`, rounded to 2,111). The formal isolated contract at `H:\Trader-pong-serial-measurement\docs\operations\pong-serial-acceptance-v1.json` is an offered end-to-end workload for the authenticated receiver, one production analytical owner and durable collector. It requires at least three sustained and three burst repeats, zero gap/loss/overflow/reject/duplicate, bounded non-growing queues, full drain/worker shutdown, and signed-wire/journal/decision parity.

It is not a minimum natural-market rate and is not currently enforced by production source. The FHB qualification policy intentionally says `minimum_observed_event_rate=NONE` and evaluates the actual offered natural load. Natural traffic at approximately 351/s neither fails nor proves the 2,111/s capacity contract. No current same-build 2,111/s acceptance series exists, so input/whole-system capacity is **UNPROVEN**.

## Remaining bottleneck and release conditions

The immediate performance blocker is the serial Pong host becoming stale while the safety fallback performs event-driven protection. The measured allocation repair is complete, but it did not establish a whole-path improvement. The smallest safe next action is not another unchanged run. It is to obtain evidence for one of these existing-contract conditions:

1. exercise the true 20 ms periodic-flat path only when the current supported runtime bridge and all flat/reconciliation authority gates legitimately make it available; or
2. if the event-driven fallback is the required target, add bounded low-overhead production timers around parse/authentication, FHB maintenance, feature projection and microstructure evaluation, then repair the single dominant operation without throttling safety work.

Any subsequent natural proof must start a distinct evidence epoch and require producer transport delta zero, host/collector age within existing limits, no persistent breach, no sequence/reject/overflow faults, non-growing queues, FHB ready/maintained/output/consumed, fallback `NONE`, flat/disarmed zero orders, complete drain and post-stop ledger PASS.

The current source-bound history receipt is refused after the fingerprint change. Before qualification, the supported provider must retrieve exactly the two immediately preceding completed UTC hourly bars and the complete authenticated 2,016-bar volume-history contract for the loaded source/build/session. Qualification is a subsequent full live UTC hour and cannot be replaced by or overlapped with those retrieved warm-start bars.

## Dimension verdicts

| Dimension | Verdict |
|---|---|
| Positional coherence | PASS in source/independent replay; loaded natural recovery exercised, with startup ambiguity preserved fail-closed |
| FHB effective wiring | ACTIVE AND CONSUMED in the brief; epoch not accepted |
| Performance repair | PARTIAL LOCAL IMPROVEMENT / HOLD: maintenance span -2.45% to -4.55%, but no proven whole-path gain and the live persistent freshness breach remains |
| Input capacity | UNPROVEN at 2,111/s; actual natural load only |
| Natural-feed continuity | FAIL: +209 producer transport failures despite zero stream gaps/rejects/overflows |
| Periodic scheduling | UNQUALIFIED: configured 20 ms, active event-driven protection |
| Paper readiness | HOLD / NOT QUALIFIED |
