# Acceptance decisions — no-FHB corrective readiness

## Current release decision

`HOLD_R2_NATIVE_OUTBOUND_SOCKET_RECEIVER_BACKPRESSURE`

Gate A delta passed for frozen candidate `767f850...`, and Sol spent the one
unconditional receiver-first R2 attempt. R2 failed on observed native outbound
socket/receiver backpressure. Gate B was not run; Gates B–D remain blocked.
The one conditional R2 retry is preserved and may be spent only after a named,
reviewed correction passes a new Gate A delta review.

## Owner A decision

`OWNER_A_PHASE_1 = ACCEPTED_SOURCE_OFFLINE`

The full contract below is met by the exact five-record collector fixture,
narrow production-host delta, existing plus new negative coverage, and Caelum's
independent 42-test/8-subtest run. The decision does not accept the composite
candidate or authorize native install/R2.

## Owner A acceptance contract

Accept only if all are true:

1. authentic records 146–150 are verified and replayed through actual
   production-host logic, with the first failing record and exact branch
   identified; current evidence predicts record 148 / DEPTH 143, not record
   149, because its resolved Ask Add at position 10 lies outside the retained
   top ten;
2. the change is the smallest production-host semantic correction and does not
   broaden depth/continuity semantics; a truthful available mutation outside
   the retained visible top ten may leave the exact 10x10 canonical book
   unchanged without requiring its resolved price to appear in that retained
   book;
3. explicit unavailable unresolved Remove preserves authenticated continuity
   accounting, invalidates canonical analytical readiness, emits no ordinary
   analysis, and does not inspect/fabricate an unresolved removed price;
4. unavailable remains unavailable until a later exact authenticated coherent
   available 10x10 state recovers it;
5. the seam demonstrates available -> unavailable/no analysis -> exact
   recovery without accepting partial/malformed available input;
6. all named negative cases in requirement P1.4 pass, including independent
   source gap/generation/reset behavior and available-partial fatality;
7. fixture provenance and hashes are recorded; a synthetic-only replay cannot
   satisfy item 1;
8. command lines, counts, diff, and residual risks are returned to Sol/Caelum.

If meeting this contract requires broad depth semantic changes, the release
remains HOLD pending review.

## Owner B acceptance contract

Accept telemetry only if aggregate and attributable across the same interval:

- callback DEPTH/QUOTE/TRADE rates, callback processing aggregate and
  publication attempts;
- native queue capacity/current/high-water/enqueue/dequeue/overflow/oldest-age
  plus frames and bytes per sender batch;
- sender batches/records/bytes, socket write count/total/max time, reconnects,
  and transport failures;
- receiver bytes/frames/decode/reject, occupancy and service latency;
- host authoritative ingress/admission, queue/age, processing, scheduler,
  protection supersession and full evaluations;
- collector metrics labelled separately.

The causal conclusion must identify the first persistent `input > output`
boundary or a p50/p95/p99/max stall distribution and be labelled **OBSERVED**.
If current evidence cannot do that before runtime, label it **INFERRED / needs
runtime proof** and do not integrate a speculative performance repair.

An implicated correction may receive one initial implementation and at most
one evidence-supported corrective revision. It must preserve every record and
sequence, must not wait to fill a batch, sample/drop depth, enlarge a queue to
hide service-rate deficit, relax freshness, or suppress protection.

### Owner B/C composite correction required

Native source/compiler evidence is provisionally acceptable, and the causal
classification is correct: only the broad native producer boundary is
`OBSERVED`; the internal boundary is `INFERRED / needs runtime proof`; no repair
budget is spent. Composite acceptance is withheld because the Python listener
requires `native_callback_cpu_time_supported` while the authoritative native
schema emits `native_callback_cpu_cycles_supported` and cycle counters. Align
and test exact schema congruency before Gate A delta review.

**Correction accepted:** exact emitted-set congruency and CPU-cycle retention
are now covered; independent source tests pass. Owner B/C instrumentation is
accepted source/offline. Runtime causality remains unproven, so no performance
repair is accepted or spent.

## Gate A delta review checklist

- [x] Owner A acceptance contract fully met.
- [x] Owner B/C instrumentation contract met at source/offline level; runtime
      causal observation remains required before any performance repair.
- [x] Performance correction explicitly deferred pending runtime causality;
      zero repair attempts spent.
- [x] Required focused and affected integration tests pass, with C: low-disk
      refusal isolated and exact ledger suite passing on adequate N: temp.
- [x] Compiler/source-contract validation passes.
- [x] Observer fingerprint and loaded artifact/policy/runtime identities are
      recomputed and mutually congruent.
- [x] Old identity-bound histories/receipts are marked ineligible.
- [x] Qualifying import/hash graph excludes FHB and E4.
- [x] Exact 10x10, 20 ms owner, 1,000 ms freshness, serial compute, 2,111/s,
      durability and safety contracts are unchanged.
- [x] Candidate is reviewable commits `4630230` + `767f850`, with unrelated
      dirt excluded.
- [x] Retry ledger remains unused before runtime.

Gate A delta PASS authorizes only Sol's bounded receiver-first commissioning.
It is not proof of installation, loaded identity, runtime health, capacity,
qualification, arming, or order authority.

## Fresh R2 decision

`R2 = FAIL`

Distinct receiver-first current identity and safe authority passed. Clean
initialization and sustained lossless/fresh service did not. Reject delta
`+2623`, producer overflow `+2590`, DEPTH gap 347, source age ~145 s, native
queue age 35.626 s and zero periodic full evaluations prohibit Gate B.

`GATE_B = NOT_RUN_BLOCKED`

One conditional R2 retry remains available only after a named, reviewed
correction. No unchanged retry, Gate B run, capacity series, rehearsal, or
readiness classification is accepted.

Final corrective-pass classification:

`HOLD_R2_NATIVE_OUTBOUND_SOCKET_RECEIVER_BACKPRESSURE`

Smallest next cut, not authorized in this closed pass: separate or remove the
synchronous legacy fanout from the authoritative receiver ingress read loop
without dropping, sampling, reordering, weakening durability, or changing
20 ms/exact-10x10/freshness/capacity/safety contracts. Then perform source/
offline Gate A delta review before spending the one conditional R2 retry.
