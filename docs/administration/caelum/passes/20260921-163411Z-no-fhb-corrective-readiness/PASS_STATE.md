# Caelum pass state — no-FHB corrective readiness

Pass ID: `20260921-163411Z-no-fhb-corrective-readiness`

Opened: `2026-09-21T16:34:11Z` / `2026-09-21T10:34:11-06:00`

Closed: `2026-09-21T17:21:00Z` / `2026-09-21T11:21:00-06:00`

Administrator: Caelum (`beelzebub_admin`)

Integrator, benchmark coordinator, native-install operator, runtime operator,
and ledger/process owner: Sol parent

Current phase: fresh R2 failed and stopped; next-cut HOLD

Overall verdict: **HOLD — Gate A passed, fresh R2 failed, Gate B not run**

## Objective and boundaries

Move the frozen no-FHB candidate from `Gate A PASS / R2 initialization HOLD`
to the furthest legitimately earned state through fresh R2, Gates B–D, and a
possible `READY_FOR_QUALIFICATION_RUN` recommendation. The architecture stays
`FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`; FHB stays archived; E4 stays
parked; exact 10x10, 1,000 ms freshness, and 2,111 records/s thresholds do not
change. Work remains Sim101-only, flat, disarmed, order-free, command-free, and
live-capital denied.

Caelum writes only this pass directory. Caelum may inspect repository and
evidence read-only but may not edit application/tests/configuration, operate a
runtime, install or reload NinjaTrader source, benchmark, touch a ledger, arm,
or send orders. Sol retains every integration and operational action.

## Phase 0 truth

Observed at `2026-09-21T16:35:19Z`–`2026-09-21T16:37:08Z`:

- branch `codex/lane-iii-perpetual-corrective-20260906` at exact HEAD
  `2e2c743e6205f09f10901ad6af4d8375fd0a4ffb`;
- sealed mandatory-repair reference
  `c76cccc2a3242f9bda00f4677648f589f70be317` is an ancestor of HEAD;
- only pre-existing unrelated user dirt was present at intake: modified
  `docs/administration/caelum/audits/20260920-1700MDT-rehearsal/CAELUM_REHEARSAL_REVIEW.md`
  and untracked audit/rehearsal/depth-recovery paths reported by `git status`;
- no listener was present on `8090`, `48135`, `48136`, or `48137`;
- operator-owned NinjaTrader PID `17208`, created `2026-09-20T15:49:24-06:00`,
  was the only relevant persistent process; the PowerShell processes in the
  snapshot were bounded inspection commands;
- `src/l3g_paper/pong_qualifying.py` still binds the qualifying architecture,
  20 ms `PERIODIC_FLAT_PROTECTED` scheduling, and
  `EXPERIMENTAL_ARCHIVED_NOT_PART_OF_QUALIFYING_BUILD`;
- the qualifying loaded-source hash list in `pong_production.py` does not
  include `src/l3g_paper/fhb.py`; the FHB import is type-only at module load and
  runtime import/instantiation remains guarded by an explicit non-null FHB
  configuration, which qualifying construction rejects;
- the prior R2 commissioning receipt, short diagnostic, first-fault/final host
  receipts, failed collector corpus, and ledger closeout verifier still exist;
  the exact receipt hashes are in `EVIDENCE_REGISTER.md`.

No prior epoch has been cleared, renamed, extended, or relabelled. The prior
session/source/build/generation are historical failure evidence and become
ineligible after any source change.

## Starting acceptance state

- **PRESERVED PASS:** receiver-first identity boundary, truthful native
  availability, host telemetry, FHB exclusion, Sim101 safety, and ledger
  closeout through sequence 233998.
- **PRESERVED FAIL:** clean R2 initialization. The host reached
  `PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK` in the authentic signed seam
  spanning journal records 146–150. Intake attributed it to record 149 / DEPTH
  144, but current record-level inspection supersedes that attribution below.
- **PRESERVED PERFORMANCE BLOCKER:** the diagnostic showed 305.078 DEPTH/s,
  43.985 opportunities/s, zero full evaluations, producer overflow `+6335`
  during the measured interval and `+45772` by closeout, zero transport
  failures, and source age 81.321 s while the host queue recovered 58 to 5.
- **NOT RUN:** Gate B, Gate C, Gate D. The short failed diagnostic is not any of
  those gates.

## Decisions

1. Priority zero is the exact authentic 146–150 seam. **SUPERSEDING CAUSAL
   FINDING:** production source returns early for explicit unavailable input.
   Signed record 148 / DEPTH 143 is an available exact 10x10 Add at Ask position
   10 with resolved price 30599.5, which lies immediately beyond the retained
   visible top ten and is therefore the likely trigger for the post-book
   membership fault. Record 149 / DEPTH 144 is the adjacent truthful unavailable
   Remove at position 11, not yet the demonstrated fatal application. Owner A
   must reproduce the fault through production logic before final attribution.
   The requested unavailable fail-closed/recoverable semantics remain mandatory
   non-regression behavior.
2. Overflow/source-age is independent until evidence proves otherwise. The
   recovered downstream host queue means Python analytics may not be named the
   bottleneck without a stage-rate or stall measurement.
3. Aggregate telemetry is authorized; per-event logging, event discard,
   sampling, buffer enlargement as concealment, relaxed freshness, and
   speculative architecture are not.
4. A performance correction receives one initial repair and at most one
   evidence-supported corrective revision. No correction is accepted before a
   measured first under-service/stall boundary exists.
5. Native install and fresh R2 remain held until Caelum reviews the integrated
   candidate and issues Gate A delta PASS.

## Phase 1 acceptance

**ACCEPTED SOURCE/OFFLINE; NOT YET INTEGRATED/INSTALLED/LOADED/RUNTIME-VERIFIED.**

Owner A proved the causal seam with the authentic collector records and made a
narrow correction in `PongProductionHost._admit_depth`: an available exact
10x10 Add/Update whose authenticated integer position is at or beyond the
retained length and whose resolved price is strictly worse than the retained
worst price may leave the visible canonical book unchanged. It restores the
signed exact snapshot but fabricates no visible `DepthObservation`. Every
inside-core inconsistency, partial/malformed available book, unresolved
available identity, gap, generation change and reset remains fail-closed.

Caelum independently confirmed the five-line fixture is exactly equal to
preserved journal records 146–150, rechecked its hash and chain fields, reviewed
the full diff, and ran the focused suite: `42 passed, 8 subtests passed` in
4.16 seconds. No Phase 1 correction is requested. Gate A delta remains held for
Phase 2, composite integration, affected tests, compiler/identity review and a
sealed candidate commit.

Sol integrated the Phase 1 files as commit
`4630230161dbbab9355c86c1f2da68aae9f85903` (`recover canonical depth across
out-of-core mutations`). This is a reviewable intermediate integration commit,
not the composite corrective candidate and not Gate A delta PASS.

## Active ownership

- Owner A (`/root/owner_a_record149`): **COMPLETE / SOURCE-OFFLINE ACCEPTED**;
  authentic seam and focused semantic tests in the source/test paths assigned
  by Sol. Files are uncommitted and parent-owned for integration.
- Owner B (`/root/owner_b_overflow`): aggregate producer-to-host stage
  telemetry and source tracing in paths assigned by Sol; diagnosis before
  performance repair.
- Sol parent: Owner C periodic-path inspection, shared integration, identities,
  tests, candidate commit, benchmark lock, and all operational work.
- Caelum (`/root/caelum_corrective`): this pass directory only; requirements,
  critical path, evidence review, retry accounting, HOLD/release decisions.

All owners share the checkout. Unrelated user work and other owners' changes
must not be reverted, overwritten, staged, or committed accidentally.

## Initial Gate A HOLD — released before R2

**Action held:** native installation, fresh R2, and every downstream gate.

**Evidence:** no corrective candidate has yet been integrated or reviewed; the
authentic 146–150 seam still produces a terminal host fault, with record 148 now
the evidence-backed likely trigger pending production replay, and native
producer overflow/source age remains an independently demonstrated performance
failure.

**Consequence:** installation or runtime work now would spend the bounded R2
retry on an unaccepted build and could not establish a clean corpus.

**Release condition:** the exact Phase 1 semantics and authentic replay pass;
the Phase 2 first under-service/stall boundary is measured and any accepted
correction stays within budget; focused plus affected integration/compiler/
identity checks pass; FHB and E4 remain excluded; Sol seals one reviewable
candidate commit; Caelum records Gate A delta PASS.

Independent source review, implementation, and focused tests continue.

### Composite telemetry correction (2026-09-21T16:54Z)

**Evidence:** Owner B's native HEALTH schema now emits
`native_callback_cpu_cycles_supported`, CPU sample/failure totals, and per-stream
CPU-cycle total/max fields. The concurrent Python listener delta still requires
the superseded `native_callback_cpu_time_supported` field and does not whitelist
the cycle counters. Its focused test uses the same stale field.

**Consequence:** the listener would silently reject a real current native
aggregate payload, erasing the CPU evidence while source-only tests remain
green. Gate A delta and native install remain held.

**Release condition:** align the listener validation/whitelist/tests to the
exact native schema and add a congruency check that fails on native/Python field
drift; rerun native-source and Phase F3 focused tests. Owner C/parent owns the
receiver correction; Owner B supplies the authoritative native field set.

**Released for source/offline integration at `2026-09-21T16:57Z`:** the receiver
now requires the exact CPU-cycle field, retains the full fixed integer schema,
rejects the stale field, and has a deterministic emitted-set equality test
against the native `AddAggregateTelemetryLocked` source. Independent focused
results are 46/46 native-source and 29/29 Phase F3. Gate A itself remains held
for full composite identity/test/commit review.

## Phase 2 acceptance

Owner B native instrumentation and Owner C receiver instrumentation are
**ACCEPTED SOURCE/OFFLINE**. Native compiler evidence reports zero errors and
1,329 warnings. The instrumentation is aggregate-only and persisted through a
signed HEALTH record; no per-event logging or event-policy change was added.
Python uses a fixed bounded latency histogram and explicitly labels framing
buffer occupancy `is_queue=false` because no independent receiver queue exists.

The causal conclusion is deliberately limited:

- **OBSERVED:** the prior failure lies at the broad native producer boundary:
  overflow rose by 6,335 while transport failures stayed zero and the host
  queue recovered 58→5 with age 110→0 ms.
- **INFERRED / needs runtime proof:** callback processing, publication lock,
  outbound queue dequeue, or sender/socket write is the first internal
  under-service/stall boundary. Current evidence cannot choose among them.

No performance repair or corrective revision budget has been spent. The next
legitimate performance action is measurement on the current instrumented
candidate, not speculative tuning.

## Gate A delta decision

`GATE_A_DELTA = PASS`

Frozen corrective candidate:
`767f8506077344baf3911ff9d08df4df4e81a39a` (`instrument native receiver
throughput path`), with Phase 1 commit
`4630230161dbbab9355c86c1f2da68aae9f85903` and sealed baseline
`c76cccc2a3242f9bda00f4677648f589f70be317` both verified ancestors.

Acceptance evidence:

- affected composite: 221 tests and 62 subtests passed; two ledger tests
  correctly refused C: test temp because 5.2 GB free is below the preserved
  8 GB floor; the exact ledger suite then passed 9/9 on N: with 211.9 GB free;
- bundled native compiler validation: PASS, zero errors, 1,329 warnings;
- callback ordering PASS; generated wrapper PASS with zero repaired errors and
  idempotence;
- observer identity congruent across supported derivation, native constant and
  Python constant:
  `051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14`;
- AddOn SHA-256
  `89700b8a4eee0bfd4c437f9ed1fdf6401e8e78d5d0c198edb8d80811ba167be7`;
  Indicator SHA-256
  `38659a9907c02923d6f96c6fa48920dde0f4f886a8b0283b326ef07663f2d310`;
- qualifying loaded-artifact hash
  `103d817809f601dbedea24bc411b74b15481da3f264d692afaa0902b79b92802`;
- fresh import leaves `src.l3g_paper.fhb` unloaded, excludes `fhb.py` from the
  qualifying hash list, and preserves archive disposition; E4 paths are absent;
- worktree contains only pre-existing unrelated dirt plus this pass directory;
  no candidate source/test file remains uncommitted;
- ports 8090/48135/48136/48137 remain free and operator-owned NinjaTrader PID
  17208 is the only relevant persistent process at the decision snapshot.

This PASS releases the prior native-install/R2 HOLD to Sol only and authorizes
exactly one fresh receiver-first commissioning attempt on this frozen commit.
It does not establish installed or loaded identity, R2, Gate B, performance
repair acceptance, qualification, arming, orders, commands, or live authority.
If R2 fails, preserve it and stop before Gate B.

## Fresh R2 terminal decision

`R2 = FAIL / HOLD`

Sol spent the one unconditional fresh R2 attempt on frozen candidate `767f850`.
The receiver-first identity boundary itself passed:

- expected source `051808624d0d91ac04e91cae27b7e8765d887774053192b909c7b7434f652a14`;
- loaded build
  `390e3497e1269195977f781d35418af5fe2b48a0702670d4684f4a43863b422a`;
- session `2b2bcac2fd8043a2a295a70989b19fc2`, generation 1;
- exact loaded temp-assembly match, single attached producer, listener/collector
  ready before source watcher rebuild;
- checkpoint producer overflow/transport zero, host fault null, exact available
  10x10 or explicit unavailable, Sim101 flat/disarmed/reconciled with zero orders.

R2 nevertheless failed. The commissioning receipt is `verified=false` because
old-source tail rejection increased by 2,623 between checkpoints; therefore no
clean accepted corpus boundary was earned. Rejections later stabilized, but
the bounded natural diagnostic then demonstrated a decisive current-generation
service deficit:

- callback/publication about 750 records/s versus native dequeue/socket about
  385.8 records/s and receiver extraction about 358.3 records/s;
- native queue 27,400/32,768, high-water 27,417, oldest frame age 35.626 s;
- sender socket write time 70.058 s of 75.247 s telemetry elapsed, maximum one
  write 3.332 s;
- synchronous receiver/downstream legacy fanout mean 1.584 ms and maximum
  205.429 ms;
- final producer overflow +2,590, transport failure +0;
- final host DEPTH gap 347, source age 143.123 s / maximum 144.958 s;
- host queue ended zero after high-water 2,592 and maximum age 7,578 ms;
- 16,869 periodic opportunities, zero ordinary periodic full evaluations, and
  68,882 protected event-driven evaluations.

**OBSERVED first under-service boundary:** native outbound enqueue exceeds
dequeue/socket delivery, while the receiver's synchronous extraction/downstream
fanout service applies loopback backpressure. The prior broad-native inference
is superseded by current source/build/session runtime evidence. This does not
justify blaming ordinary Python analytics: the host queue ultimately drained,
while sender writes were blocked and the native queue aged/overflowed.

**Action held:** Gate B, R4, Gate C, Gate D, readiness classification, and the
conditional R2 retry.

**Consequence:** overflow, source gap and extreme source age invalidate this
epoch. The stopped corpus cannot be cleaned, extended, or relabelled.

**Release condition:** one bounded correction must directly increase socket
receiver/downstream service without dropping/sampling/reordering records,
weakening durability/freshness/protection, or hiding service deficit with a
larger buffer. Only after source/offline review and a demonstrated implementation
defect may Sol spend the one conditional R2 retry. Per the user's latest
directive, no correction or retry was started in this pass.

Supported stop initially timed out under backpressure but completed on the
supported retry. All target ports are free; only operator-owned NinjaTrader PID
17208 remains. Final collector durability is 71,105/71,105/71,105 with zero
collector overflow and zero duplicates. Incremental ledger verification passed
from sequence 233,998 through 277,856. This safe closeout is not R2 readiness.

The requested metric chain and causal synthesis are preserved in the immutable
bounded diagnostic receipt at
`H:\PongPeriodic\NoFHBQualificationCorrective-20260921\R2-bounded-natural-diagnostic-20260921T171332Z.json`,
SHA-256
`47b37cb3847f128a7c2a90d7c36ae65645c2cd5a22b9edde3d30e5d56f5ffcea`.
It was composed from already captured snapshots after shutdown; creating it did
not run or retry the system.
