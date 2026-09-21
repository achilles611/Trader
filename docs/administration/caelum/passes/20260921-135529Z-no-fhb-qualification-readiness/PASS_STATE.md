# Caelum pass state — no-FHB qualification readiness

Pass ID: `20260921-135529Z-no-fhb-qualification-readiness`
Opened: `2026-09-21T13:55:29Z` / `2026-09-21T07:55:29-06:00`
Administrator: Caelum (`beelzebub_admin`)
Integrator, benchmark coordinator, and runtime operator: Sol parent
Current phase: post-R2 failed-commissioning corrective cut
Overall verdict: **HOLD — identity boundary passed; clean initialization failed; Gates B–D not run**

## Objective and authorization

Deliver the smallest stable `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`
candidate that earns `READY_FOR_QUALIFICATION_RUN` through the controlling
handoff's mandatory repairs, bounded experiments, composite verification,
short disarmed proof, unchanged formal capacity contract, and 20-minute
disarmed rehearsal.

FHB remains archived and outside the qualifying dependency graph. Work remains
disarmed. This administrative scope may inspect source and evidence and write
only this dedicated pass directory. It may not edit application, test, or
configuration files; operate processes; install, compile, or reload native
artifacts; benchmark; touch accounts or operational ledgers; launch listeners;
arm; or send orders.

## Intake state

### Repository

- **OBSERVED:** branch `codex/lane-iii-perpetual-corrective-20260906` at
  `5d9d15bb7c560faefc6f0683963509d470e9ec44`, 77 commits ahead of its remote
  tracking branch at intake.
- **OBSERVED:** commits after the no-FHB cut `efea61561e128bc44632def5d7c135e6ea7bacd9`
  add or normalize closeout documents only. No newer committed source repair
  was present at intake.
- **PRESERVED, not owned by this pass:** modified
  `docs/administration/caelum/audits/20260920-1700MDT-rehearsal/CAELUM_REHEARSAL_REVIEW.md`
  plus the existing untracked rehearsal/depth-recovery audit directories and
  document reported by `git status`. They are not to be reverted, staged, or
  rewritten by this pass.
- **OBSERVED:** `src/l3g_paper/pong_qualifying.py` still binds architecture
  `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`, scheduling
  `PERIODIC_FLAT_PROTECTED / 20 ms`, and disposition
  `EXPERIMENTAL_ARCHIVED_NOT_PART_OF_QUALIFYING_BUILD`.
- **SEALED BASELINE_R:** commit
  `c76cccc2a3242f9bda00f4677648f589f70be317` contains the exact reviewed
  mandatory-repair correction and Caelum pass state. The supported derived and
  Python observer identities were reverified equal at
  `585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`.
  At the seal, only pre-existing unrelated audit/evidence dirt remained.
- **COMMISSIONING HEAD:** Sol performed the supported disarmed runtime operation
  at `ac9b44e9d178d37c8fa1844fc36dd312a7651339`, a narrow commissioning-readiness
  state correction after `BASELINE_R`. The signed producer source identity
  remained `585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`.
  Gate A remains PASS; the failed runtime epoch does not amend or relabel the
  sealed baseline.

### Process, port, and machine snapshot

Observed at `2026-09-21T13:55:29Z`:

- ports `8090`, `48135`, `48136`, and `48137`: no listeners;
- relevant process: operator-owned NinjaTrader PID `17208` only;
- no Python, dotnet, or compiler process was present in the bounded inventory;
- CPU: Intel Core i5-8400, 6 cores / 6 logical processors, reported 2808 MHz;
- memory: 16,708,912 KiB visible, 7,700,928 KiB free;
- free space: C: 5,712,474,112 bytes; H: 30,604,910,592 bytes; N:
  228,141,559,808 bytes.

This is a point-in-time inventory, not restart or benchmark authority.

Runtime closeout after Sol's supported stop again found ports `8090`, `48135`,
`48136`, and `48137` free, with operator-owned NinjaTrader PID `17208` the only
relevant remaining process. This is a closeout snapshot, not continuous
monitoring or permission to stop NinjaTrader.

### Preserved evidence independently rechecked

- No-FHB failed collector journal: 149,996,841 bytes, SHA-256
  `361d9f5a612090475c4fa7a2a7a72d1fb6fb54defb537895dc58957d81f36087`.
- Collector status: 4,044 bytes, SHA-256
  `46f9d937cf04d52dc4ddedf4f69a71e7c3ebcdf52f0309f9fb09622c8f658b8d`.
  It is stopped/disarmed, durable `52517/52517/52517`, but producer overflow
  delta is `+44135` and transport-failure delta is `+32686`.
- Closeout verifier: 2,544 bytes, SHA-256
  `4bf5aeff8ee41100d56b9c35065a9d6ac4b98baf49143121f4f79f04bcdb9fd5`.
  It reports PASS through sequence 165842, tip
  `cfc6baa0482c4b5200c8e4b2abe53c37aa1fd1a87d51f227bb440a2bb0fdcf85`.
- Formal isolated capacity contract:
  `H:\Trader-pong-serial-measurement\docs\operations\pong-serial-acceptance-v1.json`,
  2,022 bytes, SHA-256
  `b502a94820c3c3f10fc8b4d8e8c618027394261e6bc828e5789cf56d4a6266fd`.

Bounded inspection of the preserved journal independently reproduced the exact
R1 seam. Journal 43–46 are available 10x10; journal 47 / DEPTH source 955867 is
available 10-bid/1-ask; journal 48 returns 10x10; journals 49 and 51 are again
available 10x2 and 10x3. Journal 47 is signature-verified and identifies:

- record hash `9b311649f701c7968437f94e15b61cb3320d3f7ab54fc4fa23f87974f643441d`;
- wire hash `f3aedfffc2eedd3f7cb02b4c3f5144646543ac5683fbfddc3eb34128fbc9495c`;
- payload hash `49b89207e0ade75edcf2a73c38caee42103e7500e95886595f73ea61f7277144`;
- operation `Add`, side `Ask`, position 1, price 30097, volume 7;
- a disclosed 30-row ask positional cursor whose first two rows have the same
  price, so the published strict prefix truthfully stops at one row.

This is a reproducible native publication defect, not a consumer-interpretation
or missing-data inference.

The failed journal remains negative/correctness evidence. It is not a clean
performance corpus and cannot be extended or relabelled.

## Critical path board

| Order | Work item | Owner | State | Evidence / release condition |
|---:|---|---|---|---|
| 1 | R1 truthful native availability | Native owner; Sol integrates | **PASS SOURCE/OFFLINE/LOADED TRUTH; clean epoch HOLD** | The loaded signed producer used source `585a57d4...fa2344`. Authentic journal record 149 / DEPTH source sequence 144 truthfully published an unresolved Ask Remove at position 11 as `UNAVAILABLE / NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED`, with `mutation_resolved_price=null`. That validates the repaired producer boundary; the host's fatal interpretation of this valid unavailable record is a separate next-cut defect. |
| 1 | R3 persisted host telemetry | Python telemetry owner; Sol integrates | **PASS IMPLEMENTED/RUNTIME OBSERVED** | First-fault and final receipts persisted the exact host fault, producer/receiver/host/collector counters, queue age/high-water, scheduler/protection truth, authority, clock domains and aggregates. The final collector subsequently drained `100472/100472/100472`. This is instrumentation acceptance, not candidate-readiness acceptance. |
| 2 | R2 supported receiver-first source lifecycle | Sol only | **PARTIAL PASS — DISTINCT AUTHENTICATED BOUNDARY; CLEAN INITIALIZATION FAIL/HOLD** | Receiver and collector were ready before reload; the runtime then held one distinct signed session `46adca83becf42e59e4356bb5829c88b`, source `585a57d4...fa2344`, build `70a1762e...eb759`, and generation 1 across advancing checkpoints. The commissioning receipt is nevertheless `verified=false`: host first fault was `PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK`, receiver rejects rose by 2,381 between checkpoints, and the final producer-overflow delta was `+45772`. No clean corpus boundary was earned. |
| 2 | R4 genuine periodic eligibility/current context | Sol only | **SOURCE PATH ACCEPTED; RUNTIME FAIL/HOLD** | Sim101 stayed flat, disarmed, reconciled, and without orders, but the host remained on `EVENT_DRIVEN_PROTECTION / RUNTIME_BRIDGE_UNAVAILABLE`. The 24.1676-second diagnostic produced 1,063 opportunities, 907 late/protection supersessions, 156 obsolete deadlines and zero full periodic evaluations; source age ended at 81.321 seconds. Required 2,016-bar, preceding-hour and current DEC26 bias evidence remains absent. |
| 3 | Seal `BASELINE_R` | Sol, after Caelum review | **PASS — SEALED** | Commit `c76cccc2a3242f9bda00f4677648f589f70be317`, source identity `585a57d4...fa2344`; mandatory-repairs-only no-FHB source, identity, independent review and bounded composite correction evidence complete. `efea615` remains negative evidence, not a timing baseline. |
| 4 | Optional experiment screens | Assigned disjoint owner; Sol holds benchmark lock | **CLOSED — E4 PARKED INCONCLUSIVE; OTHERS NOT APPLICABLE** | E1–E3/E5–E7 are closed from current code/telemetry. E4 exhausted its initial plus one corrective budget: the correction still couples retained ingress to real host startup, depends on a continuously settled checkpoint the collector cannot expose, lacks a durable-prefix stream boundary, and finished 11/13 tests. It is not integrated. |
| 5 | Composite integration/freeze | Sol | **PASS — BASELINE_R UNCHANGED** | No optional candidate earned integration. The frozen qualifying source remains `c76cccc2a3242f9bda00f4677648f589f70be317` with FHB absent and observer identity `585a57d4...fa2344`. |
| 6 | Gate B short natural proof | Sol only | **NOT RUN — BLOCKED BY FAILED R2 INITIALIZATION** | The 24.1676-second run was a diagnostic fail-stop after the commissioning fault, not the predeclared 3–5 minute Gate B window. It recorded DEPTH 305.078/s, opportunities 43.9845/s, zero full evaluations, producer overflow `+6335`, and increasing source age. A corrected fresh R2 epoch must pass before Gate B may be attempted. |
| 7 | Gate C formal capacity | Sol only | **NOT RUN — BLOCKED** | Gate B did not start or pass. The unchanged contract remains `2111/s`, `compute_workers=0`, three sustained plus three burst repeats, zero loss/reject/duplicate/overflow, bounded queues, durable drain, and wire/journal/decision parity. |
| 8 | Gate D 20-minute rehearsal | Sol only | **NOT RUN — BLOCKED** | Gates B and C did not pass. The stopped failed epoch and its 24.1676-second diagnostic are not a rehearsal and cannot be extended or relabelled. |
| 9 | Readiness verdict/runbook | Caelum recommends; Sol closes | **BLOCKED** | `READY_FOR_QUALIFICATION_RUN` is possible only after every pass prerequisite. It is not qualification, arming, or order authority. |

## Active assignments and file ownership

| Owner | Objective | Writable scope | Required return evidence | Prohibited side effects |
|---|---|---|---|---|
| Native R1 owner | Repair pre-signing availability while retaining exact ordered cursor transitions | `ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs`; `tests/test_native_depth_cursor_replay.py`; `tests/test_l3g_ninjascript_source.py`; `tests/test_l3f_ninjatrader_observation.py` | Exact diff, focused commands/results, authentic fixture provenance/hash, independent oracle result, unchanged ordered publication semantics | No install/compile/load/runtime/listener/account/ledger action; no FHB restoration; no consumer weakening |
| Python R3 owner | Persist bounded host start/first-fault/checkpoint/final telemetry | `src/l3g_paper/pong_production.py` and one nonoverlapping focused host-receipt test scope assigned by Sol | Exact schema/path/lifecycle, atomicity/fault tests, commands/results, receipt overhead constraints, no per-record synchronous logging | No listener/runtime/operational evidence writes; no shared control-center edits; no benchmark |
| Sol parent | R2/R4 source audit, shared integration, test sequencing, identities, benchmark lock, all supported runtime/native/ledger actions | Shared configuration/integration paths only; resolve overlaps before editing | Current source trace, integration tests, artifact identities, lifecycle receipts, gate evidence | No competing runtime owners, no blind restart, no order/arm, no threshold weakening |
| Caelum | Requirements, experiment registry, evidence review, holds and acceptance | This pass directory only | Updated matrix/decisions with exact evidence | No production/test/config edits, process control, benchmarks, or operational writes |

Workers are not alone in the checkout. They must preserve existing changes,
avoid reverting others, and route overlap through Sol.

### R1 integration identity note

Any R1 edit to either checked-in read-only NinjaScript producer changes the
computed observer source fingerprint. Sol completed the required integration:

1. derived the final fingerprint with the repository contract implemented by
   `expected_observer_source_fingerprint()` / the identical
   `Get-ExpectedFingerprint` logic in
   `tools/commission_ninjatrader_observer.ps1`;
2. updated both the native `ProducerSourceFingerprint` and Python
   `EXPECTED_OBSERVER_SOURCE_FINGERPRINT` to that exact value without changing
   the fingerprint material or bypassing validation;
3. passed the related identity/source tests and direct supported-computation
   congruency check; and
4. continues to treat every old source-bound history, warm-start, certificate, and runtime
   receipt as ineligible for the changed source rather than relabelling it.

The final supported fingerprint is
`585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`.
Caelum independently confirmed equality across the supported computation,
native constant and Python constant. This is a source identity, not proof of an
installed or loaded native artifact.

### R2 source-audit note

The current Control Center starts the collector and Pong host, installs sinks,
and then starts the Python listener. That is receiver-first inside the Python
process, but it does not itself establish a new native producer epoch. The
already-running native outbound owner has a static session, counters, and a
32,768-frame bounded queue. Its sender dequeues a batch before attempting the
loopback connection, counts failed batches as transport failures, and can leave
other older queued frames to be delivered after the listener becomes ready.
Automatic observer replacement advances the native publisher generation but
does not reset the outbound session/counters/queue.

Therefore an ordinary Control Center start or stale-callback observer
replacement is not yet the R2 release condition. Sol must identify or implement
a supported boundary that retires/attributes outstanding old-generation work
and begins a distinct authoritative producer generation only after receiver
readiness. No loss counter, queue, or signed record may be silently cleared or
relabeled.

For this pass, the narrower supported source-install plus operator F5 reload is
sufficient for R2 if its accepted external proof passes, even though
same-assembly menu/automatic reattach is not repaired. The proof is external
rather than an assumption about CLR unload:

1. preserve the old session/build/source, last durable/source cursors and loss
   counters, then prove the collector/listener is `LISTENING` before F5;
2. capture a first authenticated frame with a different session, the exact new
   source fingerprint, a newly observed actually-loaded build fingerprint,
   source generation, and absolute producer overflow/transport totals of zero;
3. require two advancing durable checkpoints with the same collector, new
   session/source/build/generation, zero stable producer deltas, no receiver
   rejection/duplicate/collector overflow, and exact disarmed Sim101/DEC26
   flat/no-order truth; and
4. reject any later return of the old session or identity bounce, and keep all
   pre-boundary/recovery observations outside the clean qualifying corpus.

The corrected commissioning workflow is **ACCEPTED FOR SOURCE/OFFLINE
INTEGRATION**. Its authenticated pre-reload receipt records the old identity,
absolute cursors/counters and raw journal absolute path, byte count and SHA-256;
commissioning recomputes that journal identity and records the SHA-256 of the
exact summary. It proves listener and collector readiness before reload,
requires a different authenticated session plus the exact new source and build,
and matches the signed loaded-build hash to an exact NinjaTrader temp-assembly
candidate rather than assuming `bin/Custom` content equality.

Both advancing checkpoints must retain the same new session/source/build/source
generation; report zero absolute new-session producer overflow and transport
failure and zero deltas; show stable receiver reject, duplicate and collector
overflow counts; show zero host missing/out-of-order and observation-queue
overflow; and retain null host input error, persisted first fault and telemetry
write error. The usable tail must be exact available 10x10 with no partial-
available publication, while Sim101/LOCAL_SIMULATION/MNQ DEC26 remains flat,
disarmed, reconciled and without owned or entry orders. Recovery history is
explicitly checkpoint-1 baseline to checkpoint-2 current with all deltas zero;
the clean corpus begins `AFTER_CHECKPOINT_1`, and Gate B deltas begin at
`CHECKPOINT_2`.

`Shutdown()` still clears the old outbound queue without reporting its exact
undelivered count. The workflow therefore labels that tail
`FAILED_UNKNOWN_PENDING_TAIL_NOT_DRAIN_PROOF`; a distinct new session starting
at genuine zero is allowed, but is neither a relabel nor proof that the old
queue drained. PowerShell parsing, probe compilation, and the focused source,
identity and native-cursor tests pass. Sol subsequently exercised this workflow;
the receiver-first identity/lifecycle portion passed, while the clean-runtime
acceptance failed as recorded below.

Same-assembly menu/automatic replacement remains unsupported for a qualifying
epoch. Its readiness snapshot would need an atomic callback-retire/drain fence,
receiver-observed authenticated barrier, and exact ledger schema support;
automatic recovery cannot bypass it. Do not exercise that path in this pass.

### R2 runtime closeout and acceptance

Sol alone performed the authorized receiver-first reload, short diagnostic and
supported stop at commissioning HEAD
`ac9b44e9d178d37c8fa1844fc36dd312a7651339`. Caelum independently re-read the
resulting receipts and hashes:

- commissioning receipt
  `H:\PongPeriodic\NoFHBQualificationReadiness-20260921\R2-commission-585a-20260921T153900Z.json`,
  SHA-256
  `df530f25afee14474898cb516ff8ad37c0a5e12cede908bf2f0870e6439522e7`;
- short diagnostic receipt
  `H:\PongPeriodic\NoFHBQualificationReadiness-20260921\R2-short-natural-chain-20260921T154100Z.json`,
  SHA-256
  `14662f1c4efe58c9c443944c2f74c80d1da74acde678b6e670efb398e218f7eb`;
- first-fault host telemetry
  `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\host-telemetry\first-fault.json`,
  SHA-256
  `e52cdbf8ae1a2f61ab62a3ad21308301ea0ad212942bfb065cf8a74eab1de68a`;
- final host telemetry
  `N:\Beelzebub\runtime\pong-production\pong\MNQZ6\v1\host-telemetry\final.json`,
  SHA-256
  `deba075a3bbf9a7ff3dc6b3117943614755f999d3c6b53dc0bf020a62fd352ea`;
- final collector status, SHA-256
  `565d751b7ffde65a681bcdf2213d9ab620e8dba3093d1ea2823fa0812bf36afa`,
  and its final journal, SHA-256
  `b5fea0bbf49cea7b0d738c84d82b36cd91c331ddde99c17472b594aecb25348d`;
- incremental ledger closeout
  `N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T154300321406Z-no-fhb-r2-canonical-fault-closeout-20260921T1542Z.json`,
  SHA-256
  `1b46e2800dbdac23b2bb0a0909cf85355bcd3d991892711705800222deaf6994`.

The receiver-first identity boundary itself passes: listener and collector were
ready before reload, the exact loaded temp assembly was observed, and both
advancing checkpoints retained session
`46adca83becf42e59e4356bb5829c88b`, source
`585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`,
build `70a1762e68d34ef12ff98c4ab1ca2b86619b4d02f52a2136454cd14e8c5eb759`,
and source generation 1. Sim101 remained flat, disarmed and without orders.
The old cleared tail remains `FAILED_UNKNOWN_PENDING_TAIL_NOT_DRAIN_PROOF`.

R2 as a clean authoritative initialization **fails and remains on HOLD**. The
commissioning receipt is `verified=false`; receiver rejects increased by 2,381
between checkpoints and the host had already latched
`PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK`. Independently reading journal
record 149 shows the exact seam: DEPTH source sequence 144, authenticated and
signature verified, reports a Remove on Ask position 11 with no resolved price
and explicitly marks the observation
`UNAVAILABLE / NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED`. The native producer is
truthful; the host must not demand that unresolved mutation be present in its
post-book. Any correction must continue to fail closed and must not make this
record analytically available.

The later diagnostic confirms a second independent blocker. Across 24.1676
seconds, DEPTH ingress was 305.078/s and periodic opportunities 43.9845/s, but
there were zero full evaluations, 907 late/protection supersessions, 156
obsolete deadlines, producer overflow `+6335`, transport failure `+0`, and
source age ended at 81.321 seconds. Final producer overflow was `+45772`.
Although the final collector durably drained `100472/100472/100472`, with zero
collector overflow and transport failures, durability cannot erase upstream
producer loss or the latched host fault. The ledger verifier's PASS through
sequence 233998 is likewise narrow closeout integrity, not R2, Gate B, or
strategy readiness.

The 24.1676-second run is classified `FAIL_STOP_AT_CANONICAL_BOOK_STAGE`, a
diagnostic after failed initialization. It is not a Gate B attempt, is shorter
than the authorized 3–5 minute window, and cannot be extended or relabelled.
Gates B, C and D were not run.

### R3 corrective-revision review

The final corrective revision was **ACCEPTED FOR SOURCE/OFFLINE INTEGRATION** and
is now **PASS IMPLEMENTED/RUNTIME OBSERVED** for its instrumentation contract. It
has fixed atomic start/first-fault/checkpoint/final
slots, factual clock-domain separation, exact canonical `10x1` and other early
fault coverage, five-second retry bounding, current oldest pending age, actual
runtime authority fields, monotonic eligible/protected accounting, a refreshed
and honestly labelled pre-collector-stop final snapshot, and bounded service,
processing, analysis and receipt-write aggregates. Unavailable receiver/lock
metrics are explicitly labelled `MISSING`, not fabricated. Caelum independently
ran all 20 focused tests successfully, plus Python compilation and diff checks.

The initially found high-water race is repaired. Producer admission holds the
bounded admission lock across the nonblocking queue put and logical occupancy/
high-water increment. A consumer may dequeue the item but then waits for that
same lock before decrementing logical occupancy, so every successful admission
is counted. The forced interleaving now reports admitted `1`, final logical and
physical occupancy `0`, and high-water `1`. No disk I/O or unbounded waiting was
added to the external callback. The resulting first-fault and final runtime
receipts captured the failed epoch accurately; this verifies telemetry behavior,
not R2 or Gate B readiness.

### R1 first-revision review

**Initial decision:** `PASS` for source/offline semantics and accepted for
integration. The subsequent supported reload installed/loaded the matching
producer, and record 149 exercised the repaired unresolved-Remove behavior.

Observed behavior:

- the production cursor is still committed exactly once before availability
  checks, so unavailable events do not roll back native positional truth;
- a Remove whose deleted price is unknown advances known positions but emits
  `NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED` as unavailable rather than inventing
  an identity;
- a coherent prefix shorter than 10 on either side emits
  `NATIVE_DEPTH_EXACT_10X10_REQUIRED` as unavailable;
- crossed, empty-side, invalid-side, and broken-continuity paths remain
  unavailable/fail-closed; exact coherent 10x10 alone returns available.

Evidence review:

- retained journal 46–52 record/source/wire hashes and mutation identities match
  the embedded compact fixture; the compact fixture SHA-256 independently
  recomputes to
  `647be4f516a1a9c58a144b43e140be1d47dd7a0728f8d914081982ffc05a8837`;
- the extracted production C# methods and independent oracle cover journal 47
  and neighbors, mirrored 10x1/1x10, partial and crossed recovery, five repeated
  authentic seams, reset/sparse rebuild, known and unknown zero-price Removes,
  invalid-side terminality, and oracle sequence/generation faults;
- owner receipt reports 60 focused tests PASS; Caelum did not duplicate the
  compiler suite while concurrent owners were active;
- scoped `git diff --check` has no whitespace error (line-ending warnings only).

The final supported derived source fingerprint is
`585a57d4c8138665b6c5734fcb84608f16c64668b028ec960371d78957fa2344`.
The native producer, Python expectation and identity test constants all match.
The supported producer was subsequently installed and loaded, and authentic
journal record 149 confirms its unresolved-Remove publication is unavailable as
designed. This loaded R1 truth does not make the failed epoch clean or satisfy
R2/Gate B.

### Composite correction review

The first full `pytest tests -q` composite at integrated HEAD `a065176` used
`H:\CodexTestTemp\nofhb-baseline-a065176` and completed in 1,466.13 seconds:
1,545 tests and 531 subtests passed; six tests failed. Three failures correctly
encountered `PROFILE_SELECTION_CHECKOUT_MISMATCH` from the preserved N: selector
for the prior checkout, two exposed Phase-F2 source-contract integration defects
(`.Reverse(` and a stale reset literal), and one reproduced a Phase-F3 test
listener-start race. The production profile-selection guard was not weakened or
its operational state rewritten.

The correction is bounded and accepted:

- both read-only producers now perform descending insert shifts and ascending
  delete shifts with explicit ordered LINQ materialization, preserving cursor
  semantics while removing the forbidden execution-like lexeme;
- the Phase-F2 assertion requires truthful dynamic `e.IsReset` serialization;
- the Phase-F3 helper waits on the existing production
  `on_listener_started` callback, which occurs after the loopback listener is
  opened and ready, without changing shutdown or partial-frame classification;
- the exact prior failure set plus identity passed 7/7 in 3.76 seconds under an
  isolated selector root; the exact Phase-F3 test passed 10/10 bounded repeats;
  and the affected suite passed 173 tests and 38 subtests in 51.27 seconds;
- an initial affected rerun placed TEMP inside the selector root and was
  correctly rejected by `PROFILE_SWITCH_RISK_GUARD_LAYOUT_UNSAFE`; it is retained
  as a harness-layout error, not a product result; and
- Caelum independently passed 63/63 Phase-F2/native cursor/source/identity tests
  in 2.434 seconds and the exact Phase-F3 test 3/3 in 0.153–0.157 seconds per run.

Scoped diff checks are clean apart from line-ending notices. The combination of
the 1,545 unaffected full-suite passes, exact failure-set replacement, affected
suite and independent focused checks is sufficient composite correction
evidence; repeating the entire 24-minute suite is not required before the
mandatory-repair commit or controlled disarmed commissioning.

### R4 source-path review

The existing scheduler is the required owner; no second scheduler or forced
eligibility repair is justified. `_event_protection_reason_locked()` permits
ordinary periodic work only when scheduling is enabled, a runtime bridge exists
in a flat-wait state with zero actual quantity and no pending action, runtime
reconciliation is current, execution is resolved, positions and owned/entry
orders are zero, and no native directive is pending. Any absent, stale,
non-flat or unresolved field selects `EVENT_DRIVEN_PROTECTION`. The worker
services the periodic deadline only after applying any already-available queued
record and supersedes obsolete flat-mode opportunities while protection is
required; immediate protective processing remains event-driven.

This establishes source-path correctness, not R4 readiness. The runtime did
prove current Sim101 reconciliation, flat quantity, disarmed authority and zero
orders, but the canonical host fault left the scheduler continuously protected:
zero ordinary full periodic evaluations and increasing source age. No
admissible receipt proves exactly 2,016 completed five-minute bars in 16 ordered
chunks, the two immediately preceding completed UTC hours, or current DEC26
daily bias for this source. This is a runtime FAIL/HOLD for the attempted epoch,
not a reason to weaken eligibility or alter sealed Gate A evidence.

## Benchmark lock

- Lock owner and sole scheduler: **Sol**.
- State at intake: **CLOSED**; no measured workload is authorized before
  `BASELINE_R`, a valid clean corpus, and experiment preregistration.
- At most one measured workload may run on Atlas. During it, pause compiles,
  test suites, profilers, scanners, dependency builds, and load generators.
  Read-only reasoning may continue if it adds no material machine load.
- Before each timing run record exact commit/artifact/dependencies/corpus hash,
  CPU topology and clock/power observation, memory/free space, storage target,
  competing process inventory, scheduler/protection mode, and profiler/tracing
  state. Contaminated runs are retained and classified, not discarded.
- One untimed warmup and one short screen per configuration; final candidate
  comparison is exactly three paired alternating A/B runs. A demonstrated
  harness fault permits one replacement only; a losing/noisy result does not.
- Profiling is separate from acceptance timing. No operational ledger, account
  channel, or live listener is a benchmark sink.

## Holds

### CLOSED C1 — Gate A structural and semantic readiness

- **Evidence:** R1–R4 source/offline repairs, lifecycle proof contract,
  identities and bounded composite correction evidence are accepted.
  `BASELINE_R` is sealed. E1–E3 and E5–E7 are closed
  `NOT_APPLICABLE_WITH_EVIDENCE`; E4 is `PARKED_INCONCLUSIVE` after its fixed
  budget. No candidate earned integration, so the final source remains the
  reviewed baseline with unchanged identity.
- **Consequence:** Gate A passes. This grants only entry to controlled disarmed
  initialization; it is not Gate B, qualification, arming or order authority.
- **Closure:** every E1–E7 family has an evidence-backed disposition; the
  unsuccessful E4 prototype is excluded from the qualifying import/hash graph;
  final intended source, identity and composite review remain `BASELINE_R`.
- **Owner:** Sol coordinates; Caelum records acceptance decisions.

### HOLD H2 — Clean R2 initialization

- **Evidence:** the receiver-first reload produced a distinct authenticated and
  stable session/source/build/generation, but the signed commissioning receipt
  is `verified=false`. Authentic record 149 / DEPTH sequence 144 is truthfully
  unavailable because its Ask-position-11 Remove has unresolved identity; the
  host instead latched `PONG_CANONICAL_DEPTH_MUTATION_NOT_IN_POST_BOOK`.
  Receiver rejects increased by 2,381 between commissioning checkpoints.
  The diagnostic then added producer overflow `+6335`, ended with source age
  81.321 seconds, and final closeout recorded producer overflow `+45772`.
- **Consequence:** no clean authoritative corpus exists. The 24.1676-second
  fail-stop is not Gate B; Gates B–D remain unrun and prohibited.
- **Release:** offline-replay the preserved record-149 seam through the host,
  implement and independently test the smallest fail-closed consumer correction
  that treats the explicit unresolved mutation as unavailable without analytics
  until coherent recovery, and separately name and correct the producer
  overflow/source-age cause. Re-enter Gate A delta/identity review for the new
  cut, then obtain a wholly fresh R2 receipt with fault-free host state, stable
  zero reject/loss deltas, exact availability truth and clean durable boundary.
  An unchanged rerun is forbidden.
- **Owner:** Sol coordinates/integrates and remains sole runtime operator;
  implementation ownership is assigned by Sol.

### HOLD H3 — Qualification/readiness claim

- **Evidence:** Gate A remains PASS and the loaded producer now truthfully marks
  unresolved Remove identity unavailable, but R2 clean initialization failed as
  described in H2. Gate B was not run; formal capacity and rehearsal are absent.
  Safe flat/disarmed authority and narrow ledger/collector closeout do not
  substitute for those gates.
- **Consequence:** state remains `HOLD`; no valid qualification or paper-start
  time exists, and no readiness certificate can be reused.
- **Release:** Gates A–D all pass on one frozen candidate with current identities,
  disarmed/flat/zero-order authority, verified ledger closeout, and Caelum's
  evidence matrix review.
- **Owner:** Caelum review; Sol evidence and closeout.

Independent source review and focused offline work continue under all holds.

## Next single action

Return to a bounded offline corrective cut at the exact preserved boundary:
replay authenticated journal record 149 / DEPTH source sequence 144 through the
host and repair the consumer contract so
`UNAVAILABLE / NATIVE_DEPTH_REMOVE_IDENTITY_UNRESOLVED` advances/preserves the
truthful continuity boundary without demanding a resolved mutation, publishing
analytics, or weakening exact 10x10. The same cut must separately isolate the
producer-overflow/source-age cause before Sol considers another runtime epoch.
After focused and affected tests, Caelum reviews the delta and identity; only
then may Sol perform a fresh R2 commissioning. Gate B, capacity, rehearsal,
arming and orders remain unauthorized.
