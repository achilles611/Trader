# Caelum pass state — no-FHB qualification readiness

Pass ID: `20260921-135529Z-no-fhb-qualification-readiness`
Opened: `2026-09-21T13:55:29Z` / `2026-09-21T07:55:29-06:00`
Administrator: Caelum (`beelzebub_admin`)
Integrator, benchmark coordinator, and runtime operator: Sol parent
Current phase: mandatory repairs, Gate A
Overall verdict: **HOLD — Gate A and all runtime/performance promotion**

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
| 1 | R1 truthful native availability | Native owner; Sol integrates | **SOURCE/OFFLINE AND IDENTITY ACCEPTED; runtime HOLD** | The integrated patch preserves the single pre-gate cursor commit, makes incomplete prefixes and unresolved Remove identity explicit unavailable, and retains crossed/empty/invalid fail-closed paths. The Phase-F2 lexical correction preserves identical ordered shifts. Native, Python and computed source identities agree at `585a57d4...fa2344`; focused and affected composite receipts pass. No installed/loaded/runtime acceptance exists. |
| 1 | R3 persisted host telemetry | Python telemetry owner; Sol integrates | **SOURCE/OFFLINE ACCEPTED; integration/runtime HOLD** | The final bounded revision covers the prior clock, first-fault, retry, exact queue occupancy/high-water/age, authority, scheduling-duration, final-stage and bounded-aggregate findings. Admission and dequeue bookkeeping now share one bounded logical-occupancy lock, and the forced drain race proves one admitted item still raises high-water to one. Caelum independently passed all 20 focused tests, Python compilation and diff checks. Composite integration and runtime receipts remain. |
| 2 | R2 supported receiver-first source lifecycle | Sol only | **SOURCE/OFFLINE ACCEPTED; OPERATOR/RUNTIME HOLD** | The corrected commissioning workflow is accepted for integration: it hash-binds the pre-reload receipt and raw journal, proves receiver readiness first, and requires a distinct authenticated session/source/build with stable generation, zero loss totals/deltas, fault-free host state, exact 10x10 truth, safe authority and two advancing durable checkpoints. The old assembly's unknown cleared tail remains explicitly failed, not drained. Release still requires the actual operator F5 compile/load and a signed passing receipt; same-assembly reattach remains unsupported. |
| 2 | R4 genuine periodic eligibility/current context | Sol only | **SOURCE PATH ACCEPTED; CURRENT-EVIDENCE HOLD** | The source audit found no scheduler defect: periodic work is owned by the existing deadline only when bridge state, runtime reconciliation/execution/position/order truth and pending-native state are independently cleanly flat; every unresolved or exposed state remains event-driven protection. The new source identity still lacks admissible 2,016-bar history, the two preceding UTC hours/current DEC26 bias, and same-epoch reconciliation/risk/ledger evidence. Release requires those actual dependencies and active periodic evidence; never force flat/ready. |
| 3 | Seal `BASELINE_R` | Sol, after Caelum review | **ACCEPTED TO SEAL AT CORRECTION COMMIT** | Mandatory-repairs-only no-FHB source, final identity, independent review and bounded composite correction evidence are complete. No full-suite replacement is required before the seal. The resulting commit becomes the optional-experiment reference; `efea615` remains negative evidence, not a timing baseline. |
| 4 | Optional experiment screens | Assigned disjoint owner; Sol holds benchmark lock | **NOT STARTED** | Execute only applicable preregistered E1–E7 candidates in `EXPERIMENTS.md` against `BASELINE_R` and a clean corpus. |
| 5 | Composite integration/freeze | Sol | **BLOCKED** | Accepted candidates together pass semantic/security/durability regression and retain FHB-null identity. Freeze before runtime evidence. |
| 6 | Gate B short natural proof | Sol only | **BLOCKED** | One predeclared 3–5 minute supervised disarmed proof after valid initialization; persisted start/fault/end evidence and all Gate B dimensions must pass. |
| 7 | Gate C formal capacity | Sol only | **BLOCKED** | Same frozen candidate must meet unchanged `2111/s`, `compute_workers=0`, three sustained plus three burst repeats, zero loss/reject/duplicate/overflow, bounded queues, durable drain, and wire/journal/decision parity. |
| 8 | Gate D 20-minute rehearsal | Sol only | **BLOCKED** | Only after Gates B and C pass; one unchanged natural disarmed rehearsal with current context and persisted receipts. |
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
identity and native-cursor tests pass. Installed, loaded and runtime-verified
states remain false until the operator F5 action and signed receipt complete.

Same-assembly menu/automatic replacement remains unsupported for a qualifying
epoch. Its readiness snapshot would need an atomic callback-retire/drain fence,
receiver-observed authenticated barrier, and exact ledger schema support;
automatic recovery cannot bypass it. Do not exercise that path in this pass.

### R3 corrective-revision review

The final corrective revision is **ACCEPTED FOR SOURCE/OFFLINE INTEGRATION**. It
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
added to the external callback. Installed, loaded and runtime-verified states
remain false until Sol integrates and Gate B produces the actual receipts.

### R1 first-revision review

**Decision:** `PASS` for source/offline semantics and accepted for integration;
installed, loaded, exercised, and runtime-accepted states remain false.

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
R1 remains uninstalled, unloaded and runtime-unverified until the supported
operator compile/load and commissioning receipt pass.

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

This establishes source-path correctness, not R4 readiness. The R1 source
fingerprint change invalidates prior source-bound volume history and warm-start
evidence. No admissible receipt yet proves exactly 2,016 completed five-minute
bars in 16 ordered chunks, the two immediately preceding completed UTC hours,
current DEC26 daily bias, or same-epoch Sim101 reconciliation and risk/ledger
continuity for the new identity. Those artifacts must be acquired after the
supported reload and revalidated at final closeout. This is a runtime evidence
HOLD, not a blocker to sealing the mandatory-repairs-only `BASELINE_R` source
identity.

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

### HOLD H1 — Gate A experiment and final integration closure

- **Evidence:** R1–R4 source/offline repairs, lifecycle proof contract,
  identities and bounded composite correction evidence are accepted. E1–E7
  applicability and bounded outcomes are not yet closed, and no final composite
  experiment candidate exists.
- **Consequence:** `BASELINE_R` may be sealed and used for the preregistered
  screens, but Gate A and Gates B–D remain closed.
- **Release:** every E1–E7 family has an evidence-backed executed disposition or
  `NOT_APPLICABLE_WITH_EVIDENCE`; demonstrated winners, if any, are integrated;
  final identities and composite review pass.
- **Owner:** Sol coordinates; Caelum records acceptance decisions.

### HOLD H2 — Runtime/native maintenance

- **Evidence:** source/offline repairs and the R2 workflow are accepted, but the
  correction commit is not yet sealed and no supported receiver-first
  new-generation lifecycle receipt exists for it.
- **Consequence:** no native install/load or natural Gate-B proof may begin from
  the unsealed working tree. Formal capacity and rehearsal remain prohibited.
- **Release:** Sol seals the exact reviewed correction as `BASELINE_R`, rechecks
  its source identity and clean intended diff, brings listener and collector to
  verified readiness, then uses the Windows operator notification for the one
  genuine F5 compile/load action. The signed R2 receipt must pass before the new
  epoch can seed a clean corpus or R4 context.
- **Owner:** Sol.

### HOLD H3 — Qualification/readiness claim

- **Evidence:** the last proof had producer loss, gaps, stale backlog, false
  available partial depth, absent host receipts, no accepted periodic path, and
  missing current-source context. The formal capacity series is absent.
- **Consequence:** state remains `HOLD`; no valid qualification or paper-start
  time exists, and no readiness certificate can be reused.
- **Release:** Gates A–D all pass on one frozen candidate with current identities,
  disarmed/flat/zero-order authority, verified ledger closeout, and Caelum's
  evidence matrix review.
- **Owner:** Caelum review; Sol evidence and closeout.

Independent source review and focused offline work continue under all holds.

## Next single action

Seal the exact reviewed correction and Caelum pass updates as `BASELINE_R`, then
reverify the committed `585a57d4...fa2344` source identity and intended diff.
Only after that seal may Sol execute the accepted receiver-first commissioning
workflow and request the one operator F5 compile/load action. Remain disarmed;
no measured optional experiment, Gate-B window or order action is authorized by
this step.
