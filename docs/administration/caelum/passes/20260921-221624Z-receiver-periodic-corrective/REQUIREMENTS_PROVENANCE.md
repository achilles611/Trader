# Requirements and provenance — receiver periodic corrective

Status: finalized recovery record, 2026-09-21. Every statement below is tagged
`OBSERVED`, `INFERRED`, or `UNKNOWN`; no threshold is supplied by arithmetic.

## Supersession decision

**OBSERVED:** Joseph's current handoff explicitly retires the fixed
`2,111 records/s` requirement and any `5.15x` target derived from it. It orders
recovery of the accepted periodic ratio and says that, if the exact replacement
cannot be recovered, ratio-dependent acceptance and runtime release must be
held rather than reinstating 2,111 or inventing a substitute.

Source: `C:/Users/atlas/.codex/attachments/7803e6fd-1255-4b34-aaee-cbaf3085d771/Pasted text.txt`,
SHA-256 `426c290f4464f887d27c518b14979da881c9217851c5bb3ef49fd1002d32c8e7`,
especially lines 11-13, 67-73 and 87-113.

**OBSERVED historical contradiction:** the latest committed receiver-repair
requirements and acceptance still call 2,111/s unchanged and hold capacity
against it. The historical isolated contract at
`H:/Trader-pong-serial-measurement/docs/operations/pong-serial-acceptance-v1.json`
sets `target_events_per_second=2111` and three sustained/three burst repeats.
Those files remain provenance, not current acceptance authority.

**OBSERVED intake executable/tool contradictions:** at pass intake, the tools
contained 2,111 defaults or receipt labels:

- `tools/pong_receiver_comparison.py:259` defaults `--rate` to 2111;
- `tools/pong_periodic_paced.py:175,340,529,593` defaults/labels 2111 and its
  help still calls it the acceptance floor;
- `tools/pong_periodic_fidelity.py:319` writes
  `target_events_per_second=2111`.

These were not production source enforcement, but they could generate stale
acceptance-looking receipts and therefore were not usable as gates in this
pass.

**OBSERVED final reconciliation:** comparison-harness commit
`ada2420ca1182006bbdffc6ced0969307f16ea32` removed its fixed-rate default, and
acceptance-tool commit `28fa75290151c4716f9c40bfef91eac647bfbdf1`
removed the remaining 2,111 defaults/labels and the misleading acceptance-floor
claim. At final evidence HEAD, a burst rate is an explicit diagnostic coordinate,
natural mode remains no-rate, and fidelity reports
`fixed_ingress_acceptance=RETIRED_NO_NUMERIC_THRESHOLD_AUTHORIZED`. A search of
the three reconciled tools found no `2111`, `5.15`, or
`target_events_per_second`. These harness/tool commits are acceptance-evidence
reconciliation, not additional receiver corrections and not a replacement
numeric contract.

## Accepted ratio: recovered and missing fields

| Contract field | State | Recovered value / evidence |
| --- | --- | --- |
| Exact name | **OBSERVED** | `Critical architecture ratio` |
| Numerator | **OBSERVED** | DEPTH events in the aligned measurement interval |
| Denominator | **OBSERVED** | ordinary full analytical evaluations in the same interval |
| Formula | **OBSERVED** | `DEPTH events : ordinary full analytical evaluations`, equivalently DEPTH count divided by ordinary-full count for reporting |
| Units | **INFERRED from exact operands** | DEPTH events per ordinary full analytical evaluation; the raw colon pair and both raw counts must also be reported |
| Applies when | **OBSERVED** | eligible healthy ordinary periodic mode; protected operation is a separate required case |
| Measurement interval | **OBSERVED** | predeclared 3-5 minute natural full-host Gate B window after clean R2 |
| Warmup/start exclusion | **OBSERVED** | clean corpus begins only after authenticated baseline checkpoint; startup/recovery tail is not Gate B |
| Intended behavior | **OBSERVED** | ordinary healthy evaluation rate tracks approximately the 20 ms cadence rather than raw DEPTH frequency |
| Numeric threshold/tolerance | **UNKNOWN** | no accepted number located in decisions, Git history, qualifying source or tests |
| Numeric acceptance direction | **UNKNOWN** | the quotient varies with natural DEPTH rate; neither smaller nor larger alone is an accepted result |
| Minimum qualitative condition | **OBSERVED** | genuine ordinary periodic evaluations greater than zero; queue/freshness/durability/safety conditions pass simultaneously |

Primary decision source:
`C:/Users/atlas/.codex/attachments/256301c8-eb13-4064-9118-33ee42eaefa8/Pasted text.txt`,
SHA-256 `a29e0df0014096326acc770fbb414f32094a7dc49dbb089c85be501c29c33c44`,
lines 620-687. Lines 636-654 name the raw periodic metrics and exact ratio;
lines 667-687 define simultaneous Gate B conditions.

Earlier architectural decision source:
`C:/Users/atlas/.codex/attachments/daddbfd8-a0a3-4607-8103-4780944a8432/Pasted text.txt`,
SHA-256 `52996a4496fa7aa2d9cd02df85370c6b392d7c6090a6f7e8fc129c69412356bb`,
lines 277-284 and 317-345. It establishes that DEPTH maintains state but does
not independently trigger ordinary full analysis while healthy periodic mode
is active; 20 ms ticks own ordinary analysis; protection remains immediate.

**UNKNOWN:** no versioned repository decision recording the threshold was
found. The project failed to preserve a complete replacement acceptance
contract. Joseph should not be asked to reconstruct a formula from memory, but
an explicit decision is the narrow release path if no missing record is found.

## Opportunity treatment from current implementation

The scheduler was introduced by commit
`b48bc4f8692619ac75dff703c997628a6b659c23` (`Integrate protected periodic
Pong scheduling`, 2026-09-18). Source: `src/l3g_paper/pong_scheduler.py`.

- **OBSERVED:** `scheduled_opportunities` includes the current due opportunity
  plus any obsolete deadlines skipped when the owner wakes late.
- **OBSERVED:** `late_evaluations` counts a current due opportunity whose start
  is after its absolute deadline. It is not itself a count of full evaluations.
- **OBSERVED:** `skipped_obsolete_deadlines` accounts missed past grid points;
  they are not dispatched and cannot enter the ordinary-full denominator.
- **OBSERVED:** `protection_mode_supersessions` terminally accounts a current
  opportunity when protection owns the path; it is not an ordinary evaluation.
- **OBSERVED:** `shutdown_deadlines` terminally accounts a due opportunity at
  shutdown.
- **OBSERVED:** `completed` counts calls to `complete()`, including
  `intentional_no_work` and `applied=False`. It is not synonymous with an
  ordinary full analytical evaluation.
- **OBSERVED:** `deadline_reconciliation` checks only that dispatched + obsolete
  + protection-superseded + shutdown equals scheduled. It is an accounting
  identity, not a cadence/ratio PASS.
- **OBSERVED:** legitimate protected periods must be reported separately. The
  decision explicitly forbids manufacturing 50/s when eligibility blocks.
- **INFERRED:** ratio receipts need aligned deltas rather than lifetime totals,
  and must retain protected/eligible elapsed time plus every scheduler terminal
  class so no interval is silently excluded. This follows from the decision's
  separate raw-count requirement and current cumulative counters.

## Executable ratio mismatch

Commit `efea61561e128bc44632def5d7c135e6ea7bacd9` (`archive FHB from qualifying
runtime`, 2026-09-20) added
`evaluation_schedule.depth_events_per_analytical_evaluation` at
`src/l3g_paper/pong_production.py:2964`.

**OBSERVED defect:** its numerator is total DEPTH stream count, but its
denominator is `_microstructure_evaluations`. That counter is incremented by
the shared analysis routine after canonical-depth readiness and can therefore
include both ordinary periodic and event-driven protected evaluations. The
accepted denominator is specifically ordinary full analytical evaluations.

**OBSERVED second ambiguity:**
`periodic_flat_analytical_evaluations` increments before
`_timed_evaluate_microstructure`; canonical-depth unavailable can then return
before `_microstructure_evaluations` increments. Therefore this counter proves
a periodic analysis attempt, not necessarily a completed *full* analytical
evaluation under the accepted wording.

**OBSERVED test gap:** current scheduler/production tests verify absolute-grid
accounting, protection supersession and that periodic mode owns healthy flat
analysis. They do not define or enforce a ratio threshold, aligned measurement
window, or accepted ordinary-full denominator.

**Consequence:** neither current ratio field nor scheduler `completed` may be
used unchanged as the accepted denominator. Alignment is an implementation
owner task; Caelum has no application/test write authority.

## Independent current requirements

### Scheduling and architecture

- **OBSERVED:** `src/l3g_paper/pong_qualifying.py:7-16` binds
  `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`,
  `PERIODIC_FLAT_PROTECTED`, 20 ms, 1,000 ms queue age, 1,000 ms event age,
  5-second source heartbeat and 5-second persistent breach threshold.
- **OBSERVED:** `src/l3g_paper/pong_production.py:347-359` rejects a qualifying
  construction unless the selected profile, scheduling mode, 20 ms interval,
  DEC26 topology, archived-FHB state and no-activation condition all match.
- **OBSERVED:** FHB remains archived/excluded; E4 is not reopened by retirement
  of the fixed throughput floor.

### Freshness

- **OBSERVED:** qualifying host service enforces both queue age and source
  receipt age against 1,000 ms at
  `src/l3g_paper/pong_production.py:1129-1168`, and exposes the current and
  maximum values in persisted telemetry.
- **OBSERVED:** a clean Gate B additionally requires bounded/recovering source
  age, bounded host queue age, queue movement toward baseline while ingress is
  open, and no persistent freshness breach. Final empty queues after shutdown
  do not establish these facts.

### Exact depth and continuity

- **OBSERVED:** qualifying available depth requires exactly 10 bids and 10 asks
  and a non-crossed ordered book (`pong_production.py:1510-1581`).
- **OBSERVED:** explicitly unavailable depth invalidates canonical readiness
  truthfully; it does not manufacture a partial available book
  (`pong_production.py:1559-1567`).
- **OBSERVED:** reset, stream gap/generation discontinuity, ambiguous mutation
  identity or inconsistent retained-book mutation fail closed; a coherent exact
  snapshot may re-establish readiness. Out-of-core mutations preserve the
  signed exact 10x10 core without manufacturing a visible price.
- **OBSERVED:** Gate B still requires zero source gaps, duplicates and receiver
  rejects, and zero available partial books.

### Durability and delivery

- **OBSERVED:** the accepted receiver source repair preserves every required
  market/control delivery, authenticated order, required-consumer failure
  propagation and collector durability; only duplicate shadow market
  projection was removed from the qualifying route.
- **OBSERVED:** `src/l3g_paper/pong_collection.py` advances durable count only
  after journal flush/fsync and performs a final durable checkpoint on stop.
- **OBSERVED:** current Gate B requires durability to advance throughout and
  final ledger verification/drain. A final drain is necessary but cannot erase
  preceding service lag or prove open-ingress equilibrium.

### Protection

- **OBSERVED:** `pong_production.py:1258-1291` permits ordinary periodic work
  only with periodic configuration, a present durable bridge in one of the two
  flat-wait states, zero actual quantity/pending action, current runtime
  reconciliation, no unresolved execution, zero positions/orders, and no
  pending native directive.
- **OBSERVED:** otherwise the authenticated event path remains authoritative.
  Protection-superseded deadlines and event-driven protection evaluations must
  remain separate and immediate; protection may not be suppressed to improve
  ratio numbers.

### Initialization / R2

- **OBSERVED:** the accepted R2 contract remains receiver and collector ready
  before producer; exact loaded build; distinct signed session/source/build/
  generation; two advancing durable checkpoints; zero absolute new-session
  producer overflow/transport totals; zero reject/duplicate/collector-overflow
  deltas; no host fault/gap/queue overflow; exact 10x10 or truthful unavailable;
  verified Sim101 LOCAL_SIMULATION, flat, disarmed, reconciled and no orders.
- **OBSERVED:** the clean corpus begins after checkpoint 1 and Gate B deltas
  begin at checkpoint 2. A later fault cannot be excluded by moving either
  boundary. Old tail and historical counters are preserved.
- **OBSERVED:** same-assembly reattach was not accepted as a distinct producer
  boundary. Old-producer isolation and supported recovery of the historical
  capacity latch remain prerequisites under current evidence.
- **OBSERVED final capacity distinction:** the selected suite passes fully when
  its temporary ledger is placed on N:, and the actual Pong runtime ledger is
  already on N: with ample observed free space. No migration is required.
  No supported same-ledger relocation exists; copying or moving the
  identity-bound ledger is not a recovery path.
  Historical lockout sequence 134169 nevertheless remains uncleared; its
  original `DEGRADED` subcause was not persisted, and historical flat
  reconciliation at sequence 274023 is not current runtime reconciliation.
  With no listener or authenticated reconciled bridge, supported clear
  prerequisites remain unproved and manual clear is prohibited.

Source: committed prior decision
`docs/administration/caelum/passes/20260921-213642Z-receiver-repair/REQUIREMENTS.md:21-25`
and the earlier detailed commissioning record it cites.

## Acceptance matrix at intake

| Requirement | Intake state | Reason |
| --- | --- | --- |
| Retire 2,111/s and 5.15x as gates | **PASS / OBSERVED decision** | Current Joseph instruction is explicit; stale artifacts identified |
| Ratio exact operands/name | **PARTIAL PASS** | Name/numerator/denominator recovered |
| Ratio threshold/tolerance/direction | **UNKNOWN / HOLD** | Not preserved in located authority; no guessing |
| Executable ratio semantics | **FAIL / HOLD** | Generic denominator includes protected evaluations; periodic count can include non-full attempts; corrective commit does not redefine the accepted ratio |
| 20 ms qualifying cadence | **PASS source identity only** | Implemented and construction-enforced; runtime not exercised in this pass |
| 1,000 ms freshness | **PASS source contract only** | Enforced constants/path; current runtime evidence absent |
| Exact depth truth/recovery | **PASS accepted source/offline** | Current source/tests and prior accepted repair; fresh natural proof pending |
| Ordered authenticated durability | **PASS accepted source/offline** | Final aligned pair reconciles 10,000/10,000 on both routes; sustained timely service remains failed |
| Immediate protection | **PASS source/offline semantics** | Final pair legitimately protected; healthy full-path ratio remains unmeasured |
| Clean producer initialization | **HOLD** | Distinct old-producer isolation and fresh R2 proof absent |
| Gate A delta | **HOLD** | Corrective benefit observed, but 25.229554 s lag, 10,938 ms collector age, protected-only runs, incomplete ratio, isolation and recovery gaps remain |
| Conditional R2 | **NOT RUN / 1 REMAINS** | Gate A delta PASS required first |

This matrix deliberately removes no blocker merely because it was previously
described under the retired throughput floor. Only the obsolete fixed-rate
reason is superseded; observed latency, semantic, initialization, freshness,
durability and safety requirements remain independently reviewable.
