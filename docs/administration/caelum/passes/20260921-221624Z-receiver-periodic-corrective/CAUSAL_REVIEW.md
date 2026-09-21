# Causal finding and corrective revision review

Review time: 2026-09-21 UTC
Decision: **ACCEPT_FOR_IMPLEMENTATION_AS_THE_SOLE_CORRECTIVE_REVISION, WITH
CONSTRAINTS**
Gate decision: **GATE_A_DELTA REMAINS HOLD PENDING IMPLEMENTATION AND EVIDENCE**

## Evidence reviewed

**OBSERVED:** retained candidate receipt
`N:/Beelzebub/receiver-repair-evidence/sealed-sustained-candidate/result.json`
contains the following aligned 10,000-record facts:

- scheduled span 4.736617717 seconds;
- receiver completion elapsed 24.417237900 seconds;
- receiver end lag 19.680620183 seconds;
- sender socket writes total 18.207599601 seconds, maximum 4.243437800
  seconds;
- strict decode/admission total 3.799287 seconds, maximum 48.548 ms;
- downstream composite total 11.104618 seconds, maximum callback 83.349 ms;
- dispatch-stage paper fanout total 10.602802 seconds, collector enqueue
  0.268224 seconds, host enqueue 0.056282 seconds;
- paper fanout is 95.48% of the measured downstream composite time;
- all 10,000 required records were accepted; prior acceptance records ordered
  and durable reconciliation.

**OBSERVED:** the 4.243-second sender `sendall` maximum is not evidence of one
receiver callback or lock held for 4.243 seconds. The receiver's maximum
downstream callback is 83.349 ms. The socket value is consistent with cumulative
sender-side backpressure while the synchronous receiver repeatedly services
downstream work.

**OBSERVED:** current `LaneIIIPaperRuntime.ingest` calls the legacy
`ExperimentalPaperPolicy.ingest_runtime` for every market observation, then
sets its decision to `None` for the Pong profile. The same method also performs
required session/time validation, stale-callback handling, freshness/mark
updates, gap/reset/warmup state, commissioning authority-epoch transitions and
risk-exit maintenance. The optimization may remove only the proved redundant
legacy Pong policy calculation; it may not skip those shared safety duties.

**OBSERVED independent residual:** the host worker required 17.791 seconds for
10,000 records and 14.369 seconds of analysis in the reviewed parent finding;
the retained receipt has host queue high-water 1,484 and maximum age 4.078
seconds, while the collector maximum age is 2.437 seconds. Removing paper
hot-path waste is well-supported, but it does not by itself prove sustainable
host analysis or a passing periodic architecture.

## Accepted revision shape

The one correction may add an exact, explicit **qualifying disarmed Pong market
ingest** path in `src/l3g_paper/runtime.py`, selected only by the already exact
Control Center route in `src/copytrade/control_center.py`. It may bypass the
legacy policy calculation whose output Pong immediately discards. It may not
add a queue/thread, alter capacity, suppress required records, weaken
freshness/depth/protection, or restore duplicate shadow processing.

This is accepted as one coherent revision because the selector and runtime
implementation are two sides of one route contract, not two independent
behavioral changes. Instrumentation and focused tests that only prove this
route do not constitute additional corrective revisions.

## Binding constraints

1. **Exact construction-time opt-in.** The fast path is disabled by default and
   admitted only when all of the following are already validated together:
   qualifying architecture `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`, exact
   Pong profile, DEC26 paper/Pong topology, 20 ms protected-periodic mode, and
   activation authorization false. Do not select it from an observation field
   or a loose profile-name check.
2. **Market-only.** Only authenticated `DEPTH`, `QUOTE` and `TRADE` callbacks may
   use it. `ACCOUNT`, `POSITION`, `ORDER`, `EXECUTION`, `HEALTH` and every
   transport/rejection/duplicate/control callback retain the full established
   path and authority ledger semantics.
3. **State drift fails closed.** If exact profile/policy/risk/topology identity
   is absent, activation becomes authorized, the runtime is not in the
   explicitly allowed disarmed state, or another route invariant changes, the
   implementation must refuse the fast selection or use the complete safe
   fallback. It must never silently skip work under an unknown state.
4. **Retain session and continuity.** Preserve contract/session resolution,
   event-time monotonicity refusal and incident evidence, local monotonic
   sequence gap behavior, reset/reconnect handling, and warmup invalidation/
   recovery. The prior available -> unavailable -> coherent exact 10x10 repair
   remains unchanged.
5. **Retain freshness and runtime projection.** Preserve quote/trade/depth
   timestamps, last valid marks, mark-to-market, market-price connectivity,
   evidence-warmed/depth-recovery/local-gap fields, commissioning warmup and
   authority-epoch advancement. The Pong host must receive the same safety and
   reconciliation snapshot values as before for equivalent input.
6. **Retain risk/protection.** Run required risk-exit and position/protection
   maintenance on the same events and preserve immediate callbacks. Disarmed
   does not authorize removing risk-reducing behavior. A non-flat, pending,
   order-bearing, unresolved or unreconciled state must not remain on a path
   that assumes harmless flat market maintenance.
7. **Retain durability ownership.** The collector remains durable owner of all
   authenticated raw market observations. Paper continues its existing
   suppressed-market-envelope policy only; controls remain durably retained.
   Required paper callback exceptions still propagate and fail the receiver
   even if incident recording fails.
8. **No favorable-counter shortcut.** Do not use the current
   `depth_events_per_analytical_evaluation` field as the accepted ratio. The
   revision must not count protected event evaluations or blocked/incomplete
   periodic attempts as ordinary full evaluations.
9. **Observable fallback.** Expose a bounded route/counter or focused receipt
   proving how many observations used fast ingest versus full fallback and why
   fallback occurred. Do not add per-event logging to the timed path.
10. **No threshold substitution.** Raw records/s, percentage improvement and
    queue drain are diagnostics only. They cannot replace the still-missing
    accepted ratio threshold or the 1,000 ms freshness/equilibrium contracts.

## Minimum evidence before Gate A delta can change

- focused positive/negative route-selection tests for every identity and state
  predicate above;
- control-record full-path retention and ledger proof;
- session reversal, sequence gap, depth reset/unavailable/recovery, reconnect,
  failure propagation and supported stop/drain regressions;
- identical safety snapshot/risk/protection outcomes between old and fast path
  for equivalent qualifying-disarmed market inputs;
- aligned before/after receiver service with offered span, completion lag,
  socket-stall distribution, decode/downstream stage time and receiver queue;
- aligned host and collector queue high-water/age, source freshness and durable
  reconciliation, demonstrating that work was removed rather than displaced;
- a legitimate healthy-flat full-path scenario and a separate legitimately
  protected scenario, with raw periodic/scheduler terminal counts;
- corrected ordinary-full ratio measurement semantics; numeric acceptance
  remains held until an attributable threshold/tolerance is located or decided;
- accepted old-producer isolation and supported capacity-latch recovery plan.

## Budget and consequence

**OBSERVED administrative disposition:** this proposal is the one
evidence-supported corrective revision selected for implementation. Before
behavioral implementation starts it is `SELECTED_NOT_IMPLEMENTED`; once the
owner edits the behavior it consumes the remaining corrective revision budget.
No second behavioral optimization is authorized by this acceptance.

The decision does **not** release Gate A, installation/reload, R2 or downstream
gates. Even a large receiver improvement leaves the independently measured host
analysis path and exact periodic-ratio acceptance to be proved. Parent remains
the implementation integrator and sole runtime operator.

## Final return review

**OBSERVED:** the owner implemented the accepted shape as corrective commit
`448081aa055279c0ce516b5fb948c4b2530c2de7`. Tool-only commits
`ada2420ca1182006bbdffc6ced0969307f16ea32` and
`28fa75290151c4716f9c40bfef91eac647bfbdf1` reconcile evidence and retired
acceptance semantics; they are not additional behavioral revisions. The
aligned pair selected the fast path for all 10,000 records with zero fallback
and improved completion by 17.534%, receiver lag by 6.176984 seconds and paper
fanout by 5,983.953 ms. Both runs reconciled order and durability.

**HOLD remains:** fast-path lag is still 25.229554 seconds, collector maximum
age is 10,938 ms, both runs remain protected with zero ordinary periodic
evaluations, and the numeric ratio threshold is still unknown. The source
revision is accepted offline; Gate A is not released. See `ACCEPTANCE.md` and
`EVIDENCE_REGISTER.md`.
