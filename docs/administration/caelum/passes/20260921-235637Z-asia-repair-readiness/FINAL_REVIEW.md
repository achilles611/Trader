# Caelum final independent review — Asia repair readiness

- Review completed: `2026-09-22T01:06:14Z`
- Final evidence HEAD reviewed: `c886f505dca2973d83bac7c85ba8f3fd50a19c82`
- Integrated repair commit reviewed: `b4a46c2d73030f008e305a687069456c5d1bda0d`
- Integrated repair tree: `c3e5ca334e63f8332883a5f31ac9013785be905a`
- Starting commit from the repair packet: `04a1f22013a00cf595f41424f77f93e65cf62918`
- Review mode: independent source, test-receipt, and sealed-evidence inspection only; Caelum did not install, reload, launch, benchmark, mutate the ledger, arm, or place orders.

## Overall determination

**`UNRESOLVED REPAIR`**

The receiver instrumentation, periodic telemetry/protection accounting, and native teardown changes are present and supported by offline tests. The final evidence HEAD is congruent with the reviewed repair commit, and the operational packet is fail-closed. Those facts do **not** reduce this pass to only a policy decision: the coordinated predeclared comparison still reproduces a receiver/collector service and freshness deficit without identifying an immutable resource or architecture limit.

The candidate preserved exact 10,000-event ordering and durability with zero producer-overflow and transport-failure deltas, but completed at approximately `288.665 events/s`, ended with `24.643 s` of processing lag, and reached `11.469 s` of local collector residence age. Rate improved only approximately `0.095%` from baseline, while the collector-age maximum became worse. The comparison itself records `NOT_QUALIFIED`.

This determination does **not** elevate `1,000 events/s` into an approved acceptance threshold. The defect is the reproducible open-ingress backlog and age growth under the declared workload, not the absence of a policy-approved throughput number. A natural-feed window at a lower ingress rate could be diagnostically clean and still would not explain or close this repair finding.

## Concrete HOLD

**HOLD:** `HOLD_RECEIVER_COLLECTOR_SERVICE_DEFICIT`

- **Evidence:** the sealed baseline/candidate comparison has exact integrity and zero declared loss deltas, yet the candidate retains `24.643 s` end lag and `11.469 s` maximum collector-local age. The stage trace localizes material elapsed time to collector encode/hash, sender socket writes, paper fanout, and status persistence, but the intervals overlap and do not prove a single limiting resource. No concrete immutable resource or architecture limit is demonstrated.
- **Consequence:** do not classify the repair pass as `REPAIRS_COMPLETE_POLICY_DECISION_REQUIRED`; do not spend the sole conditional native reload attempt (`R2`) or treat a natural ratio window as repair closure. No transition to Gates B–D, paper start, arming, or orders is authorized.
- **Release condition:** either (a) integrate a targeted repair and produce a new predeclared, matched, frozen comparison that preserves exact ordering/durability and demonstrates bounded collector age with no persistent open-ingress lag under its declared applicable load, or (b) produce a reproducible, immutable resource/architecture-limit receipt and an explicitly approved plan that treats that limit honestly. A newly selected numeric service bound must have provenance and approval; it must not be reverse-fitted to the observed result. Caelum must independently review the resulting integrated commit and evidence.
- **Owner:** parent integrator coordinates the next source owner and remains the sole runtime operator.

Independent safe work may continue on the ratio-policy decision and preparation of a future preflight packet. The HOLD applies to claiming repair completion and to consuming the one-shot native/runtime qualification opportunity.

## Requirement classification

Every row uses one of the four required final classes.

| Requirement | Classification | Independent basis |
|---|---|---|
| Receiver/collector bounded stage instrumentation and one-second publication | **FIXED/VERIFIED OFFLINE** | Integrated source exposes fixed-bound histograms for enqueue, queue residence, record encode/hash, buffered write, state apply, flush/fsync, status publication, and sender stages. Focused tests and the sealed coordinated receipt exercise the fields. |
| Receiver exact-order, exact-count, durable completion, and declared loss accounting in the predeclared 10,000-event comparison | **FIXED/VERIFIED OFFLINE** | Baseline and candidate each record 10,000 completed/durable/ordered events with the same ordered digest, no reject/duplicate accounting, and zero producer-overflow and transport-failure deltas. This is narrow synthetic integrity proof, not feed qualification. |
| Receiver/collector open-ingress service equilibrium and freshness | **UNRESOLVED REPAIR** | Candidate is approximately 288.665/s with 24.643 s end lag and 11.469 s maximum collector-local age; improvement is immaterial, age regresses, and no immutable limit is demonstrated. |
| Periodic ordinary-versus-protected accounting, aligned snapshots, identity continuity, scheduler reconciliation, and cumulative transient-disqualifier handling | **FIXED/VERIFIED OFFLINE** | Integrated source computes the ratio only from ordinary completed full analyses and rejects identity, counter, clock, reconciliation, safety-delta, endpoint, and protection-transition faults. Commit `b4a46c2...` makes transient disqualifiers cumulative for the whole window. |
| Periodic/bridge healthy, protected, and recovered fixture behavior | **FIXED/VERIFIED OFFLINE** | Sealed receipt shows healthy `R=2.0`; protected window ineligible with null ratio and `PROTECTION_TRANSITION_DURING_WINDOW`; recovered window eligible; zero native dispatch and no paper-ledger instantiation. |
| Numeric ratio threshold, comparison direction, tolerance, and minimum denominator | **POLICY DECISION REQUIRED** | Integrated output intentionally records `threshold=null` and `threshold_reason=POLICY_NOT_DEFINED`. The observed `R=2.0` is evidence, not authority. The versioned proposal remains explicitly unapproved. |
| Native outbound lifecycle source: admission stop, exact socket-owner cancellation, bounded join, generation fencing, restart refusal while an old owner survives, and explicit no-ack diagnostic | **FIXED/VERIFIED OFFLINE** | Source inspection plus isolated lifecycle harness, callback-ordering checks, focused tests, and bundled Roslyn compile support the implementation. The final embedded fingerprint is synchronized with the Python observer expectation. |
| Native add-on installation, compile inside NinjaTrader, reload, selected assembly/source identity, distinct-producer proof, and real blocked-I/O teardown | **LIVE PREFLIGHT PENDING** | The native receipt explicitly proves source/harness/Roslyn behavior only. It does not prove installed, loaded, or native-runtime state. |
| Capacity-path topology and no-migration decision | **FIXED/VERIFIED OFFLINE** | The active ledger path is already on `N:`. The proposed `C:` to `N:` migration is therefore inapplicable; no ledger copy or rewrite is needed. |
| Durable capacity-latch recovery from sequence `134169` | **LIVE PREFLIGHT PENDING** | Recovery is correctly deferred until a current, authenticated, exact Sim101 flat/no-orders/reconciled/full-verification sandwich succeeds, followed by the supported recovery action and a second full verification. No ledger mutation occurred in this review. |
| Current account, positions, orders, reconciliation, listener/storage binding, feed continuity, and disarmed state | **LIVE PREFLIGHT PENDING** | Historical and offline receipts cannot establish the state at execution time. The runbook requires current full and read-only checks and aborts on mismatch. |
| Sole conditional reload attempt (`R2`) | **LIVE PREFLIGHT PENDING** | Budget remains `0/1`. It must not be consumed while the repair-completion HOLD remains active; after release, it still requires the runbook's current preflight gates. |
| Fixed natural-feed ratio window and source continuity | **LIVE PREFLIGHT PENDING** | No natural qualification was run. The runbook's fixed `22:05Z–22:10Z` window is diagnostic while policy is undefined and must not be slid, trimmed, backfilled, or rerun after observation. |
| Gates B–D, paper start, arming, and orders | **LIVE PREFLIGHT PENDING** | They were neither requested nor performed. The runbook explicitly states that its results grant no such authority. |
| Final integrated source/test identity | **FIXED/VERIFIED OFFLINE** | `b4a46c2...` is an ancestor of `c886f505...`; repair-code paths are unchanged between them, and the latter adds the runbook. Sealed focused JUnit reports 97 total cases (`68` tests plus `29` subtests), with zero failures/errors/skips. |
| Next-pass runbook dates, time-zone conversions, commands, one-shot budget, and fail-closed boundaries | **FIXED/VERIFIED OFFLINE** | The Tuesday profile conversions are internally consistent: preflight `21:30Z`, session `22:00Z`, ratio `22:05Z–22:10Z`, cutoff `05:30Z`, hard flat `05:58Z`, and end `06:00Z` on Sep 23 where applicable. Script parameters and the supported graceful-stop path match repository interfaces. |
| Venue calendar and trading-session availability at execution time | **LIVE PREFLIGHT PENDING** | The runbook correctly requires an immediate official-calendar recheck. CME lists normal Micro E-mini trading hours but warns holiday schedules can change; a cached or earlier check is insufficient. |

## Runbook boundary review

`docs/operations/beelzebub-asia-qualification-next-pass-20260922.md` is an adequate fail-closed **preparation artifact**, not authorization to execute while the HOLD above is active.

The proposed local/ET/UTC conversions agree with the repository Asia profile (`18:00` start, `18:05` observation, `01:30` cutoff, `01:58` hard flat, `02:00` end). The official CME Micro E-mini FAQ describes ordinary Sunday–Friday `18:00–17:00 ET` hours with the ordinary daily pause; CME separately warns that holiday schedules are subject to change. Therefore the runbook's same-day official-calendar recheck and abort-on-mismatch rule are necessary and correct:

- <https://www.cmegroup.com/articles/faqs/micro-e-mini-equity-index-futures-frequently-asked-questions.html>
- <https://www.cmegroup.com/trading-hours.html>

The runbook also correctly:

- freezes the repair commit/tree and required identities;
- begins with current full verification, then a read-only native observation sandwich;
- allows at most one conditional `R2` attempt and refuses a second ambiguous attempt;
- requires distinct-producer and exact repaired-fingerprint evidence;
- places supported capacity-latch recovery only after authenticated flat/reconciled proof and surrounds it with full verification;
- makes the ratio window fixed and diagnostic while policy remains undefined;
- uses the supported graceful BeeZConsole stop and preserves evidence hashes; and
- grants no Gates B–D, paper, arming, or order authority.

## Final administrative disposition

The integrated changes are meaningful and their offline evidence should be retained. The source-repair pass is nevertheless incomplete because the primary service/freshness defect remains reproducible. The next integration gate is the HOLD release condition above, not a looser threshold, a rerun of the same natural window, or consumption of the sole reload opportunity.

The sealed artifact identities used in this determination are recorded in `FINAL_EVIDENCE_REGISTER.md` in this directory.
