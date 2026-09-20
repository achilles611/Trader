# Caelum rehearsal review — DEC26 PONG timing and retained-hour paths

Initial review: 2026-09-20 17:09 MDT (`2026-09-20T23:09:49Z`)

## Outcome

**PASS for a bounded, disarmed rehearsal. HOLD on calling the proposed
18:00–19:00 MDT interval production-qualified under current source.**

The current collector, started at 17:05 MDT, correctly targets 18:00–19:00 MDT
and can independently retain a collector-eligible interval even if the running
host later latches `PONG_WARM_START_MISSING`. It can also retain the two causal
live hourly bars needed by the offline retained-hour design: 17:00–18:00 and
18:00–19:00 MDT.

However, current HEAD cannot convert that DEC26 retained interval into the
production global certificate. `tools/certify_pong_retained_hour.py` is still
hard-coded to SEP26 for hourly bars, classified trades, completed-hour objects,
and bias construction. A genuine DEC26 bar is rejected with
`PONG_RETAINED_HOURLY_PROVENANCE_INVALID`; a partial workaround would still
produce a SEP26 bias that the DEC26 host cannot hash-validate. This is a
concrete source HOLD, not missing market evidence.

Accordingly:

- earliest possible **collector-eligible** window: 18:00–19:00 MDT, if the
  current collector remains continuously healthy and lossless through it;
- earliest production certificate with the **current unmodified online host
  path**: 19:00–20:00 MDT after a fresh post-18:00 warm start, with the
  certificate no earlier than shortly after 20:00 MDT (20:05 maximum hourly-bar
  publication allowance);
- retained 18:00–19:00 evidence should be preserved if collected, because a
  separately authorized and verified topology-aware certifier repair could
  potentially certify it later. No such repair is authorized or performed in
  this review.

## Scope, ownership and baseline

- Caelum is a concurrent source-and-receipt-only reviewer. Sol remains the sole
  integrator and runtime/process/account operator.
- Caelum's only writable path is this report. No application, test,
  configuration, process, runtime, ledger, collector or benchmark action was
  performed.
- Repository: `C:\Users\atlas\Documents\Trader`; branch
  `codex/lane-iii-perpetual-corrective-20260906`; reviewed HEAD
  `e6ad37c918b3c0f40dbdff57a01b8f4270b28dc6`; tree
  `9302f7a76cfa9fc453a9f4b365822476a32d233f`.
- HEAD records the prior safe pause and the earlier Caelum review. At review
  start the only unrelated worktree content was the preserved untracked
  `docs/administration/caelum/audits/20260920-104755MDT/` bundle.

## Pre-rehearsal timing requirements

### 1. Exact warmup bars

Warmup accepts exactly two immediately preceding, contiguous, completed UTC
hours for the selected topology. For a request at time `R`, the code floors
`R` to the current UTC hour `B`, then requires `[B-2h,B-1h)` and
`[B-1h,B)` (`BeelzebubReadOnlyAddOn.cs:972-1012`;
`pong_inputs.py:181-269`). The payload must be signed and complete, identify
`MNQ DEC26`, use the DEC26 warm-start policy hash, provider source, UTC, two
bars, no gaps, and exact start/end alignment.

The native observer requests six 60-minute provider bars, with provider lookup
and `DoNotMerge`, only to locate that exact pair
(`BeelzebubReadOnlyAddOn.cs:938-970`). Six bars are requested; only the exact
two-bar pair is admitted.

Timing consequences for 2026-09-20:

| Startup/request time | Exact required pair | Result for Sunday reopen |
| --- | --- | --- |
| 17:10–17:30 MDT | 15:00–16:00 and 16:00–17:00 MDT | The first hour is the closed maintenance hour; Friday or another prior-session hour cannot substitute because timestamps must be exact. |
| Just after 18:00 MDT | 16:00–17:00 and 17:00–18:00 MDT | First complete in-session pair, subject to provider availability. |

The preserved pause receipt independently records the earlier incomplete
warm-start and states this first valid Sunday pair
(`docs/operations/evidence/beelzebub-preflight-pause-20260920.md:96-120`).

### 2. Prior-session and subsequently retrieved bars

The warm-start validator has no same-session or same-UTC-date rule. Provider
bars retrieved after they completed are eligible if they are the exact two
immediately preceding UTC hours and satisfy all signed provenance, topology,
alignment and no-gap checks (`pong_inputs.py:188-269`). The `day_anchor` is the
date of the exact earliest required hour; crossing midnight is supported.

This does **not** allow arbitrary prior-session substitution. For a 17:xx
Sunday request, Friday's last two bars have the wrong timestamps. It also does
not turn historical OHLCV into qualification streams: historical warmup has no
authenticated depth, quote, trade, sequence or durability coverage. The pause
receipt records the same boundary (`beelzebub-preflight-pause-20260920.md:
110-120`).

### 3. May qualification overlap warmup?

The collector and host warmup gates are separate, so collection can overlap
warmup in principle. The collector selects and records its interval without
consulting host warmup (`pong_collection.py:158-225`).

Certificate creation cannot bypass warmup on the online path:

- an accepted `HISTORICAL_WARM_START` creates the input adapter;
- a `HOURLY_BAR` arriving before that adapter raises
  `PONG_WARM_START_MISSING`;
- the resulting `_input_error` is latched and `_maybe_certify_hour` refuses
  whenever `_input_error` is non-null or the current two-hour pair is absent
  (`pong_production.py:681-723,1366-1389`).

Therefore overlap is allowed only when a valid warmup arrives before the first
required live hourly-bar handoff. For this Sunday boundary, a pre-18:00 startup
cannot get the exact complete in-session pair, so overlap cannot make the
online host certify 18:00–19:00.

### 4. Collector support for 18:00–19:00 MDT

**Collector support: YES. Production certificate under current HEAD: NO.**

The collector always targets the next full UTC hour, including when constructed
exactly on a boundary, so that transport and producer identity can be witnessed
before the interval (`pong_collection.py:61-66,185-225`). A 17:05 start therefore
targets 18:00–19:00 MDT. At the target boundary it requires healthy transport
and an active producer identity; interval eligibility then requires a complete
hour, all three streams, coherent depth, at least eight BUY/SELL-classified
trades, no accumulated interval reasons, and its causal live hourly bar
(`pong_collection.py:693-773`).

The collector retains `HOURLY_BAR` independently of host admission
(`pong_collection.py:42-43,540-611`). Its journal can therefore contain the
17:00–18:00 and 18:00–19:00 causal live bars even if the host rejects the first
one for missing warmup.

The offline retained-hour design deliberately uses this separation. It accepts
only a stopped collector with an `eligible=true` interval, verifies the full
journal/HMAC/tip and exact stream counts, and requires the causal live bars for
the hour immediately before the target and the target itself
(`tools/certify_pong_retained_hour.py:51-62,118-170,172-250`;
`docs/operations/unattended-ninjatrader-observer.md:87-103`).

But its current implementation is not DEC26-capable:

1. `completed_hour()` requires `contract_id == "MNQ SEP26"` and otherwise
   raises `PONG_RETAINED_HOURLY_PROVENANCE_INVALID`
   (`certify_pong_retained_hour.py:65-82`).
2. It constructs `CompletedPongHour` without a topology instrument, so the
   dataclass default is SEP26 (`certify_pong_retained_hour.py:89-100`;
   `pong_experiment.py:64-91`).
3. It hard-codes classified trades to `instrument="MNQ SEP26"`
   (`certify_pong_retained_hour.py:225-241`).
4. It calls `_bias_from_inputs` without the DEC26 instrument argument, invoking
   the function's SEP26 default (`certify_pong_retained_hour.py:247-256`;
   `pong_production.py:1308-1333`). A DEC26 host reconstructs a loaded
   certificate with `self.topology.native_instrument` and rejects a mismatched
   result hash (`pong_production.py:1335-1364`).

Thus 18:00–19:00 can be valid retained collection evidence, but it cannot be
called a production DEC26 certificate under current source.

### 5. Effect of a 17:10–17:30 startup/stop rehearsal

A controlled stop in that window does not alter provider history and does not
prevent the 16:00–17:00 and 17:00–18:00 bars from being retrieved after 18:00.
It also does not clear or rewrite the preserved earlier fault or ledger.

It does end that collector before its 18:00 target begins, so its interval is
only `PENDING`, cannot be qualification evidence, and cannot be spliced into a
later collector or producer session. A later fresh start after 18:00 selects
19:00–20:00 as its target. This matches the current pause receipt's explicit
boundary (`beelzebub-preflight-pause-20260920.md:121-141`).

If the 17:05 collector instead remains running through 19:00, it may preserve
an eligible 18:00–19:00 retained interval. That is no longer merely a short
startup/stop rehearsal; it is a full-hour measurement and still remains
disarmed and certificate-blocked as described above.

### 6. Earliest valid windows without weakening gates

- **Collector eligibility:** 18:00–19:00 MDT, using the already-running 17:05
  collector, only if every target-hour gate passes and both causal live hourly
  bars are durably retained.
- **Production DEC26 certificate under current unmodified source:**
  19:00–20:00 MDT. Start a fresh disarmed host/collector after 18:00, only after
  the provider can return 16:00–17:00 and 17:00–18:00, and before 19:00 so the
  producer/transport pre-boundary witness exists. The 18:00–19:00 live bar then
  advances the warm pair at 19:00; the 19:00–20:00 target bar can complete the
  candidate shortly after 20:00 and no later than 20:05.
- **Potential retained certificate after a separately authorized certifier
  repair:** 18:00–19:00 evidence could be certified after 19:00, but only after
  a topology-aware implementation and tests are accepted. That is not current
  capability and is not permission to patch during this pass.

## Inherited producer transport-failure counter

The observed absolute `observer_transport_failed_frames=216217` does not by
itself block target-hour eligibility or the offline certifier.

The collector treats producer failure and overflow fields as cumulative totals.
It records a failure reason only when a newly observed total exceeds its prior
total, then updates the stored baseline (`pong_collection.py:540-560`). Because
the current collector began before the 18:00 target, the first pre-target record
established the 216217 baseline outside the target interval. A zero delta
during 18:00–19:00 adds no target reason.

The retained certifier checks interval `continuity_valid`, empty interval
blockers, collector queue overflow, rejection and duplicate counters, plus each
stream's gap/inversion counts. It does not reject a nonzero absolute
`producer_transport_failed_records` field (`certify_pong_retained_hour.py:
125-151`). This is consistent with delta semantics, not permission to ignore a
new failure.

**Read-only transient receipt at 2026-09-20T23:08:40.1250947Z:** collector
`pong-collector-20260920T230538341871Z-5b44474a5b06`, started 17:05:38 MDT,
transport `HEALTHY`, target 18:00–19:00 MDT, `continuity_valid=true`, 2,974
enqueued/written/durable, zero collector overflow/rejection/duplicate, producer
overflow 0, and the absolute producer transport-failure watermark 216217. The
target was still `PENDING`; this snapshot is not an interval pass.

Any increase above 216217 during the target is a HOLD: the collector will add
`PRODUCER_TRANSPORT_FAILED` to the interval and it cannot become eligible.

## Administrative HOLD ledger

| Affected action | Status | Evidence / consequence | Release condition | Owner |
| --- | --- | --- | --- | --- |
| Short 17:10–17:30 disarmed rehearsal | **PASS** | Does not modify provider history; produces no qualification | Controlled safe stop and preserved receipt; no qualification claim | Sol |
| Collect 18:00–19:00 with current collector | **PASS, measurement only** | Correct next-hour target and pre-boundary witness; host warmup failure does not stop collector retention | Continuous healthy producer/transport, zero target-hour loss deltas, all interval gates, both live bars, clean durable stop | Sol |
| Online host certificate from 18:00–19:00 | **HOLD** | Pre-18 warmup pair incomplete; first live bar without adapter latches `PONG_WARM_START_MISSING` | Fresh post-18 startup and move target to 19:00–20:00 | Sol |
| Offline/global DEC26 certificate from retained 18:00–19:00 | **HOLD** | Certifier is SEP26-hard-coded in four topology-bearing paths | Separately authorized topology-aware repair; tests for DEC26 acceptance, SEP26 compatibility and mixed-topology refusal; then certify only a stopped eligible collector | Integrator |
| Treat absolute producer failures 216217 as automatic failure | **NO HOLD** | It is a pre-target cumulative baseline and the certifier uses interval/delta evidence | HOLD only on a target-hour increment or resulting interval blocker | Sol |
| Paper activation/order authority | **HOLD** | This pass is disarmed review/measurement; no accepted certificate or separate operational-start authority | Accepted fresh DEC26 certificate plus all runtime/account/risk gates and separate authorization | Sol / Joseph |

## Verification and evidence boundary

- Read the project charter/concurrency protocol, prior Caelum review, safe-pause
  receipt, current source paths cited above, the retained-certifier runbook, and
  current read-only collector status.
- Ran no tests, replay, benchmark, process action, runtime API, ledger write or
  network operation.
- The last immutable account/position/order observation remains the safe-pause
  receipt: `Sim101 / LOCAL_SIMULATION / MNQ DEC26`, `READY_DISARMED`, flat zero,
  zero owned orders, live capital denied, observed before the controlled stop
  (`beelzebub-preflight-pause-20260920.md:14-39`). Caelum did not independently
  query the resumed runtime or account.

## End-of-rehearsal evidence review

**COMPLETED by the final addendum below.** The initial record above preserves
the pre-rehearsal timing decision as it stood at 17:09 MDT. The addendum
distinguishes Sol-reported rehearsal measurements, Caelum-observed read-only
state, and the subsequently implemented but still uncommitted certifier diff.

## Next concrete action

For a short rehearsal, Sol may stop safely in the requested 17:10–17:30 window
and preserve the terminal status. For the earliest measurement, keep the
current collector continuously disarmed through 19:00 and preserve its stopped
eligible/ineligible receipt and both hourly bars. Do not call it DEC26-certified
unless the current topology defect in the offline certifier is separately
repaired and accepted. Without that repair, use the post-18 warmup and
19:00–20:00 online qualification path.

---

## Final addendum — 2026-09-20 17:38 MDT

### Final verdict

**PASS for bounded disarmed rehearsal safety and observed quote/trade
continuity. HOLD for qualification, certification, and activation.**

The rehearsal stayed `READY_DISARMED`, flat and reconciled with zero orders and
zero commands. The observed ingress/admission, quote/trade analysis, and
collector persistence paths showed no reported loss, inversion, queue overflow,
rejection or duplicate. A six-record durability lag at the end snapshot caught
up completely 15.931 seconds later with queue depth zero. This is a safe
rehearsal result, not a qualifying-hour result.

The measurement ended 28.896 seconds beyond the nominal 20-minute boundary.
That late close did not create an observed safety or durability failure, but it
is a duration-control defect: this pass may not be represented as an exact
20-minute rehearsal. The collector was intentionally left running for its
18:00–19:00 target, so there is no terminal collector, journal-tip or
post-shutdown ledger-verification receipt yet.

Qualification remains held because the rehearsal and the 17:36 read-only state
have zero `DEPTH`, `warm_start_complete=false`, no qualified-hour certificate,
and zero periodic-flat evaluations. The target interval had not started and was
still `PENDING`; no target stream, causal hourly bar or interval-eligibility
result existed. Nothing in this rehearsal proves the 20 ms periodic path or the
ordinary 2,111 records/s acceptance floor.

The current operational receipt and a 17:40 read-only ledger observation expose
an additional release HOLD: repeated `INCIDENT_OBSERVATION_SINK_FAILURE`
records, classified as an unknown unverified tail with online-append integrity
false. This does not rewrite or contaminate the separately signed collector
journal, but it prevents a paper-readiness claim until the sink defect is
diagnosed/repaired and the stopped ledger tail is verified.

### Exact rehearsal record supplied by Sol

The following values are **REPORTED** by Sol for this review. Caelum did not
re-run the rehearsal, control the runtime, or derive a substitute benchmark.

| Metric | Start — 17:10:45.173 MDT | Complete snapshot — 17:31:14.069 MDT | Observed change |
| --- | ---: | ---: | ---: |
| Source ingress / authoritative admission | 6,333 / 6,333 | 40,800 / 40,800 | +34,467 / +34,467 |
| `DEPTH` / `QUOTE` / `TRADE` | 0 / 2,363 / 2,692 | 0 / 14,745 / 19,651 | 0 / +12,382 / +16,959 |
| Event / periodic analytical evaluations | 5,055 / 0 | 34,396 / 0 | +29,341 / 0 |
| Host observation queue / overflow | 0 / 0 | 0 / 0 | unchanged |
| Collector enqueued / written / durable | 5,043 / 5,043 / 5,043 | 34,385 / 34,385 / 34,379 | +29,342 / +29,342 / +29,336 |
| Collector queue / overflow | 0 / 0 | 0 / 0 | unchanged |
| Producer overflow / failed-frame watermark | 0 / 216,217 | 0 / 216,217 | 0 / 0 delta |
| Collector rejected / duplicate | 0 / 0 | 0 / 0 | unchanged |
| Host CPU / working set / private | 67.4375 s / 176.1 MB / 163.6 MB | 454.859 s / 223.6 MB / 213.5 MB | +387.4215 s / +47.5 MB / +49.9 MB |
| NinjaTrader CPU / working set | 277.78125 s / 496.0 MB | 466.281 s / 479.5 MB | +188.49975 s / -16.5 MB |
| Available memory / C: free / N: free | 8,134 MB / 8.25 GB / 215.65 GB | 8,284 MB / 8.26 GB / 215.39 GB | +150 MB / +0.01 GB / -0.26 GB |

Elapsed wall time was 20 minutes 28.896 seconds. At the complete snapshot the
host queue/overflow, every reported missing/out-of-order count, compute pending,
collector queue/overflow, producer-overflow, rejection and duplicate counts were
zero; `input_error` was null. At 17:31:30 MDT, collector durable records had
caught up from 34,379 to 34,385 with queue depth zero. State remained
`READY_DISARMED`; account truth remained `Sim101 / LOCAL_SIMULATION / MNQ DEC26`,
flat quantity zero, zero orders, reconciled and authenticated, commands sent
zero, and live capital denied. Exact scheduler configuration was retained, but
the active path remained `EVENT_DRIVEN_PROTECTION` because
`RUNTIME_BRIDGE_UNAVAILABLE` and periodic evaluations remained zero.

The equality of the 29,341 quote-plus-trade increment and the 29,341 event
evaluation increment is consistent for the observed no-depth event-driven path.
It is not evidence for depth handling, periodic scheduling, or whole-system
capacity.

### Read-only state observed after the rehearsal

At `2026-09-20T23:36:53.8006128Z`, the read-only
`GET /api/lane-iii/paper` result was **OBSERVED** as:

- `READY_DISARMED / DISARMED`, `Sim101 / LOCAL_SIMULATION / MNQ DEC26`,
  live capital denied;
- flat quantity zero, zero working owned and entry orders, current
  reconciliation true, authenticated client true and commands sent zero;
- `warm_start_complete=false`, `input_error=null`;
- collector `COLLECTING`, observe-only/disarmed, thread alive, transport
  `HEALTHY`, signed DEC26 topology, and the same producer session/build/source;
- 38,632 enqueued/written/durable, queue depth zero, collector overflow,
  producer overflow, rejection and duplicate all zero; inherited producer
  failed-frame watermark still 216,217;
- required interval 18:00–19:00 MDT still `PENDING`, `continuity_valid=true`,
  with zero target records because its start boundary had not arrived.

The parallel read-only `GET /api/lane-iii/pong` observation at
`2026-09-20T23:36:33.2971078Z` showed `state=RUNNING` but all authority still
blocked: activation candidate false, operational-start ready false,
entry-authority ready false, activation unauthorized, controller `PREPARED`,
and `live_authority=DISARMED`. It also showed `DEPTH=0`,
`warm_start_complete=false`, no qualified certificate, 38,435 event-driven and
zero periodic-flat evaluations, and the active protection reason
`RUNTIME_BRIDGE_UNAVAILABLE`.

These are transient read-only observations, not immutable completion receipts.
The collector is still running and its 18:00–19:00 interval has not been
measured.

### Review of the uncommitted retained-certifier repair

The current working copy contains only two Sol-owned tracked modifications,
`tools/certify_pong_retained_hour.py` and
`tests/test_l3g_pong_retained_certification.py` in addition to preserved
untracked audit/evidence paths. `git diff --check` reported no whitespace
errors; the final diff is 118 insertions and 12 deletions. Snapshot hashes
reviewed by Caelum:

- certifier SHA-256
  `72f0dd4a025840756efbd7ac04c9e2a61dd0d3d02b934160a03f656d0bf832e8`;
- focused-test SHA-256
  `5e5283eb4a6d43e55b1c4b4fd2e6605aceb96d74e639fe852c02a372a2cba70b`.

The diff is **implemented in the working copy, not committed or accepted**, and
the offline certifier is not a component loaded into the running host. It
correctly resolves the retained DEC26 topology from status, rejects malformed
status topology, binds hourly bars, trades, bias and the output certificate to
that topology, and rejects a market record whose `contract_id` is mixed. Those
parts are fail-closed (`certify_pong_retained_hour.py:73-127,152-179,229-268`).

Caelum's first diff review found that `completed_hour()` did not check
`policy_version` or the topology-specific hourly warm-start `policy_hash`. Sol
then corrected that HOLD. The final function requires
`PONG_HOURLY_WARM_START_POLICY_VERSION` and
`hourly_warm_start_policy_hash(topology)` before constructing an hour
(`certify_pong_retained_hour.py:73-94`), matching canonical online admission
(`pong_inputs.py:335-348`). The DEC26 fixture now uses
`hourly_warm_start_policy_hash(DEC26_TOPOLOGY)` and a separate negative test
supplies both the SEP26/default hash and a wrong policy version, requiring
`PONG_RETAINED_HOURLY_PROVENANCE_INVALID`
(`test_l3g_pong_retained_certification.py:45-57,181-209`).

Final source verdict: **PASS — the identified topology/provenance repair is
complete and fail-closed in the reviewed working copy.** Sol reports the
focused three-module suite ran ten tests with `PASS`; Caelum did not
independently rerun it. The current operational receipt records the command
`python -m unittest tests.test_l3g_pong_inputs tests.test_l3g_pong_collection
tests.test_l3g_pong_retained_certification -v` with ten tests passing. The
receipt was reviewed at SHA-256
`5e3094c32c5c9f7c3bc50bb360c94b41e5c91d42098e152f30f2e7ffb686e13f`.
These accelerated tests validate source behavior only; they do not qualify
authentic history.

Integration release condition is now limited to Sol accepting/committing or
otherwise exactly identifying these working-copy hashes before the offline
tool is used, plus preservation of the raw command/output if required by the
release record. No further certifier source correction is requested by Caelum.

### Qualification timing after final review

The earliest possible retained evidence window remains **18:00–19:00 MDT**.
The current 17:05 collector already has the required pre-boundary transport and
producer witness, and host warm-start state does not govern collector
retention. It may become collector-eligible only if all target-hour gates pass:
nonzero authenticated `DEPTH`, `QUOTE`, and `TRADE`; coherent depth; at least
eight BUY/SELL classified trades; zero interval loss/inversion/identity/
transport reasons; stable zero queue/rejection/duplicate/overflow counters; and
the causal 18:00–19:00 live bar. It must also durably retain the prior
17:00–18:00 causal live bar for offline bias construction.

The inherited absolute failed-frame watermark 216,217 remains **NO HOLD** if
its delta is zero throughout the target. The source records
`PRODUCER_TRANSPORT_FAILED` only when the cumulative total increases
(`pong_collection.py:552-560`); the retained certifier then requires empty
interval blockers and valid continuity (`certify_pong_retained_hour.py:146-164`).
Any increment during 18:00–19:00 is a qualification HOLD.

If the interval is eligible, the earliest global retained certificate is after
the 19:00 boundary, after its causal hourly bar is received (allowed through
19:05), after a clean collector stop/full drain, and only with the reviewed
certifier repair exactly identified and accepted. Because the journal evidence
is retained, applying the offline conversion after the interval does not
retroactively weaken or alter the evidence.

Without that accepted repair, the earliest supported online-host path remains
**19:00–20:00 MDT** after a fresh post-18:00 warm start retrieves the exact
16:00–17:00 and 17:00–18:00 pair and the new host establishes its pre-19:00
witness. The currently running host is still warm-false, so its existence does
not establish that online path.

### Final HOLD and release ledger

| Affected claim/action | Status | Evidence and consequence | Release condition |
| --- | --- | --- | --- |
| Bounded disarmed rehearsal safety | **PASS** | Flat, zero orders/commands, disarmed, authenticated/reconciled; no reported quote/trade loss or collector overflow; durability caught up | Preserve Sol's immutable rehearsal receipt; describe the 28.896 s late close |
| Exact 20-minute duration | **HOLD** | Measurement closed 28.896 s late | Future bounded run closes at the declared boundary or explicitly authorizes and records an extension |
| Whole-input qualification | **HOLD** | `DEPTH=0`; no depth-path observation | Nonzero valid/coherent depth across the target with zero continuity defects |
| 20 ms / 2,111 records/s acceptance | **HOLD** | Active path was protected event-driven; periodic count zero; rehearsal was not a capacity benchmark | Separate authorized acceptance evidence for the exact active path and floor |
| Retained-certifier topology repair | **PASS, source/test** | Final diff validates status, market contracts, hourly policy version and topology hash; DEC26 positive plus explicit SEP26-hash and wrong-version refusals; Sol/current receipt report 10 related tests PASS | Sol integrates with exact reviewed hashes; authentic non-test execution remains separate |
| 18:00–19:00 collector interval | **PENDING** | At 17:36 it had not started; no target record, depth, bar or terminal result existed | All target gates, both causal bars, durable drain, stopped eligible status and immutable hashes |
| Absolute failed-frame watermark 216,217 | **NO HOLD by itself** | It is unchanged and source uses interval delta semantics | HOLD on any target-hour increment or resulting blocker |
| Offline DEC26 certificate | **HOLD** | Source repair now passes review, but no stopped eligible collector or authentic certification receipt exists | Exact reviewed tool plus stopped, fully durable eligible interval and successful non-test certification |
| Online certificate | **HOLD** | Current host `warm_start_complete=false`; no certificate | Fresh valid post-18 warmup and a complete qualifying 19:00–20:00 interval, unless the earlier retained path is validly certified |
| Authority-ledger readiness | **HOLD** | At 17:40, tail state `ONLINE_APPEND_INTEGRITY_FAILED`, 63,836 rows beyond verified sequence 8,193, last blocker `INCIDENT_OBSERVATION_SINK_FAILURE` | Bounded source diagnosis/repair, safe stop/drain, full current-tail verification, then rerun affected readiness evidence |
| Activation/order authority | **HOLD** | Review/runtime remain disarmed; no certificate; observation-sink and ledger-tail fault; no separate launch authority | Accepted fresh DEC26 certificate, repaired sink, verified current ledger/risk continuity, all runtime/account gates, and Joseph's separate authorization |

### Evidence gaps at close

1. The current operational rehearsal receipt was reviewed at SHA-256
   `5e3094c32c5c9f7c3bc50bb360c94b41e5c91d42098e152f30f2e7ffb686e13f`.
   Sol's raw snapshot bundle and raw focused-test transcript/hash were not
   supplied to Caelum; the exact measurements and 10-test output remain
   reported, while the receipt records the exact focused command/result.
2. The collector remains active. There is no terminal `STOPPED` status, final
   journal SHA/tip, target interval verdict, final drain receipt or independent
   post-stop ledger verification.
3. The two required causal live hourly bars and every 18:00–19:00 target record
   remain prospective.
4. No authentic non-test retained certification was run, and none is permitted
   before the collector stops and the reviewed tool identity is accepted.
5. No depth, periodic-flat scheduler, 20 ms SLA or 2,111 records/s evidence was
   obtained in this rehearsal.
6. At `2026-09-20T23:40:13.3130493Z`, read-only ledger state showed quick-check
   pass and a healthy writer but only sequence 8,193 verified, current tip
   72,029, 63,836 unverified-tail rows, `ONLINE_APPEND_INTEGRITY_FAILED`, and
   last unknown/blocking kind `INCIDENT_OBSERVATION_SINK_FAILURE`. A safe
   stopped-tail verification and bounded sink diagnosis remain outstanding.

Caelum changed no application, test, configuration, runtime, process, ledger or
collector state. This addendum is the only Caelum-owned write.
