# Initial blocker and acceptance draft

Pass opened: 2026-09-21 23:56:37Z

Administrator: Caelum (`beelzebub_admin`), concurrent specialist. Parent Sol is
the sole integrator and runtime/ledger operator. Caelum writes only this pass
directory. No benchmark, runtime, ledger, account, installation or reload
action was performed.

## Repository and integration state

- Intake revision: `04a1f22013a00cf595f41424f77f93e65cf62918`.
- Current reviewed HEAD: `046b609a9adfc92eb352429d24dea81dbcf22734`,
  branch `codex/lane-iii-perpetual-corrective-20260906`.
- Receiver owner commit `a6ce300843c86babb4e337236f2c3ff158f41f71`
  and integrated commit `046b609...` have the same tree
  `b516827a367ce45d98d03cec14f3a237dda3715e`; `a6ce300...` is not an
  ancestor, so the accurate description is source-equivalent integration, not
  fast-forward ancestry.
- The receiver instrumentation changes the collector, comparison tool and its
  focused test. It is integrated but not yet independently accepted by Caelum.
- Periodic/bridge and native lifecycle repairs are in progress and are not part
  of this initial acceptance.
- Pre-existing unrelated modified/untracked audit and operations evidence was
  observed and preserved.

## Classification matrix

| Item | Initial class | Acceptance or release evidence |
| --- | --- | --- |
| Retired 2,111/s and 5.15x executable authority | **FIXED/VERIFIED** | Prior commits `ada2420...` and `28fa752...`; current handoff confirms retirement |
| Ratio operands, window and separate protection semantics | **FIXED/VERIFIED** | Decision attachments and scheduler/source trace recorded in `REQUIREMENTS_PROVENANCE.md` |
| Numeric ratio threshold/tolerance/direction | **POLICY DECISION REQUIRED** | No accepted numeric value found; decide the explicitly unapproved V1 proposal before qualification outcomes |
| Executable ordinary-full counter and aligned-window receipt | **UNRESOLVED REPAIR** | Must add authoritative DEPTH boundary, ordinary completed-full terminal counter, aligned deltas, identity and undefined reasons |
| Healthy periodic bridge fixture | **UNRESOLVED REPAIR** | Exercise supported isolated bridge/readiness, ordinary completions and bridge-loss protection/recovery without predicate bypass |
| Prior protected comparison interpretation | **FIXED/VERIFIED** | `RUNTIME_BRIDGE_UNAVAILABLE` is valid protected behavior for the deliberately bridgeless comparison harness, not healthy proof |
| Receiver/collector residual service attribution | **UNRESOLVED REPAIR** | Integrated instrumentation awaits focused/integration verification and one frozen matched healthy/protected comparison |
| Receiver freshness/equilibrium | **UNRESOLVED REPAIR** | Prior 25.229554 s lag and 10,938 ms collector age fail; require <=1,000 ms contracts and open-ingress equilibrium |
| Native outbound teardown/generation isolation | **UNRESOLVED REPAIR** | Shutdown must release blocked I/O, join boundedly, report timeout/failure and prevent old-generation resurrection |
| Capacity lockout sequence 134169 | **LIVE PREFLIGHT PENDING** | Use only supported recovery after fresh authenticated Sim101 reconciliation and full verified-tip proof; preserve N: ledger identity/history |
| Current account, position, orders, reconciliation and arming | **LIVE PREFLIGHT PENDING** | Fresh supported read-only observation immediately before maintenance/R2; historical flat state is insufficient |
| R2 and natural Asia qualification | **LIVE PREFLIGHT PENDING** | Explicitly reserved for later pass; 0 used / 1 remains |
| Final integrated candidate acceptance | **UNRESOLVED REPAIR** | Later independent Caelum review of exact integrated commits, identities, tests and final receipts |

## Workstream acceptance criteria

### Receiver and collector

Require aligned monotonic stage attribution for socket write/backpressure,
framing/decode/authentication, dispatch callbacks, paper path, collector enqueue,
actual writer/fsync work, host service and scheduler. Preserve every required
record, order, signature and durable acknowledgement. A matched frozen
before/after comparison must reconcile input identity and ordered durable output,
show zero overflow/transport/reject/duplicate deltas, bounded queue/source age
within the unchanged 1,000 ms limits, and queues repeatedly returning toward
baseline while ingress remains open. Final drain alone is insufficient.

### Periodic and bridge

Require an isolated healthy-flat end-to-end path through supported bridge and
readiness facilities, plus separate bridge-loss/unavailable/stale protected
scenarios and supported recovery. Report interval/identity, `D`, `E`, `O`, all
scheduler terminal classes, start lateness, freshness, queue and durability
facts. Ordinary completed-full counts must exclude protection, no-work,
unavailable and incomplete outcomes. No test may force healthy status by
patching the production readiness predicate.

### Native lifecycle

Supported stop must own and close the active socket, release a blocked write,
join the exact sender thread within a declared bound, return an explicit timeout
failure, and distinguish discarded/unsent frames from delivered frames. A new
producer/generation cannot accept work until prior ownership termination is
proved. Cover blocked write, receiver disconnect, queued/in-flight work,
concurrent stop/reconnect, repeated stop and generation replacement. Recompute
source/build identities and run the applicable compiler/source-contract checks.

### Capacity latch

Use `recover_healthy_ledger_capacity` or its loopback endpoint only. Before the
transition require the original lockout, a later safe signed reconciliation,
current exact Sim101/LOCAL_SIMULATION/MNQ DEC26 identity, flat/zero orders/no
unresolved activity, authenticated provenance-valid reconciled transport,
healthy capacity, no newer fault, and Full verification exactly through the arm
snapshot tip. After success require a durable clearance row, unchanged ledger
identity/history and a new Full verification covering the recovery row. Direct
database edits, migration, manual clear and floor weakening are prohibited.

### Next-pass packet

The later packet must freeze exact integrated commit/tree and artifact hashes;
test commands/results; synthetic/replay provenance; remaining live-only checks;
attempt accounting; and supported start/abort/stop commands. It must name the
actual Asia calendar date and UTC/America-Denver window at execution time,
profile/account/instrument selectors, receipt paths and schemas. Opening steps
must reacquire fresh process/listener/account/order/position/reconciliation/
arming evidence, complete any supported latch recovery, prove one producer and
loaded identity, then spend R2 only if all release conditions pass. The ratio
window is predeclared after clean R2 and is distinct from full qualification.

## Current HOLD

Affected transition: final repair-readiness label, installation/reload, R2 and
qualification.

Evidence: ratio policy is unapproved; ordinary-full telemetry, healthy bridge
exercise and native teardown remain under repair; residual freshness has not
passed; current safety state and supported latch recovery are pending.

Consequence: `READY_FOR_ASIA_QUALIFICATION_PREFLIGHT` is not yet available.

Release: integrate and verify the remaining repairs, decide the ratio policy,
complete the later independent Caelum review, and leave only explicitly named
fresh live preflight checks. Independent implementation and offline verification
continue under the parent.
