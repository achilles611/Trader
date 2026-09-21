# Receiver periodic corrective acceptance

Decision: **ACCEPTED_SOURCE_OFFLINE_CORRECTIVE** for source commit
`448081aa055279c0ce516b5fb948c4b2530c2de7`.

Final evidence HEAD, including tool-only acceptance reconciliation:
`28fa75290151c4716f9c40bfef91eac647bfbdf1`.

Operational decision: **GATE_A_DELTA = HOLD_PERIODIC_RATIO_FRESHNESS_AND_INITIALIZATION_UNPROVEN**.
R2 and Gates B-D were not run. The one conditional R2 retry remains unused.

## Accepted result

The exact disarmed qualifying Pong route now admits authenticated market records
through a bounded state-only paper path selected only by the full qualifying
identity. It preserves full fallback for control/non-market records, state
drift, backward time and persistence policies; retains policy continuity,
runtime snapshot/warmup/authority state and observable counters; and produces
no second Pong decision. Collector raw durability and required sink failure
propagation remain separate obligations.

Focused and combined verification found no new source regression. The initial
selected-suite failure reproduced at the pre-change root and was the fail-closed
disk warning caused by a C:-backed temporary test ledger. Re-running the full
selected suite with all temporary-directory variables on N: passed 49 tests and
20 subtests in 5.33 seconds. The guard remained unchanged. The source is
integrated at the final evidence HEAD.

The one aligned pair proves a narrow causal improvement on identical 10,000
signed inputs:

- completion 241.513589/s -> 283.860640/s, +17.534% diagnostic improvement;
- receiver lag 31.406538 s -> 25.229554 s;
- paper fanout 12,615.942 ms -> 6,631.989 ms;
- 10,000 ordered/durable completions in both cases;
- zero new producer overflow or transport failures;
- 10,000 fast-path records and zero fallbacks in the selected fast run.

This accepts the revision's source/offline causal benefit. It does not accept
the diagnostic records/s values as gates.

## Requirement supersession and ratio result

The fixed 2,111/s requirement and derived 5.15x target are retired. The pair's
explicit 1,000/s offer is not a replacement threshold. The final three
comparison/periodic tools no longer contain or emit the retired fixed target;
burst rate is explicit diagnostic input and natural mode remains rate-free.

The accepted ratio remains
`DEPTH events : ordinary full analytical evaluations`. Both runs reported zero
ordinary periodic evaluations and 10,000 event-driven protection evaluations
under `RUNTIME_BRIDGE_UNAVAILABLE`. Therefore the ratio is **NOT MEASURED / NOT
APPLICABLE TO THESE PROTECTED RUNS**, not a zero or passing ratio. Its numeric
threshold/tolerance remains `UNKNOWN`.

## Concrete HOLD

Affected action: Gate A delta PASS, operational install/reload, R2, Gates B-D
and readiness classification.

Evidence: despite improvement, fast-path receiver lag is 25.229554 seconds and
collector maximum queue age is 10,938 ms against the unchanged 1,000 ms
freshness contract. Healthy-flat ordinary periodic operation was not exercised;
the numeric ratio contract is incomplete; old-producer teardown/isolation is
unproved; capacity lockout sequence 134169 remains uncleared; and current
account/positions/orders/arming are unknown. The actual runtime ledger already
resides on N: with ample observed free space, so no migration is needed. That
does not satisfy supported latch recovery: the original `DEGRADED` subcause was
not persisted; historical healthy-capacity and flat-reconciliation receipts are
not current; ports 8090, 48135 and 48136 have no listeners; and there is no
current authenticated reconciled runtime bridge. The identity-bound N: ledger
must not be copied or moved.

Consequence: a runtime retry would lack prerequisites and could not produce a
reviewable ratio result. Final drain cannot cure the preceding freshness and
service deficit.

Release: complete the versioned ratio acceptance definition and executable
ordinary-full counter semantics; prove a legitimate healthy-flat path plus the
separate protected path with bounded freshness/equilibrium; establish supported
old-producer isolation; recover lockout sequence 134169 through the supported
current authenticated reconciliation path without migrating the N: ledger or
manually clearing state; reverify current Sim101
LOCAL_SIMULATION, flat, disarmed and zero orders; then obtain a new independent
Gate A delta PASS before spending R2. Any additional behavioral source repair
requires new explicit revision authority because this pass used its one
corrective revision.

Owner: parent integrator/runtime operator. Independent safe read-only work may
continue.

## Deliberate distinctions

- Implemented/integrated/source-tested: **accepted**.
- Offline exercised on aligned diagnostic pair: **accepted narrowly**.
- Acceptance tools reconciled to retired fixed target: **accepted**.
- Installed/loaded/native exercised: **not performed**.
- Healthy ordinary periodic ratio: **not measured**.
- Current account/position/order/arming: **unknown**.
- R2 budget: **0 used / 1 remaining**.
- Order or live-capital authority: **none**.
