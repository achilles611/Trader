# Asia repair-readiness requirements provenance

Status: initial Caelum trace, 2026-09-21. This record is an administrative
requirements review, not runtime evidence or approval of a new policy.

## Controlling decision

Joseph's current handoff authorizes repair and offline verification only. It
explicitly reserves R2, natural-feed qualification, Gates B-D, arming and
orders for a later pass. The fixed 2,111 records/s gate is retired. Historical
2,111/s receipts remain evidence but have no current executable acceptance
authority.

The recovered reporting ratio is:

`R = aligned-window delta DEPTH / delta ordinary completed full analytical evaluations`

The decision requires a predeclared natural 3-5 minute healthy-periodic window
after clean R2, with the raw counts, interval endpoints, elapsed time and
source/build/session/generation identity from one epoch. Protection evaluations
are separate. A zero denominator produces null/undefined plus a reason, never
infinity or a fabricated pass.

Sources:

- current handoff, SHA-256
  `5bfa31ab7025f81d6a83d918b5b26d3a637fee2c296ea391077993432ac83a29`;
- earlier Gate B decision, SHA-256
  `a29e0df0014096326acc770fbb414f32094a7dc49dbb089c85be501c29c33c44`,
  lines 620-687;
- earlier ownership decision, SHA-256
  `52996a4496fa7aa2d9cd02df85370c6b392d7c6090a6f7e8fc129c69412356bb`,
  lines 277-284 and 317-345;
- prior Caelum trace at
  `docs/administration/caelum/passes/20260921-221624Z-receiver-periodic-corrective/REQUIREMENTS_PROVENANCE.md`.

## What is recovered

| Field | State | Evidence-backed contract |
| --- | --- | --- |
| Numerator | OBSERVED | Authenticated DEPTH events admitted by the authoritative Pong host within the aligned eligible window |
| Denominator | OBSERVED | Ordinary periodic full analytical evaluations completed within the same window |
| Interval | OBSERVED | Predeclared natural 3-5 minute Gate B window after clean R2 and clean initialization |
| Architecture | OBSERVED | `FULL_NATIVE_DEPTH_PERIODIC_20MS_NO_FHB_V1`, `PERIODIC_FLAT_PROTECTED`, 20 ms |
| Protection | OBSERVED | Immediate event-driven protection remains separate and cannot enter the ordinary denominator |
| Eligibility | OBSERVED | Same source/build/session/generation; healthy bridge; current reconciliation; flat/no orders; canonical exact 10x10 or explicit disqualification; fresh source/queues; durable progress |
| Zero denominator | OBSERVED | Undefined/null with explicit reason |
| Raw reporting | OBSERVED | Numerator, denominator, opportunities, dispatches, completions, late/obsolete/protection outcomes, interval bounds and identities |
| Numeric R threshold/direction | UNKNOWN | No accepted number or monotonic pass direction exists in the located decisions, source, tests or Git history |

## Git and executable trace

- `b48bc4f8692619ac75dff703c997628a6b659c23` introduced the absolute QPC
  deadline scheduler. Its reconciliation identity accounts scheduled outcomes;
  it does not define a ratio threshold.
- `efea61561e128bc44632def5d7c135e6ea7bacd9` introduced
  `depth_events_per_analytical_evaluation`. The field divides total DEPTH by
  `_microstructure_evaluations`, which can include event-driven protected work.
  It is not the accepted denominator.
- `periodic_flat_analytical_evaluations` increments before the analytical call
  and before canonical-depth early refusal, so it is an attempt count rather
  than a proved completed-full count.
- Scheduler `completed` includes intentional no-work and `applied=False`; it
  also is not the accepted denominator.
- Git literal history for the ratio name and operands resolves only to the
  prior administrative recovery record. No earlier committed numeric threshold
  was found. The retired 5.15x value was derived from the retired ingress floor,
  not from this periodic ratio.

## Bridge and native provenance findings

The prior comparison tool constructed and started a host but neither supplied
the production certificate/current authenticated runtime prerequisites nor
attached a test bridge. Its `RUNTIME_BRIDGE_UNAVAILABLE` result is therefore an
intentional harness limitation. It is not evidence that production bridge
creation is defective. A healthy test path must use a legitimate isolated
bridge and the production readiness predicate, without bypassing freshness,
authentication, reconciliation or protection.

The current native outbound stop has a separate concrete defect:
`BeelzebubReadOnlyOutbound.Shutdown()` sets `senderStopping`, clears the queued
frames and pulses the monitor, but the active `TcpClient` is local to
`SenderLoop`; shutdown neither closes that socket nor joins the owning thread.
A blocked `NetworkStream.Write` and safe generation replacement are therefore
unproved at intake.

## Independent contracts that remain binding

R cannot demonstrate health by itself. Acceptance also retains zero producer
overflow/transport-failure deltas, continuous ordered streams, exact canonical
depth truth, the 1,000 ms queue/event freshness limits, open-ingress queue
equilibrium, durable progress/final verification, immediate protection,
current Sim101 `LOCAL_SIMULATION` flat/disarmed/no-order reconciliation, clean
producer isolation and supported capacity-latch recovery. A favorable R cannot
override any of them.
