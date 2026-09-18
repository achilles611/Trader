# Offline periodic Pong evaluation

This candidate is a callable offline experiment around the preserved Atlas
source. It is not connected to a runtime, native order bridge, account, or
controller. `OfflinePeriodicPong(..., interval_ms=0)` is the default and delegates
to the unchanged event-driven host. Positive intervals opt in to the experimental
scheduler. Production source files and launch configurations are unchanged.

The strict protective screen fails: a transient opposite signal between timer
ticks can request an event-driven exit and disappear before the periodic scan.
The detached synthetic regression demonstrates this for 10, 20, and 25 ms.
These synthetic signals/fills are test fixtures, not market observations.
Keep event-driven behavior selected. Do not proceed to alternating workers.

## Reproduce

Use a fresh output directory for every command; existing evidence is refused.

```powershell
Set-Location H:\PongPeriodic\20260918\candidate
& C:\Users\atlas\Documents\Trader\.venv312\Scripts\python.exe -m unittest tests.test_l3g_pong_periodic tests.test_l3g_pong_production tests.test_l3g_pong_microstructure tests.test_l3g_pong_inputs tests.test_l3g_pong tests.test_l3g_pong_runtime_bridge tests.test_l3g_pong_controller
& C:\Users\atlas\Documents\Trader\.venv312\Scripts\python.exe tools\pong_periodic_fidelity.py --corpus H:\PongMeasurement\20260914\representative-signed-17h44-v2 --output H:\PongPeriodic\20260918\REPRO-fidelity --intervals 10,20,25,20:10
& C:\Users\atlas\Documents\Trader\.venv312\Scripts\python.exe tools\pong_periodic_paced.py --corpus H:\PongMeasurement\20260914\representative-signed-17h44-v2 --output H:\PongPeriodic\20260918\REPRO-natural-20 --interval-ms 20
```

Restore event-driven mode in the offline runner with `--interval-ms 0`, or omit
the interval option. In Python use `OfflinePeriodicPong(host, interval_ms=0)`.
No operating runtime restore command is needed because no operating runtime
configuration was changed.

## Input and timing contract

The historical corpus has 150,000 market records plus 16 original signed
provider-history chunks. Authentication uses the existing local secret provider;
the key is neither printed nor persisted. Original history publication is
2026-09-14T14:46:56.239012Z. The published depth snapshots are capped at ten levels
per side; every captured mutation is retained. This is the entire captured input
universe, not proof of the exchange's full book. No FHB or small-order filter is
introduced. The 2,111 records/s acceptance floor is unchanged.

The deterministic runner applies all records with a matching receipt timestamp
in original capture order before that timestamp's tick. It uses independent
owners for candidate/reference retention, exact immutable artifact-field
equality, and unchanged production calculations at shared cutoffs. The reference
receives the same explicitly added quiet-time expiry maintenance. This proves
same-schedule consistency only. Ordinary event-driven decisions advance
independently. Entry gates remain disarmed and bias is not fabricated, so absence
of entries does not prove entry fidelity.

The paced runner uses an independent spawned producer, absolute monotonic
deadlines, bounded record/byte outstanding limits, original 100 ns receipt
spacing, and a host-wide benchmark mutex. It records actual producer release
lateness. The owner consumes an ordered prefix of records and never labels
released-but-unprocessed input as already applied. A late timer performs one
current evaluation and counts skipped deadlines; it does not queue old scans.
Snapshots bind epoch, source watermarks, cutoff, retained trades/depth, volume
history and policies. One ordered owner holds the lock through the snapshot and
calculation. This is intentionally not a claim of nonblocking live protection.

Paced timing includes the authenticated receiver, analytical owner, collector
journal, flush/fsync, collector status, and authenticated history writes.
Production controller/risk/order/protection processing and their writes are
absent. Therefore CPU/latency results describe this offline pipeline only.
Every market event receives its own receipt-to-first-completed-analytical-attempt
measurement; blocked evaluations are identified. That metric is not actionable
decision latency. Source-to-receipt age and actual-release/admission latency are
reported separately. Detailed deterministic instrumentation is excluded from
the paced performance comparison.

## Dependency map

| Path | Required cadence | Reason |
|---|---|---|
| Signature/envelope/source/sequence validation | Every record | Reject unauthenticated, future, changed-epoch, missing or reordered input |
| Quote/book mutations and trade classification | Every record | Intervening changes affect causal windows; retain all captured source events |
| Trade/depth retention, volume buckets, classified history | Every record | Preserve multiplicity, cumulative changes and provider-history availability |
| Collector journal and durable acknowledgement | Every record | Completion follows flush/fsync, never enqueue alone |
| Flow/liquidity/volume adapters and evidence mapping | Experimental timer boundary | Existing frozen calculation consumes the complete retained input view |
| Discernment expiry | Timer even without arrivals | Invalidates old evidence without manufacturing a switch |
| Discernment/Pong strategy transitions | Coupled to each analytical opportunity | Opposite switches drive exits; invalid/confidence changes affect safety and entry remainder cancellation |
| Native fills, risk, disconnect/kill/protection | Must remain immediate | Offline adapter refuses attached runtime/controller/bridge; production separation is not established |

The first concrete obstacle is the shared analytical dependency of entry and
protective strategy transitions. Preserving only the last book or last direction
cannot reproduce a short-lived exit signal. A further candidate must establish a
complete per-event protection path before reducing decision opportunities.

## Evidence disposition

Read `H:\PongPeriodic\20260918\report.md` and each run's `result.json` for measured
counts, CPU, p99/max latency, missed deadlines, queue slopes/drain, and limits.
`focused-regressions.txt` retains six synthetic protective counterexamples.
`newer-capture-audit.json` authenticates the separate September 18 MNQ DEC26
capture; it is incompatible with this MNQ SEP26 source and was not relabeled.
The two interrupted preliminary diagnostic attempts are preserved and explicitly
excluded from completed measurements.
