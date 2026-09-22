# London startup correction — offline Gate A candidate

## Scope and identities

The missed September 22 `[08:00Z,09:00Z)` / `[02:00,03:00) MDT` hour remains
`WINDOW_NOT_STARTED`, with zero qualified seconds. R2 remains exhausted at
`1/1`. This pass authorizes offline repair, tests, integration and administrative
review only. Sol is the sole integrator/operator. No runtime restart, native
installation/compile/reload, collection, recovery or order action occurred.

- STARTING COMMIT: `9d56868e691854b097d25b9b245f5412e5185e25`
  (tree `736fb8dca7eaf630734ca1647829ad73cce0517c`).
- IMPLEMENTATION COMMIT: `a091a28186fc4a32685d93046b6883dcdfd1b6db`
  (tree `44e6620eedbced6145cce3682fce126a387f7b0a`).
- Isolated implementation worktree:
  `N:\Beelzebub\worktrees\london-offline-repair-20260922`.
- New evidence root: `N:\Beelzebub\london-offline-repair-20260922`.
- FINAL / EVIDENCE HEAD: the final `candidate-seal.json` in that root binds
  the integrated implementation and report-only descendants after Caelum review.
  No operational process is running from that final head.

## Evidenced cause and corrected attribution

`-PrepareSourceOnly`, introduced at `2331c74`, deliberately bypassed receiver
readiness. Installing watched source rebuilt the native assembly at
`07:33:20.9018659Z`; native telemetry began at `07:33:22.7027699Z`. The receiver
and collector did not start until `07:41:56.651633Z`. The preparation mode let
the native producer activate while its destination was absent.

The retained journal's complete 1,885-record hash chain agrees with its final
durable tip and final status. Every native timestamp predates receiver startup.

| Measurement | First retained record | Last retained record |
| --- | --- | --- |
| Native timestamp | `07:36:55.1018541Z` | `07:37:45.6891625Z` |
| Collector receipt | `07:41:56.806637Z` | `07:42:00.671804Z` |
| Global sequence | `110549` | `133175` |
| Producer overflow counter | `54250` | `71468` |
| Producer transport-failure counter | `23309` | `28926` |

Thus `+17218` overflow and `+5617` transport failures describe the 50.587308
seconds between those native snapshots, before the receiver existed. They are
real losses and remain disqualifying. They are not failures timed over the
3.865167-second receipt burst and cannot establish its sustainable service rate.
The signed HEALTH observation at native `07:37:00.7819002Z` reported **zero
socket writes**, 107 reconnects/failure events, queue 32,520/high-water 32,768,
and oldest queue age 171,195.8499 ms. Its 111.134 records/s was dequeue/batch
accounting, not delivery.

Across the retained native span, missing sequence values were DEPTH **19,381**
over 10 jumps, QUOTE **95** over 8 jumps and TRADE **84** over 6 jumps. The
earlier host count of 534 is its first latched DEPTH gap (`103379 -> 103914`),
not the complete journal count. All retained global sequences increase; one
source/build/session/generation persists. Frame age was 254,982.642–301,710.783
ms. No startup loss is removed or hidden by a new baseline.

The rejected wire is absent from the saved artifacts. Its generic
`out_of_order_or_prior_session` reason does not identify whether MARKET_DATA,
ACCOUNT_STATE or CONTROL regressed. Multiple socket interleaving is a possible
mechanism, not an established cause. The candidate preserves admission ordering
and does not purport to repair an unobserved sequencing defect.

The old reports/receipts are preserved. This report and `startup-attribution.json`
supersede only their interpretation of loss timing and the scope of the 534 gap
count. No source observations, counters, journal or ledger history were changed.

## Minimal corrective behavior

1. **Receiver before native source activation.** `-PrepareSourceOnly` now refuses
   before native-file access. Every install requires a ready loopback `48135`
   receiver, error-free/live collector, exact requested instrument and expected
   source fingerprint, plus matching runtime/checkout identity. Checks repeat
   before each changed file and after the producer checkpoints. Unchanged
   installed files are not rewritten.
2. **Frozen attribution.** Commissioning freezes HEAD/tree/source hashes and
   requires those sources to match HEAD. The launcher accepts paired
   `-ExpectedGitSha`/`-ExpectedGitTree`, checks the checkout before/after startup
   and the binding read, checks clean runtime sources, and freezes launcher and
   entrypoint hashes. A mismatch throws and performs no automatic restart.
3. **No mixed live two-file installation.** If both native files need changes,
   installation refuses while NinjaTrader runs. Cold staging writes the
   indicator first and the fingerprint-bearing AddOn last; native startup
   remains a separately controlled operation. Installed parity and loaded
   identity checks remain mandatory.
4. **Useful refusal evidence.** Authenticated ordering refusals retain bounded
   channel/session/current/prior sequence and wire-hash metadata, with no
   payload, signature, key or account values. Unverified input retains the
   original error and receives no authenticated-context claim. Refusal behavior
   and reason prefixes are unchanged.
5. **Reproducible attribution.** A read-only analyzer verifies retained journal
   chain/status identity and reports native-time versus receipt-time spans,
   counter boundaries and per-stream gaps. Its output explicitly reports
   `signature_reverified=false`, capacity `UNKNOWN` and qualified seconds `0`.

The repository/native mismatch was a separate administration race. Runtime
binding named `5998a8d`; child commit `15db370` changed only Caelum's report in
the same worktree during startup. Commit timestamps were `07:40:35Z` and
`07:41:55Z`; those are commit times, not runtime launch timestamps. There was
no executable diff between those commits. The exact identity gate was correct
to hold, and the candidate introduces no documentation-only exception.

## Verification and evidence

Final combined offline suite: **154 passed, 34 subtests passed in 58.08 s**.
Suites: receiver-first PowerShell guards; frozen-launch guards; native source
contracts; bounded rejection provenance; startup attribution; native observation;
Lane III Phase F2; receiver safety; launcher binding. New PowerShell tests execute
the production guard functions with mocked external calls, without launching a
runtime or compiling/installing native source. Negative cases cover absent or
failed receiver/collector, wrong host/port/instrument/fingerprint, dirty source,
HEAD/tree/source drift, runtime mismatch, preparation bypass, and live two-file
mutation. Ordering tests retain all existing refusal and authentication behavior.

- JUnit: `N:\Beelzebub\london-offline-repair-20260922\regression-results.xml`
  SHA-256 `1e6602568d285109233dcceeb90c0075fdc5b97fed787f81915ad465f8726cae`.
- Attribution: `N:\Beelzebub\london-offline-repair-20260922\startup-attribution.json`
  SHA-256 `87821f3c0f4ec94d2978dc0092540d4c5f41421a4974dff8cea9874610ffa613`.
- Original journal SHA-256:
  `473b7f0c863d6572e5d54386f1060f74c72171f41874e55de83e051f81613ef5`.
- Original packet: `N:\Beelzebub\london-qualification-20260922T080000Z`.
- `git diff --check` passed. Test/temp/output paths used N:; capacity thresholds
  and production storage were not modified.

Native source is unchanged. Installed AddOn SHA-256 remains
`ffbb655d60a392e20dbeea9dd8f8db1ecbd4e68a1cd56b9c4f5a258f11e619e5` and
installed DLL remains `b70556ddcce897c018255b7cfd49e2d5ec03b96cdf6a6ec6a0a06328a2cb8756`.
No native installation or loaded-runtime claim follows from this Python and
PowerShell candidate. The original clean shutdown and Full PASS through 334199
remain historical receipts, not authority for a later start or recovery.

## Remaining operational boundary

Only a new **Joseph-authorized receiver-first native lifecycle and separately
predeclared future collection pass** can exercise the correction. The exhausted
R2 cannot be silently reused. No future time window is declared by this record.

The candidate is ready for that decision only after Caelum's Gate A review and
final source/report integration. The concrete checklist for the future operator:

- Freeze the final HEAD/tree after every administrative commit; supply that
  tuple to the launcher and keep the checkout unchanged through closeout.
- Resolve a supported cold/native lifecycle that yields a fresh producer with
  the exact receiver already ready. The guarded installer does not orchestrate
  NinjaTrader shutdown/startup. With native stopped, watcher verification needs
  a separately coordinated startup within its bounded wait; do not mistake cold
  staging alone for completed commissioning.
- Prove one fresh authenticated source/build/session/generation, continuously
  ordered records, canonical depth, freshness, current warm-up and zero new
  producer/receiver/collector loss. The old backlog cannot satisfy these checks.
- Obtain current exact Sim101 flat/no-orders/reconciled/disarmed proof, actual
  destination writer health with unchanged floors/runway, Full, supported latch
  recovery if eligible, and the required second Full before any future window.
- Measure current natural native/receiver/host/durable behavior under the
  existing contract. Sustainable native capacity and the old rejected frame's
  cause remain unknown. No ratio threshold, feed reduction or loss tolerance was
  added.

These are future operational checks, not unfinished offline implementation.
The runtime endpoint still omits tree and loaded-module hashes; launcher source
checks do not substitute for loaded-module proof. No wrapper prevents a commit
after it returns, so the operational source freeze remains necessary.
