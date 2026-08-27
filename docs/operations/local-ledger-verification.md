# Local ledger verification

## Verifier v2 operations

Full is the forensic authority: schema and identity checks, SQLite `PRAGMA quick_check`, full chain walk, sentinels, and checkpoint generation. Incremental validates read-only access, schema/identity/epoch/file binding, retained Full proof, checkpoint hash, bounded sentinels, and only the chain tail. Incremental deliberately reports `quick_check: inherited_from_full`; it never runs SQLite `quick_check` itself.

Old v1 checkpoints are upgraded only when an immutable matching Full PASS artifact is found and its full-tip hash is still in current ancestry. Otherwise verification fails closed and requires Full. Historical result artifacts are never rewritten.

The detached current artifact is atomically refreshed at stage transitions, every 64K rows, or about once per second. It includes counters, captured total, throughput, optional ETA, stage timings, and read-only DB/WAL/SHM/free-space telemetry. The verifier never checkpoints, truncates, deletes, or mutates the trading ledger. Local controls are `BEELZEBUB_L3G_VERIFIER_WARNING_FREE_BYTES` (10 GiB default), `BEELZEBUB_L3G_VERIFIER_EMERGENCY_FREE_BYTES` (2 GiB), and `BEELZEBUB_L3G_VERIFIER_WAL_GROWTH_WARNING_BYTES` (1 GiB). The emergency floor aborts only the verifier with `STORAGE_PRESSURE_ABORT`.

## Bindings, epoch adoption, and profiling

Production binds the ledger with `BEELZEBUB_L3G_PAPER_LEDGER`, audit root with `BEELZEBUB_LEDGER_AUDIT_ROOT`, and new-ledger epoch with `BEELZEBUB_L3G_PAPER_LEDGER_EPOCH`. A `hot` ledger defaults to a sibling runtime `audit` directory. Backend startup logs and exposes a non-secret `BEELZEBUB_RUNTIME_BINDING` record.

Before switching an existing deployment from the historical C: audit root to N:, copy and SHA-256 verify its immutable artifacts without deleting the source:

```powershell
scripts\migrate_ledger_audit.ps1
```

An existing `UNSPECIFIED` ledger is displayed as **LEGACY / UNSPECIFIED** and is never silently relabelled. In a confirmed maintenance window only, following a clean PASS with retained Full proof, run:

```powershell
scripts\adopt_l3g_legacy_epoch.ps1 -TargetEpoch L3G-PAPER-EPOCH-002 -OperatorId <operator-id>
```

It verifies the current Full anchor and creates a write-once external adoption receipt before changing only ledger epoch metadata; rows and the hash chain are untouched.

For a read-only storage profile after a benchmark:

```powershell
.venv312\Scripts\python.exe -m src.l3g_paper.verification profile-storage --ledger N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3
```

The report includes global/domain rows, table and index pages/bytes/payload where SQLite `dbstat` is available, main DB, WAL, and SHM. Do not VACUUM or rewrite the current epoch. If duplicated large JSON is confirmed as the main amplifier, the next clean epoch should use one authoritative payload/chain table plus domain reference indexes.

No writer-owned `PRAGMA wal_checkpoint(PASSIVE)` was added in this hotfix. It needs a dedicated concurrent writer benchmark that proves it cannot delay safety/order paths after the verifier releases its snapshot. Until then, retain telemetry only.

Lane III paper-ledger verification is a deterministic local service. It has no
LLM, API, broker, order, account, or live-capital dependency. The verifier is
started as a detached `python -m src.l3g_paper.verification run` process and
opens the configured ledger with SQLite `mode=ro` and `query_only=ON`.

## Artifacts and checkpoint

All verifier-owned state is under `runtime/audit` (or the explicitly local
`BEELZEBUB_LEDGER_AUDIT_ROOT`):

- `ledger-verification-latest.json` is the most recent terminal result.
- `ledger-verification/<timestamp>-<verification_id>.json` is an immutable
  historical terminal report.
- `ledger-verification-checkpoint.json` advances atomically only after PASS.
- `ledger-verification-current.json` and the exclusive lock record an active
  detached process. An abandoned process is converted to `INTERRUPTED` on the
  next local status read; it never becomes PASS.

The checkpoint binds the resolved path, ledger UUID, epoch, schema version,
last verified sequence/hash, verifier version, device/inode metadata, current
tip, and bounded pre-checkpoint row witnesses. A changed UUID, epoch, schema,
file identity, truncated tip, checkpoint hash, or witness fails closed. The
full scan still verifies every historical row and hash-chain link.

SQLite itself cannot prove arbitrary in-place alteration of every historical
byte in constant time without a trusted storage primitive or rereading it.
The checkpoint therefore never claims that filesystem mtime alone is trusted:
it combines stable file identity, sealed ledger identity, chain anchors, and
sentinel witnesses. Explicit Full verification remains the forensic authority.

## Modes

- **Auto** uses Full when no checkpoint exists, Incremental when a checkpoint
  is trusted, and fails closed with `full_scan_required=true` when a checkpoint
  mismatches.
- **Fast / Incremental** inherits the retained Full `quick_check` proof,
  performs schema/identity/epoch checks and checkpoint validation, and verifies
  only the chain tail after the checkpoint. It never reruns `quick_check`.
- **Full** performs the same checks plus hash-chain verification from genesis.

The verifier takes one read transaction, so an active append-only writer can
continue normally. Its checkpoint represents the coherent snapshot tip; a
later append is handled by the next incremental run. No verifier code imports
an execution transport or accepts order/account parameters.

## Scheduling and commissioning

BeezConsole persists Daily or Weekly schedules through the existing local
operations scheduler using the allowlisted `lane_iii.ledger_verification`
task. The task only launches the detached verifier process. It does not scan
rows in the scheduler, UI, HTTP request, or WebSocket task. The missed-run
policy is `SKIP`: one missed time creates a durable missed record and the next
normal occurrence runs; there is no surprise catch-up scan.

Commissioning admission consumes the compact verifier artifact plus the writer's
transactional v3 tail watermark. It requires a fresh PASS, trusted
checkpoint, valid ledger identity/ancestry, retained Full provenance, and an
anchor record hash that still matches the ledger. Exact equality with the live
tip is ideal but not required. When the tip has moved, admission accepts an
exactly classified live tail only when both the last authority mutation and the
last unknown record are at or before the verified anchor. A tail containing an
exact authority observation may report
`VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL`; a passive-only tail retains
`VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL`.

Policy `l3g-commissioning-passive-tail-v3` assigns every record to one explicit
class. `PASSIVE_DATA` contains only exact QUOTE/TRADE/DEPTH observation
envelopes, exact paper `EVIDENCE`, and exact no-side-effect strategy decisions.
`AUTHORITY_OBSERVATION` contains exact informational account items and the
version-1 commissioning-readiness attestations whose payloads carry
`authority_effect=NONE`,
`record_semantics=COMMISSIONING_READINESS_STATE_ATTESTATION`, the exact
semantics version, and the exact `WARMED` or `NOT_WARMED` state. These records
describe runtime or authority-relevant state but cannot grant or consume
execution authority. Exact key sets, session identity, policy hash, provenance,
and semantic values are required; domain membership alone grants nothing.

Known commands, receipts, intents, risk grants/events, orders, fills,
commissioning ownership, session authority changes, and other side-effect
records are `AUTHORITY_MUTATION`. `POSITION_SNAPSHOT` is not blanket-allowed;
it remains mutation-classified until an exact, independently justified
observation schema exists. Any unrecognized kind, extra or missing field,
malformed marker, old unmarked warmup row, or future shape is `UNKNOWN` and
fails closed. Both mutation and unknown watermarks must be covered by the
verified anchor or admission returns `COMMISSIONING_LEDGER_TAIL_UNTRUSTED`.

Separate mutation, observation, and unknown watermarks plus a classification
cursor are updated in the same SQLite transaction and ledger-sequence order as
each append; deferred batches use that same ordered update. Migration from v2
is bounded: previously safe classifications are retained, the old overloaded
unsafe watermark becomes unknown, and only the suffix is classified under v3.
Historical rows are never silently reinterpreted or scanned wholesale during a
hot-ledger restart.

Admission captures the verifier anchor, all three watermarks, live tip, tail
kinds/categories, current broker reconciliation, session, and entry ownership
while holding the runtime admission lock. Accepted unverified observations are
never authority for current state: the validator independently requires exact
Sim101/MNQ binding, FLAT quantity zero, complete position and order snapshots,
zero owned/entry orders, no unresolved command/native order/execution, clean
current transport reconciliation, ownership `NONE`, `READY_DISARMED`, and live
capital `DENIED`. Therefore corruption of an observational row cannot create
execution authority or substitute for current broker facts. The accepted
evidence is embedded in `COMMISSIONING_OWNERSHIP_RESERVED`; later safe records
do not revoke the reservation, while other authority paths cannot cross the
same lock before ownership is reserved. Exit and flatten safety paths remain
available.

The existing 15-minute commissioning freshness bound is retained. Current
incremental evidence shows a roughly one-to-two-second verifier over tens of
thousands of rows, so the established bound is operationally ample without
allowing a day-old anchor. Stale, mutation-bearing, or unknown tails launch Auto
and deny admission. A FAIL, IN_PROGRESS, identity mismatch, incomplete classification, or
Full-required result also denies commissioning. After a lifecycle completes,
Auto Incremental must cryptographically verify the accepted tail and all
commissioning records before closure can claim PASS.

The complete operator sequence, warmup semantics, atomic start boundary, and
post-run closure contract are documented in
[`lane-iii-commissioning-runbook.md`](lane-iii-commissioning-runbook.md).

## Performance capture

Full-scan and incremental performance are recorded in every result artifact:
`rows_scanned`, `bytes_scanned`, and `duration_seconds`. The current workspace
does not expose the approximately 28.9 GB operational ledger through
`BEELZEBUB_L3G_PAPER_LEDGER`, so no production-ledger scan was performed by
this change. Run **Full** once against the selected hot Epoch 002 image, then
append a representative tail and run **Fast**; retain those resulting JSON
artifacts as the operational performance evidence.
