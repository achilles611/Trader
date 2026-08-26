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
- **Fast / Incremental** performs SQLite `quick_check`, schema/identity/epoch
  checks, checkpoint validation, and verifies only the chain tail after the
  checkpoint.
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

Arming or submitting an explicit commissioning entry consumes the compact
artifact only. It requires a fresh PASS, trusted checkpoint, valid chain, and
a verified sequence at or beyond the current ledger tip. An absent/stale tail
launches a local Auto run and returns a fail-closed denial until PASS. A FAIL,
IN_PROGRESS, or Full-required result also denies commissioning. Exit and
flatten safety paths remain available and are not delayed by verification.

## Performance capture

Full-scan and incremental performance are recorded in every result artifact:
`rows_scanned`, `bytes_scanned`, and `duration_seconds`. The current workspace
does not expose the approximately 28.9 GB operational ledger through
`BEELZEBUB_L3G_PAPER_LEDGER`, so no production-ledger scan was performed by
this change. Run **Full** once against the selected hot Epoch 002 image, then
append a representative tail and run **Fast**; retain those resulting JSON
artifacts as the operational performance evidence.
