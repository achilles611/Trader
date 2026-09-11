# Unattended NinjaTrader observer commissioning

This workflow owns only the read-only NinjaTrader observation producer. It does
not arm paper execution, submit an order, clear a lockout, switch a strategy
profile, or copy a compiled DLL.

## One command

Run from the repository root while NinjaTrader and the bound independent Pong
collector are running:

```powershell
pwsh -NoProfile -File .\tools\commission_ninjatrader_observer.ps1 -InstallSource
```

The command performs the complete build/load/feed check:

1. It compiles the checked-in AddOn and Indicator in memory with NinjaTrader's
   bundled Roslyn compiler and refuses any error.
2. It recomputes the combined source fingerprint and requires the Python
   admission constant and the AddOn's embedded identity to match it.
3. With `-InstallSource`, it backs up and installs only the two read-only source
   files. The generated Indicator region is preserved. No DLL is copied.
4. It waits for NinjaTrader's source watcher to replace
   `NinjaTrader.Custom.dll`, then verifies that the assembly contains the exact
   source identity and current observer implementation. If the installed source
   and assembly are already current, the result reports `SKIPPED_CURRENT`.
5. It requires one active `NATIVE_ADDON` attachment for `MNQ SEP26`, an
   authenticated producer with matching build/source identity, healthy
   loopback transport, and two advancing durable checkpoints separated by ten
   seconds. Both checkpoints must contain usable native depth, and collector
   rejection, duplicate, and overflow counters must remain zero.
6. It independently requires `READY_DISARMED`, exact
   `Sim101 / LOCAL_SIMULATION / MNQ SEP26 / quantity 1`, reconciled flat/no-order
   truth, and `live_capital=DENIED`.

Success is emitted as `UNATTENDED_BUILD_AND_FEED_VERIFIED`. Any source mismatch,
compile timeout, stale or multiple attachment symptom, producer mismatch,
non-advancing journal, unusable depth, collector loss, or safety-state mismatch
returns a nonzero exit. The command never falls back to chart selection or F5.

To verify without installing anything, omit `-InstallSource`:

```powershell
pwsh -NoProfile -File .\tools\commission_ninjatrader_observer.ps1
```

The service startup remains a separate profile-bound operation because its
ledger, epoch, audit root, Git identity, and retained risk-continuity state must
be supplied together. Do not use this observer command to improvise or replace
that runtime binding.

## Ordering regression and frozen sources

Before installing, run the deterministic native callback regression:

```powershell
pwsh -NoProfile -File .\tools\test_ninjatrader_callback_ordering.ps1
```

The test compiles the actual `BeelzebubMarketCallbackGate`, pauses callback A
at the old allocation/publication race, and lets callback B contend. It must
report `legacy_failure_reproduced=true`, `production_gate_order=1,2`, stale
generation rejection, and explicit queue saturation. This is a TEST ONLY
forced interleaving and never qualifies market history.

After a successful `-InstallSource` run, treat both installed NinjaScript files
as frozen. A source edit can trigger the NinjaTrader watcher and replace the
loaded assembly, producer session, and acquisition continuity without another
operator prompt.

## Retained qualifying hour across a backend reload

Stop the collector cleanly before certifying a completed interval. The offline
certifier accepts only a retained `eligible=true` interval with zero stream
gaps, inversions, rejections, duplicates, and overflow. It verifies every
journal hash link, every native HMAC, exact stream counts, the durable tip, two
causal live hourly bars, and the classified trades before creating the global
Pong certificate:

```powershell
.\.venv312\Scripts\python.exe .\tools\certify_pong_retained_hour.py `
  --collection-root <stopped-run-audit-root>\pong-authentic-hour `
  --state-root N:\Beelzebub\runtime\pong-production
```

Accelerated fixtures must use `--test-only`; their certificate is marked
`test_only=true` and the production host refuses to load it.

## HEARTBEAT_TIME recovery

Heartbeat timestamps are allocated only after the transport acquires its
bounded write lock. A lock wait longer than 500 ms sends no heartbeat and is
reported explicitly; an ambiguous socket failure invalidates the session.
The dedicated recovery endpoint can clear only the exact retained
`EXECUTION_COMMAND_REJECTED:HEARTBEAT_TIME` latch after a new authenticated
session proves a quiet heartbeat quarantine, fresh exact reconciliation,
flat/no-order truth, and a full verified ledger tip. It writes and reads back a
hash-bound recovery event before clearing memory, and it never arms or starts
paper execution. All P&L, entries, loss limits, and unrelated lockouts remain.
