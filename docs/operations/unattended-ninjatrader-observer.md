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
