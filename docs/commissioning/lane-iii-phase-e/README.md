# Lane III Phase E (`l3e`) commissioning summary

`l3e` commissions a deterministic, offline MNQ simulated-execution lifecycle.
It is downstream of the frozen `l3b → l3c → l3d` chain:

```text
frozen L3-D SignalDecision
          ↓ admission (exact identity/hash, expiry, idempotency)
event-time latency → simulated marketable order → top-of-book partial/full fill
          ↓
simulated position / protective stop / strategy exit / risk and operator state
          ↓
append-only ledger, deterministic state hash, SQLite checkpoint/recovery
```

The implementation is [simulated_execution.py](../../../src/lane_iii/simulated_execution.py) and its durable checkpoint surface is [simulation_persistence.py](../../../src/lane_iii/simulation_persistence.py). It is deliberately not a broker adapter, a strategy optimizer, an exchange matching-engine claim, or a scientific Phase E change.

The commissioned configuration fixes one concrete CME MNQ expiry (`MNQU6`, `2026-09`), one externally owned quantity (`1`), a 100 ms event-time default fill path (25 ms processing + 50 ms submission + 25 ms venue), and an explicit top-of-book/no-queue-position fill assumption. Its configuration identity is `f9d9bb60251def787af2cade9b75047e6055c18665a72177db4bd807c3409cb0`; it is included in every checkpoint and must match on recovery.

See the individual contracts and [closure audit](closure-audit.md). Simulated P&L is mechanical diagnostic output only; it is not evidence of strategy edge.
