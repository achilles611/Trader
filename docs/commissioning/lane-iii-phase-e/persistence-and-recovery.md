# Persistence and recovery

`SimulationStateStore` checkpoints one deterministic run under its immutable run ID. Each SQLite record stores configuration hash, complete canonical state snapshot, state hash, and ledger hash. The snapshot contains simulated configuration identity, admissions, orders, fills, position, realized P&L, operator/risk state, latest replay market identity, metrics, and ledger.

Recovery requires the same simulation configuration hash, schema, simulator identity, valid ledger IDs and sequence, valid ledger hash, and an exactly reproducible state payload. Corrupt JSON, any mismatched hash, a missing run, or a different configuration raises `SimulationRecoveryRefused`.

Recovery therefore reconstructs known simulated exposure and working state rather than initializing `FLAT`. A data-quality gap may leave execution health `DEGRADED` or `UNRESOLVED`; recovery does not erase that fact. `UNKNOWN != FLAT` is preserved.

Retention is explicit and bounded by configuration (default 100,000 ledger events and 100,000 unique market-event IDs). Reaching either limit fails closed before accepting further replay data; callers must checkpoint and rotate to a new deterministic run rather than silently discarding evidence.
