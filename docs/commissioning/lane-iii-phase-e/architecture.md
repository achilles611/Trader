# Architecture

`DeterministicMNQSimulator` is the only execution-state authority in `l3e`. It consumes exact immutable L3-D `SignalDecision` objects and explicitly supplied replay market state. L3-D keeps thesis/signal state; the simulator keeps orders, fills, position, P&L, risk/operator state, and ledger. Neither state is inferred from the other.

`SimulatedMarketState` is limited to one commissioned `MNQContract`, an event timestamp, top bid/ask and displayed quantities, and frozen L3-B `DataQuality`. No transport or provider client is present. `DeterministicExecutionReplay` applies caller-provided ordering synchronously. There is no wall clock or random source.

`SimulationStateStore` persists hash-checked snapshots to SQLite. The snapshot includes configuration identity, admissions, orders, fills, position, operator/risk state, recent market identity, metrics, and complete ledger. Hash or schema mismatch refuses recovery rather than inventing a flat position.
