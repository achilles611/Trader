# Replay and determinism

`DeterministicExecutionReplay` consumes an already ordered sequence of frozen L3-D signals, explicit L3-B-quality market states, and operator commands. It has no wall-clock access and no randomness. Identical input ordering, frozen Trader V0 artifact, and immutable L3-E configuration reproduce admission outcomes, order IDs, fill IDs, ledger event IDs, position transitions, P&L, ledger hash, and final state hash.

Events cannot move backward in event time. A market event ID is idempotent, and a signal decision ID is idempotent. The simulator never reuses an earlier market observation to fill an order created at a later event time.

The primary test suite includes a deterministic replay of real `SignalDecision` contracts plus isolated adversarial fixtures. Fixtures test execution mechanics; they do not validate the strategy or infer an edge.
