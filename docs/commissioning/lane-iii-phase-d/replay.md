# Replay and deterministic diagnostics

`DeterministicTraderReplay` invokes the same `TraderV0.evaluate` method used by
normal evaluation. Given identical ordered pairs of:

```text
HypothesisEngineSnapshot
TraderDataQuality
```

and the frozen artifact, it produces identical decisions, decision IDs,
decision hashes, reason codes, transitions, and sequence hash. All time gates
use the L3-C evaluation timestamp. There is no wall-clock or random input.

`source_state_hash` binds the complete L3-C snapshot hash to the synchronized
quality payload and upstream market-state hash. Each decision separately
retains the L3-C and quality hashes.

Diagnostics record:

```text
evaluations
NO_TRADE results
LONG signals
SHORT signals
EXIT signals
blocked entries by reason
duplicate suppression
hypothesis expirations encountered
retained signaled hypothesis IDs
```

The signaled-ID memory is capped at 256. The strategy retains only the current
active thesis, the last decision/source state, counters, and that bounded
deduplication history. Metrics make no profitability claim.
