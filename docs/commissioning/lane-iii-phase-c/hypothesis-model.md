# Hypothesis model

The initial taxonomy has four narrow identities: bullish reversal, bearish
reversal, bullish continuation, and bearish continuation. A record contains
its deterministic ID, identity, creation and update time, expiry, lifecycle
state, confidence assessment, supporting/contradictory evidence IDs,
invalidation evidence IDs, and configuration hash.

States are `FORMING`, `ACTIVE`, `CONFLICTED`, `DECAYING`, `INVALIDATED`, and
`EXPIRED`. A record is conflicted when valid evidence both supports and
contradicts it. An explicit invalidating impact wins over an ordinary
confidence adjustment. When no valid impacts remain, the active record expires
and a later fresh interpretation receives a new ID.

All identities can coexist. `dominant_hypothesis_id` is absent unless an
active unconflicted record reaches the configured threshold and exceeds the
next record by the configured margin. Absence is an intentional unresolved
outcome, not a hidden instruction.
