# Replay determinism

`DeterministicHypothesisReplay` applies each canonical L3-B event through the
normal `MarketDataPipeline.apply` path and immediately sends the returned
`PipelineResult` to `HypothesisEngine.observe`. Live and replay callers use
the same engine code.

All evaluation times derive from event ordering time or explicit
`advance(as_of)` value. Backward time is refused. Retention is bounded and
deterministic: evidence and historical records sort by timestamp and stable
identity before eviction. Evidence IDs, hypothesis IDs, configuration hashes,
and snapshot hashes derive from canonical payloads. L3-C retains no wall-clock
timing in its deterministic state or replay result. Operational throughput is
measured externally during commissioning and is never an evaluation input.
