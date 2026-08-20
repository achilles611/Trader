# Lane III Phase C commissioning summary

L3-C is the deterministic MNQ Hypothesis & Confidence Engine. It is a new
interpretation layer over frozen L3-B observations:

```text
L3-B canonical observations -> L3-C evidence -> family assessment -> competing hypotheses
```

The implementation is `src/lane_iii/hypothesis_engine.py`. A caller first
applies each event to `MarketDataPipeline`, then passes that exact event,
`PipelineResult`, and pipeline to `HypothesisEngine.observe`. Replay uses the
same two calls in `DeterministicHypothesisReplay`; it has no alternate path.

L3-C creates immutable provenance-backed evidence and relative-support
assessments for bullish/bearish reversal and continuation. It can retain
conflicting records or no dominant record. A relative-support value is not a
probability, trade signal, entry, size, or capital authority.

The commissioning tests are in `tests/test_lane_iii_phase_c.py`. See the
closure audit for the freeze gate and the authority matrix for explicit denials.
