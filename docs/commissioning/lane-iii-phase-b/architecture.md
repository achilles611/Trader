# Architecture

```text
raw provider packet
       |
       v
provider adapter (protocol only; no bundled real provider)
       |
       +--> append-only raw JSONL capture
       |
       v
strict canonical MNQ event
       |
       +--> append-only normalized JSONL capture
       |
       v
single-threaded MarketDataPipeline
       +--> quote state / per-stream ordering
       +--> snapshot + delta DOM reconstruction
       +--> mechanical trade flow and CVD input stream
       +--> OHLC bars and CME session measurements
```

The pipeline has a single deterministic `apply` path used by live adapter callers and `DeterministicReplay`.  No asynchronous race determines reconstructed state.  If a caller needs decoupling, `BoundedMarketDataBuffer` fails visibly at capacity; it does not discard data.

`PipelineMetrics` exposes processed, adapter-rejected, duplicate, and sequence-gap counts together with current book quality and optional bounded-buffer pressure. These are operating measurements, not evidence or strategy inputs.

Each pipeline instance admits exactly one source and one concrete CME MNQ expiry.  A caller must make rollover mapping explicit by constructing another pipeline; there is no continuous-contract or auto-roll trading behavior.
