# Deterministic replay

`DeterministicReplay` consumes `AppendOnlyMarketCapture.normalized_events()` and calls the same `MarketDataPipeline.apply` method used by an adapter caller. It does not provide a second parser or a replay-specific book implementation.

Given identical normalized capture order, source/contract configuration, and code, replay returns identical per-event pipeline results and final reconstructed-book hash. Capture integrity failure, malformed normalized fields, source/contract mixing, a sequence gap, or an unrecovered reconnect is visible in output state or raises; it cannot silently become a healthy book.

Raw capture is intentionally separate from normalized capture. A future adapter can re-normalize preserved raw packets for forensic comparison, while current replay provides deterministic verification of the commissioned canonical path.
