# D.7 commissioning record — 2026-08-17 UTC

This is an evidence record for the first public-data commissioning campaign.
It records scientific ingestion only; it grants no trading or exchange-write
authority.

## Bounded campaign

| Field | Recorded value |
| --- | --- |
| UTC interval | `2026-08-17T00:00:00Z` to `2026-08-17T01:00:00Z` |
| Official source | `s3://hl-mainnet-node-data/node_fills_by_block/hourly/20260817/0.lz4` |
| Expected objects | 1 |
| Forecast / downloaded bytes | 34,145,536 / 34,145,536 |
| Config caps | 1 GiB, 168 hours, one acquisition worker, 256 hot corpus anchors |
| Manifest state | `INGESTED` |
| Cold artifact | `D:\BeelzebubData\source-cache\2026\08\17\hypercore_9142286ba0522de59fcd52a1.lz4` |
| SHA-256 | `0ba4159df0b3761a2cb770ffb3b70d52415845383fe252129b8f05a3b1151466` |

The verified source was copied to D: and then evicted from the E: hot cache.
The archive was never made a live-decision dependency.

## Parser and coverage evidence

| Field | Recorded value |
| --- | --- |
| Read-only archived-object parse | 398,038 fill records in 10.071 s (39,521.9 records/s) |
| Parser integrity | 0 malformed records, 0 unsupported records |
| Normalized historical observations | 796,076 (one wallet-fill and one observed trade-price record per fill) |
| Replay duplicates recorded | 50,944 (from deliberate resumable/replay verification; not silently discarded) |
| Coverage | `PROVEN_COMPLETE`, 1/1 expected hours, fraction 1.0 |
| Timestamp anomaly | 1 source event at `2026-08-16T23:59:59.892000Z`; retained as provenance and excluded from the declared end-exclusive corpus interval |
| Historical evidence limits | trade prints and per-fill volume only; spread, depth, and liquidations remain unavailable |

## D.6 evidence campaign

The immutable corpus fingerprint is
`corpus-0a4d73730b49ec6e4a3b88c441cd`. It selected 256 deterministic,
time-stratified wallet-fill anchors from 396,012 available in-range wallet
fills. The existing D.6 queue completed feature materialization and outcome
labelling without worker failures.

At record time the campaign had 68,640 feature values and 1,689 outcome labels
in the durable science database. It registered no hypothesis, promoted no
indicator or model, and emitted no forward prediction: the bounded first-hour
evidence did not meet the configured scientific gates. That is a valid negative
research result, not a signal or trade instruction.

## Next bounded expansion

The public-observer smoke test created a catch-up plan only—no additional
historical object was downloaded—for the next 19 UTC hours, bounded by the
configured 168-hour and 1 GiB limits. Operators must review its source-object
forecast before acquisition. Public observation is configured with no wallet
subscriptions by default; the five-second `allMids` smoke test persisted 1,894
midpoint observations with zero reconnects and no execution authority.
