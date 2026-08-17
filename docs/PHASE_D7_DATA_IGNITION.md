# Phase D.7 — Data Ignition & Scientific Commissioning

D.7 supplies the existing D.6 worker with public evidence. It does not add a
trading adapter, signer, private-key storage, exchange-write route, or live
capital authority.

## Sources and capability boundaries

Historical wallet fills and observed trade prices come only from the documented
official HyperCore requester-pays `node_fills_by_block/hourly` objects. Public
websocket observation uses Hyperliquid `allMids` and, only for explicitly
configured public wallets, `userFills`. Historical fill prices are trade prints,
not a reconstructed midpoint. Spread, book depth, liquidation, and unavailable
volume evidence remain missing; D.7 never substitutes zero or synthetic data.

## Historical flow

`science plan-history --start ... --end ...` creates deterministic,
end-exclusive UTC hourly slots. `science acquire-history` performs a bounded
requester-pays prefix lookup for every slot, forecasts object bytes before GET,
checks hot-drive capacity, verifies local SHA-256, parses the source, and writes
the normalized records through the batched `ScientificWorker.ingest_observations`
bridge.

Every source hour has a durable manifest state:

`PLANNED → AVAILABLE → DOWNLOADING → VERIFIED → PARSED → INGESTED`

`MISSING_SOURCE`, `FAILED`, and `SUPERSEDED` remain durable evidence rather
than being counted as complete. A cancellation request is checked between
source objects. The configurable byte and hour caps prevent archive-wide
downloads and make requester-pays exposure explicit.

After verification and ingestion, the raw hot copy is copied and checksum
verified in `D:\BeelzebubData\source-cache` before it is evicted from E. The
scientific SQLite database is never evicted for cache pressure. Cold archives
are not read by a live decision callback.

Dense verified source hours can contain far more events than the bounded D.6
hot working set. D.7 retains every normalized raw observation, then selects a
deterministic chronological time-stratified `max_corpus_observations` wallet-fill
subset for D.6 feature and outcome queueing. Out-of-corpus pending projection
work is marked `SUPERSEDED`, never deleted; the selection IDs and method are
persisted and become part of the corpus snapshot.

## Coverage and corpus policy

`science coverage` persists a `DataCoverage` result with expected/verified/
missing/parsed hours, observation and duplicate counts, timestamp anomalies,
wallet attribution quality, market-evidence availability, and a fraction.
States are `PROVEN_COMPLETE`, `PROVEN_PARTIAL`, `KNOWN_GAP`, `UNPROVEN`, or
`CORRUPT`.

When `commissioning.enabled` is true, the D.6 discovery and historical-test
stages fail closed unless an immutable D.7 corpus snapshot has
`PROVEN_COMPLETE` coverage at or above `min_coverage_fraction`. Each D.7
historical hypothesis includes that corpus fingerprint, and historical
experiment fingerprints include the snapshot too. This prevents a dataset from
silently changing under a registered experiment.

## Operation

```powershell
python main.py science source-status
python main.py science plan-history --start 2026-08-01T00:00:00Z --end 2026-08-08T00:00:00Z
python main.py science acquire-history --start 2026-08-01T00:00:00Z --end 2026-08-08T00:00:00Z
python main.py science coverage
python main.py science commission
python main.py science observe
```

`commission` only orchestrates the acquisition/corpus preparation and the
existing D.6 queue. `observe` persists public observations before expensive
science, reconnects on failure, identifies a bounded historical catch-up plan
after downtime, and never fits a model in its websocket callback.

For continuous Windows operation use `scripts/start_beelzebub.ps1`; it creates
an exclusive hot-root PID lock, starts the public observer and D.6 worker with
separate logs, and has companion `scripts/beelzebub_status.ps1` and
`scripts/stop_beelzebub.ps1`. No browser window is required.

The Control Center's **Data Ignition** page reads these manifests and derived
counts directly. It does not fabricate values and contains no acquisition or
execution control.
