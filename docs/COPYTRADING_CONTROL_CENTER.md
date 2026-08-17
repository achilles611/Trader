# Copy-Trading Control Center (Phase C)

Phase A discovery and Phase B research remain frozen. Phase C is an additive, local **PAPER-ONLY** operator interface over their persisted SQLite evidence and the existing simulated paper execution engine.

## Start locally

From the repository root, install the frontend once:

```powershell
cd control-center-ui
npm install
npm run build
cd ..
```

Then start the local application with its single supervised paper watcher lifecycle:

```powershell
.venv\Scripts\python.exe main.py copy-control-center --with-watcher
```

Open [http://127.0.0.1:8090](http://127.0.0.1:8090).  The frontend build is served by FastAPI. Use `--port 8091` if the default port is occupied.

Without `--with-watcher`, the Control Center is read/control-only and reports `NOT_ATTACHED` watcher health. With it, one local supervisor continuously derives membership from `monitored_execution_wallets()`: Active paper-entry targets plus non-active wallets that still have open paper sleeves for exit handling. It never subscribes every shadow candidate.

The supervisor performs serialized replacement on each membership change: it stops and awaits the old watcher before starting a new watcher, so two watchers never consume the same public stream concurrently. Empty membership is a clean `IDLE` state with no watcher task. Watcher health and WebSocket updates include desired and subscribed wallet lists/counts, in-sync state, last membership change, active-entry count, and exit-only sleeve-wallet count. A failed watcher enters `DEGRADED` and retries locally without stopping FastAPI. More than 10 desired subscriptions stops the watcher, pauses new paper entries as a fail-safe, and reports the explicit capacity reason; it does not promote Shadow wallets.

For frontend development, run this in a second terminal:

```powershell
cd control-center-ui
npm run dev
```

The development server proxies `/api` and `/ws` to the FastAPI process at port 8090.

## Turnkey candidate discovery

The **Discovery** page is the fresh-install entry point. It resolves the recent official HyperCore `node_fills_by_block` source through the documented `s3://hl-mainnet-node-data/node_fills_by_block/` requester-pays S3 prefix, caches verified objects under `artifacts/hypercore-cache/`, and passes those cached files to the unchanged Phase A `hypercore-file` parser.

Hyperliquid documents this node-data distribution and its requester-pays transfer requirement at [Historical data](https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data). Trader uses the standard boto3/AWS credential chain only; it never asks for credentials in the browser, stores credentials, accepts arbitrary URLs, or accepts local paths from the API. Click **Test Source Access** after configuring normal AWS credentials and requester-pays billing access. Missing access is shown with actionable setup guidance.

- **Quick Scan**: exactly one most-recent available completed UTC-hour object, up to 1,000 candidates.
- **Standard Scan**: exactly six most-recent available completed UTC-hour objects, up to 2,500 candidates.
- **Deep Scan**: exactly 24 most-recent available completed UTC-hour objects, up to 5,000 candidates.

All three presets require at least two observed events and preserve Phase A's 30-day recency rule. The resolver starts at the most recently completed UTC hour (one publication-lag hour behind the current UTC hour), derives only narrow `node_fills_by_block/hourly/YYYYMMDD/HOUR` prefixes, and walks backward no more than 48 hours to collect the requested count. It never lists or paginates the historical root prefix and never uses S3 `LastModified` to define the source interval.

The official node README documents the corresponding UTC hourly node-output organization as `hourly/{date}/{hour}` ([node output documentation](https://github.com/hyperliquid-dex/node)). The exact official S3 key returned by the narrow requester-pays listing is validated and persisted, including its actual suffix when present; Trader does not pre-assume `.lz4`, hour padding, or a manifest name. Source provenance distinguishes the path-authoritative `data_hour_start` / `data_hour_end` from storage-only `last_modified`, and retains bucket, key, size, ETag/checksum, local cache path, and acquisition time.

Click **Test Source Access** before starting discovery. This is one `ListObjectsV2` requester-pays probe below a recent UTC-date prefix with `MaxKeys=1`; credentials alone show `UNTESTED`, a successful probe shows `READY`, requester-pays denial shows `SETUP_REQUIRED`, and other source errors show `UNAVAILABLE`. The selected `AWS_PROFILE` name may be displayed, but no credential material is displayed or stored.

A durable Phase C orchestration job reports source resolution, preflight, acquisition, parsing, and frozen Phase A completion via `/ws` as `discovery_job_update`. Before its first download it resolves the complete plan, identifies valid cached objects, totals transfer bytes, reserves disk/cache capacity, and pins every current-job cache path so cleanup can remove only unrelated cached objects. A plan larger than the 5 GiB cache or insufficient disk fails before the first GET. The cache uses atomic `.partial` downloads, size checks, and a streamed local SHA-256 digest on every accepted object; legacy or same-size-tampered cache entries are re-acquired rather than trusted. Cache objects are never committed to Git. Job results include source interval, hourly-object count, bytes acquired/reused, Phase A run ID, and safe source provenance. Rediscovery preserves existing operator states and does not create Active targets, paper positions, or watcher membership. Phase B analysis remains a deliberately manual next step.

## Operator controls

- **Pause Paper Entries** persists `ENTRIES_PAUSED`: it blocks only new `OPEN`/`ADD` paper signals. Existing `REDUCE`, `CLOSE`, and flip-close handling continues.
- **Resume Paper Entries** persists `RUNNING`.
- **Close All Paper Positions** uses the existing paper engine and only closes sleeves with a fresh persisted market reference. Completion is verified from the persisted open-paper-position set after every attempt. If any position remains, whether skipped or failed, the response is `partial`, includes `attempted`, `closed`, `failed`, and `remaining_open_positions`, records a warning, and leaves new entries paused until an operator explicitly resumes them.
- **Exit + Pause Paper Trading** flattens fresh-mark paper sleeves, then persists `PAUSED` so new entries remain disabled through restart.
- Only **Active** wallets may open or add paper sleeves. Shadow, approved, muted, and rejected wallets cannot bypass this gate; every wallet still retains normal exit handling for an open sleeve.

Mutable PAPER execution belongs to the `CopyTradeService` instance shared with
the Control Center. Watcher fills, market marks, restores, and control closes
are serialized there. Closing first reloads durable portfolio truth. Mark
persistence is observational only: it can update a still-open sleeve's mark,
unrealized P&L, drawdown, and timestamp, but cannot create a sleeve, change
economic quantity/capital/fees/realized P&L, or clear `closed_at`. Therefore a
stale mark or a post-restart watcher path cannot resurrect a committed close.

Source recovery is independently visible in System health and `/api/recovery`.
Each wallet reports `CONTINUOUS`, `RECOVERING`, or `RECOVERY_INCOMPLETE` with
its exact durable overlap anchor. `RECOVERY_INCOMPLETE` blocks only new PAPER
`OPEN`/`ADD` signals; `REDUCE`/`CLOSE` remain available. `/api/recovery/{wallet}/safe-rebaseline`
requires a fresh, parseable flat public clearinghouse state and preserves the
incomplete source ledger for audit; it never changes a PAPER sleeve or creates
missing source economics.

The interface deliberately contains no private-key fields, exchange API-secret fields, order submission routes, live-mode enablement, signing, or live execution adapters.

## API and WebSocket

REST is used for research tables and details; `/ws` publishes incremental `control_state`, `portfolio_update`, `position_update`, `watcher_health`, and activity events. Candidate searches, sorting, filters, and paging are server-side and paginated.

Phase C reads B.2 through a read-only normalization boundary. The persisted Phase-B finalist-recommendation record is contract version 1 and is the sole authority for the Shadow finalist cohort: Phase C never re-scores, reapplies finalist gates, or re-diversifies a candidate. Legacy scores can be displayed as compatibility context but never influence paper-entry decisions. An Active transition is available only through `CopyControlCenter.activate_wallet`; it requires the candidate analysis `last_run_id`, an eligible canonical `provenance='phase_b'` score for that same run and the current configuration fingerprint, a completed or `completed_with_errors` parent run, and a current version-1 persisted recommendation with `finalist_eligible=true` and a non-null Phase-B diversification `selection_rank`. A missing, stale, unsupported, rejected, or non-selected recommendation returns an error before target state or audit activity is written. Approval alone does not grant watcher or paper-entry membership. The dossier renders Phase A prefilter reasons, Phase B hard gates, canonical score fields (including confidence, source quality, score version, run, provenance, and fingerprint), soft score reasons, and diversification evidence as separate concepts.

All Phase C state is persisted in the existing SQLite database via the additive `copy_control_center_state` and `copy_control_center_activity` tables.
Candidate-discovery orchestration is additionally persisted in `copy_control_center_jobs`; downloaded public node-data cache objects are ignored by Git with the rest of `artifacts/`.
# D.5 scientific Control Center

The Control Center now leads with the Beelzebub scientific ecosystem. Its left rail includes **Automated Science**, Ecosystem, Data Soil, Wallet Sensors, Hypothesis Lab, Indicator Forge, Experiments, Confidence Engine, Execution + Risk, Watchers + Alerts, and Graveyard, while the prior paper-copy operational pages remain available for compatibility.

All scientific views are read models from SQLite. Empty or unavailable sources render `No evidence`, `Unavailable`, or `Not configured`; they do not fabricate P&L, health, candidates, alerts, or confidence. The **Automated Science** workspace shows durable queue states, worker stage health, watermarks, discoveries, model calibration, drift, and E: free space. Its Pause/Resume action changes only the persisted scientific-worker control row; start `science run` separately to operate the worker. The ecosystem map is clickable and shows module health plus the persistent scientific cycle. The right rail reports actual scientific storage status and explicitly states the simulation/shadow-only authority boundary.

The UI has no endpoint to arm live execution, submit/cancel venue orders, or move capital. D.4 read-only shadow refresh remains an operator-context read only.
