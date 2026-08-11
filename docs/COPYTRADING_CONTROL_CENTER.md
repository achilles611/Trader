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

- **Quick Scan**: roughly one hour of recent source objects, up to 1,000 candidates.
- **Standard Scan**: roughly six hours, up to 2,500 candidates.
- **Deep Scan**: roughly 24 hours, up to 5,000 candidates.

All three presets require at least two observed events and preserve Phase A's 30-day recency rule. A durable Phase C orchestration job reports source resolution/acquisition, parsing, and frozen Phase A completion via `/ws` as `discovery_job_update`. Its result carries the Phase A discovery run ID and safe source provenance. Rediscovery preserves existing operator states and does not create Active targets, paper positions, or watcher membership. Phase B analysis remains a deliberately manual next step. The cache uses atomic `.partial` downloads, reuses validated objects, and prunes to a 5 GiB bounded local cache; cache objects are never committed to Git.

## Operator controls

- **Pause Paper Entries** persists `ENTRIES_PAUSED`: it blocks only new `OPEN`/`ADD` paper signals. Existing `REDUCE`, `CLOSE`, and flip-close handling continues.
- **Resume Paper Entries** persists `RUNNING`.
- **Close All Paper Positions** uses the existing paper engine and only closes sleeves with a fresh persisted market reference. Completion is verified from the persisted open-paper-position set after every attempt. If any position remains—whether skipped or failed—the response is `partial`, includes `attempted`, `closed`, `failed`, and `remaining_open_positions`, records a warning, and leaves new entries paused until an operator explicitly resumes them.
- **Exit + Pause Paper Trading** flattens fresh-mark paper sleeves, then persists `PAUSED` so new entries remain disabled through restart.
- Only **Active** wallets may open or add paper sleeves. Shadow, approved, muted, and rejected wallets cannot bypass this gate; every wallet still retains normal exit handling for an open sleeve.

The interface deliberately contains no private-key fields, exchange API-secret fields, order submission routes, live-mode enablement, signing, or live execution adapters.

## API and WebSocket

REST is used for research tables and details; `/ws` publishes incremental `control_state`, `portfolio_update`, `position_update`, `watcher_health`, and activity events. Candidate searches, sorting, filters, and paging are server-side and paginated.

Phase C reads B.2 through a read-only normalization boundary. Candidate score, eligibility, ranking, and activation all use only the canonical `provenance='phase_b'` score tied to the candidate analysis `last_run_id`; legacy scores can be displayed as compatibility context but never influence paper-entry decisions. The dossier renders the canonical `score.total`, `score.eligible`, `score.components`, `score.penalties`, and `score.reasons` fields, with hard-gate failures shown separately. Activation also requires the parent Phase B run to be `completed` or `completed_with_errors`.

All Phase C state is persisted in the existing SQLite database via the additive `copy_control_center_state` and `copy_control_center_activity` tables.
Candidate-discovery orchestration is additionally persisted in `copy_control_center_jobs`; downloaded public node-data cache objects are ignored by Git with the rest of `artifacts/`.
