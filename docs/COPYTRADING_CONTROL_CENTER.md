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

Then start the local application with its single paper watcher lifecycle:

```powershell
.venv\Scripts\python.exe main.py copy-control-center --with-watcher
```

Open [http://127.0.0.1:8090](http://127.0.0.1:8090).  The frontend build is served by FastAPI. Use `--port 8091` if the default port is occupied.

Without `--with-watcher`, the Control Center is read/control-only and reports `NOT_ATTACHED` watcher health. The attached watcher subscribes only to Active paper-entry targets plus non-active wallets that still have open paper sleeves for exit handling; it never subscribes every shadow candidate.

For frontend development, run this in a second terminal:

```powershell
cd control-center-ui
npm run dev
```

The development server proxies `/api` and `/ws` to the FastAPI process at port 8090.

## Operator controls

- **Pause Paper Entries** persists `ENTRIES_PAUSED`: it blocks only new `OPEN`/`ADD` paper signals. Existing `REDUCE`, `CLOSE`, and flip-close handling continues.
- **Resume Paper Entries** persists `RUNNING`.
- **Close All Paper Positions** uses the existing paper engine and only closes sleeves with a fresh persisted market reference. If any sleeve is skipped, the response is `partial` and entries remain paused until an operator explicitly resumes them.
- **Exit + Pause Paper Trading** flattens fresh-mark paper sleeves, then persists `PAUSED` so new entries remain disabled through restart.
- Only **Active** wallets may open or add paper sleeves. Shadow, approved, muted, and rejected wallets cannot bypass this gate; every wallet still retains normal exit handling for an open sleeve.

The interface deliberately contains no private-key fields, exchange API-secret fields, order submission routes, live-mode enablement, signing, or live execution adapters.

## API and WebSocket

REST is used for research tables and details; `/ws` publishes incremental `control_state`, `portfolio_update`, `position_update`, `watcher_health`, and activity events. Candidate searches, sorting, filters, and paging are server-side and paginated.

Phase C reads B.2 through a read-only normalization boundary. Candidate score, eligibility, ranking, and activation all use only the canonical `provenance='phase_b'` score tied to the candidate analysis `last_run_id`; legacy scores can be displayed as compatibility context but never influence paper-entry decisions.

All Phase C state is persisted in the existing SQLite database via the additive `copy_control_center_state` and `copy_control_center_activity` tables.
