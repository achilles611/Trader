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

Then start the integrated local application:

```powershell
.venv\Scripts\python.exe main.py copy-control-center
```

Open [http://127.0.0.1:8090](http://127.0.0.1:8090).  The frontend build is served by FastAPI. Use `--port 8091` if the default port is occupied.

For frontend development, run this in a second terminal:

```powershell
cd control-center-ui
npm run dev
```

The development server proxies `/api` and `/ws` to the FastAPI process at port 8090.

## Operator controls

- **Pause Paper Entries** persists `ENTRIES_PAUSED`: it blocks only new `OPEN`/`ADD` paper signals. Existing `REDUCE`, `CLOSE`, and flip-close handling continues.
- **Resume Paper Entries** persists `RUNNING`.
- **Close All Paper Positions** uses the existing paper engine and only closes sleeves with a fresh persisted market reference. It does not change trader statuses.
- **Exit + Pause Paper Trading** flattens fresh-mark paper sleeves, then persists `PAUSED` so new entries remain disabled through restart.
- Muted wallets receive no new paper entries but retain normal exit handling.

The interface deliberately contains no private-key fields, exchange API-secret fields, order submission routes, live-mode enablement, signing, or live execution adapters.

## API and WebSocket

REST is used for research tables and details; `/ws` publishes incremental `control_state`, `portfolio_update`, `position_update`, and `watcher_health` events. Candidate searches, sorting, and filters are server-side and paginated.

All Phase C state is persisted in the existing SQLite database via the additive `copy_control_center_state` and `copy_control_center_activity` tables.
