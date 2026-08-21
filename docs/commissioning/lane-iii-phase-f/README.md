# Lane III Phase F (`l3f`) — Tradovate observation boundary

`l3f` adds a concrete, **read-only** Lucid Trading / Tradovate integration boundary. It is downstream of frozen `l3b → l3c → l3d` and may feed frozen `l3e` only as simulation input.

Status on 2026-08-20: `l3f3` authenticated the NinjaTrader Desktop observation path. The loopback receiver accepted 27,492 native observations with no rejections: connection, account, quote, trade, and explicit position/order snapshot records. The evaluation account is bound only to the persistent alias `Lucid25kflex01`; Sim101 is separately classified as local simulation. The authoritative snapshot confirmed the Lucid alias is flat with no working orders.

The Level 2 re-commissioning capture subsequently accepted 46,214 genuine `DEPTH` observations from NinjaTrader alongside quotes and trades. It exposes aggregated price-level L2 snapshots, not MBO/order-by-order depth. This is still deliberately **not a freeze**: provider reconnect, authenticated contract metadata beyond the native name, and the downstream live shadow path remain incomplete. No real order, modification, cancellation, flatten operation, or capital touch occurred.

Fresh l3f3 commissioning on 2026-08-21 listened for 60 seconds on `127.0.0.1:48135` and accepted zero frames. The sanitized result is [receiver-observation-2026-08-21.json](receiver-observation-2026-08-21.json). It proves the new receiver was available but does not prove a currently connected NinjaTrader/Lucid source, so it leaves the runtime freeze gate blocked.

## Beelzebub-owned receiver command

The canonical receiver lifecycle is owned by the primary Beelzebub entry point:

```powershell
.\.venv312\Scripts\python.exe main.py ninjatrader-observe --duration-seconds 60 --report-file docs\commissioning\lane-iii-phase-f\receiver-observation-YYYY-MM-DD.json
```

The command binds exactly `127.0.0.1:48135`, emits a sanitized `LISTENING` status event to stderr before its receive loop, accepts only NinjaTrader-to-Beelzebub frames, writes a sanitized aggregate report naming Beelzebub as listener owner, and closes its listener after the bounded duration. It reads no stdin and requires no PowerShell setup, socket preparation, or broker-side action to start or stop. A zero-observation completion exits with status `3`; that is a commissioning failure, not an indication that the listener failed to bind.

The fresh `l3f3.2` run used that exact command for 60 seconds. It reached `LISTENING` and saved [receiver-observation-2026-08-21-l3f3.2.json](receiver-observation-2026-08-21-l3f3.2.json), but received and rejected zero frames. The installed AddOn and market-observer sources match this repository; the latest NinjaTrader trace instead shows the AddOn was finalized on 2026-08-20, with no new bridge activity. Reload/activate the current AddOn and the `MNQ SEP26` market observer in NinjaTrader, then rerun the same Beelzebub command. This capture is lifecycle evidence only and does not satisfy the authentic observation, reconnect, reconciliation, or shadow gates.

## GUI/runtime listener ownership

The normal BeezConsole launch starts `main.py copy-control-center --with-watcher`. Its application-level owner is the FastAPI lifespan in `src/copytrade/control_center.py`, not a route, page, websocket, or frontend component. At application startup it starts one `NinjaTraderListenerWorker`, which runs the same `NinjaTraderCommissioningHarness` receive loop as the bounded commissioning command. It logs `NINJATRADER_OBSERVER LISTENING 127.0.0.1:48135`; `/api/health` and `/api/system` expose its sanitized `OBSERVE_ONLY` state.

At application shutdown the lifespan stops and joins that worker, closing its listener so a fresh GUI runtime can reclaim `127.0.0.1:48135`. A second in-process start request reuses the already-listening worker. On Windows the receiver uses exclusive socket ownership, so a port collision reports `NINJATRADER_OBSERVER FAILED` with the actual bind error; it does not choose a different port or disable the GUI.

The implementation is [tradovate_observation.py](../../../src/l3f_provider/tradovate_observation.py). It sits outside the frozen Lane III package and contains a named read-only REST transport, a read-only WebSocket subscription client, strict L3-B normalization, account/position/order observation, reconciliation, health tracking, secret redaction, and future-only compliance diagnostics. It contains no real order authority.

See [closure audit](closure-audit.md) for the current hard-gate result.
