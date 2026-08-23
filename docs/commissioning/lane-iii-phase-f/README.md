# Lane III Phase F (`l3f`) — Tradovate observation boundary

`l3f` adds a concrete, **read-only** Lucid Trading / Tradovate integration boundary. It is downstream of frozen `l3b → l3c → l3d` and may feed frozen `l3e` only as simulation input.

Status: **`l3f3 FROZEN` on 2026-08-23.** The NinjaTrader Desktop observation path was authenticated on 2026-08-20. The loopback receiver accepted 27,492 native observations with no rejections: connection, account, quote, trade, and explicit position/order snapshot records. The evaluation account is bound only to the persistent alias `Lucid25kflex01`; Sim101 is separately classified as local simulation. The authoritative snapshot confirmed the Lucid alias was flat with no working orders at capture time.

The Level 2 re-commissioning capture subsequently accepted 51,464 observations with zero rejections, including 46,214 genuine `DEPTH`, 3,433 quote, and 869 trade observations. It exposes aggregated price-level L2 snapshots, not MBO/order-by-order depth. No real order, modification, cancellation, flatten operation, or capital touch occurred.

Fresh l3f3 commissioning on 2026-08-21 listened for 60 seconds on `127.0.0.1:48135` and accepted zero frames. The sanitized result is [receiver-observation-2026-08-21.json](receiver-observation-2026-08-21.json). It is retained as an honest unavailable-source capture and does not erase the prior authentic evidence.

The former bounded `main.py ninjatrader-observe` command is retired. It was useful commissioning evidence in `l3f3.2`, but retaining it after GUI ownership would leave a second production bind path. There is now no manual listener command and no operator-side PowerShell listener startup.

## GUI/runtime listener ownership

The normal BeezConsole launch starts `main.py copy-control-center --with-watcher`. Its application-level owner is the FastAPI lifespan in `src/copytrade/control_center.py`, not a route, page, websocket, or frontend component. At application startup it starts one `NinjaTraderListenerWorker`, which runs the commissioned `NinjaTraderCommissioningHarness` receive loop. It logs `NINJATRADER_OBSERVER LISTENING 127.0.0.1:48135`; `/api/health` and `/api/system` expose its sanitized `OBSERVE_ONLY` state.

At application shutdown the lifespan stops and joins that worker, closing its listener so a fresh GUI runtime can reclaim `127.0.0.1:48135`. A second in-process start request reuses the already-listening worker; a concurrent duplicate FastAPI lifespan is refused before a second worker is created. On Windows the receiver uses exclusive socket ownership. A port collision reports `NINJATRADER_OBSERVER FAILED` with the actual bind error and aborts application startup; it does not choose another port or leave a partial backend.

The NinjaTrader implementation is [ninjatrader_observation.py](../../../src/l3f_provider/ninjatrader_observation.py) plus [ninjatrader_commission.py](../../../src/l3f_provider/ninjatrader_commission.py). The retained direct-provider boundary is [tradovate_observation.py](../../../src/l3f_provider/tradovate_observation.py). All sit outside frozen Lane III and contain no real order authority.

See the [freeze closure record](freeze-closure-2026-08-23.md) and [closure audit](closure-audit.md).
