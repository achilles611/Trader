# Closure audit

## Status: L3-F3 FROZEN

The 2026-08-23 closure pass established one production owner for the NinjaTrader observation socket: the Control Center FastAPI lifespan creates one `NinjaTraderListenerWorker`, which owns one `NinjaTraderCommissioningHarness` and one `LoopbackNinjaTraderBridge`. The obsolete standalone `main.py ninjatrader-observe` bind path was retired. Routes, page loads, websocket connections, watcher replacement, imports, and tests do not create a production listener.

Normal BeezConsole startup now selects the checkout's Python 3.12 environment. The rebuilt local executable launched `main.py copy-control-center --with-watcher`; one Python 3.12 PID owned both the HTTP server and exactly one `127.0.0.1:48135` listener and emitted `NINJATRADER_OBSERVER LISTENING 127.0.0.1:48135`.

Adversarial lifecycle coverage and live process checks establish:

- a duplicate worker start is idempotent and a duplicate FastAPI lifespan is refused before a second worker is created;
- a real Windows port collision logs WinError 10048, aborts application startup with exit code 3, opens no HTTP port, leaves the existing owner untouched, and never selects another port;
- malformed/incomplete frames and abrupt client resets remain local to the client connection;
- NinjaTrader-side disconnect/reconnect changes transport health without creating another listener;
- repeated HTTP refreshes and websocket reconnects leave `start_attempts=1` and one listening PID;
- idle disconnected websocket tasks are reaped, allowing a single normal Ctrl+C to reach FastAPI lifespan shutdown;
- normal shutdown joins the worker, leaves no observer thread/process, and releases `48135`; and
- a fresh backend process reclaims the exact port and again reports one start attempt.

## Authentic commissioning evidence

The prior authentic 2026-08-20 evidence remains valid because this pass did not change wire admission, account isolation, market normalization, or frozen Lane III semantics:

- L1/account capture: 27,492 accepted, 0 rejected; 21,739 quotes, 4,224 trades, 1,523 account records, 2 connection records, and 4 explicit snapshots.
- L2 capture: 51,464 accepted, 0 rejected; 46,214 aggregated depth frames, 3,433 quotes, and 869 trades, plus connection/account snapshot records.
- Native instrument: `MNQ SEP26`.
- Account isolation: `Lucid25kflex01` is the provider-evaluation alias and `Sim101` is separately local simulation.
- The installed AddOn and indicator hashes still match the repository sources on 2026-08-23.

A fresh 2026-08-23 provider attempt was **BLOCKED/UNVERIFIED**: NinjaTrader started but remained at its `Welcome` window, produced only a session-break log line, and never connected to `48135`. No credentials were automated and no fresh quote/trade/depth count is claimed. This limitation does not invalidate the prior authentic captures or the newly commissioned listener lifecycle.

## Integrity and regression

- Focused Lane III A-F, listener, launcher, and Control Center result: 190 passed and 7 subtests passed.
- Full repository result: 562 passed and 71 subtests passed in 387.06 seconds.
- Frozen `src/lane_iii` semantic diff from `c7d8c9f` through this closure: empty.
- No order submission, modification, cancellation, flatten, account mutation, position mutation, strategy selection, signal generation, or live-capital authority was introduced.

Unknown Lucid news rules, drawdown behavior, mandatory flatten/reopen timing, separately authenticated contract metadata, authentic execution callbacks, and downstream live shadow operation are facts for future decision/execution readiness. They are not material to correctness of this observation-only listener and do not block the `l3f3` freeze.

The detailed evidence matrix is in [freeze-closure-2026-08-23.md](freeze-closure-2026-08-23.md).
