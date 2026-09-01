# BeezConsole Slim Mode

Slim Mode is the compact, paper-only Lane III-G operating surface. Select
**Slim Console** in Full Console to enter it; select **Full Console** in the
Slim header to return. The choice is stored locally and `?console=slim` or
`?console=full` overrides that saved preference for the current visit.

Slim Mode shows only the paper-session P&L, local ledger verification, the
guarded paper start/stop position, and a three-light readiness display. Full
Console retains rehearsal evidence, ledger diagnostics, scheduling, and all
advanced controls.

## Lights

- **Red — NOT READY:** a canonical unsafe, stale, failed, disconnected, or
  unknown runtime fact exists. Start is disabled.
- **Yellow — PREPARING:** the canonical runtime is in a recognized connection,
  reconciliation, warmup, verification, start, or stop transition. Start is
  disabled.
- **Green — READY TO START PAPER TRADING:** the authority-free production
  rehearsal has passed every current Sim101 paper gate. Green does not reserve
  authority: the server repeats its complete preflight when Start is pressed.
- **Green — PAPER TRADING ACTIVE:** the backend-owned continuous Sim101 paper
  session is running. It remains active while flat and between trades as well
  as while a one-MNQ position is protected. If any active health fact becomes
  uncertain, Slim Mode returns to yellow or red, but retains the stop control.

The browser renders the backend Slim-status projection; it does not calculate a
second readiness algorithm. **Start Paper Trading** invokes
`POST /api/lane-iii/paper/operational-start`: it reruns the full Sim101
pre-start proof, records a continuous operational-paper session, and does not
reserve commissioning ownership or submit a commissioning entry. A browser
refresh or a switch to Full Console only changes presentation; it cannot stop
that backend session. The latest successful ledger proof must be a **Full**
verification (including its structural quick check); an inherited incremental
proof is intentionally insufficient for a new Slim paper session.

**STOP TRADING** is always shown whenever the canonical or last-known runtime
might still have paper authority, including red/yellow and unavailable Slim
status. It invokes the idempotent guarded flatten-and-disarm path, cancels
owned pending entries, flattens an open position when required, and releases
the operational session only after a clean Sim101 reconciliation proves flat
with no owned orders. The next start requires a fresh verifier proof. Neither
control can reach live capital, and neither view overrides provenance,
verification, reconciliation, observer, account-class, quantity, protection,
or session gates.

## Combined-server API contract

Slim Mode requests the same-origin URL
`/api/lane-iii/paper/slim-status`. The combined BeezConsole server registers
that API route before its frontend fallback. Any unmatched `GET /api/*` route
returns structured `404 application/json` (`API_ENDPOINT_NOT_FOUND`), never
the SPA `index.html`. The browser validates both HTTP status and JSON content
type before parsing; an HTML response is shown as the concise, fail-closed
operator message `Status unavailable — backend endpoint returned HTML`.
