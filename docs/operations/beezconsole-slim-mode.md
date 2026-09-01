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
- **Green — PAPER TRADING ACTIVE:** the backend has a reconciled, protected,
  healthy one-MNQ Sim101 paper position. If any active health fact becomes
  uncertain, Slim Mode returns to yellow or red.

The browser renders the backend Slim-status projection; it does not calculate a
second readiness algorithm. **Start Paper Trading** uses the same atomic
commissioning-start endpoint as Full Console, while **Stop & Disarm** uses the
existing guarded flatten-and-disarm endpoint. Neither control can reach live
capital, and neither view overrides provenance, verification, reconciliation,
observer, account-class, quantity, protection, or session gates.

## Combined-server API contract

Slim Mode requests the same-origin URL
`/api/lane-iii/paper/slim-status`. The combined BeezConsole server registers
that API route before its frontend fallback. Any unmatched `GET /api/*` route
returns structured `404 application/json` (`API_ENDPOINT_NOT_FOUND`), never
the SPA `index.html`. The browser validates both HTTP status and JSON content
type before parsing; an HTML response is shown as the concise, fail-closed
operator message `Status unavailable — backend endpoint returned HTML`.
