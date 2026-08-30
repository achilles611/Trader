# L3H authority matrix

| Component | May observe | May persist | May dispatch | Authority limit |
| --- | --- | --- | --- | --- |
| L3G paper AddOn | Sim101 callbacks | L3G paper ledger | Sim101 paper only | unchanged paper boundary |
| L3H runtime | capability and broker facts | L3H event store | only through injected gateway | one MNQ canary |
| Default L3H gateway | none | no | no | always `LIVE_GATEWAY_NOT_CONFIGURED` |
| L3H Live AddOn | local capability/protocol | local diagnostics | no unless independently armed | fail closed |
| Operator dashboard | sanitized status | idempotency request | no direct broker path | one held start request |

`UNKNOWN` is never mapped to `FLAT`. `PROVIDER_EVALUATION` may produce
`PROVIDER_EVALUATION_READY_DISARMED`, never `LIVE_READY_DISARMED`. A local
capability binds an account by hash; neither its full identity nor key belongs
in source control or browser storage.
