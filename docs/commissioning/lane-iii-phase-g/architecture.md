# Lane III-G experimental paper architecture

One read-only listener on `127.0.0.1:48135` admits the existing NinjaTrader
observations. An ordered `ObservationFanout` delivers each admitted observation
independently to the frozen scientific shadow and to `LaneIIIPaperRuntime`.
Failures in either sink are recorded without coupling the other sink's
authority.

The experimental paper consumer labels every inference with
`LOCAL_CALLBACK_ORDER_ONLY`, `UNVERIFIED`, and `scientific_eligibility=false`.
Any local gap, observer session boundary, disconnect, reset, malformed record,
stale event, or contract mismatch clears its provisional state.

Directional output crosses four separate contracts:

```text
PaperDecision -> PaperExecutionIntent -> PaperRiskGrant -> PaperExecutionCommand
```

All four are committed to a hash-chained SQLite ledger before the signed
command is sent. The separate listener on `127.0.0.1:48136` admits one local
NinjaTrader execution client and uses HMAC-SHA256, session identity, command
TTL, monotonic sequence, idempotence, exact authority hashes, and mandatory
reconciliation.

The audit database path may be relocated with
`BEELZEBUB_L3G_PAPER_LEDGER`; this setting changes storage location only and
cannot change account, instrument, quantity, mode, or execution authority.
Evidence and `NO_TRADE` decisions enter one ordered, non-dropping writer and
commit in bounded WAL batches at `synchronous=NORMAL` so authentic depth cannot
outrun persistence. Any decision eligible to cause a paper side effect first
flushes that queue and then commits synchronously before its intent, grant,
command, or socket side effect. Directional decisions observed while disarmed
remain non-authoritative and safely batchable.
All operational and safety domains—including intents, risk grants, commands,
receipts, orders, executions, positions, incidents, and sessions—switch to
`synchronous=FULL` before their committed transaction.

Normal Control Center startup also owns a bounded Windows-only desktop-login
bootstrap. The observation and execution listeners bind first; the bootstrap
then starts NinjaTrader when absent, identifies only the exact login UI through
Windows UI Automation, decrypts a Windows-user-local DPAPI secret in the helper
process, waits for the exact Control Center, and verifies the configured
`LucidFlex25k` connection from its Accounts grid. It attempts login at most
twice, waits at least 15 seconds between attempts, and stops after 90 seconds.
Missing credentials, ambiguous UI, invalid credentials, MFA, process exit, or
timeout leave paper execution disarmed. The bootstrap contains no paper command
type and the arm route refuses requests until bootstrap state is
`AUTHENTICATED`.

The local credential files are outside the repository under the current
Windows user's NinjaTrader Documents directory. The password file contains
only DPAPI ciphertext created by `tools/seed_ninjatrader_login.ps1`; credential
values never enter configuration, command arguments, environment variables,
logs, health responses, or audit records.

`BeelzebubPaperExecutionAddOn` independently compiles the same hard boundary:
exact `Sim101`, exact `MNQ SEP26`, one contract, closed actions, owned-order
names, a 25-point stop market after entry fill, and an independent heartbeat
watchdog. It never resolves an account from a chart, SuperDOM, environment
override, display-name match, or fallback.

No live-capital adapter, registration, factory, parser, route, or UI toggle is
present.
