# Phase F.1 — Trader V0 specification

## Scope

F.1 commissions exactly one immutable Lane II operational strategy:

```text
strategy_id: trader-v0
strategy_version: 1
strategy_identity: trader-strategy-f86f9ddcdbecd20bde686ee413e5cc66
strategy_artifact_hash: ec61fd3a2a71d6b6e6356f3fe9f89f0060c433432790213990dd3abcb15156c4
```

The artifact is [trader-v0-v1.json](trader-v0-v1.json). Its canonical SHA-256 hash covers every material strategy semantic: input classes, decision gates, hysteresis, expiration, exits, sizing ceiling, scope, and authority basis.

Its authority basis is solely `TRADER_V0_OPERATIONAL_SIMULATION_SHADOW_COMMISSIONING`. This is an engineering/shadow commissioning, not a scientific conclusion or an edge claim, and it has no Phase E dependency.

## Authority

F.1 is a successor authority manifest anchored to the unchanged F.0 manifest:

```text
F.0 manifest hash: f2c4df16e2815278c500ff406090886b2a32eb53c0751a2c8e4086d7a381ee41
F.1 manifest hash: 985844cf6ebd96499a3803a6955567d8f06e1ccf53a9e7282f6738d15e9bab48
```

Only `TRADER_LANE / SIGNAL` is granted, only to the exact artifact above, and only to create a bounded simulation/shadow `TradeIntentRequest`. Scientific evaluation, prediction, execution, trading, and live-capital authority remain denied. The frozen F.0 Phase D execution sovereignty remains unchanged.

## Inputs and entry

The artifact permits only `LIVE_PUBLIC_WALLET_ACTIVITY`, `LIVE_PUBLIC_MARKET_DATA`, `OPERATIONAL_INDICATOR`, `CONFIGURATION_OR_RISK_POLICY`, and `CURRENT_ACCOUNT_OR_EXECUTION_STATE`. Every decision receives immutable `OperationalInput` references and their provenance hashes. Entry needs the first four classes exactly once; unfamiliar, undeclared, missing, duplicate, or unprovenanced input fails closed.

A wallet action is evidence, never authority. Source-wallet leverage is excluded from sizing. An entry is `LONG` or `SHORT` only with an explicit direction, a wallet action and market evidence each no more than 10 seconds old, nonempty indicators, finite inputs, positive alpha survival, valid regime, effective confidence at least 0.60, and positive operational net edge. Net edge is gross edge minus fees, spread, slippage, market impact, and latency; each component must be finite.

The request uses the authoritative evaluation time as `created_at`, expires exactly 10 seconds later, and requests no more than `min(input ceiling, 1000.0)`. It is a ceiling request only, never final sizing authority.

## Position safety and non-execution

For an open position, F.1 returns `EXIT` on hard risk, age at least 600 seconds, non-positive net edge, effective confidence below 0.52, regime invalidation, or provenance/input integrity failure. It returns `SKIP` when the position remains within those limits; the 0.52–0.60 band never opens new exposure.

F.1 exposes immutable `LONG`, `SHORT`, `SKIP`, and `EXIT` records. Reason ordering, authority decisions, provenance ordering, decisions, and entry request IDs replay deterministically for identical normalized inputs.

F.1 does not import Phase D or Phase E and has no execution, venue, credential, signing, order, cancellation, reconciliation, or capital operation. Its F.0 `TradeIntentRequest` permanently has `execution_authority == False` and `live_capital_authority == False`.
