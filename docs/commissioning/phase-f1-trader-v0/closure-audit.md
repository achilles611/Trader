# Phase F.1 Trader V0 closure audit

## Freeze anchors

- F.0 boundary source is unchanged.
- F.0 manifest hash: `f2c4df16e2815278c500ff406090886b2a32eb53c0751a2c8e4086d7a381ee41`.
- F.1 is additive in `src/lane_ii/trader_v0.py`; it does not alter the F.0 boundary.
- No Phase E or execution-transport import occurs on an F.1 path.
- F.1 has no scientific outcome read or E.6 acquisition seam.
- F.1 has no persistence database; consequently no production database was opened or mutated.

## Authority accounting

```text
F.1 strategy identity:
trader-strategy-f86f9ddcdbecd20bde686ee413e5cc66

F.1 strategy artifact hash:
ec61fd3a2a71d6b6e6356f3fe9f89f0060c433432790213990dd3abcb15156c4

Lane II scientific authority: false
Lane II prediction authority: false
Trader V0 signal/trade-intent authority: true — exact registered V0 version only
Lane II execution authority: false
Lane II trading authority: false
Lane II live-capital authority: false
Phase D execution sovereignty: true
E.5/E.6 modified: no
Orders placed: 0
Testnet orders: 0
Mainnet orders: 0
```

## Verification record

The F.1 test suite covers artifact and manifest anchors, registry refusal, protected-input refusal without invocation, entry and exit gates, notional capping, input-order invariance, deterministic request identities, execution refusal, Phase E immutability/counter checks, and protected-import absence. The commissioning run also executes F.0, Phase D, Phase E, operational decision, and full backend regressions.
