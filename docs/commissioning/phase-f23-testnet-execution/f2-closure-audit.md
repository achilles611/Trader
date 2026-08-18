# Phase F.2 Closure Audit

Date: 2026-08-18

Result: **commissioned** for `LANE_II_SIMULATOR`.

## Gate A evidence

The staged verification completed as follows:

1. F.2 focused suite: 10 tests passed.
2. Frozen F.0/F.1 plus Phase D suites: 74 tests passed.
3. End-to-end simulator evidence covered admission, Phase D risk approval and
   refusal, durable preparation, fill ledger, position reconciliation,
   verified flatten, and restart recovery.

Focused adversarial coverage includes exact-version admission; changed,
expired, corrupt, foreign, and malformed refusal; notional reduction and
round-down; stale market/metadata blocking; duplicate delivery; restart after
admission and after ambiguous submission; partial and duplicate fills;
unknown-means-reconcile; manual position and foreign order detection; and
reduce-only verified flattening.

## Freeze evidence

- `src/lane_ii/boundary.py`: unchanged.
- `src/lane_ii/trader_v0.py`: unchanged.
- F.0 commissioning artifacts: unchanged.
- F.1 commissioning artifacts and exact artifact hash: unchanged.
- E.5/E.6 implementation and commissioning artifacts: unchanged.
- Phase D ambiguity, durable identity, fill deduplication, reconciliation, and
  verified-position reduction semantics remain active; their regression suite
  passed.

## Exact frozen identity

```text
strategy identity:
trader-strategy-f86f9ddcdbecd20bde686ee413e5cc66

strategy artifact hash:
ec61fd3a2a71d6b6e6356f3fe9f89f0060c433432790213990dd3abcb15156c4
```

## Closure statement

F.2 Lane II → Phase D bridge commissioned — exact Trader V0 trade intents
may enter the Phase D execution lifecycle while Lane II execution authority
remains false.

