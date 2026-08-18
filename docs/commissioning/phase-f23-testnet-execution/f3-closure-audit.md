# Phase F.3 Closure Audit

Date: 2026-08-18

Result: **implementation complete — real testnet commissioning blocked by
external credential/account prerequisite**.

## Implementation evidence

- Official `hyperliquid-python-sdk==0.24.0` signing path pinned in both direct
  and compiled dependencies.
- Exact testnet-only host validation and runtime host-binding checks.
- Runtime-only API-wallet secret provider; trading-account/API-wallet identity
  separation.
- Deterministic 128-bit CLOID derived from Phase D durable client identity.
- CLOID submission lookup and cancellation.
- Unknown-means-reconcile handling for submission and cancellation.
- Explicit bounded IOC policy; no SDK default-slippage market helper.
- `expiresAfter`, serialized signing, rate-limit health latch, and venue-level
  dead-man renewal/clear support.
- Strict order, fill, open-order, position, balance, and metadata normalization.
- Startup reconciliation and foreign/manual activity detection.

## Automated evidence

The F.3 fake-SDK suite contains 23 tests covering credential refusal and
redaction; account/signer separation; exact-host and mutation barriers;
deterministic CLOID; startup gating; IOC/slippage/expiry/reduce-only arguments;
definitive rejection; ambiguous write recovery; malformed response handling;
partial/duplicate fill normalization; CLOID cancellation and cancel/fill race;
precision/minimum/reduce-only bounds; trading-account reads; unknown state and
account mismatch refusal; foreign orders; rate-limit latching; deterministic
dead-man behavior; startup manual-state mismatch; SQLite secret non-persistence;
restart deduplication; and a complete fake testnet entry/exit/verified-flat
lifecycle.

The final verification results were:

- F.2/F.3 plus Phase D/runtime focused regression: 85 tests passed.
- Phase E regression: 105 tests passed.
- permanent D.5 simulator-only guardrail plus F.3: 27 tests passed.
- full backend discovery: 382 tests passed.
- Python compile check: passed.
- dependency consistency (`pip check`): no broken requirements.
- representative Phase D SQLite `PRAGMA quick_check`: `ok`.
- committed-source secret patterns: zero candidates; zero non-empty
  Hyperliquid secret assignments.

Frozen F.0/F.1 and E.5/E.6 paths have no diff from the phase starting commit.

## External commissioning prerequisite

The runtime environment did not contain any of:

```text
HYPERLIQUID_TESTNET_ACCOUNT_ADDRESS
HYPERLIQUID_TESTNET_ACCOUNT_KIND
HYPERLIQUID_TESTNET_API_WALLET_PRIVATE_KEY
```

No external write was attempted. Real testnet order count is zero. Mainnet
order count is zero. This audit does not claim F.3 commissioned.

To commission later, an operator must supply a funded Hyperliquid testnet
trading account address and a distinct authorized API/agent-wallet secret,
run startup reconciliation, submit an exact F.1 descendant through F.2 and
Phase D, reconcile its fill, submit the verified reduce-only exit, and prove
`VERIFIED_FLAT`. Runtime credentials must then be removed and a secret scan
repeated.
