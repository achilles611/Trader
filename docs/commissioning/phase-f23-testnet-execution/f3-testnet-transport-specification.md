# Phase F.3 Hyperliquid Testnet Transport Specification

Status: implementation complete; real testnet commissioning requires an
external funded account and API-wallet credential.

## Structural boundary

`HyperliquidTestnetExecutionAdapter` implements the existing Phase D adapter
surface and accepts only normalized `SubmissionRequest` values. It imports no
Trader V0, Phase E, scientific, indicator, confidence, or strategy code.

The frozen `ExecutionEngine` remains simulator-only. F.3 adds the separate
`HyperliquidTestnetExecutionEngine`, which inherits the proven durable Phase D
lifecycle but admits only the exact testnet adapter mode and a matching
explicit account scope. This preserves the permanent D.5 constructor
guardrail.

The only supported mode and host are:

```text
mode: HYPERLIQUID_TESTNET
host: https://api.hyperliquid-testnet.xyz
```

Configuration containing the mainnet host, an unknown host, a path suffix,
or a trailing-slash variant is rejected. Host binding is rechecked before
every read or write, so mutation of a frozen configuration object cannot
retarget an existing signer. There is no mainnet mode or mainnet client
factory in the adapter.

## Official signing and credentials

All action signing, field ordering, number encoding, and nonce generation are
delegated to `hyperliquid-python-sdk==0.24.0`. Signed writes share one lock and
one SDK `Exchange` instance so one API wallet cannot create independent nonce
streams.

The trading account address and account kind (`MASTER` or `SUBACCOUNT`) are
explicit configuration. All state reads use that trading account address. A
runtime API-wallet private key is obtained from an injected secret provider;
it is never retained as an adapter field, written to SQLite, added to evidence,
logged by this code, returned, or included in `repr`. The derived API-wallet
address must differ from the configured trading account address.

The production environment secret name is:

```text
HYPERLIQUID_TESTNET_API_WALLET_PRIVATE_KEY
```

Missing or invalid secret material refuses adapter construction. There is no
fallback key and no master-wallet signing fallback.

## Submission identity and ambiguity

The Hyperliquid CLOID is:

```text
0x + SHA256(canonical JSON(
  schema,
  execution_domain,
  execution_account_id,
  Phase D client_order_id
))[0:16 bytes]
```

It is stable across restart and always reproducible from the durable Phase D
identity. Submission uses this CLOID; order recovery calls `orderStatus` by
CLOID; cancellation uses `cancelByCloid`.

Any exception after entering an SDK write, a 429, a lost response, or a
malformed post-write response becomes ambiguous. Exception messages are not
persisted. Phase D transitions to `SUBMISSION_UNKNOWN` and queries CLOID,
fills, and positions. It never submits that durable identity again. Only an
explicit received venue error is a definitive rejection.

## Order policy and preflight

The adapter does not use the SDK's default 5% market-order helper. It creates
an IOC limit order using a finite configured maximum slippage, current
`metaAndAssetCtxs` mid/mark evidence, and the official SDK price precision
rule. `expiresAfter` is set on each supported exchange action.

Before transmission the adapter verifies:

- symbol is present in fresh authoritative perp metadata;
- quantity is positive, finite, aligned to `szDecimals`, and at least the
  smallest representable size;
- mark-price notional satisfies the configured Hyperliquid venue minimum,
  which may not be set below the documented $10 minimum;
- `reduce_only` exactly matches the Phase D exposure effect;
- reduce-only side and quantity are bounded by a fresh position read for the
  configured trading account.

Pre-transmission failures produce deterministic local rejection evidence and
never call the SDK write.

## Reconciliation and reads

The full surface is implemented:

```text
submit
cancel
get_order
list_fills
list_open_orders
get_positions
get_balances
get_instrument_metadata
```

Responses are strictly normalized into Phase D contracts. Unknown order
states, malformed numeric/timestamp evidence, account mismatch, CLOID
mismatch, metadata misalignment, and missing fill identity fail closed. Raw
evidence is a bounded whitelist and never contains arbitrary SDK payloads.
Foreign orders receive a non-authoritative local observation identity and are
marked as external/manual activity.

## Startup, health, and dead-man safety

Network entry exposure is latched off at adapter construction. Phase D
`startup_reconcile()` first repairs active durable intents by CLOID, then
reconciles positions and open orders. Only matched/verified-flat position truth
and a clear open-order observation make entries eligible. Reductions remain
available when independently verified.

Rate-limit or uncertain write health latches new entries off for a bounded
cooldown; no retry loop exists. Reads and safe cancellation/reconciliation
retain priority.

Dead-man support uses venue-level `scheduleCancel`. Renewal schedules one
future cancel-all horizon and is a deterministic no-op until the renewal
interval elapses. Clearing explicitly calls `scheduleCancel(None)`. It is not
used as a per-order timer and does not claim to close open positions.
