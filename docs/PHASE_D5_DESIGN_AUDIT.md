# Phase D.5 — live execution design and security audit

Baseline: `f20618928dcb2291cb3fee0c6b134871cb964b03` (D.4 closed/frozen)

Status: **D.5 design/audit initiated. No live transport implementation exists in this change.**

## Purpose

D.5 introduces the architecture required for a future Hyperliquid execution capability without weakening the frozen D.0–D.4 safety model. The first D.5 decision is intentionally structural: do **not** unlock the existing `ExecutionEngine` by allowing a second `adapter_mode`. The simulator engine remains `SIMULATOR_ONLY`. Live authority will enter through a separate coordinator, signer boundary, account authority, and authorization-session contract.

D.5 is testnet-first. Mainnet capital movement remains a Phase-E deployment/canary decision and must remain impossible throughout D.5 unless a later separately reviewed roadmap explicitly changes that boundary.

## Frozen invariants inherited from D.3.2 and D.4

D.5 must preserve these properties:

- position authority, open-order authority, and integrity evidence are independent;
- a position match cannot clear an open-order failure;
- neither position nor open-order agreement can clear unresolved integrity evidence;
- new exposure fails closed when authority is incomplete, stale, mismatched, or unavailable;
- degraded exits require fresh, authoritative, direction-and-size-bounded position evidence;
- the adapter boundary repeats volatile safety checks after `READY`;
- ambiguous submission is reconciled and never blindly retried;
- deterministic client identity is persisted before any possible external transmission;
- fills are immutable and deduplicated;
- account/domain scope never crosses between simulator, paper compatibility, shadow observation, and future live execution;
- D.4 shadow evidence remains operator context only and is never promoted into live execution authority;
- no automatic rebaseline exists;
- live configuration cannot be enabled by the existing D.0 flags.

## Audit result of the frozen baseline

The frozen D.4 baseline is a sound starting point because live copy trading is still structurally impossible:

- `CopyTradeConfig.validate()` rejects every `mode == "live"` configuration even when `COPYTRADE_LIVE_ENABLED` is true;
- `ExecutionEngine` accepts only `adapter_mode == "SIMULATOR_ONLY"`;
- `HyperliquidReadOnlyShadowAdapter` exposes observation only and is rejected by the execution engine;
- the copytrade Hyperliquid client contains public `/info` and websocket reads, not exchange signing;
- the normal runtime dependency set does not contain the Hyperliquid trading SDK.

The repository does contain a separate Coinbase-capable `src/eth_bot` subsystem and generic historical live flags. Those are explicitly **not** authority for D.5 and must never be reused as copytrade live gates or credential sources.

No current D.4 medium/high defect was found by this design audit. The findings below are **blocking D.5 design requirements**: they become safety defects only if a live adapter is implemented without resolving them.

## D.5 blocking design findings

### D5-01 — current submission contract is not sufficient for a real venue

Severity if ignored: **High**.

`SubmissionRequest` currently carries symbol, side, quantity, exposure effect, and `reduce_only`, but no durable executable price, order type/TIF, slippage ceiling, asset identifier/metadata version, or expiry. Hyperliquid orders require an asset index, limit price, size, reduce-only flag, order type/TIF, and optionally a 128-bit client order ID and `expiresAfter`.

A live adapter must not invent these economics after the risk decision. D.5 therefore needs a versioned immutable **LiveOrderPlan** (name may change) persisted before signing. It should include at minimum:

- intent/submission identity;
- live account/domain;
- network and exact exchange host;
- symbol and resolved venue asset ID;
- metadata observation/version used for precision and minimums;
- side and reduce-only flag;
- normalized quantity;
- explicit limit price;
- one initially supported TIF policy;
- slippage/deviation bound and market-evidence timestamp;
- deterministic Hyperliquid CLOID;
- bounded `expiresAfter` policy;
- authorization-session ID;
- plan version and creation timestamp.

For the first write-capable testnet milestone, support only a deliberately narrow order policy. The recommended initial policy is an IOC limit order whose price is derived from fresh market evidence and bounded slippage. Do not initially add GTC opening orders, ALO, triggers, TP/SL grouping, TWAP, modify, leverage changes, or builder fees.

### D5-02 — existing client order ID is not a Hyperliquid CLOID contract

Severity if ignored: **Medium**.

Hyperliquid CLOIDs are optional 128-bit hex values represented as `0x` plus 32 hex digits. D.5 must derive a deterministic venue CLOID from the already durable Phase-D submission identity and persist it before signing. Do not depend on an arbitrary string form of the existing generic `client_order_id` being accepted by the venue.

Cancellation should prefer `cancelByCloid` for orders created by Trader so recovery does not depend on a transient venue OID.

### D5-03 — signer nonce authority does not exist

Severity if ignored: **High**.

Hyperliquid tracks the 100 highest nonces per signer. Nonces must be unique and time-bounded, and the official guidance recommends an atomic counter per trading process/API wallet. D.5 needs a signer-scoped durable monotonic nonce allocator. A nonce must never be reused after a crash merely because no acknowledgement was persisted.

The allocator must:

- bind state to the public signer/API-wallet address and network;
- reserve monotonically under concurrency;
- fast-forward to current milliseconds only when greater than durable state;
- persist reservation before an external write;
- never decrement or reuse a reserved nonce;
- expose no secret material;
- survive restart and out-of-order completion.

No write request may be retried merely by allocating a new nonce after an ambiguous outcome. Reconciliation by deterministic CLOID comes first.

### D5-04 — credentials must not enter `CopyTradeConfig`

Severity if ignored: **High**.

`CopyTradeConfig.snapshot()` serializes the config dataclass. A private key, seed phrase, or secret-bearing signer object therefore must never become a field of `CopyTradeConfig`, YAML, SQLite provenance, Control Center payloads, or ordinary application logs.

D.5 will use a separate `LiveSignerSecretProvider` boundary. The general copytrade process may retain public account and signer addresses/fingerprints, but not private material.

Rules:

- never accept a master-wallet private key or seed phrase;
- use a separately authorized Hyperliquid API/agent wallet;
- never implement `approveAgent` inside Trader;
- never persist the API-wallet private key;
- never include it in exceptions, reprs, snapshots, test fixtures, request evidence, or UI/API responses;
- do not load it through the repository's ordinary `.env`/`CopyTradeConfig` path;
- a testnet-only development secret provider may be process-local, but Phase E mainnet requires an OS-backed/isolated secret mechanism.

### D5-05 — the official trading SDK is intentionally too capable

Severity if ignored: **High**.

The official Hyperliquid `Exchange` client exposes far more than order placement: modify, transfers, withdrawals, leverage/margin actions, API-wallet approval, vault actions, TWAP, abstraction actions, and other writes. Upper layers must never receive this generic object.

If D.5 uses the official SDK for signing correctness, isolate it behind a minimal capability whose only permitted write operations are initially:

1. place one already-approved order plan;
2. cancel a Trader-owned order by deterministic CLOID.

The wrapper must reject every other action structurally. The SDK should be an optional/pinned live dependency rather than a normal dependency imported by the general service or Control Center. At audit time the current official PyPI release is 0.24.0; the implementation step must re-verify and pin the reviewed version before use.

### D5-06 — live account reconciliation authority must be separate from D.4 shadow

Severity if ignored: **High**.

D.4 `SHADOW_REAL_VENUE` observations are explicitly non-authoritative. D.5 needs a new scoped domain, proposed:

`LIVE_REAL_VENUE / LIVE:hyperliquid:<network>:<account>`

Live account authority must independently observe and persist:

- fresh positions;
- fresh open orders;
- fills/order status relevant to deterministic CLOIDs;
- public account identity;
- instrument metadata used by the plan.

D.4 rows must not be copied or relabeled into live authority. The same public `/info` transport may be reused behind a separate authority-producing service, but live reconciliation must write the ordinary Phase-D reconciliation ledger under the live domain/account and preserve D.3.2 latches.

### D5-07 — signer identity must be bound to the configured account

Severity if ignored: **High**.

A Hyperliquid API wallet signs on behalf of a master/subaccount, while account queries use the actual account address. Startup preflight must verify the public signer address and its role/account binding before any arm session can become valid. Hyperliquid's public `userRole` info response can identify an agent and the user it belongs to.

The signer process must prove only its public signer address to the coordinator. The coordinator verifies that the configured live account, network, signer address, and returned public role all match the authorization session.

### D5-08 — legacy/global live flags are not sufficient authorization

Severity if ignored: **High**.

`COPYTRADE_MODE=live` plus `COPYTRADE_LIVE_ENABLED=true` may remain prerequisite intent signals, but they are never sufficient to create transport authority. Likewise, `LIVE_TRADING_ENABLED`, `BOT_TRADING_ENABLED`, and the Coinbase credentials used by `src/eth_bot` are unrelated and must be ignored by D.5.

D.5 requires a short-lived, explicit, account-bound **LiveAuthorizationSession** persisted without secrets. It should include:

- authorization-session ID;
- live domain/account and network;
- public signer address;
- exact reviewed config fingerprint;
- issued/accepted/expiry times;
- allowed symbols;
- maximum order notional;
- maximum session notional;
- maximum live position notional;
- optional maximum order count;
- explicit operator acceptance version/fingerprint;
- state (`DISARMED`, `ARMED`, `EXPIRED`, `REVOKED`, `CONSUMED` as appropriate);
- reason/audit evidence.

Arming must be a local operator workflow, not a Control Center HTTP button. Restart should default to no usable transport authority unless an unexpired session is deliberately designed to survive restart and passes a fresh preflight; the safer initial implementation is restart-disarmed.

### D5-09 — live routing must not replay simulator/paper signals

Severity if ignored: **High**.

`phase_d_execution_intents.signal_id` is globally unique. Preserve that property. Do not create a simulator intent and then duplicate/promote the same signal into live execution later.

When live routing eventually exists, a newly emitted Phase-C signal must be routed exactly once to its selected execution authority. If that signal already has any Phase-D intent, live acceptance fails closed. Historical simulator/paper signals are never retrospectively replayed into capital.

The existing Phase-C service is explicitly PAPER-oriented, so live routing should be introduced as a separate service/coordinator rather than silently changing `_execute_reconstructed_signal()` to call a live adapter.

### D5-10 — first D.5 arm should require a verified flat account

Severity if ignored: **Medium**.

Adopting an already non-flat live account requires a separate provenance/rebaseline policy. Do not solve that inside the first live skeleton. D.5 should initially require authoritative verified-flat live positions **and** clear open orders **and** no unresolved submissions/integrity issues before an authorization session can arm.

Existing positions or unattributed orders cause preflight failure. A later explicit non-flat adoption design can be reviewed separately.

## Proposed capability architecture

```text
Phase C CopySignal
       |
       v
LiveExecutionCoordinator (new; never the frozen simulator engine)
       |
       +-- Live authorization session gate
       +-- LIVE_REAL_VENUE account authority / reconciliation
       +-- D.3.2 integrity + open-order + position latches
       +-- fresh market and instrument metadata
       +-- per-order/session risk ceilings
       |
       v
Immutable LiveOrderPlan
       |
       v
Durable submission identity + deterministic Hyperliquid CLOID
       |
       v
Isolated signer/transport capability
       |     - API-wallet secret exists only here
       |     - durable signer-scoped nonce reservation
       |     - exact host/network pin
       |     - allowlisted order/cancel-by-CLOID actions only
       v
Hyperliquid /exchange (TESTNET during D.5)
       |
       v
Public /info reconciliation -> ordinary Phase-D live-domain ledger
```

The coordinator must not hold the private key. The signer must not decide trading economics, risk, symbol selection, size, price, or authorization limits. It receives an already-approved immutable plan and can either transmit that exact plan or refuse it.

## Multiple independent gates

A D.5 testnet write should be possible only when every gate is simultaneously true:

1. D.5 write-capable optional dependency is installed and reviewed.
2. Copytrade explicitly requests live execution; historical generic bot flags do not count.
3. Network is exactly Hyperliquid **testnet** during D.5.
4. Configured public live account matches the durable live execution account scope.
5. Isolated signer is present and reports its expected public API-wallet address.
6. Public `userRole` evidence proves that signer is an agent for the configured account.
7. A short-lived local `LiveAuthorizationSession` is ARMED, unexpired, config-bound, network-bound, account-bound, and signer-bound.
8. Global/copytrade transport kill switch is clear.
9. Source recovery is continuous for an increase.
10. Market evidence and instrument metadata are fresh enough to build the immutable order plan.
11. Live positions are fresh/authoritative.
12. Live open orders are fresh/authoritative and acceptable.
13. No unresolved live integrity issue or ambiguous increasing submission exists.
14. Initial D.5 live account is verified flat before first arm.
15. Order and session notional/count ceilings permit the plan.
16. The complete gate set is re-evaluated immediately before signer invocation.
17. The signer independently rechecks arm-session identity/expiry, host/network, deterministic CLOID, action allowlist, and transport stop before signing/sending.

A single boolean must never collapse these gates into hidden authority.

## Transport and failure semantics

- Persist the submission identity and immutable live order plan before signer invocation.
- Persist/reserve the signer nonce before external transmission.
- Use a bounded `expiresAfter` appropriate for the request type.
- Do not automatically retry any write after timeout, disconnect, malformed response, process loss, or uncertain acknowledgement.
- An exception after signer invocation is `SUBMISSION_UNKNOWN` unless authoritative venue evidence proves rejection.
- Reconcile by deterministic CLOID and public fills/order state.
- A cancel request is itself an external write and must use the same signer/nonce/authorization/transport-stop discipline.
- Prefer cancel by CLOID for Trader-owned orders.
- Hard transport stop blocks every write, including cancel, while public reconciliation reads remain available.
- Degraded reconciliation may permit only a direction-and-size-bounded reduce-only exit if the frozen D.3.2 requirements are met.

## Hyperliquid-specific constraints to encode, not improvise

The implementation must be based on current official Hyperliquid documentation and reviewed SDK behavior at implementation time. As of this audit:

- account queries use the actual master/subaccount address, not the API-wallet address;
- API/agent wallets may sign for the account and nonces are tracked per signer;
- Hyperliquid stores the 100 highest nonces per signer and requires unique, time-bounded nonces;
- CLOID is a 128-bit hex identifier;
- order requests require asset ID, side, price, size, reduce-only, and TIF/trigger order type;
- `expiresAfter` is supported for relevant exchange actions and stale expiry has rate-limit consequences;
- the `/exchange` surface contains many non-trading-adjacent privileged actions and therefore must not be exposed generically;
- REST traffic shares an IP weight budget, while exchange actions also have address-based limits.

Primary references:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- https://github.com/hyperliquid-dex/hyperliquid-python-sdk

## D.5 staged implementation plan

### D.5.0 — design/audit (this change)

- freeze architecture and threat model;
- add permanent guardrails proving the simulator engine stays simulator-only and copytrade config carries no signer secret;
- add no `/exchange` call, signer, live dependency, or credential.

### D.5.1 — contracts, authorization, and dry-run envelope

Still **no external exchange write**.

Implement:

- live domain/account naming;
- versioned `LiveOrderPlan`;
- versioned `LiveAuthorizationSession` and append-only audit storage;
- deterministic Hyperliquid CLOID derivation;
- testnet-only host/network policy;
- public signer/account-role preflight interface;
- separate live reconciliation authority contracts;
- order/session risk ceilings;
- dry-run signer envelope that serializes/validates the exact action but cannot transmit or sign;
- Control Center read-only visibility for DISARMED/ARMED/EXPIRED/preflight health, with **no arming endpoint**.

### D.5.2 — isolated signer and TESTNET transport

Only after D.5.1 closure:

- add a pinned/optional reviewed Hyperliquid signing dependency;
- isolate API-wallet secret from the application config/read model;
- implement durable signer-scoped nonce reservation;
- expose only order and cancel-by-CLOID capabilities;
- use exact testnet `/exchange` host;
- implement testnet submission/ack/fill/reconciliation and ambiguous-outcome handling;
- preserve mainnet hard rejection.

### D.5.3 — chaos/security closure

- concurrent submissions and nonce races;
- crash before/after nonce reservation;
- crash before/after network transmission;
- duplicate/out-of-order fills;
- stale/malformed account/order evidence;
- API-wallet deauthorization/expiry;
- wrong signer/account/network;
- authorization-session expiry during READY-boundary race;
- cancel races and late fills;
- hostile SDK/HTTP responses;
- rate-limit exhaustion;
- secret scanning/logging/API/UI persistence audit;
- prove mainnet remains impossible.

Only after D.5.3 is frozen may Phase E separately review a minimal-mainnet canary.

## Phase E handoff boundary

D.5 is not the phase that enables ordinary mainnet capital deployment. Its goal is to prove the write architecture against testnet and close the signing/nonce/authority hazards.

Phase E must separately decide and audit:

- mainnet host enablement;
- OS-backed production secret storage;
- tiny-capital canary limits;
- operator runbook and incident response;
- kill-switch drill;
- monitoring/alerting;
- rollback and reconciliation recovery;
- progressive capital scaling.

No D.5 test, environment variable, config file, UI control, or stale authorization artifact should be sufficient to cross that Phase-E boundary.
