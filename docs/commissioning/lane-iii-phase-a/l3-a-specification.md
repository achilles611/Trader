# Lane III Phase A constitutional specification

## Scope and authority

Lane III is a future MNQ short-horizon evidence-fusion scalper. L3-A grants no trading edge, predictive claim, signal authority, broker access, prop-account access, execution authority, or live-capital authority. The machine-readable [authority manifest](l3-a-authority-manifest.json) is canonical; its hash is `a0cf55741f1ba0db7d713dd3f8523257b82edb9b432367d470b715ea9126a8a9`.

| Authority | L3-A status |
| --- | --- |
| Observe MNQ market data | architecture only |
| Observe DOM / resting liquidity | architecture only |
| Observe options context | architecture only |
| Construct market hypotheses | contract only |
| Compute confidence | semantics only |
| Generate trade signals | NO |
| Request futures execution | contract only |
| Execute futures orders | NO |
| Access broker account | NO |
| Access prop account | NO |
| Change hard risk limits | NO |
| Override flatten | NO |
| Control follower accounts | NO |
| Scientific authority | NO |
| Modify active strategy from P&L | NO |
| Modify Phase E | NO |
| Modify Lane II | NO |
| Live-capital authority | NO |

`ExecutionIntent.execution_authority` and `ExecutionIntent.live_capital_authority` are permanently `False`. The only L3-A admission outcome is eligibility for a future sovereign review; that outcome is not an order submission.

## Evidence-family model

`EvidenceObservation` carries only an identity, MNQ scope, family, source payload hash, observed/expiry timestamps, and temporal behaviour. It has no signal, score, order, account, or strategy-control capability. Future data sources fit under five causal/informational families:

| Family | Future examples | Constitutional treatment |
| --- | --- | --- |
| Structural context | session highs/lows, opening/overnight range, VWAP, value, range acceptance/rejection, CRT-derived range structure | structural evidence only |
| Order flow | aggressive flow, delta/CVD, footprint imbalance, exhaustion, trapped flow | flow evidence only |
| Resting liquidity | DOM depth, pulling, replenishment, stacking, sweeps, suspected iceberg behaviour | observed temporal behaviour, never proved support/resistance |
| Derivatives context | expiry calendar, strike concentration, OI, positioning changes | contextual evidence, never automatic direction or magnet logic |
| Timing/session context | NY/cash open, lunch, settlement, releases, expiration, session regime | contextual evidence only |

CRT has no special authority. It can later supply a structural observation about a defined range, boundary, sweep, re-entry, acceptance, rejection, or reclaim. Resting liquidity explicitly distinguishes `SNAPSHOT`, `PERSISTENT`, `PULLED`, `REPLENISHED`, `EXECUTED`, and `UNKNOWN`; displayed size alone is not behavioural confirmation.

`FamilyAssessment` accepts one family and one or more observations from that family. `ConfidenceState` admits one assessment per family and will reject a repeated observation across assessments. Thus four correlated flow measures can remain four observations inside one order-flow assessment; they cannot become four independent global confirmations. Dependence is not statistically solved in L3-A, but the representation keeps later dependence modelling possible and prevents naïve additive architecture.

## Hypothesis and confidence contract

`MarketHypothesis` is deterministic state, not prose. It contains a deterministic ID, constrained kind (`REVERSAL`, `CONTINUATION`, or `UNRESOLVED`), explicit direction, UTC creation/expiry, MNQ market location, strategy identity/artifact hash, supporting and contradictory evidence references, and validity (`ACTIVE`, `INVALIDATED`, or `EXPIRED`). An unresolved hypothesis must be neutral; directional hypotheses must be bullish or bearish. An evidence observation cannot be in both support and contradiction sets.

`ConfidenceState` contains no score, numeric weight, threshold, or entry boolean. Its lifecycle is `UNRESOLVED`, `BUILDING`, `ALIGNED`, `DECAYING`, `CONTRADICTED`, or `EXPIRED` and it carries one assessment per evidence family. Contradictory evidence requires `CONTRADICTED`; stale evidence requires `DECAYING` or `EXPIRED`. The future semantics are intentionally stateful:

```text
independent family alignment may build confidence
evidence disappearance may decay confidence
contradiction collapses confidence
staleness expires confidence
```

No L3-A threshold commissions an entry. A later strategy may use confidence to request an entry, retain exposure, or request a thesis exit before a hard stop. It may never use confidence to defeat a hard stop, loss limit, disarm, operator flatten, stale/unknown state, or broker safety gate.

## MNQ and intent contract

`LaneIIIInstrument` has exactly one value: `MNQ`. A strategy root is always `MNQ`; `FuturesExecutionInstrument` separately requires a concrete CME MNQ expiry (for example `MNQU6`). The externally owned risk constitution admits one concrete expiry. A different MNQ expiry is rejected, so no implicit rollover rule is hidden in strategy code.

The future request is `ExecutionIntent`:

```text
strategy identity + strategy artifact hash
hypothesis ID
specific MNQ CME expiry
purpose (ENTRY | THESIS_EXIT | PROTECTIVE_EXIT)
target exposure (LONG | SHORT | FLAT) + whole-contract quantity
created/expiry UTC timestamps
required entry stop semantics
optional bounded profit-taking semantics
confidence snapshot hash + evidence-state hash
deterministic intent ID and intent hash
```

It cannot express an account, follower, copier, broker command, amend, cancel, transfer, withdrawal, credential, arbitrary symbol, quantity cap, or flatten override. Entry requires an explicit protective price-stop request; strategy exits can only target flat and cannot request a reversal.

## Futures risk constitution and failure semantics

`FuturesRiskConstitution` is external to both the artifact and intent. It sets maximum MNQ exposure, maximum individual order quantity, maximum daily loss, maximum session loss, the one-position mode, and one admitted concrete expiry. The strategy may request size but cannot set its own maximum.

`SafetySnapshot` requires explicit market freshness, broker freshness, position state/whole-contract quantity, working-order state, and session risk. For a future entry, the admission kernel refuses disarm/pause, stale or unknown market/broker state, unknown position, unknown or present working orders, loss-limit breach, quantity/exposure breach, existing position, non-admitted strategy/hash/expiry, expired intent, mismatch confidence, or mismatch active hypothesis. An exit cannot reverse, exceed known position size, or operate from unknown state.

The frozen failure rules are:

```text
UNKNOWN POSITION != ZERO
UNKNOWN ORDER != CANCELLED
UNKNOWN BROKER STATE != SAFE
UNKNOWN DATA AGE != FRESH
```

No L3-A code contacts a broker, so it cannot place a risk flatten. The operator `FLATTEN` command instead writes a durable, disarming, non-resumable `flatten_latched` state for a future sovereign execution service to consume. The strategy has no field or operation that can clear or veto it. Authority priority remains:

```text
operator flatten / sovereign risk
    > hard stop / mandatory risk exit
        > strategy confidence or normal strategy exit
```

The three flatten authorities are intentionally separate: `THESIS_EXIT` is a strategy request because supporting evidence materially decayed; `PROTECTIVE_EXIT` is a strategy request for its declared protection semantics; hard risk flatten belongs to the future futures sovereign; and an operator `FLATTEN` is a highest-priority human command. Strategy intent cannot express a risk flatten, suppress either flatten class, or retarget flat into a reversal.

The `ARM`, `DISARM`, `PAUSE_NEW_ENTRIES`, `RESUME_NEW_ENTRIES`, `FLATTEN`, `VERIFY_POSITION`, `VERIFY_BROKER_STATE`, and inspection commands are enumerated as `OperatorCommand`. Inspection/verification commands are audit semantics in L3-A; they do not make stale or unknown information fresh. A future authenticated operator surface must retain these meanings.

Because no futures broker lifecycle is commissioned, L3-A does not pretend to implement fills or cancellations. The successor futures sovereign must keep these minimum semantics: partial fills remain position evidence; a rejection is not an unknown order; unknown submission/order status blocks new exposure; working orders are reconciled before shutdown/restart/session transition; disconnect, machine restart, exchange halt, rollover, or inability to verify flat state block new exposure; and duplicate submissions converge through durable correlation rather than retrying blindly.

## Persistence, recovery, and time

`LaneIIISafetyStore` persists only: operator state and append-only operator events; active hypothesis reference; confidence snapshot; latest authoritative safety snapshot; immutable intent ID/hash/payload; disposition; and unresolved intent correlation. It does not persist raw market observations, secrets, broker payloads, followers, or research data.

On restart unresolved intent records remain unresolved. Explicit recovery only can resolve one record, and only after a fresh broker observation reports an exactly flat position and clear working orders. It does not rebaseline, infer flatness, fabricate a fill, submit/cancel anything, or clear a latched operator flatten. Exact intent hashes make duplicate reprocessing idempotent.

All contract timestamps are canonical UTC ISO-8601 with offsets. `FuturesTimeContext` explicitly carries receipt UTC, exchange UTC timestamp, futures-session identity, named session phase, well-formed IANA display-timezone label, and hashed economic-event/expiration context. `SessionRiskState` also has an explicit futures-session identity. The frozen time rule for successors is: exchange timestamps and trading-session identity are authoritative operational inputs; UTC is persisted and replayed; local time is display-only; overnight, cash-open, daylight-saving, event, and expiry interpretation must live in one futures-time service rather than scattered strategy code. L3-A deliberately contains no naïve local-time strategy logic.

## Phase E and copier boundaries

Phase E may later discover, test, reject, support, or refine candidate relationships and may inform human/controlled promotion of a new frozen Lane III artifact. It cannot rewrite an active artifact, receive an L3-A execution capability, or be modified by Lane III. Live P&L is not scientific truth and cannot self-modify a strategy.

The future execution topology is one authoritative master account followed by an external copier. L3-A does not send orders to followers, inspect follower success, adapt from follower count, or treat copier state as market evidence.

## Explicit deferrals

L3-A implements no broker/prop selection, transport, credentials, DOM or footprint ingestion, delta/CVD/CRT/options signals, confidence scoring, threshold calibration, real strategy, ML, parameter search, testnet or live order, copier, GUI, rollover engine, or Phase E promotion path. It is the vessel only.
