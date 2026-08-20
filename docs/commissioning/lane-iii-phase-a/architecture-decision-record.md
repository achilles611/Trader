# L3-A architecture decision record

Date: 2026-08-19

## Decision

L3-A selects **Option 2: an isolated futures-specific constitutional service that preserves Phase-D safety semantics without importing, reusing, or modifying Phase-D execution code.**

Lane III lives in `src/lane_iii/`. Its only dependencies are the Python standard library and its own contracts. It has no import path to Phase D's crypto execution engine, Lane II, Phase E, HTTP clients, websocket clients, a broker SDK, credentials, accounts, or copier infrastructure.

```text
evidence observations
       ↓
family assessments
       ↓
deterministic market hypothesis + confidence state
       ↓
bounded future ExecutionIntent (contract only)
       ↓
LaneIIIFuturesAdmissionKernel (external limits + persisted safety state)
       ↓
ELIGIBLE_FOR_SOVEREIGN_REVIEW | REFUSED

No broker, order, account, or follower operation exists below this point.
```

## Why Option 2

Phase D is frozen as the crypto execution/evidence foundation and has a materially different venue environment, existing Phase-C `CopySignal` contract, SQLite namespaces, execution domains, simulator, and transport roadmap. Importing it would couple MNQ futures to crypto-specific ownership and create the appearance that a simulated crypto adapter can safely stand in for a futures broker/prop boundary. Copying its full execution state machine would duplicate a large trusted surface before a futures adapter, account model, or order-lifecycle design exists.

L3-A instead preserves the constitutional invariants Phase D established:

- external hard risk and operator authority;
- immutable intent identity and duplicate containment;
- explicit durable safety state;
- unknown broker/order/position state fails closed;
- no casual restart rebaseline; and
- reconciliation before a state can be considered safe.

The result is a small, independently testable vessel. A later futures execution phase may evaluate a narrow shared kernel only if a concrete, reviewed invariant is genuinely common. It must not modify frozen Phase D for architectural neatness.

## Consequences

- **Phase D modifications:** none.
- **Phase E modifications:** none.
- **Phase F / Lane II modifications:** none.
- **Broker abstraction:** deferred to a future futures-specific adapter. L3-A accepts no venue endpoint, account ID, account type, secret, credential, signing material, broker command, or generic symbol.
- **Copier boundary:** external and downstream of one future master execution account. No L3-A contract contains a follower, copier, or account field.
- **Current expiry:** an external `FuturesRiskConstitution` admits exactly one concrete CME MNQ expiry; an intent for another MNQ expiry is refused. This preserves root/expiry separation without implementing rollover.
