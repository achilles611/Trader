# Signal contract

`SignalDecision` has four possible decision values:

```text
NO_TRADE
LONG
SHORT
EXIT
```

The immutable record contains:

| Field | Purpose |
| --- | --- |
| `decision_id` | deterministic idempotency identity |
| strategy identity/hash | exact authority binding |
| `decision` | the four-value directional result |
| hypothesis ID | admitted or active L3-C thesis |
| related hypothesis ID | opposing thesis for an opposing-dominance exit, if any |
| creation/expiration | supplied event time and five-second signal TTL |
| relative-support snapshot | L3-C relative support, never probability |
| family summary | one summary per L3-C family contribution |
| reason code | exact entry, hold, exit, or abstention cause |
| L3-C, quality, source hashes | replay and provenance binding |

Each family summary retains strongest support/contradiction balances and the
contributing evidence IDs, evidence hashes, L3-B observation IDs, canonical
event IDs, and source payload hashes.

The contract has no quantity, price, account, broker, order, protective stop,
profit target, fill, position, or arbitrary command. In particular:

```text
SignalDecision(LONG) != ExecutionIntent != order
```

An exact duplicate source-state evaluation returns the identical decision ID.
It does not create a semantically new entry signal.
