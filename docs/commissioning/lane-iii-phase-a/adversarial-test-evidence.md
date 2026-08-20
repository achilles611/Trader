# L3-A adversarial test evidence

`tests/test_lane_iii_phase_a.py` is the L3-A proof suite. Targeted result: **22 passed**.

| Required containment | Evidence |
| --- | --- |
| Unauthorized identity / wrong artifact | immutable registry rejects with `unauthorized_strategy_identity` or `strategy_artifact_hash_mismatch` |
| Wrong instrument / expiry | non-MNQ and unspecified expiry constructors refuse; non-admitted MNQ expiry refuses |
| Excessive quantity / malformed / expired intent | external quantity cap, required protective stop, UTC expiry validation |
| Duplicate intent | deterministic ID/hash and SQLite unique correlation return replay evidence; one unresolved record remains |
| Stale broker / stale market | explicit `STALE` refuses independently |
| Unknown position / order | `UNKNOWN` refuses and cannot be treated as zero/cancelled |
| Loss / one-position hard risk | daily loss, session loss, and existing position refuse despite `ALIGNED` confidence |
| Arbitrary broker command | kernel accepts only exact `ExecutionIntent`; dict command is refused |
| Change hard risk limits | intent has no risk-limit fields; kernel owns external immutable constitution |
| Suppress flatten | operator flatten latches, disarms, and cannot be resumed through strategy-side controls |
| Follower account access | contract field inspection proves no follower, copier, or account field exists |
| Phase E / Lane II crossover | AST test verifies no Phase E, Lane II, Phase D transport, HTTP, or websocket import |
| Evidence directly issuing order | evidence object is refused by exact intent boundary |
| Evidence-state integrity | intent confidence and evidence hashes must match the persisted active hypothesis state |
| Time semantics | UTC/exchange UTC, session identity, phase, IANA display zone, event and expiry hashes are explicit |
| Restart persistence | active hypothesis, confidence hash, safety snapshot, operator state, and unresolved intent survive fresh store creation |
| Unresolved recovery | recovery refuses unknown state and requires fresh broker + exact flat + clear order state |
| Correlated evidence | four order-flow observations remain one family assessment; duplicate family aggregation is refused |
| Operator disarm / emergency flatten | disarm blocks a request; flatten writes the durable highest-priority latch |

The test suite contains no broker fixture, API credential, account, endpoint, or network transport. It therefore demonstrates containment rather than a simulated claim of broker execution.
