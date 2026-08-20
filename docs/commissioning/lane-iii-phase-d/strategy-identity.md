# Strategy identity

Signal authority is registered to one exact semantic artifact:

```text
strategy_id:            l3-trader-v0
strategy_version:       1
instrument:             MNQ
strategy_identity:      l3-strategy-f6f549ced930c2b411b965984ffff555
strategy_artifact_hash: 9cc6526264ca340dbff8ca32f680d05563bed0e617c746c5af1bdceb4ccbc90a
```

`trader-v0-artifact.json` contains the canonical semantic payload plus the
resulting identity and hash. The artifact hash is SHA-256 over the canonical
payload excluding only the resulting identity/hash fields. The L3-A
`LaneIIIStrategyArtifact` then derives `strategy_identity` from strategy ID,
version, hash, and instrument.

The payload binds the L3-C version/configuration, allowed archetypes, all
thresholds and time gates, family requirements, candidate ranking, competition
scope, entry conjunction, exit precedence, duplicate/re-entry behavior, signal
schema, and explicit authority denials.

Changing any bound parameter produces both a different artifact hash and a
different strategy identity. `SignalAuthorityRegistry` contains one
registration and refuses a wrong strategy ID, version, instrument, identity,
or artifact hash. A mutable module filename has no authority.
