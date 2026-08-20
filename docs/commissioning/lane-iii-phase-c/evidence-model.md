# Evidence model

`EvidenceObject` is immutable and typed. It contains an identity, one L3-A
`EvidenceFamily`, a machine-testable `EvidenceKind`, raw or higher-order
derivation, creation/expiry timestamps, a `SourceProvenance` record, explicit
hypothesis impacts, a correlation key, and optional numeric measurement.

`SourceProvenance` retains canonical L3-B event IDs and canonical event
payload hashes, exact inclusive window bounds, source L3-B data quality, and
a deterministic ID for the L3-B aggregate observation. This supports:

```text
L3-C evidence -> L3-B source observation/window -> canonical L3-B event(s)
```

Session-VWAP evidence identifies the triggering canonical trade and a hash of
the deterministic L3-B session snapshot, including session ID, VWAP, high/low,
and volume. Replay reconstructs that aggregate from canonical events. Short
structural and flow windows retain every constituent canonical ID and hash.

`EvidenceState` separates immutable evidence from current usability. An
authoritative state can become unusable or expired with a typed reason;
evidence is not silently carried forward. `RejectedObservation` preserves why
a bad source observation did not create usable evidence.
