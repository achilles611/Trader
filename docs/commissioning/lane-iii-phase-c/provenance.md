# Provenance and quality

Every evidence object has source IDs, event hashes, a deterministic
observation/window identity, and source quality. Evidence is emitted only from
the L3-B pipeline result corresponding to the canonical event passed to the
engine. A caller cannot substitute a source or concrete contract.

Quality gating is family-specific:

| L3-B condition | L3-C result |
| --- | --- |
| Healthy trade/session | Structural, flow, and timing selectors may evaluate |
| Stale/gapped/recovering/incomplete/invalid trade | Source-dependent evidence is unusable; no fresh flow evidence |
| Healthy reconstructed book | Mechanically classified liquidity may evaluate |
| Gapped/recovering/incomplete/invalid/stale book | Liquidity evidence is unusable; no fresh authoritative book evidence |
| Stale derivatives vintage | Preserved only as degraded, non-directional context |

An `INCOMPLETE` flow record never produces signed-flow or effort-versus-result
evidence. A depth reduction becomes pull evidence only if an L3-B caller has
already mechanically classified it as `PULL`; L3-C does not infer a pull from
a reduction or declare participant intent.
