# Confidence semantics

`relative_support` is a deterministic interpretation score in `[0, 1]`; it is
not calibrated win probability, expected return, trade quality, or a directive.
`0.50` means the fixed family model has no net directional support for that
hypothesis. Values above/below it reflect currently valid family balances.

For the configured family set, L3-C calculates:

```text
relative support = clamp(0.50 + sum(family balances) / (2 * configured family count))
```

This is intentionally small and unweighted. Selectors supply a directly
observable magnitude or the common `0.50` structural/liquidity increment; no
P&L, outcomes, calibration, or fitted weights are read. `FamilyContribution`
lists the IDs responsible for the strongest support and contradiction, so each
increase and decrease is inspectable through `ConfidenceUpdate`.
