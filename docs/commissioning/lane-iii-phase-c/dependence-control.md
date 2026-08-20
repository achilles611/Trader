# Dependence and double-counting control

L3-C does not count indicators. Each evidence impact is assigned to one frozen
L3-A evidence family. For every hypothesis and family, it calculates at most:

```text
strongest support - strongest contradiction = family balance
```

No sum occurs within a family. Thus negative delta, CVD-like signed flow,
aggressive sell imbalance, and price-response failure can improve diagnostics,
but their shared `ORDER_FLOW` family can contribute only once to a hypothesis's
global score. Duplicate evidence IDs are rejected, and duplicate/late L3-B
events are not interpreted as new evidence.

Separate families may contribute separately because they are deliberately
different measurement domains. This is a conservative transparent control,
not covariance estimation or a claim of statistical independence. Correlation
keys remain with evidence for future audit, but never create an extra vote.
