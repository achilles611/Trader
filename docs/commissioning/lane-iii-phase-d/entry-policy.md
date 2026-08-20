# Entry policy

Trader V0 ranks current L3-C hypotheses by relative support descending and
hypothesis ID ascending. An allowed candidate enters only when every gate
passes:

1. trade, quote, book, and context quality are all `HEALTHY`;
2. archetype is bullish reversal or bearish continuation;
3. lifecycle state is exactly `ACTIVE`;
4. there is no invalidator, contradictory evidence ID, or family contradiction;
5. hypothesis creation/update/evaluation and contributing evidence are fresh;
6. relative support is at least `0.65`;
7. the lead over the strongest other current hypothesis is at least `0.10`;
8. structural, order-flow, and resting-liquidity families all contribute
   positive family balances, with at least three positive families overall;
9. this exact hypothesis ID has never emitted an entry; and
10. any event-time re-entry cooldown has completed.

Competition includes unauthorized archetypes. An interesting but nearly tied
hypothesis therefore abstains even if its absolute support is high.

## Dependence control

L3-D consumes each frozen L3-C `FamilyContribution` once. It never counts
individual indicators or supporting evidence IDs as additional votes. Four
equal flow-derived evidence IDs remain one `ORDER_FLOW` family and cannot meet
three-family breadth.

## Contradiction

V0 tolerates no entry contradiction. A conflicted or invalidated hypothesis is
not tradable. This conservative gate is deliberate and hash-bound.

## Freshness and continuity

Entry requires a same-time L3-C/quality snapshot, at most 15 seconds since the
hypothesis update, no more than 60 seconds since hypothesis creation, and at
most 30 seconds since every positively contributing evidence source window.
All referenced evidence must remain authoritative, unexpired, healthy, and
traceable. Evaluation reads no wall clock.
