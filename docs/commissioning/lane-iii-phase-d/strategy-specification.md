# Trader V0 strategy specification

## Exact scope

Trader V0 is a short-horizon, directional signal policy for MNQ. It admits two
L3-C archetypes:

| L3-C interpretation | Signal when entry gates pass |
| --- | --- |
| bullish reversal | `LONG` |
| bearish continuation | `SHORT` |

Bullish continuation and bearish reversal remain interpretable in L3-C and
compete in dominance calculations, but are not entry-authorized. This narrow
pair is the smallest scope that covers both directions and the required clean
reversal/continuation scenarios. It is V0 policy, not an empirical edge claim.

## Input boundary

The strategy accepts only:

1. the exact frozen L3-C configuration hash
   `7e731c61ed7a3b75209bbc31af149dde8185a3e2babb6f1a640952f9be7f250f`;
2. immutable L3-C hypotheses, family contributions, evidence, provenance, and
   snapshot hash; and
3. a same-event-time `TraderDataQuality` record containing only trade, quote,
   book, and context quality plus an upstream market-state hash.

It does not consume prices, raw delta, CVD, DOM measurements, P&L, outcomes,
account state, fills, or orders. It never reconstructs evidence.

## Frozen parameters

| Parameter | V0 value |
| --- | --- |
| Entry relative support | `0.65` |
| Entry dominance margin | `0.10` |
| Retention relative support | `0.58` |
| Retention dominance margin | `0.03` |
| Required entry families | structural, order flow, resting liquidity |
| Minimum entry breadth | 3 positive families |
| Minimum retention breadth | 2 positive families: structural plus flow or liquidity |
| Hypothesis update freshness | 15 seconds |
| Contributing evidence freshness | 30 seconds |
| Maximum hypothesis age for a new entry | 60 seconds |
| Maximum active thesis duration | 120 seconds |
| Signal TTL | 5 seconds |
| Re-entry cooldown | 30 seconds, event time |
| Previously signaled hypothesis memory | 256 IDs, bounded |

All values are immutable during evaluation and included in the semantic
artifact hash. No outcome or live override can mutate them.

## Lifecycle boundary

The trader remembers that it emitted an entry signal for a hypothesis. That is
strategy decision state only. It never claims the signal filled and never
fabricates actual exposure. A later explicit execution/reconciliation boundary
must own position truth.
