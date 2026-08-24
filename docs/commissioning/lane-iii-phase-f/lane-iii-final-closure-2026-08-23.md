# Lane III final commissioning and closure audit — 2026-08-23

## Judgment

**LANE III BLOCKED**

Exact remaining material boundary: the public NinjaScript market-data boundary
exposes neither a provider-authoritative event sequence nor a synchronized
full-book snapshot/recovery watermark. Frozen L3-B intentionally keeps
unsequenced trade flow incomplete, refuses unsequenced deltas, and treats
unsequenced book snapshots as `INCOMPLETE`. Relabeling the bridge's local
counter as a provider sequence, guessing completion from callback silence, or
diffing snapshots into pretend provider deltas would violate the frozen
provenance contract.

The minimum legitimate next action is to commission a read-only source that
exposes authoritative event sequencing plus a sequenced incremental MBP stream
and an explicitly synchronized full snapshot/recovery boundary. CME MDP 3.0 is
a concrete sufficient class of source: its MBP records carry per-instrument
`RptSeq`, update action, side, price, size, and level; its recovery snapshot
carries the last incremental sequence processed. That integration must remain
outside `src/lane_iii`, then NinjaTrader must be authenticated and the combined
path recommissioned before directional shadow is enabled.

Lane III can remain frozen as an observation/normalization/interpreter
laboratory with directional live shadow disabled. It cannot be declared fully
frozen under this pass's evidence-complete directional standard.

## Repository

| Field | Result |
| --- | --- |
| Branch | `codex/l3-f3-live-shadow-prep` |
| Starting SHA | `c272a9dcb6b00bb7d204b8b52a7690b1721d6af7` |
| Starting tree | Clean; matched `origin/codex/l3-f3-live-shadow-prep` |
| Prior frozen baseline | `7f01d6b52f1ca3987054ea6002697c552680995f` |
| Final SHA | Closure commit containing this record; exact SHA is reported in the handoff |
| Frozen `src/lane_iii` diff | Empty |

## Authentic commissioning attempt

Normal startup was used. NinjaTrader 8.1.6.3 started through its installed
executable. `BeezConsole.exe` launched the normal Control Center runtime. One
Python PID owned both `127.0.0.1:8090` and the sole
`127.0.0.1:48135` listener; listener `start_attempts` was `1`.

The monitored evidence interval was
`2026-08-24T00:12:15Z` through `2026-08-24T00:26:53Z` (14 minutes 38
seconds). NinjaTrader remained at its Welcome screen with the visible error
`Incorrect username or password. Please try again.` It never established the
observer connection. Therefore this interval is an authentic commissioning
*attempt*, not authentic MNQ market-flow evidence.

| Counter | Observed |
| --- | ---: |
| Accepted observations | 0 |
| Rejected observations | 0 |
| Quotes | 0 |
| Trades | 0 |
| Depth | 0 |
| Normalized admissions | 0 |
| Stale / malformed / duplicate | 0 / 0 / 0 |
| Transport resets | 0 |
| L3-C calls | 0 |
| L3-D evaluations | 0 |
| LONG / SHORT / NO_TRADE | 0 / 0 / 0 |
| Suppressions | 0 |
| Execution denials | 0 |
| Execution attempts | 0 |

The configured binding remained native `MNQ SEP26`, provider contract ID
`MNQ SEPT26`, canonical `MNQU6`, CME, tick `0.25`. Current provider traffic
could not re-authenticate that identity. Earlier authentic captures remain
historical evidence only and were not relabeled as this pass's commissioning.

Because no provider session was established, a second authentic run and a
controlled live reconnect could not produce market evidence. The normal
backend was then stopped so lifecycle tests could own their ports; NinjaTrader
remained visible at its login screen. Automated disconnect, reconnect,
price-feed loss, and session-boundary tests provide fail-closed implementation
evidence, but are not represented as authentic commissioning.

## Trade provenance

Installed-version reflection and the public NinjaTrader contract establish:

| Value | Provenance |
| --- | --- |
| Last price, Last volume, Last time | Directly furnished on `MarketDataEventArgs` |
| Bid and ask at the Last event | Directly furnished on `MarketDataEventArgs` |
| Latest bid/ask sizes | Directly furnished by preceding Bid/Ask callbacks and retained with their source times |
| Native aggressor side | Unavailable in the public callback |
| Exchange timestamp | Unavailable in the public callback |
| Provider sequence | Unavailable on the public Last, quote, and depth callbacks |

The provider boundary now supports explicit `PROVIDER_NATIVE`,
`BID_ASK_CLASSIFICATION`, and `UNKNOWN` source labels. NinjaTrader's current
bridge uses only the latter two. A trade is quote-derived only when:

1. direct Last-event bid/ask are finite, positive, and not locked/crossed;
2. cached bid/ask prices exactly match those Last-event values;
3. both furnished size contexts are positive, non-future, and at most 10
   seconds old;
4. the source emits a quote before the trade and carries its observation ID;
5. the adapter resolves that exact quote, exact prices, and equal event time;
6. price is at/above ask (`BUY`) or at/below bid (`SELL`).

Inside-spread, missing-context, locked, crossed, stale, mismatched-reference,
quote-after-trade, and invalid-context prints remain `UNKNOWN`. No tick
direction, candle direction, future quote, future trade, or subsequent price
movement is read. Runtime counters separately retain provider-native,
quote-derived, and unknown classifications. No live classification counts are
claimed because authentication prevented flow.

Quote-derived classification repairs the known side-provenance defect without
claiming native provider evidence. It does not repair sequencing: an
authentic-shaped end-to-end test carried three same-time quote-derived BUY
classifications through the bridge, but with the public boundary's null
provider sequences L3-B correctly kept quote/trade quality and signed flow
`INCOMPLETE`. L3-C created no `ORDER_FLOW` evidence and L3-D was not evaluated.

## Depth provenance

`MarketDepthEventArgs` directly supplies side, `Add`/`Update`/`Remove`, price,
volume, zero-based level position, time, and reset state. The bridge now
preserves those exact mutation fields alongside its accumulated snapshot.
Price-feed transitions and reset callbacks clear all locally accumulated quote
and depth state. The shadow consumer records provider price state and discards
L3-B/L3-C/L3-D state on a loss boundary. A depth callback carrying `is_reset`
also advances the runtime state generation and suppresses immediately, even if
the callback's rebuilt book is still empty or partial.

The adapter intentionally continues to emit `BookSnapshotEvent`, not
`BookDeltaEvent`. `provider_sequence` remains null, so frozen reconstruction
remains `INCOMPLETE` and cannot construct authoritative
`RESTING_LIQUIDITY`. Initial subscription, reconnect rebuild, partial book,
and stale book cannot be distinguished as complete using the public callback.
That is the fail-closed boundary, not an implementation omission to paper over.
No authentic depth deltas, snapshots/rebuilds, malformed depth suppressions, or
resting-liquidity evidence are claimed from the logged-out run; all such live
counts were zero.

## Scientific pipeline and safety

The runtime still has exactly one path:

```text
NinjaTrader observation
-> listener admission/session ledger
-> one managed listener worker
-> LaneIIIShadowRuntime
-> NinjaTraderMarketDataAdapter
-> frozen L3-B
-> frozen L3-C
-> frozen L3-D
-> bounded shadow audit / STOP
```

No Lane II, broker, provider-execution, simulated-execution, submit, modify,
cancel, or flatten interface is imported by the live-shadow module. A forced
handoff reaches only `ShadowExecutionGuard.deny()`, records a denial, raises,
and leaves attempts at zero.

| Safety action | Count |
| --- | ---: |
| Listener owners during normal attempt | 1 |
| Shadow consumers during normal attempt | 1 |
| Execution attempts | 0 |
| Broker calls | 0 |
| Orders placed | 0 |
| Orders modified | 0 |
| Orders cancelled | 0 |
| Flatten actions | 0 |
| Capital touched | 0 |

## Verification

| Verification | Result |
| --- | --- |
| Provenance + bridge + listener + live-shadow + L3-B/C/D focused set | 133 passed, 32 subtests passed in 10.27s |
| Complete backend suite | 584 passed, 96 subtests passed in 387.62s |
| Skips / failures | 0 / 0 in the clean final runs |
| Replay/determinism | Passed in focused and full suites |
| Forced execution boundary | Passed; attempts remain 0 |
| `git diff 7f01d6b..HEAD -- src/lane_iii` | Empty |
| Installed source mirror | Repository and installed AddOn/indicator SHA-256 hashes match |
| Installed AddOn SHA-256 | `1A9C56A7951B77BBCAA5E4D17A5BE1EFFF8FA3E1F7547B3CB34B88AC6CF3F443` |
| Installed indicator SHA-256 | `F3C62EAD83647D68097C1D8F98A73156DDCF77204DFEE1C1FA78AB2BB13B05AB` |
| NinjaScript compilation | Bundled-Roslyn in-memory compile: 0 errors; 500 `CS1701` runtime-policy warnings only; authenticated in-platform load unavailable |

An earlier focused invocation while the real listener intentionally owned
`48135` produced two expected bind-collision failures. After the controlled
listener stop, the same lifecycle coverage passed in the 133-test final run;
these were environmental collision proofs, not unexplained regressions.

During final review, the new depth-reset regression test initially queried a
nonexistent field in the sanitized status mapping. The assertion was corrected
to inspect the in-process pipeline state; the implementation was unchanged,
and both subsequent final runs above were clean.

## Final authority

```text
Observation:       YES — observe-only boundary; authenticated live source unavailable
Normalization:     YES — within frozen contracts; authoritative sequencing NO
Interpretation:    YES — frozen L3-C authority; current inputs fail closed
Shadow decision:   YES — no execution path; directional evidence incomplete
Execution:         NO
Live capital:      NO
```

Directional live shadow must remain disabled. The one exact scientific next
boundary is provider-authoritative event sequencing plus explicit synchronized
book snapshot recovery.
