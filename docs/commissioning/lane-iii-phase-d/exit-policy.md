# Exit policy

Trader V0 emits `EXIT` for an active strategy thesis using this deterministic
precedence:

1. active thesis is missing from the supplied L3-C snapshot;
2. any required market quality is degraded;
3. hypothesis is invalidated;
4. hypothesis is expired;
5. strategy thesis age reaches 120 seconds;
6. an allowed opposite-direction hypothesis passes the full new-entry gates;
7. a blocking contradiction appears;
8. relative support falls below the `0.58` retention threshold;
9. retention breadth loses structural plus either flow or liquidity support;
10. the lead over the strongest competitor falls below `0.03`; or
11. hypothesis/evidence freshness is lost.

The `0.58` support and `0.03` dominance retention thresholds are deliberately
weaker than the `0.65` and `0.10` entry gates. Retention also needs two families
instead of three. This explicit hysteresis prevents small oscillations around
an entry boundary from producing uncontrolled churn.

A decisive opposing thesis produces `EXIT`, never a same-event reversal. The
opposing hypothesis ID is recorded for audit. After exit, a different
hypothesis must wait 30 event-time seconds; the same previously signaled
hypothesis can never re-enter while retained in the bounded 256-ID history.

Confidence decay can therefore kill a thesis before any hard stop. This is a
strategy signal only. It cannot weaken, replace, delay, or veto sovereign hard
risk or operator flatten authority, and it does not assert that a broker
position exists.
