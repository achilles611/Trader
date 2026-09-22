# Caelum HOLD — fixed Asia diagnostic window

- Pass: `20260922-040111Z-asia-diagnostic-oversight`
- First observation: `2026-09-22T04:01:11Z`
- Confirming observation: `2026-09-22T04:02:35Z`
- Fixed window: `2026-09-22T04:15:00Z–04:20:00Z`
- Parent integrator and sole runtime/ledger operator: Sol (`/root`)
- Administrative reviewer: Caelum (`beelzebub_admin`)
- Repository HEAD: `748b3930ef01bc3539da467f721347180a03382b`
- Repository tree: `95ffbcf5693d4cc7a9acee8598556cfa37658030`
- Repair commit `7ac86fac1e3d4e775df151cacd75ae220394a8be` is an ancestor of the reviewed HEAD.

## Decision

**`HOLD_FIXED_WINDOW_20260922T0415Z — ABORTED_PREFLIGHT`**

The fixed `04:15Z–04:20Z` diagnostic window cannot be admitted. At `04:01:11Z`,
fewer than fourteen minutes remained and the required evidence root
`N:\Beelzebub\asia-qualification-20260922` did not exist. At `04:02:35Z` it
remained absent; ports `8090`, `48135` and `48136` had no listener. The
operator-owned NinjaTrader process was present, but no authenticated observer,
account, position, order, reconciliation, loaded-build or ledger state may be
inferred from process presence.

## Concrete HOLD

- **Action held:** consuming the single R2 attempt; calling supported capacity
  recovery; capturing, sliding, trimming, backfilling or rerunning the
  `04:15Z–04:20Z` window; or proceeding to any later gate.
- **Evidence:** no Step 0 host packet, no Step 1 exact runtime binding and no
  Step 2 authenticated preflight sandwich existed at the first two checks.
  R2 is allowed only after those checks are completely clean. A fresh Full,
  exact current flat/no-orders/reconciled proof, supported latch recovery and a
  second Full were all required before `04:15Z` and could not be truthfully
  completed and reviewed within the remaining time.
- **Practical consequence:** this fixed window is missed. Evidence created
  after its boundary cannot repair or qualify it. R2 remains unspent, the
  durable capacity latch remains uncleared, and no ratio observation is
  admitted.
- **Release condition:** predeclare a wholly new future window, then complete
  Steps 0–2 with exact current evidence early enough for independent review;
  only after a clean authenticated preflight may Joseph authorize the one R2
  attempt. Complete fresh Full verification, an exact current
  flat/no-orders/reconciled sandwich, supported recovery and the required
  second Full before the new immutable window boundary.
- **Owner:** Sol remains sole runtime/ledger operator; Joseph remains operator
  and owner. Caelum provides read-only administrative review only.
- **Independent work that may continue:** preserve the missed-window record,
  reconcile the runbook to the newly declared time, prepare N:-rooted evidence
  paths, and perform read-only source/evidence review. Do not operate or stop
  operator-owned NinjaTrader merely to satisfy this record.

## Boundaries retained

- The ratio threshold, direction, tolerance and minimum denominator remain
  undefined; no ratio can be accepted.
- No natural-feed qualification, capacity claim, current runtime authority or
  latch recovery is established.
- No Gates B–D, paper start, arming, orders or live capital are authorized.
- No retry, threshold weakening, feed reduction, relabeling or cross-epoch
  evidence substitution is permitted.
- The checked-in runbook still prints the earlier `22:05Z–22:10Z` window. The
  new `04:15Z–04:20Z` authorization was not written into that file by Caelum;
  this mismatch is additional reason not to improvise execution from the
  document.

## Caelum actions

Caelum read the runbook, inspected repository identity, checked only the N:
evidence-root existence, relevant listener ownership and process presence, and
immediately notified Sol. Caelum did not create the operational evidence root,
start or stop a process, install or reload NinjaTrader, call an API, verify or
mutate the ledger, consume R2, recover a latch, arm, or place an order.
