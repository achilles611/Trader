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

## Final closeout — 2026-09-22T04:07:28Z

**Decision confirmed: `HOLD_FIXED_WINDOW_20260922T0415Z — ABORTED_PREFLIGHT`.**

Sol subsequently completed Steps 0–2, but the authenticated sandwich captured
at `04:05:01Z` did not clear admission. The Step 0 evidence HEAD was
`f60314b12a6544b9c6967885189498a55452a459`, while the selected runtime reported
Git SHA `ac451a1ccdb940b156dd4f3f5c8d5bd93c96e778`. Native input and the native
runtime bridge were blocked, source identity was unavailable, and canonical
depth and warm start were not ready. The ledger writer was `DEGRADED`; Full was
not current to tip (`290757` highest versus `277856` verified, leaving `12901`
unverified rows), and the last blocking row was an `UNKNOWN`
`INCIDENT_OBSERVATION_REJECTION`. These conditions, with fewer than nine
minutes remaining, made the prerequisite chain incomplete.

The same receipt narrowly observed `Sim101` / `LOCAL_SIMULATION`, exact flat
quantity zero, zero working owned and entry orders, complete position and order
snapshots, current reconciliation at `04:04:32Z`, `DISARMED`, activation not
authorized and live capital denied. This clean safety snapshot did not cure the
Git, source, native-path, ledger-tip or time defects and therefore did not
authorize R2 or recovery.

At `04:06:31Z`, Sol recorded `ABORTED_PREFLIGHT`: R2 remained unspent,
recovery was not called, the fixed `04:15Z–04:20Z` window was not moved or
captured, and no Gates B–D, paper start, arming or order action occurred. The
supported stop then exited BeeZConsole PID `15556` cleanly and freed port
`8090`; the `04:07:28Z` closeout recorded ports `8090`, `48135` and `48136`
free, zero Python runtime processes, and operator-owned NinjaTrader PID `17208`
untouched. The operational ledger grew from `1498558464` bytes in the
preflight receipt to `1769041920` bytes at closeout while incident-observation
rejections accumulated. This is preserved failure evidence, not qualification.
Runtime is therefore stopped, not qualified.

Evidence root: `N:\Beelzebub\asia-qualification-20260922`. All 14 entries in
`SHA256SUMS.txt` were independently re-hashed with zero mismatches. Key hashes:

- `01-preflight-sandwich.json` — `EEDAC93814CD85B53BD6EB92BD195C952C38EBAB1AE02AA3B90B4CBA8A2BC3BE`
- `02-aborted-preflight.json` — `34203C073F7A80C25F17B19D519564C7C64B74D81AD5F1172E4751D8A192C4D6`
- `03-supported-stop.log` — `237B4E9337A7822451FAB71A4EE28EF2E6B216A7557AE86E80737D7DDAC219E0`
- `00-fixed-window-predeclaration.json` — `C3067C879041CC356441612788B32051C2A3771A9D64208E0265DE3F04E14800`
- `SHA256SUMS.txt` — `4D3A7EB265CBE6550877F40746BB7D74D4111F314D6D3F7317875F29FB83728D`

The predeclaration itself records `source_identity: null` and
`ABORTED_PREFLIGHT_CAPTURE_PROHIBITED`, independently preserving the no-capture
boundary for the fixed window.

Capacity floors remained unchanged at 8 GiB warning / 4 GiB minimum and the
latch was not recovered. The natural-feed and ratio-policy qualifications also
remain held. Release requires a separately authorized future window and fresh,
fully congruent evidence satisfying the pre-existing conditions; the missed
window must not be moved or retroactively repaired. Sol remains the sole
runtime/ledger operator. This addendum is Caelum administrative review only.
