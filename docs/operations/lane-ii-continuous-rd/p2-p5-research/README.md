# Lane II P2 and P5 research

[Wave 01](WAVE_01.json) defines the next paper research roster. It contains 25 experimental arms per family, including the two seeds, with one unchanged baseline per family. The original five-model discovery study used one retained BTC five-minute block of 9,950.94 seconds and 140,310 events. Its exact event digest is `1f131c1b8cbb89af2464cfa8d3107c791f6620b31bbd19fec61da5453199820b`.

## What worked on the discovery tape

| Family | Best observed result | Comparison that matters |
| --- | --- | --- |
| P2 sustained confirmation | **+$4.49678**, four closes and three winners | Targets of 30% and 50%, spreads of 1.5¢ and 2¢, and the 12% friction cap each matched the 40% target baseline's complete realized P&L path. A 9% cap left three closes and +$2.17258 after excluding the first +$2.32420 winner. |
| P5 late momentum | **+$11.69237** at the 100% target, one close | The 80% target earned +$11.22341. The 100% target closed about 4.08 seconds later, added $0.46896, reduced modeled fees from $0.43659 to $0.40763, and had the same $2.48780 max marked drawdown. The 60% target earned +$8.68719 on that opportunity. |

For P2, the leading arms repeatedly captured the same four outcomes across individual execution and target changes. The $4.50 stake arm retained those four close times with +$3.23270 net and $0.95520 max marked drawdown, versus $1.34636 at the $6 baseline stake. For P5, the difference came from the exit on one observed opportunity. Spread and friction changes each preserved the baseline result on that trade. Reusing the same contracts across variants gives matched comparisons. It supplies one observation block.

## Seeds and agreement

| Family | Spread ceiling | Estimated round-trip friction cap | Profit target | Baseline sizing retained at seed |
| --- | ---: | ---: | ---: | --- |
| P2 | 2¢ | 12% | 40% | $6 |
| P5 confluence | 2¢ | 20% | 100% | $10 standard and $15 high-confidence |

The seeds combine individually tested settings. Their joint behavior is a separate hypothesis. Each family branches in both directions across execution filters, target, stake, stop, persistence, timing, and depth. P2 also varies the minimum spot move and net target headroom. P5 also varies payout multiple and proxy margin. Each family includes two deliberate crosses and an unchanged matched baseline. The 52 accounts are independent counterfactuals and their P&L stays separate.

The shadow agreement compares three peers within each family, its baseline, seed, and tighter-execution arm. Record the first agreement for a contract and direction when two distinct arms emit matching entry intents within five seconds. Use only intents already available at that timestamp. Track agreement yield and matched outcomes beside each single arm. Baseline P2 admits at 30–90 seconds after open and baseline P5 at 180–285 seconds, so their simultaneous votes have no overlap. A separate sequential hypothesis could use an early P2 intent followed by a later P5 intent on the same contract and side.

## Next evidence

Use one public-data tape and only the P2 and P5 paper research roster. Register fresh calendar windows, disjoint market-day blocks, exact source and control hashes, power, multiplicity, and a fixed final look before observing results. Compare net P&L after fees with both the matched parent and cash. Report closed trades, fills, peak open basis, marked drawdown, and each market day's contribution. Repeat comparisons with adverse tick, delay, and depth stress. Favor a parameter neighborhood whose gains persist across blocks and adjacent arms. Freeze the finalists before a separate confirmation window and test whether each result survives removal of its best block.

Wave 01 is a research design and can rehearse against sealed historical evidence. The fresh calendar and confirmation release are separate gates. The operational 8-hour/24-hour qualification and the shared paper campaign retain their own roster and evidence requirements.

### Offline rehearsal on Atlas

Use the retained baseline result and the same Python executable bound in its source manifest. The runner binds the Wave 01 file at preparation and checks it again at replay. Use a fresh external output directory.

```powershell
python -I -B scripts/lane_ii_continuous/long_research.py prepare --baseline $RetainedBaseline --experiment docs/operations/lane-ii-continuous-rd/p2-p5-research/WAVE_01.json --output $PreparedPlan
python -I -B scripts/lane_ii_continuous/offline.py -- scripts/lane_ii_continuous/long_research.py run --plan $PreparedPlan --output $FreshReplayDirectory --status $StatusFile --maximum-seconds 600
```

`RESULT.json` binds the compact `ENTRY_EVENTS.json` trace. The trace supplies entry intents, cancellations, and fills for the shadow agreement analysis.

### Wave 01 rehearsal result

The offline rehearsal completed all 50 arms and both baselines on the original 140,310-event discovery tape. Both baselines reproduced the recorded financial events, decisions, and terminal inventory. All 185 tests passed, including the scope, exact binding, and network-denial checks.

| Family | Seed | Leading closed arm on this tape |
| --- | --- | --- |
| P2 | +$4.49678, four closes and $1.34636 max marked drawdown | Three-second persistence earned **+$5.87329** with five closes, one additional filled contract and the same $1.34636 max marked drawdown. The later window earned +$5.28943 across five closes. |
| P5 | +$11.69237 on one close, with $2.48780 max marked drawdown | The larger stake earned **+$12.75530** on the seed's same single opportunity, with $2.72375 max marked drawdown. Keep the original stake in the seed and treat the larger-stake arm as sizing sensitivity. |

The P2 earlier window and P5 120% target each ended with one open position and sit outside the closed-arm rankings. The three shadow peers agreed on 13 distinct P2 contract and direction pairs and two P5 pairs. Four P2 pairs and one P5 pair had a subsequent fill by a peer. This measures intent overlap and follow-through. The rehearsal uses the same discovery block as the initial study.

## P1 diagnosis

P1 is an early BTC continuation model. The prior impulse combines the last 120 seconds before open at 60%, the previous completed five-minute move at 30%, and the previous completed hour at 10%. It takes the direction of that score only when the current spot and order book confirm it. The tested persistence arm admits at 5–45 seconds after open.

The persistence baseline closed 13 trades with one winner, ten hard stops, and **−$9.45707 net**. Holding its fills and exit times fixed, adding back $2.85707 in modeled fees leaves **−$6.60**. Adding back $1.78 in modeled adverse ticks leaves **−$4.82**. The 7.2% friction cap earned +$0.67941 on one close. A useful inversion diagnostic would preserve the original signal and quote time, then price the opposite token at its own executable bid and ask, depth, delay, and fees. Compare both directions at matched fixed horizons before evaluating a side-aware lifecycle. P1 remains outside Wave 01.
