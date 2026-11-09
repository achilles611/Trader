# Lane II paper qualification

## Eight-hour window

Atlas started the owned collector in prewarm on November 9, 2026. The fixed paper window runs from **03:45 to 11:45 MST**. The collector is preregistered for P13, P2, and a cash control under the same cumulative $25 paper campaign accepted at short closeout.

| Gate | Observed state |
| --- | --- |
| Hourly replay and accounting release | Passed retained split-versus-contiguous replay, native checkpoint fixtures, and the first live hour |
| Eight-hour qualification | In progress. The 03:45–04:45 MST prefix verified 3,600 of 3,600 paper seconds, both profile accounts, and four contiguous quarter-hour receipts. |
| 24-hour qualification | Requires the sealed eight-hour pass and fresh admission on the same campaign. |
| Discovery | The isolated worker completed all 50 variations across five retained P-model families and reconciled the five baseline profiles. |

Both retrospective passes processed 22,314 sealed events and agreed on causal decisions, financial accounting, risk, and coverage. The reviewed release also passed 150 continuous-service tests and 22 campaign-risk tests. The collector, offline replay, health task, and dashboard task are installed on Atlas. Windows triggers the health check each quarter hour and refreshes the dashboard one minute later. The current task principal requires a logged-in session.

The exact release, independent admission review, and scheduler evidence are retained on Atlas. The runtime report and crash graph are available through the [local dashboard](http://127.0.0.1:8889/). See the [operator contract](README.md) and [PR #3](https://github.com/achilles611/Trader/pull/3).

## Accepted short window

Capture evidence as of **2026-11-09T05:50:00.099125+00:00**. At report generation, source evidence was **1015 seconds old**. The local status service continues to report current heartbeat and source freshness.

| Dimension | Observed result |
| --- | --- |
| Implemented / installed / exercised | Yes; scheduled collector and watchdog observed |
| Short paper execution | COMPLETE (DEADLINE); 900.0 elapsed / 900.0 planned seconds |
| Short operational qualification | **QUALIFIED**; 900 independently replayed seconds; 3/3 matched fresh eligible windows |
| Closeout | Raw audit PASS_RETAINED_CAPTURE_INTEGRITY_ONLY; accounting PASS_RETAINED_PROFILE_ACCOUNTING; causal replay PASS (scheduled recovery) |
| Owner exit | Code 0; forced closure false |
| Profitability | Insufficient evidence |

The first prewarm failed before any paper tick. Its raw integrity and unchanged flat accounts were reconciled. The transport repair was verified against the public HTTP and price feeds; the replacement used the same campaign and the original sealed predecessor receipt. The cumulative $25 paper limit did not reset.

The original automatic closeout held when the audit import failed. A scheduled retry verified the same sealed capture with a corrected offline import and a replay that uses the collector's directly recorded missing-feature diagnostic order. It published a revised report, including exact decision, accounting and risk parity. All 148 tests for the corrected build passed. The first-pass hold remains in the original records.

The final receipt records one HTTP request canceled at cutoff. The sealed journal contains every scheduled paper second.

Admission measured CPU p95 **36.4%** and at least **5.25 GiB** available RAM. The system disk had **29.35 GiB** free. All **141 tests** passed against the frozen build. Unattended boot remains unqualified; the installed tasks require user logon.

Original coordinator queue: 1 admission-held, 8 completed.

Current short qualification holds: none recorded.

The short receipt recorded zero funded orders and zero model calls.

Frozen application `0174929829a5863be53fb0c05ea9c035798491978f206012fcec2006477e60f5`; configuration `c34001602fdbe232aaa7d7cccad430fab3fc94d9e906ec6ea8f16a38b2fe753b`; collector `f48a341499ae69257aea03ac1e7599172fa430123a26063b940c597cb7b6a46f`.

See [sanitized evidence](EVIDENCE.json), the [operator contract](README.md), and [PR #3](https://github.com/achilles611/Trader/pull/3). Raw tapes, account details and exact local bindings remain on Atlas.

The [earlier held commissioning record](https://github.com/achilles611/Trader/blob/84d137cf601f216d94c6cdc1a9ee857c2335fd38/docs/operations/lane-ii-continuous-rd/EVIDENCE.json) preserves the November 7 capture findings and the original scheduler exercise.

[Independent Caelum assessment](../../administration/caelum/passes/20261108-continuous-rd-resume/PUBLIC_ASSESSMENT.md) accepted the recovered short only.
