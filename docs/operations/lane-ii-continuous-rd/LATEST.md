# Lane II continuous research: short paper qualification

Capture evidence as of **2026-11-09T05:50:00.099125+00:00**. At report generation, source evidence was **1015 seconds old**. The local status service continues to report current heartbeat and source freshness.

| Dimension | Observed result |
| --- | --- |
| Implemented / installed / exercised | Yes; scheduled collector and watchdog observed |
| Short paper execution | COMPLETE (DEADLINE); 900.0 elapsed / 900.0 planned seconds |
| Short operational qualification | **QUALIFIED**; 900 independently replayed seconds; 3/3 matched fresh eligible windows |
| Closeout | Raw audit PASS_RETAINED_CAPTURE_INTEGRITY_ONLY; accounting PASS_RETAINED_PROFILE_ACCOUNTING; causal replay PASS (scheduled recovery) |
| Owner exit | Code 0; forced closure false |
| Eight-hour and 24-hour qualification | Neither run started |
| Profitability | Insufficient evidence |

The first prewarm failed before any paper tick. Its raw integrity and unchanged flat accounts were reconciled. The transport repair was verified against the public HTTP and price feeds; the replacement used the same campaign and the original sealed predecessor receipt. The cumulative $25 paper limit did not reset.

The original automatic closeout held when the audit import failed. A scheduled retry verified the same sealed capture with a corrected offline import and a replay that uses the collector's directly recorded missing-feature diagnostic order. It published a revised report, including exact decision, accounting and risk parity. All 148 tests for the corrected build passed. The first-pass hold remains in the original records.

The final receipt records one HTTP request canceled at cutoff. The sealed journal contains every scheduled paper second.

Admission measured CPU p95 **36.4%** and at least **5.25 GiB** available RAM. The system disk had **29.35 GiB** free. All **141 tests** passed against the frozen build. Unattended boot remains unqualified; the installed tasks require user logon.

Original coordinator queue: 1 admission-held, 8 completed.

Current short qualification holds: none recorded.

Next action: Qualify hourly incremental accounting and causal replay before eight-hour admission. No funded orders or model calls were recorded.

Frozen application `0174929829a5863be53fb0c05ea9c035798491978f206012fcec2006477e60f5`; configuration `c34001602fdbe232aaa7d7cccad430fab3fc94d9e906ec6ea8f16a38b2fe753b`; collector `f48a341499ae69257aea03ac1e7599172fa430123a26063b940c597cb7b6a46f`.

See [sanitized evidence](EVIDENCE.json), the [operator contract](README.md), and [PR #3](https://github.com/achilles611/Trader/pull/3). Raw tapes, account details and exact local bindings remain on Atlas.

The [earlier held commissioning record](https://github.com/achilles611/Trader/blob/84d137cf601f216d94c6cdc1a9ee857c2335fd38/docs/operations/lane-ii-continuous-rd/EVIDENCE.json) preserves the November 7 capture findings and the original scheduler exercise.

[Independent Caelum assessment](../../administration/caelum/passes/20261108-continuous-rd-resume/PUBLIC_ASSESSMENT.md) accepted the recovered short only.
