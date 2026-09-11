# Pong commissioning — blocked, not deployed

This pass used Joseph's elevated paper-only authority (`b4a7b3c3da4305a1be9bb1c28e8086deba4d29ee5d1733f4564f606b997cf17e`). The accepted baseline remains `7132bae6c8afc63f7c09c8131842ba98edf97ff0`. The deployment checkout remains `7d63f68a9e5fe994a78eb249c366fa606aaa141f`.

**No Pong operational start occurred. No T0, deadline, execution epoch, or independent Pong controller was established. Native compilation/deployment and selector activation were not performed. This is incomplete commissioning, not a successful paper launch.**

## What was implemented and tested

The isolated candidate now has frozen experimental hourly-trend and CRT definitions, scoped unavailable/non-voting options handling, a distinct one-contract paper policy/risk identity, atomic Discernment frame ingestion, and hash-bound reconciliation build identities. Existing profile hashes and the accepted scalar Discernment/Pong behavior are retained. Options absence grants no positive confidence; the research mapping remains unvalidated directional support.

The hourly interpretation requires two contiguous, fully observed completed UTC hours: strictly higher highs and lows indicate LONG trend, strictly lower highs and lows indicate SHORT; a one-sided sweep of the prior range followed by a close strictly inside that range supplies mirrored CRT direction. All other cases are neutral. Exact definitions and authority are hashed by `pong_experiment.experimental_definition()`.

Registration does not establish execution readiness. Runtime readiness, arming, final entry admission and entry transport all deny the uncommissioned Pong profile. Risk-reducing commands keep their existing path. The legacy unsigned observer does not feed a falsely authenticated Pong producer or fall through to Scalper entry logic.

Focused validation: 103 tests plus 50 subtests passed for changed Pong definitions, frame handling, quantity identity, profile contracts and denial fences; 48 tests plus 31 subtests passed for affected policy, risk and transport boundaries. These are preparation/denial tests, not native end-to-end or controller certification. Accepted full-suite evidence was reused for unchanged code; the historical full suite was not rerun.

## Verified native evidence

Evidence directory: `D:/CodexRecovery/Pong-elevated-20260911T104000Z`. Its directory name is not a verification timestamp; embedded UTC timestamps are authoritative.

The final signed read-only reconciliation probe completed **2026-09-11T10:54:20.033548Z**, PASS in two stable sessions, with exact `Sim101 / LOCAL_SIMULATION / MNQ SEP26`, position zero, working orders zero, working entries zero, complete snapshots, no foreign activity, and protective-stop state NONE. It sent zero execution commands, zero execution session grants and zero heartbeats. The two probe grants confer observation-only authority.

Loaded paper AddOn source fingerprint: `ded3309410f44aeb56018838fe1e59347c552664c4052cdab2bc56e9a71b2ef0`.

Loaded assembly fingerprint: `3a94f74f816d12d5e11a9eeba9bca60d15031a898301c18dda438c695779995f`.

Final probe proof hash: `eb82f8a2fd33b0281ef6697b2ff9fcc35100a5409542a829f2af1390d777edad`.

This is current native account/build evidence at that timestamp, not proof that a Pong build was loaded. Existing losses, counters, lockouts, production ledger and profile-switch history were not reset or replaced. Test ledgers used isolated temporary directories on H:.

## Source readiness failure and remaining integration

A bounded read-only native capture ran from **2026-09-11T10:44:10.825739+00:00** to **2026-09-11T10:44:31.035014+00:00**. It recorded 45 QUOTE, 15 TRADE, 6 DEPTH, 4 HEALTH and 83 informational ACCOUNT callbacks. The receiver reported no rejections. Raw input is preserved in `native-input-probe-1044/native-observations.jsonl`.

The source declares `BOUNDED_LATEST_STATE_2HZ`; its native publisher throttles trade publication. That capture cannot prove the complete hourly trade/volume coverage required by the new producer interpretation. The published depth has crossed bid/ask ranges. For example, the first captured snapshot has best published bid 29328.25 and ask 29312.75. A green observer attachment therefore does not establish usable Pong liquidity.

The execution probe authenticates its own signed reconciliation channel. The existing market-observation wire has no signature/source-build attestation; typed loopback records and ordinary content hashes are not signatures. A Pong input authentication boundary was not completed. The discarded prototype incorrectly assumed different depth payload fields and complete/authenticated stream coverage, so it is not imported or retained as executable candidate code. Its exact draft was preserved outside the checkout at `unintegrated-draft/pong_inputs.py`, SHA-256 `265ab1da24e15f3b8a6470c9699c894e41c209cc9a86269d6e41fb5b273a1041`, for review only.

Native book reconstruction also needs review against the platform's positional depth contract. NinjaTrader defines `Position` as the zero-based ladder position and `Operation` as the action for building the book: [official MarketDepthEventArgs documentation](https://docs.ninjatrader.com/ninjascript/marketdeptheventargs). The local price-keyed snapshot implementation is a candidate cause of stale levels; the capture alone does not establish the exact loaded observer source or prove that cause.

Open gates are `PONG_NATIVE_INPUT_CONTRACT_UNCOMMISSIONED`, `PONG_NATIVE_INTEGRATION_UNCOMMISSIONED`, and `PONG_BOUNDED_STOP_UNVERIFIED`. The native source repair/authentication and real complete-hour evidence, runtime/state-machine execution/reconciliation/checkpoint bridge, selector/start path, and independently enforced controller lease/deadline/closure path remain unfinished. No genuine native readiness, protection race, or hour-end proof is claimed for them.

The required termination contract remains T0 at armed-and-eligible readiness, immutable T0+3600 elapsed seconds, loss-of-controller entry fencing, retained protection, controlled owned flatten, signed flat/zero-orders reconciliation, and sealed closure. No background process may auto-arm or later start this unfinished candidate. Requested cohort LONDON is reporting metadata only; actual labels/timestamps are in the receipt. No session label changes strategy behavior.

The earlier diagnostic `native-input-probe-1043` had a Python serialization error (`mappingproxy` is not deepcopyable), so its empty raw journal is not usable source evidence. The corrected 1044 capture succeeded. Neither diagnostic created execution authority. All probe/listener processes ended normally; NinjaTrader was left connected.
