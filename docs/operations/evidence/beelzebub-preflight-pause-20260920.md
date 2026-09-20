# Beelzebub preflight safe-pause receipt — 2026-09-20

## Decision

`PAUSED_BLOCKED_DISARMED`

The pass was paused on operator instruction before the Sunday market reopen.
No activation or order authority was granted. The pass-owned BeezConsole host,
Pong production host, collector, listener transports, and ledger writer were
stopped through the authenticated graceful-shutdown route. The warm-start
fault was preserved. No risk state was reset, no recovery route was invoked,
and no restart or scheduled task was created.

## Binding at pause

- checkout: `C:\Users\atlas\Documents\Trader`
- branch: `codex/lane-iii-perpetual-corrective-20260906`
- commit: `1101980e21a1c74555c1eb141ab5362cdfe84e54`
- tree: `2b9e0a048dbd02bfcfa6e65fc86dd95196b3367f`
- selected profile: `BEELZEBUB_PONG_DISCERNMENT_V1`
- native instrument / canonical contract: `MNQ DEC26` / `MNQZ6`
- topology: `PONG_CONTRACT_TOPOLOGY_DEC26_V1`
- topology hash: `a339cb56a2dc1280272062d8bd73a4f85829f3ebf92bacc1a6256904fa87e5e7`
- account / class: `Sim101` / `LOCAL_SIMULATION`
- live capital: `DENIED`
- execution state before stop: `READY_DISARMED` / `DISARMED`
- position and owned orders before stop: `FLAT 0` / `0`
- realized / unrealized P&L: `0` / `0`
- FHB: absent (`fhb=null`, `fhb_freshness=null`)
- scheduler configuration: `PERIODIC_FLAT_PROTECTED`, `20 ms`
- scheduler active path before stop: `EVENT_DRIVEN_PROTECTION`
- periodic-flat evaluations: `0` (the 20 ms live path was not qualified)
- ordinary Pong acceptance floor: `2,111 records/s` (not proven by this pass)

The native producer was authenticated as session
`7188cfb06971417d9a6707e91080aad0`, observer source
`1825e9890fe54a71f9b495e778e07c9e523ca1b36f66569b21f9ca4549102b73`,
and loaded build
`ed4d7b62945afb003ad84a1b6d7594cbb52a997f010bc3e423131c981cd0c7ca`.

## Controlled stop and drain

- graceful shutdown requested with `scripts\stop_beezconsole.ps1 -Port 8090`
- host PID: `16536`
- parent watcher PID: `10200`
- shutdown completed: `2026-09-20T21:55:08.812831Z`
- final collector state: `STOPPED`
- final transport state: `DISCONNECTED`
- collector ID: `pong-collector-20260920T214912876586Z-643b8ba4adb2`
- enqueued / written / durable: `323 / 323 / 323`
- queue depth / overflow: `0 / 0`
- producer overflow / transport failures: `0 / 0`
- rejected / duplicate: `0 / 0`
- final journal bytes: `3,395,727`
- final journal chain tip: `9ef0b50138ef709d1ebda84d5afbfd92061dd982077880f930660bdfe35eebfc`
- final journal SHA-256: `ea9b546aeacea60ed3a8696dc1e1e1ba95a13575036195758b5961e01bc1419e`
- terminal collection-status SHA-256: `5a10a9e926020246f12874dfa88af7e6858c10c9cddd6d7e652b91687a053d04`

Preserved journal:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260920T214912876586Z-643b8ba4adb2\authenticated-observations.jsonl`

Preserved terminal status:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collection-status.json`

The selected 16:00–17:00 MDT interval remained `PENDING` at the stop because
the pass ended before its boundary. It contains zero target-interval records
and is not qualification evidence.

## Ledger checkpoint

An independent read-only incremental verification ran only after the host and
writer exited:

- verification ID: `preflight-pause-20260920T215508Z`
- completed: `2026-09-20T21:55:58.465072Z`
- result: `PASS`
- verified through sequence: `8193`
- captured tip / chain tip: `8193` / `0b9d0975f91e1374bef74f08ad2cbb5e7afec018722706d8f49328155ebf8d45`
- scanned rows: `2,908`
- chain / checkpoint / risk-continuity guard: `true / true / true`
- WAL bytes at start and end: `0 / 0`
- duration: `0.240554 s`
- verifier receipt SHA-256: `3c6fc85bca73b7e0011fd9f356b68f34c995f8150bfc35eb770f98335331fbc4`

Receipt:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification-latest.json`

The risk-continuity anchor remained at sequence `0`, last updated
`2026-09-20T21:16:07.451628Z`, and SHA-256
`b2a7c4b62da6c9b97f46c936c84a8f37a97ccf7356db09ef75ad258b19677c5f`.
No risk-state reset was performed.

## Preserved warm-start fault

Before shutdown, the host remained blocked by
`PONG_HOURLY_WARM_START_INCOMPLETE`. The last signed fault observation is
`nt-7188cfb06971417d9a6707e91080aad0-1434`, payload SHA-256
`53e302fdbbde1e0c2e365e1da805cd6c369c67ee94b6c550f0693f81d06691bf`.
It requested exactly two completed UTC hours, received `0`, and recorded gaps
at `2026-09-20T20:00:00Z` and `2026-09-20T21:00:00Z`. The fault observation
remains in the preserved authenticated journal; it was not cleared.

## Warmup and earliest valid qualification window

Implementation review establishes these separate dependencies:

1. Warmup requires exactly the two immediately preceding completed, UTC-hour
   aligned bars for `MNQ DEC26`. They need not have been observed live by the
   resumed process. At startup the native observer requests six provider bars,
   selects the required pair, uses provider lookup, and uses `DoNotMerge`.
2. Because Sunday trading reopens at 16:00 MDT / 22:00 UTC, the first pair of
   completed in-session hours is 16:00–17:00 and 17:00–18:00 MDT. A fresh
   request just after 18:00 MDT can therefore retrieve the pair ending at
   17:00 and 18:00 MDT, subject to provider availability.
3. The historical request can recover those hourly OHLCV bars after resume.
   It cannot recover the missed authenticated depth, quote, trade, sequence,
   queue, and durability evidence required for qualification.
4. A collector always selects the next full UTC hour after startup so it has a
   healthy pre-boundary witness. Therefore a manual disarmed restart after
   18:00 MDT and before 19:00 MDT selects **19:00–20:00 MDT** as the earliest
   valid live qualification interval.
5. The host must remain continuously connected across that interval, with one
   producer identity; zero gaps, duplicates, rejections, and overflow; all
   three market streams; coherent depth; at least eight classified trades;
   fully durable records; and its causal live hourly bar within the five-minute
   publication allowance. Certificate creation also requires the host's
   current live hour pair to match that eligible interval.

Accordingly, the earliest possible hour certificate is shortly after
**20:00 MDT on 2026-09-20** (the live hourly bar allowance extends to 20:05).
This is only the earliest collection/certificate window. It does not prove the
20 ms scheduler SLA, the ordinary `2,111 records/s` service floor, or activation
readiness, and it does not authorize paper execution.

No automated restart was installed. A future attempt must be a manual,
separately authorized, disarmed startup after the provider can return the two
completed warmup hours and early enough to establish the pre-19:00 collection
witness.

## Remaining processes and ownership

- `NinjaTrader.exe`, PID `17208`: operator-owned native platform process. It
  remains open and is not a pass-owned collection or execution process.
- BeezConsole/control-center PID `16536`: exited.
- watcher/launcher PID `10200`: exited.
- pass ports `8090`, `48135`, `48136`, and `48137`: no listeners.
- matching Beelzebub/Pong/preflight scheduled tasks: none.

## Administrative review

Caelum's bounded, source-and-receipt-only review is preserved at
`docs\administration\caelum\audits\20260920-1530MDT-preflight\CAELUM_PREFLIGHT_REVIEW.md`,
SHA-256 `44e3fff5e0a250de861aef4d377c626506fe379143a6e72ac3558ba3c378412e`.
Caelum performed no application edit, runtime control, ledger write, or
benchmark.
