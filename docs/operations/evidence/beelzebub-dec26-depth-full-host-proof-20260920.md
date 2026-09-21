# Beelzebub DEC26 depth/full-host ingress proof — 2026-09-20

## Verdict

`FULL_HOST_PROOF_BLOCKED_DISARMED`

Native `MNQ DEC26` depth transport is proven and is not reopened here. The
full-host proof admitted and durably captured authentic depth, quote and trade
records, and the DEC26 observation-sink policy-identity defect did not recur.
The proof is not accepted as a complete DEPTH-incident resolution because its
lossless-ingress conditions failed: the host collector queue grew during the
window, one parser rejection was preserved, source-sequence gaps were present,
and the producer overflow watermark advanced. No qualification, activation or
order authority was exercised.

## Bounded source repair

Commit `1e0e46d503bfbcfb014cee8812929f1dee05d24d` recognizes only the compiled
SEP26 and DEC26 policy identities. It repairs the exact failure in which a
valid compiled DEC26 Pong policy raised `ValueError("Paper decision policy
identity is required.")` during observation ingestion. It does not alter
strategy thresholds, scheduling, warm-start rules, topology, risk limits or
activation authority.

Focused verification:

```text
python -m unittest tests.test_l3g_versioned_operational_contract \
  tests.test_l3g_paper_contracts tests.test_pong_contract_topology -v
Ran 13 tests in 1.802s — OK
```

The live proof then wrote zero `INCIDENT_OBSERVATION_SINK_FAILURE` records and
zero records containing `ValueError` after prior ledger sequence `155697`.

## Sustained native/listener proof

The observe-only listener ran continuously from `18:21:46.792` to
`18:24:46.809 MDT` and accepted:

- `160,995 DEPTH`
- `8,364 QUOTE`
- `12,994 TRADE`
- zero listener/parser rejections
- exact `MNQ DEC26`, producer session
  `7188cfb06971417d9a6707e91080aad0`, source generation `31`
- authentic positional snapshots retaining 10 bid and 10 ask levels

Summary:
`docs/operations/evidence/depth-recovery-20260920T1822MDT-sustained/summary.json`
(SHA-256 `D9A1A6E6EFFB72700AD89CC7C60E26F4A891A39BD5AAAB4442139A7C5921637A`).

Raw observations:
`docs/operations/evidence/depth-recovery-20260920T1822MDT-sustained/native-observations.jsonl`
(SHA-256 `C945729FBFD0D76D1B448680672A35B2712BDD1C0C602263781D811565CB95C2`).

This proves the native callback, AddOn construction/emission, loopback
transport, signature/parser boundary and observe-only listener admission. It
does not substitute for the full-host proof below.

## Full-host proof

The first explicit-maintenance start safely refused before adoption because
the launch shell lacked both DEC26 topology selectors and therefore defaulted
to SEP26. The supported retry set both process-local selectors to `DEC26`,
retained ordinary Pong, FHB off, `PERIODIC_FLAT_PROTECTED` at 20 ms, bounded
activation off, and reused the preserved ledger and epoch. The host loaded
commit `1e0e46d503bfbcfb014cee8812929f1dee05d24d`.

The proof ran approximately `18:27:43–18:33:28 MDT`. Its sealed collector was
`pong-collector-20260921T002743247233Z-12827bc9155e`.

Accepted and durable stream facts:

- total enqueued / written / durable: `149,809 / 149,809 / 149,809`
- `DEPTH / QUOTE / TRADE`: `137,150 / 5,294 / 7,365`
- contract mismatches: `0`
- maximum retained depth shape: `10 bid + 10 ask`
- explicit unavailable/partial depth records: `6,236`
- ledger warmup resets caused by `MALFORMED_DEPTH_MUTATION`: `631`
- duplicate/out-of-order observations: `0`
- collector queue overflow: `0`
- parser/admission rejection: `1`
  (`MALFORMED_PROVIDER_PAYLOAD: bridge_json`)
- source-sequence gaps: DEPTH `99,660`, QUOTE `4,217`, TRADE `6,489`
- producer queue-overflow watermark: `101,147 → 212,146`
- producer transport-failure watermark: `273,718 → 292,150`
- maximum sampled host collector queue: at least `7,430`; it did not drain
  while admission remained open, but controlled shutdown drained it to `0`
- live queue capacity: `262,144`; host queue overflow remained `0`

The ledger observation sink did accept the repaired DEC26 policy identity:
`1,069 OBSERVATION_ENVELOPE` records were written, with zero sink-failure or
policy-identity `ValueError` records. This does not cure the source gaps or
backlog.

Final collector status:
`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collection-status.json`
(SHA-256 `E0F7DAB26C6DE4C6AA7FEB472190A2E738B2674E4928F4C6F7257AF61BB28506`).

Sealed collector journal:
`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\pong-authentic-hour\collectors\pong-collector-20260921T002743247233Z-12827bc9155e\authenticated-observations.jsonl`
(SHA-256 `75BC11A743ED468C29E9E493948B815CDC610352D7CAB2227F4ABB393D311B9C`).

## Account, authority and shutdown

The fresh native reconciliation at `18:27:43 MDT` reported exact
`Sim101 / LOCAL_SIMULATION / MNQ DEC26`, position `0`, working orders `0`,
working entries `0`, complete position/order snapshots, no foreign activity
and protective stop `NONE`. The proof recorded no entry/order/execution/fill
kind. Collector state remained `paper_execution=DISARMED`,
`live_authority=DISARMED`, and `live_capital=DENIED`.

The first graceful-stop client timed out while the overloaded HTTP loop was
unresponsive, but the authenticated request was accepted and completed. The
collector drained and fsynced all admitted records; the paper ledger completed
controlled shutdown with admitted and durable tips equal. Pass-owned PIDs and
listeners `8090`, `48135`, `48136`, and `48137` are absent. Operator-owned
NinjaTrader PID `17208` remains running and was not restarted.

Post-stop incremental verification passed through sequence `157479`, tip
`5f897aa358e3f8a83b78bdff1c456858f3976fa17d52af44cbac2847d147423b`,
with chain, checkpoint and risk-continuity guard valid and WAL `0`:

`N:\Beelzebub\runtime\profiles\beelzebub_pong_discernment_v1\runs\20260920T211544Z-ca2ceba7da11\audit\ledger-verification\20260921T003410292692Z-depth-recovery-full-host-stop-20260921T0034Z.json`
(SHA-256 `B1905F44B6CFCD09592B1F244D0A96E787BA8C31993ADCCB9B09D8CD90320F4F`).

## Release condition and next blocker

Do not restart qualification from this evidence. Release requires one fresh,
brief, disarmed full-host natural-feed proof with the already-proven native
stream in which:

- parser/admission rejection is zero;
- source-sequence gaps and duplicate/out-of-order counts are zero;
- producer overflow and transport-failure watermarks do not advance;
- the collector queue remains bounded and returns to zero without stopping
  admission;
- valid DEC26 10+10 depth, quote and trade records advance and become durable;
- the repaired observation sink remains free of policy-identity failures; and
- flat Sim101 reconciliation and zero orders/commands are fresh.

The next actual blockers are full-host admission/durability continuity under
the natural observed depth rate and the admitted malformed-depth mutations,
not native depth availability. No threshold, periodic, warm-start, FHB or
activation work was performed.

Caelum performed a bounded read-only source/evidence review and changed no
files, processes, configuration or ledgers. Its historical lifecycle concern
does not override the operator-directed and freshly measured native transport
proof; the full-host continuity failure above is the current release blocker.
