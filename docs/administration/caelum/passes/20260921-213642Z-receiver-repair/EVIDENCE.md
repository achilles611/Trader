# Evidence register

Observed read-only in isolated candidate worktree on 2026-09-21. All earlier runtime receipts are historical failure evidence, not current qualification.

| Artifact | SHA-256 / finding |
|---|---|
| H:/PongPeriodic/NoFHBQualificationCorrective-20260921/R2-commission-0518-20260921T1709Z.json | 61f49a152e545587b72834afdd35fbf2ad788ba9245f1eec51ec5a19bea6df79; rehashed match; verified=false; old reject delta 2623; both checkpoints capacity DEGRADED |
| Same root / R2-bounded-natural-diagnostic-20260921T171332Z.json | 47b37cb3847f128a7c2a90d7c36ae65645c2cd5a22b9edde3d30e5d56f5ffcea; rehashed match; service deficit/overflow/gap receipt |
| H:/Trader-pong-serial-measurement/docs/operations/pong-serial-acceptance-v1.json | b502a94820c3c3f10fc8b4d8e8c618027394261e6bc828e5789cf56d4a6266fd; rehashed match |
| Prior five Caelum records under 20260921-163411Z-no-fhb-corrective-readiness | Complete files read. Prior PASS accepted 767f850 source only; R2 failed; no performance correction had been spent |
| Candidate Git | 438e081b6017c0779e142b8389835f6d00113e53; 767f850 ancestor check exit 0; clean at intake |

Historical metrics cannot be subtracted across unlike windows: native elapsed 75.2468 s, delayed HEALTH timestamp 17:10:40.907Z, API response 17:12:42.145Z with 23 s request latency. Receiver cumulative/extraction and accepted-span rates differ. Robust observation is native enqueue > dequeue and long socket stalls plus dominant synchronous fanout; removal alone was an intake hypothesis; the controlled evidence below demonstrates a narrow improvement with a persistent service deficit.

At intake the separate attachment copies were not checked; repository prior records were read as authoritative local records. Parent later verified root copies against both supplied hashes as recorded below.

Initial synthetic diagnostic receipts independently read and hashed (not qualifying comparisons because timestamp bytes differ):

| Receipt under N:/Beelzebub/receiver-repair-evidence | SHA-256 | Result |
|---|---|---|
| synthetic-2111-baseline/result.json | 64fb5529d837db1256822163f0da9781cb0ee00801396f266270d30b92384291 | 361.215/s; lag 22.9477s; max socket write 4.32623s; total socket writes 20.4616s; paper mean 1.09323ms; shadow mean .352948ms |
| synthetic-2111-candidate/result.json | 9a9bd075757b37d3a8ca7f253d37e37fa7532df2af855d62e931bd1d1851fb52 | 409.655/s; lag 19.6742s; max socket write 3.26725s; total socket writes 18.1427s; paper mean 1.08854ms; shadow skipped |

Both reconcile 10000 ordered records across expected, receiver, paper and collector. Candidate collector enqueued/written/durable 10000/10000/10000, reject/duplicate/overflow 0, high-water 4626 and maximum queue age 12469ms, stopped/thread false; listener stopped, 10000 accepted, zero reject/duplicate. Offline ledger shutdown clean, writer/checkpoint stopped, durable/expected tips equal. None of this proves current natural producer service or healthy ordinary analysis. The host has no execution transport; result explicitly says healthy_flat_analysis_proof=false and qualification NOT_QUALIFIED. Capacity release remains HOLD.

## Final controlled diagnostics (source frozen at 0cc1a865)

Caelum independently read and SHA-256 checked the following parent-produced receipts. All paths below are under N:/Beelzebub/receiver-repair-evidence/.

| Result | Rate / end lag | Max socket write / total write time | Collector max queue age | SHA-256 |
|---|---|---|---|---|
| sealed-sustained-baseline/result.json | 343.443/s /24.380s | 3.021s /19.939s | 5.250s | 135ecf3688e8e9792d6a72ccb4d16c3485fa42fb693455fcc715ebd58bdf3295 |
| sealed-sustained-candidate/result.json | 409.547/s /19.681s | 4.243s /18.208s | 2.437s | a4ed8c888c075af21833de73732d2a1f4540bb24561dee6c9d4bca1caf7d421f |
| sealed-burst-candidate/result.json | 390.371/s /21.248s | 3.271s /17.327s | 6.797s | c8b6a5187307b9931392ae7996a97e29406203e8d8ecd1f5d1855eb3592b3119 |
| sealed-natural-baseline/result.json | 348.696/s /6.101s | 1.797s /3.798s | 1.266s | 98ede89e89de5e9d43f7327856f8513e867bc2ed117ee6d598fe18a174364cb8 |
| sealed-natural-candidate/result.json | 441.001/s /3.700s | 1.308s /1.600s | 1.594s | d9f54b1c1ab852de94e32a01b8ca86ebfbe52a71046a0a423e75b2112258eb63 |

Shared synthetic fixture shared-synthetic-10000.jsonl SHA-256 126bd492774b6fcc43774bee46bd6ba457276ae235831d0a20ace7653b4930db, 17097467 bytes; exact same bytes supplied to both routes. Synthetic mix DEPTH8000/QUOTE1000/TRADE1000 differs from formal contract corpus. Each synthetic run received/paper-delivered/collector-durable10000 with matching ordered wire hashes. Historical natural pair used identical4000-record prefix SHA cbb452bd8a38206cea9c84b602a2385b9a12edd533d962215bdbd4fb81b258c2 and tip328fab1f813eee2fbd3fb0c48f18294e2613b0116f7ff318cd5b49ce95f3d5ca; all4000 ordered/durable, zero collector reject/overflow. That prefix remains AUTHENTIC_HISTORICAL_FAILED_EPOCH_DIAGNOSTIC / INELIGIBLE, not a clean qualification corpus.

Frozen receipt source SHA-256: control_center.py41d0691e7a1dea922a7f599cbcd3b14721d49c27201b52f8cd97d41cd6077fe8; ninjatrader_commission.py f719cf252265e5a12532f08c820d99f366b3fb32c37891e6bd732d2cb5aa0237; runtime.py507d225855c5250c5522957f4e0bd505a833b4bc7b6f343a60e5a9cb90330ced; harness089b87aec79849dc37c72a7a68264c02ec49d6bfcbf3f346c02a476fb09f094b. The baseline changes dispatch selection while using identical frozen modules, not a separately checked-out old executable. Pair claims are limited to that production-route contrast.

Ordinary periodic completions remain0 in all screen receipts. Synthetic protection10000 each; historical protection3999 each, justified by RUNTIME_BRIDGE_UNAVAILABLE. Sustained host max queue ages baseline5.125s /candidate4.078s, burst4.250s. Natural pair host queue high-water309→881 and max age.875s→1.766s: faster ingress can increase downstream queue burden. Final queue drain and these finite samples do not prove sustained slope stability. Fixed timestamps and historical source ages are not current freshness evidence; native enqueue/dequeue/age are unavailable in this offline socket test, and no live-native value is synthesized.

Affected test receipt affected-tests.txt SHA-256 6bdf33a08a0cfd2b22476d03354f33ecd89ddcb9fa0cada3fd4ca0edf1f78f3d; independently read142 passed,36 subtests passed in22.99s. Test execution belongs to parent/owners, not Caelum.

Attachment hash disposition supersedes initial unchecked note: parent rehashed original root prior ACCEPTANCE.md=a9933029b51d603920356467c99b8453f46e81f3b7ab46e18af18539d90aaab9 and PASS_STATE.md=23f67947e1718cbd98ba47c0ade9e0dc0529948d931dcf22b779d7550748fbbb, matching handoff attachments. New worktree byte hashes can differ through checkout line endings; no original evidence mismatch is claimed.

## Final receipt index and integration preservation

All following receipts are in N:/Beelzebub/receiver-repair-evidence/ and were independently read/hashed by Caelum:

- comparison-summary.json: 53542e6c7f0b38beb42494c81a3c87e616b29a7291c220beb8f40c901060f7a9. Includes each run, host-telemetry receipt/hash, limitations, source hash graph, policy hash333f3c07bc6c7340e67b9238532eb1d9538c4e9b22644f6ecd23d4d49c8773e0 and risk hash404251db8a372641f3a25d7edbb921cf5da6a4277bc3a6fd2fab5a2768269962.
- final-process-state.json: dd8382f98f678e60130d314e265bae3623c2a82103a0a684800726db1efab980. Point-in-time observation2026-09-21T21:55:31.4737056Z; no target listener/Python, existing NinjaTrader17208 untouched; current account UNKNOWN.
- byte-identity-alignment.json: 812d506ce2bff213aebb8274932876dcb0d83dd385251168eb04da9d89784e9a. Parent normalized-identical checked-out bytes to match measured source artifact after fast-forward; no native install.
- root-diff-before-integration.patch and root-diff-after-integration.patch: both d65afd4118ebf02316277bf758c7a68a390831abe413eefa49aaa508c72150e1.
- root-status-before-integration.txt and root-status-after-byte-refresh.txt: both037ec91c9ce320c4f8460f5f93c87021cec7f414a5a8795541213198ab8c11e5. Unrelated user dirt preserved.

Exact affected command, executed by parent from isolated worktree with TEMP/TMP=N:/Beelzebub/scratch/receiver-repair:

```powershell
C:/Users/atlas/Documents/Trader/.venv312/Scripts/python.exe -m pytest tests/test_receiver_repair_dispatch.py tests/test_receiver_repair_safety.py tests/test_lane_iii_phase_f3.py tests/test_l3g_paper_runtime.py tests/test_l3g_paper_control_center.py tests/test_l3g_pong_production.py tests/test_l3g_pong_collection.py tests/test_l3g_pong_periodic.py tests/test_l3g_pong_scheduler.py tests/test_l3g_starvation_hotfix_load.py -q
```

Result:142 passed,36 subtests passed in22.99s. Caelum independently reviewed receipt; Caelum did not execute tests. Formal capacity series, qualifying native R2 and Gates B–D remained NOT_RUN.

Post-integration root regression receipt independently read: integrated-root-tests.txt at N:/Beelzebub/receiver-repair-evidence/, SHA-256 f1ca2ef48ae61c3d147d63d240253a61eaca773043b31466fbc98bfc73527361. Parent executed the same repository Python with `-m pytest tests/test_receiver_repair_dispatch.py tests/test_receiver_repair_safety.py -q`, TEMP/TMP=N:/Beelzebub/scratch/receiver-repair, after root fast-forward and byte identity alignment. Result: 15 passed, 12 subtests passed in 2.00s. This verifies the integrated scoped source; it does not release Gate A.

The timed baseline was reconstructed by selecting `qualifying_architecture=None` in the same frozen integrated artifact. Candidate selected the exact qualifying architecture; other components were identical. No distinct deployed baseline Git/runtime build was compared.

