# Requirements matrix — no-FHB corrective readiness

States distinguish implemented, tested, integrated, installed, loaded,
runtime-exercised, and accepted. A source PASS cannot substitute for a current
loaded/runtime PASS.

| ID | Requirement | Intake state | Acceptance / next evidence |
|---|---|---|---|
| P0.1 | Exact repository and prior evidence truth | **PASS observed** | HEAD `2e2c743e...`, sealed ancestor `c76cccc...`, unrelated dirt preserved, receipts rehashed |
| P0.2 | FHB absent from qualifying import/hash/runtime path | **PASS source-observed** | Qualifying identity says archived/disabled; FHB source absent from loaded hash list; qualifying constructor rejects FHB config/request. Reverify after integration |
| P0.3 | E4 remains parked and E1–E7 not reopened without trigger | **PASS intake / ongoing** | No E4 source in qualifying graph; any reopen requires the recorded production trigger and separate approval |
| P1.1 | Explicit unavailable unresolved Remove is fail-closed | **PASS SOURCE/OFFLINE** | Authentic records 147/149 advance source cursor, invalidate canonical readiness, keep input error null and emit no analytical evaluation; no null-price post-book check or fabrication |
| P1.2 | Unavailable state is recoverable, not silently accepted or terminal forever | **PASS SOURCE/OFFLINE** | Authentic records 148/150 exact coherent 10x10 recover after 147/149; independent terminal continuity tests remain terminal |
| P1.3 | Authentic 146–150 production-host replay | **PASS SOURCE/OFFLINE** | Exact preserved five-record fixture exercises actual production host: available -> unavailable/no analysis -> exact recovery -> unavailable/no analysis -> exact recovery. Supersedes unsupported record-149-first attribution |
| P1.3a | Available exact 10x10 mutation may resolve outside retained top ten | **PASS SOURCE/OFFLINE** | Narrow fix accepts only integer position >= retained length and resolved price strictly worse than retained worst; signed exact 10x10 snapshot recovers with no fabricated visible mutation |
| P1.4 | Required negative semantic coverage | **PASS FOCUSED** | New and existing tests cover unresolved zero-price unavailable; known resolved Remove; unavailable 10x1, 1x10, crossed; available partial fatal; malformed available; gap; generation; reset; recovery; no analytics while unavailable |
| P1.5 | Broad depth contract unchanged | **PASS SOURCE/OFFLINE; REVERIFY COMPOSITE** | Exact available 10x10 remains mandatory; no hidden reconstruction or source continuity weakening; only demonstrably outside-retained Add/Update is exempted from membership check |
| P2.1 | Aggregate native callback telemetry | **PASS LOADED/RUNTIME OBSERVED** | Current signed HEALTH exposed DEPTH/QUOTE/TRADE rates, truthful current-thread CPU-cycle support/counters, wall totals/max and publication attempts under source `051808...`; no per-event logging |
| P2.2 | Aggregate native outbound queue telemetry | **PASS LOADED/RUNTIME OBSERVED** | Capacity/current/high-water, enqueue/dequeue/overflow/oldest and in-flight age exposed; current diagnostic showed 27,400/32,768 and 35.626 s oldest |
| P2.3 | Aggregate sender telemetry | **PASS LOADED/RUNTIME OBSERVED** | Current sender rate/write/reconnect/failure metrics exposed; 70.058/75.247 s in writes and 3.332 s max located backpressure |
| P2.4 | Aggregate receiver telemetry | **PASS RUNTIME OBSERVED** | Bytes/frames, strict decode/reject/duplicate, truthful non-queue framing occupancy and fixed p50/p95/p99/max observed; extraction 358.345/s |
| P2.5 | Aggregate host telemetry | **PASS RUNTIME OBSERVED** | Ingress/admission, queue/age, processing, scheduler, protection, evaluations and collector metrics persisted separately in final/diagnostic receipts |
| P2.6 | First under-service/stall boundary classified | **PASS OBSERVED / RUNTIME** | Current identity: ~750 callback/publication/s > ~385.8 dequeue/socket > ~358.3 receiver extraction; sender write occupied 70.058/75.247 s, max 3.332 s; native queue aged 35.626 s and overflowed. Receiver/downstream synchronous fanout is the measured backpressure boundary |
| P2.7 | Bounded measured correction only | **BLOCKED ON P2.6** | Smallest implicated correction, every record/sequence preserved, no wait-to-fill, no speculative duplicate research, <=1 initial + <=1 corrective |
| P3.1 | Composite corrective candidate | **PASS FROZEN SOURCE** | `767f850...` contains P1 correction + required telemetry and no speculative performance repair; FHB/E4 excluded |
| P4.1 | Focused and affected tests | **PASS** | 221 +62 subtests; exact ledger 9/9 on adequate N: temp; compiler/order/wrapper PASS. C: low-disk refusals preserved as environment evidence |
| P4.2 | Identities recomputed and congruent | **PASS SOURCE** | Observer `051808...2a14`; artifact `103d81...2802`; native SHA hashes recorded; all old identity-bound evidence ineligible |
| GA | Gate A delta | **PASS** | Independent commit/diff/test/compiler/identity/FHB/E4 review complete for `767f850...`; one fresh R2 attempt released to Sol only |
| R2 | Fresh receiver-first clean commissioning | **FAIL / HOLD** | Distinct identity and safety passed; +2623 rejects prevented clean baseline, then +2590 overflow, DEPTH gap 347 and ~145 s source age failed hard conditions. Conditional retry only after named correction |
| GB | 3–5 minute natural proof | **NOT RUN / BLOCKED** | R2 PASS first; all natural load, periodic ratio, queue/freshness/durability/safety metrics and simultaneous acceptance conditions |
| R4 | Current contextual readiness | **NOT RUN / BLOCKED** | Exactly 2,016 5m bars in 15x128+96, two immediately prior UTC hours, current DEC26 bias, flat/disarmed/reconciled/current risk/ledger |
| GC | Formal capacity >=2,111 records/s | **NOT RUN / BLOCKED** | Existing H: serial contract; three sustained + three burst, each zero-loss and parity clean; no averaging or corpus change |
| GD | 20-minute disarmed rehearsal | **NOT RUN / BLOCKED** | B/C/R4 PASS, exact frozen candidate, full authentic window, every hard gate clean |
| FINAL | `READY_FOR_QUALIFICATION_RUN` | **NOT EARNED** | A–D PASS on one frozen candidate plus exact package/runbook; still no qualification bypass, arming, order, or live authority |
