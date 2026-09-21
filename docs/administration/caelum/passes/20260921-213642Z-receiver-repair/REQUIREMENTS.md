# Requirements and authoritative gate map

The user requested execution of the receiver repair handoff; referenced historical documents supply gate definitions, not independent new authority.

| Requirement | Acceptance evidence | State |
|---|---|---|
| Named receiver correction | Actual qualifying call graph, obligations of every removed/retained consumer, preserved ordered authenticated delivery and durability | ACCEPTED_SOURCE_OFFLINE_INTEGRATION |
| Valid controlled comparison | Same host, production socket/receiver wiring, clean valid workload, natural pacing and unchanged 2111/s sustained/burst contract; monotonic windows and queue/age/durable reconciliation | PASS controlled diagnostic; insufficient formal capacity; 2111/s HOLD |
| Semantics | Authentic records 146–150 and negatives, unavailable/no ordinary analysis/exact recovery, strict 10x10 | PASS affected semantic regression; no current natural-feed proof |
| Failure paths | Saturation, exception, disconnect/generation and stop/drain fail closed | PASS strict required-consumer/socket fail-stop, pause and drain regression; same-source commissioning UNPROVEN |
| Ordinary/protection | Healthy flat replay performs ordinary periodic work; valid protection retains priority and reason attribution | PASS isolated semantic test; benchmark protected, ordinary capacity UNPROVEN |
| Old-source isolation | Supported retirement/connection ownership; preserved old tail; no late rejection contamination or counter reset | HOLD_INITIALIZATION_UNPROVEN; no current commissioning attempt |
| Ledger capacity | Exact runtime path/headroom/reserve/degradation cause plus supported disposition | SOURCE_DISPOSITION_RECORDED; original subreason UNKNOWN; supported operational recovery NOT_RUN |
| Identity | Sealed commit and affected policy/artifact identities; native identity changes only if native source changes | PASS sealed/source identity and root raw-byte congruency; native unchanged; install/load NOT_PERFORMED |
| Gate A delta | Independent review of preceding evidence | HOLD_RECEIVER_CAPACITY_AND_INITIALIZATION_UNPROVEN |
| Conditional R2 | New Gate A delta PASS first; sole parent runtime operator | Not run |
| B / C / D | Required preceding gates PASS, same frozen candidate | Not run |

Authoritative original readiness document: C:/Users/atlas/Desktop/bis/Beelzebub_NoFHB_Qualification_Readiness_Context/Beelzebub_NoFHB_Qualification_Readiness_Handoff_SolHigh_Caelum.md, R2 lines 110–118, R4 128–134, B–D 309–347.

R2: receiver and collector ready before new producer; preserve old counters/tail. tools/commission_ninjatrader_observer.ps1 uses -RequireDistinctProducer -PreReloadReceiptPath <preserved receipt> -ExpectedInstrument 'MNQ DEC26' -EvidencePath <unique receipt>. Its default instrument is SEP26, so DEC26 must be explicit. CheckpointSeconds defaults 10 (allowed 5–60); compile timeout defaults 60 (10–300). This is a command template, not an executed command. The prior accepted commissioning contract is detailed in 20260921-135529Z/PASS_STATE.md lines 182–230: exact loaded temp-DLL build; distinct signed session/source/build/generation; two advancing durable checkpoints; zero absolute new-session overflow/transport totals; zero reject/duplicate/collector overflow deltas; no host fault/gap/queue overflow; exact available 10x10 or truthful unavailable; flat/disarmed/reconciled Sim101 LOCAL_SIMULATION, no orders. Clean corpus begins AFTER_CHECKPOINT_1; Gate B deltas begin CHECKPOINT_2. Do not move either boundary after a fault. Same-assembly menu/automatic reattach is not an accepted qualifying boundary. Old pending tail remains FAILED_UNKNOWN_PENDING_TAIL_NOT_DRAIN_PROOF.

No explicit attempt-start instant was located in these documents. Parent must conservatively record retry start no later than first receiving-runtime initialization for the retry, before reload; a preliminary natural-feed probe cannot evade accounting.

Gate B: predeclared fixed 3–5 minute full-host natural window after R2. Simultaneous exact depth truth, all three stream continuity and zero reject/loss/duplicate/reorder deltas, bounded queues/freshness, genuine eligible ordinary periodic work with separate protection, required durability/drain, safe account truth, exact identities, persisted start/fault/end and final ledger PASS. Truthful unavailable is allowed only under existing semantics. Abort first decisive failure.

Gate C: H:/Trader-pong-serial-measurement/docs/operations/pong-serial-acceptance-v1.json. At least three sustained and three burst repeats, every run >=2111/s; compute_workers=0; no loss/reject/overflow/duplicate; queues bounded and ages stable, complete durable drain and shutdown, signed-byte/journal verification and per-frame reference/hash/blocker/timestamp/decision parity. Instrumented or parity runs cannot replace uninstrumented rates. No new runtime authority is inferred from this older offline contract.

Historical workload implementation: H:/Trader-pong-serial-measurement/tools/pong_measurement.py due_times lines 426–432: sustained due i/2111; burst repeats 2 seconds at 4222/s then 2 seconds idle. Corpus H:/PongMeasurement/20260914/representative-signed-17h44-v2 contains 150016 records, including 16 history chunks; natural market receipt span 176.229832 s. Its manifest says PENDING_FULL_PATH_REPLAY and authentic-hour authority DENIED. Do not treat this historical corpus as currently valid without source/semantic review. The JSON contract has no fixed duration. Historical reproduction command is documented in pong-serial-measurement-20260914.md lines 408–413: python tools/pong_measurement.py replay --corpus <above> --output <unique isolated path> --mode sustained --rate 2111 --timeout 600, omit --timing for control, --mode burst for bursts, --parity separately. The tool is absent from current candidate checkout, so adapting it to current production wiring requires parent review; invoking the historical worktree does not test the candidate.

R4: 2016 completed current-source 5m bars in 16 ordered chunks (15x128+96), two immediately preceding UTC hourly bars under source policy, valid DEC26 bias, current Sim101/risk/ledger truth. Gate D: only after B/C and context prerequisites pass, fixed 20-minute disarmed natural rehearsal on frozen candidate, complete authentic window and unchanged gates; no splicing or qualification relabeling.

## Final disposition detail

| Requirement | Final disposition |
|---|---|
| Named correction/consumer contracts | ACCEPTED_SOURCE_OFFLINE; only market shadow excluded, paper/control/collector/host retained |
| Valid controlled comparison | Diagnostic exact-byte sustained/burst and authentic failed-prefix natural pair completed; useful narrow gain, insufficient2111/s; formal valid corpus series NOT_RUN |
| Semantics/failure paths | Accepted affected regression142 tests+36 subtests; strict socket fail-stop, no paused sink buffering, ordinary/protection counterexample coverage |
| Ordinary/protection | Healthy TEST ONLY bridge regression permits ordinary tick; production-like screens legitimately protected, no ordinary capacity proof |
| Old-source isolation | Source-backed diagnosis; operational same-source bootstrap UNPROVEN; affected transition held |
| Ledger capacity | Historical durable latch predates R2; current resource alone does not clear it; exact original subreason UNKNOWN; supported recovery pending |
| Identity | Source commit0cc1a865 and Python artifact ba242161...545b0; native unchanged; parent root raw-byte congruency PASS with preserved unrelated diff/status receipts |
| Gate A delta | HOLD_RECEIVER_CAPACITY_AND_INITIALIZATION_UNPROVEN |
| R2/B/C/D | NOT_RUN; one conditional R2 unused |

Final identity update: parent integrated source into root and confirmed exact measured qualifying artifact ba24216118cb7ad7a58e7093ddc36042fe5b6bb22f61a11f3ee63a77dd8545b0 after documented line-ending byte alignment; unrelated root diff/status receipts match. Installed/loaded runtime remains NOT_PERFORMED. Gate A HOLD is unchanged.

