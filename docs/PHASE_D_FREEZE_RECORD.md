# Phase D freeze record

Status: **frozen as the Evidence Foundation**

Baseline: `93206d3dc9ca780e1d6a58994a4adb7cb9d6a11a` — `Add D7 data
ignition commissioning` on `codex/phase-d7-data-ignition`.

## Audit conclusion

The baseline commit was fetched from `origin` and verified before this record
was created. The D.6/D.7 targeted regression suite passed: 27 tests covering
the durable scientific queue, D.7 acquisition/coverage/corpus path, existing
scientific object guards, and D.5 design guardrails. No unresolved medium or
high defect was found in the D.7 evidence contract.

The commissioning record remains the authoritative record of the initial
bounded campaign:

- official requester-pays source, checksum, and archive provenance are in
  `PHASE_D7_COMMISSIONING_RECORD.md`;
- coverage is `PROVEN_COMPLETE` for the declared hour;
- raw normalized evidence is immutable and D.7's corpus snapshot is
  fingerprinted;
- public observation is read-only and no process was retained after the smoke
  test.

## Frozen responsibility

Phase D acquires reality, proves and preserves provenance, normalizes evidence
deterministically, and exposes reproducible corpus snapshots. It owns the raw
observation and source-manifest contract, not a claim that those observations
predict future outcomes.

The existing D.6 compatibility worker and its database records are retained as
part of the `93206d3` baseline. They are not an extension point for new
scientific-intelligence work. New experiment contracts start in
`src/phase_e/`, bind to D snapshots by fingerprint, and write only their own
`phase_e_*` ledger tables.

## Explicitly outside Phase D

Do not add hypothesis generation, predictive feature selection/ranking,
strategy discovery, fitting or selection of predictive models, statistical
promotion, forward scoring, trade signals, capital allocation, or execution
authority to Phase D. In short: **D determines what happened; E evaluates
whether it predicts anything.**

## Allowed changes after the freeze

Only these changes may modify D without opening a new phase-boundary decision:

- correctness, security, compatibility, and data-integrity fixes;
- tests that preserve D's semantic evidence contract;
- operational fixes necessary to preserve the existing contract.

Any claimed defect must identify the affected D provenance/evidence invariant,
include a regression test, explain why a new D baseline is necessary, and
record the replacement baseline commit. Phase E outcomes, P&L, or an
unfavorable experiment are never a reason to alter D evidence retrospectively.
