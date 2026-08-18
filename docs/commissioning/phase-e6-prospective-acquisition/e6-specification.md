# Phase E.6 prospective acquisition specification

Date: 2026-08-18

Frozen scientific dependency: Phase E.5, not modified by this phase.

- Protocol ID: `e5p-ae597d81614b76feba54168141de6a73`
- Protocol hash: `ae597d81614b76feba54168141de6a738876107639213a56a1c1aaa21c17c27f`
- E.5 source commit: `ed0c8f95c369364662a33728093cd8b2e916a6df`

## Scope and authority

E.6 implements the operational acquisition boundary only. `PhaseE6Acquisition` accepts predictor-side candidates from the frozen `NEW_E5_PROSPECTIVE_ONLY` source partition, records resolution timing metadata, and retains no return, P&L, outcome, effect, p-value, bootstrap, or inference interface.

Its trading, execution, signal, and prediction authorities are all permanently `false`; its reported trades placed count is zero. It neither starts E.7 nor invokes E.5's outcome capability.

## Frozen protocol verification and schedule

Every operational entry point re-reads and validates the on-disk E.5 artifact. It refuses startup or further action unless the schema, protocol ID, and exact protocol hash match the frozen values above. The acquisition database stores that immutable document and materializes exactly the 60 blocks returned by E.5's existing deterministic schedule function. Block ordinal has a database constraint of `0..59`; schedule fields and block hashes have an immutable-trigger guard. There is no replacement, extension, or block-creation API. The hard stop remains `2027-12-25T00:00:00Z`.

## Lifecycle and clock semantics

The only normal state path is:

`scheduled -> open -> acquiring -> sealed -> awaiting_resolution -> finalized`.

Opening requires frozen wall-clock start `<= now <` frozen wall-clock end. An attempt after the scheduled end durably records `acquisition_failed` as a missed block and then raises; it does not recreate the interval. Sealing is allowed only at or after the scheduled end, is atomic, and makes membership immutable. Recovery serially marks missed scheduled blocks or seals an expired open/acquiring block using its original scheduled cutoff; it never restarts a 30-minute interval or recalculates membership.

## Membership, provenance, and integrity

Each candidate and decision is immutable and audited. Admitted membership is a separate immutable table linked to its candidate decision. Admission uses only the already-frozen E.5 predictor-side fields and enforces:

- exact E.5 observation schema and protocol hash;
- new prospective source namespace (not historical E.4 or synthetic fixtures);
- anchor inside the scheduled 30-minute interval and exposure no later than anchor plus ten seconds / the fixed block envelope;
- E.5 pre-anchor symbol eligibility flag;
- E.5 salted wallet cohort matching;
- global first-admitted wallet and source-event uniqueness;
- no cross-block transaction, endpoint-family, campaign, or same-symbol overlapping exposure relation.

Within-block relations are retained; E.6 adds no stricter exclusion rule. Later-discovered cross-block relations are append-only integrity events and may move a nonterminal block to `contamination_detected`; neither side is deleted.

SQLite `BEGIN IMMEDIATE`, foreign keys, unique constraints, compare-and-swap block transitions, and idempotent candidate identities make concurrent/restarted processing behave as one authoritative sequencer. Audit events include schedule materialization, transitions, candidate decisions, admissions, resolution metadata, maturity events, recovery, and integrity failures. `replay_hash()` canonically hashes fixed schedule and persisted acquisition decisions.

## Resolution and outcome blindness

`ResolutionMetadata` can contain only an observation ID, qualifying event timestamp, ingestion timestamp, and structural-unavailability flag. It has no outcome-value field. E.5's existing outcome-free classification function turns this metadata into maturity status after sealing. Membership and maturity are stored separately. After finalization, incoming resolution metadata is retained separately as append-only late evidence and an integrity event; it cannot reopen maturity or create a member. Conflicting pre-finalization metadata is preserved as a resolution-integrity failure.

Operational `status()` exposes only protocol identity, block lifecycle counts, outcome-blind observation/maturity counts, hard stop, integrity health, and access accounting. Its scientific-evaluation count is fixed to zero and reserved test-query count is fixed to zero. No performance dashboard or interim scientific result exists.

## Deployment boundary

This module intentionally does not auto-create a production control database or start a block. A production deployment must supply a dedicated acquisition database and keep the E.5 outcome repository under its separately sealed capability/ACL. Any code change during live acquisition that could affect membership, timing, relation checks, sealing, maturity, resolution, or provenance requires scientific review before deployment.
