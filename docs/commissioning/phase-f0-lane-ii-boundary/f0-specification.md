# Phase F.0 — Lane II constitutional boundary

Date: 2026-08-18

## Decision

**Yes. Lane II may begin isolated operational development while E.5 remains prospectively unresolved, but only behind strict capability separation.** F.0 establishes that separation. It creates no strategy, signal, execution adapter, venue order path, testnet path, or capital authority.

Lane I (`SCIENTIFIC_LANE`) remains the owner of E.5/E.6 scientific work. Its scientific-evaluation capability is protocol-gated; prediction, signal, execution, trading, and live-capital authority remain denied. Lane II (`TRADER_LANE`) is a distinct identity and every one of those capabilities is denied in F.0. Phase D is the separate `PHASE_D_EXECUTION_SOVEREIGN`; it retains the execution boundary and independent safety/risk lifecycle. Neither lane identity may be used in place of the other.

The checked-in machine-readable contract is [f0-authority-manifest.json](f0-authority-manifest.json). Its canonical hash is `f2c4df16e2815278c500ff406090886b2a32eb53c0751a2c8e4086d7a381ee41`.

## Constitutional rule

Scientific evidence authority and operational trading authority are independent capabilities. A hypothesis, model, result, or repository object has no operational authority merely by existing. A strategy object has no signal authority merely by existing. A trade intent request has no execution or live-capital authority merely by existing.

The F.0 package is isolated from both `src.phase_e` and Phase D execution transport. It stores and accepts only explicit provenance references and SHA-256 fingerprints—never raw scientific artifacts or outcome-bearing data. Unknown objects, mappings, callbacks, repositories, result objects, and inputs without approved source provenance are refused before their fields or methods are read.

## E.5/E.6 protected boundary

Lane II must not receive, read, derive from, or proxy any of the following:

- prospective outcomes, returns, P&L, effect estimates, p-values, confidence intervals, bootstrap distributions, interim performance, or conclusions;
- E.5/E.6 outcome readers, result records, scientific repositories, or sealed-artifact storage;
- maturity-derived performance information or any value computed from it;
- authority to mutate the E.5 protocol, hypothesis family, schedule, E.6 membership/timing, maturity rules, resolution metadata, or integrity/provenance semantics.

F.0 exposes no mutable reference to those artifacts and has no import path to the Phase E package. Existing E.5/E.6 enforcement remains the owner of those writes and reads. Deployment must additionally keep the sealed outcome repository in a scientific-only ACL/domain; a declared source label is not a replacement for infrastructure access control.

## Allowed future input classes

Only provenance-bearing references to these source classes may be supplied to the future F.1 commissioning seam:

- live public market data;
- live public wallet activity;
- Phase A/B/C operational observations;
- Phase D authoritative market timestamps;
- explicit operational indicators not derived from E.5/E.6 outcomes;
- configuration or risk-policy artifacts;
- current account or execution state; and
- independently approved operational strategy artifacts.

Each `OperationalInput` is immutable and has an identity, approved class, UTC observation time, source system, and content hash. The strategy independently declares its allowed source classes. Any omission, unknown class, or mismatch fails closed.

## Strategy and intent contract

An operational strategy must have a `trader-` identity, version, immutable artifact hash, and its own allowed-input declaration. E.5 hypothesis identifiers (for example, `wallet-action-gt-zero`) cannot occupy strategy identity fields. A possible future F.1 strategy may be inspired by research, but it must be separately versioned, registered, and commissioned.

The maximum pre-Phase-D output type is an immutable `TradeIntentRequest` containing strategy ID/version/identity, symbol, direction, a notional ceiling, creation and expiry times, input and authority-decision hashes, and exit/risk policy references. Both `execution_authority` and `live_capital_authority` are hard-coded `false`. F.0's intent factory always refuses because no strategy is registered with signal authority.

There is no order signing, submission, adapter, credential, testnet, or mainnet code in F.0. Direct execution requests unconditionally refuse. A later Phase D bridge must be separately commissioned and must revalidate the request under Phase D risk, lifecycle, reconciliation, and venue-exposure controls.

## Replay and successor rule

Every decision is canonical-hashed from the frozen manifest, exact strategy identity/version, and immutable input provenance hashes. The same inputs produce the same allow/deny decision and decision hash. F.0 itself has no permitted decision path: `TRADER_LANE` signal, execution, trading, and live-capital authority are all denied.

Changing an authority decision, admitting a strategy, adding an input source, or connecting an intent to Phase D requires a successor commissioning phase. It must not alter the frozen E.5/E.6 protocol or use prospective E.5 results as its authority basis.
