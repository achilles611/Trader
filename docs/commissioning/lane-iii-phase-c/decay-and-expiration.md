# Decay and expiration

L3-C uses discrete relevance expiry rather than arbitrary continuous decay.
The versioned defaults are: structural 90 seconds, flow 30 seconds, resting
liquidity 20 seconds, timing 5 minutes, and derivatives context 1 day. These
are time-to-live semantics for observation relevance, not fitted parameters.

At each canonical event, and on explicit `advance(as_of, pipeline)`, the
engine uses the supplied evaluation timestamp and L3-B staleness call. It does
not read the machine clock. Evidence becomes `EXPIRED` at its exact expiry
boundary, or `UNUSABLE` when the relevant L3-B source becomes stale, gapped,
recovering, incomplete, or invalid.

Hypotheses expire when valid impacts disappear. Fresh evidence can extend an
active record only to the configured idle lifetime and never past its maximum
lifetime. This prevents a short-horizon interpretation surviving merely
because no replacement event arrived.
