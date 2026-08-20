# L3-B authority matrix

| Authority / capability | L3-B |
| --- | --- |
| Normalize MNQ market events | YES |
| Reconstruct quote state | YES |
| Reconstruct DOM state | YES |
| Record trade flow | YES |
| Compute mechanical flow/session measurements | YES |
| Produce replayable observations | YES |
| Store raw/normalized provenance | YES |
| Detect invalid, stale, gapped, recovering, incomplete data | YES |
| Interpret observations as bullish/bearish | NO |
| Construct hypotheses | NO |
| Compute Lane III confidence | NO |
| Emit trade signals | NO |
| Create execution intents | NO |
| Change risk limits | NO |
| Access broker or prop account | NO |
| Control copier/follower accounts | NO |
| Scientific authority / Phase E changes | NO |
| Live-capital authority | NO |

L3-B loads only after `require_l3a_manifest()` verifies the frozen L3-A constitution. It uses L3-A's MNQ root and refusal semantics, while deliberately not materializing `EvidenceObservation`: assigning an evidence family/expiry policy to an arbitrary observation is a downstream L3-C decision. L3-C must turn source-grounded observations into the existing L3-A evidence contract under a separately reviewed boundary; Phase B does not replace that contract or invent a parallel evidence model.
