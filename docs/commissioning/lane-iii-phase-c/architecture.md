# Architecture

```text
canonical L3-B event + matching L3-B PipelineResult
                         |
                         v
                   HypothesisEngine
                     |          |
                     |          +-- explicit quality gate / rejection record
                     v
              immutable EvidenceObject
                     v
       max-within-family FamilyContribution
                     v
      relative-support ConfidenceAssessment
                     v
       concurrent HypothesisRecord lifecycle state
```

The engine accepts one declared `MarketDataSource` and one concrete CME MNQ
contract. It imports only L3-A contracts and L3-B observation types. There are
no imports from Lane II, Phase E, copy-trading, transports, network clients,
or account code.

The selected initial deterministic evidence selectors are deliberately narrow:
position versus session VWAP; fixed event-count range expansion and reclaim;
signed aggressive-flow imbalance; effort-versus-result; mechanically
classified book replenishment/pull; session phase; and explicitly-vintaged
derivatives context. Selectors are interfaces, not strategy rules.
