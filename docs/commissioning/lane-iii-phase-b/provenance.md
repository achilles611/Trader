# Provenance and persistence

L3-B maintains two configurable append-only JSONL streams:

| Stream | Contents |
| --- | --- |
| `raw-events.jsonl` | provider/feed identity, receipt time, provider event ID, unmodified JSON-safe payload, payload hash |
| `normalized-events.jsonl` | strict canonical event plus raw ID and raw payload hash from its header |

Every line has a canonical SHA-256 record hash. Replay verifies the hash before deserializing through strict canonical constructors. Raw payloads are copied into immutable mappings at the contract boundary so a caller cannot mutate an already accepted record through the original object.

The reconstructed book preserves the canonical event IDs since its snapshot; each of those records points back to its raw event and payload hash. Storage path selection is an `AppendOnlyMarketCapture` caller concern, not a market-semantics constant. `rejected-events.jsonl` can retain the raw ID and visible adapter rejection reason without pretending the packet was valid.
