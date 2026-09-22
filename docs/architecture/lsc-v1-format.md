# Lossless Semantic Compaction v1 format

Status: standalone offline format contract. LSC v1 is default-off and has no
runtime, persistence, ledger, deployment, qualification, or trading authority.

## Scope and invariants

LSC v1 encodes only already-admitted NinjaTrader `QUOTE`, `TRADE`, and `DEPTH`
observations. It does not admit, filter, sample, coalesce, replace, reorder, or
infer observations. All other observation types are refused before the payload
is traversed. The caller remains responsible for invoking the codec only after
the existing `NinjaTraderSessionLedger.admit` boundary.

An encoded record reconstructs the exact observation ID, session ID, type,
local sequence, three optional/source timestamps, provider sequence, optional
account provenance, payload scalar types and values, canonical payload hash,
and existing wire hash. A required context also binds source, provider,
environment, frozen source identity, loaded-build identity, contract identity,
contract ID, tick size, session, generation, and timestamp origin. A segment
must contain one context digest only.

No LSC v1 success is qualification evidence by itself. A codec or store error
invalidates shadow proof; it cannot alter admission or authorize fallback loss.

## Primitive encoding

All fixed-width numeric fields are little-endian. Unsigned integers use minimal
unsigned LEB128. Signed integers use ZigZag followed by minimal unsigned
LEB128. Float values retain their exact IEEE-754 binary64 bits. Text is an
unsigned byte length followed by UTF-8 with the surrogate-pass rule. Object keys
are strictly sorted by their UTF-8 byte strings. Duplicate or non-increasing
keys, overlong varints, unknown tags, trailing bytes, invalid UTF-8, and unknown
versions are refused.

The format limits are normative:

| Item | Limit |
| --- | ---: |
| Encoded context | 65,536 bytes |
| Canonical JSON payload | 1,048,576 bytes |
| Encoded record | 2,097,152 bytes |
| One text value | 1,048,576 bytes |
| Items in one list or object | 65,536 |
| Payload nesting depth | 32 |
| Payload integer magnitude | 4,096 bits |
| Sequence and generation | `0..2^63-1` |

Decoders enforce these limits before allocating the corresponding body.

## Context envelope

The context is encoded once per identity-homogeneous segment:

1. ASCII magic `LSCC`.
2. One-byte version `1`.
3. Nine length-prefixed UTF-8 dictionary entries in fixed order: source,
   provider, environment, source identity, build identity, contract identity,
   contract ID, tick-size text, and session ID.
4. Generation as unsigned LEB128.
5. Exact canonical UTC timestamp-origin text.
6. A 32-byte context digest.

The timestamp origin is `YYYY-MM-DDTHH:MM:SS[.1-9 digits]Z`. The digest is:

```text
SHA-256("BEELZEBUB_LSC_V1_CONTEXT\0" || all preceding context bytes)
```

`decode_context` verifies the byte limit, magic, version, field bounds, digest,
complete consumption, semantic constraints, and byte-for-byte canonical
re-encoding. `context_digest` performs those same checks before returning the
embedded lowercase digest. A segment store can therefore bind context bytes to
record metadata without recanonicalizing an event.

## String dictionaries and typed payload

Every string is encoded as exactly one of raw UTF-8, a static format-dictionary
index, or a context-dictionary index. The static dictionary contains the v1
market payload keys and enumerations in the order defined by
`src/lsc/codec.py`; changing that order requires a new format version. Runtime
identities and the repeated contract/session strings refer to context entries.

Payload values have explicit tags for null, false, true, signed integer,
float64, string, list, object, tick-projected price, and price escape. This
preserves distinctions such as `20000`, `20000.0`, and `"20000.00"`, Unicode
text, null, and the sign bit of `-0.0`. Unknown payload keys and nested JSON
values survive. Map insertion order is not semantic; canonical key order is.

Known price locations are:

- `QUOTE`: `bid`, `ask`
- `TRADE`: `price`, `bid_at_trade`, `ask_at_trade`
- `DEPTH`: `mutation_price`, and each `bids[*].price` / `asks[*].price`

When a scalar is finite, positive, and exactly divisible by the context tick
size, the record stores its signed integer-tick projection plus the exact
original scalar representation. Decode verifies both agree. All other admitted
values use the explicit price-escape tag and preserve the typed scalar exactly.
An aligned price encoded through the escape path is noncanonical and refused.
This rule compacts native quarter-tick values without turning an off-tick or
unusual admitted value into invented evidence.

## Exact timestamps

Null timestamps use a null tag. Canonical UTC timestamps with up to nine
fractional digits are encoded as a signed integer-nanosecond delta from the
context origin plus the original fractional digit count. Reconstruction is
integer-only, so seven-digit .NET timestamps are never truncated to Python
microseconds and trailing precision is retained exactly. Any other ISO spelling
already accepted by the observation boundary, including an explicit nonzero
offset, uses an exact raw-text escape. Canonical re-encoding prevents alternate
spellings of the binary form.

## Record envelope

An event record contains, in order:

1. ASCII magic `LSCR`, version byte `1`, flags byte `0`, and one-byte kind
   (`QUOTE=1`, `TRADE=2`, `DEPTH=3`).
2. The exact 32-byte context digest.
3. Local sequence as unsigned LEB128.
4. Observation ID: derived `nt-{session}-{sequence}` marker or exact string.
5. Ninja receipt timestamp, provider timestamp, optional provider sequence,
   and exchange timestamp.
6. Optional account alias and class, jointly absent or present.
7. Encoded-payload byte length and typed payload.
8. Existing canonical payload hash and existing observation wire hash, each 32
   bytes.
9. A 32-byte event digest.

Canonical payload bytes use the repository contract exactly:

```python
json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
```

The decoder reconstructs the payload, recomputes those canonical bytes and
their SHA-256, reconstructs `NinjaTraderObservation`, and verifies its existing
`wire_hash()`. The event digest is:

```text
SHA-256("BEELZEBUB_LSC_V1_EVENT\0" || all preceding record bytes)
```

`EncodedRecord.event_digest` is the verified leaf value a segment store uses
for its ordered Merkle tree. The store must not replace it with another event
canonicalization. Decode finally re-encodes the complete observation and
requires exact byte equality, refusing alternate dictionaries, price escapes,
timestamp forms, integer forms, field order, or observation-ID spellings.

## Golden vectors and compatibility

`tests/lsc/golden_vectors.json` fixes the full context and record bytes plus
payload, wire, context, and event digests for representative QUOTE, TRADE, and
DEPTH observations. It includes an off-tick trade escape, mixed numeric/string
quarter-tick prices, unknown nested payload data, negative zero, a nonzero UTC
offset, a seven-digit timestamp, and a nine-digit timestamp. Any intentional
format change must introduce a new version; silently updating v1 vectors is not
compatible.

LSC v1 uses only Python's standard library and performs no compression.
Compression belongs after canonical record framing in the segment store and
must have an independent bounded decompression contract.
