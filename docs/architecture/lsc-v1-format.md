# Lossless Semantic Compaction v1 format

Status: standalone offline format contract. LSC v1 is default-off and has no
runtime, persistence, ledger, deployment, qualification, or trading authority.

## Scope and invariants

LSC v1 encodes only already-admitted NinjaTrader `QUOTE`, `TRADE`, and `DEPTH`
observations. It does not admit, filter, sample, coalesce, replace, reorder, or
infer observations. All other observation types are refused before the payload
is traversed. The caller remains responsible for invoking the codec only after
the existing `NinjaTraderSessionLedger.admit` boundary.

An encoded record reconstructs the exact current L3F3 observation: schema,
observation ID, session ID, type, local sequence, three optional/source
timestamps, provider sequence, optional account provenance, payload scalar
types and values, native payload hash, full producer attestation, signature,
verified-signature state, exact signed wire bytes, and wire hash. It also binds
the native source sequence and generation carried by the payload. A required
context binds source, provider, environment, producer protocol, source and
loaded-build fingerprints, build timestamp, separate native and canonical
contract IDs, tick size, session, generation, and timestamp origin. A segment
must contain one context digest only.

`source_stream` and `publication_policy` are intentionally record fields, not
context fields. Native QUOTE, TRADE, and DEPTH frames use distinct values and
interleave in one admitted local sequence. Keeping those two attestation fields
per record permits one mixed-kind segment to preserve that global order.

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
3. Observation schema, source, provider, and environment as length-prefixed
   UTF-8.
4. One-byte attestation mode: `1` for current L3F3, `0` only for explicitly
   encoded legacy L3F2.
5. For mode `1`, producer protocol version, source fingerprint, build
   fingerprint, and exact build timestamp as length-prefixed UTF-8. Mode `0`
   omits these fields.
6. Native contract ID, canonical contract ID, tick-size text, and session ID
   as length-prefixed UTF-8. For the current DEC26 contract these are separate
   values `MNQ DEC26` and `MNQZ6`; a combined invented identifier is invalid.
7. Generation as unsigned LEB128.
8. Exact canonical UTC timestamp-origin text.
9. A 32-byte context digest.

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
The current stream and publication-policy enumerations are static dictionary
entries and are encoded independently in every attested record. The native
paired-quote policy is `EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1`.

Payload values have explicit tags for null, false, true, signed integer,
float64, string, list, object, tick-projected price, and price escape. This
preserves distinctions such as `20000`, `20000.0`, and `"20000.00"`, Unicode
text, null, and the sign bit of `-0.0`. Unknown payload keys and nested JSON
values survive. Map insertion order is not semantic; canonical key order is.

Known price locations are:

- `QUOTE`: `bid`, `ask`
- `TRADE`: `price`, `bid_at_trade`, `ask_at_trade`
- `DEPTH`: `mutation_price`, `mutation_resolved_price`, and every
  `bids[*].price`, `asks[*].price`, `position_bids[*].price`, and
  `position_asks[*].price`

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
7. For attested L3F3 only, dictionary-coded producer `source_stream` and
   `publication_policy`. Explicit legacy mode omits them.
8. Encoded-payload byte length and typed payload.
9. The native canonical payload hash (32 bytes).
10. Signature marker (`1` for L3F3, `0` for explicit legacy) and, for L3F3,
    the exact 32-byte HMAC-SHA256 signature.
11. Existing signed-wire hash (32 bytes).
12. A 32-byte event digest.

Canonical payload bytes use the repository contract exactly:

```python
json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
```

The decoder reconstructs the payload, recomputes those canonical bytes and
their SHA-256, reconstructs the full producer attestation from invariant
context fields and per-record stream/policy fields, then reconstructs the
current `NinjaTraderObservation`. It requires `signature_verified=True` for
L3F3, exact native payload-hash parity, exact `signed_wire_bytes()`, and exact
`wire_hash()`. The codec does not possess the HMAC key; it accepts only an
observation already authenticated by the native admission boundary and
losslessly preserves that result. The event digest is:

```text
SHA-256("BEELZEBUB_LSC_V1_EVENT\0" || all preceding record bytes)
```

`EncodedRecord.event_digest` is the verified leaf value a segment store uses
for its ordered Merkle tree. The store must not replace it with another event
canonicalization. Decode finally re-encodes the complete observation and
requires exact byte equality, refusing alternate dictionaries, price escapes,
timestamp forms, integer forms, field order, or observation-ID spellings.

## Golden vectors and compatibility

`tests/lsc/golden_vectors.json` fixes one mixed-stream context and the full
record bytes plus
payload, wire, context, and event digests for representative QUOTE, TRADE, and
DEPTH observations. It includes each native stream/policy pair, exact producer
identity and signature, an off-tick trade escape, mixed numeric/string
quarter-tick prices, unknown nested payload data, negative zero, a nonzero UTC
offset, and seven-digit timestamps. After this pre-freeze current-source update,
any intentional format change must introduce a new version; silently updating
v1 vectors is not compatible.

Legacy L3F2 compatibility is fail-closed and never inferred. It requires the
context mode byte `0`, the L3F2 schema, no producer identity, no signature, and
independent verifier coverage. An L3F2 record cannot be written beneath an
L3F3 context or upgraded into an attested claim.

The codec performs no compression. The segment store compresses only the
already framed canonical record body with the Python runtime's standard-library
zlib binding. The segment header binds the exact runtime library and frozen
parameters as
`zlib-runtime-<zlib.ZLIB_RUNTIME_VERSION>-deflate-wbits15-mem8-level6-default`.
The commissioned Python 3.12 runtime reports zlib `1.3.1`, so its identity is
`zlib-runtime-1.3.1-deflate-wbits15-mem8-level6-default`. Writers explicitly
select DEFLATE, wrapper window 15, memory level 8, compression level 6, and the
default strategy. A reader requires an exact identity match before
decompression; a different zlib runtime or parameter set is incompatible and
fails closed rather than being silently treated as the frozen compressor.

Compressed input, decompressed output, record count, context bytes, metadata,
and complete segment-file size are independently bounded. Decompression also
requires EOF with no trailing or unconsumed bytes. This compression layer does
not change codec golden vectors or their event digests; it is covered by the
segment content digest, commitment, and deterministic same-runtime receipts.

## Shadow commitment destination identity

The append-only shadow commitment journal is separate from the operational
`PaperLedger` and has no authority effect. Its identity context binds a
canonical storage identity to the actual segment destination, rather than
accepting a caller label. The store root and segment root must already exist.
They and the independent shadow-journal root are resolved through path aliases,
symlinks, and junctions, then normalized for the host filesystem. A
not-yet-created journal destination is resolved from its existing parent before
any directory is created. The canonical identity is:

```text
"beelzebub-lsc-storage-v1:" ||
SHA-256("beelzebub.lsc.v1.storage.identity\0" || canonical_json({
  "journal_root": normalized_resolved_shadow_journal_root,
  "root": normalized_resolved_store_root,
  "schema": "beelzebub-lsc-storage-identity-v1",
  "segment_root": normalized_resolved_segment_root
}))
```

Lexical aliases of the same resolved destinations therefore have one identity,
while another drive, store root, redirected segment directory, custom journal
root, or substituted destination has a different identity. A custom journal
root is permitted only when that exact resolved destination was included while
constructing the identity. Construction refuses a supplied identity that does
not equal all three derived destinations before creating a journal directory.
Verification checks it both before and after reading the segment and journal
chains. Recovery checks it before any temporary can be quarantined and again
after validation. An unresolvable or changed destination fails closed; its
journal cannot attest a false storage location.
