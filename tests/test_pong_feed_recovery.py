"""TEST ONLY: executable ingress parity and connection-ownership regressions."""
import json
import random
import threading
import unittest
from typing import Mapping
from unittest.mock import patch

from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness
from src.l3f_provider.ninjatrader_observation import _contains_prohibited_identity
from tests.test_l3f_ninjatrader_observation import ROOT_KEY, SOURCE_HASH, signed_record


class PongFeedRecoveryTests(unittest.TestCase):
    def test_reused_wire_bytes_match_canonical_encoding_and_journal_chain(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
        from src.l3g_paper.pong_collection import PongAuthenticHourCollector
        from src.lane_iii.contracts import canonical_hash
        import hashlib
        for kind in ('QUOTE', 'TRADE', 'DEPTH'):
            raw = signed_record(kind=kind)
            record = NinjaTraderObservation.from_wire(json.dumps(raw)).verify_attestation(
                ROOT_KEY, expected_source_fingerprint=SOURCE_HASH)
            expected = {**record.unsigned_wire_payload(), 'signature': record.signature}
            self.assertEqual(record.signed_wire_bytes(), json.dumps(expected,
                sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode())
            self.assertEqual(record.wire_hash(), canonical_hash(expected))
            with TemporaryDirectory() as root:
                collector = PongAuthenticHourCollector(Path(root), expected_source_fingerprint=SOURCE_HASH)
                line = collector._observation_line(record, record.ninja_receipt_time)
                payload = json.loads(line)
                digest = payload.pop('record_hash')
                self.assertEqual(payload['observation'], expected)
                self.assertEqual(digest, canonical_hash(payload))

    def test_verified_nested_payload_cannot_diverge_from_cached_signature_material(self):
        from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
        raw = signed_record(sequence=1)
        # Re-sign a native-shaped nested payload using the isolated fixture key.
        from src.lane_iii.contracts import canonical_hash
        from src.l3f_provider.ninjatrader_observation import derive_observation_signing_key
        import hashlib, hmac
        raw['payload']['nested'] = [{'price': 100, 'sizes': [1, 2]}]
        raw['payload_hash'] = canonical_hash(raw['payload'])
        unsigned = {k: v for k, v in raw.items() if k != 'signature'}
        raw['signature'] = hmac.new(derive_observation_signing_key(ROOT_KEY),
            json.dumps(unsigned, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode(), hashlib.sha256).hexdigest()
        record = NinjaTraderObservation.from_wire(json.dumps(raw)).verify_attestation(
            ROOT_KEY, expected_source_fingerprint=SOURCE_HASH)
        before = record.wire_hash()
        with self.assertRaises(TypeError):
            record.payload['nested'][0]['price'] = 200
        with self.assertRaises(TypeError):
            record.payload['nested'][0]['sizes'].append(3)
        raw['payload']['nested'][0]['price'] = 300
        self.assertEqual(record.payload['nested'][0]['price'], 100)
        self.assertEqual(record.wire_hash(), before)
        self.assertTrue(record.verify_attestation(ROOT_KEY, expected_source_fingerprint=SOURCE_HASH).signature_verified)

    def test_production_mapper_omits_only_unused_rejected_artifacts(self):
        from dataclasses import replace
        from tests.test_l3g_pong_microstructure import BASE, MicrostructureEvidenceMapperTests
        from src.l3g_paper.pong_microstructure import MicrostructureEvidenceMapper
        original = MicrostructureEvidenceMapper.map_frame
        def compare(mapper, *args, **kwargs):
            full = original(mapper, *args, **kwargs)
            fast = original(mapper, *args, **kwargs, include_blocked_artifacts=False)
            self.assertEqual(full, fast)  # valid evidence and hashes are exact
            for index in range(4):
                inputs = list(args)
                inputs[index] = replace(inputs[index], valid=False, reason_codes=('TEST_ONLY_UNAVAILABLE',))
                rejected = original(mapper, *inputs, **kwargs)
                production = original(mapper, *inputs, **kwargs, include_blocked_artifacts=False)
                self.assertEqual(production.blockers, rejected.blockers)
                self.assertEqual(production.evidence, rejected.evidence)
                self.assertEqual(production.mapping_hash, rejected.mapping_hash)
                self.assertEqual(production.volume_regime, rejected.volume_regime)
            return full
        with patch.object(MicrostructureEvidenceMapper, 'map_frame', compare):
            for direction in (True, False):
                MicrostructureEvidenceMapperTests.frame(BASE, long=direction, revision=1)

    def test_immutable_ref_order_cache_preserves_duplicates_and_future_rejection(self):
        from dataclasses import replace
        from datetime import timedelta
        from src.l3g_paper.pong_microstructure import _ordered_refs, _ordered_ref_shape, _time
        from tests.test_l3g_pong_microstructure import BASE, trade, AggressorSide
        refs = tuple(trade(i, AggressorSide.BUY, '100').event for i in range(1, 12))
        def legacy(items, at):
            ordered = tuple(sorted(items, key=lambda e: (
                _time(e.available_at, 'Event available-at time'), _time(e.event_at, 'Event time'),
                e.sequence, e.event_id)))
            if len({e.event_id for e in ordered}) != len(ordered):
                raise ValueError('Duplicate event IDs are forbidden.')
            if len({(e.continuity_domain, e.sequence) for e in ordered}) != len(ordered):
                raise ValueError('Duplicate sequence coordinates are forbidden.')
            if any(_time(e.available_at, 'Event available-at time') > at for e in ordered):
                raise ValueError('Future-available evidence cannot enter an earlier result.')
            return ordered
        rng = random.Random(78312)
        cases = [(), refs, refs + (refs[0],), refs + (replace(refs[0], event_id='different'),)]
        for _ in range(100):
            cases.append(tuple(rng.sample(refs, rng.randint(0, len(refs)))))
        for values in cases:
            for seconds in (12, 5, 12, 0):  # same cache entry, varying causal cutoff
                at = BASE + timedelta(seconds=seconds)
                try:
                    expected = legacy(values, at)
                except ValueError as error:
                    with self.assertRaises(ValueError) as caught:
                        _ordered_refs(values, at)
                    self.assertEqual(str(caught.exception), str(error))
                else:
                    self.assertEqual(_ordered_refs(values, at), expected)
        self.assertEqual(_ordered_ref_shape.cache_info().maxsize, 128)

    def test_indexed_depth_window_matches_full_scan_with_expiry_and_price_changes(self):
        from collections import deque
        from dataclasses import replace
        from datetime import timedelta
        from decimal import Decimal
        from src.l3g_paper.pong_production import _DepthWindow, _time
        from tests.test_l3g_pong_microstructure import BASE, depth, BookSide
        indexed = _DepthWindow(maxlen=63)
        legacy = deque(maxlen=63)
        rng = random.Random(91231)
        for sequence in range(1, 700):
            item = depth(sequence, BookSide.BID, '5')
            item = replace(item, price=Decimal(rng.randint(95, 105)), touch_price=Decimal('105'))
            # Vary selection time across old, current and future values. The
            # adapter still receives every selected future ref and rejects it.
            indexed.append(item)
            legacy.append(item)
            cutoff = BASE + timedelta(seconds=sequence+rng.randint(-75, 4))
            values = tuple(v for v in legacy if _time(v.event.available_at) >= cutoff)
            expected = tuple(v for v in values if v.price == values[-1].price) if values else ()
            self.assertEqual(indexed.price_window(cutoff), expected)
            self.assertEqual(tuple(indexed), tuple(legacy))
        indexed.clear()
        self.assertEqual(indexed.price_window(BASE), ())

    def test_flow_window_uses_event_time_without_retiming_delayed_trade(self):
        from dataclasses import replace
        from datetime import timedelta
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from unittest.mock import Mock
        from src.l3g_paper.pong_production import PongProductionHost
        from tests.test_l3g_pong_production import runtime_snapshot, pending_collection
        from tests.test_l3g_pong_microstructure import (
            BASE, stamp, trade, depth, AggressorSide, BookSide,
        )
        late = trade(4, AggressorSide.BUY, '100')
        late = replace(late, event=replace(late.event, received_at=stamp(9), available_at=stamp(9)))
        boundary = trade(5, AggressorSide.SELL, '100')
        current = trade(9, AggressorSide.BUY, '100.25')
        with TemporaryDirectory() as directory:
            host = PongProductionHost(Path(directory), selected_profile='BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2',
                expected_market_source_fingerprint=SOURCE_HASH, runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection, controller_dispatch=lambda *args: False,
                native_dispatch=lambda *args: self.fail('TEST ONLY cannot dispatch'))
            host._trades.extend((late, boundary, current))
            host._depth[BookSide.BID].append(depth(9, BookSide.BID, '10'))
            host._depth[BookSide.ASK].append(depth(9, BookSide.ASK, '10'))
            actual_flow = host._flow
            results = []
            def evaluate(*args, **kwargs):
                result = actual_flow.evaluate(*args, **kwargs)
                results.append(result)
                return result
            host._flow = Mock(wraps=actual_flow)
            host._flow.evaluate.side_effect = evaluate
            host._evaluate_microstructure(BASE + timedelta(seconds=10))
            self.assertEqual(len(results), 1, host._microstructure_blockers)
            self.assertEqual(results[0].source_ids, (boundary.event.event_id, current.event.event_id))
            self.assertEqual(tuple(host._trades), (late, boundary, current))
            self.assertEqual(late.event.event_at, stamp(4))
            self.assertEqual(late.event.available_at, stamp(9))
            self.assertNotIn('PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:ValueError', host._microstructure_blockers)

    def test_timestamp_cache_preserves_validation_offsets_and_error_types(self):
        from src.l3g_paper.pong_microstructure import _time, _validated_time
        from src.l3g_paper.pong_production import _time_text
        for value in ('2026-09-14T15:00:00Z', '2026-09-14T09:00:00-06:00', '2026-09-14T15:00:00.1234567Z'):
            self.assertEqual(_time(value, 'test'), _validated_time.__wrapped__(value, 'test'))
            self.assertEqual(_time_text(value), _time_text.__wrapped__(value))
        for value in (None, [], {}, 'bad', '2026-09-14T15:00:00'):
            with self.assertRaises(ValueError):
                _time(value, 'test')
        self.assertEqual(_validated_time.cache_info().maxsize, 65536)
        self.assertEqual(_time_text.cache_info().maxsize, 65536)

    def test_decoded_json_security_scan_matches_legacy_nested_contract(self):
        def legacy(value):
            forbidden = {"account_id", "accountid", "password", "token", "secret", "authorization"}
            if isinstance(value, Mapping):
                return any(str(k).replace('-', '_').lower() in forbidden or legacy(v) for k, v in value.items())
            if isinstance(value, list):
                return any(legacy(v) for v in value)
            return False
        rng = random.Random(8491)
        def tree(depth):
            if depth == 0:
                return rng.choice([None, True, False, 12, 3.5, 'token is a value, not a key'])
            if rng.choice([True, False]):
                return {rng.choice(['price', 'size', 'Account-ID', 'payload', 'AUTHORIZATION']): tree(depth-1) for _ in range(3)}
            return [tree(depth-1) for _ in range(3)]
        for _ in range(500):
            decoded = json.loads(json.dumps(tree(4)))
            self.assertEqual(_contains_prohibited_identity(decoded), legacy(decoded))
        for key in ('account_id', 'Account-ID', 'ACCOUNTID', 'password', 'Token', 'SECRET', 'authorization'):
            self.assertTrue(_contains_prohibited_identity({'bids': [[{'nested': {key: 0}}]]}))
        self.assertFalse(_contains_prohibited_identity({'bids': [{'price': 1, 'size': 2}], 'text': 'secret'}))

    def test_retired_socket_close_cannot_disconnect_authenticated_market_owner(self):
        def encoded(sequence):
            return json.dumps(signed_record(sequence=sequence)).encode() + b'\n'
        class Connection:
            def __init__(self, chunks): self.chunks = list(chunks)
            def setblocking(self, value): pass
            def recv(self, size): return self.chunks.pop(0)
            def close(self): pass
        old = Connection([encoded(1), b''])
        current = Connection([encoded(2), encoded(3), b''])
        unauthenticated = Connection([b'{"invalid":true}\n'])
        class Listener:
            def __init__(self): self.pending = [old, current, unauthenticated]
            def setblocking(self, value): pass
            def accept(self):
                if not self.pending: raise BlockingIOError()
                return self.pending.pop(0), ('127.0.0.1', 30000)
            def close(self): pass
        listener = Listener()
        harness = NinjaTraderCommissioningHarness(attestation_key=ROOT_KEY, require_attestation=True,
                                                 expected_source_fingerprint=SOURCE_HASH)
        stop = threading.Event()
        steps = iter([listener, old, current, old, current, current, unauthenticated])
        count = 0
        def select_step(*args):
            nonlocal count
            count += 1
            if count == 5:
                self.assertEqual(harness.summary.health['LOCAL_BRIDGE'], 'HEALTHY')
            if count == 7:
                self.assertEqual(harness.summary.health['LOCAL_BRIDGE'], 'DISCONNECTED')
            try: return [next(steps)], [], []
            except StopIteration:
                self.assertEqual(harness.summary.health['LOCAL_BRIDGE'], 'DISCONNECTED')
                stop.set()
                return [], [], []
        with patch.object(harness.bridge, 'open_listener', return_value=listener), patch(
                'src.l3f_provider.ninjatrader_commission.select.select', side_effect=select_step):
            harness.run(5, stop_event=stop)
        self.assertEqual(harness.summary.accepted, 3)
        self.assertEqual(harness.summary.rejected, 1)
        self.assertEqual(harness.summary.duplicates, 0)
