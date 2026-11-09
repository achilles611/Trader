import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

from scripts.lane_ii_continuous import hourly_replay as hourly


def endpoint(path, offset=None):
    stat = path.stat()
    return {"offset": stat.st_size if offset is None else offset, "device": stat.st_dev, "inode": stat.st_ino}


class CursorTests(unittest.TestCase):
    def test_split_ranges_have_exact_offsets_and_same_final_hash(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.jsonl"
            raw = b'{"a":1}\n{"a":2}\n{"a":3}\n'; path.write_bytes(raw)
            cursor = hourly.Cursor(path)
            cursor.begin(endpoint(path, 8)); self.assertEqual(cursor.read(), raw[:8]); first = cursor.finish()
            cursor.begin(endpoint(path))
            self.assertEqual(cursor.read() + cursor.read(), raw[8:]); self.assertIsNone(cursor.read())
            last = cursor.finish()
            self.assertEqual(first["end_offset"], last["start_offset"])
            self.assertEqual(last["prefix_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(cursor.rows, 3)

    def test_partial_line_replacement_and_offset_regression_are_held(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "events.jsonl"; path.write_bytes(b'{"a":1}\n')
            cursor = hourly.Cursor(path)
            cursor.begin(endpoint(path, 5))
            with self.assertRaisesRegex(hourly.Hold, "INCOMPLETE"):
                cursor.read()
            cursor.close()
            with self.assertRaisesRegex(hourly.Hold, "IDENTITY"):
                cursor.begin({**endpoint(path), "inode": -1})
            cursor.begin(endpoint(path)); cursor.read(); cursor.finish()
            with self.assertRaisesRegex(hourly.Hold, "REGRESSION"):
                cursor.begin(endpoint(path, 0))


class StreamingStateTests(unittest.TestCase):
    def test_disk_sequences_preserve_strict_event_comparison(self):
        with tempfile.TemporaryDirectory() as folder:
            store = hourly.Store(Path(folder) / "analysis.sqlite", 16 * hourly.MIB)
            try:
                left, right = hourly.DiskSequence(store, "left"), hourly.DiskSequence(store, "right")
                event = {"kind": "PAPER_ENTRY", "data": {"quantity": 2.5}}
                left.append(event); right.append(event)
                self.assertTrue(hourly.strict.retained.compare_events(left, right)["pass"])
                right.append(event)
                self.assertFalse(hourly.strict.retained.compare_events(left, right)["pass"])
                mapping = hourly.DiskMap(store, "http")
                mapping[11] = {"status": 200}; mapping[11] = {"status": 201}
                self.assertEqual(mapping[11], {"status": 201}); self.assertEqual(len(mapping), 1)
                store.commit()
            finally:
                store.close()

    def test_accounting_reconciles_open_basis_then_exit_without_adding_accounts(self):
        ledger = hourly.Accounting(3000.)
        entry = {"market": "m", "side": "Up", "quantity": 10., "basis": 5.05, "entry_fee": .05,
                 "fills": [{"quantity": 10., "price": .5}]}
        ledger.consume("PAPER_ENTRY", entry)
        profile = SimpleNamespace(entries=1, exits=0, settlements=0, positions={"m": entry}, realized=0., fees=.05)
        opened = ledger.verify(profile)
        self.assertAlmostEqual(opened["virtual_cash"], 2994.95)
        self.assertEqual(opened["unresolved_basis"], 5.05)
        exit_row = {"market": "m", "side": "Up", "basis": 5.05, "proceeds": 5.4, "net": .35, "exit_fee": .1,
                    "fills": [{"quantity": 10., "price": .55}]}
        ledger.consume("PAPER_EXIT", exit_row)
        profile.positions = {}; profile.exits = 1; profile.realized = .35; profile.fees = .15
        result = ledger.verify(profile)
        self.assertAlmostEqual(result["virtual_cash"], 3000.35)
        self.assertEqual(result["unresolved_positions"], 0)
        with self.assertRaisesRegex(hourly.Hold, "WITHOUT_POSITION"):
            ledger.consume("PAPER_EXIT", exit_row)

    def test_coverage_retains_paired_windows_without_unbounded_bundle_history(self):
        coverage = hourly.CompactCoverage(0, 300)
        market = {"start": 0, "end": 300, "accepting": True, "closed": False, "metadata_received": 1}
        quotes = {side: {"bid": .4, "ask": .5} for side in ("Up", "Down")}
        for bundle in range(1000):
            for identity in hourly.IDS:
                coverage.evaluation({"evaluation_wall": 2, "profile": identity, "operation": "tick", "enabled": True,
                                     "entry_allowed": True, "bundle_id": bundle}, market, quotes, 100000)
        self.assertLessEqual(len(coverage.fresh_bundles), 2)
        self.assertEqual(coverage.result()["shared_fresh_eligible_5m_windows"], 1)

    def test_checkpoint_skips_and_duplicate_sequences_are_held(self):
        consumer = object.__new__(hourly.Consumer)
        consumer.start, consumer.cutoff, consumer.first_scheduled = 0, 3600, 900
        consumer.engine = SimpleNamespace(risk=SimpleNamespace(state={"segment_id": "s"}))
        consumer.manifest_sha = "a" * 64; consumer.checkpoint_sequence = 0; consumer.previous_checkpoint = hourly.ZERO
        consumer.scheduled_epochs = []; consumer.limits = hourly.Limits()
        consumer.cursors = {name: SimpleNamespace(offset=0) for name in hourly.STREAMS}
        packet = {"schema": hourly.CHECKPOINT_SCHEMA, "kind": "SCHEDULED", "safe_loop_boundary": True,
                  "source_manifest_sha256": consumer.manifest_sha, "segment_id": "s", "paper_start_epoch": 0,
                  "fixed_cutoff_epoch": 3600, "checkpoint_sequence": 1, "previous_checkpoint_sha256": hourly.ZERO,
                  "streams": {name: {"offset": 0} for name in hourly.STREAMS}, "scheduled_epoch": 900,
                  "captured_epoch": 900, "captured_utc": "1970-01-01T00:15:00Z"}
        consumer.validate_packet(packet)
        with self.assertRaisesRegex(hourly.Hold, "MISSING_FIFTEEN"):
            consumer.validate_packet({**packet, "scheduled_epoch": 1800})
        with self.assertRaisesRegex(hourly.Hold, "SEQUENCE"):
            consumer.validate_packet({**packet, "checkpoint_sequence": 2})


def sealed_fixture_script():
    path = Path(__file__).with_name("test_lane_ii_continuous_qualification_replay.py")
    tree = ast.parse(path.read_text())
    return next(node.value.value for node in tree.body if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "WHOLE_RUN_SCRIPT" for target in node.targets)).split("result=replay.run(")[0]


SPLIT_SCRIPT = r'''
from scripts.lane_ii_continuous import hourly_replay as hourly
left=hourly.verify_sealed(candidate,run,root/'audit.json',root/'supervisor.json',root/'split',source['sha256'],split_seconds=1)
right=hourly.verify_sealed(candidate,run,root/'audit.json',root/'supervisor.json',root/'contiguous',source['sha256'],split_seconds=86400)
for key in ('profiles','parity','recorded_decision_parity','risk_parity','coverage','admission_parity','accounting'):
    assert left[key]==right[key],key
assert left['pass'] and right['pass'] and left['sealed_retrospective_validation_only']
assert len(json.loads((root/'split/SEALED_RANGE_TEST_PLAN.json').read_text())['ranges'])==2
assert len(json.loads((root/'contiguous/SEALED_RANGE_TEST_PLAN.json').read_text())['ranges'])==1
print('split_contiguous_equivalent')
'''


NATIVE_SCRIPT = r'''
from scripts.lane_ii_continuous import hourly_replay as hourly
context=replay.preflight(candidate,run,root/'audit.json',root/'supervisor.json',root/'not-created',expected_manifest_sha256=source['sha256'])
plan=hourly.sealed_range_plan(context,900)
assert len(plan)==5
(capture/'checkpoints').mkdir()
previous_checkpoint='0'*64
for sequence,index in enumerate((0,1,2,4),1):
    bound=plan[index];final=sequence==4;epoch=4500 if final else 900+900*sequence
    packet={'schema':hourly.CHECKPOINT_SCHEMA,'kind':'FINAL' if final else 'SCHEDULED','safe_loop_boundary':True,
      'checkpoint_sequence':sequence,'previous_checkpoint_sha256':previous_checkpoint,'scheduled_epoch':epoch,
      'captured_epoch':epoch+.1,'captured_utc':utc(epoch+.1),'source_manifest_sha256':source['sha256'],
      'segment_id':state['segment_id'],'paper_start_epoch':900,'fixed_cutoff_epoch':4500,'streams':bound['streams'],
      'event_seq':bound['event_seq'],'chain_sha256':bound['chain_sha256'],'ws_count':0,'paper_risk_state':state,
      'paper_risk_state_sha256':risk.digest(state),'paper_risk_anchor_sha256':risk.digest(anchor),
      'paper_risk_action_cursor':{'sequence':0,'event_id':None,'sha256':None},
      'account_carryover':receipt['account_carryover'],'pending_intents':{p.id:[] for p in profiles}}
    path=capture/'checkpoints'/('FINAL.json' if final else str(epoch)+'.json')
    previous_checkpoint=write(path,packet)['sha256']
consumer=hourly.Consumer(candidate,run,root/'native',source['sha256'])
consumer.consume_checkpoint(capture/'checkpoints/1800.json')
consumer.consume_checkpoint(capture/'checkpoints/2700.json')
consumer.close()
try:hourly.Consumer(candidate,run,root/'native',source['sha256'])
except hourly.Hold as exc:assert 'RESTART' in str(exc)
else:raise AssertionError('Silent restart forbidden')
consumer=hourly.Consumer(candidate,run,root/'native',source['sha256'],restart_policy='rebuild')
assert consumer.checkpoint_sequence==2
consumer.consume_checkpoint(capture/'checkpoints/3600.json')
consumer.consume_checkpoint(capture/'checkpoints/FINAL.json')
assert json.loads((root/'native/STATUS.json').read_text())['state']=='AWAITING_FINAL_INPUTS'
write(root/'FINAL_INPUTS.json',{'schema':'LANE_II_HOURLY_REPLAY_FINAL_INPUTS_V1','bindings':{
 'raw_audit':replay.binding(root/'audit.json'),'supervisor_result':replay.binding(root/'supervisor.json')}})
result=consumer.finish(root/'FINAL_INPUTS.json');consumer.close()
assert result['pass'] and result['hourly_epochs']==[4500] and not result['sealed_retrospective_validation_only']
assert result['coverage']['missing_scheduled_tick_seconds']==3596
assert len(result['hourly_bindings'])==1
# A crash after RESULT publication must revalidate, retain it, and finish publication.
(root/'native/ARTIFACT_MANIFEST.json').unlink()
consumer=hourly.Consumer(candidate,run,root/'native',source['sha256'],restart_policy='rebuild')
again=consumer.finish(root/'FINAL_INPUTS.json');consumer.close()
assert again==result and (root/'native/ARTIFACT_MANIFEST.json').is_file()
print('native_quarters_restart_and_final_tail_verified')
'''


class FrozenIntegrationTests(unittest.TestCase):
    candidate = Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5")

    @unittest.skipUnless(candidate.is_dir(), "local frozen source fixture")
    def test_split_and_contiguous_causal_and_accounting_results_match(self):
        with tempfile.TemporaryDirectory() as folder:
            completed = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", sealed_fixture_script() + SPLIT_SCRIPT,
                str(self.candidate), str(Path(__file__).resolve().parents[1]), folder], text=True, capture_output=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertIn("split_contiguous_equivalent", completed.stdout)

    @unittest.skipUnless(candidate.is_dir(), "local frozen source fixture")
    def test_four_quarters_rebuild_final_tail_and_interrupted_publication(self):
        script = sealed_fixture_script().replace("utc(1001)", "utc(900)").replace("utc(1002)", "utc(4500)")
        script = script.replace("utc(1002.02)", "utc(4500.02)").replace("utc(1002.03)", "utc(4500.03)")
        script = script.replace("expected_starts=[900]", "expected_starts=range(900,4500,300)")
        script = script.replace("pre=write(capture/'preregistration.json',prereg)", """prereg['capture_checkpoint_policy']={'schema':'LANE_II_CAPTURE_CHECKPOINT_V1','enabled':True,'interval_seconds':900,'cadence_origin_epoch':900,'fixed_cutoff_epoch':4500,'partial_contexts_permitted':False}
prereg['paper_risk_initial_action_cursor']={'sequence':0,'event_id':None,'sha256':None}
pre=write(capture/'preregistration.json',prereg)""")
        start, end = script.index("events=["), script.index("previous='0'*64;raw=b''")
        script = script[:start] + """events=[('START',899,{'preregistration_sha256':pre['sha256'],'manifest_sha256':source['sha256'],'pid':123})]
for second in (1799,2699,3599,4499):
    model={'schema':decision.SCHEMA,'second':second,'mark':FeedHistory().sample(second),'forecasts':{},'construction_start_wall':second+.04,'available_wall':second+.05,'construction_start_monotonic_ns':int((second+.04)*1e9),'available_monotonic_ns':int((second+.05)*1e9)}
    events.extend([('CAUSAL_MODEL_INPUT',second+.06,model),('MODEL_AND_PROFILE_TICK',second+.08,{**model,'feature_available_wall':second+.07,'feature_available_monotonic_ns':int((second+.07)*1e9)})])
events.append(('END',4500.01,{'state':'COMPLETE','reason':'DEADLINE','terminal_censoring':{p.id:[] for p in profiles}}))
""" + script[end:]
        with tempfile.TemporaryDirectory() as folder:
            completed = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", script + NATIVE_SCRIPT,
                str(self.candidate), str(Path(__file__).resolve().parents[1]), folder], text=True, capture_output=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertIn("native_quarters_restart_and_final_tail_verified", completed.stdout)


if __name__ == "__main__":
    unittest.main()
