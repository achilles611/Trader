from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone

from scripts.lane_ii_continuous import qualification_replay as replay


def write(path, value):
    raw = replay.canonical(value)
    Path(path).write_bytes(raw)
    return replay.binding(path)


def preflight_fixture(root, *, stopped=False):
    candidate, run = root / "candidate", root / "run"
    candidate.mkdir(); (run / "capture").mkdir(parents=True)
    files = {name: b"# fixture only\n" for name in ("continuous_decision_capture.py", "continuous_paper_risk.py", "slim_roster.py", "study.py")}
    files["MINIMAL_ROSTER.json"] = b"{}"
    for name, raw in files.items():
        (candidate / name).write_bytes(raw)
    manifest = {"schema": "LANE_II_CONTINUOUS_FROZEN_CAPTURE_V1",
                "config": {"decision_capture_schema": replay.DECISION_SCHEMA, "paper_risk_schema": replay.RISK_SCHEMA},
                "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()}}
    source = write(candidate / "SOURCE_MANIFEST.json", manifest)
    start, cutoff = "2026-11-08T12:00:00Z", "2026-11-08T12:05:00Z"
    state, reason, end = ("STOPPED", "USER_STOP", "2026-11-08T12:03:00Z") if stopped else ("COMPLETE", "DEADLINE", cutoff)
    counters = {"real_orders": 0, "model_api_calls": 0, "authenticated_account_calls": 0}
    prereg = {"source_manifest_sha256": source["sha256"], "paper_start_utc": start, "cutoff_utc": cutoff,
              "policy": {**counters, "virtual_cash_per_profile": 3000}, "deadline_mono": 600}
    prereg_binding = write(run / "capture/preregistration.json", prereg)
    receipt = {"source_manifest_sha256": source["sha256"], "preregistration_sha256": prereg_binding["sha256"],
               "state": state, "reason": reason, "pid": 123, "paper_start_utc": start,
               "cutoff_utc": cutoff, "actual_end_utc": end, "utc": end, "events": 2,
               "profiles": [{"id": identity} for identity in sorted(replay.EXPECTED_IDS)],
               "ledger_sha256": {identity: "0" * 64 for identity in replay.EXPECTED_IDS},
               "terminal_censoring": {identity: [] for identity in replay.EXPECTED_IDS}, **counters}
    write(run / "capture/receipt.json", receipt)
    marker = {"schema": "BTC_NEXT12_COLLECTION_CLOSED_V1", "worker_pid": 123,
              "source_manifest_sha256": source["sha256"], "fixed_cutoff_utc": cutoff,
              "state_at_collection_close": state, **{key: True for key in
              ("collection_closed", "network_closed", "producers_exited", "snapshot_publisher_closed", "finalization_only_no_more_http")}}
    write(run / "capture/COLLECTION_CLOSED.json", marker)
    operator = {"pid": 123, "creation_epoch": 100, "boot_epoch": 1, "owned_job_before_startup_gate": True,
                "source_manifest_sha256": source["sha256"], "command": [sys.executable, "-I", "-S", "-B",
                str(candidate / "study.py"), "worker", "--run-dir", str(run / "capture"),
                "--paper-start-utc", start, "--cutoff-utc", cutoff, "--startup-gate"]}
    write(run / "operator.json", operator)
    write(run / "exit.json", {"pid": 123, "creation_epoch": 100, "boot_epoch": 1,
          "source_manifest_sha256": source["sha256"], "exit_code": 0, "forced_closure": False,
          "retained_handle_exit_observed": True, "owned_job_closed": True, "owned_children_at_parent_exit": []})
    audit = {"schema": "LANE_II_READONLY_CAPTURE_AUDIT_V1", "status": "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY",
             "problem_counts": {}, "events_checked": 2, "execution": {"state": state},
             "input_bindings": [replay.binding(run / "capture/receipt.json"), replay.binding(run / "capture/COLLECTION_CLOSED.json")]}
    write(root / "audit.json", audit)
    paths = {"receipt": run / "capture/receipt.json", "preregistration": run / "capture/preregistration.json",
             "collection_closed": run / "capture/COLLECTION_CLOSED.json", "worker_exit": run / "exit.json",
             "worker_identity": run / "operator.json", "raw_audit": root / "audit.json", "source_manifest": candidate / "SOURCE_MANIFEST.json"}
    if stopped:
        paths["stop_request"] = run / "capture/STOP"
        write(paths["stop_request"], {"schema": "LANE_II_COORDINATOR_STOP_V1", "utc": end, "reason": "CPU_PERSISTENT"})
    write(root / "supervisor.json", {"schema": "LANE_II_PROSPECTIVE_SUPERVISOR_RESULT_V1", "receipt": receipt,
          "worker_exit_code": 0, "forced_closure": False, "bindings": {key: replay.binding(path) for key, path in paths.items()},
          "capture_audit": {"status": "PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY", "problems": [], "events_checked": 2}})
    for name in ("events.jsonl", "websocket-frames.jsonl", "5m-P13-ledger.jsonl", "5m-P2-ledger.jsonl"):
        (run / "capture" / name).write_bytes(b"")
    return (candidate, run, root / "audit.json", root / "supervisor.json", root / "out"), source["sha256"]


class PreflightTests(unittest.TestCase):
    def test_exact_fixture_metadata_is_bound_without_running_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            args, digest = preflight_fixture(Path(directory))
            result = replay.preflight(*args, expected_manifest_sha256=digest)
            self.assertEqual(result["receipt"]["state"], "COMPLETE")
            self.assertFalse(args[-1].exists())

    def test_stopped_endpoint_requires_the_exact_supervisor_stop_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            args, digest = preflight_fixture(Path(directory), stopped=True)
            result = replay.preflight(*args, expected_manifest_sha256=digest)
            stop = args[1] / "capture/STOP"
            self.assertEqual(result["sources"]["stop_request"], replay.binding(stop))
            self.assertEqual(result["decision_bound"], replay.stamp("2026-11-08T12:03:00Z"))
            changed = replay.load(stop)
            changed["utc"] = "2026-11-08T12:02:59Z"
            write(stop, changed)
            with self.assertRaisesRegex(ValueError, "EXACT_ARTIFACT_BINDINGS"):
                replay.preflight(*args, expected_manifest_sha256=digest)

    def test_legacy_failed_endpoint_is_refused_even_with_audit_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            args, digest = preflight_fixture(Path(directory))
            path = args[1] / "capture/receipt.json"
            receipt = replay.load(path); receipt.update(state="FAILED", reason="HTTP_TIMEOUT")
            write(path, receipt)
            with self.assertRaisesRegex(ValueError, "LEGACY_FAILURES_INELIGIBLE"):
                replay.preflight(*args, expected_manifest_sha256=digest)

    def test_missing_prospective_instrumentation_and_source_edits_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            args, digest = preflight_fixture(Path(directory))
            (args[0] / "continuous_paper_risk.py").write_text("changed")
            with self.assertRaisesRegex(ValueError, "SOURCE_CHANGED"):
                replay.preflight(*args, expected_manifest_sha256=digest)

    def test_supervisor_binding_tamper_is_not_a_genuine_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            args, digest = preflight_fixture(Path(directory))
            supervisor = replay.load(args[3]); supervisor["bindings"]["worker_exit"]["sha256"] = "f" * 64
            write(args[3], supervisor)
            with self.assertRaisesRegex(ValueError, "EXACT_ARTIFACT_BINDINGS"):
                replay.preflight(*args, expected_manifest_sha256=digest)


class GateAndCoverageTests(unittest.TestCase):
    def test_decision_context_has_an_aggregate_memory_bound(self):
        engine = object.__new__(replay.ProspectiveReplay)
        engine.buffer = None
        with self.assertRaisesRegex(ValueError, "CONTEXT_MEMORY_BOUND"):
            engine.consume({"kind": "DECISION_BEGIN", "data": {}, "_verified_raw_bytes": 17 * replay.MIB})

    def check(self, **updates):
        row = {"evaluation_id": "e1", "index": 1, "wall": 99, "monotonic": 9,
               "stop_requested": False, "stop_event_set": False, "hard_allowed": True}
        row.update(updates)
        return row

    def test_lifecycle_gate_recomputes_recorded_result_and_call_count(self):
        gate = replay.HardGateChecks("e1", [self.check()], 100, 10)
        self.assertTrue(gate()); gate.finish(1)
        bad = replay.HardGateChecks("e1", [self.check(wall=100)], 100, 10)
        with self.assertRaisesRegex(ValueError, "RESULT_MISMATCH"):
            bad()
        stopped = replay.HardGateChecks("e1", [self.check(stop_event_set=True, hard_allowed=False)], 100, 10)
        self.assertFalse(stopped())
        with self.assertRaisesRegex(ValueError, "CALL_COUNT"):
            stopped.finish(2)

    def test_missing_seconds_and_true_abstention_are_separate(self):
        coverage = replay.Coverage(0, 300)
        coverage.tick(1)
        market = {"start": 0, "end": 300, "accepting": True, "closed": False, "metadata_received": 1}
        quotes = {side: {"bid": .4, "ask": .5} for side in ("Up", "Down")}
        for identity in replay.EXPECTED_IDS:
            coverage.evaluation({"evaluation_wall": 2, "profile": identity, "operation": "tick",
                                 "enabled": True, "entry_allowed": True, "bundle_id": "b1"}, market, quotes, 100000)
        result = coverage.result()
        self.assertEqual(result["missing_scheduled_tick_seconds"], 299)
        self.assertEqual(result["shared_fresh_eligible_5m_windows"], 1)
        self.assertTrue(all(row["true_abstention_5m_windows"] == 1 for row in result["per_profile"].values()))
        self.assertFalse(result["full_window_completion_inferred"])

    def test_raw_reference_hash_and_path_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / "raw.bin").write_bytes(b"{}")
            proofs = replay.RawProofs(root, 100)
            self.assertEqual(proofs.json(root, {"file": "raw.bin", "sha256": hashlib.sha256(b"{}").hexdigest()}), {})
            with self.assertRaisesRegex(ValueError, "PATH_ESCAPE"):
                proofs.json(root, {"file": "../elsewhere", "sha256": "a" * 64})
            with self.assertRaisesRegex(ValueError, "HASH_OR_SIZE"):
                proofs.json(root, {"file": "raw.bin", "sha256": "a" * 64})

    def test_spot_mark_is_reconstructed_from_raw_frame_and_cannot_be_substituted(self):
        from scripts.btc_parallel_paper.model import FeedHistory
        from scripts.short_day_slim.lean_marks import MarkHistory
        from collections import Counter
        import base64
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = replay.canonical({"type": "ticker", "product_id": "BTC-USD", "price": "100000",
                "trade_id": 1, "time": "1970-01-01T00:00:11Z"})
            record = {"seq": 1, "raw_b64": base64.b64encode(payload).decode(), "sha256": hashlib.sha256(payload).hexdigest(),
                      "receipt_wall": 11.1, "receipt_mono": 11.1, "binary": False}
            raw = replay.canonical(record) + b"\n"
            (root / "websocket-frames.jsonl").write_bytes(raw)
            tape = replay.WebsocketTape(root / "websocket-frames.jsonl", hashlib.sha256(raw).hexdigest(), 1, 10000)
            engine = object.__new__(replay.ProspectiveReplay)
            engine.websocket, engine.feed, engine.proxy = tape, FeedHistory(), MarkHistory()
            engine.decision_bound, engine.feed_rejections = 100, []
            engine.last_model_input, engine.counts = None, Counter()
            engine.coverage_evidence = replay.Coverage(0, 100)
            engine.feed.connection(10, True)
            engine._websocket({"data": {"raw_sequence": 1, "sha256": record["sha256"], "receipt_wall": 11.1, "raw_only": False}})
            mark = engine.feed.sample(12)
            self.assertEqual(mark["price"], 100000)
            data = {"second": 12, "mark": mark, "forecasts": {}, "construction_start_wall": 12.04,
                    "available_wall": 12.05, "construction_start_monotonic_ns": 12040000000,
                    "available_monotonic_ns": 12050000000}
            tampered = deepcopy(data); tampered["mark"]["price"] = 110000
            with self.assertRaisesRegex(ValueError, "RAW_SPOT_MARK_MISMATCH"):
                engine._model_input({"data": tampered, "utc": "1970-01-01T00:00:12.060000Z"})
            engine._model_input({"data": data, "utc": "1970-01-01T00:00:12.060000Z"})
            self.assertEqual(engine.marks[12], 100000)
            tape.finish(); tape.close()


class MissingFeatureDiagnosticTests(unittest.TestCase):
    weights = {"book_momentum": .25, "depth": .25, "prior_hour": .15, "spot30": .35}
    values = {"prior_hour": .8, "spot30": .8}

    def begin(self, evaluation=1, profile="5m-P13"):
        return {"evaluation_id": evaluation, "profile": profile, "market": "m", "evaluation_wall": 100.}

    def witness(self, evaluation=1, key="book_momentum", profile="5m-P13"):
        return {**self.begin(evaluation, profile), "begin_seq": 10, "end_seq": 13, "result_seq": 20,
                "result_row_sha256": "a" * 64, "last_problem": "INPUT:" + str(KeyError(key))}

    def test_direct_missing_key_witness_covers_later_callback_without_using_hashes(self):
        order = replay.MissingFeatureDiagnostics({1: self.witness()})
        self.assertEqual(order.select(self.weights, self.values, self.begin(), directly_attributable=True), "book_momentum")
        self.assertEqual(order.select(self.weights, self.values, self.begin(2), directly_attributable=False), "book_momentum")
        report = order.result()
        self.assertEqual(report["ambiguous_contexts"], 2)
        self.assertEqual(report["contexts_using_previously_witnessed_order"], 1)
        self.assertFalse(report["end_hash_or_parity_target_used_to_select"])

    def test_unwitnessed_or_incompletely_ordered_ambiguity_holds(self):
        order = replay.MissingFeatureDiagnostics({1: self.witness()})
        with self.assertRaisesRegex(replay.ReproducibilityHold, "UNRESOLVED"):
            order.select(self.weights, self.values, self.begin(2), directly_attributable=False)
        order.select(self.weights, self.values, self.begin(), directly_attributable=True)
        with self.assertRaisesRegex(replay.ReproducibilityHold, "UNRESOLVED"):
            order.select(self.weights, {"prior_hour": .8}, self.begin(3), directly_attributable=False)

    def test_contradictory_or_not_missing_plaintext_is_refused(self):
        order = replay.MissingFeatureDiagnostics({1: self.witness(), 2: self.witness(2, "depth")})
        order.select(self.weights, self.values, self.begin(), directly_attributable=True)
        with self.assertRaisesRegex(replay.ReproducibilityHold, "CONTRADICTION"):
            order.select(self.weights, self.values, self.begin(2), directly_attributable=True)
        invalid = replay.MissingFeatureDiagnostics({1: self.witness(key="spot30")})
        with self.assertRaisesRegex(replay.ReproducibilityHold, "NOT_GENUINELY_MISSING"):
            invalid.select(self.weights, self.values, self.begin(), directly_attributable=True)

    def test_mixed_missing_and_nonfinite_inputs_are_not_normalized(self):
        order = replay.MissingFeatureDiagnostics({1: self.witness()})
        with self.assertRaisesRegex(replay.ReproducibilityHold, "NONFINITE"):
            order.select(self.weights, {"spot30": float("nan"), "prior_hour": .8}, self.begin(), directly_attributable=True)

    def test_snapshot_cannot_witness_a_superseded_scheduled_evaluation(self):
        def event(kind, data, sequence):
            return {"kind": kind, "data": data, "seq": sequence, "sha256": str(sequence)}
        begin = {**self.begin(), "operation": "tick", "trigger": "SCHEDULED_TICK"}
        events = [event("CAUSAL_MODEL_INPUT", {"second": 100, "construction_start_wall": 100.}, 1),
                  event("DECISION_BEGIN", begin, 2),
                  event("DECISION_END", {"evaluation_id": 1, "profile": "5m-P13"}, 3)]
        result = event("MODEL_AND_PROFILE_TICK", {"second": 100, "construction_start_wall": 100.,
            "profile_rows": [{"id": "5m-P13", "last_problem": "INPUT:'book_momentum'"}]}, 6)
        self.assertEqual(replay.scheduled_diagnostic_witnesses(events + [result])[1]["result_seq"], 6)
        callback = event("DECISION_BEGIN", {**begin, "evaluation_id": 2, "trigger": "BOOK_CALLBACK"}, 4)
        end = event("DECISION_END", {"evaluation_id": 2, "profile": "5m-P13"}, 5)
        self.assertEqual(replay.scheduled_diagnostic_witnesses(events + [callback, end, result]), {})


ACTUAL_CONTEXT_SCRIPT = r'''
import json,sys
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,sys.argv[1]);sys.path.insert(0,sys.argv[2])
from scripts.lane_ii_continuous import qualification_replay as replay
sys.path.insert(0,sys.argv[1])
from roster import profile_specs,make_profile,profile_view,qualified_quotes
import continuous_paper_risk as risk
import continuous_decision_capture as decision
from model import FeedHistory
from lean_marks import MarkHistory
root=Path(sys.argv[3])
state={'schema':risk.SCHEMA,'domain':risk.DOMAIN,'campaign_id':'fixture','database_id':'fixture-db',
       'roster_sha256':'a'*64,'maximum_loss':'25','policy_sha256':risk.digest(risk.POLICY),
       'gross_losses':'0','paid_fees':'0','reservations':{},'holds':[],'phase':'ACTIVE',
       'segment_id':'fixture-segment','last_terminal':None,'carry_accounts':{}}
current={'now':1021.,'held':False}
def prior_for(now):
    return {'source':'COINBASE_PROXY_NOT_CHAINLINK','new_window_prices_used':False,'available_wall':now,
      'values':{'hour':.8},'references':{'hour':{'source_window_start':-3600,'source_window_end':0,
      'start_ref':{'kind':'COMPLETED_60S_CANDLE_OPEN','bucket_start':-3600,'available':0},
      'end_ref':{'kind':'COMPLETED_60S_CANDLE_CLOSE','bucket_start':-60,'available':0}}}}
class Prior:
    def direction(self,start,horizon,marks,now):return prior_for(now)
def features_for(market,quotes,history,marks,forecast,prior,now):
    history.append(now)
    return {'available_wall':now,'prior':prior,'values':{'spot30':.8,'book_momentum':.8,'depth':.8,'prior_hour':.8},'forecast':forecast}
helpers=SimpleNamespace(PriorHistory=Prior,live_features=features_for)
registry=SimpleNamespace(make_profile=make_profile,qualified_quotes=qualified_quotes,profile_view=profile_view)
context={'capture':root,'start':1000.,'cutoff':1400.,'decision_bound':1400.,
         'preregistration':{'paper_risk_initial_state':state,'deadline_mono':2000.}}
engine=replay.ProspectiveReplay(context,profile_specs(),registry,helpers,SimpleNamespace(),
    SimpleNamespace(FeedHistory=FeedHistory),SimpleNamespace(MarkHistory=MarkHistory),decision,risk,None,lambda row:None)
source_risk=replay.MemoryRiskLedger(risk,state)
recorded=[];controller=None
def source_emit(kind,data):
    recorded.append({'kind':kind,'data':deepcopy(data)})
    controller.financial_event(kind,data)
def clock_status():
    return {'state':'held' if current['held'] else 'accepted','reason':'FIXTURE_HOLD' if current['held'] else None,
      'anchor':{'monotonic':1000.,'utc_epoch':1000.,'host_epoch':1000.,'correction_seconds':0.,'boot_id':'fixture'},
      'anchor_age_seconds':current['now']-1000.,'accepted_revisions':1,'host_clock_modified':False}
controller=decision.DecisionCapture(SimpleNamespace(),source_risk,
    emit=lambda kind,data:recorded.append({'kind':kind,'data':deepcopy(data)}),
    hard_check=lambda:{'wall':current['now'],'host_wall':current['now'],'monotonic':current['now'],
                       'stop_event_set':False,'stop_requested':False,'hard_allowed':current['now']<1400},
    clock_status=clock_status,transport_status=lambda:{'state':'healthy','maximum_attempts_per_request':3,
                   'total_request_seconds':30.,'minimum_entry_spacing_seconds':.501})
profiles=[make_profile(spec,source_emit) for spec in profile_specs()]
assert {p.id for p in profiles}=={'5m-P13','5m-P2'}
for p in profiles:controller.attach_profile(p)
market={'id':'m','start':1000.,'end':1300.,'metadata_received':1021.,'fee_rate':.02,'closed':False,
        'accepting':True,'tokens':{'Up':'1','Down':'2'}}
sequence=0
def tick(now,bid=.50,ask=.51):
    global sequence
    current['now']=now;market['metadata_received']=now
    quote={'received':now,'source_timestamp':now,'request_entry_wall':now,'bid':bid,'ask':ask,'minimum':1,
           'bids':[(bid,1000)],'asks':[(ask,1000)],'raw_ref':{'file':'http-'+str(now)+'.bin','sha256':'a'*64}}
    quotes={'Up':quote};prior=prior_for(now)
    features=features_for(market,quotes,[],{},None,prior,now)
    engine.markets['m']=deepcopy(market);engine.books['m']=deepcopy(quotes);engine.marks[int(now)]=100000.
    engine.last_model_input={'second':int(now),'construction_start_wall':now}
    offset=len(recorded);bundle=controller.new_bundle()
    for p in profiles:controller.invoke_tick(p,market,quotes,deepcopy(features),deepcopy(prior),now,1400.,True,trigger='SCHEDULED_TICK',bundle_id=bundle)
    for item in recorded[offset:]:
        sequence+=1
        engine.consume({'seq':sequence,'kind':item['kind'],'data':item['data'],
           'utc':datetime.fromtimestamp(now+.001,tz=timezone.utc).isoformat()})
    engine.last_model_input=None
for now in range(1021,1032):tick(float(now))
assert [p.entries for p in profiles]==[1,1]
current['held']=True;tick(1032.,.30,.31);tick(1033.,.30,.31);tick(1034.)
assert [p.exits for p in profiles]==[1,1]
assert engine.risk.state==source_risk.state
assert all(decision.profile_state(a,'m')==decision.profile_state(engine.profiles[a.id],'m') for a in profiles)
assert all(len(engine.financial[p.id])==2 for p in profiles)
assert engine.admissions==28
assert any(row['action']=='PREPARE_ENTRY' and row['result']['snapshot']['unknown_reservations'] for row in engine.risk.actions)
assert risk.decimal(risk.snapshot(engine.risk.state)['gross_losses'])>0
assert all(p.phase=='NEW_EXPOSURE_HELD' for p in engine.profiles.values())
print(json.dumps({'contexts':engine.admissions,'financial_parity':True,'risk_parity':True,'true_clock_hold':True}))
'''


class ActualFrozenContextTests(unittest.TestCase):
    @unittest.skipUnless(Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5/SOURCE_MANIFEST.json").is_file(), "local frozen candidate fixture")
    def test_frozen_missing_feature_abort_preserves_both_native_error_orders(self):
        candidate = Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5")
        result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", MISSING_FEATURE_SCRIPT,
            str(candidate), str(Path(__file__).resolve().parents[1])], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertIn("both_native_orders_verified", result.stdout)

    @unittest.skipUnless(Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5/SOURCE_MANIFEST.json").is_file(), "local frozen candidate fixture")
    def test_actual_frozen_entries_exits_risk_and_clock_holds_replay_exactly(self):
        candidate = Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5")
        replay.verify_candidate(candidate, "d416ffb7ff68cf2620f6b3ddf13220bdb86dc914c5d73d114f9e98a3e18a1fe8")
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", ACTUAL_CONTEXT_SCRIPT,
                str(candidate), str(Path(__file__).resolve().parents[1]), directory], capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertIn('"financial_parity": true', result.stdout)

    @unittest.skipUnless(Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5/SOURCE_MANIFEST.json").is_file(), "local frozen candidate fixture")
    def test_whole_offline_adapter_binds_fixture_capture_and_writes_result(self):
        candidate = Path("N:/Beelzebub/runtime/lane-ii-continuous-rd/engineering/capture/candidate-v5")
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", WHOLE_RUN_SCRIPT,
                str(candidate), str(Path(__file__).resolve().parents[1]), directory], capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertIn('"whole_offline_fixture_pass": true', result.stdout)


MISSING_FEATURE_SCRIPT = r'''
import sys, types
from copy import deepcopy
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from scripts.lane_ii_continuous import qualification_replay as replay
guard=replay.offline_boundary();guard.__enter__()
candidate=Path(sys.argv[1]);shim,helpers,market_types=replay.retained.load_runtime(candidate)
registry=shim.bound_module('missing_feature_fixture_roster',candidate/'roster.py')
market={'id':'m','start':1000.,'end':1300.,'metadata_received':1021.,'fee_rate':.02,'closed':False,'accepting':True,'tokens':{'Up':'1','Down':'2'}}
prior={'source':'COINBASE_PROXY_NOT_CHAINLINK','new_window_prices_used':False,'available_wall':1021.,
 'values':{'hour':.8},'references':{'hour':{'source_window_start':-3600,'source_window_end':0,
 'start_ref':{'kind':'COMPLETED_60S_CANDLE_OPEN','bucket_start':-3600,'available':0},
 'end_ref':{'kind':'COMPLETED_60S_CANDLE_CLOSE','bucket_start':-60,'available':0}}}}
features={'available_wall':1021.,'prior':prior,'values':{'spot30':.8,'prior_hour':.8}}
begin={'evaluation_id':1,'profile':'5m-P13','market':'m','evaluation_wall':1021.,'operation':'tick','trigger':'SCHEDULED_TICK'}
witness={**begin,'begin_seq':1,'end_seq':4,'result_seq':9,'result_row_sha256':'a'*64,'last_problem':"INPUT:'book_momentum'"}
for first in ('depth','book_momentum'):
    profile=registry.make_profile(registry.profile_specs()[0],lambda *args:None)
    profile.ensure(market,prior,1400.,1021.)
    original=profile.sample.__func__
    # Fixture-only injection of the two legal set iteration orders; no source edit.
    keys=[first,*[k for k in profile.model['score_weights'] if k!=first]]
    clone=types.FunctionType(original.__code__,{**original.__globals__,'set':lambda _:keys},original.__name__,original.__defaults__,original.__closure__)
    profile.sample=types.MethodType(clone,profile)
    before=(deepcopy(profile.positions),profile.entries,profile.exits,profile.realized,profile.fees)
    order=replay.MissingFeatureDiagnostics({1:witness})
    order.attach(profile,lambda:begin,candidate)
    try:profile.sample(market,deepcopy(features),1021.)
    except KeyError as exc:assert str(exc)=="'book_momentum'",str(exc)
    else:raise AssertionError('Missing input must abort sampling')
    assert not profile.histories['m']
    assert before==(profile.positions,profile.entries,profile.exits,profile.realized,profile.fees)
    assert order.result()['ambiguous_contexts']==1
    malformed=deepcopy(features);del malformed['values']['prior_hour']
    try:profile.sample(market,malformed,1021.)
    except KeyError as exc:assert str(exc)=="'prior_hour'",str(exc)
    else:raise AssertionError('Prior validation error must remain unchanged')
    assert order.result()['ambiguous_contexts']==1
print('both_native_orders_verified')
'''


WHOLE_RUN_SCRIPT = r'''
import hashlib,json,sys
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from scripts.lane_ii_continuous import qualification_replay as replay
sys.path.insert(0,sys.argv[1])
from roster import profile_specs,make_profile,profile_view,REGISTRY
import continuous_paper_risk as risk
import continuous_decision_capture as decision
from model import FeedHistory
root=Path(sys.argv[3]);candidate=Path(sys.argv[1]);run=root/'run';capture=run/'capture';capture.mkdir(parents=True)
def utc(epoch):return datetime.fromtimestamp(epoch,tz=timezone.utc).isoformat()
def write(path,data):
    path.write_bytes(replay.canonical(data))
    return replay.binding(path)
source=replay.binding(candidate/'SOURCE_MANIFEST.json')
state={'schema':risk.SCHEMA,'domain':risk.DOMAIN,'campaign_id':'whole-fixture','database_id':'fixture-db',
       'roster_sha256':REGISTRY['roster_sha256'],'maximum_loss':'25.00','policy_sha256':risk.digest(risk.POLICY),
       'gross_losses':'0','paid_fees':'0','reservations':{},'holds':[],'phase':'ACTIVE',
       'segment_id':'fixture-segment','last_terminal':None,'carry_accounts':{}}
genesis=deepcopy(state);genesis.update(phase='GENESIS',segment_id=None)
anchor={'schema':'LANE_II_PAPER_RISK_ANCHOR_V1',**{k:state[k] for k in ('database_id','campaign_id','roster_sha256','maximum_loss','policy_sha256')},
        'policy':risk.POLICY,'genesis_state_sha256':risk.digest(genesis)}
counters={'real_orders':0,'model_api_calls':0,'authenticated_account_calls':0}
prereg={'source_manifest_sha256':source['sha256'],'paper_start_utc':utc(1001),'cutoff_utc':utc(1002),'deadline_mono':2000,
        'policy':{**counters,'virtual_cash_per_profile':3000},'profiles':profile_specs(),
        'decision_capture_schema':decision.SCHEMA,'paper_risk_initial_state':state,'initial_account_carryover':{},
        'paper_risk_anchor':anchor,'paper_risk_anchor_sha256':risk.digest(anchor)}
pre=write(capture/'preregistration.json',prereg)
profiles=[make_profile(spec,lambda *args:None,expected_starts=[900],initial_cash=3000) for spec in profile_specs()]
mark=FeedHistory().sample(1001)
model={'schema':decision.SCHEMA,'second':1001,'mark':mark,'forecasts':{},'construction_start_wall':1001.04,
       'available_wall':1001.05,'construction_start_monotonic_ns':1001040000000,'available_monotonic_ns':1001050000000}
events=[('START',1000,{'preregistration_sha256':pre['sha256'],'manifest_sha256':source['sha256'],'pid':123}),
        ('CAUSAL_MODEL_INPUT',1001.06,model),
        ('MODEL_AND_PROFILE_TICK',1001.08,{**model,'feature_available_wall':1001.07,'feature_available_monotonic_ns':1001070000000}),
        ('END',1002.01,{'state':'COMPLETE','reason':'DEADLINE','terminal_censoring':{p.id:[] for p in profiles}})]
previous='0'*64;raw=b''
for seq,(kind,at,data) in enumerate(events,1):
    row={'seq':seq,'utc':utc(at),'monotonic_ns':int(at*1000000000),'kind':kind,'data':data,'previous_sha256':previous}
    previous=hashlib.sha256(replay.canonical(row)).hexdigest();row['sha256']=previous;raw+=replay.canonical(row)+b'\n'
(capture/'events.jsonl').write_bytes(raw);(capture/'websocket-frames.jsonl').write_bytes(b'')
for p in profiles:(capture/(p.id+'-ledger.jsonl')).write_bytes(b'')
receipt={'source_manifest_sha256':source['sha256'],'preregistration_sha256':pre['sha256'],'state':'COMPLETE','reason':'DEADLINE',
         'pid':123,'paper_start_utc':utc(1001),'cutoff_utc':utc(1002),'utc':utc(1002.02),'actual_end_utc':utc(1002.03),
         'events':len(events),'chain_sha256':previous,'events_sha256':hashlib.sha256(raw).hexdigest(),'ws_frames':0,
         'websocket_sha256':hashlib.sha256(b'').hexdigest(),'profiles':[profile_view(p,{}, {},1002.02) for p in profiles],
         'ledger_sha256':{p.id:hashlib.sha256(b'').hexdigest() for p in profiles},'terminal_censoring':{p.id:[] for p in profiles},
         'paper_risk_terminal_state':state,'paper_risk':risk.snapshot(state),'account_carryover':decision.account_carryover(profiles),**counters}
receipt_binding=write(capture/'receipt.json',receipt)
endpoint={'receipt_path':str(capture/'receipt.json'),'receipt_sha256':receipt_binding['sha256'],'source_manifest_sha256':source['sha256'],
          'state':'COMPLETE','actual_end_utc':receipt['actual_end_utc'],'cutoff_utc':receipt['cutoff_utc']}
closed=deepcopy(state);closed.update(phase='CLOSED',last_terminal=endpoint,carry_accounts=receipt['account_carryover'])
write(run/'RISK_CLOSURE.json',{'schema':'LANE_II_PAPER_RISK_CLOSURE_V1','receipt':endpoint,'state':closed,
                            'snapshot':risk.snapshot(closed),'anchor_sha256':risk.digest(anchor)})
marker={'schema':'BTC_NEXT12_COLLECTION_CLOSED_V1','worker_pid':123,'source_manifest_sha256':source['sha256'],
        'fixed_cutoff_utc':utc(1002),'state_at_collection_close':'COMPLETE',**{key:True for key in
        ('collection_closed','network_closed','producers_exited','snapshot_publisher_closed','finalization_only_no_more_http')}}
write(capture/'COLLECTION_CLOSED.json',marker)
operator={'pid':123,'creation_epoch':999,'boot_epoch':1,'owned_job_before_startup_gate':True,'source_manifest_sha256':source['sha256'],
          'command':[sys.executable,'-I','-S','-B',str(candidate/'study.py'),'worker','--run-dir',str(capture),
                     '--paper-start-utc',utc(1001),'--cutoff-utc',utc(1002),'--startup-gate']}
write(run/'operator.json',operator)
write(run/'exit.json',{'pid':123,'creation_epoch':999,'boot_epoch':1,'source_manifest_sha256':source['sha256'],
      'exit_code':0,'forced_closure':False,'retained_handle_exit_observed':True,'owned_job_closed':True,'owned_children_at_parent_exit':[]})
write(root/'audit.json',{'schema':'LANE_II_READONLY_CAPTURE_AUDIT_V1','status':'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY',
      'problem_counts':{},'events_checked':len(events),'execution':{'state':'COMPLETE'},
      'input_bindings':[replay.binding(capture/'receipt.json'),replay.binding(capture/'COLLECTION_CLOSED.json')]})
paths={'receipt':capture/'receipt.json','preregistration':capture/'preregistration.json','collection_closed':capture/'COLLECTION_CLOSED.json',
       'worker_exit':run/'exit.json','worker_identity':run/'operator.json','raw_audit':root/'audit.json','source_manifest':candidate/'SOURCE_MANIFEST.json'}
write(root/'supervisor.json',{'schema':'LANE_II_PROSPECTIVE_SUPERVISOR_RESULT_V1','receipt':receipt,'worker_exit_code':0,'forced_closure':False,
      'bindings':{k:replay.binding(p) for k,p in paths.items()},'capture_audit':{'status':'PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY','problems':[],'events_checked':len(events)}})
result=replay.run(candidate,run,root/'audit.json',root/'supervisor.json',root/'output',expected_manifest_sha256=source['sha256'])
assert result['pass'] and result['risk_parity']['pass'] and result['risk_parity']['flat']
assert result['source_profiles_with_financial_parity']==result['source_profiles_with_recorded_decision_parity']==2
assert result['coverage']['missing_scheduled_tick_seconds']==0
assert result['admission_parity']['contexts']==0 and result['coverage']['shared_fresh_eligible_5m_windows']==0
assert not result['full_duration_qualified'] and not result['strategy_edge_qualified']
assert (root/'output/RESULT.json').is_file()
print(json.dumps({'whole_offline_fixture_pass':True,'no_trade_quota':True,'coverage_not_invented':True}))
'''


if __name__ == "__main__":
    unittest.main()
