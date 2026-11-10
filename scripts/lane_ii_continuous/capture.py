"""Freeze and observe a thin shared public collector; never launch a process.

The coordinator owns process handles, Windows jobs, startup release, resource
admission, stop escalation and the serial evidence queue.  This module only
prepares a source-bound derivative and supplies exact lifecycle adapters.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sys

BASE_MANIFEST_SHA256 = "6c8ad5ef440cb7b9b995c1b66dfb43b69685692a6772c2443c445514bfe5b9b5"
STARTUP_RELEASE = b"BTC_NEXT12_PAPER_START\n"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _load(path, maximum=8 * 1024**2):
    path = Path(path)
    if path.stat().st_size > maximum:
        raise ValueError("CAPTURE_DOCUMENT_BOUND")
    return json.loads(path.read_bytes())


def _new(path, value):
    with Path(path).open("xb") as stream:
        stream.write(_canonical(value))
        stream.flush()
        os.fsync(stream.fileno())


def _replace(text, old, new):
    if text.count(old) != 1:
        raise ValueError("SOURCE_PATCH_BINDING:" + old[:80])
    return text.replace(old, new)


ROSTER_ADAPTER = '''"""Two source-bound sizing alternatives on the same captured observations."""
from copy import deepcopy
import json
from pathlib import Path
import types
from next12_imports import bootstrap, bound_module
bootstrap()
from direction_gate import require_outcome
from quote_freshness import QUOTE_FRESHNESS_POLICY, qualified_quotes
HERE = Path(__file__).resolve().parent
CONTROL = bound_module('next12_optimized_controls', HERE / 'optimized_controls.py')
REGISTRY = json.loads((HERE / 'MINIMAL_ROSTER.json').read_bytes())
SPECS = REGISTRY['profiles']
ROSTER_VERSION = 'LANE_II_MINIMAL_SHARED_CAPTURE_V1'
EXPECTED_IDS = frozenset({'5m-P13', '5m-P2'})
ARMS = ('SHORT_DAY', 'MATCHED_BASELINE')
if set(row['id'] for row in SPECS) != EXPECTED_IDS or len(SPECS) != 2:
    raise ValueError('EXACT_MINIMAL_ROSTER_REQUIRED')
for row in SPECS:
    if row['horizon_seconds'] != 300 or row['allowed_entry_outcomes'] != ['Up', 'Down'] or row['model']['id'] != 'P2' or row['model']['requires_current_forecast'] is not False:
        raise ValueError('EXACT_BIDIRECTIONAL_P13_P2_REQUIRED')
def profile_specs(): return deepcopy(SPECS)
def retired_models(): return []
def requires_current_forecast(profile): return False
def requires_current_mark(profile): return True
def make_profile(spec, emit, live_check=lambda: True, expected_starts=(), initial_cash=3000):
    if spec not in SPECS: raise ValueError('UNREGISTERED_OR_CHANGED_PROFILE')
    original = next(model for model in CONTROL.MODELS if model['horizon_seconds'] == 300 and model['id'] == 'P2')
    profile = CONTROL.OptimizedControlProfile(300, deepcopy(original), emit, live_check, expected_starts, initial_cash)
    profile.id = spec['id']; profile.model = deepcopy(spec['model'])
    profile.roster_kind = spec['kind']; profile.roster_strategy_version = spec['strategy_version']
    profile.roster_lineage = deepcopy(spec['lineage'])
    profile.comparison_arm = spec['comparison_arm']; profile.control_id = spec['control_id']
    original_decision, original_candidate = profile.decision, profile.execution_candidate
    def decision(self, market, current, now):
        return require_outcome(original_decision(market, current, now))
    def candidate(self, market, quotes, signal, now, budget):
        require_outcome(signal)
        return original_candidate(market, quotes, signal, now, budget)
    profile.decision = types.MethodType(decision, profile)
    profile.execution_candidate = types.MethodType(candidate, profile)
    return profile
def profile_view(profile, markets, books, now):
    row = profile.view(markets, {key: qualified_quotes(value, now) for key, value in books.items()}, now)
    row.update(roster_kind=profile.roster_kind, strategy_version=profile.roster_strategy_version,
        lineage=deepcopy(profile.roster_lineage), comparison_arm=profile.comparison_arm, control_id=profile.control_id,
        direction_bias='NONE', allowed_entry_outcomes=['Up', 'Down'], display_model_id=profile.id.split('-')[-1],
        paper_only=True, compute_profile='SHARED_BTC5_P13_P2_NO_FORECAST_NO_COHORT_NO_VIEWER')
    return row
'''


def _patch_study(text, maximum_seconds):
    text = text.replace("\r\n", "\n")
    text = _replace(text, "from next12_imports import bootstrap", "import continuous_clock\nimport continuous_transport\nimport continuous_decision_capture\nimport continuous_paper_risk\nimport continuous_capture_checkpoint\nfrom next12_imports import bootstrap")
    text = _replace(text, "QUOTE_FRESHNESS_POLICY,profile_view", "QUOTE_FRESHNESS_POLICY,profile_view,REGISTRY")
    # These are duration/evidence bounds, not changes to signal, fill or fees.
    multiplier = math.ceil(maximum_seconds / 10800)
    text = _replace(text, "'maximum_paper_seconds':10800", "'maximum_paper_seconds':" + str(maximum_seconds))
    text = _replace(text, "'public_http_requests':24576,'raw_bytes':512*1024**2,'total_bytes':2*1024**3",
                    "'public_http_requests':" + str(24576 * multiplier) + ",'raw_bytes':" + str(512 * 1024**2 * multiplier) + ",'total_bytes':" + str(2 * 1024**3 * multiplier))
    text = _replace(text, "'minimum_free_bytes':20*1024**3", "'minimum_free_bytes':80*1024**3")
    text = _replace(text, "'ws_connections':36", "'ws_connections':" + str(36 * multiplier))
    text = _replace(text, "'ws_frames':3000000", "'ws_frames':" + str(3000000 * multiplier))
    text = _replace(text, "  self.ledgers={};self.profiles=[]", "  self.transport_control=continuous_transport.RetryController()\n  self.decision_capture=continuous_decision_capture.DecisionCapture.from_study(self,REGISTRY['roster_sha256'])\n  self.ledgers={};self.profiles=[]")
    text = _replace(text, "self.profiles.append(profile)", "self.profiles.append(profile);self.decision_capture.attach_profile(profile)")
    text = _replace(text, "  new(self.root/'preregistration.json',", "  self.checkpoint_writer=continuous_capture_checkpoint.CheckpointWriter.from_study(self)\n  new(self.root/'preregistration.json',")
    text = _replace(text, "'compute_profile':'SHORT_DAY_SLIM_SINGLE_BTC5'", "'compute_profile':'SHARED_BTC5_P13_P2_NO_FORECAST_NO_COHORT_NO_VIEWER'")
    text = _replace(text, "'profiles':self.roster,", "'profiles':self.roster,'cash_control':{'id':'cash','kind':'ABSTAIN','virtual_initial_cash':3000,'entries':0,'fees':0},'decision_capture_schema':continuous_decision_capture.SCHEMA,'paper_risk_initial_state':self.decision_capture.initial_state,'paper_risk_anchor':deepcopy(self.decision_capture.ledger.anchor),'paper_risk_anchor_sha256':file_sha(self.decision_capture.ledger.manifest_path),'initial_account_carryover':self.decision_capture.initial_accounts,'capture_checkpoint_policy':self.checkpoint_writer.policy,")
    text = _replace(text, "'capture_checkpoint_policy':self.checkpoint_writer.policy,", "'capture_checkpoint_policy':self.checkpoint_writer.policy,'paper_risk_initial_action_cursor':self.decision_capture.ledger.action_cursor(),")
    old_participation = next(line for line in text.splitlines() if "'participation':'The sole" in line)
    text = _replace(text, old_participation, "   'participation':'Two selective paper sizing alternatives share exact observations and one cumulative25USD simulation risk cap. No mandatory entries, topups or automatic parameter changes; only bound prior simulated accounting may carry across verified-flat segments.',")
    old_execution = next(line for line in text.splitlines() if "'execution':'Original paper" in line)
    text = _replace(text, old_execution, "   'execution':'Frozen P13 and optimized P2 signals retain depth walk, adverse slippage, rounded modeled fees, entry delay, intent expiry and five-second source/receipt freshness. Recorded clock/transport/risk admission holds new exposure while owned exits and dual-proof settlements remain possible. Durable shared reservations precede intent acceptance.',")
    old_comparison = next(line for line in text.splitlines() if "'comparison':{'arms':1" in line)
    text = _replace(text, old_comparison, "   'comparison':{'arms':2,'profiles':2,'cash_control_nonexecuting':True,'retained_control_economics_unchanged':True,'same_evaluation_timestamp_and_inputs':True,'sizing':'P13_BUDGET5_VS_EXACT_OPTIMIZED_P2_BUDGET6','counterfactual_accounts_nonadditive':True},")
    text = _replace(text, " def put(self,kind,data):", " def clock_ready(self):return continuous_clock.decision_allowed()\n\n def decision_live(self):return self.live() and self.clock_ready() and self.transport_control.healthy\n\n def put(self,kind,data):")
    text = _replace(text, "seq=self.log.emit(kind,data);self.ledgers[data['profile']].write(canonical({'event_seq':seq,'kind':kind,'data':data})+b'\\n')", "seq=self.log.emit(kind,data);self.ledgers[data['profile']].write(canonical({'event_seq':seq,'kind':kind,'data':data})+b'\\n')\n  self.decision_capture.financial_event(kind,data)")
    first, last = text.index(" def http_receiver(self):"), text.index(" def ws_receiver(self):")
    text = text[:first] + " def http_receiver(self):\n  continuous_transport.http_receiver(self,fetch,POLICY)\n\n" + text[last:]
    text = _replace(text, "  for attempt in range(1,POLICY['ws_connections']+1):\n   if not self.live():return", "  for attempt in range(1,POLICY['ws_connections']+1):\n   while self.live() and not self.clock_ready():self.stop.wait(.1)\n   if not self.live():return")
    text = _replace(text, "  job=data['job'];self.pending.discard(job['key'])", "  job=data['job']\n  if data.get('attempt_terminal',True):self.pending.discard(job['key'])")
    text = _replace(text, "'received_mono':data['received_mono'],'raw_only':late,**response", "'received_mono':data['received_mono'],'received_host_wall':data.get('received_host_wall'),'raw_only':late,**response")
    text = _replace(text, "'receipt_mono':time.monotonic(),'binary':not isinstance(message,str)", "'receipt_mono':time.monotonic(),'receipt_host_wall':continuous_clock.RAW_TIME(),'binary':not isinstance(message,str)")
    text = _replace(text, "'entry_wall':entry,'entry_mono':mono})", "'entry_wall':entry,'entry_mono':mono,'entry_host_wall':continuous_clock.RAW_TIME()})")
    text = _replace(text, "  features['forecast']=forecast\n  for profile", "  features['forecast']=forecast\n  bundle_id=self.decision_capture.new_bundle()\n  for profile")
    text = _replace(text, "   profile.tick(m,quotes,deepcopy(features),deepcopy(prior),now,self.cutoff,self.paper_start<=now<self.cutoff)", "   self.decision_capture.invoke_tick(profile,m,quotes,deepcopy(features),deepcopy(prior),now,self.cutoff,self.paper_start<=now<self.cutoff,trigger='BOOK_CALLBACK',bundle_id=bundle_id)")
    text = _replace(text, "  enabled=begin_wall>=self.paper_start", "  self.checkpoint_writer.model_started(second)\n  self.log.emit('CAUSAL_MODEL_INPUT',{'schema':continuous_decision_capture.SCHEMA,'second':second,'mark':mark,'forecasts':forecasts,'construction_start_wall':begin_wall,'available_wall':time.time(),'construction_start_monotonic_ns':begin_mono,'available_monotonic_ns':time.monotonic_ns()})\n  enabled=begin_wall>=self.paper_start")
    text = _replace(text, "  snapshot_bin=second//POLICY['snapshot_seconds']", "  self.checkpoint_writer.model_finished(second)\n  snapshot_bin=second//POLICY['snapshot_seconds']")
    text = _replace(text, "    if begin_wall>=window['end']:profile.finalize_window(key)", "    if begin_wall>=window['end']:self.decision_capture.invoke_finalize(profile,key,self.markets[key],now=begin_wall,bundle_id=self.decision_capture.new_bundle())")
    text = _replace(text, "    input_bundles[key]=(prior,quotes,features)", "    input_bundles[key]=(prior,quotes,features,self.decision_capture.new_bundle())")
    text = _replace(text, "   prior,quotes,features=input_bundles[key]", "   prior,quotes,features,bundle_id=input_bundles[key]")
    text = _replace(text, "   profile.tick(m,quotes,deepcopy(features),deepcopy(prior),begin_wall,self.cutoff,enabled and self.live())", "   self.decision_capture.invoke_tick(profile,m,quotes,deepcopy(features),deepcopy(prior),begin_wall,self.cutoff,enabled and self.live(),trigger='SCHEDULED_TICK',bundle_id=bundle_id)")
    text = _replace(text, "     for profile in self.profiles:profile.settlement(m['id'],g['winner'],m,received,[g['raw_ref'],ref])", "     bundle_id=self.decision_capture.new_bundle()\n     for profile in self.profiles:self.decision_capture.invoke_settlement(profile,m['id'],g['winner'],m,received,[g['raw_ref'],ref],bundle_id=bundle_id)")
    text = _replace(text, "    for key in profile.windows:profile.finalize_window(key)", "    for key in profile.windows:self.decision_capture.invoke_finalize(profile,key,self.markets[key],now=time.time(),bundle_id=self.decision_capture.new_bundle(),terminal=True)")
    text = _replace(text, "   'counterfactual_profiles_not_additive_portfolio':True}", "   'counterfactual_profiles_not_additive_portfolio':True,'clock_control':continuous_clock.status(),'transport_control':self.transport_control.status(),'network_boundary':continuous_transport.boundary_status(),'paper_risk':continuous_paper_risk.snapshot(self.decision_capture.ledger.state),'cash_control':{'id':'cash','kind':'ABSTAIN','virtual_initial_cash':3000,'virtual_cash':3000,'entries':0,'fees':0}}")
    text = _replace(text, "   receipt={**status,'started_utc'", "   receipt={**status,'paper_risk_terminal_state':deepcopy(self.decision_capture.ledger.state),'account_carryover':continuous_decision_capture.account_carryover(self.profiles,self.decision_capture.initial_accounts),'started_utc'")
    text = _replace(text, "     sync_at=time.monotonic()+2", "     self.checkpoint_writer.boundary(recovery=True)\n     sync_at=time.monotonic()+2")
    text = _replace(text, "    self.stop.wait(.02)", "    self.checkpoint_writer.boundary()\n    self.stop.wait(.02)")
    text = _replace(text, "   self.log.close();self.ws_file.flush();os.fsync(self.ws_file.fileno());self.ws_file.close()", "   self.checkpoint_writer.finalize(producers_exited=not any(t.is_alive() for t in (http,ws)))\n   self.log.close();self.ws_file.flush();os.fsync(self.ws_file.fileno());self.ws_file.close()")
    text = _replace(text, "   new(self.root/'receipt.json',receipt)", "   if self.checkpoint_writer.final_binding:receipt['capture_final_checkpoint']=self.checkpoint_writer.final_binding\n   new(self.root/'receipt.json',receipt)")
    text = _replace(text, "   new(self.root/'receipt.json',receipt)", "   new(self.root/'receipt.json',receipt)\n   self.decision_capture.finish(self.root/'receipt.json',self.profiles)")
    text = _replace(text, "choices=('supervise','worker','stop')", "choices=('worker',)")
    text = _replace(text, "  return Study(args.run_dir,args.paper_start_utc,args.cutoff_utc).run()", "  continuous_clock.install()\n  continuous_transport.install_boundary('worker',HERE/'http_helper.py')\n  return Study(args.run_dir,args.paper_start_utc,args.cutoff_utc).run()")
    return text


def _patch_profile_hooks(text):
    text = text.replace("\r\n", "\n")
    text = _replace(text, "        if pending is None:\n            window['pending'] = {'signal_wall': now, 'available_after': now + 1,", "        if pending is None:\n            if hasattr(self,'before_intent') and not self.before_intent(m,quote,fill,signal,now,budget):return\n            window['pending'] = {'signal_wall': now, 'available_after': now + 1,")
    text = _replace(text, "        position = {'market': m['id'], 'side': signal['side'],", "        if hasattr(self,'before_entry') and not self.before_entry(m,quote,fill,signal,now,budget):return\n        position = {'market': m['id'], 'side': signal['side'],")
    return text


def _patch_helper(text):
    text = text.replace("\r\n", "\n")
    text = _replace(text, "from runtime_guard import Job,canonical,process_identity,verify_sources", "from runtime_guard import Job,canonical,process_identity,verify_sources\nimport continuous_clock\nimport continuous_transport")
    first, last = text.index("def allowed(url):"), text.index("def net():")
    text = text[:first] + "def allowed(url):return continuous_transport.allowed(url)\n\n\n" + text[last:]
    text = _replace(text, "def net():\n", "def net():\n    verify_sources()\n    continuous_clock.install()\n    continuous_transport.install_boundary('helper')\n")
    text = _replace(text, "opener=urllib.request.build_opener(NoRedirect())", "opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())")
    text = _replace(text, "started=False;entry=mono=None", "started=False;entry=mono=entry_host_wall=None")
    text = _replace(text, "entry,mono=time.time(),time.monotonic()", "entry,mono=time.time(),time.monotonic();entry_host_wall=continuous_clock.RAW_TIME()")
    text = _replace(text, "    sys.stdout.buffer.write(canonical(out));sys.stdout.buffer.flush()", "    out['network_boundary']=continuous_transport.boundary_status()\n    out['clock_control']=continuous_clock.status()\n    out['entry_host_wall']=entry_host_wall\n    out['end_host_wall']=continuous_clock.RAW_TIME()\n    sys.stdout.buffer.write(canonical(out));sys.stdout.buffer.flush()")
    first = text.index("        timeout=min(8,")
    last = text.index("        if process.returncode:", first)
    text = text[:first] + "        raw,err=continuous_transport.communicate_bounded(process,canonical({'url':url,'cutoff_wall':cutoff,'deadline_mono':deadline,'stop_path':str(stop_path)})+b'\\n',cutoff=cutoff,deadline=deadline,stop_path=stop_path,job=job)\n" + text[last:]
    return text


def verify_candidate(candidate):
    directory = Path(candidate["candidate_dir"] if isinstance(candidate, dict) else candidate).resolve()
    manifest = _load(directory / "SOURCE_MANIFEST.json")
    manifest_sha = _sha((directory / "SOURCE_MANIFEST.json").read_bytes())
    if isinstance(candidate, dict) and manifest_sha != candidate["source_manifest_sha256"]:
        raise ValueError("DERIVED_SOURCE_MANIFEST_CHANGED")
    for name, expected in manifest["files"].items():
        path = (directory / name).resolve()
        if not path.is_relative_to(directory) or _sha(path.read_bytes()) != expected:
            raise ValueError("DERIVED_SOURCE_CHANGED:" + name)
    return manifest_sha


def prepare_candidate(base_candidate, candidate_dir, roster_manifest=None, *,
                      expected_base_sha256=BASE_MANIFEST_SHA256, max_paper_seconds=86400):
    from scripts.lane_ii_continuous.hypotheses import freeze_minimal_roster
    source, target = Path(base_candidate).resolve(), Path(candidate_dir).resolve()
    if target == source or target.is_relative_to(source) or source.is_relative_to(target):
        raise ValueError("DERIVED_CANDIDATE_REQUIRES_DISTINCT_DIRECTORY")
    if not isinstance(max_paper_seconds, int) or not 1 <= max_paper_seconds <= 86400:
        raise ValueError("MAXIMUM_PAPER_DURATION_BOUND")
    source_bytes = (source / "SOURCE_MANIFEST.json").read_bytes()
    if _sha(source_bytes) != expected_base_sha256:
        raise ValueError("BASE_SOURCE_MANIFEST_CHANGED")
    manifest = json.loads(source_bytes)
    expected_roster = freeze_minimal_roster(source, expected_base_sha256)
    roster = expected_roster if roster_manifest is None else _load(roster_manifest) if isinstance(roster_manifest, (str, Path)) else roster_manifest
    if roster != expected_roster:
        raise ValueError("EXACT_FROZEN_MINIMAL_ROSTER_REQUIRED")
    # Verify every copied source before writing; no historical runtime import.
    original = {}
    total = 0
    for name, expected in manifest["files"].items():
        path = (source / name).resolve()
        if not path.is_relative_to(source) or path.is_symlink():
            raise ValueError("BASE_SOURCE_PATH_ESCAPE")
        total += path.stat().st_size
        if total > 32 * 1024**2:
            raise ValueError("SOURCE_COPY_BYTE_BOUND")
        raw = path.read_bytes()
        if _sha(raw) != (expected if isinstance(expected, str) else expected["sha256"]):
            raise ValueError("BASE_SOURCE_CHANGED:" + name)
        original[name] = raw
    derived = dict(original)
    derived["BASE_SOURCE_MANIFEST.json"] = source_bytes
    derived["MINIMAL_ROSTER.json"] = _canonical(roster)
    derived["slim_roster.py"] = ROSTER_ADAPTER.encode()
    derived["study.py"] = _patch_study(original["study.py"].decode(), max_paper_seconds).encode()
    derived["http_helper.py"] = _patch_helper(original["http_helper.py"].decode()).encode()
    derived["homerun_strategies.py"] = _patch_profile_hooks(original["homerun_strategies.py"].decode()).encode()
    for name in ("clock", "transport", "paper_risk", "decision_capture", "capture_checkpoint"):
        derived["continuous_" + name + ".py"] = Path(__file__).with_name(name + ".py").read_bytes()
    for name, raw in derived.items():
        if name.endswith(".py"):
            compile(raw, name, "exec")
    changes = [{"path": name, "before_sha256": _sha(original[name]) if name in original else None,
                "after_sha256": _sha(raw)} for name, raw in sorted(derived.items()) if original.get(name) != raw]
    patches = {"schema": "LANE_II_CAPTURE_PATCH_PROVENANCE_V1", "base_source_manifest_sha256": expected_base_sha256,
               "adapter_source_sha256": _sha(Path(__file__).read_bytes()), "patches": changes,
               "source_economics_changed": False, "registered_sizing_difference": "P13_5_USD_VS_P2_6_USD",
               "changes": ["shared minimal roster", "bounded transient observation retries", "public GET/DNS/socket boundary",
                           "renewable process clock and decision holds", "coordinator-owned direct worker",
                           "duration-scaled evidence bounds and 80GiB free-space floor",
                           "observable decision contexts and hard lifecycle checks", "separate cumulative shared25USD international paper risk campaign",
                           "opt-in durable 15-minute producer checkpoints and typed flat crash handoff"]}
    derived["CAPTURE_PATCHES.json"] = _canonical(patches)
    derived_manifest = {"schema": "LANE_II_CONTINUOUS_FROZEN_CAPTURE_V1", "base_manifest_sha256": expected_base_sha256,
                        "files": {name: _sha(raw) for name, raw in sorted(derived.items())},
                        "config": {"maximum_paper_seconds": max_paper_seconds, "profile_count": 2,
                                   "profile_ids": ["5m-P13", "5m-P2"], "nonexecuting_cash_control": True,
                                   "single_public_http_receiver": True, "single_spot_stream": True,
                                   "clock_control_supported": True, "clock_refresh_seconds": 300,
                                   "clock_maximum_age_seconds": 600, "clock_continuity_seconds": .2,
                                   "decision_capture_schema": "LANE_II_DECISION_CAPTURE_V1", "paper_risk_schema": "LANE_II_SHARED_PAPER_RISK_V1",
                                   "capture_checkpoint_schema": "LANE_II_CAPTURE_CHECKPOINT_V1", "capture_checkpoint_opt_in": True,
                                   "minimum_free_bytes": 80 * 1024**3, "hourly_sealed_partitions_supported": False,
                                   "model_api_calls": 0, "authenticated_account_calls": 0, "real_orders": 0}}
    derived["SOURCE_MANIFEST.json"] = _canonical(derived_manifest)
    result = {"schema": "LANE_II_FORWARD_CANDIDATE_V1", "candidate_dir": str(target),
              "source_manifest_sha256": _sha(derived["SOURCE_MANIFEST.json"]), "base_manifest_sha256": expected_base_sha256,
              "roster_sha256": roster["roster_sha256"], "maximum_paper_seconds": max_paper_seconds,
              "clock_control_supported": True, "clock_control_env": "LANE_II_CLOCK_CONTROL",
              "clock_boot_env": "LANE_II_BOOT_ID", "startup_release": STARTUP_RELEASE.decode(),
              "startup_release_requires_owned_job": True, "hourly_sealed_partitions_supported": False,
              "decision_capture_schema": "LANE_II_DECISION_CAPTURE_V1", "paper_risk_schema": "LANE_II_SHARED_PAPER_RISK_V1",
              "capture_checkpoint_schema": "LANE_II_CAPTURE_CHECKPOINT_V1", "capture_checkpoint_env": "LANE_II_CAPTURE_CHECKPOINT_SECONDS",
              "capture_recovery_sync_env": "LANE_II_CAPTURE_RECOVERY_SYNC",
              "capture_checkpoint_origin_env": "LANE_II_CAPTURE_CHECKPOINT_ORIGIN_EPOCH",
              "risk_anchor_required": True,
              "paper_only": True, "strategy_edge_qualified": False}
    if target.exists():
        if not (target / "SOURCE_MANIFEST.json").exists() or verify_candidate(result) != result["source_manifest_sha256"]:
            raise ValueError("EXISTING_DERIVED_CANDIDATE_MISMATCH")
        return result
    target.mkdir(parents=True, exist_ok=False)
    for name, raw in derived.items():
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    verify_candidate(result)
    return result


def _stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def build_forward_command(candidate_manifest, run_dir, start_utc, cutoff_utc):
    verify_candidate(candidate_manifest)
    seconds = _stamp(cutoff_utc) - _stamp(start_utc)
    if not 0 < seconds <= candidate_manifest["maximum_paper_seconds"]:
        raise ValueError("FORWARD_INTERVAL_BOUND")
    root = Path(run_dir).resolve()
    if (root / "capture").exists():
        raise ValueError("CAPTURE_ALREADY_EXISTS_NO_RESTART")
    return [sys.executable, "-I", "-S", "-B", str(Path(candidate_manifest["candidate_dir"]) / "study.py"),
            "worker", "--run-dir", str(root / "capture"), "--paper-start-utc", start_utc,
            "--cutoff-utc", cutoff_utc, "--startup-gate"]


def forward_manifest(candidate_manifest, run_dir, start_utc, cutoff_utc):
    argv = build_forward_command(candidate_manifest, run_dir, start_utc, cutoff_utc)
    return {"schema": "LANE_II_FORWARD_RUN_V1", "run_dir": str(Path(run_dir).resolve()),
            "candidate": candidate_manifest, "command": argv, "paper_start_utc": start_utc, "cutoff_utc": cutoff_utc,
            "planned_seconds": _stamp(cutoff_utc) - _stamp(start_utc), "execution": {"state": "queued"},
            "coverage": {"designation": "NONE", "observed_seconds": 0},
            "closure": {"capture_sealed": False, "integrity_audited": False, "accounting_reconciled": False},
            "accounts_nonadditive": True, "restart_after_failure_allowed": False}


def request_stop(run_dir, reason):
    capture = Path(run_dir).resolve() / "capture"
    if not (capture / "preregistration.json").is_file():
        raise ValueError("EXACT_EXISTING_CAPTURE_REQUIRED")
    path = capture / "STOP"
    if not path.exists():
        _new(path, {"schema": "LANE_II_COORDINATOR_STOP_V1", "utc": datetime.now(timezone.utc).isoformat(), "reason": str(reason)})
    return str(path)


def read_terminal(run_dir):
    root = Path(run_dir).resolve()
    capture = root / "capture"
    if not (capture / "receipt.json").exists():
        return None
    receipt = _load(capture / "receipt.json")
    marker = _load(capture / "COLLECTION_CLOSED.json") if (capture / "COLLECTION_CLOSED.json").exists() else {}
    sealed = (marker.get("schema") == "BTC_NEXT12_COLLECTION_CLOSED_V1" and marker.get("collection_closed") is True
              and marker.get("network_closed") is True and marker.get("producers_exited") is True
              and marker.get("snapshot_publisher_closed") is True and marker.get("worker_pid") == receipt.get("pid")
              and marker.get("finalization_only_no_more_http") is True
              and marker.get("source_manifest_sha256") == receipt.get("source_manifest_sha256")
              and marker.get("fixed_cutoff_utc") == receipt.get("cutoff_utc"))
    end = _stamp(receipt["actual_end_utc"])
    start, cutoff = _stamp(receipt["paper_start_utc"]), _stamp(receipt["cutoff_utc"])
    observed = max(0, min(end, cutoff) - start)
    full = receipt["state"] == "COMPLETE" and end >= cutoff and sealed
    return {"schema": "LANE_II_FORWARD_TERMINAL_V1", "receipt": receipt,
            "execution": {"state": receipt["state"].lower(), "reason": receipt.get("reason")},
            "coverage": {"planned_seconds": cutoff - start, "observed_seconds": observed,
                         "designation": "DURATION_REACHED_EVIDENCE_PENDING" if full else "PARTIAL", "full_duration_qualified": False},
            "closure": {"capture_sealed": sealed, "integrity_audited": False, "accounting_reconciled": False,
                        "supervisor_exit_verified": False, "analysis_published": False},
            "accounts": receipt.get("profiles", []), "cash_control": receipt.get("cash_control"),
            "strategy_edge_qualified": False}


_SNAPSHOTS = None


def capture_state(run_dir, last_snapshot_sequence=0):
    terminal = read_terminal(run_dir)
    if terminal is not None:
        return terminal
    global _SNAPSHOTS
    if _SNAPSHOTS is None:
        from scripts.overnight_paper.snapshot_lookup import LatestSnapshot
        _SNAPSHOTS = LatestSnapshot()
    path = _SNAPSHOTS.latest(run_dir)
    if path is None:
        return {"execution": {"state": "starting"}, "closure": {"capture_sealed": False}, "snapshot_sequence": last_snapshot_sequence}
    value = _load(path)
    return {"execution": {"state": value["state"].lower(), "reason": value.get("reason")},
            "snapshot_sequence": int(path.stem[1:]), "snapshot_path": str(path), "snapshot": value,
            "closure": {"capture_sealed": False, "integrity_audited": False, "accounting_reconciled": False}}
