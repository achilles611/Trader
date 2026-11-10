"""Exercise original P13/P2 signals through prospective admission wrappers."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.lane_ii_continuous.capture import prepare_candidate

BASE = Path("N:/Beelzebub/runtime/lane-ii-prediction-markets/short-day-slim-3h-preparation-20261105/candidate-v2/study")

SCRIPT = r'''
import json,sys
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,sys.argv[1])
from roster import profile_specs,make_profile,REGISTRY
import continuous_paper_risk as risk
from continuous_decision_capture import DecisionCapture,account_carryover
root=Path(sys.argv[2])
anchor=risk.initialize_campaign(root/'campaign.sqlite',root/'ANCHOR.json',campaign_id='fixture',roster_sha256=REGISTRY['roster_sha256'])
ledger=risk.RiskLedger(anchor['path'],anchor['sha256'],roster_sha256=REGISTRY['roster_sha256'])
ledger.begin_segment('qualification-fixture')
events=[]
clock={'now':1021.,'held':False}
class Study:
    cutoff=1400.;deadline=2000.;root=root
    def profile_event(self,kind,data):
        events.append((kind,data))
        self.decision_capture.financial_event(kind,data)
study=Study()
study.log=SimpleNamespace(emit=lambda kind,data:events.append((kind,data)))
study.decision_capture=DecisionCapture(study,ledger,
    hard_check=lambda:{'wall':clock['now'],'host_wall':clock['now'],'monotonic':clock['now'],'stop_event_set':False,'stop_requested':False,'hard_allowed':clock['now']<1400},
    clock_status=lambda:{'state':'held' if clock['held'] else 'accepted'},transport_status=lambda:{'state':'healthy'})
profiles=[make_profile(spec,study.profile_event) for spec in profile_specs()]
for profile in profiles:study.decision_capture.attach_profile(profile)
market={'id':'m','start':1000.,'end':1300.,'metadata_received':1021.,'fee_rate':.02,'closed':False,'accepting':True,'tokens':{'Up':'1','Down':'2'}}
def inputs(now,bid=.50,ask=.51):
    market['metadata_received']=now
    quote={'received':now,'source_timestamp':now,'request_entry_wall':now,'bid':bid,'ask':ask,'minimum':1,'bids':[(bid,1000)],'asks':[(ask,1000)],'raw_ref':{'file':'http-'+str(now)+'.bin','sha256':'a'*64}}
    prior={'source':'COINBASE_PROXY_NOT_CHAINLINK','new_window_prices_used':False,'available_wall':now,'values':{'hour':.8},'references':{'hour':{'source_window_start':-3600,'source_window_end':0,'start_ref':{'kind':'COMPLETED_60S_CANDLE_OPEN','bucket_start':-3600,'available':0},'end_ref':{'kind':'COMPLETED_60S_CANDLE_CLOSE','bucket_start':-60,'available':0}}}}
    features={'available_wall':now,'prior':prior,'values':{'spot30':.8,'book_momentum':.8,'depth':.8,'prior_hour':.8}}
    return {'Up':quote},features,prior
def tick(now,bid=.50,ask=.51):
    clock['now']=now
    quotes,features,prior=inputs(now,bid,ask)
    bundle=study.decision_capture.new_bundle()
    for profile in profiles:
        study.decision_capture.invoke_tick(profile,market,quotes,features,prior,now,1400.,True,trigger='SCHEDULED_TICK',bundle_id=bundle)
for now in range(1021,1032):tick(float(now))
assert [p.entries for p in profiles]==[1,1],[(p.id,p.entries,p.phase,p.last_problem) for p in profiles]
assert all(p.positions for p in profiles)
assert risk.decimal(risk.snapshot(ledger.state)['consumed_or_reserved'])<=25
for index,(kind,data) in enumerate(events):
    if kind=='PAPER_ENTRY_INTENT':
        earlier=[d for k,d in events[:index] if k=='PAPER_RISK_ACTION' and d['action']=='RESERVE_INTENT' and d['payload']['profile']==data['profile']]
        assert earlier and earlier[-1]['result']['accepted']
    if kind=='PAPER_ENTRY':
        earlier=[d for k,d in events[:index] if k=='PAPER_RISK_ACTION' and d['action']=='PREPARE_ENTRY' and d['payload']['profile']==data['profile']]
        assert earlier and earlier[-1]['result']['snapshot']['unknown_reservations']
clock['held']=True
tick(1032.,.30,.31)
tick(1033.,.30,.31)
assert [p.exits for p in profiles]==[1,1],[(p.id,p.exits,p.phase,p.last_problem) for p in profiles]
assert not ledger.state['reservations']
tick(1034.)
assert any(k=='DECISION_SKIPPED' and 'CLOCK_CONFIDENCE_HOLD' in d['reason'] for k,d in events)
assert all(p.phase=='NEW_EXPOSURE_HELD' for p in profiles)
assert sum(k=='DECISION_BEGIN' for k,d in events)==sum(k=='DECISION_END' for k,d in events)
assert any(k=='DECISION_GATE_CHECK' for k,d in events)
assert risk.decimal(risk.snapshot(ledger.state)['gross_losses'])>0
print(json.dumps({'entries':[p.entries for p in profiles],'exits':[p.exits for p in profiles],'risk':risk.snapshot(ledger.state),'context_count':sum(k=='DECISION_BEGIN' for k,d in events)}))
ledger.close()
'''


class DecisionCaptureTests(unittest.TestCase):
    @unittest.skipUnless((BASE / "SOURCE_MANIFEST.json").exists(), "retained source fixture is local to Atlas")
    def test_actual_frozen_models_reserve_before_intents_and_exit_during_clock_hold(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate = prepare_candidate(BASE, root / "candidate")
            result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", SCRIPT, candidate["candidate_dir"], str(root)],
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertIn('"entries": [1, 1]', result.stdout)
            self.assertIn('"exits": [1, 1]', result.stdout)


if __name__ == "__main__":
    unittest.main()
