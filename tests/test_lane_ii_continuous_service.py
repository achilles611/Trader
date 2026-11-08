from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts.lane_ii_continuous.common import atomic_json,application_lock,binding,digest,process_identity,read_json,same_process
from scripts.lane_ii_continuous.queue import Registry,FenceLost
from scripts.lane_ii_continuous.resources import DEFAULT_LIMITS
from scripts.lane_ii_continuous.service import enqueue,reconcile_terminal_jobs,admission_reasons,Service
from scripts.lane_ii_continuous.worker import worker_environment

REPO=Path(__file__).resolve().parents[1]


class IntegratedTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.queue=Registry(self.root/'jobs.sqlite')
        self.config={'root':str(self.root),'source_sha256':'a'*64,'limits':dict(DEFAULT_LIMITS)}
    def tearDown(self):self.tmp.cleanup()
    def job(self,kind='report',**kw):
        return enqueue(self.queue,self.config,kind,kw.pop('payload',{'fixture':1}),**kw)
    def crash_child(self,job,after_output=False):
        script=self.root/'child.py'
        script.write_text("import sys,os\nfrom pathlib import Path\nsys.path.insert(0,sys.argv[1])\n"
            "from scripts.lane_ii_continuous.queue import Registry\n"
            "from scripts.lane_ii_continuous.common import process_identity\n"
            "q=Registry(Path(sys.argv[2])/'jobs.sqlite');j=q.claim(process_identity(),job_id=sys.argv[3],lease_seconds=1)\n"
            +("q.publish(j['id'],j['fence'],Path(sys.argv[2])/'outputs',{'value':42},crash_after_output=True)\n" if after_output else 'os._exit(73)\n'))
        result=subprocess.run([sys.executable,'-I','-B',str(script),str(REPO),str(self.root),job['id']],capture_output=True,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.assertNotEqual(result.returncode,0)
        self.queue.recover_leases(same_process,monotonic=time.monotonic()+1000)
    def test_real_process_crash_after_claim_requeues_with_fence(self):
        job=self.job();self.crash_child(job)
        old=self.queue.get(job['id']);self.assertEqual(old['state'],'queued')
        new=self.queue.claim(process_identity(),job_id=job['id'])
        self.assertGreater(new['fence'],1)
        with self.assertRaises(FenceLost):self.queue.publish(job['id'],1,self.root/'outputs',{})
        self.queue.publish(job['id'],new['fence'],self.root/'outputs',{'value':42})
    def test_real_process_crash_after_output_acknowledged_once(self):
        job=self.job();self.crash_child(job,True)
        self.assertTrue(self.queue.reconcile_dead_job(job['id'],self.root/'outputs',same_process))
        self.assertEqual(self.queue.get(job['id'])['state'],'completed')
        self.assertEqual(len(list((self.root/'outputs').glob('*.json'))),1)
        self.assertIsNone(self.queue.claim(process_identity(),job_id=job['id']))
    def test_abandoned_forward_with_open_circuit_never_restarts(self):
        job=self.job('forward',experiment_id='ex',payload={'run_dir':str(self.root/'run'),'candidate':{}})
        self.crash_child(job)
        self.queue.recovery_attempt('ex','disk',eligible=False,release_condition='Stable capacity')
        self.assertTrue(self.queue.reconcile_dead_job(job['id'],self.root/'outputs',same_process))
        self.assertEqual(self.queue.get(job['id'])['state'],'failed')
        one=reconcile_terminal_jobs(self.queue,self.config);two=reconcile_terminal_jobs(self.queue,self.config)
        self.assertEqual(one[:6],two);self.assertEqual(len(one),7)
        self.assertEqual(len(self.queue.jobs()),8)
        analysis=next(j for j in self.queue.jobs() if j['type']=='analysis')
        self.assertEqual(self.queue.get(analysis['dependencies'][0])['type'],'accounting')
    def test_report_revision_tracks_closure_and_dedupes_unchanged_state(self):
        job=self.job('forward',payload={'run_dir':str(self.root/'run'),'candidate':{}})
        claimed=self.queue.claim(process_identity());self.queue.publish(job['id'],claimed['fence'],self.root/'outputs',{},final_state='failed')
        reconcile_terminal_jobs(self.queue,self.config)
        report=next(j for j in self.queue.jobs() if j['type']=='report')
        claimed=self.queue.claim(process_identity(),job_id=report['id']);self.queue.publish(report['id'],claimed['fence'],self.root/'outputs',{})
        reconcile_terminal_jobs(self.queue,self.config)
        self.assertEqual(len([j for j in self.queue.jobs() if j['type']=='report']),1)
        seal=next(j for j in self.queue.jobs() if j['type']=='seal');self.queue.hold_pending(seal['id'],'missing','restore')
        reconcile_terminal_jobs(self.queue,self.config)
        self.assertEqual(len([j for j in self.queue.jobs() if j['type']=='report']),2)
    def test_persisted_live_owner_blocks_competing_admission(self):
        busy=self.job('integrity');self.queue.claim(process_identity(),job_id=busy['id'])
        candidate=self.job('forward',payload={'seconds':10,'run_dir':str(self.root/'new')})
        observation={'cpu_percent':0,'system_free_bytes':20*2**30,'runtime_free_bytes':300*2**30,
                     'available_ram_bytes':10*2**30,'owned_rss_bytes':100*2**20,'owned_process_count':1}
        reasons=admission_reasons(candidate,self.config,[observation],{'state':'accepted'},self.queue)
        self.assertIn('PERSISTED_OWNER_NOT_PROVEN_ABSENT',reasons)
    def test_owner_sensitive_facts_are_fenced(self):
        job=self.job();claim=self.queue.claim(process_identity(),job_id=job['id'])
        self.queue.fact_owned(job['id'],claim['fence'],'test',1)
        self.queue.publish(job['id'],claim['fence'],self.root/'outputs',{})
        with self.assertRaises(FenceLost):self.queue.fact_owned(job['id'],claim['fence'],'test',2)
        self.assertEqual(self.queue.fact('test'),1)
    def test_partition_progress_recovers_completed_output_without_checkpoint(self):
        from scripts.lane_ii_continuous.partitions import seal_partition
        capture=self.root/'capture';capture.mkdir();journal=capture/'events.jsonl';journal.write_text('{"n":1}\n')
        result=seal_partition(journal,self.root/'partition')
        job=self.job('partition',payload={'forward_job':'forward-fixture','hour':1})
        claimed=self.queue.claim(process_identity(),job_id=job['id'])
        self.queue.publish(job['id'],claimed['fence'],self.root/'outputs',result,artifacts=[binding(result['manifest_path'])])
        self.assertIsNone(self.queue.fact('partition:forward-fixture'))
        service=Service(self.root,config=self.config);service._reconcile()
        self.assertEqual(self.queue.fact('partition:forward-fixture')['partition']['end_offset'],journal.stat().st_size)
    def test_environment_removes_credentials_and_proxy(self):
        with patch.dict(os.environ,{'OPENAI_API_KEY':'fixture','HTTPS_PROXY':'fixture','KALSHI_KEY':'fixture'}):
            env=worker_environment(self.root)
        for key in ('OPENAI_API_KEY','HTTPS_PROXY','KALSHI_KEY'):self.assertNotIn(key,env)
    def test_offline_subprocess_blocks_dns_before_network(self):
        script=self.root/'network.py';script.write_text('import socket\nsocket.getaddrinfo("api.openai.com",443)\n')
        result=subprocess.run([sys.executable,'-I','-B',str(REPO/'scripts/lane_ii_continuous/offline.py'),'--',str(script)],capture_output=True)
        self.assertNotEqual(result.returncode,0);self.assertIn(b'OFFLINE_REPLAY_NETWORK_OR_DESCENDANT_DENIED',result.stderr)
    def test_application_lock_excludes_actual_second_process(self):
        script=self.root/'lock.py';script.write_text('import sys,time\nsys.path.insert(0,sys.argv[1])\nfrom scripts.lane_ii_continuous.common import application_lock\nwith application_lock(sys.argv[2]) as ok:\n print(ok,flush=True)\n time.sleep(15)\n')
        child=subprocess.Popen([sys.executable,'-I','-B',str(script),str(REPO),str(self.root/'lock')],stdout=subprocess.PIPE,
                               creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        try:
            self.assertEqual(child.stdout.readline().strip(),b'True')
            with application_lock(self.root/'lock') as acquired:self.assertFalse(acquired)
        finally:child.kill();child.wait(timeout=5);child.stdout.close()


if __name__=='__main__':unittest.main()
