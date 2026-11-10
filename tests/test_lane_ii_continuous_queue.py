import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.lane_ii_continuous.queue import Registry, QueueFull, FenceLost
from scripts.lane_ii_continuous.resources import admission, Guard, DEFAULT_LIMITS, GIB


def owner(pid=1, boot=1, creation=1):
    return {'pid':pid,'creation_epoch':creation,'boot_epoch':boot,'executable':'fixture-python','executable_sha256':'a'*64}


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.q=Registry(self.root/'queue.sqlite',max_pending=6,reserve_jobs=2,
                        max_output_bytes=1000,reserve_output_bytes=100)

    def tearDown(self): self.temp.cleanup()

    def add(self,kind='discovery',n=0,**kwargs):
        return self.q.enqueue(kind,{'n':n},input_hash=str(n),code_hash='code',config_hash='config',now=10,**kwargs)

    def test_duplicate_dispatch_and_changed_payload(self):
        a=self.add(); b=self.add()
        self.assertEqual(a['id'],b['id'])
        with self.assertRaises(ValueError):
            self.q.enqueue('discovery',{'n':999},input_hash='0',code_hash='code',config_hash='config')
        self.assertEqual(len(self.q.jobs()),1)

    def test_dependency_and_deadline(self):
        a=self.add('integrity',1)
        b=self.add('analysis',2,dependencies=[a['id']])
        claim=self.q.claim(owner(1),now=11,monotonic=11)
        self.assertEqual(claim['id'],a['id'])
        self.assertIsNone(self.q.claim(owner(2),now=11,monotonic=11))
        self.q.fail(a['id'],claim['fence'],'BAD_CHAIN',now=12,monotonic=12)
        self.assertIsNone(self.q.claim(owner(2),now=13,monotonic=13))
        self.assertEqual(self.q.get(b['id'])['state'],'admission-held')
        d=self.add('report',3,deadline=10)
        self.q.claim(owner(1),now=15,monotonic=15)
        self.assertEqual(self.q.get(d['id'])['terminal_reason'],'DEADLINE_BEFORE_START')

    def test_reserved_closeout_slots_and_bytes(self):
        for n in range(4): self.add(n=n)
        with self.assertRaises(QueueFull): self.add(n=5)
        self.add('closeout',6); self.add('report',7)
        with self.assertRaises(QueueFull): self.add('closeout',8)
        other=Registry(self.root/'small.sqlite',max_pending=6,reserve_jobs=2,max_output_bytes=1000,reserve_output_bytes=100)
        with self.assertRaises(QueueFull):
            other.enqueue('discovery',{},input_hash='x',code_hash='c',config_hash='f',estimated_bytes=901)
        other.enqueue('closeout',{},input_hash='x',code_hash='c',config_hash='f',estimated_bytes=901)

    def test_dead_owner_reclaim_fences_old_worker(self):
        self.add(); first=self.q.claim(owner(1,1),now=11,monotonic=11,lease_seconds=2)
        self.q.recover_leases(lambda _:False,now=14,monotonic=14)
        second=self.q.claim(owner(2,2),now=14,monotonic=14)
        with self.assertRaises(FenceLost): self.q.heartbeat(first['id'],first['fence'],now=14,monotonic=14)
        with self.assertRaises(FenceLost): self.q.publish(first['id'],first['fence'],self.root/'out',{},now=14,monotonic=14)
        self.q.publish(second['id'],second['fence'],self.root/'out',{'ok':True},now=15,monotonic=15)
        self.assertEqual(self.q.get(second['id'])['state'],'completed')

    def test_live_expired_owner_holds_instead_of_duplicate(self):
        self.add(); j=self.q.claim(owner(1),now=11,monotonic=11,lease_seconds=2)
        self.q.recover_leases(lambda _:True,now=14,monotonic=14)
        self.assertEqual(self.q.get(j['id'])['state'],'admission-held')
        self.assertIsNone(self.q.claim(owner(2),now=15,monotonic=15))

    def test_crash_after_output_reuses_one_report(self):
        self.add('report'); a=self.q.claim(owner(1),now=11,monotonic=11,lease_seconds=2)
        with self.assertRaisesRegex(RuntimeError,'INJECTED'):
            self.q.publish(a['id'],a['fence'],self.root/'out',{'result':1},now=12,monotonic=12,crash_after_output=True)
        self.q.recover_leases(lambda _:False,now=14,monotonic=14)
        b=self.q.claim(owner(2),now=14,monotonic=14)
        self.assertIsNotNone(self.q.reuse_published(b,self.root/'out',now=15,monotonic=15))
        self.assertEqual(len(list((self.root/'out').glob('*.json'))),1)
        self.assertEqual(sum(e['state']=='completed' for e in self.q.history(a['id'])),1)

    def test_transient_retries_bounded_across_registry_restart(self):
        a=self.add(); now=11
        for attempt in range(4):
            q=Registry(self.q.path)
            j=q.claim(owner(1),now=now,monotonic=now)
            state=q.fail(j['id'],j['fence'],'TIMEOUT',retryable=True,retry_delay=1,now=now,monotonic=now)
            self.assertEqual(state,'queued' if attempt<3 else 'failed')
            now+=2
        self.assertEqual(self.q.get(a['id'])['attempts'],4)

    def test_recovery_cap_and_release_require_evidence(self):
        for _ in range(2): self.assertTrue(self.q.recovery_attempt('e','TIMEOUT',eligible=True,release_condition='verified'))
        q=Registry(self.q.path)
        self.assertFalse(q.recovery_attempt('e','LIMIT',eligible=True,release_condition='operator review'))
        a=self.add(); j=q.claim(owner(1),now=11,monotonic=11)
        q.hold(j['id'],j['fence'],'DISK','space restored',now=11,monotonic=11)
        with self.assertRaises(ValueError):q.release_hold(a['id'],{'satisfied':False})
        q.release_hold(a['id'],{'satisfied':True,'samples':[1,2,3]},now=12)
        self.assertEqual(q.get(a['id'])['state'],'queued')

    def test_open_circuit_cannot_be_bypassed_by_later_eligibility(self):
        self.assertFalse(self.q.recovery_attempt('e','CAPACITY',eligible=False,release_condition='headroom'))
        self.assertFalse(Registry(self.q.path).recovery_attempt('e','RETRY',eligible=True,release_condition='headroom'))
        with self.assertRaises(ValueError): self.q.release_circuit('e',{'satisfied':True})
        self.q.release_circuit('e',{'satisfied':True,'stable_seconds':300,'samples':3,'evidence_sha256':'a'*64})
        self.assertTrue(self.q.recovery_attempt('e','RETRY',eligible=True,release_condition='headroom'))

    def test_policy_and_attempt_ownership_survive_reopen(self):
        q=Registry(self.q.path)
        self.assertEqual(q.max_pending,6)
        with self.assertRaises(ValueError): Registry(self.q.path,max_pending=128)
        self.add();a=q.claim(owner(11,1,9),now=11,monotonic=11,lease_seconds=2)
        q.recover_leases(lambda _:False,now=14,monotonic=14)
        q.claim(owner(22,2,13),now=14,monotonic=14)
        claims=[json.loads(e['detail']) for e in q.history(a['id']) if e['state']=='leased']
        self.assertEqual([c['owner']['pid'] for c in claims],[11,22])

    def test_failed_publication_does_not_become_success(self):
        self.add('closeout');a=self.q.claim(owner(1),now=11,monotonic=11)
        self.q.publish(a['id'],a['fence'],self.root/'out',{'partial':True},final_state='failed',reason='CAPTURE_FAILED',now=12,monotonic=12)
        self.assertEqual(self.q.get(a['id'])['state'],'failed')

    def test_wall_clock_jump_does_not_extend_lease(self):
        self.add();j=self.q.claim(owner(),now=1000,monotonic=10,lease_seconds=5)
        with self.assertRaises(FenceLost):self.q.heartbeat(j['id'],j['fence'],now=1,monotonic=16)
        self.q.recover_leases(lambda _:False,now=1,monotonic=16)
        self.assertEqual(self.q.get(j['id'])['state'],'queued')

    def test_unknown_owner_is_not_dead(self):
        self.add();j=self.q.claim(owner(),now=11,monotonic=11,lease_seconds=1)
        self.q.recover_leases(lambda _:None,now=13,monotonic=13)
        self.assertEqual(self.q.get(j['id'])['terminal_reason'],'OWNERSHIP_UNKNOWN')
        with self.assertRaises(ValueError):self.q.claim({'pid':1})

    def test_published_copy_survives_attempt_file_change(self):
        from scripts.lane_ii_continuous.common import binding
        self.add('report');j=self.q.claim(owner(),now=11,monotonic=11)
        source=self.root/'attempt.json';source.write_text('original')
        self.q.publish(j['id'],j['fence'],self.root/'out',{},artifacts=[binding(source)],now=12,monotonic=12)
        source.write_text('later mutation')
        receipt=json.loads((self.root/'out'/f"{j['id']}.json").read_text())
        self.assertEqual(Path(receipt['artifacts'][0]['path']).read_text(),'original')

    def test_writer_lock_is_bounded(self):
        db=sqlite3.connect(self.q.path,isolation_level=None)
        db.execute('BEGIN IMMEDIATE')
        try:
            with self.assertRaises(sqlite3.OperationalError):self.add()
        finally: db.rollback();db.close()


class ResourceTests(unittest.TestCase):
    def sample(self):
        return {'monotonic':0,'cpu_percent':10,'available_ram_bytes':8*GIB,
                'system_free_bytes':2*GIB,'runtime_free_bytes':100*GIB,
                'owned_rss_bytes':100*2**20,'owned_process_count':1}

    def test_disk_holds_without_lowering_floor(self):
        s=self.sample();s['system_free_bytes']=100*2**20
        self.assertIn('SYSTEM_DISK_FLOOR',admission([s],3600)['reasons'])
        self.assertEqual(DEFAULT_LIMITS['system_free_floor_bytes'],GIB)

    def test_cpu_persistence_and_memory_pressure(self):
        s=self.sample();g=Guard();s['cpu_percent']=95
        self.assertIsNone(g.check(s,60))
        s['monotonic']=5;self.assertEqual(g.check(s,60),'CPU_PERSISTENT')
        s=self.sample();s['available_ram_bytes']=GIB
        self.assertEqual(Guard().check(s,60),'RAM_HARD_FLOOR')


if __name__=='__main__':unittest.main()
