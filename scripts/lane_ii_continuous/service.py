"""Local scheduler, serialized admissions, restart reconciliation and read-only status.

No strategy generation, credentials, exchange mutations or model clients live here.
Forward accounts are never relaunched after a process interruption.
"""
from __future__ import annotations

from collections import Counter, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

from .common import atomic_bytes, atomic_json, application_lock, binding, digest, file_hash, process_identity, read_json, same_process, utc
from .queue import Registry, TERMINAL, QueueFull, RESERVED_TYPES
from .resources import admission, sample, Guard
from .reports import build_status, render_latest, daily_digest, weekly_review

LIGHT = {'import', 'seal', 'accounting', 'analysis', 'decision', 'report', 'partition'}
HEAVY = {'integrity', 'replay'}
PIPELINE = ('seal','integrity','accounting','replay','analysis','decision')


def load_config(root):
    root=Path(root).resolve()
    config=read_json(root/'CONFIG.json')
    if config['paper_only'] is not True or config['model_enrichment_enabled'] is not False:
        raise ValueError('PAPER_ONLY_NO_MODEL_RUNTIME_REQUIRED')
    if config['root']!=str(root):raise ValueError('Runtime root identity changed')
    source=read_json(root/'SOURCE.json')
    if digest(source)!=config['source_sha256']:raise ValueError('Source manifest changed')
    for entry in source['files']:
        if binding(entry['path'])!=entry:raise ValueError('Frozen application source changed')
    if file_hash(sys.executable) not in {config['interpreter_sha256'],config['interpreter_windows']['sha256']}:raise ValueError('Interpreter changed')
    return config


def enqueue(registry, config, kind, payload, **kwargs):
    return registry.enqueue(kind,payload,input_hash=digest(payload),code_hash=config['source_sha256'],
                            config_hash=digest(config),**kwargs)


def reconcile_terminal_jobs(registry,config):
    """Idempotent DB discovery; includes failures whose worker never published."""
    added=[]
    for job in registry.jobs(TERMINAL):
        if job['type']!='forward':continue
        previous=None;stages={}
        for kind in PIPELINE:
            payload={'run_dir':job['payload']['run_dir'],'candidate':job['payload']['candidate'],
                     'forward_job':job['id'],'stage':kind}
            # Failure reporting never waits behind an unavailable analysis dependency.
            deps=() if kind in {'seal','report'} else ((stages['accounting'],) if kind=='analysis' else (previous,))
            result=enqueue(registry,config,kind,payload,parent_id=job['id'],
                           experiment_id=job['experiment_id'],segment_id=job['segment_id'],dependencies=deps,
                           priority=5 if kind=='report' else 20,estimated_bytes=16*2**20,
                           resource={'class':'heavy' if kind in HEAVY else 'light'})
            previous=result['id'];stages[kind]=result['id'];added.append(result['id'])
        snapshot=[{'id':ident,'type':kind,'state':registry.get(ident)['state'],
                   'reason':registry.get(ident)['terminal_reason'],'output':registry.get(ident)['artifact_manifest']}
                  for kind,ident in stages.items()]
        pending=[j for j in registry.jobs({'queued','leased','running'}) if j['type']=='report' and j['parent_id']==job['id']]
        if not pending:
            report=enqueue(registry,config,'report',{'forward_job':job['id'],'closure_snapshot':snapshot},
                           parent_id=job['id'],experiment_id=job['experiment_id'],priority=5,estimated_bytes=2**20)
            added.append(report['id'])
    return added


def admission_reasons(job,config,measurements,clock,registry):
    if job['type'] not in HEAVY|{'forward'}:return []
    decision=admission(measurements,job['payload'].get('seconds',0),limits=config['limits'],heavy=job['type'] in HEAVY)
    reasons=list(decision['reasons'])
    for other in registry.jobs({'leased','running','admission-held'}):
        if other['id']==job['id'] or not other.get('owner'):continue
        if other['state']=='admission-held' and other['terminal_reason'] not in {'OWNERSHIP_STILL_ALIVE','OWNERSHIP_UNKNOWN'}:continue
        if same_process(other['owner']) is not False:reasons.append('PERSISTED_OWNER_NOT_PROVEN_ABSENT');break
    if job['type']=='forward':
        gates=registry.fact('qualification') or {}
        for key in ('engineering_reviewed','paper_scope_reviewed','replay_reproduced'):
            if gates.get(key) is not True:reasons.append(key.upper()+'_REQUIRED')
        if clock.get('state')!='accepted':reasons.append('CLOCK_ADMISSION_HOLD')
        if (Path(job['payload']['run_dir'])/'capture').exists():reasons.append('EXISTING_ACCOUNT_REQUIRES_RECONCILIATION')
    return reasons


def identity_with_source(config):
    return {**process_identity(),'source_sha256':config['source_sha256'],'config_sha256':digest(config)}


class Service:
    def __init__(self,root,*,config=None):
        self.root=Path(root).resolve();self.config=load_config(root) if config is None else config
        self.queue=Registry(self.root/'jobs.sqlite')
        self.owner=identity_with_source(self.config)
        self.workers={};self.measurements=deque(maxlen=60);self.stable={}
        self.clock={};self.last_clock=0.;self.last_tick=0.;self.last_status=0.
        self.guard=Guard(self.config['limits']);self.server=None
        self.control='run';self.error=None;self.stop_started=None;self.execution='running'

    def _observe_workers(self):
        for ident,entry in list(self.workers.items()):
            if entry['process'].poll() is None:continue
            code=entry['process'].returncode
            entry['owned'].close();entry['log'].close()
            atomic_json(self.root/'worker-exits'/(ident+'.json'),{
                'utc':utc(),'job_id':ident,'owner':entry['identity'],'exit_code':code,
                'all_owned_descendants_closed':True})
            del self.workers[ident]

    def _pressure(self,measurement):
        reason=self.guard.check(measurement,0)
        if not reason:return
        # Optional heavy work yields first. Fencing precedes owned-only teardown,
        # so an interrupted analysis cannot publish after its admission is held.
        for ident,entry in list(self.workers.items()):
            if entry['type'] not in HEAVY:continue
            job=self.queue.get(ident)
            if job['state'] in {'leased','running'}:
                self.queue.hold(ident,job['fence'],'RESOURCE:'+reason,'Measured stable headroom for300seconds/3samples')
            entry['owned'].close()
            entry['process'].wait(timeout=15)
        # The forward worker has its own1second guard. This outer guard includes
        # the coordinator, status server and all owned worker descendants.
        if any(entry['type']=='forward' for entry in self.workers.values()):
            atomic_json(self.root/'STOP_WORKERS.json',{'utc':utc(),'reason':reason,'scope':'ENTIRE_OWNED_SERVICE'})

    def _launch(self,job):
        from scripts.slim_paper_operator.owned_job import OwnedJob
        cli=self.config['cli']
        log=(self.root/'logs'/(job['id']+'.log')).open('ab',buffering=0)
        command=[self.config['interpreter'],'-I','-B',cli,'worker','--root',str(self.root),'--job',job['id']]
        from .worker import worker_environment
        env=worker_environment(self.root)
        process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=log,stderr=log,env=env,
                                 cwd=self.config['app'],creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        try:
            owned=OwnedJob(process);identity=process_identity(process.pid)
            atomic_json(self.root/'worker-owners'/(job['id']+'.json'),{'owner':identity,'command':command,
                        'source_sha256':self.config['source_sha256'],'owned_job_before_release':True})
            self.workers[job['id']]={'process':process,'owned':owned,'log':log,'identity':identity,'type':job['type']}
            process.stdin.write(b'LANE_II_JOB_START\n');process.stdin.flush();process.stdin.close()
        except BaseException:
            if 'owned' in locals():owned.close()
            else:process.kill()
            process.wait(timeout=15);log.close();raise

    def _clock_refresh(self):
        from .clock import calibrate_windows
        previous=self.clock.get('anchor') or self.clock.get('last_accepted_anchor')
        self.clock=calibrate_windows(boot_id=self.owner['boot_epoch'],prior_anchor=previous)
        if previous and self.clock.get('state')!='accepted':self.clock['last_accepted_anchor']=previous
        atomic_json(self.root/'CLOCK_CONTROL.json',self.clock);self.last_clock=time.monotonic()

    def _reconcile(self):
        changed=self.queue.recover_leases(same_process)
        for item in changed:
            job=self.queue.get(item['id'])
            self.queue.reconcile_dead_job(job['id'],self.root/'outputs',same_process)
            if job['type']=='forward':
                self.queue.recovery_attempt(job['experiment_id'],'FORWARD_ACCOUNT_CONTINUITY_UNPROVEN',eligible=False,
                    release_condition='Reconcile original account, exit and immutable closeout; never restart a fresh account in this segment')
        reconcile_terminal_jobs(self.queue,self.config)
        # Recover publication-before-checkpoint gaps from the sealed output, never
        # from a mutable attempt pointer. Replaying this projection is idempotent.
        partitions=sorted((j for j in self.queue.jobs({'completed'}) if j['type']=='partition'),key=lambda j:j['payload']['hour'])
        for job in partitions:
            if binding(job['artifact_manifest']['path'])!=job['artifact_manifest']:raise ValueError('PARTITION_OUTPUT_CHANGED')
            receipt=read_json(job['artifact_manifest']['path']);result=receipt['result']
            current=self.queue.fact('partition:'+job['payload']['forward_job'])
            if current is None or current['hour']<job['payload']['hour']:
                self.queue.fact('partition:'+job['payload']['forward_job'],{'hour':job['payload']['hour'],'partition':result},set_value=True)

    def _schedule_partitions(self):
        for job in self.queue.jobs({'running','leased'}):
            if job['type']!='forward':continue
            state=self.queue.fact('forward:'+job['id']) or {}
            started=state.get('started_monotonic');boot=state.get('boot_epoch')
            if started is None or boot!=self.owner['boot_epoch']:continue
            hour=int((time.monotonic()-started)//3600)
            previous=self.queue.fact('partition:'+job['id'])
            completed_hour=0 if previous is None else previous['hour']
            if hour<=completed_hour:continue
            pending=[j for j in self.queue.jobs({'queued','leased','running','admission-held'})
                     if j['type']=='partition' and j['parent_id']==job['id']]
            if pending:continue
            enqueue(self.queue,self.config,'partition',{'journal':str(Path(job['payload']['run_dir'])/'capture/events.jsonl'),
                    'previous':None if previous is None else previous['partition'],'hour':hour,'forward_job':job['id']},
                    parent_id=job['id'],experiment_id=job['experiment_id'],priority=10,estimated_bytes=2*2**30)

    def tick(self):
        control=read_json(self.root/'CONTROL.json');self.control=control['mode']
        self._observe_workers();self._reconcile();self._schedule_partitions()
        jobs=self.queue.jobs()
        for job in jobs:
            if job['state']=='admission-held' and job['terminal_reason'].startswith('RESOURCE:'):
                reasons=admission_reasons(job,self.config,list(self.measurements),self.clock,self.queue)
                if reasons:self.stable.pop(job['id'],None)
                else:
                    entry=self.stable.setdefault(job['id'],{'since':time.monotonic(),'count':0});entry['count']+=1
                    if time.monotonic()-entry['since']>=300 and entry['count']>=3:
                        self.queue.release_hold(job['id'],{'satisfied':True,'samples':entry['count'],
                            'stable_seconds':time.monotonic()-entry['since'],'evidence_sha256':digest(list(self.measurements))})
        if self.control=='stop':
            self.stop_started=self.stop_started or time.monotonic()
            atomic_json(self.root/'STOP_WORKERS.json',{'reason':'OPERATOR_GRACEFUL_STOP','utc':utc()})
        active_types={v['type'] for v in self.workers.values()}
        for job in self.queue.jobs({'queued'}):
            if job['id'] in self.workers or job['due']>time.time():continue
            if self.control!='run' and job['type'] in HEAVY|{'forward'}:continue
            if job['type'] not in RESERVED_TYPES and self.queue.circuit(job['experiment_id']):
                self.queue.hold_pending(job['id'],'CIRCUIT:'+self.queue.circuit(job['experiment_id']),
                    'Explicit source-bound stable release plus account continuity review');continue
            if job['type']=='forward' and self.workers:continue
            if job['type'] in HEAVY and active_types:continue
            if job['type'] in LIGHT and any(t!='forward' for t in active_types):continue
            if job['type'] in HEAVY|{'forward'} and len(self.measurements)<3:continue
            reasons=admission_reasons(job,self.config,list(self.measurements),self.clock,self.queue)
            if reasons:
                self.queue.hold_pending(job['id'],'RESOURCE:'+','.join(reasons),
                    'All named gates verified; measured capacity and clock stable for 300 seconds across at least 3 samples')
                continue
            deps=[self.queue.get(x) for x in job['dependencies']]
            if any(d is None or d['state'] in {'failed','cancelled','admission-held'} for d in deps):
                self.queue.hold_pending(job['id'],'DEPENDENCY_UNAVAILABLE','Reconcile named dependency; no automatic evidence substitution')
                continue
            if any(d['state']!='completed' for d in deps):continue
            self._launch(job);active_types.add(job['type'])
        self.last_tick=time.monotonic()

    def status(self):
        now=utc();jobs=self.queue.jobs();counts=dict(Counter(j['state'] for j in jobs))
        resource=self.measurements[-1] if self.measurements else {}
        evidence=self.queue.fact('latest_evidence') or {'as_of_utc':now,'execution':{'state':'admission-held'},
            'coverage':{'planned_seconds':900,'observed_seconds':0,'gap_seconds':0},'closure':{}}
        forwards=[j for j in jobs if j['type']=='forward' and j['state'] in TERMINAL]
        if forwards:
            latest=forwards[-1];closure={}
            for kind,key,good in [('seal','seal','SEALED'),('integrity','integrity','PASS'),('accounting','accounting','RECONCILED'),
                                  ('replay','replay','PASS'),('analysis','analysis','PUBLISHED')]:
                stages=[j for j in jobs if j['parent_id']==latest['id'] and j['type']==kind]
                stage=stages[-1] if stages else None
                closure[key]=good if stage and stage['state']=='completed' else ('PENDING' if stage is None else stage['state'].upper())
                if kind=='accounting' and stage and stage['state']=='completed':
                    result=read_json(stage['artifact_manifest']['path'])['result']
                    component=result.get('components',{}).get('study',{})
                    evidence={**evidence,'paper_accounts':component.get('accounting',{}).get('accounts',[]),
                        'coverage':component.get('coverage',evidence.get('coverage',{}))}
            evidence={**evidence,'closure':closure,'execution':{'state':latest['state'],'reason':latest['terminal_reason']}}
        blockers=[j['terminal_reason'] for j in jobs if j['state']=='admission-held']
        state={**evidence,'source_sha256':self.config['source_sha256'],'config_sha256':digest(self.config),
            'components':{**evidence.get('components',{}),'supervisor':{'state':self.execution,'as_of_utc':now,
                'currently_running_verified':self.execution=='running'},'report':{'state':'completed','as_of_utc':now}},
            'queue':{'counts':counts,'bounded':True,'estimated_output_bytes':sum(j['estimated_bytes'] for j in jobs if j['state'] not in TERMINAL),
                'oldest_age_seconds':max([time.time()-j['created'] for j in jobs if j['state'] not in TERMINAL] or [0])},
            'resources':{'cpu_percent':resource.get('cpu_percent'),'available_ram_bytes':resource.get('available_ram_bytes'),
                'owned_rss_bytes':resource.get('owned_rss_bytes'),'free_disk_bytes':resource.get('runtime_free_bytes'),
                'disk_reserve_bytes':self.config['limits']['runtime_free_floor_bytes'],'process_count':resource.get('owned_process_count'),
                'optional_analysis_paused':self.control!='run' or bool(blockers)},
            'clock':self.clock,'blockers':blockers+([] if not self.error else [self.error]),'paper_only':True,
            'runtime_model_calls_observed':0,'next_action':'Resolve recorded admission gates; keep failed evidence unchanged',
            'experiment_decision':'INSUFFICIENT_EVIDENCE','cohort_decision':'FROZEN_OBSERVATION_ONLY_COPYING_INELIGIBLE',
            'evidence_links':['https://github.com/achilles611/Trader/pull/3']}
        public=build_status(state,now_utc=now)
        private={'schema':'LANE_II_SERVICE_STATUS_V1','utc':now,'heartbeat_monotonic':time.monotonic(),
            'owner':self.owner,'control':self.control,'source_sha256':self.config['source_sha256'],
            'config_sha256':digest(self.config),'workers':[{'job_id':k,'identity':v['identity'],'type':v['type']} for k,v in self.workers.items()],
            'resources':resource,'public':public,'model_call_evidence':'No model client; Python socket connects denied in coordinator/offline workers; fixed public-only collector allowlist'}
        atomic_json(self.root/'STATUS.json',private);atomic_json(self.root/'public/STATUS.json',public)
        atomic_bytes(self.root/'public/LATEST.md',render_latest(public).encode('utf-8'))
        day=datetime.now(timezone.utc).strftime('%Y-%m-%d')
        digest_path=self.root/'public/daily'/ (day+'.json')
        # One replaceable current daily digest, not a new job on each health tick.
        atomic_json(digest_path,daily_digest(public))
        week=datetime.now(timezone.utc).strftime('%G-W%V')
        weekly_path=self.root/'public/weekly'/(week+'.json')
        if not weekly_path.exists():
            hypotheses=self.queue.fact('hypotheses');wallets=self.queue.fact('wallet_registry')
            if hypotheses is not None and wallets is not None:
                atomic_json(weekly_path,weekly_review(hypotheses,wallets,as_of_utc=now))
        self.last_status=time.monotonic()
        return private

    def serve_http(self):
        root=self.root
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                paths={'/':('public/LATEST.md','text/plain; charset=utf-8'),'/status.json':('public/STATUS.json','application/json')}
                target=paths.get(self.path)
                if not target:self.send_error(404);return
                try:data=(root/target[0]).read_bytes()
                except OSError:self.send_error(503);return
                self.send_response(200);self.send_header('Content-Type',target[1]);self.send_header('Cache-Control','no-store')
                self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
            def log_message(self,*args):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',self.config['status_port']),Handler)
        threading.Thread(target=self.server.serve_forever,daemon=True).start()

    def run(self,seconds=None):
        with application_lock(self.root/'coordinator.lock') as acquired:
            if not acquired:return {'state':'ALREADY_OWNED'}
            started=time.monotonic();self.serve_http()
            atomic_json(self.root/'launches'/(str(os.getpid())+'-'+str(time.time_ns())+'.json'),{'utc':utc(),'owner':self.owner})
            try:
                while seconds is None or time.monotonic()-started<seconds:
                    measurement=sample(self.root,cpu_interval=None);self.measurements.append(measurement)
                    self._pressure(measurement)
                    self._observe_workers()
                    if time.monotonic()-self.last_clock>=300:self._clock_refresh()
                    if time.monotonic()-self.last_tick>=60:self.tick()
                    if time.monotonic()-self.last_status>=5:self.status()
                    if self.control=='stop' and not self.workers:break
                    if self.stop_started and time.monotonic()-self.stop_started>120:break
                    time.sleep(1)
            finally:
                for entry in self.workers.values():entry['owned'].close();entry['log'].close()
                self.workers.clear()
                self.execution='stopped'
                final=self.status();final['service_execution']='stopped';atomic_json(self.root/'SERVICE_EXIT.json',final)
                if self.server:self.server.shutdown();self.server.server_close()
            return {'state':'STOPPED'}


def watchdog(root):
    root=Path(root).resolve()
    try:config=load_config(root)
    except (OSError,ValueError,KeyError) as exc:
        result={'utc':utc(),'action':'CONFIGURATION_HOLD_NO_RESTART','error_type':type(exc).__name__}
        atomic_json(root/'WATCHDOG.json',result);return result
    with application_lock(root/'watchdog.lock') as acquired:
        if not acquired:return {'state':'WATCHDOG_ALREADY_OWNED'}
        status=read_json(root/'STATUS.json') if (root/'STATUS.json').exists() else None
        owner=None if status is None else status['owner'];alive=False if owner is None else same_process(owner)
        heartbeat_age=None if status is None else time.monotonic()-status['heartbeat_monotonic']
        fresh=(alive is True and 0<=heartbeat_age<=120)
        result={'utc':utc(),'owner_alive':alive,'heartbeat_age_seconds':heartbeat_age,'fresh':fresh,
                'action':'NONE','automatic_forced_kills':False,'source_sha256':config['source_sha256']}
        if read_json(root/'CONTROL.json')['mode']=='stop':result['action']='OPERATOR_STOP_RETAINED'
        elif alive is False:
            registry=Registry(root/'jobs.sqlite')
            recovery=registry.fact('watchdog_recovery') or {'attempts':0,'healthy_samples':0}
            if recovery['attempts']>=2:result['action']='COORDINATOR_RECOVERY_CIRCUIT_OPEN'
            else:
                recovery.update(attempts=recovery['attempts']+1,healthy_samples=0,last_attempt_utc=utc())
                registry.fact('watchdog_recovery',recovery,set_value=True)
                subprocess.run(['schtasks.exe','/Run','/TN',config['coordinator_task']],capture_output=True,check=True,
                    timeout=15,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                result['action']='REQUESTED_SCHEDULED_RESTART'
        elif fresh:
            registry=Registry(root/'jobs.sqlite')
            recovery=registry.fact('watchdog_recovery') or {'attempts':0,'healthy_samples':0}
            recovery['healthy_samples']+=1
            # Keep attempts across healthy intervals; an operator must review and
            # release a spent recovery circuit. A heartbeat is not repair proof.
            registry.fact('watchdog_recovery',recovery,set_value=True)
        elif not fresh:result['action']='HOLD_STALE_OR_UNKNOWN_OWNER_RECONCILE_NO_DUPLICATE_LAUNCH'
        atomic_json(root/'WATCHDOG.json',result)
        return result
