"""One fenced job per process. All task output goes to the runtime volume."""
from __future__ import annotations

import os
import math
from pathlib import Path
import subprocess
import sys
import threading
import time

from .common import atomic_json, binding, digest, fresh_coordinator_clock, process_identity, read_json, same_process, utc
from .queue import Registry, FenceLost
from .resources import sample, admission, Guard


class Held(RuntimeError):pass


def worker_environment(root):
    env={k:v for k,v in os.environ.items() if k.upper() in {'SYSTEMROOT','WINDIR','SYSTEMDRIVE','PATH','PATHEXT','COMSPEC','NUMBER_OF_PROCESSORS','PROCESSOR_ARCHITECTURE'}}
    env.update(PYTHONDONTWRITEBYTECODE='1',TEMP=str(Path(root)/'temp'),TMP=str(Path(root)/'temp'))
    return env


def deny_python_network():
    # Coordinator/offline arithmetic cannot call any model or exchange endpoint.
    # The collector is a separate frozen process with a narrower public-data policy.
    def hook(event,args):
        if event in {'socket.connect','socket.getaddrinfo','socket.gethostbyname'}:
            raise PermissionError('OFFLINE_WORKER_NETWORK_DENIED')
    sys.addaudithook(hook)


def verified(binding_value):
    if binding(binding_value['path'])!=binding_value:raise ValueError('BOUND_INPUT_CHANGED')
    return read_json(binding_value['path'])


def source_result(queue,job_id,kind):
    matches=[j for j in queue.jobs({'completed'}) if j['parent_id']==job_id and j['type']==kind]
    if not matches:raise Held('MISSING_COMPLETED_'+kind.upper())
    return verified(matches[-1]['artifact_manifest'])


def offline_child(config, script, arguments, log_path):
    command=[config['interpreter'],'-I','-B',str(Path(config['cli']).with_name('offline.py')),'--',
             str(Path(config['cli']).with_name(script)),*map(str,arguments)]
    with Path(log_path).open('wb') as log:
        return subprocess.run(command,stdout=log,stderr=log,check=False,timeout=config['maximum_replay_seconds'],
                              env=worker_environment(config['root']),
                              creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)).returncode


def exited_owned_descendants(owned, process, *, settle_seconds=1.0):
    """Distinguish a verified root exit from Job members that still remain."""
    if process.poll() is None:
        raise ValueError('ROOT_EXIT_NOT_OBSERVED')
    deadline=time.monotonic()+settle_seconds
    while True:
        # Windows can list the exited root while its retained handle is open.
        # The handle proves this PID's exit; never ignore a different member.
        children=owned.pids()-{process.pid}
        if not children or time.monotonic()>=deadline:return children
        time.sleep(.05)


def forward(job,config,queue,attempt):
    from scripts.slim_paper_operator.owned_job import OwnedJob
    from .capture import build_forward_command, request_stop, read_terminal, capture_state
    from .clock import project
    root=Path(config['root']);run=Path(job['payload']['run_dir']);candidate=job['payload']['candidate']
    if (run/'capture').exists():raise Held('EXISTING_ACCOUNT_CANNOT_BE_RESET')
    clock=read_json(root/'CLOCK_CONTROL.json')
    if not fresh_coordinator_clock(clock):raise Held('RESOURCE:CLOCK_ADMISSION_HOLD')
    measures=[sample(root,cpu_interval=.15) for _ in range(3)]
    decision=admission(measures,job['payload']['seconds'],limits=config['limits'])
    if not decision['admitted']:raise Held('RESOURCE:'+','.join(decision['reasons']))
    prospective=config.get('qualification_policy') is not None
    if prospective:
        from .qualification import launch_reasons
        reasons=launch_reasons(config,queue,job)
        if reasons:raise Held('QUALIFICATION:'+','.join(reasons))
    elif not all((queue.fact('qualification') or {}).get(k) is True for k in ('engineering_reviewed','paper_scope_reviewed','replay_reproduced')):
        raise Held('QUALIFICATION_REQUIRED')
    run.mkdir(parents=True,exist_ok=True)
    # Existing collector retains its validated decision cadence and prewarm.
    start=project(clock['anchor'])+config['prewarm_seconds']
    if prospective:
        alignment=config['qualification_policy']['alignment_seconds']
        start=math.ceil(start/alignment)*alignment
    cutoff=start+job['payload']['seconds']
    argv=build_forward_command(candidate,run,utc(start),utc(cutoff));argv[0]=config['interpreter']
    env=worker_environment(root)
    env.update(LANE_II_CLOCK_CONTROL=str(root/'CLOCK_CONTROL.json'),LANE_II_BOOT_ID=str(clock['boot_id']),
               PYTHONDONTWRITEBYTECODE='1',TEMP=str(root/'temp'),TMP=str(root/'temp'))
    if prospective:
        paper_manifest=config['paper_risk_manifest']
        env.update(LANE_II_PAPER_RISK_MANIFEST=paper_manifest['path'],
                   LANE_II_PAPER_RISK_MANIFEST_SHA256=paper_manifest['sha256'],LANE_II_PAPER_SEGMENT_ID=job['segment_id'])
        predecessor=job['payload'].get('predecessor')
        env['LANE_II_PAPER_PREDECESSOR_RECEIPT_SHA256']=predecessor['sha256'] if predecessor else 'GENESIS'
        atomic_json(run/'ADMISSION.json',{'schema':'LANE_II_FORWARD_ADMISSION_V1','utc':utc(),
            'source_sha256':config['source_sha256'],'config_sha256':digest(config),'candidate_sha256':candidate['source_manifest_sha256'],
            'qualification_phase':job['payload']['phase'],'job_id':job['id'],'segment_id':job['segment_id'],
            'paper_risk_manifest':paper_manifest,'predecessor':predecessor,
            'paper_start_utc':utc(start),'cutoff_utc':utc(cutoff),'clock':clock,'resources':decision,
            'resource_samples':measures,'release':queue.fact('short_release')})
    log=(attempt/'collector.log').open('wb',buffering=0)
    process=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=log,stderr=log,cwd=candidate['candidate_dir'],env=env,
                             creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    guard=Guard(config['limits']);stop_at=None;stop_reason=None;last_state=0.;owned=None;forced=False
    cutoff_mono=time.monotonic()+max(0,cutoff-project(clock['anchor']))
    try:
        owned=OwnedJob(process);identity=process_identity(process.pid)
        atomic_json(run/'operator.json',{'pid':process.pid,'creation_utc':utc(identity['creation_epoch']),
            **identity,'command':argv,'source_manifest_sha256':candidate['source_manifest_sha256'],
            'owned_job_before_startup_gate':True,'parent_job':job['id'],'fence':job['fence']})
        queue.fact_owned(job['id'],job['fence'],'forward:'+job['id'],{'started_monotonic':time.monotonic(),'boot_epoch':identity['boot_epoch'],
            'child_owner':identity,'run_dir':str(run),'paper_start_utc':utc(start),'cutoff_utc':utc(cutoff),
            'paper_start_monotonic':cutoff_mono-job['payload']['seconds']})
        process.stdin.write(candidate['startup_release'].encode());process.stdin.flush();process.stdin.close()
        previous_bytes=0;previous_mono=time.monotonic()
        while process.poll() is None:
            now=time.monotonic();observation=sample(root)
            tape=run/'capture/events.jsonl';size=tape.stat().st_size if tape.exists() else 0
            rate=max(0,size-previous_bytes)/max(1,now-previous_mono);previous_bytes=size;previous_mono=now
            reason=guard.check(observation,max(0,cutoff_mono-now),rate)
            marker=run/'capture/COLLECTION_CLOSED.json'
            closure=False
            if marker.exists():
                marker_value=read_json(marker)
                closure=all(marker_value.get(k) is True for k in ('collection_closed','network_closed','producers_exited','snapshot_publisher_closed'))
            if now>cutoff_mono+(300 if closure else 20):reason='BOUNDED_FINALIZATION_TIMEOUT'
            if (root/'STOP_WORKERS.json').exists():reason=read_json(root/'STOP_WORKERS.json')['reason']
            if now-last_state>=5:
                view=capture_state(run);last_state=now
                snap=view.get('snapshot',{})
                if snap:
                    state={'as_of_utc':snap.get('utc',utc()),'execution':view['execution'],
                        'coverage':{'planned_seconds':job['payload']['seconds'],'observed_seconds':max(0,min(cutoff,project(clock['anchor']))-start)},
                        'components':{'study':{'state':snap.get('state'),'as_of_utc':snap.get('utc'),
                            'currently_running_verified':True}},'current_experiment':job['experiment_id']}
                    queue.fact_owned(job['id'],job['fence'],'latest_evidence',state)
            if reason and stop_at is None:
                stop_reason=reason
                if (run/'capture/preregistration.json').exists():request_stop(run,reason)
                stop_at=now
                queue.recovery_attempt(job['experiment_id'],reason,eligible=False,
                    release_condition='Reconcile final account and typed cause; no fresh-account reset')
            if stop_at is not None and now-stop_at>config['closeout_wait_seconds']:
                forced=True;owned.close();owned=None;process.wait(timeout=15);break
            time.sleep(1)
        exit_code=process.wait(timeout=15)
        children=exited_owned_descendants(owned,process) if owned is not None else set()
        forced=forced or bool(children)
        if owned is not None:owned.close();owned=None
        atomic_json(run/'exit.json',{'pid':process.pid,'exit_code':exit_code,'observed_exit_utc':utc(),
            'creation_epoch':identity['creation_epoch'],'boot_epoch':identity['boot_epoch'],
            'executable_sha256':identity['executable_sha256'],'observed_exit_monotonic':time.monotonic(),
            'owned_children_at_parent_exit':sorted(children),'owned_job_closed':True,
            'retained_handle_exit_observed':True,'forced_closure':forced,
            'reason':stop_reason,'source_manifest_sha256':candidate['source_manifest_sha256']})
    finally:
        if owned is not None:owned.close()
        if process.poll() is None:
            process.kill();process.wait(timeout=15)
        log.close()
    terminal=read_terminal(run)
    if terminal is None:
        terminal={'execution':{'state':'failed','reason':'MISSING_TERMINAL_RECEIPT'},'coverage':{'planned_seconds':job['payload']['seconds']},
                  'closure':{'capture_sealed':False,'accounting_reconciled':False}}
    terminal['supervisor_exit']=binding(run/'exit.json')
    successful=(exit_code==0 and not forced and terminal['execution']['state']=='complete' and terminal['closure']['capture_sealed'])
    paths=[run/'operator.json',run/'exit.json']+([run/'ADMISSION.json'] if prospective else [])
    if prospective:
        from .paper_risk import validate_closure
        try:
            risk=validate_closure(run,expected_anchor_sha256=config['paper_risk_manifest']['sha256'])
            risk_path=run/'RISK_CLOSURE.json'
            risk_binding=binding(risk_path)
            if (risk['valid'] is not True or risk['campaign_id']!=job['experiment_id']
                    or risk['segment_id']!=job['segment_id']
                    or risk['closure_sha256']!=risk_binding['sha256']
                    or risk['receipt_sha256']!=binding(run/'capture/receipt.json')['sha256']):
                raise ValueError('RISK_CLOSURE_JOB_BINDING_MISMATCH')
            terminal.update(risk_continuity=risk_binding,risk_validation=risk)
            paths.append(risk_path)
        except (OSError,ValueError,KeyError,TypeError) as exc:
            terminal['risk_hold']='RISK_CLOSURE_UNPROVEN:'+type(exc).__name__+':'+str(exc)[:120]
            successful=False
    queue.fact_owned(job['id'],job['fence'],'latest_evidence',{'as_of_utc':utc(),'execution':{'state':'completed' if successful else 'failed',
        'reason':terminal['execution'].get('reason')},'coverage':terminal['coverage'],'closure':{},
        'current_experiment':job['experiment_id']})
    return terminal,[binding(path) for path in paths],('completed' if successful else 'failed')


def handle(job,config,queue,attempt):
    from .service import enqueue
    kind=job['type'];p=job['payload'];root=Path(config['root'])
    if kind=='forward':return forward(job,config,queue,attempt)
    if kind=='import':
        values={key:verified(value) for key,value in p['artifacts'].items()}
        if p.get('latest_evidence'):queue.fact_owned(job['id'],job['fence'],'latest_evidence',p['latest_evidence'])
        return {'verified_artifacts':p['artifacts'],'values':values},list(p['artifacts'].values()),'completed'
    if kind=='analysis' and 'evidence' in p:
        from .research import analyze_closeout
        result=analyze_closeout(p['evidence'],attempt/'analysis')
        return result,result['artifacts'],'completed'
    if kind=='partition':
        from .partitions import seal_partition
        result=seal_partition(p['journal'],attempt/'partition',previous=p['previous'])
        return result,[binding(result['manifest_path'])],'completed'
    run=Path(p['run_dir']) if 'run_dir' in p else None
    if kind=='seal':
        from .capture import read_terminal
        terminal=read_terminal(run)
        if terminal is None:raise Held('NO_GENUINE_TERMINAL_RECEIPT')
        if not terminal['closure']['capture_sealed']:raise Held('CAPTURE_NOT_SEALED')
        if not (run/'operator.json').is_file():raise Held('MISSING_OWNERSHIP_RECORD')
        operator=read_json(run/'operator.json')
        if same_process(operator) is not False:raise Held('PRIOR_OWNER_NOT_PROVEN_ABSENT')
        return terminal,[binding(run/'capture/receipt.json'),binding(run/'capture/COLLECTION_CLOSED.json')],'completed'
    if kind=='integrity':
        from .closeout import audit_capture_readonly
        result=audit_capture_readonly(run/'capture',p['candidate']['candidate_dir'],attempt/'audit')
        if result['problem_counts']:raise Held('RAW_INTEGRITY_FAILED')
        audit_path=attempt/'audit/RAW_CAPTURE_AUDIT.json';artifacts=[binding(audit_path)]
        if config.get('qualification_policy') and result['execution']['state'] in {'COMPLETE','STOPPED'}:
            supervisor_path=attempt/'audit/PAPER_RUN_RESULT.json'
            code=offline_child(config,'prospective_supervisor.py',[
                '--candidate',p['candidate']['candidate_dir'],'--run-dir',run,'--raw-audit',audit_path,
                '--manifest-sha256',p['candidate']['source_manifest_sha256'],'--output',supervisor_path],attempt/'supervisor-audit.log')
            if code or not supervisor_path.is_file():
                # Raw integrity and genuine replay eligibility are distinct.
                # A valid raw audit still supports bounded partial accounting.
                return {**result,'supervisor_bridge':'FAILED','worker_exit_code':code},artifacts,'completed'
            result={**result,'supervisor_bridge':'PASS','supervisor_exit_proved':True}
            artifacts.append(binding(supervisor_path))
        return result,artifacts,'completed'
    if kind=='accounting':
        from .closeout import closeout_capture
        result=closeout_capture(run,attempt/'accounting',p['candidate']['candidate_dir'])
        return result,[binding(attempt/'accounting/CLOSEOUT.json')],'completed'
    if kind=='replay':
        from .research import build_replay_commands, assess_replay_results
        if config.get('qualification_policy'):
            from .qualification import prospective_replay_pass
            integrity=source_result(queue,p['forward_job'],'integrity')
            artifact_paths={a['name']:a['path'] for a in integrity['artifacts']}
            if 'PAPER_RUN_RESULT.json' not in artifact_paths:raise Held('GENUINE_PROSPECTIVE_SUPERVISOR_PROOF_REQUIRED')
            # The supervisor was bound to the original streaming audit. Both
            # original and published audit must still have the same sealed bytes.
            supervisor_path=Path(artifact_paths['PAPER_RUN_RESULT.json'])
            supervisor=read_json(supervisor_path)
            audit_binding=supervisor['bindings']['raw_audit']
            if binding(audit_binding['path'])!=audit_binding or binding(artifact_paths['RAW_CAPTURE_AUDIT.json'])['sha256']!=audit_binding['sha256']:
                raise Held('PROSPECTIVE_RAW_AUDIT_ARTIFACT_CHANGED')
            for artifact in integrity['artifacts']:
                if binding(artifact['path'])['sha256']!=artifact['sha256']:raise Held('PROSPECTIVE_INTEGRITY_ARTIFACT_CHANGED')
            out=attempt/'replay'
            code=offline_child(config,'qualification_replay.py',[
                '--candidate',p['candidate']['candidate_dir'],'--run-dir',run,'--raw-audit',audit_binding['path'],
                '--supervisor-result',supervisor_path,'--manifest-sha256',p['candidate']['source_manifest_sha256'],
                '--output',out],attempt/'replay.log')
            result_path=out/'RESULT.json'
            if not result_path.exists():raise Held('PROSPECTIVE_REPLAY_NO_RESULT_EXIT_'+str(code))
            result=read_json(result_path)
            parity=assess_replay_results(result)
            return {'driver_result':binding(result_path),'parity':parity,'driver_exit_code':code},[binding(result_path)],\
                'completed' if code==0 and parity['all_requested_parity_passed'] and prospective_replay_pass(result) else 'failed'
        plan=build_replay_commands(config['app'],config['interpreter'],candidate=p['candidate']['candidate_dir'],study=run,output=attempt/'replay')
        atomic_json(attempt/'REPLAY_PLAN.json',plan)
        if plan['state']!='READY_FOR_ADMISSION':raise Held(';'.join(plan['reasons']))
        if p['candidate'].get('clock_control_supported'):
            raise Held('REPLAY_ADMISSION_EVENT_CONTRACT_UNQUALIFIED')
        for step in plan['commands']:
            command=[config['interpreter'],'-I','-B',str(Path(config['cli']).with_name('offline.py')),'--',*step['command'][3:]]
            with (attempt/(step['type']+'.log')).open('wb') as log:
                subprocess.run(command,stdout=log,stderr=log,check=True,timeout=config['maximum_replay_seconds'],env=worker_environment(root),
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        result_path=attempt/'replay/study/RESULT.json'
        if not result_path.exists():raise Held('REPLAY_RESULT_SCHEMA_REQUIRES_RECONCILIATION')
        result=assess_replay_results(read_json(result_path))
        return result,[binding(result_path)],'completed' if result['all_requested_parity_passed'] else 'failed'
    if kind=='analysis':
        from .research import analyze_closeout
        audit=source_result(queue,p['forward_job'],'integrity');accounting=source_result(queue,p['forward_job'],'accounting')
        evidence={'artifacts':{'terminal_receipt':binding(run/'capture/receipt.json'),
            'raw_audit':audit['artifacts'][0],'accounting':accounting['artifacts'][0]},
            'source_sha256':config['source_sha256'],'config_sha256':digest(config)}
        if config.get('qualification_policy'):
            try:replay=source_result(queue,p['forward_job'],'replay')
            except Held:pass
            else:evidence['artifacts']['prospective_replay']=replay['artifacts'][0]
        result=analyze_closeout(evidence,attempt/'analysis')
        return result,result.get('artifacts',[]),'completed'
    if kind=='decision':
        result=source_result(queue,p['forward_job'],'analysis')
        return {'decision':'INSUFFICIENT_EVIDENCE','capital_review_open':False,'analysis':result['artifacts'],
                'reason':'Descriptive closeout is not untouched powered confirmation'},[],'completed'
    if kind=='report':
        stages=p['closure_snapshot']
        result={'utc':utc(),'forward':queue.get(p['forward_job'])['state'],'stages':stages,
                'decision':'INSUFFICIENT_EVIDENCE','partial_analysis_allowed':True}
        atomic_json(attempt/'CLOSEOUT_STATUS.json',result)
        return result,[binding(attempt/'CLOSEOUT_STATUS.json')],'completed'
    raise Held('UNSUPPORTED_JOB_TYPE')


def run_worker(root,job_id):
    from .service import load_config
    if sys.stdin.buffer.readline()!=b'LANE_II_JOB_START\n':raise RuntimeError('OWNED_JOB_STARTUP_GATE_REQUIRED')
    root=Path(root);config=load_config(root);deny_python_network()
    queue=Registry(root/'jobs.sqlite');owner=process_identity();owner['source_sha256']=config['source_sha256']
    job=queue.claim(owner,job_id=job_id)
    if job is None:return {'state':'NOT_CLAIMABLE'}
    if queue.reuse_published(job,root/'outputs'):
        if job['type']=='partition':
            result=read_json(root/'outputs'/(job['id']+'.json'))['result']
            queue.fact('partition:'+job['payload']['forward_job'],{'hour':job['payload']['hour'],'partition':result},set_value=True)
        return {'state':'REUSED_SEALED_OUTPUT'}
    done=threading.Event();lease_lost=[]
    def heartbeat():
        while not done.wait(20):
            try:queue.heartbeat(job['id'],job['fence'])
            except BaseException as exc:
                lease_lost.append(type(exc).__name__)
                # Terminating this owned worker closes its nested collector Job.
                os._exit(74)
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    attempt=root/'work'/job['id']/str(job['fence']);attempt.mkdir(parents=True,exist_ok=False)
    try:
        queue.heartbeat(job['id'],job['fence'],checkpoint={'attempt_path':str(attempt)})
        result,artifacts,state=handle(job,config,queue,attempt)
        queue.publish(job['id'],job['fence'],root/'outputs',result,artifacts=artifacts,final_state=state,
                      reason='OUTPUT_SEALED' if state=='completed' else 'SOURCE_EXECUTION_FAILED')
        if job['type']=='partition':
            queue.fact('partition:'+job['payload']['forward_job'],{'hour':job['payload']['hour'],'partition':result},set_value=True)
        return {'state':state}
    except Held as exc:
        queue.hold(job['id'],job['fence'],str(exc),'Reconcile exact missing evidence/cause; explicit bounded release after verification')
        return {'state':'admission-held','reason':str(exc)}
    except (ValueError,KeyError,TypeError,FileNotFoundError) as exc:
        queue.hold(job['id'],job['fence'],'DETERMINISTIC:'+type(exc).__name__+':'+str(exc)[:200],
                   'Repair versioned handler/input binding and re-review; no blind retry')
        return {'state':'admission-held'}
    except BaseException as exc:
        # Only explicitly typed temporary local I/O contention is retried.
        retry=isinstance(exc,PermissionError)
        state=queue.fail(job['id'],job['fence'],type(exc).__name__+':'+str(exc)[:200],retryable=retry,
                         retry_delay=min(900,30*2**(job['attempts']-1)))
        return {'state':state}
    finally:done.set();thread.join(timeout=1)
