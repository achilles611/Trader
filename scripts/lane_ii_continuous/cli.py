"""Frozen local Lane II coordinator entry point (no model client)."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys

if __package__ in {None,''}:
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.common import atomic_json, binding, digest, read_json, utc
from scripts.lane_ii_continuous.queue import Registry
from scripts.lane_ii_continuous.resources import DEFAULT_LIMITS


def initialize(root,repo,candidate,*,port=8889,paper_risk_manifest=None,prewarm_recovery=None):
    if prewarm_recovery is not None and paper_risk_manifest is None:
        raise ValueError('PREWARM_REPLACEMENT_REQUIRES_EXISTING_CAMPAIGN')
    root=Path(root).resolve();repo=Path(repo).resolve()
    if root.exists() and any(root.iterdir()):raise ValueError('NEW_EMPTY_RUNTIME_REQUIRED')
    root.mkdir(parents=True,exist_ok=True)
    app=root/'app';app.mkdir()
    for relative in ('scripts/lane_ii_continuous','scripts/slim_paper_operator','scripts/overnight_paper','scripts/btc_next_paper'):
        shutil.copytree(repo/relative,app/relative,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    files=[binding(p) for p in sorted(app.rglob('*')) if p.is_file()]
    source={'schema':'LANE_II_FROZEN_APPLICATION_V1','files':files}
    for directory in ('outputs','work','temp','logs','public','launches','worker-owners','worker-exits','installation'):
        (root/directory).mkdir()
    interpreter=binding(sys.executable)
    pythonw=Path(sys.executable).with_name('pythonw.exe')
    config={'schema':'LANE_II_SERVICE_CONFIG_V1','root':str(root),'app':str(app),
        'cli':str(app/'scripts/lane_ii_continuous/cli.py'),'source_sha256':digest(source),
        'interpreter':str(Path(sys.executable).resolve()),'interpreter_sha256':interpreter['sha256'],
        'interpreter_windows':binding(pythonw) if pythonw.exists() else interpreter,
        'limits':dict(DEFAULT_LIMITS),'prewarm_seconds':145,'closeout_wait_seconds':90,'maximum_replay_seconds':3600,
        'status_port':port,'paper_only':True,'model_enrichment_enabled':False,
        'coordinator_task':'Trader-LaneII-Continuous-Coordinator','watchdog_task':'Trader-LaneII-Continuous-Watchdog',
        'candidate':read_json(candidate),'automatic_fresh_account_restarts':False,'scheduled_runtime_model_calls':0,
        'public_remote_publish':'MANUAL_CONNECTED_GITHUB_REVIEW_PATH; no credential copied into runtime',
        'risk_scope':'International counterfactual slim study; separate US PAPER campaign kernel unjoined; launch held pending scope review'}
    if paper_risk_manifest is not None:
        from scripts.lane_ii_continuous.qualification import POLICY,phase_payload,phase_segment
        from scripts.lane_ii_continuous.capture import verify_candidate
        from scripts.lane_ii_continuous.paper_risk import POLICY as RISK_POLICY, digest as risk_digest
        campaign=read_json(paper_risk_manifest)
        verify_candidate(config['candidate'])
        if (campaign['schema']!='LANE_II_PAPER_RISK_ANCHOR_V1' or campaign['policy']!=RISK_POLICY
                or campaign['policy_sha256']!=risk_digest(RISK_POLICY) or campaign['maximum_loss']!='25.00'
                or campaign['roster_sha256']!=config['candidate']['roster_sha256']
                or config['candidate'].get('risk_anchor_required') is not True
                or config['candidate'].get('paper_only') is not True):
            raise ValueError('EXACT_FROZEN_PROSPECTIVE_CAMPAIGN_REQUIRED')
        config.update(qualification_policy=POLICY,paper_risk_manifest=binding(paper_risk_manifest),
                      campaign_id=campaign['campaign_id'],
                      risk_scope='International shared paper campaign; unchanged separate US PAPER kernel')
        predecessor=None
        if prewarm_recovery is not None:
            from scripts.lane_ii_continuous.prewarm_recovery import validate_replacement
            config['prewarm_recovery']=binding(prewarm_recovery)
            predecessor=validate_replacement(config,config['prewarm_recovery'])
        payload=phase_payload(config,'short',predecessor=predecessor)
        experiment_id=config['campaign_id'];segment_id=phase_segment(config,'short')
    else:
        payload={'run_dir':str(root/'runs/prospective-short-001'),'candidate':config['candidate'],'seconds':900}
        experiment_id='prospective-short-001';segment_id='segment-001'
    atomic_json(root/'SOURCE.json',source);atomic_json(root/'CONFIG.json',config)
    atomic_json(root/'CONTROL.json',{'mode':'run','utc':utc(),'reason':'Install held; admission gates remain enforced'})
    registry=Registry(root/'jobs.sqlite')
    registry.fact('qualification',{'engineering_reviewed':False,'paper_scope_reviewed':False,'replay_reproduced':False,
        'short_pipeline_qualified':False,'eight_hour_qualified':False,'twenty_four_hour_qualified':False,
        'profitability_qualified':False,'reason':'Evidence gates are not implied by installation'},set_value=True)
    from scripts.lane_ii_continuous.service import enqueue
    job=enqueue(registry,config,'forward',payload,experiment_id=experiment_id,segment_id=segment_id,
        priority=80,estimated_bytes=2*2**30,resource={'class':'forward'},max_retries=0)
    atomic_json(root/'INSTALL_PREPARATION.json',{'utc':utc(),'source':binding(root/'SOURCE.json'),
        'config':binding(root/'CONFIG.json'),'interpreter':interpreter,'short_job_id':job['id'],
        'scheduler_installed':False,'capture_launched':False})
    return {'root':str(root),'config_sha256':digest(config),'source_sha256':digest(source),'short_job_id':job['id']}


def control(root,mode):
    from scripts.lane_ii_continuous.common import same_process
    root=Path(root);registry=Registry(root/'jobs.sqlite')
    if mode=='run':
        unresolved=[j for j in registry.jobs({'leased','running'}) if same_process(j['owner']) is not True]
        if unresolved:raise ValueError('Resume requires prior owner/lease reconciliation by coordinator')
        stop=root/'STOP_WORKERS.json'
        if stop.exists():
            # Preserve old control evidence rather than erasing an exit reason.
            destination=root/'installation'/('previous-stop-'+str(__import__('time').time_ns())+'.json')
            stop.replace(destination)
    value={'mode':mode,'utc':utc(),'reason':'Explicit operator command; qualification/circuit gates remain enforced'}
    atomic_json(root/'CONTROL.json',value)
    registry.fact('last_operator_control',value,set_value=True)
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('init','run','worker','watchdog','status','pause','stop','resume','import','release-short'))
    parser.add_argument('--root',required=True,type=Path);parser.add_argument('--repo',type=Path)
    parser.add_argument('--candidate',type=Path);parser.add_argument('--job');parser.add_argument('--seconds',type=float)
    parser.add_argument('--payload',type=Path);parser.add_argument('--port',type=int,default=8889)
    parser.add_argument('--paper-risk-manifest',type=Path)
    parser.add_argument('--prewarm-recovery',type=Path)
    args=parser.parse_args()
    if args.command=='init':result=initialize(args.root,args.repo,args.candidate,port=args.port,paper_risk_manifest=args.paper_risk_manifest,
                                               prewarm_recovery=args.prewarm_recovery)
    elif args.command=='release-short':
        from scripts.lane_ii_continuous.service import load_config
        from scripts.lane_ii_continuous.qualification import register_short_release
        result=register_short_release(load_config(args.root),Registry(args.root/'jobs.sqlite'),args.payload)
    elif args.command in {'pause','stop','resume'}:result=control(args.root,{'pause':'pause-new-work','stop':'stop','resume':'run'}[args.command])
    elif args.command=='status':
        from scripts.lane_ii_continuous.common import same_process
        result=read_json(args.root/'STATUS.json');result['owner_alive_now']=same_process(result['owner'])
        result['heartbeat_age_seconds_now']=__import__('time').monotonic()-result['heartbeat_monotonic']
    elif args.command=='worker':
        from scripts.lane_ii_continuous.worker import run_worker
        result=run_worker(args.root,args.job)
    elif args.command=='run':
        from scripts.lane_ii_continuous.service import Service
        from scripts.lane_ii_continuous.worker import deny_python_network
        deny_python_network();result=Service(args.root).run(args.seconds)
    elif args.command=='watchdog':
        from scripts.lane_ii_continuous.service import watchdog
        result=watchdog(args.root)
    else:
        from scripts.lane_ii_continuous.service import load_config,enqueue
        config=load_config(args.root);payload=read_json(args.payload)
        result=enqueue(Registry(args.root/'jobs.sqlite'),config,'import',payload,priority=1,estimated_bytes=4*2**20)
    if sys.stdout:print(json.dumps(result,indent=2))


if __name__=='__main__':main()
