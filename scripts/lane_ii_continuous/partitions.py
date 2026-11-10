"""Seal complete append-only journal byte ranges without rotating account state."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .common import atomic_json, binding, file_hash, read_json


def seal_partition(journal, output, *, previous=None, max_bytes=2*2**30):
    journal=Path(journal).resolve(); output=Path(output).resolve()
    if output==journal or journal.parent in output.parents:
        raise ValueError('Partition output must be outside the live capture')
    def range_hash(start,end):
        h=hashlib.sha256()
        with journal.open('rb') as stream:
            stream.seek(start)
            while start<end:
                chunk=stream.read(min(1024*1024,end-start))
                if not chunk:raise ValueError('Source range truncated')
                h.update(chunk);start+=len(chunk)
        return h.hexdigest()
    info=journal.stat()
    identity={'device':info.st_dev,'inode':info.st_ino,'creation_ns':getattr(info,'st_birthtime_ns',None)}
    start=0
    if previous is not None:
        manifest=Path(previous['manifest_path'])
        if file_hash(manifest)!=previous['manifest_sha256']:raise ValueError('Prior manifest changed')
        stored=read_json(manifest)
        if stored!={k:v for k,v in previous.items() if k not in {'manifest_path','manifest_sha256'}}:
            raise ValueError('Prior manifest fields changed')
        if previous['source_journal']!=str(journal):raise ValueError('Partition source changed')
        if previous['source_identity']!=identity:raise ValueError('Journal identity changed')
        a,b=previous['start_offset'],previous['end_offset']
        if not 0<=a<=b or previous['payload']['bytes']!=b-a:raise ValueError('Invalid prior byte range')
        if binding(previous['payload']['path'])!=previous['payload']:raise ValueError('Prior partition changed')
        if range_hash(a,b)!=previous['payload']['sha256']:raise ValueError('Source preceding range changed')
        start=previous['end_offset']
    size=journal.stat().st_size
    if size<start or size-start>max_bytes:raise ValueError('Partition range exceeds bound or source truncated')
    output.mkdir(parents=True,exist_ok=False)
    target=output/'events.jsonl'
    h=hashlib.sha256();lines=0;end=start
    with journal.open('rb') as source,target.open('xb') as dest:
        source.seek(start)
        while source.tell()<size:
            # The size at entry is a fixed boundary; an incomplete last line is
            # left for the next partition, even if the live writer completes it.
            line=source.readline(min(8*2**20,size-source.tell())+1)
            if len(line)>8*2**20:raise ValueError('Journal line too large')
            if source.tell()>size or not line.endswith(b'\n'):break
            json.loads(line)
            dest.write(line);h.update(line);lines+=1;end+=len(line)
        dest.flush();os.fsync(dest.fileno())
    check=hashlib.sha256()
    with journal.open('rb') as source:
        source.seek(start);remaining=end-start
        while remaining:
            chunk=source.read(min(1024*1024,remaining))
            if not chunk:raise ValueError('Source changed while sealing')
            check.update(chunk);remaining-=len(chunk)
    if check.hexdigest()!=h.hexdigest():raise ValueError('Source prefix changed while sealing')
    final_info=journal.stat()
    if (final_info.st_dev,final_info.st_ino)!=(info.st_dev,info.st_ino):raise ValueError('Journal replaced during copy')
    result={'schema':'LANE_II_CAPTURE_PARTITION_V1','source_journal':str(journal),'source_identity':identity,
            'start_offset':start,'end_offset':end,'source_observed_size':size,'complete_lines':lines,
            'payload':binding(target),'previous_partition_sha256':None if previous is None else previous['manifest_sha256'],
            'account_reset':False,'capture_continues':True,'semantic_integrity':'PENDING_SEPARATE_AUDIT',
            'raw_http_files':'Retained immutable source artifacts; no active evidence moved or deleted'}
    manifest=output/'PARTITION.json';atomic_json(manifest,result)
    result['manifest_sha256']=file_hash(manifest)
    result['manifest_path']=str(manifest)
    return result
