"""Bounded transactional work registry with fencing and idempotent publication.

Delivery is at least once. Published output is keyed by immutable job input, not
by a process attempt. SQLite and output files must live on the same local disk.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .common import atomic_bytes, atomic_json, canonical, digest, file_hash, read_json, utc

TERMINAL = {"completed", "failed", "cancelled"}
RESERVED_TYPES = {"seal", "integrity", "accounting", "analysis", "report", "closeout", "recovery", "partition", "replay", "decision"}


class QueueFull(RuntimeError):
    pass


class FenceLost(RuntimeError):
    pass


class Registry:
    def __init__(self, path, *, max_pending=None, reserve_jobs=None,
                 max_output_bytes=None, reserve_output_bytes=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        supplied = dict(max_pending=max_pending,reserve_jobs=reserve_jobs,
                        max_output_bytes=max_output_bytes,reserve_output_bytes=reserve_output_bytes)
        defaults = dict(max_pending=128,reserve_jobs=16,max_output_bytes=64*2**30,reserve_output_bytes=4*2**30)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
              id TEXT PRIMARY KEY, dedupe TEXT NOT NULL UNIQUE, type TEXT NOT NULL,
              parent_id TEXT, experiment_id TEXT, segment_id TEXT,
              payload TEXT NOT NULL, input_hash TEXT NOT NULL, code_hash TEXT NOT NULL,
              config_hash TEXT NOT NULL, priority INTEGER NOT NULL, state TEXT NOT NULL,
              created REAL NOT NULL, due REAL NOT NULL, deadline REAL,
              started REAL, ended REAL, dependencies TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL,
              estimated_bytes INTEGER NOT NULL, resource TEXT NOT NULL,
              owner TEXT, fence INTEGER NOT NULL DEFAULT 0, lease_until REAL,
              lease_monotonic REAL, lease_boot REAL,
              checkpoint TEXT, terminal_reason TEXT, release_condition TEXT,
              artifact_manifest TEXT, analysis_state TEXT NOT NULL DEFAULT 'not_queued'
            );
            CREATE INDEX IF NOT EXISTS due_jobs ON jobs(state,due,priority);
            CREATE TABLE IF NOT EXISTS transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
              at REAL NOT NULL, previous TEXT, state TEXT NOT NULL, fence INTEGER NOT NULL,
              reason TEXT, detail TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS experiments (
              id TEXT PRIMARY KEY, recovery_count INTEGER NOT NULL DEFAULT 0,
              circuit TEXT, release_condition TEXT
            );
            CREATE TABLE IF NOT EXISTS facts (
              key TEXT PRIMARY KEY, value TEXT NOT NULL, updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queue_policy (id INTEGER PRIMARY KEY CHECK(id=1), policy TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS experiment_events (
              seq INTEGER PRIMARY KEY AUTOINCREMENT, experiment_id TEXT NOT NULL,
              at REAL NOT NULL, event TEXT NOT NULL, detail TEXT NOT NULL
            );
            """)
            columns={r[1] for r in db.execute('PRAGMA table_info(jobs)')}
            for column in ('lease_monotonic','lease_boot'):
                if column not in columns:db.execute('ALTER TABLE jobs ADD COLUMN '+column+' REAL')
            db.execute("BEGIN IMMEDIATE")
            try:
                stored=db.execute("SELECT policy FROM queue_policy WHERE id=1").fetchone()
                policy=json.loads(stored[0]) if stored else {k: defaults[k] if v is None else v for k,v in supplied.items()}
                if stored and any(v is not None and v != policy[k] for k,v in supplied.items()):
                    raise ValueError("Persisted queue policy mismatch")
                if (any(type(v) is not int or v < 0 for v in policy.values())
                        or not 0 <= policy['reserve_jobs'] < policy['max_pending']
                        or not 0 <= policy['reserve_output_bytes'] < policy['max_output_bytes']):
                    raise ValueError("Invalid queue reserve")
                if not stored:
                    db.execute("INSERT INTO queue_policy(id,policy) VALUES(1,?)",(canonical(policy).decode(),))
                db.commit()
            except BaseException:
                db.rollback();raise
        for key,value in policy.items():setattr(self,key,value)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=2000")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        value = dict(row)
        for key in ("payload", "dependencies", "resource", "owner", "checkpoint", "artifact_manifest"):
            if value.get(key) is not None:
                value[key] = json.loads(value[key])
        return value

    @staticmethod
    def _event(db, job_id, previous, state, fence, reason=None, detail=None, now=None):
        db.execute("INSERT INTO transitions(job_id,at,previous,state,fence,reason,detail) VALUES(?,?,?,?,?,?,?)",
                   (job_id, time.time() if now is None else now, previous, state, fence, reason,
                    canonical({"monotonic":time.monotonic(),**(detail or {})}).decode()))

    def enqueue(self, kind, payload, *, input_hash, code_hash, config_hash, version=1,
                parent_id=None, experiment_id=None, segment_id=None, priority=50,
                due=None, deadline=None, dependencies=(), estimated_bytes=0,
                resource=None, max_retries=3, now=None):
        now = time.time() if now is None else now
        if not 0 <= max_retries <= 3 or estimated_bytes < 0:
            raise ValueError("Invalid retry/output bound")
        key = digest({"type": kind, "version": version, "input_hash": input_hash,
                      "code": code_hash, "config": config_hash})
        with self.transaction() as db:
            old = db.execute("SELECT * FROM jobs WHERE dedupe=?", (key,)).fetchone()
            if old:
                if json.loads(old["payload"]) != payload or json.loads(old["dependencies"]) != list(dependencies):
                    raise ValueError("Dedupe key reused with changed payload/dependencies")
                return self._decode(old)
            count, size = db.execute("SELECT COUNT(*),COALESCE(SUM(estimated_bytes),0) FROM jobs WHERE state NOT IN ('completed','failed','cancelled')").fetchone()
            reserved = kind in RESERVED_TYPES
            if count >= self.max_pending - (0 if reserved else self.reserve_jobs):
                raise QueueFull("Pending job count reserve")
            if size + estimated_bytes > self.max_output_bytes - (0 if reserved else self.reserve_output_bytes):
                raise QueueFull("Pending output-byte reserve")
            job_id = uuid.uuid4().hex
            db.execute("""INSERT INTO jobs(id,dedupe,type,parent_id,experiment_id,segment_id,payload,
                input_hash,code_hash,config_hash,priority,state,created,due,deadline,dependencies,
                max_retries,estimated_bytes,resource) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job_id,key,kind,parent_id,experiment_id,segment_id,canonical(payload).decode(),input_hash,
                 code_hash,config_hash,priority,"queued",now,now if due is None else due,deadline,
                 canonical(list(dependencies)).decode(),max_retries,estimated_bytes,canonical(resource or {}).decode()))
            if experiment_id:
                db.execute("INSERT OR IGNORE INTO experiments(id) VALUES(?)", (experiment_id,))
            self._event(db,job_id,None,"queued",0,now=now)
            return self._decode(db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    @staticmethod
    def _clock(monotonic=None,boot_epoch=None):
        import psutil
        return (time.monotonic() if monotonic is None else monotonic,
                psutil.boot_time() if boot_epoch is None else boot_epoch)

    def claim(self, owner, *, now=None, lease_seconds=180, allowed_types=None,monotonic=None,boot_epoch=None,job_id=None):
        now = time.time() if now is None else now
        mono,boot=self._clock(monotonic,boot_epoch)
        if (type(owner.get('pid')) is not int or owner['pid']<=0
                or not all(type(owner.get(k)) in {int,float} for k in ('creation_epoch','boot_epoch'))
                or not isinstance(owner.get('executable'),str) or not owner['executable']
                or not isinstance(owner.get('executable_sha256'),str) or len(owner['executable_sha256'])!=64):
            raise ValueError('Complete process ownership identity required')
        with self.transaction() as db:
            rows = db.execute("SELECT * FROM jobs WHERE state='queued' AND due<=? ORDER BY priority,created", (now,)).fetchall()
            for row in rows:
                if job_id is not None and row['id']!=job_id:continue
                if allowed_types is not None and row["type"] not in allowed_types:
                    continue
                if row["deadline"] is not None and row["deadline"] < now:
                    db.execute("UPDATE jobs SET state='failed',ended=?,terminal_reason='DEADLINE_BEFORE_START' WHERE id=?",(now,row["id"]))
                    self._event(db,row["id"],"queued","failed",row["fence"],"DEADLINE_BEFORE_START",now=now)
                    continue
                deps = [db.execute("SELECT state FROM jobs WHERE id=?",(d,)).fetchone() for d in json.loads(row["dependencies"])]
                if any(d is None or d[0] in {"failed","cancelled"} for d in deps):
                    db.execute("UPDATE jobs SET state='admission-held',terminal_reason='DEPENDENCY_UNAVAILABLE',release_condition='Reconcile missing/failed dependency' WHERE id=?",(row["id"],))
                    self._event(db,row["id"],"queued","admission-held",row["fence"],"DEPENDENCY_UNAVAILABLE",now=now)
                    continue
                if any(d[0] != "completed" for d in deps):
                    continue
                if row["experiment_id"] and row["type"] not in RESERVED_TYPES:
                    ex = db.execute("SELECT circuit FROM experiments WHERE id=?",(row["experiment_id"],)).fetchone()
                    if ex and ex[0]:
                        continue
                fence = row["fence"] + 1
                db.execute("UPDATE jobs SET state='leased',owner=?,fence=?,lease_until=?,lease_monotonic=?,lease_boot=?,attempts=attempts+1,started=COALESCE(started,?),terminal_reason=NULL,release_condition=NULL WHERE id=?",
                           (canonical(owner).decode(),fence,now+lease_seconds,mono+lease_seconds,boot,now,row["id"]))
                self._event(db,row["id"],"queued","leased",fence,detail={"owner":owner,
                            "lease_until":now+lease_seconds,"lease_monotonic":mono+lease_seconds,
                            "lease_boot":boot,"attempt":row['attempts']+1},now=now)
                return self._decode(db.execute("SELECT * FROM jobs WHERE id=?",(row["id"],)).fetchone())
        return None

    @classmethod
    def _owned(cls,db, job_id, fence, now,monotonic=None,boot_epoch=None):
        mono,boot=cls._clock(monotonic,boot_epoch)
        row = db.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone()
        if (row is None or row["fence"] != fence or row["state"] not in {"leased","running"}
                or row['lease_monotonic'] is None or row['lease_boot'] is None
                or abs(row['lease_boot']-boot)>2 or row['lease_monotonic']<mono):
            raise FenceLost(job_id)
        return row

    def heartbeat(self, job_id, fence, *, checkpoint=None, lease_seconds=180, now=None,monotonic=None,boot_epoch=None):
        now = time.time() if now is None else now
        mono,boot=self._clock(monotonic,boot_epoch)
        with self.transaction() as db:
            row = self._owned(db,job_id,fence,now,mono,boot)
            db.execute("UPDATE jobs SET state='running',lease_until=?,lease_monotonic=?,checkpoint=COALESCE(?,checkpoint) WHERE id=?",
                       (now+lease_seconds,mono+lease_seconds,None if checkpoint is None else canonical(checkpoint).decode(),job_id))
            self._event(db,job_id,row["state"],"running",fence,"HEARTBEAT",
                        {"lease_until":now+lease_seconds,"lease_monotonic":mono+lease_seconds,"checkpoint":checkpoint},now)

    def publish(self, job_id, fence, output_dir, result, *, artifacts=(), now=None, crash_after_output=False,
                final_state="completed", reason="OUTPUT_SEALED",monotonic=None,boot_epoch=None):
        """Fence the publication, fsync its receipt, then acknowledge it.

        A crash in the file/SQLite gap leaves a recoverable identical receipt.
        Artifact hashes are verified before a completion transition is committed.
        """
        now = time.time() if now is None else now
        if final_state not in TERMINAL:
            raise ValueError("Publication requires a terminal execution state")
        output_dir = Path(output_dir).resolve()
        output_dir.mkdir(parents=True,exist_ok=True)
        receipt_path = output_dir / (job_id + ".json")
        with self.transaction() as db:
            row = self._owned(db,job_id,fence,now,monotonic,boot_epoch)
            sealed=[]
            for artifact in artifacts:
                p = Path(artifact["path"]).resolve()
                if not p.is_file() or p.stat().st_size != artifact["bytes"] or file_hash(p) != artifact["sha256"]:
                    raise ValueError("Output artifact not sealed")
                if artifact['bytes']>64*2**20:raise ValueError('Publish a manifest for large evidence, not raw payloads')
                name=artifact.get('name',p.name)
                if Path(name).name!=name or '/' in name or '\\' in name:raise ValueError('Invalid artifact name')
                owned=output_dir/job_id/'artifacts'/(artifact['sha256']+'-'+name)
                if owned.exists():
                    if file_hash(owned)!=artifact['sha256']:raise ValueError('Published artifact changed')
                else:
                    payload=p.read_bytes()
                    import hashlib
                    if hashlib.sha256(payload).hexdigest()!=artifact['sha256']:raise ValueError('Artifact changed during publication')
                    atomic_bytes(owned,payload)
                    owned.chmod(0o444)
                sealed.append({'path':str(owned),'name':name,'bytes':owned.stat().st_size,'sha256':artifact['sha256']})
            receipt = {"schema":"LANE_II_JOB_OUTPUT_V1","job_id":job_id,"input_hash":row["input_hash"],
                       "code_hash":row["code_hash"],"config_hash":row["config_hash"],
                       "result":result,"artifacts":sealed,"execution_state":final_state,"terminal_reason":reason}
            if receipt_path.exists():
                if read_json(receipt_path) != receipt:
                    raise ValueError("Idempotent output differs")
            else:
                atomic_json(receipt_path,receipt)
            manifest = {"path":str(receipt_path),"bytes":receipt_path.stat().st_size,"sha256":file_hash(receipt_path)}
            if crash_after_output:
                raise RuntimeError("INJECTED_CRASH_AFTER_OUTPUT")
            db.execute("UPDATE jobs SET state=?,ended=?,lease_until=NULL,artifact_manifest=?,terminal_reason=? WHERE id=?",
                       (final_state,now,canonical(manifest).decode(),reason,job_id))
            self._event(db,job_id,row["state"],final_state,fence,reason,manifest,now)
            return manifest

    def reuse_published(self, job, output_dir, *, now=None,monotonic=None,boot_epoch=None):
        p = Path(output_dir) / (job["id"] + ".json")
        if not p.exists():
            return None
        receipt = read_json(p)
        if any(receipt.get(k) != job[k] for k in ("input_hash","code_hash","config_hash")):
            raise ValueError("Published output identity mismatch")
        return self.publish(job["id"],job["fence"],output_dir,receipt["result"],artifacts=receipt["artifacts"],now=now,
                            final_state=receipt['execution_state'],reason=receipt['terminal_reason'],
                            monotonic=monotonic,boot_epoch=boot_epoch)

    def fail(self, job_id, fence, reason, *, retryable=False, retry_delay=30, now=None,monotonic=None,boot_epoch=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            row = self._owned(db,job_id,fence,now,monotonic,boot_epoch)
            retry = retryable and row["attempts"] <= row["max_retries"]
            state = "queued" if retry else "failed"
            db.execute("UPDATE jobs SET state=?,due=?,ended=?,lease_until=NULL,terminal_reason=? WHERE id=?",
                       (state,now+max(1,min(retry_delay,900)),None if retry else now,reason,job_id))
            self._event(db,job_id,row["state"],state,fence,reason,{"retryable":retryable,"retry_scheduled":retry},now)
            return state

    def hold(self, job_id, fence, reason, release_condition, *, now=None,monotonic=None,boot_epoch=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            row = self._owned(db,job_id,fence,now,monotonic,boot_epoch)
            db.execute("UPDATE jobs SET state='admission-held',lease_until=NULL,terminal_reason=?,release_condition=? WHERE id=?",
                       (reason,release_condition,job_id))
            self._event(db,job_id,row["state"],"admission-held",fence,reason,{"release_condition":release_condition},now)

    def release_hold(self, job_id, evidence, *, now=None):
        if not evidence or not evidence.get("satisfied"):
            raise ValueError("Release requires explicit satisfied evidence")
        now = time.time() if now is None else now
        with self.transaction() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone()
            if row is None or row["state"] != "admission-held":
                return False
            db.execute("UPDATE jobs SET state='queued',due=?,terminal_reason=NULL,release_condition=NULL WHERE id=?",(now,job_id))
            self._event(db,job_id,"admission-held","queued",row["fence"],"HOLD_RELEASED",evidence,now)
            return True

    def hold_pending(self,job_id,reason,release_condition):
        with self.transaction() as db:
            row=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
            if row is None or row['state']!='queued':return False
            db.execute("UPDATE jobs SET state='admission-held',terminal_reason=?,release_condition=? WHERE id=?",
                       (reason,release_condition,job_id))
            self._event(db,job_id,'queued','admission-held',row['fence'],reason,{'release_condition':release_condition})
            return True

    def recover_leases(self, owner_alive, *, now=None,monotonic=None,boot_epoch=None):
        now = time.time() if now is None else now
        mono,boot=self._clock(monotonic,boot_epoch)
        changed=[]
        with self.transaction() as db:
            for row in db.execute("SELECT * FROM jobs WHERE state IN ('leased','running')").fetchall():
                if (row['lease_monotonic'] is not None and row['lease_boot'] is not None
                        and abs(row['lease_boot']-boot)<=2 and row['lease_monotonic']>=mono):continue
                alive = owner_alive(json.loads(row["owner"]))
                state = "admission-held" if alive is not False else ("queued" if row["attempts"] <= row["max_retries"] else "failed")
                reason = "OWNERSHIP_STILL_ALIVE" if alive is True else "OWNERSHIP_UNKNOWN" if alive is None else "LEASE_EXPIRED"
                db.execute("UPDATE jobs SET state=?,fence=fence+1,lease_until=NULL,due=?,ended=?,terminal_reason=?,release_condition=? WHERE id=?",
                           (state,now,now if state=='failed' else None,reason,"Verify prior owner exit" if alive is not False else None,row["id"]))
                self._event(db,row["id"],row["state"],state,row["fence"]+1,reason,
                            {"expired_owner":json.loads(row['owner']),"expired_lease_until":row['lease_until'],
                             "attempt_ended":now,"expired_fence":row['fence']},now=now)
                changed.append({"id":row["id"],"state":state,"reason":reason})
        return changed

    def reconcile_dead_job(self,job_id,output_dir,owner_alive):
        """Acknowledge an orphan sealed output, or fail an abandoned forward account.

        This never starts a process and never releases an experiment circuit.
        """
        output_dir=Path(output_dir);output_dir.mkdir(parents=True,exist_ok=True)
        with self.transaction() as db:
            row=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
            if row is None or row['state'] not in {'queued','failed','admission-held'} or not row['owner']:return False
            if owner_alive(json.loads(row['owner'])) is not False:return False
            path=output_dir/(job_id+'.json')
            if path.exists():
                receipt=read_json(path)
                if any(receipt.get(k)!=row[k] for k in ('input_hash','code_hash','config_hash')):raise ValueError('Orphan output identity mismatch')
                for artifact in receipt['artifacts']:
                    p=Path(artifact['path'])
                    if not p.is_relative_to(output_dir.resolve()) or p.stat().st_size!=artifact['bytes'] or file_hash(p)!=artifact['sha256']:
                        raise ValueError('Orphan output artifact mismatch')
            elif row['type']=='forward':
                receipt={'schema':'LANE_II_JOB_OUTPUT_V1','job_id':job_id,'input_hash':row['input_hash'],
                    'code_hash':row['code_hash'],'config_hash':row['config_hash'],'artifacts':[],
                    'execution_state':'failed','terminal_reason':'OWNER_LOST_ACCOUNT_RECONCILIATION_REQUIRED',
                    'result':{'execution':{'state':'failed','reason':'OWNER_LOST'},'fresh_account_restarted':False}}
                atomic_json(path,receipt)
            else:return False
            if receipt['execution_state'] not in TERMINAL:raise ValueError('Invalid orphan terminal state')
            manifest={'path':str(path.resolve()),'bytes':path.stat().st_size,'sha256':file_hash(path)}
            db.execute('UPDATE jobs SET state=?,ended=?,fence=fence+1,lease_until=NULL,artifact_manifest=?,terminal_reason=? WHERE id=?',
                       (receipt['execution_state'],time.time(),canonical(manifest).decode(),receipt['terminal_reason'],job_id))
            self._event(db,job_id,row['state'],receipt['execution_state'],row['fence']+1,'CONFIRMED_DEAD_OWNER_RECONCILIATION',
                        {'owner':json.loads(row['owner']),'manifest':manifest})
            return True

    def recovery_attempt(self, experiment_id, reason, *, eligible, release_condition):
        with self.transaction() as db:
            db.execute("INSERT OR IGNORE INTO experiments(id) VALUES(?)",(experiment_id,))
            row=db.execute("SELECT * FROM experiments WHERE id=?",(experiment_id,)).fetchone()
            if row['circuit']:
                db.execute("INSERT INTO experiment_events(experiment_id,at,event,detail) VALUES(?,?,?,?)",
                           (experiment_id,time.time(),'RECOVERY_REFUSED_CIRCUIT_OPEN',canonical({'reason':reason}).decode()))
                return False
            if not eligible or row["recovery_count"] >= 2:
                db.execute("UPDATE experiments SET circuit=?,release_condition=? WHERE id=?",(reason,release_condition,experiment_id))
                db.execute("INSERT INTO experiment_events(experiment_id,at,event,detail) VALUES(?,?,?,?)",
                           (experiment_id,time.time(),'CIRCUIT_OPEN',canonical({'reason':reason,'release_condition':release_condition}).decode()))
                return False
            db.execute("UPDATE experiments SET recovery_count=recovery_count+1 WHERE id=?",(experiment_id,))
            db.execute("INSERT INTO experiment_events(experiment_id,at,event,detail) VALUES(?,?,?,?)",
                       (experiment_id,time.time(),'RECOVERY_ATTEMPT',canonical({'count':row['recovery_count']+1,'reason':reason}).decode()))
            return True

    def release_circuit(self,experiment_id,evidence):
        if (evidence.get('satisfied') is not True or evidence.get('stable_seconds',0)<300
                or evidence.get('samples',0)<3 or len(evidence.get('evidence_sha256',''))!=64):
            raise ValueError('Stable, bound release evidence required')
        with self.transaction() as db:
            row=db.execute('SELECT circuit FROM experiments WHERE id=?',(experiment_id,)).fetchone()
            if row is None or row[0] is None:return False
            db.execute('UPDATE experiments SET circuit=NULL,release_condition=NULL WHERE id=?',(experiment_id,))
            db.execute('INSERT INTO experiment_events(experiment_id,at,event,detail) VALUES(?,?,?,?)',
                       (experiment_id,time.time(),'CIRCUIT_RELEASED',canonical(evidence).decode()))
            return True

    def fact(self,key,value=None,*,set_value=False):
        with self.transaction() as db:
            if set_value:
                db.execute("INSERT INTO facts(key,value,updated) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated=excluded.updated",
                           (key,canonical(value).decode(),time.time()))
            row=db.execute("SELECT value FROM facts WHERE key=?",(key,)).fetchone()
            return None if row is None else json.loads(row[0])

    def fact_owned(self,job_id,fence,key,value):
        with self.transaction() as db:
            self._owned(db,job_id,fence,time.time())
            db.execute('INSERT INTO facts(key,value,updated) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated=excluded.updated',
                       (key,canonical(value).decode(),time.time()))

    def circuit(self,experiment_id):
        with self.connection() as db:
            row=db.execute('SELECT circuit FROM experiments WHERE id=?',(experiment_id,)).fetchone()
            return None if row is None else row[0]

    def jobs(self,states=None):
        with self.connection() as db:
            rows=db.execute("SELECT * FROM jobs ORDER BY created").fetchall()
        return [self._decode(r) for r in rows if states is None or r["state"] in states]

    def get(self,job_id):
        with self.connection() as db:
            return self._decode(db.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone())

    def history(self,job_id):
        with self.connection() as db:
            return [dict(r) for r in db.execute("SELECT * FROM transitions WHERE job_id=? ORDER BY seq",(job_id,))]
