from copy import deepcopy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from scripts.lane_ii_continuous import capture_checkpoint as cp
from scripts.lane_ii_continuous import paper_risk as risk
from tests.test_lane_ii_continuous_paper_risk import entry, proposal

START = 1800000000


class Journal:
    def __init__(self, root):
        self.handle = (root / "events.jsonl").open("xb")
        self.seq, self.chain = 0, "0" * 64

    def emit(self, kind, data, epoch=START):
        self.seq += 1
        row = {"seq": self.seq, "previous_sha256": self.chain, "kind": kind, "data": data,
               "utc": cp.utc(epoch), "monotonic_ns": self.seq}
        self.chain = risk.digest(row)
        self.handle.write(risk.canonical({**row, "sha256": self.chain}) + b"\n")
        self.handle.flush()
        return self.seq


class Fixture:
    def __init__(self, root, *, start=START):
        self.run = root / "run"
        self.root = self.run / "capture"
        self.root.mkdir(parents=True)
        self.anchor = risk.initialize_campaign(root / "risk.sqlite", root / "ANCHOR.json", campaign_id="fixture", roster_sha256="r" * 64)
        self.ledger = risk.RiskLedger(self.anchor["path"], self.anchor["sha256"], roster_sha256="r" * 64)
        self.ledger.begin_segment("attempt-1")
        self.profiles = [SimpleNamespace(id=identity, initial_cash=3000., realized=0., entries=0, exits=0, settlements=0,
                                        fees=0., positions={}, windows={}) for identity in ("5m-P13", "5m-P2")]
        self.study = SimpleNamespace(root=self.root, paper_start=start, cutoff=start + 3600, manifest="s" * 64,
                                     log=Journal(self.root), ws_file=(self.root / "websocket-frames.jsonl").open("xb"),
                                     ledgers={identity: (self.root / (identity + "-ledger.jsonl")).open("xb") for identity in ("5m-P13", "5m-P2")},
                                     ws_count=0, state="RUNNING", reason=None, profiles=self.profiles,
                                     decision_capture=SimpleNamespace(active=None, ledger=self.ledger, initial_accounts={}))
        self.study.log.emit("START", {})
        self.operator = {"pid": 1234, "creation_epoch": START - 60, "boot_epoch": START - 500,
                         "executable": "fixture-python", "executable_sha256": "e" * 64,
                         "source_manifest_sha256": self.study.manifest, "owned_job_before_startup_gate": True}
        self.exited = {**self.operator, "retained_handle_exit_observed": True, "owned_job_closed": True,
                       "owned_children_at_parent_exit": [], "observed_exit_utc": cp.utc(START + 905), "exit_code": 9}
        (self.run / "operator.json").write_bytes(risk.canonical(self.operator))
        (self.run / "exit.json").write_bytes(risk.canonical(self.exited))
        (self.root / "preregistration.json").write_bytes(risk.canonical({"source_manifest_sha256": self.study.manifest,
                                                                        "cutoff_utc": cp.utc(self.study.cutoff)}))

    def close(self):
        for handle in (self.study.log.handle, self.study.ws_file, *self.study.ledgers.values()):
            if not handle.closed:
                handle.close()
        self.ledger.close()

    def recover(self):
        with patch("scripts.lane_ii_continuous.common.same_process", return_value=False):
            return cp.recover_flat_attempt(self.run, self.anchor, "attempt-1")


class CaptureCheckpointTests(unittest.TestCase):
    @contextmanager
    def fixture(self, **options):
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Fixture(Path(tmp), **options)
            try:
                yield fixture
            finally:
                fixture.close()

    def test_opt_in_and_original_grid_survive_five_minute_restart(self):
        with self.fixture(start=START + 1200) as fixture:
            disabled = cp.CheckpointWriter(fixture.study)
            self.assertIsNone(disabled.boundary(now=START + 1800))
            self.assertFalse((fixture.root / "checkpoints").exists())
            writer = cp.CheckpointWriter(fixture.study, interval=900, origin_epoch=START)
            self.assertIsNone(writer.boundary(now=START + 1799))
            binding = writer.boundary(now=START + 1800)
            packet, _ = cp.read(binding["path"])
            self.assertEqual(packet["scheduled_epoch"], START + 1800)
            self.assertEqual(packet["cadence_origin_epoch"], START)
            self.assertEqual(packet["checkpoint_sequence"], 1)

    def test_all_streams_fsync_before_atomic_marker_and_final_includes_end(self):
        with self.fixture() as fixture:
            writer = cp.CheckpointWriter(fixture.study, interval=900)
            expected = [fixture.study.log.handle.fileno(), fixture.study.ws_file.fileno(),
                        *(handle.fileno() for handle in fixture.study.ledgers.values())]
            real_fsync, seen = cp.os.fsync, []
            def sync(fd):
                seen.append(fd); real_fsync(fd)
            with patch.object(cp.os, "fsync", side_effect=sync):
                first = writer.boundary(now=START + 900)
            self.assertEqual(seen[:4], expected)
            self.assertGreater(len(seen), 4)
            packet, _ = cp.read(first["path"])
            self.assertEqual(packet["paper_risk_action_cursor"], {"sequence": 0, "event_id": None, "sha256": None})
            for name, value in packet["streams"].items():
                self.assertEqual(value["offset"], (fixture.root / name).stat().st_size)
            fixture.study.log.emit("END", {"state": "COMPLETE"}, START + 3600)
            fixture.study.state, fixture.study.reason = "COMPLETE", "DEADLINE"
            final = writer.finalize(producers_exited=True, now=START + 3600)
            end, _ = cp.read(final["path"])
            self.assertEqual(end["event_seq"], fixture.study.log.seq)
            self.assertEqual(end["chain_sha256"], fixture.study.log.chain)
            self.assertEqual(end["previous_checkpoint_sha256"], first["sha256"])
            self.assertEqual(end["checkpoint_sequence"], 2)
            self.assertEqual(end["kind"], "FINAL")

    def test_partial_context_and_failed_fsync_publish_no_usable_checkpoint(self):
        with self.fixture() as fixture:
            writer = cp.CheckpointWriter(fixture.study, interval=900)
            fixture.study.decision_capture.active = {"evaluation_id": 1}
            with self.assertRaisesRegex(ValueError, "PARTIAL_CONTEXT"):
                writer.boundary(now=START + 900)
            fixture.study.decision_capture.active = None
            writer.model_started(START + 900)
            with self.assertRaisesRegex(ValueError, "PARTIAL_CONTEXT"):
                writer.boundary(now=START + 900)
            writer.model_finished(START + 900)
            with patch.object(cp.os, "fsync", side_effect=OSError("fixture disk failure")):
                with self.assertRaisesRegex(OSError, "disk failure"):
                    writer.boundary(now=START + 900)
            self.assertEqual(list(writer.directory.glob("*.json")), [])
            writer.model_started(START + 3600)
            final = writer.finalize(producers_exited=True)
            hold, _ = cp.read(final["path"])
            self.assertFalse(hold["safe_loop_boundary"])
            self.assertFalse((writer.directory / "FINAL.json").exists())

    def test_failed_recovery_marker_replace_keeps_previous_durable_point(self):
        with self.fixture() as fixture:
            writer = cp.CheckpointWriter(fixture.study, interval=900, recovery_sync=True)
            first = writer.boundary(recovery=True, now=START + 2)
            original = Path(first["path"]).read_bytes()
            fixture.study.log.emit("WS_CONNECTED", {}, START + 3)
            with patch.object(cp.os, "replace", side_effect=OSError("fixture interrupted rename")):
                with self.assertRaisesRegex(OSError, "interrupted rename"):
                    writer.boundary(recovery=True, now=START + 4)
            self.assertEqual(Path(first["path"]).read_bytes(), original)

    def test_flat_crash_preserves_losses_fees_cash_and_excludes_observation_tail(self):
        with self.fixture() as fixture:
            payload = proposal()
            entry(fixture.ledger, payload, "loss")
            financial = {"profile": "5m-P13", "market": "m", "side": "Up", "exit_fee": .15,
                         "proceeds": 3.45, "net": 3.45 - 4.59, "fills": [{"quantity": 9}]}
            fixture.ledger.apply("loss-exit", "EXIT", {"reservation_id": payload["reservation_id"], "financial": financial})
            fixture.profiles[0].realized = financial["net"]
            fixture.profiles[0].fees, fixture.profiles[0].entries, fixture.profiles[0].exits = .24, 1, 1
            before = deepcopy(fixture.ledger.state)
            writer = cp.CheckpointWriter(fixture.study, interval=900)
            writer.boundary(now=START + 900)
            fixture.study.log.emit("WS_CONNECTED", {"observation_only": True}, START + 901)
            source_before = (fixture.root / "events.jsonl").read_bytes()
            result = fixture.recover()
            handoff, _ = cp.read(result["handoff"]["path"])
            self.assertEqual(result["schema"], "LANE_II_CRASH_HANDOFF_V1")
            self.assertFalse(handoff["normal_terminal_receipt"])
            self.assertFalse(handoff["qualified"])
            self.assertEqual(handoff["excluded_tail"]["streams"]["events.jsonl"]["rows"], 1)
            self.assertEqual((fixture.root / "events.jsonl").read_bytes(), source_before)
            self.assertFalse((fixture.root / "receipt.json").exists())
            self.assertEqual(result["state"]["gross_losses"], before["gross_losses"])
            self.assertEqual(result["state"]["paid_fees"], before["paid_fees"])
            self.assertAlmostEqual(result["state"]["carry_accounts"]["5m-P13"]["closing_cash"], 2998.86)
            self.assertEqual(fixture.recover()["handoff"], result["handoff"])
            verified = cp.validate_crash_handoff(result["handoff"]["path"], fixture.anchor["sha256"])
            self.assertEqual(verified["closed_state"], result["state"])
            ledger = risk.RiskLedger(fixture.anchor["path"], fixture.anchor["sha256"], roster_sha256="r" * 64)
            try:
                with self.assertRaisesRegex(ValueError, "FIXED_CUTOFF"):
                    ledger.begin_segment("attempt-2", result["predecessor_token"], paper_start_epoch=START + 1200, cutoff_epoch=START + 7200)
                ledger.begin_segment("attempt-2", result["predecessor_token"], paper_start_epoch=START + 1200, cutoff_epoch=START + 3600)
                self.assertEqual(ledger.state["gross_losses"], before["gross_losses"])
                self.assertEqual(ledger.state["paid_fees"], before["paid_fees"])
            finally:
                ledger.close()

    def test_monetary_torn_replaced_or_unknown_owner_tail_holds_without_handoff(self):
        for fault in ("financial", "torn", "replaced", "owner"):
            with self.subTest(fault=fault), self.fixture() as fixture:
                writer = cp.CheckpointWriter(fixture.study, interval=900)
                writer.boundary(now=START + 900)
                if fault == "financial":
                    fixture.study.log.emit("PAPER_ENTRY", {"profile": "5m-P13"}, START + 901)
                elif fault == "torn":
                    fixture.study.log.handle.write(b'{"kind":"PAPER_'); fixture.study.log.handle.flush()
                elif fault == "replaced":
                    target = fixture.root / "5m-P2-ledger.jsonl"
                    fixture.study.ledgers["5m-P2"].close()
                    target.rename(target.with_suffix(".retained")); target.write_bytes(b"")
                else:
                    fixture.exited["owned_children_at_parent_exit"] = [9876]
                    (fixture.run / "exit.json").write_bytes(risk.canonical(fixture.exited))
                with self.assertRaises(ValueError):
                    fixture.recover()
                self.assertFalse((fixture.run / "CRASH_HANDOFF.json").exists())
                self.assertEqual(fixture.ledger._read()["phase"], "ACTIVE")

    def test_risk_cursor_detects_new_rejected_action_even_if_state_is_identical(self):
        with self.fixture() as fixture:
            writer = cp.CheckpointWriter(fixture.study, interval=900)
            writer.boundary(now=START + 900)
            before = deepcopy(fixture.ledger.state)
            result = fixture.ledger.apply("rejected-after-checkpoint", "RESERVE_INTENT", proposal(quantity=200, fee_rate=1))
            self.assertFalse(result["accepted"])
            self.assertEqual(fixture.ledger.state, before)
            with self.assertRaisesRegex(ValueError, "UNACCOUNTED_RISK_TRANSACTIONS"):
                fixture.recover()
            self.assertFalse((fixture.run / "CRASH_HANDOFF.json").exists())

    def test_present_or_unqueryable_owner_and_missing_manager_exit_hold(self):
        with self.fixture() as fixture:
            cp.CheckpointWriter(fixture.study, interval=900).boundary(now=START + 900)
            for observed in (True, None):
                with self.subTest(owner=observed), patch("scripts.lane_ii_continuous.common.same_process", return_value=observed):
                    with self.assertRaisesRegex(ValueError, "OWNER_NOT_CLOSED"):
                        cp.recover_flat_attempt(fixture.run, fixture.anchor, "attempt-1")
            (fixture.run / "exit.json").unlink()
            with self.assertRaises(FileNotFoundError):
                fixture.recover()
            self.assertFalse((fixture.run / "CRASH_HANDOFF.json").exists())

    def test_unknown_or_pending_checkpoint_never_rebinds_budget(self):
        for fault in ("unknown", "pending"):
            with self.subTest(fault=fault), self.fixture() as fixture:
                if fault == "unknown":
                    payload = proposal()
                    fixture.ledger.apply("reserve", "RESERVE_INTENT", payload)
                    fixture.ledger.apply("prepare", "PREPARE_ENTRY", payload)
                else:
                    fixture.profiles[0].windows["m"] = {"pending": {"signal_wall": START}}
                cp.CheckpointWriter(fixture.study, interval=900).boundary(now=START + 900)
                with self.assertRaisesRegex(ValueError, "NOT_PROVEN_FLAT"):
                    fixture.recover()
                self.assertFalse((fixture.run / "CRASH_HANDOFF.json").exists())

    @unittest.skipUnless(os.name == "nt", "Windows OwnedJob fixture")
    def test_actual_owned_job_exit_proof_rebinds_only_fixture_ledger(self):
        from scripts.lane_ii_continuous.common import process_identity, same_process
        from scripts.lane_ii_continuous.worker import exited_owned_descendants
        from scripts.slim_paper_operator.owned_job import OwnedJob
        with self.fixture(start=int(time.time() // 900) * 900) as fixture:
            writer = cp.CheckpointWriter(fixture.study, interval=900, recovery_sync=True)
            writer.boundary(recovery=True)
            code = "import sys\ndef deny(event,args):\n if event.startswith('socket.') or event=='subprocess.Popen':raise AssertionError('FIXTURE_NETWORK_OR_CHILD')\nsys.addaudithook(deny)\nassert sys.stdin.buffer.readline()==b'OWNED_FIXTURE_RELEASE\\n'\nraise SystemExit(9)"
            environment = {key: value for key, value in os.environ.items() if key.upper() in {"SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "TEMP", "TMP", "COMSPEC", "PATHEXT"}}
            command = [sys.executable, "-I", "-S", "-B", "-c", code]
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=environment, creationflags=subprocess.CREATE_NO_WINDOW)
            owned = None
            try:
                owned = OwnedJob(process)
                identity = process_identity(process.pid)
                self.assertIn(process.pid, owned.pids())
                self.assertTrue(same_process(identity))
                operator = {**identity, "command": command, "source_manifest_sha256": fixture.study.manifest,
                            "owned_job_before_startup_gate": True}
                (fixture.run / "operator.json").write_bytes(risk.canonical(operator))
                process.stdin.write(b"OWNED_FIXTURE_RELEASE\n"); process.stdin.flush(); process.stdin.close()
                code = process.wait(timeout=5)
                self.assertEqual(code, 9, process.stderr.read().decode())
                descendants = exited_owned_descendants(owned, process)
                self.assertEqual(descendants, set())
                owned.close(); owned = None
                exited = {**identity, "source_manifest_sha256": fixture.study.manifest, "exit_code": code,
                          "observed_exit_utc": cp.utc(time.time()), "owned_children_at_parent_exit": sorted(descendants),
                          "owned_job_closed": True, "retained_handle_exit_observed": True}
                (fixture.run / "exit.json").write_bytes(risk.canonical(exited))
                self.assertFalse(same_process(operator))
                result = cp.recover_flat_attempt(fixture.run, fixture.anchor, "attempt-1")
                self.assertTrue(result["recovered"])
                self.assertFalse(result["qualified"])
            finally:
                if owned is not None:
                    owned.close()
                if process.poll() is None:
                    process.kill(); process.wait(timeout=5)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None and not stream.closed:
                        stream.close()

    @unittest.skipUnless(Path("N:/Beelzebub/runtime/lane-ii-prediction-markets/short-day-slim-3h-preparation-20261105/candidate-v2/study/SOURCE_MANIFEST.json").is_file(), "retained source fixture is local to Atlas")
    def test_actual_frozen_study_constructor_and_finalization_without_network(self):
        from scripts.lane_ii_continuous.capture import prepare_candidate
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate = prepare_candidate("N:/Beelzebub/runtime/lane-ii-prediction-markets/short-day-slim-3h-preparation-20261105/candidate-v2/study", root / "candidate")
            code = r'''
import json, os, pathlib, sys, time, types
root=pathlib.Path(sys.argv[1]); sys.path.insert(0,str(root/'candidate'))
def no_network_or_children(event,args):
    if event.startswith('socket.') or event in ('subprocess.Popen','os.system','os.posix_spawn','os.spawn'):
        raise AssertionError('OFFLINE_FIXTURE_BOUNDARY:'+event)
sys.addaudithook(no_network_or_children)
import study, continuous_paper_risk as risk
anchor=risk.initialize_campaign(root/'fixture-risk.sqlite',root/'FIXTURE_ANCHOR.json',campaign_id='offline-constructor-fixture',roster_sha256=study.REGISTRY['roster_sha256'])
os.environ.update(LANE_II_PAPER_RISK_MANIFEST=anchor['path'],LANE_II_PAPER_RISK_MANIFEST_SHA256=anchor['sha256'],LANE_II_PAPER_SEGMENT_ID='fixture',LANE_II_PAPER_PREDECESSOR_RECEIPT_SHA256='GENESIS',LANE_II_CAPTURE_CHECKPOINT_SECONDS='900',LANE_II_CAPTURE_RECOVERY_SYNC='1')
study.shutil.disk_usage=lambda _:types.SimpleNamespace(free=100*1024**3)
now=time.time()
capture=root/'fixture-run/capture'
instance=study.Study(capture,study.utc(now+.01),study.utc(now+.02))
# Real constructor, journal, profiles, risk, finalization and checkpoint hooks;
# receiver bodies are inert and the audit hook forbids every socket/child.
instance.http_receiver=lambda:None
instance.ws_receiver=lambda:None
while time.time()<instance.cutoff:time.sleep(.002)
assert instance.run()==0
receipt=json.loads((capture/'receipt.json').read_bytes())
final=json.loads((capture/'checkpoints/FINAL.json').read_bytes())
last=json.loads((capture/'events.jsonl').read_bytes().splitlines()[-1])
prereg=json.loads((capture/'preregistration.json').read_bytes())
assert last['kind']=='END' and final['event_seq']==last['seq']==receipt['events']
assert final['chain_sha256']==last['sha256']==receipt['chain_sha256']
assert final['paper_risk_state']==receipt['paper_risk_terminal_state']
assert final['account_carryover']==receipt['account_carryover']
assert prereg['paper_risk_initial_action_cursor']=={'sequence':0,'event_id':None,'sha256':None}
assert receipt['capture_final_checkpoint']['path']==str((capture/'checkpoints/FINAL.json').resolve())
assert (capture/'COLLECTION_CLOSED.json').is_file() and (capture.parent/'RISK_CLOSURE.json').is_file()
print(json.dumps({'final_kind':final['kind'],'events':final['event_seq'],'source':study.verify_sources()}))
'''
            environment = {key: value for key, value in os.environ.items() if key.upper() in {"SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "TEMP", "TMP", "COMSPEC", "PATHEXT", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE"}}
            result = subprocess.run([sys.executable, "-I", "-S", "-B", "-c", code, str(root)], env=environment,
                                    capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            observed = json.loads(result.stdout)
            self.assertEqual(observed["source"], candidate["source_manifest_sha256"])
            self.assertEqual(observed["final_kind"], "FINAL")
            self.assertEqual(observed["events"], 2)


if __name__ == "__main__":
    unittest.main()
