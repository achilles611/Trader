"""Scheduled health receipts and a local, read-only qualification dashboard."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time

from .common import application_lock, atomic_json, read_json, same_process, utc
from .qualification import _epoch

CADENCE = 900
REFRESH_OFFSET = 60


def due_slot(plan, epoch, offset=0):
    start, cutoff = plan['start_epoch'], plan['cutoff_epoch']
    due = start + int((epoch - offset - start) // CADENCE) * CADENCE
    return min(due, cutoff) if due >= start + CADENCE else None


def health(root, observer, *, epoch=None):
    root = Path(root)
    status = read_json(root / 'STATUS.json')
    epoch = time.time() if epoch is None else epoch
    plan = status.get('plan')
    age = max(0.0, epoch - _epoch(status['as_of_utc']))
    healthy = same_process(status['owner']) is True and age <= 30
    collector = status.get('collector')
    collector_alive = same_process(collector['owner']) if collector and collector.get('owner') else None
    snapshot = collector.get('snapshot_utc') if collector else None
    snapshot_age = max(0.0, epoch - _epoch(snapshot)) if snapshot else None
    slot = due_slot(plan, epoch) if plan else None
    row = {'schema': 'LANE_II_LONG_HEALTH_V1', 'observer': observer, 'observed_utc': utc(epoch),
           'scheduled_epoch': slot, 'phase': plan['phase'] if plan else None,
           'supervisor_live_and_fresh': healthy, 'heartbeat_age_seconds': age, 'collector_alive': collector_alive,
           'collector_snapshot_age_seconds': snapshot_age,
           'collector_snapshot_fresh': snapshot_age is not None and snapshot_age <= 35,
           'state': status['state'], 'hold': status.get('hold'), 'risk': status.get('risk'),
           'profiles': status.get('profiles', []), 'ticks': status.get('ticks'),
           'replay': status.get('replay'), 'crash_count': status.get('crash_count', 0),
           'collector_crash_count': status.get('collector_crash_count', 0),
           'resource': status.get('resource')}
    if slot is not None and epoch < slot + CADENCE and observer in {'host', 'codex'}:
        folder = root / plan['phase'] / 'health' / observer
        path = folder / f'{slot}.json'
        with application_lock(folder / 'write.lock') as acquired:
            if acquired and not path.exists():
                atomic_json(path, row)
    return row


def dashboard(root, *, epoch=None, immediate=False):
    root = Path(root)
    epoch = time.time() if epoch is None else epoch
    status = read_json(root / 'STATUS.json')
    plan = status.get('plan')
    if plan is None:
        return None
    slot = due_slot(plan, epoch, REFRESH_OFFSET)
    target = root / 'public' / 'dashboard.json'
    with application_lock(root / 'dashboard.lock') as acquired:
        if not acquired:
            return None
        prior = read_json(target) if target.is_file() else {}
        if (not immediate and prior.get('plan') == plan
                and prior.get('refresh_slot_epoch') == slot and prior.get('state') == status['state']):
            return prior
        history = []
        for path in sorted((root / plan['phase'] / 'health' / 'host').glob('*.json')):
            check = read_json(path)
            normal_end = check['scheduled_epoch'] == plan['cutoff_epoch'] and check['state'] in {'CLOSING', 'QUALIFIED'}
            history.append({'epoch': check['scheduled_epoch'], 'observed_utc': check['observed_utc'],
                            'profiles': check['profiles'], 'healthy': check['supervisor_live_and_fresh']
                            and check['collector_snapshot_fresh'] and (check['collector_alive'] is True or normal_end)})
        value = {'schema': 'LANE_II_LONG_DASHBOARD_V1', 'plan': plan, 'generated_utc': utc(epoch),
                 'as_of_utc': status['as_of_utc'], 'state': status['state'], 'hold': status.get('hold'),
                 'refresh_slot_epoch': slot, 'refresh_seconds': CADENCE, 'refresh_offset_seconds': REFRESH_OFFSET,
                 'profiles': status.get('profiles', []), 'risk': status.get('risk'), 'replay': status.get('replay'),
                 'history': history[-96:], 'crashes': status.get('crashes', []), 'research': status.get('research'),
                 'collector_crash_count': status.get('collector_crash_count', 0),
                 'qualification': status.get('qualification')}
        atomic_json(root / 'public' / (plan['phase'] + '.json'), value)
        atomic_json(target, value)
        return value


def serve(root, port):
    root = Path(root)
    page = Path(__file__).with_name('long_dashboard.html')

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/':
                path, mime = page, 'text/html; charset=utf-8'
            elif self.path == '/status.json':
                path, mime = root / 'public' / 'dashboard.json', 'application/json'
            elif self.path in {'/eight-hour.json', '/twenty-four-hour.json'}:
                path, mime = root / 'public' / self.path[1:], 'application/json'
            else:
                self.send_error(404)
                return
            try:
                data = path.read_bytes()
            except OSError:
                self.send_error(503)
                return
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    threading.Thread(target=server.serve_forever, name='lane-ii-dashboard', daemon=True).start()
    return server
