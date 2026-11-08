"""Run one source-bound legacy arithmetic script with process-local network denial."""
import runpy
import sys
from pathlib import Path


def deny(event,args):
    if event in {'socket.connect','socket.getaddrinfo','socket.gethostbyname','subprocess.Popen','os.system','os.posix_spawn'}:
        raise PermissionError('OFFLINE_REPLAY_NETWORK_OR_DESCENDANT_DENIED')


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--startup-gate':
        if sys.stdin.buffer.readline()!=b'LANE_II_OFFLINE_START\n':raise SystemExit('Owned Job release required')
        del sys.argv[1]
    if len(sys.argv)<3 or sys.argv[1]!='--':raise SystemExit('Exact replay script required')
    sys.addaudithook(deny)
    sys.argv=sys.argv[2:]
    sys.path.insert(0,str(Path(sys.argv[0]).resolve().parent))
    runpy.run_path(sys.argv[0],run_name='__main__')
