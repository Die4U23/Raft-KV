"""Read-only verification of the archived continuous-write restart test."""
from collections import Counter
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

from verify_ubuntu_abba import require
from verify_linux_fresh_validation import verify as verify_fresh_build

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
ARCHIVE = ROOT / 'evidence/write-restart-validation-AtdL0N.tar.gz'
SHA = 'e29cf7c3e475aca468a83edc7d85779d373a85f6251e15dc4bf1ee216ad4b4ff'
COMMIT = '07a612713116b58037567a49bfdb65b2401fc0ce'
PREFIX = 'run-px5ykvv0/'
STEPS = [
    'continuous writer acknowledges at least 64 unique keys before the fault',
    'leader is SIGKILLed after a batch is sent and before its replies are read',
    'writer continues through failover and survivors acknowledge at least 64 more keys',
    'all acknowledged keys survive leader restart; every attempted key agrees on three replicas',
    'all nodes restart from the same paired directories and retain all acknowledged keys']


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_report(report):
    require(report['status'] == 'PASS' and report['test'] == 'continuous_write_restart', 'Test result')
    require(report['steps'] == STEPS and not any(k in report for k in ('error', 'cleanup_error')), 'Test steps/errors')
    rows = report['attempts']
    require(len(rows) == 1832 and [r['sequence'] for r in rows] == list(range(1832)), 'Attempt sequence')
    require(len({r['key'] for r in rows}) == 1832, 'Duplicate attempted keys')
    for row in rows:
        sequence = row['sequence']
        require(row['key'] == 'restart-{:06d}'.format(sequence)
                and row['value'] == 'value-{:06d}-'.format(sequence) + 'x' * 128, 'Attempt key/value')
        require(row['node'] in range(3) and row['started_at'] <= row['finished_at'], 'Attempt node/timing')
        if 'sent_at' in row:
            require(row['started_at'] <= row['sent_at'] <= row['finished_at'], 'Send timing')
        if row['outcome'] == 'acknowledged':
            require('sent_at' in row and 'error' not in row and 'reply' not in row, 'Acknowledgement record')
        if row['outcome'] == 'rejected':
            require(row['reply'].startswith(('ERR MOVED ', 'ERR BUSY ')), 'Rejection semantics')
        if row['outcome'] == 'not_sent':
            require('sent_at' not in row and 'error' in row, 'Unsent record')
    counts = dict(Counter(r['outcome'] for r in rows))
    require(counts == report['writes'] == dict(acknowledged=1704, rejected=64, unknown=8, not_sent=56),
            'Attempt totals')
    fault = report['inflight_crash']
    require(report['initial_leader'] == fault['node'] == 0 and report['failover_leader'] == 2
            and report['final_leader'] == 1, 'Leader sequence')
    require(fault['sequences'] == list(range(576, 584)), 'In-flight batch')
    require(fault['sent_at'] <= fault['signal_started_at'] <= fault['process_exited_at']
            < report['old_leader_restarted_at'] < report['writer_stopped_at']
            < report['verification_barrier']['acknowledged_at'] < report['full_cluster_crash_at'], 'Fault ordering')
    require(sum(r['outcome'] == 'acknowledged' and r['finished_at'] < fault['signal_started_at'] for r in rows)
            == fault['acknowledged_before'] == 576, 'Acknowledgements before fault')
    for sequence in fault['sequences']:
        row = rows[sequence]
        require(row['node'] == 0 and row['sent_at'] == fault['sent_at'] and row['outcome'] == 'unknown'
                and row['finished_at'] >= fault['process_exited_at'], 'In-flight unknown outcome')
    phase_counts = {}
    for name, start, end in [
            ('before_crash', rows[0]['started_at'], fault['signal_started_at']),
            ('old_leader_down', fault['process_exited_at'], report['old_leader_restarted_at']),
            ('after_leader_restart', report['old_leader_restarted_at'], report['writer_stopped_at'])]:
        acknowledged = sum(r['outcome'] == 'acknowledged' and start <= r['finished_at'] <= end for r in rows)
        require(acknowledged >= 64 and end - start >= 2, 'Continuous-write phase: ' + name)
        phase_counts[name] = dict(acknowledged=acknowledged, window_seconds=round(end - start, 6))
    require(set(report['verification']) == {'after_leader_restart', 'after_full_cluster_restart'}, 'Verification phases')
    for nodes in report['verification'].values():
        require(set(nodes) == {'0', '1', '2'}, 'Verified replica inventory')
        require(all(info == dict(checked=1832, acknowledged_checked=1704, barrier_checked=True, unknown_present=[])
                    for info in nodes.values()), 'Per-replica recovery verification')
    require(set(report['last_info']) == {'0', '1', '2'}, 'Final snapshot inventory')
    for node, info in report['last_info'].items():
        require(info['node_id'] == int(node) and info['commit_index'] == info['last_applied'] == 1709
                and info['term'] == 7 and info['leader_id'] == 1, 'Final convergence')
        require(int(info['apply_lag']) == int(info['apply_inflight']) == int(info['pending_proposals']) == 0,
                'Final backlog')
    require(report['verification_barrier']['minimum_commit_index'] == 1707, 'Post-writer commit barrier')
    require(len(report['nodes']) == 3 and {n['id'] for n in report['nodes']} == {0, 1, 2}, 'Process inventory')
    for node in report['nodes']:
        starts = node['starts']
        require([s['exit_code'] for s in starts] == ([-9, -9, -15] if node['id'] == 0 else [-9, -15]),
                'Process lifecycle')
        require(all(s['command'] == starts[0]['command'] for s in starts), 'Restart command/directory changed')
        require(starts[0]['command'][0] == report['identity']['binary']['path'], 'Executed binary path')
    return phase_counts


def verify(archive_path=ARCHIVE):
    raw = archive_path.read_bytes()
    require(digest(raw) == SHA, 'Archive hash mismatch')
    expected = {'report.json', 'referenced-build-report.json', 'git-head.log', 'git-status.log',
                'node-0.log', 'node-1.log', 'node-2.log'}
    blobs, manifest = {}, []
    with tarfile.open(archive_path) as archive:
        members = archive.getmembers()
        require(len(members) == len({m.name for m in members}) == 8, 'Archive inventory')
        for member in members:
            path = PurePosixPath(member.name)
            require(not path.is_absolute() and '..' not in path.parts and '\\' not in member.name, 'Archive path')
            if member.isdir():
                require(member.name.rstrip('/') == PREFIX.rstrip('/'), 'Archive directory')
                continue
            require(member.name.startswith(PREFIX) and member.isfile() and member.size < 2 * 1024 * 1024,
                    'Archive file type/size')
            name = member.name[len(PREFIX):]
            require(name in expected, 'Unexpected file')
            blobs[name] = archive.extractfile(member).read()
            manifest.append(dict(path=member.name, bytes=len(blobs[name]), sha256=digest(blobs[name])))
    require(set(blobs) == expected, 'Missing evidence')
    require(digest(blobs['report.json']) == '1832f1fba7b408df0c4d92dd885d9b415f7b8ff627a501015b1da7a8660d7c4d',
            'Report differs from earlier supplied JSON')
    report = json.loads(blobs['report.json'])
    phases = verify_report(report)
    require(blobs['git-head.log'].decode().strip() == COMMIT, 'Git revision')
    identity = report['identity']
    require(identity['git-head-returncode'] == identity['git-status-returncode'] == 0, 'Git collection')
    require(digest(blobs['referenced-build-report.json']) == identity['build_report_sha256'], 'Build report identity')
    fresh = verify_fresh_build()
    with tarfile.open(ROOT / 'evidence/linux-fresh-validation-wjgjNs.tar.gz') as archive:
        previous = archive.extractfile('run-a3q01rgw/build-report.json').read()
    require(previous == blobs['referenced-build-report.json'], 'Referenced build differs from verified archive')
    build = json.loads(previous)
    require(identity['binary']['sha256'] == fresh['binary_sha256_recorded_at_test_time']
            and identity['binary']['path'] == build['binary']['path'], 'Binary identity')
    compiled = {name: sha for name, sha in build['source_manifest_before'].items()
                if name in ('CMakeLists.txt', 'third_party/muduo.zip') or
                (name.startswith(('src/', 'proto/')) and Path(name).suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))}
    require(compiled == identity['compiled_inputs'], 'Compiled source inventory')
    require(set(identity['test_source_hashes']) == {'cluster_write_restart.py', 'cluster_partition.py',
                                                 'cluster_smoke.py', 'raft_proxy.py'}, 'Test helper inventory')
    inputs = dict(compiled)
    inputs.update({'tests/' + name: sha for name, sha in identity['test_source_hashes'].items()})
    stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=REPO,
        input=''.join(COMMIT + ':' + name + '\n' for name in inputs).encode()))
    for name, sha in inputs.items():
        header = stream.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'Missing Git object')
        data = stream.read(int(header[2]))
        require(stream.read(1) == b'\n' and digest(data) == sha, 'Source fingerprint: ' + name)
    for node in range(3):
        require(blobs['node-{}.log'.format(node)].count(b'--- start ') == (3 if node == 0 else 2),
                'Node log start markers')
    return dict(status='PASS', scope='Archived bounded continuous-write and process-restart test; not power-loss proof',
        archive_sha256=SHA, archive_bytes=len(raw), file_count=len(blobs),
        file_manifest=sorted(manifest, key=lambda item: item['path']), test_commit=COMMIT,
        compiled_inputs=26, matched_test_helpers=4, binary_sha256=identity['binary']['sha256'], binary_in_archive=False,
        steps_passed=5, attempts=1832, outcomes=report['writes'], phases=phases,
        acknowledged_keys_checked_per_replica_per_recovery=1704, recovery_checks=2, replicas=3,
        unknown_keys_present_after_recovery=0, final_leader=1, final_term=7, final_commit_and_applied_index=1709,
        elapsed_seconds=report['elapsed_seconds'],
        limitation='Report contains client outcome ledger and verification summaries, not raw GET reply transcripts')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
