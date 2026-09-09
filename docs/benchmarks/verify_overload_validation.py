"""Read-only verification of the archived bounded overload/resource test."""
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import statistics
import subprocess
import tarfile

from verify_ubuntu_abba import require
from verify_linux_fresh_validation import verify as verify_fresh_build

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
ARCHIVE = ROOT / 'evidence/overload-validation-ulMugv.tar.gz'
SHA = 'd2b0b7e2ac12439dafb502c96edecfa1253544572c749b02a28ec20735c638c1'
COMMIT = 'b00e221e555f585fe02073a10e28ca3b571ec4b7'
PREFIX = 'run-ypucp0tw/'
STEPS = [
    'connection limit rejects excess clients and accepts new clients after release',
    'pending-write saturation returns BUSY and releases queues after quorum recovery',
    'fixed-key SET/GET workload completes the bounded soak without client errors',
    'queues drain, fixed keys converge and bounded RSS/FD growth guards pass']


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check_info(info, idle=False):
    for key, limit in [('connected_clients', 32), ('queued_writes', 1024), ('pending_proposals', 1024),
                       ('queued_write_bytes', 16 * 1024**2), ('pending_proposal_bytes', 16 * 1024**2),
                       ('client_input_bytes', 64 * 1024**2), ('client_output_reserved_bytes', 64 * 1024**2)]:
        require(0 <= int(info[key]) <= limit, 'Counter bound: ' + key)
    if idle:
        require(int(info['connected_clients']) == 1, 'Only monitoring connection remains')
        require(all(int(info[k]) == 0 for k in ('queued_writes', 'queued_write_bytes', 'pending_proposals',
            'pending_proposal_bytes', 'client_input_bytes', 'client_output_reserved_bytes', 'apply_lag',
            'apply_inflight')), 'Recovery backlog')


def verify_report(r):
    require(r['test'] == 'bounded_overload_soak' and r['status'] == 'PASS' and r['steps'] == STEPS,
            'Test identity/result')
    require(not any(k in r for k in ('error', 'cleanup_error')), 'Execution errors')
    require(r['configuration'] == dict(max_clients=32, pending_writes=24, value_bytes=819200, soak_seconds=60,
        soak_clients=2, fixed_keys=128, rss_growth_guard_bytes=32 * 1024**2,
        peak_rss_guard_bytes=512 * 1024**2, fd_growth_guard=8), 'Configuration')
    require(r['admission'] == dict(held_clients=31, monitor_clients=1, excess_closed=5,
                                   overload_rejections_delta=5), 'Admission evidence')
    pending = r['pending_overload']
    require([row['key'] for row in pending['outcomes']] == ['pending-overload-' + str(i) for i in range(24)],
            'Unique overload keys')
    require(Counter(row['outcome'] for row in pending['outcomes']) == dict(busy=4, unknown=20)
            and pending['busy'] == pending['overload_rejections_delta'] == 4 and pending['unknown'] == 20,
            'Overload outcomes')
    require(all(row.get('reply') == 'ERR BUSY proposal capacity exhausted' for row in pending['outcomes']
                if row['outcome'] == 'busy'), 'BUSY semantics')
    require(r['load'] == dict(confirmed_set_get_pairs=[1276, 1276], client_errors=[]), 'Load results')
    samples = r['samples']
    phases = Counter(s['phase'] for s in samples)
    require(phases == dict(baseline=1, admission_full=1, pending_overload=15, warmup=10, soak=60, recovery=10),
            'Sample inventory')
    require(all(a['at'] < b['at'] for a, b in zip(samples, samples[1:])), 'Sample ordering')
    for s in samples:
        require(set(s['info']) == set(s['process']) == set(s['log_bytes']) == {'0', '1', '2'}, 'Node inventory')
        for node, info in s['info'].items():
            require(info['node_id'] == int(node), 'INFO node identity')
            check_info(info, s['phase'] == 'recovery')
    for phase, duration in [('warmup', 10), ('soak', 60)]:
        w = r['load_windows'][phase]
        require(w['requested_seconds'] == duration and w['ended_at'] - w['started_at'] >= duration,
                'Load window duration')
        require(all(w['started_at'] <= s['at'] <= w['ended_at'] for s in samples if s['phase'] == phase),
                'Samples outside load window')
    steady = [s for s in samples if s['phase'] == 'soak']
    resources = {}
    for node in ('0', '1', '2'):
        def growth(key):
            return statistics.median(s['process'][node][key] for s in steady[-10:]) - statistics.median(
                s['process'][node][key] for s in steady[:10])
        rss, fd = growth('rss_bytes'), growth('fd_count')
        peak = max(s['process'][node]['rss_bytes'] for s in samples)
        cpu = steady[-1]['process'][node]['cpu_seconds'] - steady[0]['process'][node]['cpu_seconds']
        require(rss <= 32 * 1024**2 and fd <= 8 and peak <= 512 * 1024**2 and cpu >= 0, 'Resource guards')
        resources[node] = dict(rss_growth_bytes=rss, fd_growth=fd, peak_rss_bytes=peak,
            soak_cpu_percent_of_one_core=100 * cpu / (steady[-1]['at'] - steady[0]['at']))
        info = r['last_info'][node]
        check_info(info, True)
        require(info['commit_index'] == info['last_applied'] == 2555 and info['term'] == 4
                and info['leader_id'] == 2 and info['state'] == ('leader' if node == '2' else 'follower'),
                'Final convergence')
    require(resources == r['resources'], 'Recalculated resource summary')
    require(r['initial_leader'] == 1 and r['final_leader'] == 2, 'Leader transition')
    before, after, final = [r[k] for k in ('proxy_before_partition', 'proxy_after_partition', 'proxy_final')]
    require(all(not p['errors'] for p in (before, after, final)), 'Proxy errors')
    require(before['events'][0]['groups'] == [[1], [0, 2]] and final['events'][-1]['groups'] == [[0, 1, 2]],
            'Partition and heal')
    for edge in ('0->1', '1->0', '1->2', '2->1'):
        require(all(before['edges'][edge][k] == after['edges'][edge][k] for k in ('request_bytes', 'reply_bytes')),
                'Cross-partition bytes changed')
    require(sum(e['refused'] for e in after['edges'].values()) == r['reconnect_refused_connections'] == 7897,
            'Reconnect observation')
    require(r['warnings'] == ['High reconnect count during TCP-close partition: 7897; retry/log cost remains an open issue'],
            'Known issue must remain visible')
    require(len(r['nodes']) == 3 and {n['id'] for n in r['nodes']} == {0, 1, 2}, 'Process inventory')
    for node in r['nodes']:
        require(len(node['starts']) == 1 and node['starts'][0]['exit_code'] == -15, 'Process lifecycle')
        cmd = node['starts'][0]['command']
        require(cmd[0] == r['identity']['binary']['path'] and '--max_clients=32' in cmd, 'Executed configuration')
    return resources


def verify(archive_path=ARCHIVE):
    raw = archive_path.read_bytes()
    require(digest(raw) == SHA, 'Archive hash')
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
            require(member.isfile() and member.name.startswith(PREFIX) and member.size < 2 * 1024**2, 'File type/size')
            name = member.name[len(PREFIX):]
            require(name in expected, 'Unexpected file')
            blobs[name] = archive.extractfile(member).read()
            manifest.append(dict(path=member.name, bytes=len(blobs[name]), sha256=digest(blobs[name])))
    require(set(blobs) == expected, 'Missing evidence')
    require(digest(blobs['report.json']) == 'd57bdccf638fa6e532ba0c96f79218d8be53c7c995d60ded34ae33913f998b7a',
            'Report differs from earlier supplied JSON')
    r = json.loads(blobs['report.json'])
    resources = verify_report(r)
    require(blobs['git-head.log'].decode().strip() == COMMIT, 'Git revision')
    require(blobs['git-status.log'].decode().strip() == '?? CMakeLists.txt.before-boost190-20260905-215703', 'Git status')
    identity = r['identity']
    require(identity['git-head-returncode'] == identity['git-status-returncode'] == 0, 'Git collection')
    fresh = verify_fresh_build()
    with tarfile.open(ROOT / 'evidence/linux-fresh-validation-wjgjNs.tar.gz') as archive:
        previous = archive.extractfile('run-a3q01rgw/build-report.json').read()
    require(previous == blobs['referenced-build-report.json'] and digest(previous) == identity['build_report_sha256'],
            'Verified build report identity')
    build = json.loads(previous)
    require(identity['binary']['sha256'] == fresh['binary_sha256_recorded_at_test_time']
            and identity['binary']['path'] == build['binary']['path'], 'Binary identity')
    compiled = {name: sha for name, sha in build['source_manifest_before'].items()
                if name in ('CMakeLists.txt', 'third_party/muduo.zip') or
                (name.startswith(('src/', 'proto/')) and Path(name).suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))}
    require(compiled == identity['compiled_inputs'] and len(compiled) == 26, 'Compiled source inventory')
    require(set(identity['test_source_hashes']) == {'cluster_overload.py', 'cluster_partition.py', 'cluster_smoke.py',
                                                  'raft_proxy.py'}, 'Test helper inventory')
    inputs = dict(compiled)
    inputs.update({'tests/' + name: sha for name, sha in identity['test_source_hashes'].items()})
    for name, sha in inputs.items():
        require(digest(subprocess.check_output(['git', 'show', COMMIT + ':' + name], cwd=REPO)) == sha,
                'Source fingerprint: ' + name)
    for node in range(3):
        log = blobs['node-{}.log'.format(node)]
        require(log.count(b'--- start ') == 1, 'Node log start markers')
        require(len(log) >= r['samples'][-1]['log_bytes'][str(node)], 'Log ends before final sample')
    return dict(status='PASS', scope='Archived bounded overload and 60-second soak; not long-term or capacity proof',
        archive_sha256=SHA, archive_bytes=len(raw), file_count=7, file_manifest=sorted(manifest, key=lambda m: m['path']),
        test_commit=COMMIT, compiled_inputs=26, matched_test_helpers=4,
        binary_sha256=identity['binary']['sha256'], binary_in_archive=False, steps_passed=4,
        sample_phases=dict(Counter(s['phase'] for s in r['samples'])), admission=r['admission'],
        pending_outcomes=dict(busy=4, unknown=20), confirmed_set_get_pairs_including_warmup=2552,
        client_errors=0, resources=resources, reconnect_refused_connections=7897, warnings=r['warnings'],
        final_leader=2, final_term=4, final_commit_and_applied_index=2555, elapsed_seconds=r['elapsed_seconds'],
        limitation='Key checks are script assertions, not raw reply transcripts; no binary or database bodies')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
