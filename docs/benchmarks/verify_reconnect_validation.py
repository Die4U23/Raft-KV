"""Read-only validation of the archived reconnect optimization and its baseline."""
from collections import Counter
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

from verify_ubuntu_abba import require
from verify_partition_validation import verify as verify_baseline, STEPS

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
ARCHIVE = ROOT / 'evidence/reconnect-validation-uhdGVa.tar.gz'
SHA = '880ca41e3a77ea98e176e4bc27e1a2ae342ad6399d98319247bcc3ea67a04293'
COMMIT = 'ef17ef73f844a38482818bbf223103f69f13a18d'
BUILD = 'reports/run-c4arz1uk/'
RUN = 'reconnect-reports/run-gno7_f3s/'
SMOKE = BUILD + 'cluster/run-9q8wyky_/'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_report(r):
    require(r['status'] == 'PASS' and r['test'] == 'raft_tcp_partition' and r['steps'] == STEPS, 'Partition result')
    require(not any(k in r for k in ('error', 'cleanup_error')), 'Execution error')
    require((r['initial_leader'], r['majority_leader'], r['final_leader']) == (1, 0, 2), 'Leader transitions')
    snapshots = r['snapshots']
    phases = [('before_minority_partition', 1), ('minority_partition', 39), ('first_heal', 1),
              ('before_no_quorum', 1), ('no_quorum', 39), ('final_heal', 1)]
    require([s['phase'] for s in snapshots] == [p for p, count in phases for _ in range(count)], 'Snapshot sequence')
    require(all(a['monotonic_seconds'] < b['monotonic_seconds'] for a, b in zip(snapshots, snapshots[1:])), 'Time order')
    grouped = {p: [s for s in snapshots if s['phase'] == p] for p, _ in phases}
    for s in snapshots:
        require(set(s['nodes']) == {'0', '1', '2'}, 'Snapshot nodes')
        require(all(i['node_id'] == int(n) for n, i in s['nodes'].items()), 'Node identity')
    for phase, base, nodes in [('minority_partition', 'before_minority_partition', ['1']),
                                ('no_quorum', 'before_no_quorum', ['0', '1', '2'])]:
        for s in grouped[phase]:
            for n in nodes:
                require(all(s['nodes'][n][k] == grouped[base][0]['nodes'][n][k]
                            for k in ('commit_index', 'last_applied')), 'No-quorum index advanced')
    for s in grouped['minority_partition']:
        for n in ('0', '2'):
            require(s['nodes'][n]['commit_index'] > grouped['before_minority_partition'][0]['nodes'][n]['commit_index'],
                    'Majority made no progress')
    for phase in ('first_heal', 'final_heal'):
        infos = grouped[phase][0]['nodes']
        require(len({(i['leader_id'], i['term'], i['commit_index'], i['last_applied']) for i in infos.values()}) == 1,
                'Recovery convergence')
        require(all(i['commit_index'] == i['last_applied'] and
                    all(int(i[k]) == 0 for k in ('pending_proposals', 'apply_lag', 'apply_inflight'))
                    for i in infos.values()), 'Recovery backlog')
    for i in r['last_info'].values():
        require(i['commit_index'] == i['last_applied'] == 9 and i['leader_id'] == 2 and i['term'] == 30, 'Final state')
    require(len(r['probes']) == 4 and all(p['sent'] for p in r['probes']), 'Probe count')
    require([(p['node'], p['outcome']) for p in r['probes']] ==
            [(1, 'unknown'), (0, 'rejected'), (1, 'rejected'), (2, 'unknown')], 'Probe results')
    require(all(p['reply'] == ('ERR MOVED 2' if p['outcome'] == 'rejected' else
                'reply deadline elapsed; not proof of rejection') for p in r['probes']), 'Probe semantics')
    require(r['uncertain_after_recovery'] == dict(first_heal={'minority-probe': 'absent'},
        final_heal={'minority-probe': 'absent', 'no-quorum-0': 'absent', 'no-quorum-1': 'absent',
                    'no-quorum-2': 'applied'}), 'Unknown outcome after recovery')
    groups = [[[1], [0, 2]], [[0, 1, 2]], [[0], [1], [2]], [[0, 1, 2]]]
    events = r['proxy']['events']
    require([e['groups'] for e in events] == groups and [p['groups'] for p in r['partitions']] == groups, 'Topology')
    require([e['closed_connections'] for e in events] == [2, 0, 3, 0] and not r['proxy']['errors'], 'Proxy transitions')
    windows = {}
    for index in (0, 2):
        p = r['partitions'][index]
        before, after = p['proxy_at_cut'], p['proxy_after_observation']
        require(not before['errors'] and not after['errors'], 'Partition proxy errors')
        for edge in before['edges']:
            src, dst = map(int, edge.split('->'))
            if not any(src in g and dst in g for g in p['groups']):
                require(all(before['edges'][edge][k] == after['edges'][edge][k]
                            for k in ('request_bytes', 'reply_bytes')), 'Blocked edge forwarded data')
        duration = events[index + 1]['monotonic_seconds'] - events[index]['monotonic_seconds']
        require(duration >= r['observation_seconds'] == 4, 'Observation window')
        windows[p['phase']] = duration
    require(sum(e['refused'] for e in r['proxy']['edges'].values()) == r['reconnect_check']['refused_connections'] == 11
            and r['reconnect_check']['limit'] == 100, 'Reconnect count and threshold')
    require(len(r['nodes']) == 3 and {n['id'] for n in r['nodes']} == {0, 1, 2}, 'Process inventory')
    for n in r['nodes']:
        require(len(n['starts']) == 1 and n['starts'][0]['exit_code'] == -15, 'No restart during partition')
        require(n['starts'][0]['command'][0] == r['identity']['binary']['path'], 'Executed binary')
    return windows


def verify(archive_path=ARCHIVE):
    raw = archive_path.read_bytes()
    require(digest(raw) == SHA, 'Archive hash')
    blobs = {}
    with tarfile.open(archive_path) as t:
        members = t.getmembers()
        require(len(members) == len({m.name for m in members}) == 36, 'Archive member inventory')
        for m in members:
            p = PurePosixPath(m.name)
            require(not p.is_absolute() and '..' not in p.parts and '\\' not in m.name, 'Archive path')
            require(m.name.startswith((BUILD.rstrip('/'), RUN.rstrip('/'))), 'Archive prefix')
            require(m.isdir() or (m.isfile() and m.size < 2 * 1024**2), 'Archive type/size')
            if m.isfile(): blobs[m.name] = t.extractfile(m).read()
    require(len(blobs) == 32, 'File count')
    build_raw, run_raw = blobs[BUILD + 'build-report.json'], blobs[RUN + 'report.json']
    require(digest(build_raw) == '8f237c0e630bef84f08219603cffbfcae673584852d75b3f37490e729d26b816'
            and digest(run_raw) == '5089f2b518a1473f27e480bfe6f2235910b9b6a310bdcb141429e23c739bbb61', 'Prior JSON identity')
    b, r, smoke = json.loads(build_raw), json.loads(run_raw), json.loads(blobs[SMOKE + 'report.json'])
    windows = verify_report(r)
    require(all(b[k] == 'PASS' for k in ('status', 'linux_server_build', 'ctest', 'smoke')), 'Build result')
    require(all(c['returncode'] == 0 and BUILD + c['log'] in blobs for c in b['commands']), 'Build steps/logs')
    require(not blobs[BUILD + 'tracked-code-changes.log'], 'Tracked changes during build')
    for prefix in (BUILD, RUN):
        require(blobs[prefix + 'git-head.log'].decode().strip() == COMMIT, 'Git commit')
        require(blobs[prefix + 'git-status.log'].decode().strip() ==
                '?? CMakeLists.txt.before-boost190-20260905-215703', 'Git status')
    require(blobs[RUN + 'referenced-build-report.json'] == build_raw, 'Referenced build bytes')
    identity = r['identity']
    require(identity['build_report_sha256'] == digest(build_raw) and
            identity['binary']['sha256'] == b['binary']['sha256'] == b['smoke_binary_sha256'], 'Binary identity')
    require(identity['binary']['path'] == b['binary']['path'], 'Binary path')
    manifest = b['source_manifest_before']
    require(digest(json.dumps(manifest, sort_keys=True).encode()) == b['source_manifest_sha256'], 'Manifest digest')
    expected = {p: h for p, h in manifest.items() if p != 'src/server/main.cpp.bak'}
    require(len(expected) == 58 and len(manifest) == 59, 'Source inventory')
    stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=REPO,
        input=''.join(COMMIT + ':' + p + '\n' for p in expected).encode()))
    for p, h in expected.items():
        header = stream.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'Git blob')
        require(digest(stream.read(int(header[2]))) == h and stream.read(1) == b'\n', 'Source hash: ' + p)
    compiled = {p: h for p, h in expected.items() if p in ('CMakeLists.txt', 'third_party/muduo.zip') or
                (p.startswith(('src/', 'proto/')) and Path(p).suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))}
    require(compiled == identity['compiled_inputs'] and len(compiled) == 27, 'Compiled inventory')
    require(identity['test_source_hashes'] == {p: expected['tests/' + p] for p in
            ('cluster_partition.py', 'cluster_smoke.py', 'raft_proxy.py')}, 'Partition helper identity')
    tests = {'peer_manager_transport_tests', 'protocol_tests', 'core_tests', 'storage_batch_tests',
             'async_executor_tests', 'batch_flush_tests', 'peer_retry_tests'}
    log = blobs[BUILD + 'ctest.log'].decode()
    require(set(re.findall(r'Test #\d+: (\w+)\s+\.+\s+Passed', log)) == tests
            and '100% tests passed, 0 tests failed out of 7' in log, 'CTest 7/7')
    require(smoke['status'] == 'PASS' and len(smoke['steps']) == 10, 'Smoke result')
    require(all(i['commit_index'] == i['last_applied'] == 42 and i['leader_id'] == 2 and i['term'] == 3
                for i in smoke['last_info'].values()), 'Smoke convergence')
    for n in range(3):
        require(blobs[RUN + 'node-{}.log'.format(n)].count(b'--- start ') == 1, 'Partition log lifecycle')
        require(blobs[SMOKE + 'node-{}.log'.format(n)].count(b'--- start ') == (2 if n == 0 else 1), 'Smoke log lifecycle')
    verify_baseline()
    with tarfile.open(ROOT / 'evidence/partition-validation-vyHx1d.tar.gz') as t:
        old = json.load(t.extractfile('run-g5hsd49h/report.json'))
        old_logs = sum(t.getmember('run-g5hsd49h/node-{}.log'.format(n)).size for n in range(3))
    with tarfile.open(ROOT / 'evidence/linux-fresh-validation-wjgjNs.tar.gz') as t:
        old_build = json.load(t.extractfile('run-a3q01rgw/build-report.json'))
    require(b['muduo']['source_manifest'] == old_build['muduo']['source_manifest'], 'Prepared Muduo unchanged')
    old_count = sum(e['refused'] for e in old['proxy']['edges'].values())
    new_logs = sum(len(blobs[RUN + 'node-{}.log'.format(n)]) for n in range(3))
    require(old_count == 24731 and old_logs == 5498325 and new_logs == 146110, 'Baseline/log totals')
    return dict(status='PASS', archive_sha256=SHA, archive_bytes=len(raw), file_count=len(blobs), test_commit=COMMIT,
        file_manifest=[dict(path=p, bytes=len(v), sha256=digest(v)) for p, v in sorted(blobs.items())],
        matched_build_sources=58, compiled_inputs=27, matched_partition_helpers=3,
        binary_sha256=b['binary']['sha256'], ctest_passed=7, smoke_steps=10, partition_steps=5,
        observation_windows=windows, reconnect_before=old_count, reconnect_after=11,
        reconnect_reduction_percent=100 * (1 - 11 / old_count), full_log_bytes_before=old_logs,
        full_log_bytes_after=new_logs, full_log_reduction_percent=100 * (1 - new_logs / old_logs),
        pre_cleanup_log_bytes=sum(r['reconnect_check']['node_log_bytes'].values()),
        final_term=30, final_commit_and_applied_index=9, uncertain_after_recovery=r['uncertain_after_recovery'],
        limitation='One before/after VM comparison with different leaders; not a CPU, QPS or long-term claim')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
