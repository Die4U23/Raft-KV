"""Read-only verification of the archived, bounded Linux TCP partition test."""
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
ARCHIVE = ROOT / 'evidence/partition-validation-vyHx1d.tar.gz'
SHA = '92dc46eab0c3268c844847b9c072ece3990d354240614dde25007b2363d9196e'
COMMIT = '9cfcfd6e836b18ea2b15ce96cd6fdf0ac3567680'
PREFIX = 'run-g5hsd49h/'
STEPS = [
    'three live nodes agree and replicate through directed TCP relays',
    'isolated old leader does not acknowledge or commit the probe; majority continues writing',
    'first heal preserves acknowledged data and converges on uncertain write outcome',
    'three isolated live nodes acknowledge no probes and advance no commit or apply index',
    'final heal restores writes and all replicas converge without restarting nodes']


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_report(report):
    """Check the safety observations independently of the report's PASS label."""
    require(report['status'] == 'PASS' and report['test'] == 'raft_tcp_partition', 'Partition result')
    require(report['steps'] == STEPS, 'Partition steps')
    require(not any(k in report for k in ('error', 'cleanup_error')), 'Reported execution error')
    require(report['initial_leader'] == 0 and report['majority_leader'] == 2
            and report['final_leader'] == 0, 'Leaders')
    snapshots = report['snapshots']
    phases = [('before_minority_partition', 1), ('minority_partition', 39),
              ('first_heal', 1), ('before_no_quorum', 1), ('no_quorum', 38), ('final_heal', 1)]
    require([s['phase'] for s in snapshots] == [phase for phase, count in phases for _ in range(count)],
            'Snapshot sequence')
    require(all(a['monotonic_seconds'] < b['monotonic_seconds'] for a, b in zip(snapshots, snapshots[1:])),
            'Snapshot timestamps')
    grouped = {phase: [s for s in snapshots if s['phase'] == phase] for phase, _ in phases}
    for snapshot in snapshots:
        require(set(snapshot['nodes']) == {'0', '1', '2'}, 'Live-node snapshot inventory')
        for node, info in snapshot['nodes'].items():
            require(info['node_id'] == int(node) and int(info['async_apply']) == 1, 'Node identity/mode')
    for snapshot in grouped['minority_partition']:
        info = snapshot['nodes']['0']
        require(info['commit_index'] == info['last_applied'] == 2, 'Minority advanced without quorum')
        for node in ('1', '2'):
            info = snapshot['nodes'][node]
            require(info['commit_index'] == info['last_applied'] == 4 and info['leader_id'] == 2,
                    'Majority write progress')
    for snapshot in grouped['no_quorum']:
        require(all(info['commit_index'] == info['last_applied'] == 5 for info in snapshot['nodes'].values()),
                'Isolated node advanced without quorum')
    for phase, index, leader, term in [('before_minority_partition', 2, 0, 1),
                                      ('first_heal', 5, 2, 2), ('before_no_quorum', 5, 2, 2),
                                      ('final_heal', 7, 0, 20)]:
        for node, info in grouped[phase][0]['nodes'].items():
            require(info['commit_index'] == info['last_applied'] == index
                    and info['leader_id'] == leader and info['term'] == term
                    and info['state'] == ('leader' if int(node) == leader else 'follower'), 'Convergence: ' + phase)
            require(int(info['pending_proposals']) == int(info['apply_lag']) == int(info['apply_inflight']) == 0,
                    'Backlog at convergence')
    for info in report['last_info'].values():
        require(info['commit_index'] == info['last_applied'] == 7 and info['term'] == 20 and info['leader_id'] == 0,
                'Final INFO')
    probes = report['probes']
    require(len(probes) == 4 and all(p['sent'] is True for p in probes), 'Probe inventory')
    expected_probes = [('minority-probe', 0, 'unknown', 'uncertain-one'),
                       ('no-quorum-0', 0, 'rejected', 'uncertain-0'),
                       ('no-quorum-1', 1, 'rejected', 'uncertain-1'),
                       ('no-quorum-2', 2, 'unknown', 'uncertain-2')]
    for probe, (key, node, outcome, value) in zip(probes, expected_probes):
        require((probe['key'], probe['node'], probe['outcome'], probe['value']) == (key, node, outcome, value),
                'Probe outcome')
        require(probe['reply'] == ('ERR MOVED 2' if outcome == 'rejected' else
                                  'reply deadline elapsed; not proof of rejection'), 'Probe response')
    require(report['uncertain_after_recovery'] == {
        'first_heal': {'minority-probe': 'absent'},
        'final_heal': {key: 'absent' for key, _, _, _ in expected_probes}}, 'Uncertain-write recovery outcome')
    partitions = report['partitions']
    names = ['minority_partition', 'first_heal', 'no_quorum', 'final_heal']
    groups = [[[0], [1, 2]], [[0, 1, 2]], [[0], [1], [2]], [[0, 1, 2]]]
    require([p['phase'] for p in partitions] == names and [p['groups'] for p in partitions] == groups,
            'Partition topology')
    events = report['proxy']['events']
    require([e['groups'] for e in events] == groups and
            [e['closed_connections'] for e in events] == [2, 0, 3, 0], 'Proxy transitions')
    require(all(a['monotonic_seconds'] < b['monotonic_seconds'] for a, b in zip(events, events[1:])),
            'Partition timestamps')
    phase_stats = {}
    edge_names = {'{}->{}'.format(a, b) for a in range(3) for b in range(3) if a != b}
    for i in (0, 2):
        partition = partitions[i]
        before, after = partition['proxy_at_cut'], partition['proxy_after_observation']
        require(before['errors'] == after['errors'] == [], 'Proxy errors')
        require(before['events'] == after['events'] == events[:i + 1], 'Proxy event history')
        require(set(before['edges']) == set(after['edges']) == edge_names, 'Proxy edge inventory')
        mapping = {node: j for j, group in enumerate(groups[i]) for node in group}
        for edge, initial in before['edges'].items():
            a, b = map(int, edge.split('->'))
            current = after['edges'][edge]
            require(all(current[k] >= initial[k] for k in initial), 'Monotonic proxy counters')
            if mapping[a] != mapping[b]:
                require(all(current[k] == initial[k] for k in ('request_bytes', 'reply_bytes')),
                        'Traffic crossed partition')
        duration = events[i + 1]['monotonic_seconds'] - events[i]['monotonic_seconds']
        require(duration >= report['observation_seconds'] == 4, 'Partition observation too short')
        samples = grouped[names[i]]
        require(events[i]['monotonic_seconds'] <= samples[0]['monotonic_seconds']
                < samples[-1]['monotonic_seconds'] < events[i + 1]['monotonic_seconds'], 'Sample window')
        phase_stats[names[i]] = dict(samples=len(samples), partition_seconds=round(duration, 6),
            sample_span_seconds=round(samples[-1]['monotonic_seconds'] - samples[0]['monotonic_seconds'], 6))
    require(report['proxy']['errors'] == [] and report['proxy']['active_connections'] == 0, 'Final proxy cleanup')
    require(report['minimum_applied_index'] == 7, 'Recovery commit barrier')
    return phase_stats


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
            require(member.name.startswith(PREFIX) and member.isfile() and member.size < 8 * 1024 * 1024,
                    'Archive member type/size')
            name = member.name[len(PREFIX):]
            require(name in expected, 'Unexpected file')
            blobs[name] = archive.extractfile(member).read()
            manifest.append(dict(path=member.name, bytes=len(blobs[name]), sha256=digest(blobs[name])))
    require(set(blobs) == expected, 'Missing evidence')
    report = json.loads(blobs['report.json'])
    phase_stats = verify_report(report)
    require(blobs['git-head.log'].decode().strip() == COMMIT, 'Test Git revision')
    identity = report['identity']
    require(identity['git-head-returncode'] == identity['git-status-returncode'] == 0, 'Git collection')
    require(digest(blobs['referenced-build-report.json']) == identity['build_report_sha256'], 'Build report fingerprint')
    fresh = verify_fresh_build()
    with tarfile.open(ROOT / 'evidence/linux-fresh-validation-wjgjNs.tar.gz') as archive:
        prior = archive.extractfile('run-a3q01rgw/build-report.json').read()
    require(prior == blobs['referenced-build-report.json'], 'Referenced build differs from verified fresh build')
    build = json.loads(prior)
    require(identity['binary']['sha256'] == fresh['binary_sha256_recorded_at_test_time']
            and identity['binary']['path'] == build['binary']['path'], 'Binary identity')
    recorded = build['source_manifest_before']
    compiled = {name: value for name, value in recorded.items()
                if name in ('CMakeLists.txt', 'third_party/muduo.zip') or
                (name.startswith(('src/', 'proto/')) and Path(name).suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))}
    require(compiled == identity['compiled_inputs'], 'Compiled inputs versus build')
    source_hashes = dict(compiled)
    require(set(identity['test_source_hashes']) == {'cluster_partition.py', 'cluster_smoke.py', 'raft_proxy.py'},
            'Test helper inventory')
    source_hashes.update({'tests/' + name: value for name, value in identity['test_source_hashes'].items()})
    output = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=REPO,
        input=''.join(COMMIT + ':' + path + '\n' for path in source_hashes).encode())
    stream = io.BytesIO(output)
    for path, expected_hash in source_hashes.items():
        header = stream.readline().split()
        require(len(header) == 3 and header[1] == b'blob', 'Missing Git object')
        data = stream.read(int(header[2]))
        require(stream.read(1) == b'\n' and digest(data) == expected_hash, 'Source fingerprint: ' + path)
    require({n['id'] for n in report['nodes']} == {0, 1, 2} and len(report['nodes']) == 3, 'Node inventory')
    direct_ports = {n[key] for n in report['nodes'] for key in ('client_port', 'raft_port')}
    relay_ports = []
    for node in report['nodes']:
        require(len(node['starts']) == 1 and node['starts'][0]['exit_code'] == -15, 'Unexpected node restart/exit')
        args = node['starts'][0]['command']
        require(args[0] == identity['binary']['path'], 'Node binary path')
        options = dict(arg[2:].split('=', 1) for arg in args[1:])
        require(int(options['node_id']) == node['id'] and int(options['raft_port']) == node['raft_port']
                and int(options['client_port']) == node['client_port'], 'Node launch identity')
        peers = [peer.split(':') for peer in options['peers'].split(',')]
        require(len(peers) == 3 and {int(peer[0]) for peer in peers} == {0, 1, 2}, 'Peer configuration')
        for peer, host, port in peers:
            require(host == '127.0.0.1', 'Non-loopback peer')
            if int(peer) == node['id']:
                require(int(port) == node['raft_port'], 'Self peer port')
            else:
                relay_ports.append(int(port))
    require(len(set(relay_ports)) == 6 and not set(relay_ports) & direct_ports, 'Directed relay ports')
    return dict(status='PASS', scope='Archived bounded TCP partition safety and recovery; not a local live rerun',
        archive_sha256=SHA, archive_bytes=len(raw), file_count=len(blobs),
        file_manifest=sorted(manifest, key=lambda m: m['path']), test_commit=COMMIT,
        compiled_input_count=len(compiled), matched_test_helpers=3, binary_sha256=identity['binary']['sha256'],
        binary_in_archive=False, steps_passed=5, snapshot_count=len(report['snapshots']), phases=phase_stats,
        probes=dict(sent=4, acknowledged=0, rejected=2, unknown=2),
        final_leader=0, final_term=20, final_commit_and_applied_index=7,
        uncertain_after_recovery=report['uncertain_after_recovery'], nodes_restarted=0,
        refused_proxy_connections=sum(e['refused'] for e in report['proxy']['edges'].values()),
        node_log_bytes=sum(len(blobs['node-{}.log'.format(i)]) for i in range(3)),
        elapsed_seconds=report['elapsed_seconds'], observation_limit='Two short symmetric TCP partitions; no CPU/soak assessment')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
