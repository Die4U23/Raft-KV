#!/usr/bin/env python3
"""Real Linux quorum/partition test through owned TCP relays; no sudo or firewall changes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time
import traceback

from cluster_smoke import Cluster, RespError, encode_command, expect
from raft_proxy import RaftProxyMesh


ROOT = Path(__file__).resolve().parents[1]


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_build(path):
    report = json.loads(path.read_text(encoding='utf-8'))
    expect(report['status'], 'PASS', 'referenced build status')
    expect(report['linux_server_build'], 'PASS', 'referenced Linux build')
    binary = Path(report['binary']['path']).resolve()
    if not binary.is_file() or not os.access(str(binary), os.X_OK):
        raise ValueError('Referenced binary is missing or not executable: {}'.format(binary))
    expect(file_hash(binary), report['binary']['sha256'], 'binary versus build report')
    # New fault-test scripts may differ; the compiled inputs must still match the build.
    files = [ROOT / 'CMakeLists.txt', ROOT / 'third_party/muduo.zip']
    for directory in ('src', 'proto'):
        files.extend(p for p in (ROOT / directory).rglob('*')
                     if p.is_file() and p.suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))
    inputs = {p.relative_to(ROOT).as_posix(): file_hash(p) for p in files}
    recorded = report['source_manifest_before']
    recorded_inputs = {name: value for name, value in recorded.items()
                       if name in ('CMakeLists.txt', 'third_party/muduo.zip') or
                       (name.startswith(('src/', 'proto/')) and
                        Path(name).suffix in ('.cc', '.cpp', '.h', '.hpp', '.proto'))}
    expect(inputs, recorded_inputs, 'compiled source inputs versus build report')
    return binary, inputs


def classify_reply(reply):
    if isinstance(reply, RespError):
        return dict(outcome='rejected', reply=reply.message)
    raise AssertionError('Write succeeded or returned an unexpected reply without quorum: {!r}'.format(reply))


def probe_write(cluster, node, key, value, window):
    """One attempt, no retries. A timeout leaves its eventual commit outcome unknown."""
    with cluster.client(node, io_timeout=window) as client:
        client.request_deadline = min(cluster.deadline, time.monotonic() + window)
        client._arm()
        client.sock.sendall(encode_command('SET', key, value))
        # Only a reply timeout after a successful send is an acceptable unknown result.
        try:
            result = classify_reply(client.read_reply())
        except (socket.timeout, TimeoutError):
            result = dict(outcome='unknown', reply='reply deadline elapsed; not proof of rejection')
    result.update(node=node, key=key, value=value, sent=True)
    return result


def assert_frozen(before, current):
    for field in ('commit_index', 'last_applied'):
        expect(current[field], before[field], 'isolated node {} {}'.format(current['node_id'], field))


def check_reconnects(snapshot, limit):
    counts = [edge['refused'] for edge in snapshot['edges'].values()]
    if any(count < 0 for count in counts):
        raise AssertionError('Negative refused-connection counter')
    total = sum(counts)
    if limit is not None and total > limit:
        raise AssertionError('Reconnect refusals {} exceed configured limit {}'.format(total, limit))
    return dict(refused_connections=total, limit=limit)


class PartitionCluster(Cluster):
    def __init__(self, binary, data, artifacts, timeout, window):
        super().__init__(binary, data, artifacts, timeout)
        self.mesh = None
        self.window = window
        self.report.update(test='raft_tcp_partition', snapshots=[], probes=[], partitions=[],
                           observation_seconds=window, scope='three local processes; TCP disconnect/refusal partition')

    def capture(self, phase):
        self.running(range(3))
        states = {str(node): self.info(node) for node in range(3)}
        self.report['snapshots'].append(dict(phase=phase, monotonic_seconds=time.monotonic(), nodes=states))
        return states

    def settled(self, expected, minimum):
        if self.leader(range(3)) is None or self.convergence(expected, minimum) is None:
            return None
        states = [self.info(node) for node in range(3)]
        if len({s['commit_index'] for s in states}) == 1 and all(
                s['last_applied'] == s['commit_index'] for s in states):
            return states[0]['commit_index']
        return None

    def write(self, node, key, value):
        with self.client(node, io_timeout=5) as client:
            expect(client.command('SET', key, value), 'OK', 'acknowledged write ' + key)
            expect(client.command('GET', key), value.encode(), 'read acknowledged write ' + key)
        return self.info(node)['commit_index']

    def cut(self, groups, phase):
        snapshot = self.mesh.partition(groups)
        self.report['partitions'].append(dict(phase=phase, groups=groups, proxy_at_cut=snapshot))

    def observe(self, phase, baseline, frozen_nodes, futures, until):
        samples = 0
        while time.monotonic() < until or any(not future.done() for future in futures):
            if time.monotonic() >= self.deadline:
                raise TimeoutError('Partition observation exceeded overall deadline')
            states = self.capture(phase)
            for node in frozen_nodes:
                assert_frozen(baseline[str(node)], states[str(node)])
            for future in futures:
                if future.done():
                    future.result()  # Fail immediately if any probe observed success.
            samples += 1
            time.sleep(0.1)
        if samples < 2:
            raise AssertionError('Insufficient partition observation samples')
        self.report['probes'].extend(future.result() for future in futures)
        proxy = self.mesh.snapshot()
        expect(proxy['errors'], [], 'proxy errors')
        self.report['partitions'][-1]['proxy_after_observation'] = proxy

    def heal(self, phase, expected, minimum, uncertain):
        self.cut([[0, 1, 2]], phase)
        leader = self.wait_for('election after ' + phase, lambda: self.leader(range(3)))
        key = 'barrier-' + phase
        minimum = self.write(leader, key, 'recovered')
        expected.append(('default', key, b'recovered'))
        self.wait_for('convergence after ' + phase, lambda: self.settled(expected, minimum))
        # Timeout is not cancellation: allow uncertain writes to be absent or applied,
        # but demand agreement after the recovery barrier on all three replicas.
        def uncertain_converged():
            outcomes = {}
            for key, value in uncertain:
                replies = []
                for node in range(3):
                    with self.client(node) as client:
                        replies.append(client.command('GET', key))
                if any(reply not in (None, value.encode()) for reply in replies):
                    raise AssertionError('Unexpected value for uncertain write ' + key)
                if replies[1:] != replies[:1] * 2:
                    return None
                outcomes[key] = 'absent' if replies[0] is None else 'applied'
            return outcomes
        self.report.setdefault('uncertain_after_recovery', {})[phase] = self.wait_for(
            'uncertain write convergence', uncertain_converged)
        self.capture(phase)
        return minimum

    def run(self):
        self.mesh = RaftProxyMesh({node.node_id: node.raft_port for node in self.nodes})
        for node in self.nodes:
            for reservation in self.reservations[2 * node.node_id:2 * node.node_id + 2]:
                reservation.close()
            peers = ','.join('{}:127.0.0.1:{}'.format(
                other.node_id, other.raft_port if other.node_id == node.node_id else
                self.mesh.ports[node.node_id, other.node_id]) for other in self.nodes)
            node.start(self.binary, peers)
        leader = self.wait_for('initial election through relays', lambda: self.leader(range(3)))
        self.report['initial_leader'] = leader
        minimum = self.write(leader, 'baseline', 'before-partition')
        expected = [('default', 'baseline', b'before-partition')]
        self.wait_for('initial replication', lambda: self.settled(expected, minimum))
        self.step('three live nodes agree and replicate through directed TCP relays')

        baseline = self.capture('before_minority_partition')
        majority = [node for node in range(3) if node != leader]
        self.cut([[leader], majority], 'minority_partition')
        with ThreadPoolExecutor(max_workers=1) as pool:
            until = time.monotonic() + self.window
            future = pool.submit(probe_write, self, leader, 'minority-probe', 'uncertain-one', self.window)
            new_leader = self.wait_for('majority election', lambda: self.leader(majority))
            self.report['majority_leader'] = new_leader
            minimum = self.write(new_leader, 'majority-write', 'with-two-nodes')
            expected.append(('default', 'majority-write', b'with-two-nodes'))
            # Keep a full observation window even if election took longer than expected.
            self.observe('minority_partition', baseline, [leader], [future],
                         max(until, time.monotonic() + self.window))
        self.step('isolated old leader does not acknowledge or commit the probe; majority continues writing')
        uncertain = [('minority-probe', 'uncertain-one')]
        minimum = self.heal('first_heal', expected, minimum, uncertain)
        self.step('first heal preserves acknowledged data and converges on uncertain write outcome')

        baseline = self.capture('before_no_quorum')
        self.cut([[0], [1], [2]], 'no_quorum')
        with ThreadPoolExecutor(max_workers=3) as pool:
            until = time.monotonic() + self.window
            futures = [pool.submit(probe_write, self, node, 'no-quorum-{}'.format(node),
                                   'uncertain-{}'.format(node), self.window) for node in range(3)]
            self.observe('no_quorum', baseline, range(3), futures, until)
        self.step('three isolated live nodes acknowledge no probes and advance no commit or apply index')
        uncertain.extend(('no-quorum-{}'.format(node), 'uncertain-{}'.format(node)) for node in range(3))
        minimum = self.heal('final_heal', expected, minimum, uncertain)
        self.report['minimum_applied_index'] = minimum
        self.report['final_leader'] = self.wait_for('final agreement', lambda: self.leader(range(3)))
        self.step('final heal restores writes and all replicas converge without restarting nodes')
        expect(self.mesh.snapshot()['errors'], [], 'final proxy errors')

    def close(self):
        try:
            super().close()
        finally:
            if self.mesh is not None:
                try:
                    self.report['proxy'] = self.mesh.snapshot()
                finally:
                    self.mesh.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-reconnect-refusals', type=int,
                        help='Optional failure threshold for total relay refusals across both partitions')
    parser.add_argument('--build-report', required=True, type=Path,
                        help='PASS build-report.json identifying the existing server binary')
    parser.add_argument('--artifacts', type=Path, default=Path('build/cluster-partition'))
    parser.add_argument('--timeout', type=float, default=90)
    parser.add_argument('--observe-seconds', type=float, default=4)
    args = parser.parse_args()
    if args.max_reconnect_refusals is not None and args.max_reconnect_refusals < 0:
        parser.error('--max-reconnect-refusals must be nonnegative')
    if sys.platform != 'linux':
        parser.error('Real partition testing requires Linux; run cluster_partition_tests.py for helper tests')
    if not 30 <= args.timeout <= 300 or not 3 <= args.observe_seconds <= 10:
        parser.error('timeout must be 30..300 seconds; observe-seconds must be 3..10')
    build_report = args.build_report.resolve()
    try:
        binary, inputs = validate_build(build_report)
    except (OSError, ValueError, KeyError, AssertionError) as error:
        parser.error(str(error))
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: {}'.format(artifacts), flush=True)
    helpers = [Path(__file__).resolve(), ROOT / 'tests/cluster_smoke.py', ROOT / 'tests/raft_proxy.py']
    identity = dict(binary=dict(path=str(binary), sha256=file_hash(binary)),
                    build_report_sha256=file_hash(build_report), compiled_inputs=inputs,
                    test_source_hashes={p.name: file_hash(p) for p in helpers}, platform=platform.platform())
    (artifacts / 'referenced-build-report.json').write_bytes(build_report.read_bytes())
    for label, command in [('git-head', ['git', 'rev-parse', 'HEAD']),
                           ('git-status', ['git', 'status', '--porcelain=v1'])]:
        try:
            result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=5)
            (artifacts / (label + '.log')).write_bytes(result.stdout + result.stderr)
            identity[label + '-returncode'] = result.returncode
        except (OSError, subprocess.TimeoutExpired) as error:
            identity[label + '-error'] = str(error)
    with tempfile.TemporaryDirectory(prefix='raft-kv-partition-') as data:
        cluster = PartitionCluster(binary, Path(data), artifacts, args.timeout, args.observe_seconds)
        cluster.report['identity'] = identity
        cluster.report['started_at_unix'] = time.time()
        started = time.monotonic()
        try:
            cluster.run()
            snapshot = cluster.mesh.snapshot()
            cluster.report['reconnect_check'] = dict(
                refused_connections=sum(e['refused'] for e in snapshot['edges'].values()),
                limit=args.max_reconnect_refusals,
                node_log_bytes={str(n.node_id): n.log_path.stat().st_size for n in cluster.nodes})
            check_reconnects(snapshot, args.max_reconnect_refusals)
            expect(file_hash(binary), identity['binary']['sha256'], 'binary unchanged during test')
            expect({name: file_hash(ROOT / name) for name in inputs}, inputs, 'compiled inputs unchanged')
            expect({p.name: file_hash(p) for p in helpers}, identity['test_source_hashes'], 'test helpers unchanged')
            cluster.report['status'] = 'PASS'
        except BaseException:
            cluster.report.update(status='FAIL', error=traceback.format_exc())
        finally:
            try:
                cluster.close()
            except BaseException:
                cluster.report.update(status='FAIL', cleanup_error=traceback.format_exc())
            cluster.report['elapsed_seconds'] = round(time.monotonic() - started, 3)
            (artifacts / 'report.json').write_text(json.dumps(cluster.report, ensure_ascii=False, indent=2) + '\n',
                                                   encoding='utf-8')
    print('{}: real Linux partition test; report: {}'.format(cluster.report['status'], artifacts / 'report.json'))
    if cluster.report['status'] != 'PASS':
        print(cluster.report.get('error', cluster.report.get('cleanup_error')), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
