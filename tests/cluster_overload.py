#!/usr/bin/env python3
"""Bounded admission, pending-write overload and Linux resource soak checks."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback

from cluster_partition import ROOT, file_hash, validate_build
from cluster_smoke import Cluster, RespError, encode_command, expect, parse_info
from raft_proxy import RaftProxyMesh


QUEUE_LIMIT = 16 * 1024 * 1024


def assert_bounds(info):
    for key, limit in [('connected_clients', 32), ('queued_writes', 1024), ('pending_proposals', 1024),
                       ('queued_write_bytes', QUEUE_LIMIT), ('pending_proposal_bytes', QUEUE_LIMIT),
                       ('client_input_bytes', 64 * 1024 * 1024),
                       ('client_output_reserved_bytes', 64 * 1024 * 1024)]:
        value = int(info[key])
        if not 0 <= value <= limit:
            raise AssertionError('{}={} exceeds {}'.format(key, value, limit))


def drained(info):
    return int(info['connected_clients']) == 1 and all(int(info[key]) == 0 for key in (
        'queued_writes', 'queued_write_bytes', 'pending_proposals', 'pending_proposal_bytes',
        'apply_lag', 'apply_inflight', 'client_input_bytes', 'client_output_reserved_bytes'))


def parse_resources(status_text, stat_text, fd_count, ticks_per_second):
    status = dict(line.split(':', 1) for line in status_text.splitlines()
                  if ':' in line)
    fields = stat_text.rsplit(')', 1)[1].split()
    return dict(rss_bytes=int(status['VmRSS'].split()[0]) * 1024,
                cpu_seconds=(int(fields[11]) + int(fields[12])) / ticks_per_second,
                threads=int(status['Threads']), fd_count=fd_count)


def process_resources(pid):
    return parse_resources(Path('/proc/{}/status'.format(pid)).read_text(),
                           Path('/proc/{}/stat'.format(pid)).read_text(),
                           len(list(Path('/proc/{}/fd'.format(pid)).iterdir())), os.sysconf('SC_CLK_TCK'))


def resource_summary(samples):
    steady = [sample for sample in samples if sample['phase'] == 'soak']
    if len(steady) < 30:
        raise AssertionError('Fewer than 30 steady-state resource samples')
    result = {}
    for node in ('0', '1', '2'):
        head = steady[:10]
        tail = steady[-10:]
        rss_growth = statistics.median(s['process'][node]['rss_bytes'] for s in tail) - statistics.median(
            s['process'][node]['rss_bytes'] for s in head)
        fd_growth = statistics.median(s['process'][node]['fd_count'] for s in tail) - statistics.median(
            s['process'][node]['fd_count'] for s in head)
        peak_rss = max(s['process'][node]['rss_bytes'] for s in samples)
        if rss_growth > 32 * 1024 * 1024 or fd_growth > 8 or peak_rss > 512 * 1024 * 1024:
            raise AssertionError('Resource guard exceeded for node {}: RSS growth {}, FD growth {}, peak RSS {}'.format(
                node, rss_growth, fd_growth, peak_rss))
        seconds = steady[-1]['at'] - steady[0]['at']
        cpu = steady[-1]['process'][node]['cpu_seconds'] - steady[0]['process'][node]['cpu_seconds']
        result[node] = dict(rss_growth_bytes=rss_growth, fd_growth=fd_growth, peak_rss_bytes=peak_rss,
                            soak_cpu_percent_of_one_core=100 * cpu / seconds)
    return result


class FixedKeyLoad:
    def __init__(self, cluster, leader):
        self.cluster, self.leader = cluster, leader
        self.stop_event = threading.Event()
        self.counts = [0, 0]
        self.errors = []
        self.threads = [threading.Thread(target=self.run, args=(i,), daemon=True) for i in range(2)]

    @staticmethod
    def key_value(worker, index):
        key = 'soak-{}-{}'.format(worker, index % 64)
        return key, ('value-' + key + '-' + 'x' * 128).encode()

    def run(self, worker):
        try:
            with self.cluster.client(self.leader, io_timeout=3) as client:
                while not self.stop_event.is_set():
                    key, value = self.key_value(worker, self.counts[worker])
                    expect(client.pipeline([('SET', key, value), ('GET', key)]), ['OK', value], 'soak SET/GET')
                    self.counts[worker] += 1
                    self.stop_event.wait(0.01)
        except BaseException:
            self.errors.append(traceback.format_exc())

    def start(self):
        for thread in self.threads:
            thread.start()

    def check(self):
        if self.errors:
            raise RuntimeError('Soak client failed: ' + self.errors[0])

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError('Soak worker failed to stop')
        self.check()


class OverloadCluster(Cluster):
    def __init__(self, binary, data, artifacts, timeout, seconds):
        super().__init__(binary, data, artifacts, timeout)
        self.mesh = None
        self.monitors = {}
        self.load = None
        self.seconds = seconds
        self.report.update(test='bounded_overload_soak', samples=[], warnings=[],
                           configuration=dict(max_clients=32, pending_writes=24, value_bytes=800 * 1024,
                                              soak_seconds=seconds, soak_clients=2, fixed_keys=128,
                                              rss_growth_guard_bytes=32 * 1024 * 1024,
                                              peak_rss_guard_bytes=512 * 1024 * 1024, fd_growth_guard=8))

    def info(self, node):
        if node not in self.monitors:
            self.monitors[node] = self.client(node, io_timeout=1)
        fields = parse_info(self.monitors[node].command('INFO'))
        expect(fields['node_id'], node, 'monitor node')
        self.report['last_info'][str(node)] = fields
        return fields

    def sample(self, phase):
        self.running(range(3))
        infos = {str(node): self.info(node) for node in range(3)}
        for info in infos.values():
            assert_bounds(info)
        record = dict(phase=phase, at=time.monotonic(), info=infos,
                      process={str(n.node_id): process_resources(n.process.pid) for n in self.nodes},
                      log_bytes={str(n.node_id): n.log_path.stat().st_size for n in self.nodes})
        self.report['samples'].append(record)
        return record

    def admission(self, leader):
        holders = []
        before = int(self.info(leader)['overload_rejections'])
        try:
            for _ in range(31):
                client = self.client(leader)
                holders.append(client)
                expect(client.command('PING'), 'PONG', 'admitted client')
            expect(int(self.info(leader)['connected_clients']), 32, 'configured admission limit')
            closed = 0
            for _ in range(5):
                try:
                    with self.client(leader, io_timeout=1) as client:
                        reply = client.command('PING')
                except (socket.timeout, TimeoutError):
                    raise AssertionError('Excess connection timed out instead of being closed')
                except (OSError, ConnectionError):
                    closed += 1
                else:
                    raise AssertionError('Excess client accepted: {!r}'.format(reply))
            delta = int(self.info(leader)['overload_rejections']) - before
            if closed != 5 or delta < 5:
                raise AssertionError('Missing admission rejection evidence')
            self.report['admission'] = dict(held_clients=31, monitor_clients=1, excess_closed=closed,
                                            overload_rejections_delta=delta)
            self.sample('admission_full')
        finally:
            for client in holders:
                client.sock.close()
        self.wait_for('admission slots released', lambda: True if drained(self.info(leader)) else None)
        with self.client(leader) as client:
            expect(client.command('PING'), 'PONG', 'new client after overload release')
        self.step('connection limit rejects excess clients and accepts new clients after release')

    def pending_overload(self, leader):
        majority = [node for node in range(3) if node != leader]
        self.report['proxy_before_partition'] = self.mesh.partition([[leader], majority])
        before = int(self.info(leader)['overload_rejections'])
        value = b'x' * (800 * 1024)

        def attempt(index):
            key = 'pending-overload-{}'.format(index)
            with self.client(leader, io_timeout=3) as client:
                client.request_deadline = min(self.deadline, time.monotonic() + 3)
                client._arm()
                client.sock.sendall(encode_command('SET', key, value))
                try:
                    reply = client.read_reply()
                except (socket.timeout, TimeoutError):
                    return dict(key=key, outcome='unknown')
                if isinstance(reply, RespError) and reply.message.startswith('ERR BUSY '):
                    return dict(key=key, outcome='busy', reply=reply.message)
                raise AssertionError('Unexpected isolated-leader write reply: {!r}'.format(reply))

        with ThreadPoolExecutor(max_workers=24) as pool:
            futures = [pool.submit(attempt, i) for i in range(24)]
            while any(not future.done() for future in futures):
                if time.monotonic() >= self.deadline:
                    raise TimeoutError('Pending overload deadline')
                self.sample('pending_overload')
                for future in futures:
                    if future.done():
                        future.result()
                time.sleep(0.2)
            outcomes = [future.result() for future in futures]
        busy = sum(row['outcome'] == 'busy' for row in outcomes)
        if busy < 1:
            raise AssertionError('No explicit BUSY reply: pending-write overload was not exercised')
        after = int(self.info(leader)['overload_rejections'])
        if after - before < busy:
            raise AssertionError('BUSY replies not reflected by overload counter')
        self.report['pending_overload'] = dict(outcomes=outcomes, busy=busy,
                                               unknown=24 - busy, overload_rejections_delta=after - before)
        self.report['proxy_after_partition'] = self.mesh.snapshot()
        self.mesh.partition([[0, 1, 2]])
        leader = self.wait_for('leader after overload heal', lambda: self.leader(range(3)))
        with self.client(leader, io_timeout=5) as client:
            expect(client.command('SET', 'overload-recovery', 'ready'), 'OK', 'write after overload')
        self.wait_for('all pending resources released', lambda: True if all(
            drained(self.info(node)) for node in range(3)) else None)
        expected = [('default', 'overload-recovery', b'ready')]
        self.wait_for('recovery marker replication', lambda: self.convergence(expected, self.info(leader)['commit_index']))
        refused = sum(edge['refused'] for edge in self.report['proxy_after_partition']['edges'].values())
        self.report['reconnect_refused_connections'] = refused
        if refused > 100:
            self.report['warnings'].append('High reconnect count during TCP-close partition: {}; retry/log cost remains an open issue'.format(refused))
        self.step('pending-write saturation returns BUSY and releases queues after quorum recovery')
        return leader

    def run(self):
        self.mesh = RaftProxyMesh({n.node_id: n.raft_port for n in self.nodes})
        for node in self.nodes:
            for sock in self.reservations[2 * node.node_id:2 * node.node_id + 2]:
                sock.close()
            peers = ','.join('{}:127.0.0.1:{}'.format(other.node_id, other.raft_port if other.node_id == node.node_id
                else self.mesh.ports[node.node_id, other.node_id]) for other in self.nodes)
            node.start(self.binary, peers, extra_args=['--max_clients=32'])
        leader = self.wait_for('initial agreement', lambda: self.leader(range(3)))
        self.report['initial_leader'] = leader
        self.sample('baseline')
        self.admission(leader)
        leader = self.pending_overload(leader)
        self.load = FixedKeyLoad(self, leader)
        self.load.start()
        for phase, duration in [('warmup', 10), ('soak', self.seconds)]:
            phase_started = time.monotonic()
            until = phase_started + duration
            print('Running {}: {} seconds'.format(phase, duration), flush=True)
            while time.monotonic() < until:
                if time.monotonic() >= self.deadline:
                    raise TimeoutError('Overall resource test deadline')
                self.load.check()
                self.sample(phase)
                time.sleep(1)
            self.report.setdefault('load_windows', {})[phase] = dict(started_at=phase_started,
                ended_at=time.monotonic(), requested_seconds=duration)
        self.load.stop()
        if min(self.load.counts) < 64:
            raise AssertionError('Fixed key set was not fully exercised')
        self.report['load'] = dict(confirmed_set_get_pairs=list(self.load.counts), client_errors=list(self.load.errors))
        self.step('fixed-key SET/GET workload completes the bounded soak without client errors')
        self.wait_for('post-load queues drain', lambda: True if all(drained(self.info(node)) for node in range(3)) else None)
        expected = [('default', *FixedKeyLoad.key_value(worker, index)) for worker in range(2) for index in range(64)]
        self.wait_for('all fixed keys replicated', lambda: self.convergence(expected, self.info(leader)['commit_index']))
        self.wait_for('verification clients released', lambda: True if all(drained(self.info(node)) for node in range(3)) else None)
        print('Running recovery sampling: 10 seconds', flush=True)
        for _ in range(10):
            record = self.sample('recovery')
            if not all(drained(info) for info in record['info'].values()):
                raise AssertionError('Resources did not remain drained after load stopped')
            time.sleep(1)
        self.report['resources'] = resource_summary(self.report['samples'])
        self.report['final_leader'] = self.wait_for('final agreement', lambda: self.leader(range(3)))
        expect(self.mesh.snapshot()['errors'], [], 'proxy errors')
        self.step('queues drain, fixed keys converge and bounded RSS/FD growth guards pass')

    def close(self):
        try:
            if self.load:
                self.load.stop()
        finally:
            for client in self.monitors.values():
                client.sock.close()
            try:
                super().close()
            finally:
                if self.mesh:
                    try:
                        self.report['proxy_final'] = self.mesh.snapshot()
                    finally:
                        self.mesh.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, default=Path('build/overload-soak'))
    parser.add_argument('--soak-seconds', type=int, default=60)
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('Real overload/resource testing requires Linux and /proc')
    if not 60 <= args.soak_seconds <= 180:
        parser.error('--soak-seconds must be 60..180')
    path = args.build_report.resolve()
    try:
        binary, inputs = validate_build(path)
    except (OSError, ValueError, KeyError, AssertionError) as error:
        parser.error(str(error))
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: {}'.format(artifacts), flush=True)
    helpers = [ROOT / 'tests' / name for name in ('cluster_overload.py', 'cluster_partition.py', 'cluster_smoke.py', 'raft_proxy.py')]
    identity = dict(binary=dict(path=str(binary), sha256=file_hash(binary)), compiled_inputs=inputs,
                    build_report_sha256=file_hash(path), platform=platform.platform(),
                    test_source_hashes={p.name: file_hash(p) for p in helpers})
    (artifacts / 'referenced-build-report.json').write_bytes(path.read_bytes())
    for name, command in [('git-head', ['git', 'rev-parse', 'HEAD']), ('git-status', ['git', 'status', '--porcelain=v1'])]:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=5)
        (artifacts / (name + '.log')).write_bytes(result.stdout + result.stderr)
        identity[name + '-returncode'] = result.returncode
    with tempfile.TemporaryDirectory(prefix='raft-kv-overload-') as data:
        cluster = OverloadCluster(binary, Path(data), artifacts, args.soak_seconds + 90, args.soak_seconds)
        cluster.report.update(identity=identity, started_at_unix=time.time())
        started = time.monotonic()
        try:
            cluster.run()
            expect(file_hash(binary), identity['binary']['sha256'], 'binary unchanged')
            expect({name: file_hash(ROOT / name) for name in inputs}, inputs, 'compiled sources unchanged')
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
            (artifacts / 'report.json').write_text(json.dumps(cluster.report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('{}: bounded Linux overload/soak test; report: {}'.format(cluster.report['status'], artifacts / 'report.json'))
    for warning in cluster.report['warnings']:
        print('OBSERVATION: ' + warning)
    if cluster.report['status'] != 'PASS':
        print(cluster.report.get('error', cluster.report.get('cleanup_error')), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
