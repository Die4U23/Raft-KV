#!/usr/bin/env python3
"""Write continuously across a leader crash, then verify acknowledged keys after restarts."""
import argparse
import json
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback

from cluster_partition import ROOT, file_hash, validate_build
from cluster_smoke import Cluster, RespError, encode_command, expect, parse_info


def write_batch(cluster, target, rows, after_send):
    """Keep every attempt; never retry a key whose result is unknown."""
    attempted_send = False
    next_reply = 0
    try:
        with cluster.client(target, io_timeout=1.5) as client:
            client.request_deadline = min(cluster.deadline, time.monotonic() + 1.5)
            client._arm()
            attempted_send = True
            client.sock.sendall(b''.join(encode_command('SET', row['key'], row['value']) for row in rows))
            sent_at = time.monotonic()
            for row in rows:
                row['sent_at'] = sent_at
            after_send()
            for i, row in enumerate(rows):
                reply = client.read_reply()
                row['finished_at'] = time.monotonic()
                if reply == 'OK':
                    row['outcome'] = 'acknowledged'
                elif isinstance(reply, RespError):
                    if reply.message in ('ERR leadership lost; outcome unknown', 'ERR server stopped; outcome unknown'):
                        row.update(outcome='unknown', reply=reply.message)
                    elif reply.message.startswith(('ERR MOVED ', 'ERR BUSY ')):
                        row.update(outcome='rejected', reply=reply.message)
                    else:
                        raise AssertionError('Unexpected SET error: ' + reply.message)
                    if reply.message.startswith('ERR MOVED '):
                        candidate = reply.message.split()[-1]
                        if candidate.isdigit() and int(candidate) in range(3):
                            target = int(candidate)
                else:
                    raise AssertionError('Unexpected SET reply: {!r}'.format(reply))
                next_reply = i + 1
    except (OSError, TimeoutError, ConnectionError) as error:
        for row in rows[next_reply:]:
            row.update(outcome='unknown' if attempted_send else 'not_sent',
                       error=repr(error), finished_at=time.monotonic())
        target = (target + 1) % 3
    return target


class ContinuousWriter:
    def __init__(self, cluster, leader):
        self.cluster = cluster
        self.target = leader
        self.rows = []
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.fault_done = threading.Event()
        self.plan = None
        self.fault = None
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self):
        self.thread.start()

    def totals(self):
        with self.lock:
            totals = {name: 0 for name in ('acknowledged', 'rejected', 'unknown', 'not_sent')}
            for row in self.rows:
                totals[row['outcome']] += 1
            return totals

    def check(self):
        if self.error:
            raise RuntimeError('Continuous writer failed: ' + self.error)
        if not self.thread.is_alive() and not self.stopping.is_set():
            raise RuntimeError('Continuous writer stopped unexpectedly')

    def crash_next_batch(self, node):
        with self.lock:
            if self.plan is not None or self.fault is not None:
                raise RuntimeError('Only one in-flight leader crash is supported')
            self.plan = node

    def run(self):
        sequence = 0
        try:
            while not self.stopping.is_set():
                if time.monotonic() >= self.cluster.deadline or sequence >= 20000:
                    raise TimeoutError('Continuous writer exceeded deadline or 20000-attempt bound')
                with self.lock:
                    plan = self.plan
                target = self.target if plan is None else plan
                rows = [dict(sequence=i, node=target, key='restart-{:06d}'.format(i),
                             value='value-{:06d}-'.format(i) + 'x' * 128,
                             started_at=time.monotonic()) for i in range(sequence, sequence + 8)]
                sequence += len(rows)

                def after_send():
                    if plan is not None:
                        # Stop after sendall and before reading any reply in this batch.
                        # This establishes client-side in-flight work, not its commit state.
                        self.fault = dict(node=plan, sequences=[r['sequence'] for r in rows],
                                          sent_at=rows[0]['sent_at'],
                                          acknowledged_before=self.totals()['acknowledged'],
                                          signal_started_at=time.monotonic())
                        self.cluster.nodes[plan].stop(crash=True)
                        self.fault['process_exited_at'] = time.monotonic()
                        with self.lock:
                            self.plan = None
                        self.fault_done.set()

                self.target = write_batch(self.cluster, target, rows, after_send)
                with self.lock:
                    self.rows.extend(rows)
                self.stopping.wait(0.01)  # Bounded functional load, not a throughput benchmark.
        except BaseException:
            self.error = traceback.format_exc()

    def stop(self):
        self.stopping.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError('Writer did not stop within cleanup deadline')
        if self.error:
            raise RuntimeError(self.error)


def check_value(row, value):
    expected = row['value'].encode()
    if row['outcome'] == 'acknowledged':
        expect(value, expected, 'acknowledged key ' + row['key'])
    elif row['outcome'] == 'unknown':
        if value not in (None, expected):
            raise AssertionError('Unexpected value for unknown key ' + row['key'])
    else:
        expect(value, None, 'unaccepted key ' + row['key'])


class WriteRestartCluster(Cluster):
    def __init__(self, binary, data, artifacts, timeout):
        super().__init__(binary, data, artifacts, timeout)
        self.writer = None
        self.report.update(test='continuous_write_restart', checkpoints=[], verification={})

    def capture(self, phase, nodes):
        self.running(nodes)
        record = dict(phase=phase, at=time.monotonic(),
                      nodes={str(node): self.info(node) for node in nodes})
        if self.writer:
            record['writes'] = self.writer.totals()
        self.report['checkpoints'].append(record)
        return record

    def wait_writes(self, phase, minimum, started, nodes):
        def ready():
            self.writer.check()
            self.running(nodes)
            if self.writer.totals()['acknowledged'] >= minimum and time.monotonic() - started >= 2:
                return True
            return None
        self.wait_for(phase, ready)
        return self.capture(phase, nodes)

    def settled(self, minimum):
        if self.leader(range(3)) is None:
            return None
        states = [self.info(node) for node in range(3)]
        if all(s['commit_index'] == s['last_applied'] >= minimum for s in states) and len(
                {s['commit_index'] for s in states}) == 1:
            return states[0]['commit_index']
        return None

    def verify_keys(self, phase, rows):
        results = {}
        self.report['verification'][phase] = results
        canonical = None
        for node in range(3):
            self.running(range(3))
            observed = []
            with self.client(node, io_timeout=5) as client:
                for start in range(0, len(rows), 64):
                    batch = rows[start:start + 64]
                    values = client.pipeline([('GET', row['key']) for row in batch])
                    for row, value in zip(batch, values):
                        check_value(row, value)
                        observed.append(value is not None)
                expect(client.command('GET', 'restart-verification-barrier'), b'writer-stopped',
                       'verification barrier after recovery')
            results[str(node)] = dict(checked=len(rows), acknowledged_checked=sum(
                r['outcome'] == 'acknowledged' for r in rows),
                barrier_checked=True,
                unknown_present=[r['sequence'] for r, present in zip(rows, observed)
                                 if r['outcome'] == 'unknown' and present])
            if canonical is not None:
                expect(observed, canonical, 'replica agreement on all attempted keys')
            canonical = observed
        return results

    def run(self):
        for node in self.nodes:
            for reservation in self.reservations[2 * node.node_id:2 * node.node_id + 2]:
                reservation.close()
            node.start(self.binary, self.peers)
        leader = self.wait_for('initial election', lambda: self.leader(range(3)))
        self.report['initial_leader'] = leader
        self.writer = ContinuousWriter(self, leader)
        self.writer.start()
        self.wait_writes('before_crash', 64, time.monotonic(), range(3))
        self.step('continuous writer acknowledges at least 64 unique keys before the fault')
        leader = self.wait_for('leader before crash', lambda: self.leader(range(3)))
        self.writer.crash_next_batch(leader)
        def crashed():
            self.writer.check()
            return True if self.writer.fault_done.is_set() else None
        self.wait_for('crash after batch send', crashed)
        self.report['inflight_crash'] = self.writer.fault
        self.step('leader is SIGKILLed after a batch is sent and before its replies are read')
        survivors = [node for node in range(3) if node != leader]
        new_leader = self.wait_for('survivor election', lambda: self.leader(survivors))
        self.report['failover_leader'] = new_leader
        before = self.writer.totals()['acknowledged']
        self.wait_writes('writes_with_old_leader_down', before + 64, time.monotonic(), survivors)
        self.step('writer continues through failover and survivors acknowledge at least 64 more keys')
        self.nodes[leader].start(self.binary, self.peers)
        self.report['old_leader_restarted_at'] = time.monotonic()
        before = self.writer.totals()['acknowledged']
        self.wait_writes('writes_after_old_leader_restart', before + 64, time.monotonic(), range(3))
        self.writer.stop()
        self.report['writer_stopped_at'] = time.monotonic()
        rows = list(self.writer.rows)
        self.report['writes'] = self.writer.totals()
        leader = self.wait_for('agreement after writer stop', lambda: self.leader(range(3)))
        with self.client(leader, io_timeout=5) as client:
            expect(client.command('SET', 'restart-verification-barrier', 'writer-stopped'), 'OK',
                   'post-writer verification barrier')
            minimum = parse_info(client.command('INFO'))['commit_index']
        self.report['verification_barrier'] = dict(node=leader, minimum_commit_index=minimum,
                                                   acknowledged_at=time.monotonic())
        minimum = self.wait_for('apply all acknowledged writes', lambda: self.settled(minimum))
        self.verify_keys('after_leader_restart', rows)
        self.capture('verified_after_leader_restart', range(3))
        self.step('all acknowledged keys survive leader restart; every attempted key agrees on three replicas')

        # Quiescent full-cluster crash checks recovery from the paired disk directories.
        # The continuous-write fault above is separate; this is not a power-loss simulation.
        self.report['full_cluster_crash_at'] = time.monotonic()
        for node in self.nodes:
            node.stop(crash=True)
        for node in self.nodes:
            node.start(self.binary, self.peers)
        self.wait_for('full-cluster recovery', lambda: self.settled(minimum))
        self.verify_keys('after_full_cluster_restart', rows)
        expect(self.report['verification']['after_full_cluster_restart'],
               self.report['verification']['after_leader_restart'], 'key outcomes survive full-cluster restart')
        self.report['final_leader'] = self.wait_for('final leader', lambda: self.leader(range(3)))
        self.capture('verified_after_full_cluster_restart', range(3))
        self.step('all nodes restart from the same paired directories and retain all acknowledged keys')

    def close(self):
        try:
            if self.writer:
                try:
                    self.writer.stop()
                finally:
                    self.report['writes'] = self.writer.totals()
                    self.report['attempts'] = list(self.writer.rows)
                    if self.writer.fault:
                        self.report['inflight_crash'] = self.writer.fault
        finally:
            super().close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, default=Path('build/write-restart'))
    parser.add_argument('--timeout', type=float, default=120)
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('Real service testing requires Linux; run cluster_write_restart_tests.py for helper checks')
    if not 30 <= args.timeout <= 300:
        parser.error('--timeout must be between 30 and 300 seconds')
    build_report = args.build_report.resolve()
    try:
        binary, inputs = validate_build(build_report)
    except (OSError, ValueError, KeyError, AssertionError) as error:
        parser.error(str(error))
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: {}'.format(artifacts), flush=True)
    helpers = [ROOT / 'tests' / name for name in ('cluster_write_restart.py', 'cluster_partition.py',
                                                'cluster_smoke.py', 'raft_proxy.py')]
    identity = dict(binary=dict(path=str(binary), sha256=file_hash(binary)), compiled_inputs=inputs,
                    build_report_sha256=file_hash(build_report), platform=platform.platform(),
                    test_source_hashes={p.name: file_hash(p) for p in helpers})
    (artifacts / 'referenced-build-report.json').write_bytes(build_report.read_bytes())
    for label, command in [('git-head', ['git', 'rev-parse', 'HEAD']),
                           ('git-status', ['git', 'status', '--porcelain=v1'])]:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=5)
        (artifacts / (label + '.log')).write_bytes(result.stdout + result.stderr)
        identity[label + '-returncode'] = result.returncode
    with tempfile.TemporaryDirectory(prefix='raft-kv-write-restart-') as data:
        cluster = WriteRestartCluster(binary, Path(data), artifacts, args.timeout)
        cluster.report.update(identity=identity, started_at_unix=time.time())
        started = time.monotonic()
        try:
            cluster.run()
            expect(file_hash(binary), identity['binary']['sha256'], 'binary unchanged')
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
    print('{}: real Linux continuous-write restart test; report: {}'.format(
        cluster.report['status'], artifacts / 'report.json'))
    if cluster.report['status'] != 'PASS':
        print(cluster.report.get('error', cluster.report.get('cleanup_error')), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
