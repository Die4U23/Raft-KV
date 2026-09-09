#!/usr/bin/env python3
"""One bounded Linux baseline with aligned client/server CPU and INFO windows."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import traceback
from types import SimpleNamespace

from cluster_partition import ROOT, file_hash, validate_build
from cluster_smoke import Cluster, RespClient, parse_info
from cluster_overload import process_resources
from load_benchmark import benchmark


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def resources(pids):
    # Linux aggregate CPU fields: guest time is already included in user/nice.
    fields = Path('/proc/stat').read_text().splitlines()[0].split()
    names = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal')
    return dict(at=time.monotonic(), processes={name: process_resources(pid) for name, pid in pids.items()},
                system=dict(zip(names, map(int, fields[1:9]))))


def cpu_summary(start, end):
    elapsed = end['at'] - start['at']
    if elapsed <= 0 or set(start['processes']) != set(end['processes']):
        raise ValueError('Invalid CPU sample window')
    processes = {}
    for name, first in start['processes'].items():
        seconds = end['processes'][name]['cpu_seconds'] - first['cpu_seconds']
        if seconds < 0:
            raise ValueError('Process CPU counter moved backwards')
        processes[name] = dict(cpu_seconds=seconds, percent_of_one_core=100 * seconds / elapsed)
    delta = {key: end['system'][key] - value for key, value in start['system'].items()}
    # /proc/stat iowait can decrease. Preserve raw samples and reject the summary
    # instead of silently clamping a counter to fabricate percentages.
    if min(delta.values()) < 0 or sum(delta.values()) <= 0:
        return dict(window_seconds=elapsed, processes=processes,
                    system_percent=None, system_note='non-monotonic or empty aggregate CPU delta')
    return dict(window_seconds=elapsed, processes=processes,
                system_percent={key: 100 * value / sum(delta.values()) for key, value in delta.items()})


def stage_summary(before, after):
    if (before['node_id'], before['term'], before['state']) != (after['node_id'], after['term'], after['state']):
        raise ValueError('Leader identity/term changed; stage deltas are not comparable')
    result = {}
    for name in ('write_queue_wait', 'leader_log_write', 'replication_data_ack', 'kv_apply',
                 'apply_dispatch', 'write_completed', 'local_read'):
        count = int(after[name + '_count']) - int(before[name + '_count'])
        total = int(after[name + '_total_us']) - int(before[name + '_total_us'])
        if count < 0 or total < 0:
            raise ValueError('Stage counter moved backwards: ' + name)
        result[name] = dict(count=count, mean_us=total / count if count else None)
        if name + '_entries' in before:
            entries = int(after[name + '_entries']) - int(before[name + '_entries'])
            if entries < 0:
                raise ValueError('Stage entries moved backwards')
            result[name]['mean_entries'] = entries / count if count else None
    return result


def worker(job_path):
    job = json.loads(job_path.read_text())
    result = {'status': 'RUNNING', 'windows': {}}
    pids = dict(job['server_pids'], client=os.getpid())

    def infos():
        values = {}
        for node, port in job['ports'].items():
            with RespClient.connect(port, job['deadline'], 2) as client:
                values[node] = parse_info(client.command('INFO'))
        return values

    def observe(event):
        # INFO is outside the workload CPU window: collect before its opening
        # sample and after its closing sample. Preloads are excluded from deltas.
        if event == 'start':
            result['info_before'] = infos()
        result['windows'][event] = resources(pids)
        if event == 'end':
            result['info_after'] = infos()

    async def run():
        args = SimpleNamespace(**job['configuration'])
        warmup = SimpleNamespace(**dict(job['configuration'], requests=10000))
        print('Running warmup: 10000 requests', flush=True)
        result['warmup'] = await benchmark(warmup)
        if result['warmup']['errors']:
            raise RuntimeError('Warmup had client errors')
        print('Running measured workload: {} requests'.format(args.requests), flush=True)
        result['measurement'] = await benchmark(args, measurement_observer=observe)
        if result['measurement']['errors']:
            raise RuntimeError('Measured workload had client errors')
        for state in (result['info_before'], result['info_after']):
            leader = state[str(job['leader'])]
            if leader['state'] != 'leader' or any(i['leader_id'] != job['leader'] or i['term'] != leader['term']
                                                for i in state.values()):
                raise RuntimeError('Nodes disagree about leadership')
        result['cpu'] = cpu_summary(result['windows']['start'], result['windows']['end'])
        result['stages'] = stage_summary(result['info_before'][str(job['leader'])],
                                         result['info_after'][str(job['leader'])])
        if result['stages']['write_completed']['count'] != result['measurement']['writes_attempted']:
            raise RuntimeError('Write counter window differs from measured writes')
        result['status'] = 'PASS'

    try:
        asyncio.run(run())
    except BaseException:
        result.update(status='FAIL', error=traceback.format_exc())
    write_json(job_path.parent / 'client-report.json', result)
    return 0 if result['status'] == 'PASS' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report', type=Path)
    parser.add_argument('--artifacts', type=Path, default=Path('build-linux-reconnect/profile-reports'))
    parser.add_argument('--requests', type=int, default=100000)
    parser.add_argument('--worker', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('Requires Linux /proc; use profile_benchmark_tests.py for helper checks')
    if args.worker:
        return worker(args.worker)
    if not args.build_report or not 10000 <= args.requests <= 200000:
        parser.error('Provide --build-report; requests must be 10000..200000')
    binary, inputs = validate_build(args.build_report.resolve())
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: ' + str(artifacts), flush=True)
    helpers = {name: file_hash(ROOT / 'tests' / name) for name in ('profile_benchmark.py', 'load_benchmark.py',
        'cluster_partition.py', 'cluster_smoke.py', 'cluster_overload.py', 'raft_proxy.py')}
    report = dict(status='RUNNING', test='aligned_performance_diagnostic', samples=[],
        identity=dict(binary=dict(path=str(binary), sha256=file_hash(binary)), compiled_inputs=inputs,
            test_source_hashes=helpers, build_report_sha256=file_hash(args.build_report)),
        environment=dict(cpu_count=os.cpu_count(), affinity=sorted(os.sched_getaffinity(0)),
                         kernel=os.uname().release, deployment='three servers and one Python load process on one VM'))
    (artifacts / 'referenced-build-report.json').write_bytes(args.build_report.read_bytes())
    for label, command in [('git-head', ['git', 'rev-parse', 'HEAD']), ('git-status', ['git', 'status', '--porcelain=v1'])]:
        p = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=5)
        (artifacts / (label + '.log')).write_bytes(p.stdout + p.stderr)
        report['identity'][label + '-returncode'] = p.returncode
    with tempfile.TemporaryDirectory(prefix='raft-kv-profile-') as data:
        cluster = Cluster(binary, Path(data), artifacts, 240)
        process = None
        started = time.monotonic()
        try:
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=['--async_apply=true', '--group_commit_ms=1'])
            leader = cluster.wait_for('initial leader agreement', lambda: cluster.leader(range(3)))
            report['leader'] = leader
            print('Leader: ' + str(leader), flush=True)
            configuration = dict(host='127.0.0.1', port=cluster.nodes[leader].client_port, connections=32,
                requests=args.requests, pipeline=1, value_size=128, write_ratio=0.5, timeout=5, namespace='profile')
            job = dict(configuration=configuration, leader=leader, deadline=cluster.deadline,
                server_pids={str(n.node_id): n.process.pid for n in cluster.nodes},
                ports={str(n.node_id): n.client_port for n in cluster.nodes})
            job_path = artifacts / 'client-job.json'
            write_json(job_path, job)
            with (artifacts / 'client.log').open('wb') as log:
                process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--worker', str(job_path)],
                                           stdout=log, stderr=subprocess.STDOUT)
                pids = dict(job['server_pids'], client=process.pid, sampler=os.getpid())
                while process.poll() is None:
                    cluster.running(range(3))
                    if time.monotonic() >= cluster.deadline:
                        raise TimeoutError('Diagnostic exceeded 240 seconds')
                    try:
                        report['samples'].append(resources(pids))
                    except (FileNotFoundError, ProcessLookupError, KeyError):
                        if process.poll() is None:
                            raise
                    time.sleep(0.5)
                client = json.loads((artifacts / 'client-report.json').read_text())
                report['client'] = client
                if process.returncode or client['status'] != 'PASS':
                    raise RuntimeError('Client failed; see client-report.json and client.log')
            start, end = (client['windows'][key]['at'] for key in ('start', 'end'))
            report['measurement_sample_count'] = sum(start <= s['at'] <= end for s in report['samples'])
            report['sampling_note'] = 'samples include setup/warmup; use only timestamps inside client.windows for measured load'
            report['cpu_window_note'] = 'CPU snapshots bracket the timed workload with small /proc sampling overhead; INFO and connection setup/cleanup are outside'
            measured = client['measurement']
            expected = [('profile', 'run-{}-worker-{}'.format(measured['run_id'], i), b'x' * 128) for i in range(32)]
            minimum = client['info_after'][str(leader)]['commit_index']
            cluster.wait_for('measured keys converge', lambda: cluster.convergence(expected, minimum))
            report['verified_keys_per_replica'] = 32
            if file_hash(binary) != report['identity']['binary']['sha256'] or any(file_hash(ROOT / p) != h for p, h in inputs.items()):
                raise RuntimeError('Binary or compiled source changed')
            if any(file_hash(ROOT / 'tests' / p) != h for p, h in helpers.items()):
                raise RuntimeError('Diagnostic helper changed during run')
            report['status'] = 'PASS'
        except BaseException:
            report.update(status='FAIL', error=traceback.format_exc())
        finally:
            try:
                try:
                    if process and process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                finally:
                    cluster.close()
            except BaseException:
                report.update(status='FAIL', cleanup_error=traceback.format_exc())
            report.update(nodes=cluster.report.get('nodes', []), last_info=cluster.report['last_info'],
                          elapsed_seconds=time.monotonic() - started)
            write_json(artifacts / 'report.json', report)
    print('{}: aligned performance diagnostic; report: {}'.format(report['status'], artifacts / 'report.json'))
    if report['status'] == 'PASS':
        c = report['client']
        print(json.dumps(dict(goodput=c['measurement']['goodput_ops_per_second'],
            latency_ms=c['measurement']['batch_latency_ms'], cpu=c['cpu'], stages=c['stages']), ensure_ascii=False, indent=2))
    else:
        print(report.get('error', report.get('cleanup_error')), file=sys.stderr)
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
