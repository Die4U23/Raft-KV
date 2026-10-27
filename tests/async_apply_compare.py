#!/usr/bin/env python3
"""Sequential sync-apply versus async-apply comparison on one three-node cluster at a time.

pipeline=1 cells are the latency comparison. pipeline=16 cells are a separate throughput
comparison. Repeats alternate which mode starts first. This script does not decide that
one mode is faster in general; the JSON report is one run on this machine.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from types import SimpleNamespace

from cluster_partition import ROOT, file_hash, validate_build
from cluster_smoke import Cluster, RespClient, parse_info
from load_benchmark import benchmark
from profile_benchmark import cpu_summary, resources, stage_summary


PIPELINES = (1, 16)
INFO_NAMES = ('async_apply', 'apply_inflight', 'apply_lag', 'commit_index', 'last_applied',
              'term', 'state', 'leader_id')


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def cell_sequence(repeats=3, pipelines=PIPELINES):
    if repeats < 1:
        raise ValueError('repeats must be positive')
    cells = []
    for pipeline in pipelines:
        for index in range(repeats):
            modes = (False, True) if index % 2 == 0 else (True, False)
            for async_apply in modes:
                cells.append({'pipeline': pipeline, 'async_apply': async_apply, 'repeat': index})
    return cells


def longest_mount(path, lines):
    path = Path(path).resolve()
    best = None
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point = Path(parts[1].replace('\\040', ' '))
        try:
            path.relative_to(mount_point)
        except ValueError:
            continue
        if best is None or len(mount_point.parts) > len(Path(best['mount']).parts):
            best = {'device': parts[0].replace('\\040', ' '),
                    'mount': str(mount_point), 'fstype': parts[2]}
    return best


def summarize_info_samples(samples):
    overhead = 0.0
    max_lag = None
    terms = []
    disagreements = 0
    for sample in samples:
        overhead += sample['duration_s']
        nodes = list(sample['nodes'].values())
        leaders = [node for node in nodes if node.get('state') == 'leader']
        leader_ids = {node.get('leader_id') for node in nodes}
        terms_now = {node.get('term') for node in nodes}
        if len(nodes) < 1 or len(leader_ids) != 1 or len(terms_now) != 1 or len(leaders) != 1:
            disagreements += 1
            continue
        leader = leaders[0]
        lag = int(leader['apply_lag'])
        max_lag = lag if max_lag is None else max(max_lag, lag)
        if not terms or terms[-1] != leader['term']:
            terms.append(leader['term'])
    return {'sample_count': len(samples), 'sampling_overhead_seconds': overhead,
            'leadership_disagreements': disagreements, 'max_apply_lag': max_lag,
            'term_increases': max(0, len(terms) - 1), 'terms_seen': terms}


def _kept_info(fields):
    return {name: fields[name] for name in INFO_NAMES if name in fields}


class InfoSampler:
    def __init__(self, ports, deadline, interval):
        self.ports = ports
        self.deadline = deadline
        self.interval = interval
        self.samples = []
        self.errors = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            begun = time.perf_counter()
            try:
                nodes = {}
                for node, port in self.ports.items():
                    with RespClient.connect(port, self.deadline, 1) as client:
                        nodes[node] = _kept_info(parse_info(client.command('INFO')))
                self.samples.append({'at': time.time(), 'duration_s': time.perf_counter() - begun,
                                     'nodes': nodes})
            except Exception as error:
                self.errors.append(repr(error))
            self._stop.wait(self.interval)


def worker(job_path):
    job = json.loads(Path(job_path).read_text(encoding='utf-8'))
    result = {'status': 'RUNNING', 'windows': {}}
    pids = dict(job['server_pids'], client=os.getpid())
    sampler = InfoSampler(job['ports'], job['deadline'], job['info_interval'])

    def infos():
        values = {}
        for node, port in job['ports'].items():
            with RespClient.connect(port, job['deadline'], 2) as client:
                values[node] = parse_info(client.command('INFO'))
        return values

    def observe(event):
        if event == 'start':
            result['info_before'] = infos()
            result['windows'][event] = resources(pids)
            sampler.start()
            return
        sampler.stop()
        result['windows'][event] = resources(pids)
        result['info_during'] = sampler.samples
        result['info_during_errors'] = sampler.errors
        result['info_after'] = infos()

    async def run():
        args = SimpleNamespace(**job['configuration'])
        warmup = SimpleNamespace(**dict(job['configuration'], requests=job['warmup_requests']))
        print('Running warmup: {} requests'.format(job['warmup_requests']), flush=True)
        result['warmup'] = await benchmark(warmup)
        if result['warmup']['errors']:
            raise RuntimeError('Warmup had client errors')
        print('Running measured workload: {} requests'.format(args.requests), flush=True)
        result['measurement'] = await benchmark(args, measurement_observer=observe)
        if result['measurement']['errors']:
            raise RuntimeError('Measured workload had client errors')
        expected = '1' if job['async_apply'] else '0'
        for state in (result['info_before'], result['info_after']):
            leader = state[str(job['leader'])]
            if leader['state'] != 'leader' or any(
                    info['leader_id'] != job['leader'] or info['term'] != leader['term'] or
                    info.get('async_apply') != expected for info in state.values()):
                raise RuntimeError('Nodes disagree about leadership or async_apply')
        result['cpu'] = cpu_summary(result['windows']['start'], result['windows']['end'])
        result['stages'] = stage_summary(result['info_before'][str(job['leader'])],
                                         result['info_after'][str(job['leader'])])
        if result['stages']['write_completed']['count'] != result['measurement']['writes_attempted']:
            raise RuntimeError('Write counter window differs from measured writes')
        result['info_summary'] = summarize_info_samples(result['info_during'])
        result['status'] = 'PASS'

    try:
        asyncio.run(run())
    except BaseException:
        result.update(status='FAIL', error=traceback.format_exc())
        if sampler._thread.is_alive():
            sampler.stop()
    write_json(Path(job_path).parent / 'client-report.json', result)
    return 0 if result['status'] == 'PASS' else 1


def _public_cell(cell, client, artifact):
    measured = client.get('measurement', {})
    return {'pipeline': cell['pipeline'], 'async_apply': cell['async_apply'], 'repeat': cell['repeat'],
            'status': client.get('status', 'FAIL'), 'artifact': str(artifact),
            'goodput_ops_per_second': measured.get('goodput_ops_per_second'),
            'errors': measured.get('errors'), 'errors_by_category': measured.get('errors_by_category'),
            'elapsed_seconds': measured.get('elapsed_seconds'),
            'batch_latency_ms': measured.get('batch_latency_ms'),
            'cpu': client.get('cpu'), 'stages': client.get('stages'),
            'info_summary': client.get('info_summary'),
            'info_during_errors': client.get('info_during_errors'),
            'error': client.get('error'), 'cleanup_error': client.get('cleanup_error')}


def run_cell(binary, artifacts, cell, requests, warmup_requests, timeout):
    cell_dir = artifacts / 'p{}-async-{}-r{}'.format(cell['pipeline'], int(cell['async_apply']), cell['repeat'])
    cell_dir.mkdir()
    configuration = dict(host='127.0.0.1', port=0, connections=32, requests=requests,
                         pipeline=cell['pipeline'], value_size=128, write_ratio=0.5, timeout=5,
                         namespace='applycmp', client_mode='classic')
    with tempfile.TemporaryDirectory(prefix='raft-kv-apply-cmp-') as data:
        cluster = Cluster(binary, Path(data), cell_dir, timeout)
        process = None
        client = {'status': 'FAIL'}
        try:
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=[
                    '--async_apply={}'.format('true' if cell['async_apply'] else 'false'),
                    '--group_commit_ms=1', '--snapshot_threshold=1024'])
            leader = cluster.wait_for('initial leader agreement', lambda: cluster.leader(range(3)))
            configuration['port'] = cluster.nodes[leader].client_port
            job = dict(configuration=configuration, leader=leader, deadline=cluster.deadline,
                       async_apply=cell['async_apply'], warmup_requests=warmup_requests, info_interval=1.0,
                       server_pids={str(node.node_id): node.process.pid for node in cluster.nodes},
                       ports={str(node.node_id): node.client_port for node in cluster.nodes})
            job_path = cell_dir / 'client-job.json'
            write_json(job_path, job)
            with (cell_dir / 'client.log').open('wb') as log:
                process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--worker', str(job_path)],
                                           stdout=log, stderr=subprocess.STDOUT)
                while process.poll() is None:
                    cluster.running(range(3))
                    if time.monotonic() >= cluster.deadline:
                        raise TimeoutError('Cell exceeded {} seconds'.format(timeout))
                    time.sleep(0.5)
            client = json.loads((cell_dir / 'client-report.json').read_text(encoding='utf-8'))
            if process.returncode or client['status'] != 'PASS':
                raise RuntimeError('Client failed; see {}'.format(cell_dir / 'client-report.json'))
            measured = client['measurement']
            expected = [('applycmp', 'run-{}-worker-{}'.format(measured['run_id'], index), b'x' * 128)
                        for index in range(measured['effective_connections'])]
            minimum = client['info_after'][str(leader)]['commit_index']
            cluster.wait_for('measured keys converge', lambda: cluster.convergence(expected, minimum))
        except BaseException:
            if client.get('status') != 'FAIL':
                client = dict(client, status='FAIL')
            client.setdefault('error', traceback.format_exc())
        finally:
            cleanup = None
            try:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
            except Exception:
                cleanup = traceback.format_exc()
            try:
                cluster.close()
            except Exception:
                cleanup = traceback.format_exc()
            if cleanup:
                if client.get('status') != 'FAIL':
                    client = dict(client, status='FAIL', error=cleanup)
                else:
                    client.setdefault('cleanup_error', cleanup)
        write_json(cell_dir / 'cell.json', _public_cell(cell, client, cell_dir))
        return _public_cell(cell, client, cell_dir)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report', type=Path)
    parser.add_argument('--artifacts', type=Path, default=Path('build-linux-reconnect/apply-compare'))
    parser.add_argument('--requests', type=int, default=20000)
    parser.add_argument('--warmup-requests', type=int, default=5000)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--cell-timeout', type=int, default=300)
    parser.add_argument('--worker', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        return worker(args.worker)
    if sys.platform != 'linux':
        parser.error('Requires Linux /proc')
    if not args.build_report or not 5000 <= args.requests <= 100000 or not 1 <= args.warmup_requests <= args.requests:
        parser.error('Provide --build-report; requests must be 5000..100000 and warmup must fit inside that')
    if not 1 <= args.repeats <= 3:
        parser.error('repeats must be 1..3')
    binary, inputs = validate_build(args.build_report.resolve())
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: ' + str(artifacts), flush=True)
    script_hash = file_hash(Path(__file__).resolve())
    mount_lines = Path('/proc/mounts').read_text(encoding='utf-8').splitlines()
    report = {
        'status': 'RUNNING', 'test': 'async_apply_compare',
        'note': 'One machine, one binary, sequential cells. Three repeats are this run only; '
                'do not treat a difference as a stable benefit. pipeline=1 latency and pipeline=16 '
                'batch latency are separate measurements. INFO polls during the timed window add the '
                'recorded sampling overhead and are not a heartbeat-delay measurement. '
                'RocksDB writes use DurableWriteOptions sync=true.',
        'identity': {'binary': {'path': str(binary), 'sha256': file_hash(binary)},
                     'compiled_inputs': inputs, 'script_sha256': script_hash,
                     'build_report_sha256': file_hash(args.build_report)},
        'environment': {'cpu_count': os.cpu_count(), 'affinity': sorted(os.sched_getaffinity(0)),
                        'kernel': os.uname().release,
                        'filesystem': longest_mount(artifacts, mount_lines),
                        'deployment': 'three servers and one Python load process on one machine, one cell at a time'},
        'configuration': {'connections': 32, 'requests': args.requests, 'warmup_requests': args.warmup_requests,
                          'value_size': 128, 'write_ratio': 0.5, 'pipelines': list(PIPELINES),
                          'repeats': args.repeats, 'group_commit_ms': 1, 'snapshot_threshold': 1024,
                          'linearizable_reads': False, 'info_interval_seconds': 1.0},
        'cells': [],
    }
    for cell in cell_sequence(args.repeats):
        label = 'pipeline={} async_apply={} repeat={}'.format(
            cell['pipeline'], cell['async_apply'], cell['repeat'])
        print('Starting ' + label, flush=True)
        outcome = run_cell(binary, artifacts, cell, args.requests, args.warmup_requests, args.cell_timeout)
        report['cells'].append(outcome)
        write_json(artifacts / 'report.json', report)
        print('{} {}'.format(outcome['status'], label), flush=True)
    if file_hash(binary) != report['identity']['binary']['sha256'] or any(
            file_hash(ROOT / path) != digest for path, digest in inputs.items()):
        report['status'] = 'FAIL'
        report['error'] = 'Binary or compiled source changed'
    elif file_hash(Path(__file__).resolve()) != script_hash:
        report['status'] = 'FAIL'
        report['error'] = 'Compare script changed during the run'
    elif any(cell['status'] != 'PASS' for cell in report['cells']):
        report['status'] = 'FAIL'
    else:
        report['status'] = 'PASS'
    write_json(artifacts / 'report.json', report)
    print('{}: async_apply comparison; report: {}'.format(report['status'], artifacts / 'report.json'))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
