"""Verify archived client-header ABBA evidence without executing archive contents."""
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

from verify_profile_validation import digest, REPO
from verify_reconnect_validation import verify as verify_build, ARCHIVE as BUILD_ARCHIVE, BUILD
from verify_ubuntu_abba import require

ROOT = Path(__file__).resolve().parent
ARCHIVE = ROOT / 'evidence/client-header-validation-wlZzuO.tar.gz'
SHA = '8bf02548a6deff9fab319694a4c02905e5f9e78257500cae8fb0c486d11931e2'
COMMIT = '653cc1400e0b67beeec3eecb97bfa46a639f06c7'
PREFIX = 'run-bq8q3abw/'
ORDER = ['classic', 'combined-header', 'combined-header', 'classic']
RUNS = ['round-1-classic/run-jnvy6v2b/', 'round-2-combined-header/run-oyiptpad/',
        'round-3-combined-header/run-ruua91ht/', 'round-4-classic/run-cjiof3u4/']
FILES = {'report.json', 'client-report.json', 'client-job.json', 'client.log', 'git-head.log',
         'git-status.log', 'referenced-build-report.json', 'node-0.log', 'node-1.log', 'node-2.log'}


def verify_round(r, mode):
    c, m = r['client'], r['client']['measurement']
    require(r['status'] == c['status'] == 'PASS' and m['status'] == 'COMPLETED', 'Round status')
    require(r['test'] == 'aligned_performance_diagnostic', 'Test identity')
    config = dict(m['configuration'])
    config.pop('port')
    require(config == dict(host='127.0.0.1', connections=32, requests=100000, pipeline=1,
        value_size=128, write_ratio=0.5, timeout=5, namespace='profile', client_mode=mode), 'Workload')
    require(m['requested'] == m['attempted'] == m['success'] == 100000 and m['errors'] == 0
            and all(v == 0 for v in m['errors_by_category'].values()), 'Request accounting')
    require(m['reads_attempted'] == m['writes_attempted'] == 50000 and m['effective_connections'] == 32, 'Mix')
    require(m['completed_batches'] == m['latency_sample_count'] == 100000, 'Latency sample count')
    require(m['elapsed_seconds'] > 0 and m['goodput_ops_per_second'] == 100000 / m['elapsed_seconds'], 'Goodput')
    warm = c['warmup']
    require(warm['success'] == warm['attempted'] == 10000 and warm['errors'] == 0
            and warm['configuration'] == dict(m['configuration'], requests=10000), 'Warmup')
    start, end = c['windows']['start'], c['windows']['end']
    elapsed = end['at'] - start['at']
    require(elapsed > 0 and c['cpu']['window_seconds'] == elapsed, 'CPU window')
    require(set(start['processes']) == set(end['processes']) == {'0', '1', '2', 'client'}, 'CPU processes')
    for name, first in start['processes'].items():
        seconds = end['processes'][name]['cpu_seconds'] - first['cpu_seconds']
        require(seconds >= 0 and c['cpu']['processes'][name] ==
                dict(cpu_seconds=seconds, percent_of_one_core=100 * seconds / elapsed), 'CPU delta')
    delta = {k: end['system'][k] - v for k, v in start['system'].items()}
    require(set(delta) == {'user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal'}
            and min(delta.values()) >= 0 and sum(delta.values()) > 0, 'System CPU fields')
    require(c['cpu']['system_percent'] == {k: 100 * v / sum(delta.values()) for k, v in delta.items()}, 'System CPU')
    require(sum(start['at'] <= s['at'] <= end['at'] for s in r['samples']) == r['measurement_sample_count'], 'Sample window')
    for state in (c['info_before'], c['info_after'], r['last_info']):
        require(set(state) == {'0', '1', '2'}, 'Node inventory')
        for key, info in state.items():
            require(info['node_id'] == int(key) and info['term'] == 2 and info['leader_id'] == r['leader']
                    and info['state'] == ('leader' if int(key) == r['leader'] else 'follower'), 'Leader stability')
    before, after = c['info_before'][str(r['leader'])], c['info_after'][str(r['leader'])]
    require(set(c['stages']) == {'write_queue_wait', 'leader_log_write', 'replication_data_ack',
            'kv_apply', 'apply_dispatch', 'write_completed', 'local_read'}, 'Stage inventory')
    for name, stage in c['stages'].items():
        count = int(after[name + '_count']) - int(before[name + '_count'])
        total = int(after[name + '_total_us']) - int(before[name + '_total_us'])
        require(count > 0 and total >= 0 and stage['count'] == count and stage['mean_us'] == total / count, 'Stage delta')
        if name in ('leader_log_write', 'kv_apply'):
            require(stage['mean_entries'] == (int(after[name + '_entries']) - int(before[name + '_entries'])) / count,
                    'Batch size')
    require(c['stages']['write_completed']['count'] == c['stages']['local_read']['count'] == 50000, 'Formal counters')
    require(r['verified_keys_per_replica'] == 32, 'Key verification')
    for info in r['last_info'].values():
        require(info['commit_index'] == info['last_applied'] == 55065 and
                all(int(info[k]) == 0 for k in ('queued_writes', 'pending_proposals', 'apply_lag', 'apply_inflight')), 'Convergence')
    require(len(r['nodes']) == 3 and {n['id'] for n in r['nodes']} == {0, 1, 2}, 'Process inventory')
    for n in r['nodes']:
        require(len(n['starts']) == 1 and n['starts'][0]['exit_code'] == -15, 'Process lifecycle')
        command = n['starts'][0]['command']
        require(command[0] == r['identity']['binary']['path'] and '--async_apply=true' in command
                and '--group_commit_ms=1' in command and '--leader_only_reads=false' in command, 'Server settings')


def verify_summary(comparison, reports):
    require(comparison['status'] == 'PASS' and comparison['test'] == 'client_header_abba'
            and comparison['order'] == ORDER, 'Comparison identity')
    require(comparison['reports'] == [p + 'report.json' for p in RUNS] and len(reports) == 4, 'Round inventory')
    rows, groups = [], {}
    for index, (mode, r) in enumerate(zip(ORDER, reports), 1):
        verify_round(r, mode)
        require(r['identity'] == reports[0]['identity'] and r['environment'] == reports[0]['environment'], 'Cross-round identity')
        c, m = r['client'], r['client']['measurement']
        cpu = c['cpu']['processes']['client']['cpu_seconds']
        rows.append(dict(round=index, mode=mode, leader=r['leader'], success=m['success'], errors=m['errors'],
            goodput=m['success'] / m['elapsed_seconds'], latency_ms=m['batch_latency_ms'],
            client_cpu_us_per_op=cpu * 1e6 / m['success'], system_percent=c['cpu']['system_percent']))
        g = groups.setdefault(mode, dict(success=0, elapsed_seconds=0, client_cpu_seconds=0))
        g['success'] += m['success']
        g['elapsed_seconds'] += m['elapsed_seconds']
        g['client_cpu_seconds'] += cpu
    for g in groups.values():
        g['goodput'] = g['success'] / g['elapsed_seconds']
        g['client_cpu_us_per_op'] = g['client_cpu_seconds'] * 1e6 / g['success']
    s = comparison['summary']
    require(s['rounds'] == rows and s['aggregate'] == groups, 'Summary recomputation')
    a, b = (groups[mode] for mode in ORDER[:2])
    require(s['goodput_change_percent'] == 100 * (b['goodput'] / a['goodput'] - 1)
            and s['client_cpu_change_percent'] == 100 * (b['client_cpu_seconds'] / a['client_cpu_seconds'] - 1), 'Change percentages')


def verify(archive_path=ARCHIVE):
    raw = archive_path.read_bytes()
    require(digest(raw) == SHA, 'Archive SHA256')
    blobs = {}
    with tarfile.open(archive_path) as t:
        members = t.getmembers()
        require(len(members) == len({m.name for m in members}) == 50, 'Archive inventory')
        for m in members:
            p = PurePosixPath(m.name)
            require(not p.is_absolute() and '..' not in p.parts and '\\' not in m.name and
                    (m.name == PREFIX.rstrip('/') or m.name.startswith(PREFIX)), 'Archive path')
            require(m.isdir() or (m.isfile() and m.size < 1024**2), 'Archive type/size')
            if m.isfile(): blobs[m.name[len(PREFIX):]] = t.extractfile(m).read()
    require(set(blobs) == {'comparison.json'} | {p + f for p in RUNS for f in FILES}, 'File inventory')
    require(digest(blobs['comparison.json']) == 'c3436a3cccae20762444227d3258bdef52ac815ada168f2f432d96fd758ef059', 'Original summary')
    comparison = json.loads(blobs['comparison.json'])
    reports = [json.loads(blobs[p + 'report.json']) for p in RUNS]
    verify_summary(comparison, reports)
    verify_build()
    with tarfile.open(BUILD_ARCHIVE) as t: build_raw = t.extractfile(BUILD + 'build-report.json').read()
    binary = json.loads(build_raw)['binary']
    identity = reports[0]['identity']
    require(all(identity['binary'][k] == binary[k] for k in ('path', 'sha256'))
            and identity['build_report_sha256'] == digest(build_raw), 'Verified binary/build identity')
    require(len(identity['compiled_inputs']) == 27 and len(identity['test_source_hashes']) == 6, 'Source inventory')
    inputs = dict(identity['compiled_inputs'])
    inputs.update({'tests/' + p: h for p, h in identity['test_source_hashes'].items()})
    inputs['tests/compare_clients.py'] = comparison['driver_sha256']
    for p, h in inputs.items():
        require(digest(subprocess.check_output(['git', 'show', COMMIT + ':' + p], cwd=REPO)) == h, 'Source hash: ' + p)
    for p, r in zip(RUNS, reports):
        require(blobs[p + 'referenced-build-report.json'] == build_raw, 'Build bytes')
        require(blobs[p + 'git-head.log'].decode().strip() == COMMIT and blobs[p + 'git-status.log'].decode().strip()
                == '?? CMakeLists.txt.before-boost190-20260905-215703', 'Git identity')
        require(identity['git-head-returncode'] == identity['git-status-returncode'] == 0, 'Git capture')
        require(json.loads(blobs[p + 'client-report.json']) == r['client'], 'Separate client report')
        job = json.loads(blobs[p + 'client-job.json'])
        require(job['configuration'] == r['client']['measurement']['configuration'] and job['leader'] == r['leader'], 'Job config')
        for n in r['nodes']:
            require(job['server_pids'][str(n['id'])] == n['starts'][0]['pid'] and job['ports'][str(n['id'])] == n['client_port'], 'Job processes')
            require(blobs[p + 'node-{}.log'.format(n['id'])].count(b'--- start ') == 1, 'Log lifecycle')
    return dict(status='PASS', archive_sha256=SHA, archive_bytes=len(raw), test_commit=COMMIT,
        file_manifest=[dict(path=PREFIX + p, bytes=len(b), sha256=digest(b)) for p, b in sorted(blobs.items())],
        matched_sources=len(inputs), binary_sha256=binary['sha256'], summary=comparison['summary'],
        measured_samples=[r['measurement_sample_count'] for r in reports],
        stages=[r['client']['stages'] for r in reports], final_commit_and_applied_index=55065,
        decision='Keep classic default; no demonstrated benefit in this run',
        limitation='Single same-VM ABBA with variation; quantiles copied from reports, raw latency samples unavailable')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
