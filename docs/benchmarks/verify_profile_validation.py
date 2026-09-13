"""Read-only validation of the original normal-load diagnostic archive."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

from verify_ubuntu_abba import require
from verify_reconnect_validation import verify as verify_build, ARCHIVE as BUILD_ARCHIVE, BUILD

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]
ARCHIVE = ROOT / 'evidence/profile-validation-WLU34w.tar.gz'
SHA = 'c5e475de761152a666c5cc883072f37c083e73bee441c82c2e4c7cced6a91520'
COMMIT = '74880f392e10695e7f132cd021ef9f2d58df4724'
PREFIX = 'run-sqgv6xg2/'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify_report(r):
    c, m = r['client'], r['client']['measurement']
    require(r['status'] == c['status'] == 'PASS' and m['status'] == 'COMPLETED', 'Result status')
    require(r['test'] == 'aligned_performance_diagnostic' and r['leader'] == 0, 'Diagnostic identity')
    require(m['success'] == m['attempted'] == m['requested'] == 100000 and m['errors'] == 0, 'Request accounting')
    require(m['reads_attempted'] == m['writes_attempted'] == 50000, 'Read/write mix')
    require(m['completed_batches'] == m['latency_sample_count'] == 100000, 'Latency accounting')
    require(m['configuration']['connections'] == 32 and m['configuration']['pipeline'] == 1
            and m['configuration']['value_size'] == 128 and m['configuration']['write_ratio'] == 0.5, 'Load configuration')
    require(m['goodput_ops_per_second'] == m['success'] / m['elapsed_seconds'], 'Goodput calculation')
    require(c['warmup']['success'] == 10000 and c['warmup']['errors'] == 0, 'Warmup')
    first, last = c['windows']['start'], c['windows']['end']
    elapsed = last['at'] - first['at']
    require(elapsed > 0 and c['cpu']['window_seconds'] == elapsed, 'CPU window')
    require(set(first['processes']) == set(last['processes']) == {'0', '1', '2', 'client'}, 'CPU process inventory')
    for n, p in first['processes'].items():
        seconds = last['processes'][n]['cpu_seconds'] - p['cpu_seconds']
        require(seconds >= 0 and c['cpu']['processes'][n] ==
                dict(cpu_seconds=seconds, percent_of_one_core=100 * seconds / elapsed), 'CPU accounting')
    delta = {k: last['system'][k] - v for k, v in first['system'].items()}
    require(min(delta.values()) >= 0 and c['cpu']['system_percent'] ==
            {k: 100 * v / sum(delta.values()) for k, v in delta.items()}, 'System CPU accounting')
    require(len(r['samples']) == 46 and sum(first['at'] <= s['at'] <= last['at'] for s in r['samples']) ==
            r['measurement_sample_count'] == 38, 'External sample window')
    before, after = c['info_before']['0'], c['info_after']['0']
    require(before['state'] == after['state'] == 'leader' and before['term'] == after['term'] == 2, 'Leader stability')
    for name, stage in c['stages'].items():
        count = int(after[name + '_count']) - int(before[name + '_count'])
        total = int(after[name + '_total_us']) - int(before[name + '_total_us'])
        require(count > 0 and total >= 0 and stage['count'] == count and stage['mean_us'] == total / count, 'Stage mean')
        if 'mean_entries' in stage:
            require(stage['mean_entries'] == (int(after[name + '_entries']) - int(before[name + '_entries'])) / count,
                    'Batch size')
    require(c['stages']['write_completed']['count'] == 50000 and r['verified_keys_per_replica'] == 32, 'Write/key checks')
    require(set(r['last_info']) == {'0', '1', '2'} and len(r['nodes']) == 3, 'Final nodes')
    for info in r['last_info'].values():
        require(info['commit_index'] == info['last_applied'] == 55065 and info['term'] == 2
                and info['leader_id'] == 0 and int(info['pending_proposals']) == int(info['apply_lag']) == 0, 'Convergence')
    require(all(len(n['starts']) == 1 and n['starts'][0]['exit_code'] == -15 for n in r['nodes']), 'Process lifecycle')


def verify(archive_path=ARCHIVE):
    raw = archive_path.read_bytes()
    require(digest(raw) == SHA, 'Archive hash')
    blobs = {}
    with tarfile.open(archive_path) as t:
        members = t.getmembers()
        require(len(members) == len({m.name for m in members}) == 11, 'Archive inventory')
        for m in members:
            p = PurePosixPath(m.name)
            require(not p.is_absolute() and '..' not in p.parts and '\\' not in m.name, 'Archive path')
            require(m.name == PREFIX.rstrip('/') or m.name.startswith(PREFIX), 'Archive prefix')
            require(m.isdir() or (m.isfile() and m.size < 1024**2), 'Archive file type/size')
            if m.isfile(): blobs[m.name[len(PREFIX):]] = t.extractfile(m).read()
    require(len(blobs) == 10 and digest(blobs['report.json']) ==
            '163ba70e6e00634d753d7bd70c1042cad48cdd5bfbb860958938ced041730a41', 'Original report identity')
    r = json.loads(blobs['report.json'])
    verify_report(r)
    require(json.loads(blobs['client-report.json']) == r['client'], 'Separate client report')
    require(blobs['git-head.log'].decode().strip() == COMMIT and blobs['git-status.log'].decode().strip() ==
            '?? CMakeLists.txt.before-boost190-20260905-215703', 'Git identity')
    verify_build()
    with tarfile.open(BUILD_ARCHIVE) as t: build_raw = t.extractfile(BUILD + 'build-report.json').read()
    require(build_raw == blobs['referenced-build-report.json'] and digest(build_raw) ==
            r['identity']['build_report_sha256'], 'Verified build bytes')
    binary = json.loads(build_raw)['binary']
    require(all(binary[key] == r['identity']['binary'][key] for key in ('path', 'sha256')), 'Binary identity')
    inputs = dict(r['identity']['compiled_inputs'])
    inputs.update({'tests/' + p: h for p, h in r['identity']['test_source_hashes'].items()})
    require(len(inputs) == 33, 'Source count')
    for p, h in inputs.items():
        require(digest(subprocess.check_output(['git', 'show', COMMIT + ':' + p], cwd=REPO)) == h, 'Source hash: ' + p)
    for n in range(3): require(blobs['node-{}.log'.format(n)].count(b'--- start ') == 1, 'Log lifecycle')
    return dict(status='PASS', archive_sha256=SHA, archive_bytes=len(raw), test_commit=COMMIT,
        file_manifest=[dict(path=PREFIX + p, bytes=len(b), sha256=digest(b)) for p, b in sorted(blobs.items())],
        binary_sha256=r['identity']['binary']['sha256'], matched_sources=33, success=100000, errors=0,
        goodput=r['client']['measurement']['goodput_ops_per_second'],
        latency_ms=r['client']['measurement']['batch_latency_ms'], cpu=r['client']['cpu'], stages=r['client']['stages'],
        measured_samples=38, final_commit_and_applied_index=55065,
        limitation='One VM diagnostic; quantiles are reported, raw latency samples unavailable; not an optimization comparison')


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
