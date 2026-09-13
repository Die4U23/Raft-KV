"""Bounded classic/combined-header ABBA using the same verified Linux server."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import traceback

from cluster_partition import file_hash
from profile_benchmark import main as profile_main, write_json

ORDER = ('classic', 'combined-header', 'combined-header', 'classic')


def summarize(reports):
    if len(reports) != len(ORDER):
        raise ValueError('ABBA requires four completed rounds')
    baseline = reports[0]
    reference = dict(baseline['client']['measurement']['configuration'])
    for key in ('port', 'client_mode'):
        reference.pop(key)
    rounds, grouped = [], {}
    for index, (mode, report) in enumerate(zip(ORDER, reports), 1):
        client, measurement = report['client'], report['client']['measurement']
        config = dict(measurement['configuration'])
        if config.pop('client_mode') != mode:
            raise ValueError('Client mode/order mismatch')
        config.pop('port')  # Each round reserves fresh local TCP ports.
        if config != reference or report['identity'] != baseline['identity'] or report['environment'] != baseline['environment']:
            raise ValueError('Workload, build, helper sources or environment changed across rounds')
        if (report['status'] != 'PASS' or client['status'] != 'PASS' or measurement['status'] != 'COMPLETED'
                or measurement['errors'] or measurement['success'] != reference['requests']
                or measurement['attempted'] != measurement['success']):
            raise ValueError('A round failed or has incomplete/error requests')
        seconds = measurement['elapsed_seconds']
        cpu_seconds = client['cpu']['processes']['client']['cpu_seconds']
        if seconds <= 0 or cpu_seconds < 0:
            raise ValueError('Invalid time accounting')
        rounds.append(dict(round=index, mode=mode, leader=report['leader'], success=measurement['success'],
            errors=measurement['errors'], goodput=measurement['success'] / seconds,
            latency_ms=measurement['batch_latency_ms'], client_cpu_us_per_op=cpu_seconds * 1e6 / measurement['success'],
            system_percent=client['cpu']['system_percent']))
        group = grouped.setdefault(mode, dict(success=0, elapsed_seconds=0, client_cpu_seconds=0))
        group['success'] += measurement['success']
        group['elapsed_seconds'] += seconds
        group['client_cpu_seconds'] += cpu_seconds
    for group in grouped.values():
        group['goodput'] = group['success'] / group['elapsed_seconds']
        group['client_cpu_us_per_op'] = group['client_cpu_seconds'] * 1e6 / group['success']
    classic, combined = (grouped[mode] for mode in ORDER[:2])
    return dict(rounds=rounds, aggregate=grouped,
        goodput_change_percent=100 * (combined['goodput'] / classic['goodput'] - 1),
        client_cpu_change_percent=(100 * (combined['client_cpu_seconds'] / classic['client_cpu_seconds'] - 1)
                                   if classic['client_cpu_seconds'] else None),
        limitation='One same-VM ABBA; no pooled latency quantile and no independent server capacity claim')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-report', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, default=Path('build-linux-reconnect/client-comparison'))
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('Requires Linux; use compare_clients_tests.py for helper checks')
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    source = Path(__file__).resolve()
    result = dict(status='RUNNING', test='client_header_abba', order=ORDER,
                  driver_sha256=file_hash(source), reports=[])
    print('Comparison artifacts: ' + str(artifacts), flush=True)
    try:
        reports = []
        for index, mode in enumerate(ORDER, 1):
            directory = artifacts / ('round-{}-{}'.format(index, mode))
            print('Round {}/4: {}'.format(index, mode), flush=True)
            code = profile_main(['--build-report', str(args.build_report.resolve()), '--artifacts', str(directory),
                                 '--requests', '100000', '--client-mode', mode])
            paths = list(directory.glob('run-*/report.json'))
            result['reports'].extend(str(p.relative_to(artifacts)) for p in paths)
            if code != 0 or len(paths) != 1:
                raise RuntimeError('Round {} failed; stop comparison and inspect its logs'.format(index))
            reports.append(json.loads(paths[0].read_text(encoding='utf-8')))
        result['summary'] = summarize(reports)
        if file_hash(source) != result['driver_sha256']:
            raise RuntimeError('Comparison driver changed during execution')
        result['status'] = 'PASS'
    except BaseException:
        result.update(status='FAIL', error=traceback.format_exc())
    write_json(artifacts / 'comparison.json', result)
    print('{}: client header ABBA; report: {}'.format(result['status'], artifacts / 'comparison.json'))
    if result['status'] == 'PASS':
        print(json.dumps(result['summary'], ensure_ascii=False, indent=2))
    else:
        print(result['error'], file=sys.stderr)
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
