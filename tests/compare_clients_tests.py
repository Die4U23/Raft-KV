"""Reject incomparable rounds and test weighted throughput/CPU accounting."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from compare_clients import ORDER, main, summarize


def reports():
    return [dict(status='PASS', identity={'binary': 'same'}, environment={'cpu_count': 2}, leader=i % 3,
        client=dict(status='PASS', measurement=dict(status='COMPLETED', errors=0, success=100000,
            attempted=100000, elapsed_seconds=elapsed,
            configuration=dict(requests=100000, connections=32, pipeline=1, port=18080 + i, client_mode=mode),
            batch_latency_ms=dict(p99=20 + i)),
            cpu=dict(processes={'client': {'cpu_seconds': cpu}}, system_percent={'idle': 5})))
        for i, (mode, elapsed, cpu) in enumerate(zip(ORDER, (20, 10, 30, 40), (10, 5, 15, 20)))]


class CompareTests(unittest.TestCase):
    def test_weighted_summaries_preserve_per_round_quantiles(self):
        result = summarize(reports())
        self.assertAlmostEqual(result['aggregate']['classic']['goodput'], 200000 / 60)
        self.assertEqual(result['aggregate']['combined-header']['goodput'], 5000)
        self.assertEqual(result['aggregate']['classic']['client_cpu_us_per_op'], 150)
        self.assertEqual(result['aggregate']['combined-header']['client_cpu_us_per_op'], 100)
        self.assertAlmostEqual(result['goodput_change_percent'], 50)
        self.assertAlmostEqual(result['client_cpu_change_percent'], -100 / 3)
        self.assertEqual([r['latency_ms']['p99'] for r in result['rounds']], [20, 21, 22, 23])
        self.assertNotIn('latency_ms', result['aggregate']['classic'])

    def test_reject_error_drift_order_and_incomplete_rounds(self):
        changes = [('status', 'FAIL'), ('identity', {}), ('environment', {})]
        for key, value in changes:
            data = reports()
            data[2][key] = value
            with self.assertRaises(ValueError): summarize(data)
        for key, value in [('errors', 1), ('attempted', 99999), ('elapsed_seconds', 0)]:
            data = reports()
            data[1]['client']['measurement'][key] = value
            with self.assertRaises(ValueError): summarize(data)
        for key, value in [('client_mode', 'classic'), ('connections', 16)]:
            data = reports()
            data[1]['client']['measurement']['configuration'][key] = value
            with self.assertRaises(ValueError): summarize(data)
        with self.assertRaises(ValueError): summarize(reports()[:3])

    def run_driver(self, fail_at=None):
        data, calls = reports(), []
        def profile(argv):
            directory = Path(argv[argv.index('--artifacts') + 1]) / 'run-test'
            directory.mkdir(parents=True)
            index = len(calls)
            calls.append(argv[argv.index('--client-mode') + 1])
            report = data[index]
            if index == fail_at: report['status'] = 'FAIL'
            (directory / 'report.json').write_text(json.dumps(report), encoding='utf-8')
            return 1 if index == fail_at else 0
        with tempfile.TemporaryDirectory() as directory, patch('compare_clients.profile_main', profile), \
                patch('compare_clients.sys.platform', 'linux'), \
                patch('sys.argv', ['compare_clients.py', '--build-report', 'unused.json', '--artifacts', directory]), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = main()
            paths = list(Path(directory).glob('run-*/comparison.json'))
            self.assertEqual(len(paths), 1)
            result = json.loads(paths[0].read_text(encoding='utf-8'))
            self.assertTrue(all((paths[0].parent / p).is_file() for p in result['reports']))
        return code, calls, result

    def test_driver_runs_four_rounds_and_retains_reports(self):
        code, calls, result = self.run_driver()
        self.assertEqual((code, calls, result['status']), (0, list(ORDER), 'PASS'))
        self.assertEqual(len(result['reports']), 4)

    def test_driver_stops_at_first_failure(self):
        code, calls, result = self.run_driver(fail_at=1)
        self.assertEqual((code, calls, result['status']), (1, list(ORDER[:2]), 'FAIL'))
        self.assertEqual(len(result['reports']), 2)
        self.assertNotIn('summary', result)


if __name__ == '__main__':
    unittest.main(verbosity=2)
