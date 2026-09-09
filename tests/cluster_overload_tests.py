"""Portable resource/admission-oracle checks; not a live Linux overload test."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from cluster_overload import FixedKeyLoad, assert_bounds, drained, parse_resources, resource_summary
from cluster_smoke import Node


def empty_info():
    return dict(connected_clients=1, queued_writes=0, queued_write_bytes=0, pending_proposals=0,
                pending_proposal_bytes=0, client_input_bytes=0, client_output_reserved_bytes=0,
                apply_lag=0, apply_inflight=0)


def samples():
    return [dict(phase='soak', at=float(i), process={str(node): dict(
        rss_bytes=64 * 1024 * 1024, cpu_seconds=i / 4, fd_count=20, threads=5) for node in range(3)})
        for i in range(60)]


class OverloadHelperTests(unittest.TestCase):
    def test_bounds_reject_overflow_and_negative_counters(self):
        info = empty_info()
        assert_bounds(info)
        for key, value in [('connected_clients', 33), ('pending_proposals', 1025),
                           ('queued_writes', 1025), ('pending_proposal_bytes', 16 * 1024 * 1024 + 1),
                           ('queued_write_bytes', -1), ('client_input_bytes', 64 * 1024 * 1024 + 1)]:
            with self.assertRaises(AssertionError):
                assert_bounds(dict(info, **{key: value}))

    def test_drain_requires_idle_monitor_only_and_zero_backlog(self):
        info = empty_info()
        self.assertTrue(drained(info))
        for key in info:
            self.assertFalse(drained(dict(info, **{key: info[key] + 1})), key)

    def test_proc_parser_handles_spaced_parenthesized_process_name(self):
        fields = ['S'] + ['0'] * 30
        fields[11], fields[12] = '250', '50'
        result = parse_resources('VmRSS:\t2048 kB\nThreads:\t5\n',
                                 '123 (worker (example)) ' + ' '.join(fields), 19, 100)
        self.assertEqual(result, dict(rss_bytes=2097152, cpu_seconds=3.0, threads=5, fd_count=19))

    def test_stable_resource_window_passes_and_reports_cpu_units(self):
        result = resource_summary(samples())
        self.assertEqual(result['0']['soak_cpu_percent_of_one_core'], 25.0)
        self.assertEqual(result['0']['rss_growth_bytes'], 0)
        self.assertEqual(result['0']['fd_growth'], 0)
        with self.assertRaises(AssertionError):
            resource_summary(samples()[:29])

    def test_memory_fd_growth_and_peak_memory_fail(self):
        for key, value in [('rss_bytes', 100 * 1024 * 1024), ('fd_count', 29)]:
            data = samples()
            for row in data[-10:]:
                row['process']['1'][key] = value
            with self.assertRaises(AssertionError):
                resource_summary(data)
        data = samples()
        peak = copy.deepcopy(data[0])
        peak['phase'] = 'pending_overload'
        peak['process']['2']['rss_bytes'] = 513 * 1024 * 1024
        with self.assertRaises(AssertionError):
            resource_summary([peak] + data)

    def test_fixed_key_load_does_not_grow_keyspace(self):
        pairs = {FixedKeyLoad.key_value(worker, index) for worker in range(2) for index in range(1000)}
        self.assertEqual(len(pairs), 128)
        self.assertEqual(FixedKeyLoad.key_value(1, 2), FixedKeyLoad.key_value(1, 66))

    def test_node_extra_arguments_preserve_paired_directories(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            node = Node(0, [12345, 12346], root, root)
            process = Mock(pid=123, returncode=0)
            process.poll.return_value = 0
            with patch('cluster_smoke.subprocess.Popen', return_value=process) as launch:
                node.start(root / 'server', '0:127.0.0.1:12346', extra_args=['--max_clients=32'])
                args = launch.call_args[0][0]
                self.assertEqual(args[-1], '--max_clients=32')
                self.assertIn('--db_path=' + str(root / 'node-0/kv'), args)
                self.assertIn('--raft_log_path=' + str(root / 'node-0/raft-log'), args)
                node.stop()
                node.start(root / 'server', '0:127.0.0.1:12346')
                self.assertFalse(any(arg.startswith('--max_clients=') for arg in launch.call_args[0][0]))
                node.stop()


if __name__ == '__main__':
    unittest.main(verbosity=2)
