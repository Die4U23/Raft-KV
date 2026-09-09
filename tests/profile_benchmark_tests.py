"""Diagnostic accounting tests; never represent Linux service performance."""
import argparse
import asyncio
import copy
import unittest
from unittest.mock import patch

from profile_benchmark import cpu_summary, stage_summary
from load_benchmark import benchmark


class SummaryTests(unittest.TestCase):
    def test_cpu_units_and_guest_exclusion(self):
        start = dict(at=10, processes={'client': {'cpu_seconds': 2}, '0': {'cpu_seconds': 5}},
                     system=dict(user=100, nice=0, system=50, idle=200, iowait=0, irq=0, softirq=0, steal=0))
        end = copy.deepcopy(start)
        end['at'] = 12
        end['processes']['client']['cpu_seconds'] = 3
        end['processes']['0']['cpu_seconds'] = 5.5
        end['system']['user'] += 100
        end['system']['idle'] += 300
        result = cpu_summary(start, end)
        self.assertEqual(result['processes']['client']['percent_of_one_core'], 50)
        self.assertEqual(result['processes']['0']['percent_of_one_core'], 25)
        self.assertEqual(result['system_percent']['idle'], 75)
        end['system']['iowait'] = -1
        self.assertIsNone(cpu_summary(start, end)['system_percent'])
        end['processes']['client']['cpu_seconds'] = 0
        with self.assertRaises(ValueError):
            cpu_summary(start, end)

    def test_stage_deltas_not_difference_of_averages(self):
        names = ('write_queue_wait', 'leader_log_write', 'replication_data_ack', 'kv_apply',
                 'apply_dispatch', 'write_completed', 'local_read')
        before = dict(node_id=1, term=3, state='leader')
        for n in names:
            before.update({n + '_count': '10', n + '_total_us': '1000'})
        after = dict(before, leader_log_write_count='12', leader_log_write_total_us='1600')
        before['leader_log_write_entries'] = '40'
        after['leader_log_write_entries'] = '50'
        result = stage_summary(before, after)
        self.assertEqual(result['leader_log_write'], dict(count=2, mean_us=300, mean_entries=5))
        self.assertIsNone(result['local_read']['mean_us'])
        after['term'] = 4
        with self.assertRaises(ValueError):
            stage_summary(before, after)


class ObserverTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_start_observer_closes_preloaded_streams(self):
        closed = []
        class Writer:
            def close(self): closed.append(self)
            async def wait_closed(self): pass
        async def connect(*_a, **_kw): return object(), Writer()
        async def one(_r, _w, payload, _timeout):
            return b'state:leader\r\nleader_id:0\r\n' if b'INFO' in payload else b'OK'
        def fail(_event): raise RuntimeError('sampling failed')
        args = argparse.Namespace(host='unused', port=1, connections=2, requests=4, pipeline=1,
                                  value_size=8, write_ratio=1, timeout=1, namespace='profile')
        with patch('load_benchmark.asyncio.open_connection', connect), patch('load_benchmark.one', one):
            with self.assertRaisesRegex(RuntimeError, 'sampling failed'):
                await benchmark(args, fail)
        self.assertEqual(len(closed), 3)  # INFO connection plus both preloaded connections.

    async def test_observer_brackets_workload_after_preload_before_cleanup(self):
        events, writes = [], []
        class Writer:
            def close(self): events.append('close')
            async def wait_closed(self): pass
        async def connect(*_a, **_kw): return object(), Writer()
        async def one(_r, _w, payload, _timeout):
            if b'INFO' in payload: return b'state:leader\r\nleader_id:0\r\n'
            if b'SET' in payload: events.append('preload')
            return b'OK'
        async def exchange(_r, _w, payload, count, replies):
            self.assertIn('start', events)
            self.assertNotIn('end', events)
            writes.append(payload)
            replies.extend([b'OK'] * count)
        def observer(event):
            if event == 'start': self.assertEqual(events.count('preload'), 2)
            if event == 'end': self.assertEqual(len(writes), 4)
            events.append(event)
        args = argparse.Namespace(host='unused', port=1, connections=2, requests=4, pipeline=1,
                                  value_size=8, write_ratio=1, timeout=1, namespace='profile')
        with patch('load_benchmark.asyncio.open_connection', connect), patch('load_benchmark.one', one), \
                patch('load_benchmark.exchange', exchange):
            result = await benchmark(args, observer)
        self.assertEqual((result['success'], result['errors']), (4, 0))
        self.assertEqual(events[-3:], ['end', 'close', 'close'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
