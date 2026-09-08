"""Portable restart-test accounting checks; not a real Linux service run."""
from contextlib import contextmanager
import socket
import time
import unittest

from cluster_smoke import RespClient
from cluster_write_restart import ContinuousWriter, check_value, write_batch


def rows():
    return [dict(sequence=i, node=0, key='key-{}'.format(i), value='value-{}'.format(i)) for i in range(3)]


class Wire:
    def __init__(self, payload, send_error=None):
        self.payload = payload
        self.send_error = send_error
        self.sent = False
        self.after_send = False
    def settimeout(self, _):
        pass
    def sendall(self, payload):
        self.sent = True
        if self.send_error:
            raise self.send_error
    def recv(self, count):
        if not self.after_send:
            raise AssertionError('Read happened before after_send callback')
        payload, self.payload = self.payload[:count], self.payload[count:]
        return payload
    def close(self):
        pass


class BatchTests(unittest.TestCase):
    def run_batch(self, wire, batch):
        class FakeCluster:
            deadline = time.monotonic() + 10
            def client(self, node, io_timeout):
                return RespClient(wire, self.deadline, io_timeout)
        def after_send():
            self.assertTrue(wire.sent)
            wire.after_send = True
        return write_batch(FakeCluster(), 0, batch, after_send)

    def test_acknowledged_prefix_survives_eof_and_suffix_is_unknown(self):
        batch = rows()
        self.run_batch(Wire(b'+OK\r\n'), batch)
        self.assertEqual([r['outcome'] for r in batch], ['acknowledged', 'unknown', 'unknown'])
        self.assertTrue(all('sent_at' in row for row in batch))

    def test_complete_replies_and_redirection_are_recorded_in_order(self):
        batch = rows()
        target = self.run_batch(Wire(b'+OK\r\n-ERR MOVED 2\r\n+OK\r\n'), batch)
        self.assertEqual(target, 2)
        self.assertEqual([r['outcome'] for r in batch], ['acknowledged', 'rejected', 'acknowledged'])

    def test_send_failure_is_unknown_and_does_not_inject_fault(self):
        batch = rows()
        wire = Wire(b'', send_error=socket.timeout('partial send'))
        self.run_batch(wire, batch)
        self.assertFalse(wire.after_send)
        self.assertEqual([r['outcome'] for r in batch], ['unknown'] * 3)

    def test_connect_failure_is_not_sent(self):
        class Disconnected:
            deadline = time.monotonic() + 10
            def client(self, node, io_timeout):
                raise ConnectionRefusedError('server down')
        batch = rows()
        target = write_batch(Disconnected(), 0, batch, lambda: self.fail('Fault injected before send'))
        self.assertEqual(target, 1)
        self.assertEqual([r['outcome'] for r in batch], ['not_sent'] * 3)

    def test_unexpected_success_format_fails(self):
        with self.assertRaises(AssertionError):
            self.run_batch(Wire(b':1\r\n'), rows())

    def test_leadership_loss_error_is_unknown_not_rejection(self):
        batch = rows()
        self.run_batch(Wire(b'-ERR leadership lost; outcome unknown\r\n'
                            b'-ERR server stopped; outcome unknown\r\n-ERR BUSY write queue full\r\n'), batch)
        self.assertEqual([r['outcome'] for r in batch], ['unknown', 'unknown', 'rejected'])
        with self.assertRaises(AssertionError):
            self.run_batch(Wire(b'-ERR unknown command\r\n'), rows())


class RecoveryTests(unittest.TestCase):
    def test_recovery_rejects_missing_or_corrupt_acknowledged_data(self):
        row = dict(key='key', value='value', outcome='acknowledged')
        check_value(row, b'value')
        for value in (None, b'corrupt'):
            with self.assertRaises(AssertionError):
                check_value(row, value)
        row['outcome'] = 'unknown'
        for value in (None, b'value'):
            check_value(row, value)
        with self.assertRaises(AssertionError):
            check_value(row, b'corrupt')
        for outcome in ('rejected', 'not_sent'):
            row['outcome'] = outcome
            check_value(row, None)
            with self.assertRaises(AssertionError):
                check_value(row, b'value')

    def test_writer_fault_precedes_reply_accounting_and_keys_are_never_retried(self):
        class Node:
            calls = 0
            def stop(self, crash=False):
                if not crash:
                    raise AssertionError('Expected abrupt crash')
                self.calls += 1
        class FakeCluster:
            deadline = time.monotonic() + 10
            nodes = [Node(), Node(), Node()]
            @contextmanager
            def client(self, node, io_timeout):
                wire = Wire(b'+OK\r\n' * 8)
                # The individual ordering contract is checked by BatchTests.
                wire.after_send = True
                yield RespClient(wire, self.deadline, io_timeout)
        cluster = FakeCluster()
        writer = ContinuousWriter(cluster, 0)
        writer.crash_next_batch(0)
        writer.start()
        try:
            self.assertTrue(writer.fault_done.wait(timeout=2))
            until = time.monotonic() + 2
            while writer.totals()['acknowledged'] < 16 and time.monotonic() < until:
                writer.check()
                time.sleep(0.01)
        finally:
            writer.stop()
        self.assertEqual(cluster.nodes[0].calls, 1)
        self.assertEqual(writer.fault['acknowledged_before'], 0)
        self.assertGreaterEqual(writer.totals()['acknowledged'], 16)
        self.assertEqual(len({r['key'] for r in writer.rows}), len(writer.rows))
        for row in writer.rows[:8]:
            self.assertGreaterEqual(row['finished_at'], writer.fault['process_exited_at'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
