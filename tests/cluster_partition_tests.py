"""Cross-platform helper tests with real loopback TCP; does not validate the Raft service."""
import socket
import socketserver
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import cluster_partition
from cluster_partition import assert_frozen, classify_reply, file_hash, probe_write, validate_build
from cluster_smoke import RespClient, RespError
from raft_proxy import RaftProxyMesh


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(2)
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    return
                self.request.sendall(data)
        except OSError:
            pass


class EchoServer(socketserver.ThreadingTCPServer):
    daemon_threads = True


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.server = EchoServer(('127.0.0.1', 0), Echo)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.mesh = RaftProxyMesh({i: self.server.server_address[1] for i in range(3)})

    def tearDown(self):
        self.mesh.close()
        self.assertFalse(self.mesh.thread.is_alive())
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)

    def connect(self, src, dst):
        return socket.create_connection(('127.0.0.1', self.mesh.ports[src, dst]), timeout=2)

    def echo(self, stream):
        payload = b'frame\x00\r\n' * 20000  # Exercise backpressure and binary forwarding.
        # Send/receive in bounded chunks to avoid blocking the echo peer's buffers.
        for offset in range(0, len(payload), 8192):
            chunk = payload[offset:offset + 8192]
            stream.sendall(chunk)
            reply = b''
            while len(reply) < len(chunk):
                received = stream.recv(len(chunk) - len(reply))
                self.assertTrue(received)
                reply += received
            self.assertEqual(reply, chunk)

    def assert_closed(self, stream):
        try:
            self.assertEqual(stream.recv(1), b'')
        except (ConnectionResetError, ConnectionAbortedError):
            pass

    def test_partition_cuts_existing_and_new_edges_preserves_majority_then_heals(self):
        with self.connect(0, 1) as forward, self.connect(1, 0) as reverse, self.connect(1, 2) as majority:
            for stream in (forward, reverse, majority):
                self.echo(stream)
            snapshot = self.mesh.partition([[0], [1, 2]])
            self.assertGreaterEqual(snapshot['events'][-1]['closed_connections'], 2)
            self.assert_closed(forward)
            self.assert_closed(reverse)
            with self.connect(0, 2) as refused:
                self.assert_closed(refused)
            self.echo(majority)
            self.mesh.partition([[0, 1, 2]])
            with self.connect(0, 1) as recovered:
                self.echo(recovered)
            self.assertEqual(self.mesh.snapshot()['errors'], [])

    def test_all_isolated_blocks_every_directed_edge(self):
        self.mesh.partition([[0], [1], [2]])
        for src, dst in self.mesh.ports:
            with self.connect(src, dst) as stream:
                self.assert_closed(stream)
        stats = self.mesh.snapshot()['edges']
        self.assertTrue(all(edge['refused'] >= 1 and edge['request_bytes'] == 0 for edge in stats.values()))

    def test_invalid_partition_does_not_change_connectivity(self):
        for groups in ([[0], [1]], [[0], [1, 1, 2]], [[0], [1, 3]]):
            with self.assertRaises(ValueError):
                self.mesh.partition(groups)
        with self.connect(0, 1) as stream:
            self.echo(stream)


class SafetyOracleTests(unittest.TestCase):
    def test_success_and_unexpected_replies_fail(self):
        for reply in ('OK', 1, None, b'OK'):
            with self.assertRaises(AssertionError):
                classify_reply(reply)
        self.assertEqual(classify_reply(RespError('ERR MOVED 1'))['outcome'], 'rejected')

    def test_commit_or_apply_progress_fails(self):
        baseline = dict(node_id=0, commit_index=8, last_applied=8)
        assert_frozen(baseline, dict(baseline))
        for field in ('commit_index', 'last_applied'):
            changed = dict(baseline, **{field: 9})
            with self.assertRaises(AssertionError):
                assert_frozen(baseline, changed)

    def test_only_reply_timeout_is_unknown_not_connection_failure(self):
        class Wire:
            def __init__(self, error):
                self.error = error
                self.sent = False
            def settimeout(self, timeout):
                pass
            def sendall(self, data):
                self.sent = True
            def recv(self, count):
                raise self.error
            def close(self):
                pass
        class FakeCluster:
            deadline = time.monotonic() + 10
            def client(self, node, io_timeout):
                return RespClient(wire, self.deadline, io_timeout)
        wire = Wire(socket.timeout('deadline'))
        outcome = probe_write(FakeCluster(), 0, 'key', 'value', 3)
        self.assertTrue(wire.sent)
        self.assertEqual(outcome['outcome'], 'unknown')
        wire = Wire(ConnectionResetError('disconnected'))
        with self.assertRaises(ConnectionResetError):
            probe_write(FakeCluster(), 0, 'key', 'value', 3)

    def test_build_identity_rejects_changed_binary_and_source_inventory(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for subdir in ('src', 'proto', 'third_party'):
                (root / subdir).mkdir()
            inputs = ['CMakeLists.txt', 'third_party/muduo.zip', 'src/main.cpp', 'proto/message.proto']
            for name in inputs:
                (root / name).write_bytes(b'original')
            binary = root / 'server'
            binary.write_bytes(b'binary')
            report = root / 'build-report.json'
            report.write_text(json.dumps(dict(status='PASS', linux_server_build='PASS',
                binary=dict(path=str(binary), sha256=file_hash(binary)),
                source_manifest_before={name: file_hash(root / name) for name in inputs})), encoding='utf-8')
            with patch.object(cluster_partition, 'ROOT', root), patch.object(cluster_partition.os, 'access', return_value=True):
                self.assertEqual(validate_build(report)[0], binary)
                binary.write_bytes(b'other')
                with self.assertRaises(AssertionError):
                    validate_build(report)
                binary.write_bytes(b'binary')
                (root / 'src/main.cpp').write_bytes(b'changed')
                with self.assertRaises(AssertionError):
                    validate_build(report)
                (root / 'src/main.cpp').write_bytes(b'original')
                (root / 'src/extra.cpp').write_bytes(b'extra')
                with self.assertRaises(AssertionError):
                    validate_build(report)
                (root / 'src/extra.cpp').unlink()
                (root / 'src/main.cpp').unlink()
                with self.assertRaises(AssertionError):
                    validate_build(report)


if __name__ == '__main__':
    unittest.main(verbosity=2)
