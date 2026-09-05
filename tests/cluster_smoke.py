#!/usr/bin/env python3
"""Real three-process Linux smoke test; standard library only.

Run --self-test to check only the RESP helpers without starting a server.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import unittest


class RespError:
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "RespError({!r})".format(self.message)


def encode_command(*args):
    parts = [arg if isinstance(arg, bytes) else str(arg).encode("utf-8")
             for arg in args]
    return ("*{}\r\n".format(len(parts)).encode("ascii") +
            b"".join(b"$" + str(len(part)).encode("ascii") +
                     b"\r\n" + part + b"\r\n" for part in parts))


class RespClient:
    """Persistent, buffered RESP2 connection with a deadline per request/batch."""

    def __init__(self, sock, deadline, io_timeout=2.0):
        self.sock = sock
        self.deadline = deadline
        self.io_timeout = io_timeout
        self.buffer = bytearray()
        self.request_deadline = deadline

    @classmethod
    def connect(cls, port, deadline, io_timeout=2.0):
        remaining = min(io_timeout, deadline - time.monotonic())
        if remaining <= 0:
            raise TimeoutError("cluster deadline exceeded")
        sock = socket.create_connection(("127.0.0.1", port), timeout=remaining)
        return cls(sock, deadline, io_timeout)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.sock.close()

    def _arm(self):
        remaining = min(self.deadline, self.request_deadline) - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("RESP request deadline exceeded")
        self.sock.settimeout(remaining)

    def _fill(self):
        self._arm()
        chunk = self.sock.recv(4096)
        if not chunk:
            raise ConnectionError("EOF before a complete RESP reply")
        self.buffer.extend(chunk)
        if len(self.buffer) > 8 * 1024 * 1024:
            raise ValueError("RESP reply buffer exceeds smoke-test limit")

    def _line(self):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                value = bytes(self.buffer[:end])
                del self.buffer[:end + 2]
                return value
            self._fill()

    def _take(self, count):
        while len(self.buffer) < count:
            self._fill()
        value = bytes(self.buffer[:count])
        del self.buffer[:count]
        return value

    def read_reply(self, depth=0):
        if depth > 32:
            raise ValueError("RESP nesting exceeds smoke-test limit")
        prefix = self._take(1)
        line = self._line()
        if prefix == b"+":
            return line.decode("utf-8")
        if prefix == b"-":
            return RespError(line.decode("utf-8", errors="replace"))
        if prefix == b":":
            return int(line)
        if prefix in (b"$", b"*"):
            count = int(line)
            if count == -1:
                return None
            if count < 0 or count > 4 * 1024 * 1024:
                raise ValueError("invalid RESP length")
            if prefix == b"*":
                return [self.read_reply(depth + 1) for _ in range(count)]
            value = self._take(count)
            if self._take(2) != b"\r\n":
                raise ValueError("invalid bulk-string terminator")
            return value
        raise ValueError("unsupported RESP prefix {!r}".format(prefix))

    def pipeline(self, commands):
        self.request_deadline = min(self.deadline, time.monotonic() + self.io_timeout)
        self._arm()
        self.sock.sendall(b"".join(encode_command(*args) for args in commands))
        return [self.read_reply() for _ in commands]

    def command(self, *args):
        return self.pipeline([args])[0]

    def fragmented_ping(self):
        self.request_deadline = min(self.deadline, time.monotonic() + self.io_timeout)
        payload = encode_command("PING")
        # Force writes across the array header, bulk header, payload and CRLF.
        for start, end in ((0, 1), (1, 6), (6, 10), (10, len(payload))):
            self._arm()
            self.sock.sendall(payload[start:end])
            time.sleep(0.02)
        return self.read_reply()


def expect(actual, expected, label):
    if actual != expected:
        raise AssertionError("{}: expected {!r}, got {!r}".format(label, expected, actual))


def parse_info(reply):
    if not isinstance(reply, bytes):
        raise AssertionError("INFO did not return a bulk string: {!r}".format(reply))
    fields = dict(line.split(":", 1) for line in reply.decode("utf-8").splitlines()
                  if ":" in line)
    for name in ("node_id", "leader_id", "term", "commit_index", "last_applied"):
        fields[name] = int(fields[name])
    return fields


class Node:
    def __init__(self, node_id, ports, data_root, artifacts):
        self.node_id = node_id
        self.client_port, self.raft_port = ports
        self.data = data_root / "node-{}".format(node_id)
        self.data.mkdir()
        self.log_path = artifacts / "node-{}.log".format(node_id)
        self.process = None
        self.log = None
        self.starts = []

    def start(self, binary, peers):
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError("node is already running")
        command = [str(binary), "--node_id={}".format(self.node_id),
                   "--client_port={}".format(self.client_port),
                   "--raft_port={}".format(self.raft_port),
                   "--db_path={}".format(self.data / "kv"),
                   "--raft_log_path={}".format(self.data / "raft-log"),
                   "--peers=" + peers, "--leader_only_reads=false", "--logtostderr=true"]
        self.log = self.log_path.open("ab", buffering=0)
        self.log.write(("\n--- start {} ---\n".format(time.time())).encode("ascii"))
        try:
            self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT)
        except BaseException:
            self.log.close()
            self.log = None
            raise
        self.starts.append({"pid": self.process.pid, "command": command})

    def stop(self, crash=False):
        if self.process is not None:
            if self.process.poll() is None:
                if crash:
                    self.process.kill()  # SIGKILL on Linux: simulate abrupt leader loss.
                else:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=3)
            if self.starts:
                self.starts[-1]["exit_code"] = self.process.returncode
        if self.log is not None:
            self.log.close()
            self.log = None


class Cluster:
    def __init__(self, binary, data_root, artifacts, timeout):
        self.binary = binary
        self.deadline = time.monotonic() + timeout
        self.nodes = []
        self.reservations = []
        self.report = {"status": "RUNNING", "steps": [], "last_info": {}}
        # Hold all reservations until starting their respective node. No fixed ports.
        for _ in range(6):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.reservations.append(sock)
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in self.reservations]
        for node_id in range(3):
            self.nodes.append(Node(node_id, ports[2 * node_id:2 * node_id + 2],
                                   data_root, artifacts))
        self.peers = ",".join("{}:127.0.0.1:{}".format(node.node_id, node.raft_port)
                              for node in self.nodes)

    def client(self, node_id, io_timeout=2.0):
        return RespClient.connect(self.nodes[node_id].client_port, self.deadline, io_timeout)

    def step(self, name):
        self.report["steps"].append(name)
        print("PASS: " + name, flush=True)

    def running(self, node_ids):
        for node_id in node_ids:
            process = self.nodes[node_id].process
            if process is None or process.poll() is not None:
                raise RuntimeError("node {} unexpectedly exited ({})".format(
                    node_id, None if process is None else process.returncode))

    def wait_for(self, name, check):
        last = "condition not yet met"
        while time.monotonic() < self.deadline:
            try:
                result = check()
                if result is not None:
                    return result
            except (OSError, TimeoutError) as error:
                last = repr(error)
            time.sleep(min(0.10, max(0, self.deadline - time.monotonic())))
        raise TimeoutError("{}: {}; last INFO: {}".format(
            name, last, self.report["last_info"]))

    def info(self, node_id):
        with self.client(node_id, io_timeout=0.5) as client:
            fields = parse_info(client.command("INFO"))
        expect(fields["node_id"], node_id, "INFO node identity")
        self.report["last_info"][str(node_id)] = fields
        return fields

    def leader(self, node_ids):
        self.running(node_ids)
        states = [self.info(node_id) for node_id in node_ids]
        leaders = [state for state in states if state["state"] == "leader"]
        if len(leaders) == 1:
            leader = leaders[0]
            if all(state["leader_id"] == leader["node_id"] and
                   state["term"] == leader["term"] and
                   state["state"] == ("leader" if state["node_id"] == leader["node_id"]
                                      else "follower") for state in states):
                return leader["node_id"]
        return None

    def convergence(self, expected, minimum_index):
        self.running(range(3))
        for node in self.nodes:
            with self.client(node.node_id, io_timeout=0.5) as client:
                for namespace, key, value in expected:
                    expect(client.command("SELECT", namespace), "OK", "SELECT on convergence")
                    if client.command("GET", key) != value:
                        return None
                info = parse_info(client.command("INFO"))
                self.report["last_info"][str(node.node_id)] = info
                if min(info["commit_index"], info["last_applied"]) < minimum_index:
                    return None
        return True

    def run(self):
        for node in self.nodes:
            for reservation in self.reservations[2 * node.node_id:2 * node.node_id + 2]:
                reservation.close()
            node.start(self.binary, self.peers)
        leader = self.wait_for("initial election", lambda: self.leader(range(3)))
        self.report["initial_leader"] = leader
        self.step("three nodes agree on one leader")

        original = b"before\x00failure\r\nvalue"
        with self.client(leader) as client:
            expect(client.fragmented_ping(), "PONG", "fragmented PING")
            expect(client.pipeline([("SET", "shared", original), ("GET", "shared")]),
                   ["OK", original], "SET/GET pipeline response and apply order")
            self.step("fragmented PING and binary SET/GET pipeline")
            expect(client.pipeline([("PING",)] * 257 + [("GET", "shared")]),
                   ["PONG"] * 257 + [original], "pipeline across scheduled drain turns")
            self.step("pipeline spans multiple 128-command event-loop turns")
            expect(client.command("SELECT", "smoke_a"), "OK", "SELECT smoke_a")
            expect(client.command("SET", "shared", "value-a"), "OK", "SET smoke_a")
            expect(client.command("SELECT", "smoke_b"), "OK", "SELECT smoke_b")
            expect(client.command("GET", "shared"), None, "namespace isolation")
            expect(client.command("SET", "shared", "value-b"), "OK", "SET smoke_b")
            expect(client.command("SELECT", "smoke_a"), "OK", "return to smoke_a")
            expect(client.command("GET", "shared"), b"value-a", "smoke_a retained value")
            expect(client.command("SELECT", "default"), "OK", "return to default")
            expect(client.command("GET", "shared"), original, "default retained value")
            self.step("namespace isolation and switching on one persistent connection")
            expect(client.command("DEL", "absent"), 0, "DEL absent key")
            expect(client.command("SET", "deleted", "temporary"), "OK", "SET delete target")
            expect(client.command("DEL", "deleted"), 1, "DEL existing key")
            expect(client.command("DEL", "deleted"), 0, "DEL already deleted key")
            committed = parse_info(client.command("INFO"))["commit_index"]
            self.step("DEL absent=0, present=1, then absent=0")

        follower = next(node_id for node_id in range(3) if node_id != leader)
        with self.client(follower) as client:
            expect(parse_info(client.command("INFO"))["namespace"], "default",
                   "fresh connection namespace")
            reply = client.command("SET", "rejected", "never-applied")
            if not isinstance(reply, RespError) or not reply.message.startswith("ERR MOVED "):
                raise AssertionError("follower SET should return MOVED, got {!r}".format(reply))
        self.step("follower rejects writes with a RESP error")

        expected = [("default", "shared", original), ("default", "deleted", None),
                    ("default", "rejected", None), ("smoke_a", "shared", b"value-a"),
                    ("smoke_b", "shared", b"value-b")]
        def concurrent_write(index):
            key = "parallel-{}".format(index)
            with self.client(leader, io_timeout=5) as client:
                expect(client.pipeline([("SET", key, "parallel-value"), ("GET", key)]),
                       ["OK", b"parallel-value"], "concurrent SET/GET order")
            return ("default", key, b"parallel-value")
        with ThreadPoolExecutor(max_workers=32) as workers:
            expected.extend(workers.map(concurrent_write, range(32)))
        committed = self.info(leader)["commit_index"]
        self.step("32 concurrent connections complete ordered writes and reads")
        self.wait_for("pre-failure replication", lambda: self.convergence(expected, committed))
        self.nodes[leader].stop(crash=True)
        self.step("leader killed abruptly")
        survivors = [node_id for node_id in range(3) if node_id != leader]
        new_leader = self.wait_for("election after leader loss", lambda: self.leader(survivors))
        self.report["failover_leader"] = new_leader
        with self.client(new_leader) as client:
            expect(client.command("GET", "shared"), original, "committed value after failover")
            expect(client.command("SET", "failover", "after-failure"), "OK", "write after failover")
            expect(client.command("GET", "failover"), b"after-failure", "read after failover write")
            committed = parse_info(client.command("INFO"))["commit_index"]
        self.step("survivors elect a new leader and acknowledge a new write")

        # Reuse the Node object and precisely the same paired kv / raft-log paths.
        self.nodes[leader].start(self.binary, self.peers)
        expected.append(("default", "failover", b"after-failure"))
        self.wait_for("restart state convergence", lambda: self.convergence(expected, committed))
        self.report["final_leader"] = self.wait_for("agreement after restart",
                                                    lambda: self.leader(range(3)))
        self.report["minimum_applied_index"] = committed
        self.step("old leader restarts from paired directories; all three nodes converge")

    def close(self):
        errors = []
        for node in self.nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append("node {} cleanup: {!r}".format(node.node_id, error))
        for reservation in self.reservations:
            reservation.close()
        self.report["nodes"] = [{"id": node.node_id, "client_port": node.client_port,
                                 "raft_port": node.raft_port, "starts": node.starts,
                                 "log": str(node.log_path)} for node in self.nodes]
        if errors:
            raise RuntimeError("; ".join(errors))


class RespHelperTests(unittest.TestCase):
    class Wire:
        def __init__(self, payload, chunk_size):
            self.payload = payload
            self.chunk_size = chunk_size
            self.sent = b""

        def settimeout(self, _):
            pass

        def recv(self, count):
            size = min(count, self.chunk_size)
            chunk, self.payload = self.payload[:size], self.payload[size:]
            return chunk

        def sendall(self, data):
            self.sent += data

    def test_binary_encoding(self):
        self.assertEqual(encode_command("SET", "键", b"a\x00\r\nb"),
                         b"*3\r\n$3\r\nSET\r\n$3\r\n\xe9\x94\xae\r\n$5\r\na\x00\r\nb\r\n")

    def test_fragmented_and_coalesced_replies(self):
        payload = (b"*6\r\n+OK\r\n:42\r\n$5\r\na\x00\r\nb\r\n$-1\r\n"
                   b"-ERR MOVED 2\r\n*2\r\n$0\r\n\r\n*-1\r\n+PONG\r\n")
        for chunk_size in (1, 2, 7, 4096):
            with self.subTest(chunk_size=chunk_size):
                wire = self.Wire(payload, chunk_size)
                client = RespClient(wire, time.monotonic() + 5)
                replies = client.pipeline([("INFO",), ("PING",)])
                self.assertEqual(replies[0][:4], ["OK", 42, b"a\x00\r\nb", None])
                self.assertEqual(replies[0][4].message, "ERR MOVED 2")
                self.assertEqual(replies[0][5], [b"", None])
                self.assertEqual(replies[1], "PONG")
                self.assertEqual(wire.sent, encode_command("INFO") + encode_command("PING"))

    def test_incomplete_and_invalid_replies(self):
        for payload in (b"$3\r\na", b"$1\r\naXX", b"$-2\r\n", b"?bad\r\n"):
            with self.subTest(payload=payload):
                client = RespClient(self.Wire(payload, 2), time.monotonic() + 5)
                with self.assertRaises((ConnectionError, ValueError)):
                    client.read_reply()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", "--binary", dest="binary", type=Path,
                        default=Path(__file__).resolve().parents[1] / "build" / "raft_kv_server")
    parser.add_argument("--timeout", type=float, default=90,
                        help="whole-cluster deadline in seconds, excluding bounded cleanup (default: 90)")
    parser.add_argument("--artifacts", type=Path, default=Path("build/cluster-smoke"),
                        help="parent for a fresh run directory containing logs and report.json")
    parser.add_argument("--self-test", action="store_true", help="test RESP helpers only; no server required")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(RespHelperTests))
        return 0 if result.wasSuccessful() else 1
    if sys.platform != "linux":
        parser.error("the real cluster test requires Linux; --self-test runs on other platforms")
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(str(binary), os.X_OK):
        parser.error("server binary is missing or not executable: {}".format(binary))
    if not 0 < args.timeout <= 3600:
        parser.error("--timeout must be greater than 0 and at most 3600")
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix="run-", dir=str(args.artifacts.resolve())))
    print("Artifacts: {}".format(artifacts), flush=True)
    with tempfile.TemporaryDirectory(prefix="raft-kv-cluster-smoke-") as data:
        cluster = Cluster(binary, Path(data), artifacts, args.timeout)
        started = time.monotonic()
        try:
            cluster.run()
            cluster.report["status"] = "PASS"
        except BaseException:
            cluster.report["status"] = "FAIL"
            cluster.report["error"] = traceback.format_exc()
        finally:
            try:
                cluster.close()
            except Exception:
                cluster.report["status"] = "FAIL"
                cluster.report["cleanup_error"] = traceback.format_exc()
            cluster.report["elapsed_seconds"] = round(time.monotonic() - started, 3)
            (artifacts / "report.json").write_text(
                json.dumps(cluster.report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        if cluster.report["status"] != "PASS":
            print(cluster.report.get("error", cluster.report.get("cleanup_error", "FAIL")), file=sys.stderr)
            for node in cluster.nodes:
                if node.log_path.exists():
                    with node.log_path.open("rb") as log:
                        log.seek(max(0, node.log_path.stat().st_size - 8192))
                        tail = log.read().decode("utf-8", errors="replace")
                    print("--- {} (last 8 KiB) ---\n{}".format(node.log_path, tail), file=sys.stderr)
            print("FAIL: diagnostic logs and report retained in {}".format(artifacts), file=sys.stderr)
            return 1
    print("PASS: real Linux three-node smoke test; report: {}".format(artifacts / "report.json"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
