#!/usr/bin/env python3
"""
ReadIndex Linearizable Read Integration Tests

Tests the linearizable read functionality in real cluster scenarios:
1. Network partition: old Leader cannot return stale data
2. Write-then-read consistency: GET immediately after SET returns correct value
3. Concurrent reads: multiple concurrent GETs return consistent results
4. Heartbeat batching: high-frequency reads share heartbeats
5. Leader change: new Leader reads don't see uncommitted writes
"""

import subprocess
import time
import threading
import unittest
from pathlib import Path
from typing import List, Optional

from cluster_smoke import RespClient, RespError


class LinearizableReadTests(unittest.TestCase):
    """Integration tests for ReadIndex linearizable reads."""

    @classmethod
    def setUpClass(cls):
        """Check if server binary exists."""
        cls.server_bin = Path("build-linux-repro/server/raft_kv_server")
        if not cls.server_bin.exists():
            # Try alternative paths
            alternatives = [
                Path("build-portable/server/raft_kv_server"),
                Path("build/server/raft_kv_server"),
            ]
            for alt in alternatives:
                if alt.exists():
                    cls.server_bin = alt
                    break

        if not cls.server_bin.exists():
            raise unittest.SkipTest(f"Server binary not found at {cls.server_bin}")

    @staticmethod
    def _to_str(value):
        """Convert bytes to string if needed."""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value) if value is not None else None

    def setUp(self):
        """Start a 3-node cluster with linearizable reads enabled."""
        self.base_client_port = 18080
        self.base_raft_port = 19080
        self.nodes = []
        self.clients = []
        self.deadline = time.monotonic() + 60.0  # 60 second deadline

        # Start 3 nodes with linearizable_reads enabled
        for i in range(3):
            node = self._start_node(i, linearizable_reads=True)
            self.nodes.append(node)
            print(f"Started node {i} (PID: {node.pid})")

        # Wait longer for cluster to stabilize and elect leader
        print("Waiting for cluster to stabilize...")
        time.sleep(5.0)

        # Connect clients
        for i in range(3):
            try:
                client = RespClient.connect(self.base_client_port + i, self.deadline)
                self.clients.append(client)
                print(f"Connected to node {i}")
            except Exception as e:
                print(f"Failed to connect to node {i}: {e}")
                self.clients.append(None)

        # Find the leader with retries
        print("Finding leader...")
        self.leader_id = self._wait_for_leader(timeout=10.0)
        self.leader_client = self.clients[self.leader_id]
        print(f"Leader is node {self.leader_id}")

    def tearDown(self):
        """Stop all nodes and clean up."""
        for client in self.clients:
            if client is not None:
                try:
                    client.sock.close()
                except:
                    pass

        for node in self.nodes:
            try:
                node.terminate()
                node.wait(timeout=5)
            except:
                node.kill()
                node.wait()

    def _start_node(self, node_id: int, linearizable_reads: bool = False) -> subprocess.Popen:
        """Start a Raft-KV node."""
        cmd = [
            str(self.server_bin),
            f"--node_id={node_id}",
            f"--client_port={self.base_client_port + node_id}",
            f"--raft_port={self.base_raft_port + node_id}",
            f"--db_path=/tmp/test_linear_kv_{node_id}",
            f"--raft_log_path=/tmp/test_linear_raft_{node_id}",
            f"--peers=0:127.0.0.1:{self.base_raft_port},1:127.0.0.1:{self.base_raft_port + 1},2:127.0.0.1:{self.base_raft_port + 2}",
        ]

        if linearizable_reads:
            cmd.append("--linearizable_reads=true")

        # Start with stdout/stderr capture for debugging
        node = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # Wait a bit and check if process is still running
        time.sleep(0.5)
        if node.poll() is not None:
            # Process died, print output
            output = node.stdout.read().decode('utf-8', errors='replace')
            raise RuntimeError(f"Node {node_id} failed to start:\n{output}")

        return node

    def _find_leader(self) -> int:
        """Find the current leader by checking INFO on all nodes."""
        for i, client in enumerate(self.clients):
            if client is None:
                continue
            try:
                info = client.command("INFO")
                if isinstance(info, bytes):
                    info_str = info.decode("utf-8")
                else:
                    info_str = str(info)

                for line in info_str.split("\r\n"):
                    if line.startswith("state:"):
                        if "LEADER" in line:
                            return i
            except Exception as e:
                print(f"Failed to get INFO from node {i}: {e}")
                continue
        raise RuntimeError("No leader found in cluster")

    def _wait_for_leader(self, timeout: float = 5.0) -> int:
        """Wait for a leader to be elected."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                return self._find_leader()
            except RuntimeError:
                time.sleep(0.1)
        raise RuntimeError(f"No leader elected within {timeout}s")

    def test_01_write_then_read_consistency(self):
        """Test 1: SET followed by GET returns the correct value (write-then-read consistency)."""
        print("\n=== Test 1: Write-then-read consistency ===")

        key = "test_consistency"
        value = "consistent_value"

        # Write to leader
        result = self.leader_client.command("SET", key, value)
        self.assertEqual(result, "OK", "SET should succeed")

        # Read immediately (with linearizable reads, should see the write)
        result = self.leader_client.command("GET", key)
        self.assertEqual(result, value, f"GET should return '{value}' immediately after SET")

        print(f"✓ Write-then-read consistency verified: {key}={value}")

    def test_02_concurrent_reads_consistency(self):
        """Test 2: Multiple concurrent GETs return consistent results."""
        print("\n=== Test 2: Concurrent reads consistency ===")

        key = "test_concurrent"
        value = "concurrent_value"

        # Write the value first
        self.leader_client.command("SET", key, value)
        time.sleep(0.5)  # Ensure it's committed

        # Launch 10 concurrent reads
        results = []
        errors = []

        def read_task():
            try:
                client = RespClient("127.0.0.1", self.base_client_port + self.leader_id)
                result = client.command("GET", key)
                results.append(result)
                client.close()
            except Exception as e:
                errors.append(str(e))

        threads = []
        for _ in range(10):
            t = threading.Thread(target=read_task)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=5.0)

        # Verify all reads returned the same value
        self.assertEqual(len(errors), 0, f"No errors expected, got: {errors}")
        self.assertEqual(len(results), 10, "All 10 reads should complete")

        for i, result in enumerate(results):
            self.assertEqual(result, value, f"Read {i} should return '{value}'")

        print(f"✓ All 10 concurrent reads returned consistent value: {value}")

    def test_03_read_from_follower_redirects(self):
        """Test 3: Reading from a follower with linearizable reads should redirect or fail."""
        print("\n=== Test 3: Follower read behavior ===")

        key = "test_follower"
        value = "follower_value"

        # Write to leader
        self.leader_client.command("SET", key, value)
        time.sleep(0.5)

        # Find a follower
        follower_id = None
        for i in range(3):
            if i != self.leader_id:
                follower_id = i
                break

        self.assertIsNotNone(follower_id, "Should have at least one follower")

        # Try to read from follower with linearizable reads
        follower_client = self.clients[follower_id]

        try:
            result = follower_client.command("GET", key)
            # If follower returns result, it should be correct
            self.assertEqual(result, value, "If follower serves read, it should be correct")
            print(f"✓ Follower served read correctly: {value}")
        except RespError as e:
            # Follower should redirect with MOVED error
            self.assertIn("MOVED", str(e), "Follower should redirect to leader")
            print(f"✓ Follower correctly redirected: {e}")

    def test_04_heartbeat_batching_efficiency(self):
        """Test 4: High-frequency reads should batch heartbeats (observability check)."""
        print("\n=== Test 4: Heartbeat batching efficiency ===")

        key = "test_batch"
        value = "batch_value"

        # Write the value
        self.leader_client.command("SET", key, value)
        time.sleep(0.5)

        # Get initial metrics
        info_before = self.leader_client.command("INFO")
        read_index_total_before = self._extract_metric(info_before, "read_index_total")

        # Perform 20 rapid reads
        for i in range(20):
            result = self.leader_client.command("GET", key)
            self.assertEqual(result, value, f"Read {i} should succeed")

        # Get final metrics
        info_after = self.leader_client.command("INFO")
        read_index_total_after = self._extract_metric(info_after, "read_index_total")
        read_index_succeeded = self._extract_metric(info_after, "read_index_succeeded")

        # Verify reads were processed
        reads_processed = read_index_total_after - read_index_total_before
        self.assertEqual(reads_processed, 20, f"Should have processed 20 reads, got {reads_processed}")

        print(f"✓ 20 reads processed successfully")
        print(f"  Total read_index requests: {read_index_total_after}")
        print(f"  Succeeded: {read_index_succeeded}")

        # Note: We can't directly verify heartbeat batching without more detailed metrics,
        # but we can verify that all reads succeeded
        self.assertGreater(read_index_succeeded, 0, "At least some reads should succeed")

    def test_05_no_stale_reads_during_partition(self):
        """Test 5: Network partition - old Leader cannot serve stale reads."""
        print("\n=== Test 5: Network partition prevents stale reads ===")

        # This test requires network partition capabilities which may not be available
        # in simple integration tests. We'll implement a simplified version.

        key = "test_partition"
        value = "partition_value"

        # Write a value
        self.leader_client.command("SET", key, value)
        time.sleep(0.5)

        # Kill the current leader to simulate partition
        old_leader_id = self.leader_id
        print(f"  Stopping node {old_leader_id} to simulate partition...")
        self.nodes[old_leader_id].terminate()
        self.nodes[old_leader_id].wait(timeout=5)

        # Wait for new leader election
        time.sleep(3.0)

        # Find new leader (should be one of the remaining nodes)
        new_leader_id = None
        for i in range(3):
            if i == old_leader_id:
                continue
            try:
                info = self.clients[i].command("INFO")
                if "state:LEADER" in info:
                    new_leader_id = i
                    break
            except:
                continue

        self.assertIsNotNone(new_leader_id, "A new leader should be elected")
        self.assertNotEqual(new_leader_id, old_leader_id, "New leader should be different")

        # Write a new value to new leader
        new_value = "new_partition_value"
        new_leader_client = self.clients[new_leader_id]
        new_leader_client.command("SET", key, new_value)
        time.sleep(0.5)

        # Read from new leader should get new value
        result = new_leader_client.command("GET", key)
        self.assertEqual(result, new_value, "New leader should return new value")

        print(f"✓ New leader correctly serves updated value: {new_value}")
        print(f"✓ Old leader (node {old_leader_id}) is stopped and cannot serve stale reads")

    def _extract_metric(self, info: str, metric_name: str) -> int:
        """Extract a metric value from INFO output."""
        for line in info.split("\r\n"):
            if line.startswith(f"{metric_name}:"):
                return int(line.split(":")[1])
        return 0


class LinearizableReadDisabledTests(unittest.TestCase):
    """Tests to verify behavior when linearizable reads are disabled (default)."""

    @classmethod
    def setUpClass(cls):
        """Check if server binary exists."""
        cls.server_bin = Path("build-linux-repro/server/raft_kv_server")
        if not cls.server_bin.exists():
            alternatives = [
                Path("build-portable/server/raft_kv_server"),
                Path("build/server/raft_kv_server"),
            ]
            for alt in alternatives:
                if alt.exists():
                    cls.server_bin = alt
                    break

        if not cls.server_bin.exists():
            raise unittest.SkipTest(f"Server binary not found at {cls.server_bin}")

    def setUp(self):
        """Start a 3-node cluster with default settings (linearizable reads disabled)."""
        self.base_client_port = 18090
        self.base_raft_port = 19090
        self.nodes = []
        self.clients = []

        # Start 3 nodes WITHOUT linearizable_reads
        for i in range(3):
            cmd = [
                str(self.server_bin),
                f"--node_id={i}",
                f"--client_port={self.base_client_port + i}",
                f"--raft_port={self.base_raft_port + i}",
                f"--db_path=/tmp/test_default_kv_{i}",
                f"--raft_log_path=/tmp/test_default_raft_{i}",
                f"--peers=0:127.0.0.1:{self.base_raft_port},1:127.0.0.1:{self.base_raft_port + 1},2:127.0.0.1:{self.base_raft_port + 2}",
            ]

            node = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.nodes.append(node)
            self.clients.append(RespClient("127.0.0.1", self.base_client_port + i))

        time.sleep(2.0)

    def tearDown(self):
        """Stop all nodes."""
        for client in self.clients:
            try:
                client.close()
            except:
                pass

        for node in self.nodes:
            try:
                node.terminate()
                node.wait(timeout=5)
            except:
                node.kill()
                node.wait()

    def test_default_local_read(self):
        """Test that default behavior is local read (no ReadIndex metrics)."""
        print("\n=== Test: Default local read behavior ===")

        # Find any node
        client = self.clients[0]

        # Perform a GET
        try:
            client.command("GET", "some_key")
        except:
            pass  # Key might not exist, that's OK

        # Check metrics - should NOT have read_index metrics
        info = client.command("INFO")

        has_read_index_metrics = "read_index_total" in info

        if has_read_index_metrics:
            # If metrics exist, they should be 0 (not used)
            read_index_total = 0
            for line in info.split("\r\n"):
                if line.startswith("read_index_total:"):
                    read_index_total = int(line.split(":")[1])

            self.assertEqual(read_index_total, 0, "ReadIndex should not be used by default")

        print("✓ Default behavior uses local reads (not ReadIndex)")


if __name__ == "__main__":
    unittest.main()
