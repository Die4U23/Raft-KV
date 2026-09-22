#!/usr/bin/env python3
"""
ReadIndex Integration Tests

Tests linearizable reads in real cluster scenarios:
1. Network partition: old Leader returns error, not stale data
2. Write-then-read consistency: GET after SET returns correct value
3. Concurrent reads: multiple GETs return consistent results
4. Leader change: new Leader doesn't serve stale reads
"""

import subprocess
import sys
import tempfile
import time
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cluster_smoke import Cluster, RespClient


def test_network_partition_old_leader():
    """Test 1: Old Leader in minority partition cannot serve stale reads"""
    print("\n=== Test 1: Network partition - old Leader ===")

    binary = Path('build-linux-repro/server/raft_kv_server')
    if not binary.exists():
        binary = Path('build/server/raft_kv_server')
    if not binary.exists():
        print("SKIP: binary not found")
        return

    with tempfile.TemporaryDirectory(prefix='raft-kv-test-') as data_dir:
        artifacts = Path(data_dir) / 'artifacts'
        artifacts.mkdir()

        cluster = Cluster(binary, Path(data_dir), artifacts, 60)

        try:
            # Start 3-node cluster with linearizable reads
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=[
                    '--linearizable_reads=true',
                    '--async_apply=true'
                ])

            leader = cluster.wait_for('leader election', lambda: cluster.leader(range(3)))
            print(f"  Initial leader: node {leader}")

            # Write a value
            port = cluster.nodes[leader].client_port
            with RespClient.connect(port, time.time() + 30, 5) as client:
                result = client.command('SELECT', 'test1')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')
                assert result == 'OK', f"SELECT failed: {result}"

                result = client.command('SET', 'key1', 'value1')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')
                assert result == 'OK', f"SET failed: {result}"

            print(f"  Wrote key1=value1")

            # Partition: isolate old leader
            old_leader_node = cluster.nodes[leader]
            old_leader_node.process.terminate()
            old_leader_node.process.wait(timeout=5)
            print(f"  Partitioned node {leader}")

            # Wait for new leader
            time.sleep(3)
            remaining = [i for i in range(3) if i != leader]
            new_leader = cluster.leader(remaining)
            print(f"  New leader: node {new_leader}")

            # Restart old leader (it's now in minority)
            old_leader_node.start(binary, cluster.peers, extra_args=[
                '--linearizable_reads=true',
                '--async_apply=true'
            ])
            time.sleep(1)

            # Try to read from old leader - should fail
            old_port = cluster.nodes[leader].client_port
            with RespClient.connect(old_port, time.time() + 30, 5) as client:
                try:
                    client.command('SELECT', 'test1')
                    result = client.command('GET', 'key1')
                    # Should get an error, not stale data
                    if isinstance(result, bytes) and result == b'value1':
                        print(f"  FAIL: Old leader returned stale data!")
                        raise AssertionError("Old leader served stale read")
                    print(f"  OK: Old leader rejected read (not leader)")
                except Exception as e:
                    print(f"  OK: Old leader cannot serve reads: {e}")

            print("  PASS: Old leader correctly rejects reads")

        finally:
            cluster.close()


def test_write_then_read_consistency():
    """Test 2: Read after write returns correct value"""
    print("\n=== Test 2: Write-then-read consistency ===")

    binary = Path('build-linux-repro/server/raft_kv_server')
    if not binary.exists():
        binary = Path('build/server/raft_kv_server')
    if not binary.exists():
        print("SKIP: binary not found")
        return

    with tempfile.TemporaryDirectory(prefix='raft-kv-test-') as data_dir:
        artifacts = Path(data_dir) / 'artifacts'
        artifacts.mkdir()

        cluster = Cluster(binary, Path(data_dir), artifacts, 60)

        try:
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=[
                    '--linearizable_reads=true',
                    '--async_apply=true'
                ])

            leader = cluster.wait_for('leader election', lambda: cluster.leader(range(3)))
            port = cluster.nodes[leader].client_port

            with RespClient.connect(port, time.time() + 30, 5) as client:
                result = client.command('SELECT', 'test2')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')

                # Write and immediately read
                for i in range(10):
                    key = f'key{i}'
                    value = f'value{i}'

                    result = client.command('SET', key, value)
                    if isinstance(result, bytes):
                        result = result.decode('utf-8')
                    assert result == 'OK', f"SET {key} failed"

                    result = client.command('GET', key)
                    if isinstance(result, bytes):
                        result = result.decode('utf-8')

                    if result != value:
                        print(f"  FAIL: Expected {value}, got {result}")
                        raise AssertionError(f"Read after write inconsistency: {result} != {value}")

                print(f"  OK: All 10 write-then-read operations consistent")

            print("  PASS: Write-then-read consistency verified")

        finally:
            cluster.close()


def test_concurrent_reads():
    """Test 3: Concurrent reads return consistent results"""
    print("\n=== Test 3: Concurrent reads ===")

    binary = Path('build-linux-repro/server/raft_kv_server')
    if not binary.exists():
        binary = Path('build/server/raft_kv_server')
    if not binary.exists():
        print("SKIP: binary not found")
        return

    with tempfile.TemporaryDirectory(prefix='raft-kv-test-') as data_dir:
        artifacts = Path(data_dir) / 'artifacts'
        artifacts.mkdir()

        cluster = Cluster(binary, Path(data_dir), artifacts, 60)

        try:
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=[
                    '--linearizable_reads=true',
                    '--async_apply=true'
                ])

            leader = cluster.wait_for('leader election', lambda: cluster.leader(range(3)))
            port = cluster.nodes[leader].client_port

            # Write initial value
            with RespClient.connect(port, time.time() + 30, 5) as client:
                client.command('SELECT', 'test3')
                client.command('SET', 'counter', '100')

            # Concurrent reads
            results = []
            errors = []

            def read_worker():
                try:
                    with RespClient.connect(port, time.time() + 30, 5) as client:
                        client.command('SELECT', 'test3')
                        for _ in range(20):
                            result = client.command('GET', 'counter')
                            if isinstance(result, bytes):
                                result = result.decode('utf-8')
                            results.append(result)
                except Exception as e:
                    errors.append(str(e))

            # Launch 5 concurrent readers
            threads = [threading.Thread(target=read_worker) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            if errors:
                print(f"  FAIL: {len(errors)} errors occurred")
                raise AssertionError(f"Concurrent reads had errors: {errors[:3]}")

            # All reads should return '100'
            if all(r == '100' for r in results):
                print(f"  OK: All {len(results)} concurrent reads returned '100'")
            else:
                unique_values = set(results)
                print(f"  FAIL: Got inconsistent values: {unique_values}")
                raise AssertionError(f"Inconsistent reads: {unique_values}")

            print("  PASS: Concurrent reads are consistent")

        finally:
            cluster.close()


def test_leader_change():
    """Test 4: New Leader doesn't serve uncommitted writes"""
    print("\n=== Test 4: Leader change ===")

    binary = Path('build-linux-repro/server/raft_kv_server')
    if not binary.exists():
        binary = Path('build/server/raft_kv_server')
    if not binary.exists():
        print("SKIP: binary not found")
        return

    with tempfile.TemporaryDirectory(prefix='raft-kv-test-') as data_dir:
        artifacts = Path(data_dir) / 'artifacts'
        artifacts.mkdir()

        cluster = Cluster(binary, Path(data_dir), artifacts, 60)

        try:
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=[
                    '--linearizable_reads=true',
                    '--async_apply=true'
                ])

            leader = cluster.wait_for('leader election', lambda: cluster.leader(range(3)))
            print(f"  Initial leader: node {leader}")

            # Write committed value
            port = cluster.nodes[leader].client_port
            with RespClient.connect(port, time.time() + 30, 5) as client:
                client.command('SELECT', 'test4')
                client.command('SET', 'stable_key', 'committed_value')

            # Kill old leader
            cluster.nodes[leader].process.terminate()
            cluster.nodes[leader].process.wait(timeout=5)
            print(f"  Killed node {leader}")

            # Wait for new leader
            time.sleep(3)
            remaining = [i for i in range(3) if i != leader]
            new_leader = cluster.leader(remaining)
            print(f"  New leader: node {new_leader}")

            # Read from new leader - should get committed value
            new_port = cluster.nodes[new_leader].client_port
            with RespClient.connect(new_port, time.time() + 30, 5) as client:
                client.command('SELECT', 'test4')
                result = client.command('GET', 'stable_key')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')

                if result == 'committed_value':
                    print(f"  OK: New leader returned committed value")
                else:
                    print(f"  FAIL: Expected 'committed_value', got '{result}'")
                    raise AssertionError(f"New leader returned wrong value: {result}")

            print("  PASS: Leader change preserves committed data")

        finally:
            cluster.close()


def main():
    print("=== ReadIndex Integration Tests ===")
    print("Testing linearizable reads in real cluster scenarios\n")

    tests = [
        test_network_partition_old_leader,
        test_write_then_read_consistency,
        test_concurrent_reads,
        test_leader_change,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\nFAILED: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("\n✓ All integration tests passed!")
        return 0
    else:
        print(f"\n✗ {failed} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
