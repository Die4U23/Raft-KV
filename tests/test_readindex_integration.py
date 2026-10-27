#!/usr/bin/env python3
"""
ReadIndex Integration Tests

Tests linearizable reads in real cluster scenarios:
1. Restarted follower rejects GET in linearizable mode (not a network partition)
2. Write-then-read consistency: GET after SET returns correct value
3. Concurrent reads: multiple GETs return consistent results
4. Leader change: new Leader doesn't serve stale reads
"""

import argparse
import sys
import tempfile
import threading
from unittest import SkipTest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from cluster_smoke import Cluster, RespClient, RespError, expect

ROOT = Path(__file__).resolve().parents[1]


def require_binary(binary=None):
    candidates = [Path(binary)] if binary is not None else [
        ROOT / 'build-linux-repro/server/raft_kv_server', ROOT / 'build/server/raft_kv_server']
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise SkipTest('server binary not found; supply --binary from a real Linux build')


def test_restarted_follower_rejects_reads(binary=None):
    """Test 1: Restart a former leader and verify follower GET rejection.

    No Raft links are partitioned here. Minority/old-leader ReadIndex safety
    requires a separate proxy-based partition scenario.
    """
    print("\n=== Test 1: Restarted follower rejects linearizable GET ===")

    binary = require_binary(binary)

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
            with RespClient.connect(port, cluster.deadline, 5) as client:
                result = client.command('SELECT', 'test1')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')
                expect(result, 'OK', 'SELECT test1')

                result = client.command('SET', 'key1', 'value1')
                if isinstance(result, bytes):
                    result = result.decode('utf-8')
                expect(result, 'OK', 'SET key1')

            print(f"  Wrote key1=value1")

            # Stop/restart is a crash scenario, not a network partition.
            old_leader_node = cluster.nodes[leader]
            old_leader_node.process.terminate()
            old_leader_node.process.wait(timeout=5)
            print(f"  Stopped node {leader}")

            # Wait for new leader
            remaining = [i for i in range(3) if i != leader]
            new_leader = cluster.wait_for('replacement leader', lambda: cluster.leader(remaining))
            print(f"  New leader: node {new_leader}")

            # Restart with normal connectivity and wait for agreed roles.
            old_leader_node.start(binary, cluster.peers, extra_args=[
                '--linearizable_reads=true',
                '--async_apply=true'
            ])
            current_leader = cluster.wait_for('restart election convergence', lambda: cluster.leader(range(3)))
            expect(current_leader, new_leader, 'replacement leader remains leader during restart')

            # Try to read from old leader - should fail
            old_port = cluster.nodes[leader].client_port
            with RespClient.connect(old_port, cluster.deadline, 5) as client:
                expect(client.command('SELECT', 'test1'), 'OK', 'SELECT on restarted follower')
                result = client.command('GET', 'key1')
                # RespClient returns RespError as a value; it does not raise it.
                if not isinstance(result, RespError):
                    raise AssertionError(f'follower returned a value instead of rejection: {result!r}')
                expect(result.message, f'ERR MOVED {new_leader}', 'follower read rejection')

            print("  PASS: Old leader correctly rejects reads")

        finally:
            cluster.close()


def test_write_then_read_consistency(binary=None):
    """Test 2: Read after write returns correct value"""
    print("\n=== Test 2: Write-then-read consistency ===")

    binary = require_binary(binary)

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

            with RespClient.connect(port, cluster.deadline, 5) as client:
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
                    expect(result, 'OK', f'SET {key}')

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


def test_concurrent_reads(binary=None):
    """Test 3: Concurrent reads return consistent results"""
    print("\n=== Test 3: Concurrent reads ===")

    binary = require_binary(binary)

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
            with RespClient.connect(port, cluster.deadline, 5) as client:
                client.command('SELECT', 'test3')
                client.command('SET', 'counter', '100')

            # Concurrent reads
            results = []
            errors = []

            def read_worker():
                try:
                    with RespClient.connect(port, cluster.deadline, 5) as client:
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

            expect(len(results), 100, 'all concurrent reads completed')
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


def test_leader_change(binary=None):
    """Test 4: New leader retains a previously acknowledged write."""
    print("\n=== Test 4: Leader change ===")

    binary = require_binary(binary)

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
            with RespClient.connect(port, cluster.deadline, 5) as client:
                client.command('SELECT', 'test4')
                client.command('SET', 'stable_key', 'committed_value')

            # Kill old leader
            cluster.nodes[leader].process.terminate()
            cluster.nodes[leader].process.wait(timeout=5)
            print(f"  Killed node {leader}")

            # Wait for new leader
            remaining = [i for i in range(3) if i != leader]
            new_leader = cluster.wait_for('replacement leader', lambda: cluster.leader(remaining))
            print(f"  New leader: node {new_leader}")

            # Read from new leader - should get committed value
            new_port = cluster.nodes[new_leader].client_port
            with RespClient.connect(new_port, cluster.deadline, 5) as client:
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


def run_tests(tests, binary=None):
    print("=== ReadIndex Integration Tests ===")
    print("Testing linearizable reads in real cluster scenarios\n")

    passed = 0
    failed = 0
    skipped = 0

    for test in tests:
        try:
            test(binary)
            passed += 1
        except SkipTest as error:
            print(f'SKIP: {test.__name__}: {error}')
            skipped += 1
        except Exception as e:
            print(f"\nFAILED: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")

    if failed:
        print(f"\nFAIL: {failed} test(s) failed")
        return 1
    if skipped or passed == 0:
        print('\nINCOMPLETE: integration validation did not run in full')
        return 2
    print('\nPASS: All integration tests passed!')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, help='server from a real Linux build')
    args = parser.parse_args(argv)
    return run_tests([
        test_restarted_follower_rejects_reads,
        test_write_then_read_consistency,
        test_concurrent_reads,
        test_leader_change,
    ], args.binary)


if __name__ == '__main__':
    sys.exit(main())
