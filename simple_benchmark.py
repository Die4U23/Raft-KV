#!/usr/bin/env python3
"""Simple benchmark for ReadIndex vs regular operations."""
import asyncio
import json
import sys
import time
from pathlib import Path
import tempfile

sys.path.insert(0, str(Path(__file__).parent / 'tests'))
from cluster_smoke import Cluster, RespClient
from load_benchmark import benchmark
from types import SimpleNamespace


def main():
    binary = Path('build-linux-repro/server/raft_kv_server').resolve()
    if not binary.exists():
        print(f"Error: Binary not found at {binary}")
        return 1

    with tempfile.TemporaryDirectory(prefix='raft-kv-bench-') as data_dir:
        artifacts = Path('benchmark-results')
        artifacts.mkdir(exist_ok=True)

        cluster = Cluster(binary, Path(data_dir), artifacts, 240)

        try:
            # Start 3-node cluster
            for node in cluster.nodes:
                for sock in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    sock.close()
                node.start(binary, cluster.peers, extra_args=['--async_apply=true', '--group_commit_ms=1'])

            leader = cluster.wait_for('initial leader', lambda: cluster.leader(range(3)))
            print(f"Leader elected: node {leader}")

            leader_port = cluster.nodes[leader].client_port

            # Benchmark 1: Write-heavy workload (50% writes)
            print("\n=== Benchmark 1: Mixed workload (50% write, 50% read) ===")
            config = SimpleNamespace(
                host='127.0.0.1',
                port=leader_port,
                connections=32,
                requests=50000,
                pipeline=1,
                value_size=128,
                write_ratio=0.5,
                timeout=5,
                namespace='bench1',
                client_mode='classic'
            )

            result1 = asyncio.run(benchmark(config))
            print(f"Throughput: {result1['goodput_ops_per_second']:.2f} ops/sec")
            print(f"Latency p50/p95/p99: {result1['batch_latency_ms']['p50']:.2f} / "
                  f"{result1['batch_latency_ms']['p95']:.2f} / {result1['batch_latency_ms']['p99']:.2f} ms")

            # Benchmark 2: Read-heavy workload (10% writes)
            print("\n=== Benchmark 2: Read-heavy workload (10% write, 90% read) ===")
            config.namespace = 'bench2'
            config.write_ratio = 0.1
            config.requests = 50000

            result2 = asyncio.run(benchmark(config))
            print(f"Throughput: {result2['goodput_ops_per_second']:.2f} ops/sec")
            print(f"Latency p50/p95/p99: {result2['batch_latency_ms']['p50']:.2f} / "
                  f"{result2['batch_latency_ms']['p95']:.2f} / {result2['batch_latency_ms']['p99']:.2f} ms")

            # Benchmark 3: Pure read workload (0% writes)
            print("\n=== Benchmark 3: Pure read workload (0% write, 100% read) ===")
            config.namespace = 'bench3'
            config.write_ratio = 0.0
            config.requests = 50000

            result3 = asyncio.run(benchmark(config))
            print(f"Throughput: {result3['goodput_ops_per_second']:.2f} ops/sec")
            print(f"Latency p50/p95/p99: {result3['batch_latency_ms']['p50']:.2f} / "
                  f"{result3['batch_latency_ms']['p95']:.2f} / {result3['batch_latency_ms']['p99']:.2f} ms")

            # Save detailed results
            summary = {
                'mixed_50_50': {
                    'throughput': result1['goodput_ops_per_second'],
                    'latency_ms': result1['batch_latency_ms']
                },
                'read_heavy_90_10': {
                    'throughput': result2['goodput_ops_per_second'],
                    'latency_ms': result2['batch_latency_ms']
                },
                'pure_read_100_0': {
                    'throughput': result3['goodput_ops_per_second'],
                    'latency_ms': result3['batch_latency_ms']
                }
            }

            result_file = artifacts / 'benchmark-summary.json'
            result_file.write_text(json.dumps(summary, indent=2))
            print(f"\n=== Results saved to {result_file} ===")

            return 0

        finally:
            cluster.close()


if __name__ == '__main__':
    sys.exit(main())
