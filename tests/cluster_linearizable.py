#!/usr/bin/env python3
"""Linux linearizable-read checks against a real three-node server.

Covers Follower MOVED, write-then-read on the Leader, and an isolated old
Leader that must not return a successful stale GET. Requires raft_kv_server.
"""
import argparse
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import traceback

from cluster_smoke import Cluster, RespError, expect
from raft_proxy import RaftProxyMesh


def isolated_get_ok(reply):
    """True only if the isolated old Leader did not serve a successful value."""
    if isinstance(reply, RespError):
        return True
    return False


def run_linearizable(cluster):
    mesh = RaftProxyMesh({node.node_id: node.raft_port for node in cluster.nodes})
    cluster.mesh = mesh
    try:
        for node in cluster.nodes:
            for reservation in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                reservation.close()
            peers = ','.join('{}:127.0.0.1:{}'.format(
                other.node_id, other.raft_port if other.node_id == node.node_id else
                mesh.ports[node.node_id, other.node_id]) for other in cluster.nodes)
            node.start(cluster.binary, peers, extra_args=['--linearizable_reads=true'])
        leader = cluster.wait_for('initial election', lambda: cluster.leader(range(3)))
        cluster.report['initial_leader'] = leader

        with cluster.client(leader) as client:
            expect(client.command('SET', 'k', 'old'), 'OK', 'linearizable SET')
            expect(client.command('GET', 'k'), b'old', 'leader GET after SET')
        cluster.step('leader write-then-read under ReadIndex')

        follower = next(node_id for node_id in range(3) if node_id != leader)
        with cluster.client(follower) as client:
            reply = client.command('GET', 'k')
            if not isinstance(reply, RespError) or 'MOVED' not in reply.message:
                raise AssertionError('follower GET must MOVED, got {!r}'.format(reply))
        cluster.step('follower GET returns MOVED when linearizable_reads is on')

        majority = [node for node in range(3) if node != leader]
        mesh.partition([[leader], majority])
        new_leader = cluster.wait_for('majority election', lambda: cluster.leader(majority))
        cluster.report['majority_leader'] = new_leader
        with cluster.client(new_leader) as client:
            expect(client.command('SET', 'k', 'new'), 'OK', 'majority SET')
            expect(client.command('GET', 'k'), b'new', 'majority GET')
        cluster.step('majority serves the post-partition write')

        stale = None
        try:
            with cluster.client(leader, io_timeout=2.5) as client:
                stale = client.command('GET', 'k')
        except (socket.timeout, TimeoutError, OSError) as error:
            stale = RespError('timeout-or-disconnect: {}'.format(error))
        if not isolated_get_ok(stale):
            raise AssertionError(
                'isolated old leader served a successful linearizable GET: {!r}'.format(stale))
        if stale == b'old' or stale == b'new' or stale == 'old' or stale == 'new':
            raise AssertionError('isolated old leader returned a KV value: {!r}'.format(stale))
        cluster.report['isolated_get'] = repr(stale)
        cluster.step('isolated old leader does not return a successful stale GET')

        mesh.partition([[0, 1, 2]])
        healed_leader = cluster.wait_for('election after heal', lambda: cluster.leader(range(3)))
        with cluster.client(healed_leader) as client:
            expect(client.command('GET', 'k'), b'new', 'healed cluster GET')
        cluster.step('healed cluster reads the majority value')
    finally:
        mesh.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--timeout', type=float, default=90)
    parser.add_argument('--artifacts', type=Path, default=Path('build/cluster-linearizable'))
    args = parser.parse_args()
    if sys.platform != 'linux':
        parser.error('real linearizable-read tests require Linux')
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(str(binary), os.X_OK):
        parser.error('server binary is missing or not executable: {}'.format(binary))
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=str(args.artifacts.resolve())))
    print('Artifacts: {}'.format(artifacts), flush=True)
    with tempfile.TemporaryDirectory(prefix='raft-kv-linearizable-') as data:
        cluster = Cluster(binary, Path(data), artifacts, args.timeout)
        try:
            run_linearizable(cluster)
            cluster.report['status'] = 'PASS'
        except BaseException:
            cluster.report['status'] = 'FAIL'
            cluster.report['error'] = traceback.format_exc()
        finally:
            try:
                cluster.close()
            except Exception:
                cluster.report['status'] = 'FAIL'
                cluster.report['cleanup_error'] = traceback.format_exc()
        if cluster.report.get('status') != 'PASS':
            print(cluster.report.get('error', 'FAIL'), file=sys.stderr)
            return 1
    print('PASS: Linux linearizable-read isolation; artifacts: {}'.format(artifacts))
    return 0


if __name__ == '__main__':
    sys.exit(main())
