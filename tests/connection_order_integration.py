#!/usr/bin/env python3
"""Real Linux TCP regression for input scheduling and per-connection ordering."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import sys
import tempfile
import time
import traceback

from cluster_smoke import Cluster, RespError, encode_command, expect


def cpu_seconds(pid):
    # comm is parenthesized and can contain spaces; fields after it start at 3.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def expect_error(reply, fragment):
    if not isinstance(reply, RespError) or fragment not in reply.message:
        raise AssertionError(f'expected protocol error {fragment!r}, got {reply!r}')


def exercise(cluster):
    for node in cluster.nodes:
        for reservation in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
            reservation.close()
        node.start(cluster.binary, cluster.peers, extra_args=['--group_commit_ms=10'])
    leader = cluster.wait_for('leader election', lambda: cluster.leader(range(3)))
    with cluster.client(leader, io_timeout=5) as client:
        expect(client.command('SELECT', 'ordered'), 'OK', 'namespace setup')
        replies = client.pipeline([
            ('SET', 'key', 'first'), ('GET',), ('BOGUS',),
            ('GET', 'key'), ('SET', 'key', 'second'), ('GET', 'key'), ('PING',)])
        expect(replies[0], 'OK', 'first write reply must precede errors')
        expect_error(replies[1], 'wrong number of arguments')
        expect_error(replies[2], 'unknown command')
        expect(replies[3:], [b'first', 'OK', b'second', 'PONG'], 'read/apply/reply order')
        cluster.step('async writes, invalid commands and reads preserve reply order')
        expect(client.pipeline([('PING',)] * 257), ['PONG'] * 257,
               'pipeline must continue across the 128-command parse budget')
        cluster.step('complete buffered pipeline continues without new network input')

    ping = encode_command('PING')
    with cluster.client(leader, io_timeout=5) as client:
        # Real TCP split frames must wait, then resume exactly once.
        for split in range(1, len(ping)):
            client.sock.sendall(ping[:split])
            if select.select([client.sock], [], [], 0.02)[0]:
                raise AssertionError('server replied or closed before a complete PING')
            client.sock.sendall(ping[split:])
            client.request_deadline = min(client.deadline, time.monotonic() + 5)
            expect(client.read_reply(), 'PONG', 'fragmented PING')
        expect(client.command('PING'), 'PONG', 'no duplicate reply after fragments')
        if client.buffer or select.select([client.sock], [], [], 0.05)[0]:
            raise AssertionError('extra reply after fragmented commands')
        cluster.step('every PING split waits for completion and resumes once')

    with cluster.client(leader, io_timeout=5) as client:
        pid = cluster.nodes[leader].process.pid
        client.sock.sendall(ping[:10])
        time.sleep(0.1)  # Let the first fragment reach the server before sampling.
        started = time.monotonic()
        before = cpu_seconds(pid)
        time.sleep(0.6)
        used = cpu_seconds(pid) - before
        elapsed = time.monotonic() - started
        limit = elapsed * 0.25 + 2 / os.sysconf('SC_CLK_TCK')
        cluster.report['partial_frame_cpu'] = {
            'cpu_seconds': used, 'wall_seconds': elapsed, 'maximum_cpu_seconds': limit,
            'scope': 'idle three-process test; not a capacity benchmark'}
        if used > limit:
            raise AssertionError(f'partial frame busy loop: CPU {used:.3f}s > {limit:.3f}s')
        if select.select([client.sock], [], [], 0)[0]:
            raise AssertionError('incomplete frame must not produce a reply or EOF')
        client.sock.sendall(ping[10:])
        client.request_deadline = min(client.deadline, time.monotonic() + 5)
        expect(client.read_reply(), 'PONG', 'resume after idle partial frame')
        expect(client.command('PING'), 'PONG', 'connection remains usable')
        cluster.step('idle partial frame does not busy-loop; completion resumes')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, default=Path('build/connection-order'))
    args = parser.parse_args(argv)
    binary = args.binary.resolve()
    if sys.platform != 'linux' or not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error('requires Linux and an executable server binary')
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='run-', dir=args.artifacts.resolve()))
    with tempfile.TemporaryDirectory(prefix='raft-kv-connection-order-') as data:
        cluster = Cluster(binary, Path(data), artifacts, 90)
        cluster.report['binary'] = {'path': str(binary),
                                    'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
        try:
            exercise(cluster)
            cluster.report['status'] = 'PASS'
        except Exception:
            cluster.report.update(status='FAIL', error=traceback.format_exc())
        finally:
            try:
                cluster.close()
            except Exception:
                cluster.report.update(status='FAIL', cleanup_error=traceback.format_exc())
            (artifacts / 'report.json').write_text(
                json.dumps(cluster.report, indent=2) + '\n', encoding='utf-8')
        print(f"{cluster.report['status']}: connection regression; {artifacts / 'report.json'}")
        if cluster.report['status'] != 'PASS':
            print(cluster.report.get('error', cluster.report.get('cleanup_error')), file=sys.stderr)
            return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
