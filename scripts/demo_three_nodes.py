#!/usr/bin/env python3
"""Repeatable three-node demonstration.

Starts three raft_kv_server processes, writes one key and one config version,
kills the leader, and reads both values from the new leader. Process and RESP
handling come from tests/cluster_smoke.py. This is a demonstration entry, not
the smoke suite and not a clean-machine install record.
"""

import argparse
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
import cluster_smoke as smoke  # noqa: E402


def say(text):
    print(text, flush=True)


def demonstrate(binary, artifacts, timeout):
    with tempfile.TemporaryDirectory(prefix="raft-kv-demo-") as data:
        cluster = smoke.Cluster(binary, Path(data), artifacts, timeout)
        try:
            for node in cluster.nodes:
                for reservation in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                    reservation.close()
                node.start(binary, cluster.peers, ("--linearizable_reads=true",))
            leader = cluster.wait_for("initial election", lambda: cluster.leader(range(3)))
            term = cluster.info(leader)["term"]
            say("leader node {} term {}".format(leader, term))
            with cluster.client(leader) as client:
                smoke.expect(client.command("SET", "demo:user", "alice"), "OK", "SET demo:user")
                version = client.command("CFGSET", "rollout", "canary")
                if not isinstance(version, int) or version < 1:
                    raise AssertionError("CFGSET did not return a version: {!r}".format(version))
            say("SET demo:user alice")
            say("CFGSET rollout canary -> version {}".format(version))
            cluster.nodes[leader].stop(crash=True)
            say("killed leader {}".format(leader))
            survivors = [node_id for node_id in range(3) if node_id != leader]
            new_leader = cluster.wait_for(
                "election after leader loss", lambda: cluster.leader(survivors))
            with cluster.client(new_leader) as client:
                smoke.expect(client.command("GET", "demo:user"), b"alice", "GET after failover")
                smoke.expect(client.command("CFGGET", "rollout"),
                             [version, b"canary"], "CFGGET after failover")
            say("new leader {} GET demo:user = alice".format(new_leader))
            say("new leader {} CFGGET rollout = version {} canary".format(new_leader, version))
        finally:
            cluster.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", "--binary", dest="binary", type=Path,
                        default=ROOT / "build-linux-repro" / "server" / "raft_kv_server")
    parser.add_argument("--timeout", type=float, default=90,
                        help="whole-demo deadline in seconds, excluding cleanup (default: 90)")
    parser.add_argument("--artifacts", type=Path, default=Path("/tmp/raft-kv-demo"),
                        help="parent directory for this run's node logs")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("the demo starts real server processes and requires Linux")
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(str(binary), os.X_OK):
        parser.error("server binary is missing or not executable: {}".format(binary))
    if not 0 < args.timeout <= 3600:
        parser.error("--timeout must be greater than 0 and at most 3600")
    args.artifacts.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix="run-", dir=str(args.artifacts.resolve())))
    say("logs: {}".format(artifacts))
    started = time.monotonic()
    try:
        demonstrate(binary, artifacts, args.timeout)
    except BaseException:
        traceback.print_exc()
        say("FAIL: logs kept in {}".format(artifacts))
        return 1
    say("PASS: three-node demo in {:.2f}s".format(time.monotonic() - started))
    return 0


if __name__ == "__main__":
    sys.exit(main())
