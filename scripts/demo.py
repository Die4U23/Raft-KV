#!/usr/bin/env python3
"""Three local processes: elect, write, lose the leader, keep the data.

One machine, one Raft group. Not a multi-host deployment.
"""
import argparse
import os
from pathlib import Path
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from cluster_smoke import Cluster, RespError, expect  # noqa: E402


def say(text):
    print(text, flush=True)


def show_value(cluster, key):
    lines = []
    for node_id in range(3):
        with cluster.client(node_id) as client:
            reply = client.command("GET", key)
        if isinstance(reply, RespError):
            shown = "-" + reply.message
        elif reply is None:
            shown = "(nil)"
        else:
            shown = reply.decode("utf-8", errors="replace")
        lines.append("  节点 {}  GET {} = {}".format(node_id, key, shown))
    say("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path,
                        help="raft_kv_server from a Linux build")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--hold", action="store_true",
                        help="leave the three processes up until Enter")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this demo starts the Linux server")
    if not 30 <= args.timeout <= 300:
        parser.error("timeout must be 30..300 seconds")
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("binary is not executable: {}".format(binary))

    root = Path(tempfile.mkdtemp(prefix="raft-kv-demo-"))
    data = root / "data"
    artifacts = root / "logs"
    data.mkdir()
    artifacts.mkdir()
    cluster = Cluster(binary, data, artifacts, args.timeout)
    say("数据目录 {}".format(root))
    try:
        for node in cluster.nodes:
            for reservation in cluster.reservations[2 * node.node_id:2 * node.node_id + 2]:
                reservation.close()
            node.start(binary, cluster.peers)
            say("节点 {}  客户端 {}  Raft {}".format(
                node.node_id, node.client_port, node.raft_port))

        leader = cluster.wait_for("initial election", lambda: cluster.leader(range(3)))
        term = cluster.info(leader)["term"]
        say("选出 Leader：节点 {}，任期 {}".format(leader, term))

        with cluster.client(leader) as client:
            expect(client.command("SET", "intern:name", "raft-kv"), "OK", "demo write")
        say("Leader 写入 intern:name = raft-kv")
        cluster.wait_for("demo value replicated",
                         lambda: cluster.convergence([("default", "intern:name", b"raft-kv")], 1))
        show_value(cluster, "intern:name")

        follower = next(node_id for node_id in range(3) if node_id != leader)
        with cluster.client(follower) as client:
            reply = client.command("SET", "intern:rejected", "no")
        if not isinstance(reply, RespError) or not reply.message.startswith("ERR MOVED "):
            raise AssertionError("follower SET should return MOVED, got {!r}".format(reply))
        say("向 Follower 节点 {} 写入被拒绝：{}".format(follower, reply.message))

        committed = cluster.info(leader)["commit_index"]
        say("杀掉 Leader 节点 {}".format(leader))
        cluster.nodes[leader].stop(crash=True)
        survivors = [node_id for node_id in range(3) if node_id != leader]
        new_leader = cluster.wait_for(
            "election after leader loss",
            lambda: cluster.leader_with_quorum(survivors, committed))
        say("新 Leader：节点 {}，任期 {}".format(
            new_leader, cluster.info(new_leader)["term"]))
        with cluster.client(new_leader) as client:
            expect(client.command("GET", "intern:name"), b"raft-kv", "value after failover")
            expect(client.command("SET", "intern:after", "failover"), "OK", "write after failover")
        say("旧键还在，新 Leader 又写入 intern:after = failover")
        committed = cluster.info(new_leader)["commit_index"]

        say("用原来的目录重启节点 {}".format(leader))
        cluster.nodes[leader].start(binary, cluster.peers)
        expected = [("default", "intern:name", b"raft-kv"),
                    ("default", "intern:after", b"failover")]
        cluster.wait_for("restart convergence", lambda: cluster.convergence(expected, committed))
        final = cluster.wait_for("agreement after restart", lambda: cluster.leader(range(3)))
        say("三个节点再次一致。当前 Leader 是节点 {}。".format(final))
        show_value(cluster, "intern:name")
        show_value(cluster, "intern:after")
        say("演示完成。三个进程在同一台机器上，日志在 {}。".format(artifacts))
        if args.hold:
            say("进程还留着。再按一次回车就关掉。")
            try:
                input()
            except EOFError:
                pass
    finally:
        cluster.close()
        say("已停止三个进程。")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("演示失败：{}".format(error), file=sys.stderr)
        sys.exit(1)
