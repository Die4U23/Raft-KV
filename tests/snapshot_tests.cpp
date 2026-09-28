// Production RaftNode snapshots: compact an applied prefix, restart from it,
// and install it on a follower that never saw the compacted entries.
#include "in_process_cluster.h"
#include <iostream>

static void SnapshotSurvivesRestart() {
    Cluster cluster({10});
    cluster.Elect(10);
    cluster.Node(10).SetSnapshotThreshold(1);
    int callbacks = 0;
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool ok, const std::string& value) {
            Check(ok && value == "+OK\r\n", "SET result");
            ++callbacks;
        }) > 0, "proposal rejected");
    cluster.Pump();
    Check(callbacks == 1, "proposal callback missing");
    Check(cluster.Node(10).SnapshotIndex() >= 1, "applied prefix was not snapshotted");
    const auto snapshot = cluster.Node(10).SnapshotIndex();
    std::string value;
    Check(cluster.State(10).Get("default:k", &value) && value == "v", "value missing before restart");
    cluster.Restart(10);
    Check(cluster.Node(10).SnapshotIndex() == snapshot, "restart lost the snapshot index");
    Check(cluster.State(10).Get("default:k", &value) && value == "v", "restart lost the snapshotted value");
}

static void LaggingPeerInstallsChunkedSnapshot() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Partition(50);
    cluster.Node(10).SetSnapshotThreshold(1);
    cluster.Node(10).SetSnapshotChunkBytes(8);
    cluster.Node(30).SetSnapshotThreshold(1);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "snapshot-value"}),
        [&](bool ok, const std::string&) { Check(ok, "SET failed"); }) > 0, "proposal rejected");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).SnapshotIndex() > 0, "leader did not snapshot");
    std::string value;
    Check(cluster.State(10).Get("default:k", &value) && value == "snapshot-value", "leader value missing");
    Check(!cluster.State(50).Get("default:k", &value), "partitioned node stored the write");
    cluster.Heal(50);
    bool installed = false;
    for (int attempt = 0; attempt < 40; ++attempt) {
        cluster.Settle();
        if (cluster.State(50).Get("default:k", &value) && value == "snapshot-value") {
            installed = true;
            break;
        }
    }
    Check(installed, "lagging node did not install the snapshot");
    Check(cluster.Node(50).GetLastApplied() >= cluster.Node(10).SnapshotIndex(),
          "lagging node trails the snapshot index");
}

static void OlderSnapshotDoesNotReplaceState() {
    Cluster cluster({10, 30});
    cluster.Elect(10);
    cluster.Node(10).Propose(Command({"SET", "default:k", "fresh"}), [](bool, const std::string&) {});
    cluster.Pump();
    cluster.Settle();
    const auto applied = cluster.Node(30).GetLastApplied();
    Check(applied > 0, "follower did not apply");
    raftcore::InstallSnapshot request;
    request.set_term(cluster.Node(30).GetCurrentTerm());
    request.set_leader_id(10);
    request.set_last_included_index(applied);
    request.set_last_included_term(1);
    request.set_offset(0);
    request.set_data("not-a-snapshot");
    request.set_done(true);
    request.set_rpc_id(99);
    cluster.Node(30).HandleInstallSnapshot(10, request);
    std::string value;
    Check(cluster.State(30).Get("default:k", &value) && value == "fresh",
          "older snapshot replaced newer state");
}

int main() {
    try {
        SnapshotSurvivesRestart();
        LaggingPeerInstallsChunkedSnapshot();
        OlderSnapshotDoesNotReplaceState();
        std::cout << "PASS: snapshots\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
