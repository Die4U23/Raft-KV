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

static raftcore::AppendEntries History(int leader, int term, int64_t commit, int count) {
    raftcore::AppendEntries request;
    request.set_term(term);
    request.set_leader_id(leader);
    request.set_rpc_id(1);
    request.set_prev_log_index(0);
    request.set_prev_log_term(0);
    request.set_leader_commit(commit);
    for (int index = 1; index <= count; ++index)
        *request.add_entries() = Entry(index, term, Command({"SET", "default:k", "v"}));
    return request;
}

static void DivergentFollowerJumpsToSnapshot() {
    Cluster cluster;
    cluster.Partition(50);
    cluster.Node(50).HandleAppendEntries(30, History(30, 1, 0, 24));
    cluster.messages.clear();
    cluster.Node(10).SetSnapshotThreshold(4);
    cluster.Node(10).HandleAppendEntries(30, History(30, 2, 6, 8));
    Check(LastResponse(cluster).success(), "leader history was rejected");
    cluster.Node(30).HandleAppendEntries(10, History(10, 2, 6, 8));
    cluster.messages.clear();
    const int64_t snapshot = cluster.Node(10).SnapshotIndex();
    Check(snapshot >= 4, "applied prefix was not snapshotted");
    cluster.Elect(10);
    cluster.Heal(50);
    cluster.messages.clear();
    Message append{};
    bool saw_append = false;
    for (int tick = 0; tick < 10 && !saw_append; ++tick) {
        cluster.Advance(10, RaftNode::kTickIntervalMs);
        cluster.Node(10).Tick();
        for (const auto& message : cluster.messages) {
            if (message.to == 50 && message.type == RaftMsgType::kAppendEntries) {
                append = message;
                saw_append = true;
            }
        }
    }
    Check(saw_append, "leader did not probe the divergent follower");
    raftcore::AppendEntries rpc;
    Check(rpc.ParseFromString(append.payload), "probe decode");
    Check(rpc.prev_log_index() > snapshot, "probe was already inside the snapshot");
    cluster.messages.clear();
    cluster.Deliver(append);
    Check(!cluster.messages.empty() &&
          cluster.messages.back().type == RaftMsgType::kAppendEntriesResponse,
          "divergent follower did not reject the probe");
    raftcore::AppendEntriesResponse rejection;
    Check(rejection.ParseFromString(cluster.messages.back().payload) && !rejection.success() &&
          rejection.last_log_index() == 0,
          "rejection did not jump to the start of the conflicting term");
    auto response = cluster.messages.back();
    cluster.messages.clear();
    cluster.Deliver(response);
    bool install = false;
    for (const auto& message : cluster.messages) {
        Check(!(message.to == 50 && message.type == RaftMsgType::kAppendEntries),
              "leader walked nextIndex back by one entry");
        if (message.to == 50 && message.type == RaftMsgType::kInstallSnapshot) install = true;
    }
    Check(install, "leader did not switch to InstallSnapshot");
}

static void InterruptedInstallRestartsAndReplicates() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Partition(50);
    cluster.Node(10).SetSnapshotThreshold(1);
    cluster.Node(10).SetSnapshotChunkBytes(8);
    cluster.Node(30).SetSnapshotThreshold(1);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "snapshot-value"}),
        [&](bool ok, const std::string&) { Check(ok, "SET failed"); }) > 0, "proposal rejected");
    // Drop the append that still carries the entry. After the leader compacts it,
    // a rejected retry must install the snapshot instead of replaying that append.
    uint64_t dropped_rpc = 0;
    std::deque<Message> kept;
    for (auto& message : cluster.messages) {
        if (message.to == 50 && message.type == RaftMsgType::kAppendEntries && dropped_rpc == 0) {
            dropped_rpc = AppendRpcId(message);
            continue;
        }
        kept.push_back(std::move(message));
    }
    cluster.messages = std::move(kept);
    Check(dropped_rpc != 0, "leader did not send the partitioned append");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).SnapshotIndex() > 0, "leader did not snapshot");
    raftcore::AppendEntriesResponse rejection;
    rejection.set_rpc_id(dropped_rpc);
    rejection.set_term(cluster.Node(10).GetCurrentTerm());
    rejection.set_success(false);
    rejection.set_last_log_index(0);
    cluster.Node(10).HandleAppendEntriesResponse(50, rejection);
    cluster.Heal(50);
    bool saw_partial = false;
    for (int step = 0; step < 80 && !saw_partial; ++step) {
        if (cluster.messages.empty()) {
            cluster.Advance(10, RaftNode::kTickIntervalMs);
            cluster.Node(10).Tick();
        }
        if (cluster.messages.empty()) continue;
        auto message = std::move(cluster.messages.front());
        cluster.messages.pop_front();
        if (message.to == 50 && message.type == RaftMsgType::kInstallSnapshot) {
            raftcore::InstallSnapshot rpc;
            Check(rpc.ParseFromString(message.payload), "snapshot decode");
            Check(!rpc.done(), "first snapshot chunk already finished the transfer");
            cluster.Deliver(message);
            saw_partial = true;
            break;
        }
        cluster.Deliver(message);
    }
    Check(saw_partial, "leader did not send a partial snapshot chunk");
    // The partial chunk lived only in memory. Restart drops it. The leader still
    // has a later offset, learns the follower rejected it, and sends the snapshot again.
    cluster.Restart(50);
    bool installed = false;
    std::string value;
    for (int attempt = 0; attempt < 40; ++attempt) {
        cluster.Settle();
        if (cluster.State(50).Get("default:k", &value) && value == "snapshot-value") {
            installed = true;
            break;
        }
    }
    Check(installed, "restarted follower did not finish the snapshot");
    int callbacks = 0;
    Check(cluster.Node(10).Propose(Command({"SET", "default:after", "next"}),
        [&](bool ok, const std::string&) {
            Check(ok, "post-snapshot SET failed");
            ++callbacks;
        }) > 0, "post-snapshot proposal rejected");
    cluster.Pump();
    for (int attempt = 0; attempt < 40 &&
         !(cluster.State(50).Get("default:after", &value) && value == "next"); ++attempt) {
        cluster.Settle();
    }
    Check(callbacks == 1, "post-snapshot callback missing");
    Check(cluster.State(50).Get("default:k", &value) && value == "snapshot-value",
          "snapshotted key disappeared after the later write");
    Check(cluster.State(50).Get("default:after", &value) && value == "next",
          "entry after the snapshot did not replicate");
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
        InterruptedInstallRestartsAndReplicates();
        DivergentFollowerJumpsToSnapshot();
        OlderSnapshotDoesNotReplaceState();
        std::cout << "PASS: snapshots\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
