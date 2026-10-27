// Production RaftNode snapshots: compact an applied prefix, restart from it,
// install it on a follower that never saw the compacted entries, and keep the
// applied suffix shorter than the snapshot threshold.
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

static void AppliedEntriesStayWithinSnapshotThreshold() {
    Cluster cluster;
    cluster.Elect(10);
    constexpr int64_t threshold = 4;
    for (int id : {10, 30, 50}) cluster.Node(id).SetSnapshotThreshold(threshold);
    for (int i = 0; i < 20; ++i) {
        const auto key = "default:k" + std::to_string(i);
        Check(cluster.Node(10).Propose(Command({"SET", key, "v"}),
            [](bool ok, const std::string&) { Check(ok, "SET failed"); }) > 0,
            "proposal rejected");
        cluster.Pump();
    }
    cluster.Settle();
    for (int id : {10, 30, 50}) {
        const auto applied = cluster.Node(id).GetLastApplied();
        const auto snapshot = cluster.Node(id).SnapshotIndex();
        Check(applied >= 20, "node did not apply the writes");
        Check(snapshot > 0 && applied - snapshot < threshold,
              "applied log suffix reached the snapshot threshold");
        std::string value;
        Check(cluster.State(id).Get("default:k0", &value) && value == "v",
              "snapshotted key missing");
        Check(cluster.State(id).Get("default:k19", &value) && value == "v",
              "latest key missing");
    }
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

static raftcore::InstallSnapshot InstallRequest(const std::string& data) {
    raftcore::InstallSnapshot request;
    request.set_term(1);
    request.set_leader_id(10);
    request.set_last_included_index(1);
    request.set_last_included_term(1);
    request.set_offset(0);
    request.set_data(data);
    request.set_done(true);
    request.set_rpc_id(99);
    return request;
}

static void SnapshotKVCommitCrashRecovers() {
    Cluster cluster({10, 30});
    KVStateMachine donor(cluster.Path(10, "/donor"));
    donor.Apply(1, Command({"SET", "default:recovered", "value"}));
    auto log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    auto kv = rocksdb::testing::StateFor(cluster.Path(30, "/kv"));
    kv->after_write = [log] { log->fail_writes = 1; };
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest(donor.ExportSnapshot())); });
    kv->after_write = {};
    Check(cluster.State(30).LastApplied() == 1, "fixture missed durable KV commit window");
    cluster.messages.clear();
    log->fail_writes = 1;
    Throws([&] { cluster.Restart(30); });
    Check(log->data.count("raftkv-install-intent-v1") == 1,
          "failed Raft finalization during recovery erased the intent");
    cluster.Restart(30);
    std::string value;
    Check(cluster.State(30).Get("default:recovered", &value) && value == "value",
          "redo recovery lost snapshot KV data");
    Check(cluster.Node(30).SnapshotIndex() == 1, "redo recovery did not finalize Raft metadata");
    auto writes = kv->successful_writes;
    cluster.Restart(30);
    Check(kv->successful_writes == writes, "completed intent replayed on second restart");
    cluster.messages.clear();
    cluster.Elect(30);
    Check(cluster.Node(30).Propose(Command({"SET", "default:after-redo", "next"}),
          [](bool ok, const std::string&) { Check(ok, "post-recovery write failed"); }) > 0,
          "post-recovery proposal rejected");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.State(10).Get("default:recovered", &value) && value == "value",
          "recovered snapshot did not replicate to peer");
    Check(cluster.State(10).Get("default:after-redo", &value) && value == "next",
          "new writes did not replicate after recovery");
}

static void SnapshotIntentBeforeKVAndRepeatedRecoveryFailure() {
    Cluster cluster({10, 30});
    KVStateMachine donor(cluster.Path(10, "/donor"));
    donor.Apply(1, Command({"SET", "default:redo", "v"}));
    auto kv = rocksdb::testing::StateFor(cluster.Path(30, "/kv"));
    auto log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    kv->fail_writes = 1;
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest(donor.ExportSnapshot())); });
    Check(cluster.State(30).LastApplied() == 0, "failed KV write changed applied marker");
    Check(log->data.count("raftkv-install-intent-v1") == 1, "redo intent missing before KV write");
    kv->fail_writes = 1;
    Throws([&] { cluster.Restart(30); });
    Check(log->data.count("raftkv-install-intent-v1") == 1, "failed recovery erased intent");
    cluster.Restart(30);
    Check(cluster.Node(30).SnapshotIndex() == 1, "second recovery did not complete");
    Check(log->data.count("raftkv-install-intent-v1") == 0, "completed intent not removed");
    Check(kv->non_sync_writes == 0 && log->non_sync_writes == 0, "redo protocol used async writes");
}

static void SnapshotIntentFailureDoesNotMutateKV() {
    Cluster cluster({10, 30});
    cluster.Node(30).HandleAppendEntries(10, History(10, 1, 0, 0));
    cluster.messages.clear();
    KVStateMachine donor(cluster.Path(10, "/donor"));
    donor.Apply(1, Command({"SET", "default:redo", "v"}));
    auto log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    auto kv = rocksdb::testing::StateFor(cluster.Path(30, "/kv"));
    const auto before = kv->data;
    log->fail_writes = 1;
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest(donor.ExportSnapshot())); });
    Check(kv->data == before && cluster.State(30).LastApplied() == 0,
          "KV changed before durable snapshot intent");
    Check(cluster.messages.empty(), "failed install acknowledged");
    cluster.Restart(30);
    Check(cluster.Node(30).SnapshotIndex() == 0, "failed intent changed Raft snapshot");
}

static void InvalidSnapshotDoesNotPersistIntent() {
    Cluster cluster({10, 30});
    auto log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest("bad")); });
    Check(log->data.count("raftkv-install-intent-v1") == 0, "invalid bytes persisted a redo intent");
    Check(cluster.State(30).LastApplied() == 0, "invalid bytes changed KV");
    cluster.Restart(30);
}

static void SnapshotStoreValidationPrecedesIntent() {
    Cluster cluster({10, 30});
    const auto put32 = [](std::string* out, uint32_t value) {
        for (int shift = 24; shift >= 0; shift -= 8)
            out->push_back(static_cast<char>((value >> shift) & 0xff));
    };
    const std::string key = std::string(1, '\0') + "raftkv:last_applied";
    std::string bytes;
    put32(&bytes, 1); put32(&bytes, static_cast<uint32_t>(key.size()));
    bytes += key; put32(&bytes, 0);
    auto log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest(bytes)); });
    Check(log->data.count("raftkv-install-intent-v1") == 0,
          "reserved applied marker persisted a poisoned intent");
    cluster.Restart(30);
}

static void AmbiguousKVWriteCanReplay() {
    Cluster cluster({10, 30});
    KVStateMachine donor(cluster.Path(10, "/donor"));
    donor.Apply(1, Command({"SET", "default:ambiguous", "v"}));
    auto kv = rocksdb::testing::StateFor(cluster.Path(30, "/kv"));
    kv->after_write = [] { throw std::runtime_error("crash after durable KV write"); };
    Throws([&] { cluster.Node(30).HandleInstallSnapshot(10, InstallRequest(donor.ExportSnapshot())); });
    kv->after_write = {};
    // The in-memory marker did not advance, but the persisted KV batch did.
    Check(cluster.State(30).LastApplied() == 0, "fixture did not interrupt KV completion");
    cluster.Restart(30);
    std::string value;
    Check(cluster.State(30).Get("default:ambiguous", &value) && value == "v",
          "ambiguous KV write could not replay");
    Check(cluster.Node(30).SnapshotIndex() == 1, "ambiguous replay not finalized");
}


static void OversizedAutomaticSnapshotRetainsReplicationAndRecovery() {
    Cluster cluster;
    cluster.Elect(10);
    // 513 legal 64 KiB values exceed 32 MiB, while each command stays < 1 MiB.
    for (int id : {10, 30, 50}) cluster.Node(id).SetSnapshotThreshold(514);
    cluster.Partition(50);
    const std::string value(64 * 1024, 'x');
    int callbacks = 0;
    for (int i = 0; i < 513; ++i) {
        Check(cluster.Node(10).Propose(Command({"SET", "default:large:" + std::to_string(i), value}),
            [&](bool ok, const std::string& reply) {
                Check(ok && reply == "+OK\r\n", "large SET failed"); ++callbacks;
            }) > 0, "large proposal rejected");
        cluster.Pump();
    }
    cluster.Settle();
    Check(callbacks == 513, "large write acknowledgements missing");
    Check(Metric(cluster.Node(10), "snapshot_oversize_skips") == 1, "oversize skip not counted");
    Check(cluster.Node(10).SnapshotIndex() == 0, "oversized snapshot compacted log");
    cluster.Settle();
    Check(Metric(cluster.Node(10), "snapshot_oversize_skips") == 1, "idle retry rebuilt oversized snapshot");
    cluster.Heal(50);
    for (int i = 0; i < 30; ++i) cluster.Settle();
    std::string got;
    Check(cluster.State(50).Get("default:large:512", &got) && got == value,
          "retained log did not catch up lagging peer");
    cluster.Restart(10);
    cluster.Node(10).SetSnapshotThreshold(514);
    for (int i = 0; i < 513; ++i)
        Check(cluster.State(10).Get("default:large:" + std::to_string(i), &got) && got == value,
              "restart lost oversized state");
    cluster.Elect(10);
    Check(cluster.Node(10).IsLeader(), "restart could not elect leader");
    Check(Metric(cluster.Node(10), "snapshot_oversize_skips") == 1, "restart did not safely skip oversized state");
    for (int i = 0; i < 513; ++i) {
        Check(cluster.Node(10).Propose(Command({"DEL", "default:large:" + std::to_string(i)}),
            [](bool ok, const std::string&) { Check(ok, "delete failed"); }) > 0, "delete rejected");
        cluster.Pump();
    }
    Check(cluster.Node(10).Propose(Command({"SET", "default:small", "restored"}),
        [](bool ok, const std::string&) { Check(ok, "post-shrink write failed"); }) > 0, "small write rejected");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).SnapshotIndex() > 0, "snapshot did not resume after shrink and retry interval");
    cluster.Restart(10);
    Check(cluster.State(10).Get("default:small", &got) && got == "restored", "small recovery failed");
}

int main() {
    try {
        OversizedAutomaticSnapshotRetainsReplicationAndRecovery();
        SnapshotKVCommitCrashRecovers();
        SnapshotIntentBeforeKVAndRepeatedRecoveryFailure();
        SnapshotIntentFailureDoesNotMutateKV();
        InvalidSnapshotDoesNotPersistIntent();
        SnapshotStoreValidationPrecedesIntent();
        AmbiguousKVWriteCanReplay();
        SnapshotSurvivesRestart();
        LaggingPeerInstallsChunkedSnapshot();
        InterruptedInstallRestartsAndReplicates();
        DivergentFollowerJumpsToSnapshot();
        AppliedEntriesStayWithinSnapshotThreshold();
        OlderSnapshotDoesNotReplaceState();
        std::cout << "PASS: snapshots\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
