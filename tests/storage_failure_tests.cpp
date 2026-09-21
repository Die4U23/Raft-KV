// Tests for storage failure handling and health tracking.
// Uses the test double RocksDB with fault injection capabilities.
#include "raft/raft_node.h"
#include "raft/apply_executor.h"
#include <iostream>
#include <stdexcept>

static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected exception from storage failure");
}

static std::string Command(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}

struct Message { int from, to; RaftMsgType type; std::string payload; };

// Test that log append failure marks node as unhealthy
static void LogAppendFailureMarksUnhealthy() {
    const std::vector<PeerInfo> peers = {{10, "127.0.0.1", 9010}};
    const std::string log_path = "test-log-append-failure/raft";
    const std::string kv_path = "test-log-append-failure/kv";

    auto sm = std::make_unique<KVStateMachine>(kv_path);
    auto transport = std::make_unique<PeerManager>(10, peers,
        [](int, int, RaftMsgType, const std::string&) {});

    auto raft = std::make_unique<RaftNode>(10, peers, nullptr, log_path,
                                          sm.get(), transport.get());
    raft->Start();

    // Become leader (single-node cluster)
    for (int i = 0; i < 50; ++i) raft->Tick();
    Check(raft->IsLeader(), "failed to become leader");
    Check(raft->IsHealthy(), "node should start healthy");

    // Inject log write failure
    auto state = rocksdb::testing::StateFor(log_path);
    state->fail_writes = 1;

    // Attempt to propose - should fail and mark unhealthy
    bool callback_invoked = false;
    Throws([&]() {
        raft->Propose(Command({"SET", "key", "value"}),
                     [&](bool ok, const std::string&) { callback_invoked = true; });
    });

    Check(!raft->IsHealthy(), "node should be marked unhealthy after log failure");
    Check(!callback_invoked, "callback should not be invoked on storage failure");

    // Further proposals should be rejected with -3
    const auto result = raft->Propose(Command({"SET", "key2", "value2"}),
                                     [](bool, const std::string&) {});
    Check(result == -3, "proposals should be rejected when unhealthy");
}

// Test that state machine apply failure marks node as unhealthy
static void StateMachineApplyFailureMarksUnhealthy() {
    const std::vector<PeerInfo> peers = {{10, "127.0.0.1", 9010}};
    const std::string log_path = "test-sm-apply-failure/raft";
    const std::string kv_path = "test-sm-apply-failure/kv";

    auto sm = std::make_unique<KVStateMachine>(kv_path);
    auto transport = std::make_unique<PeerManager>(10, peers,
        [](int, int, RaftMsgType, const std::string&) {});

    auto raft = std::make_unique<RaftNode>(10, peers, nullptr, log_path,
                                          sm.get(), transport.get());
    raft->Start();

    // Become leader
    for (int i = 0; i < 50; ++i) raft->Tick();
    Check(raft->IsLeader(), "failed to become leader");
    Check(raft->IsHealthy(), "node should start healthy");

    // First propose succeeds and applies
    raft->Propose(Command({"SET", "key1", "value1"}), [](bool, const std::string&) {});
    for (int i = 0; i < 10; ++i) raft->Tick();
    Check(raft->IsHealthy(), "node should still be healthy");

    // Now inject KV apply failure for next apply
    auto kv_state = rocksdb::testing::StateFor(kv_path);
    kv_state->fail_writes = 1;

    // Propose another command - apply should fail
    Throws([&]() {
        raft->Propose(Command({"SET", "key2", "value2"}), [](bool, const std::string&) {});
    });

    Check(!raft->IsHealthy(), "node should be unhealthy after apply failure");
}

// Test that follower log append failure marks node as unhealthy
static void FollowerLogAppendFailureMarksUnhealthy() {
    const std::vector<PeerInfo> peers = {{10, "127.0.0.1", 9010}, {20, "127.0.0.1", 9020}};
    const std::string log_path = "test-follower-append-failure/raft";
    const std::string kv_path = "test-follower-append-failure/kv";

    auto sm = std::make_unique<KVStateMachine>(kv_path);
    auto transport = std::make_unique<PeerManager>(20, peers,
        [](int, int, RaftMsgType, const std::string&) {});

    auto raft = std::make_unique<RaftNode>(20, peers, nullptr, log_path,
                                          sm.get(), transport.get());
    raft->Start();
    Check(raft->IsHealthy(), "follower should start healthy");

    // First, receive an empty AppendEntries to become follower of term 1
    raftcore::AppendEntries heartbeat;
    heartbeat.set_term(1);
    heartbeat.set_leader_id(10);
    heartbeat.set_rpc_id(1);
    heartbeat.set_prev_log_index(0);
    heartbeat.set_prev_log_term(0);
    heartbeat.set_leader_commit(0);
    raft->HandleAppendEntries(10, heartbeat);
    Check(raft->IsHealthy(), "follower should still be healthy after heartbeat");

    // Now inject log write failure
    auto state = rocksdb::testing::StateFor(log_path);
    state->fail_writes = 1;

    // Receive AppendEntries with actual entry - should fail on log append
    raftcore::AppendEntries request;
    request.set_term(1);
    request.set_leader_id(10);
    request.set_rpc_id(2);
    request.set_prev_log_index(0);
    request.set_prev_log_term(0);
    request.set_leader_commit(0);
    auto* entry = request.add_entries();
    entry->set_index(1);
    entry->set_term(1);
    entry->set_command(Command({"SET", "key", "value"}));

    // Handle should throw due to storage failure
    Throws([&]() {
        raft->HandleAppendEntries(10, request);
    });

    Check(!raft->IsHealthy(), "follower should be unhealthy after log failure");
}

// Test that healthy status is preserved on successful operations
static void HealthyStatusPreservedOnSuccess() {
    const std::vector<PeerInfo> peers = {{10, "127.0.0.1", 9010}};
    const std::string log_path = "test-healthy-success/raft";
    const std::string kv_path = "test-healthy-success/kv";

    auto sm = std::make_unique<KVStateMachine>(kv_path);
    auto transport = std::make_unique<PeerManager>(10, peers,
        [](int, int, RaftMsgType, const std::string&) {});

    auto raft = std::make_unique<RaftNode>(10, peers, nullptr, log_path,
                                          sm.get(), transport.get());
    raft->Start();

    // Become leader
    for (int i = 0; i < 50; ++i) raft->Tick();
    Check(raft->IsLeader(), "failed to become leader");
    Check(raft->IsHealthy(), "node should be healthy");

    // Successful operations
    int callbacks = 0;
    for (int i = 0; i < 5; ++i) {
        const auto index = raft->Propose(Command({"SET", "key" + std::to_string(i), "val"}),
                                        [&](bool ok, const std::string&) {
                                            if (ok) ++callbacks;
                                        });
        Check(index > 0, "proposal should succeed");
    }

    // Tick to apply
    for (int i = 0; i < 20; ++i) raft->Tick();

    Check(raft->IsHealthy(), "node should remain healthy after successful operations");
    Check(callbacks == 5, "all callbacks should succeed");
}

int main() {
    try {
        const std::pair<const char*, void(*)()> tests[] = {
            {"log append failure marks node unhealthy", LogAppendFailureMarksUnhealthy},
            {"state machine apply failure marks node unhealthy", StateMachineApplyFailureMarksUnhealthy},
            {"follower log append failure marks node unhealthy", FollowerLogAppendFailureMarksUnhealthy},
            {"healthy status preserved on success", HealthyStatusPreservedOnSuccess},
        };
        for (const auto& test : tests) {
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        }
        std::cout << "PASS: " << sizeof(tests) / sizeof(tests[0]) << " storage failure tests\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
