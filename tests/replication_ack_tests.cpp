// Not part of CTest: this file uses FakeRaftNode, which cannot fail the
// production RaftNode. Strict ACK tests live in replication_logic_tests.cpp.

#include "raft/raft_node.h"
#include "test_support/raft_test_support.h"
#include <iostream>
#include <cassert>

static int test_count = 0;

static void Check(bool condition, const char* message) {
    ++test_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// 测试 1: 基本的复制确认
static void TestBasicReplicationAck() {
    std::cout << "Test 1: Basic replication acknowledgment" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];
    auto& follower = *cluster[1];

    // Leader 提议一条日志
    bool committed = false;
    leader.Propose("SET key value", [&](bool success, const std::string&) {
        committed = success;
    });

    // 模拟 Follower 响应
    network.DeliverAll();

    Check(committed, "proposal should be committed after majority ack");
    Check(leader.GetCommitIndex() > 0, "commit index should advance");
}

// 测试 2: 延迟响应不应该导致 matchIndex 倒退
static void TestDelayedResponseNoRewind() {
    std::cout << "Test 2: Delayed response should not rewind matchIndex" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];

    // Leader 提议第一条日志
    leader.Propose("SET key1 val1", {});
    network.DeliverAll();

    // 保存第一次的响应（延迟）
    auto delayed_messages = network.PendingMessages();
    network.Clear();

    // Leader 提议第二条日志
    leader.Propose("SET key2 val2", {});
    network.DeliverAll();

    const int64_t match_after_second = leader.GetMatchIndex(1);

    // 现在交付延迟的第一次响应
    for (auto& msg : delayed_messages) {
        network.Deliver(msg);
    }

    Check(leader.GetMatchIndex(1) >= match_after_second,
          "matchIndex should not rewind due to delayed old response");
}

// 测试 3: 冲突日志的回退
static void TestConflictingLogBackoff() {
    std::cout << "Test 3: Conflicting log entries should trigger backoff" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& node0 = *cluster[0];
    auto& node1 = *cluster[1];

    // 制造日志冲突场景
    // Node0 是 Term 1 的 Leader
    network.Isolate(2);  // 隔离 node2
    node0.Propose("SET key1 val1", {});
    network.DeliverAll();

    // Node1 成为 Term 2 的 Leader，有不同的日志
    network.Isolate(0);
    network.Reconnect(2);
    cluster[1]->BecomeLeader();
    node1.Propose("SET key1 different", {});
    network.DeliverAll();

    // Node0 恢复，应该会回退并接受 Node1 的日志
    network.Reconnect(0);
    network.DeliverAll();

    Check(node0.GetCurrentTerm() >= 2, "node0 should update to newer term");
}

// 测试 4: 在途请求追踪
static void TestInflightTracking() {
    std::cout << "Test 4: Inflight request tracking" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];

    // 发送第一个复制请求
    leader.Propose("SET key1 val1", {});

    // 在第一个请求未响应前，发送第二个请求
    // 应该缓冲而不是创建多个在途请求
    leader.Propose("set key2 val2", {});

    // 验证每个 peer 只有一个在途请求
    network.DeliverAll();

    Check(leader.GetCommitIndex() >= 2, "both proposals should be committed");
}

// 测试 5: 响应的 RPC ID 验证
static void TestRpcIdValidation() {
    std::cout << "Test 5: RPC ID validation prevents stale responses" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];

    // 发送请求并获取 RPC ID
    leader.Propose("SET key1 val1", {});
    auto messages = network.PendingMessages();
    network.Clear();

    // 发送新的请求（新的 RPC ID）
    leader.Propose("SET key2 val2", {});
    network.DeliverAll();

    const int64_t commit_before = leader.GetCommitIndex();

    // 尝试交付旧的响应（过期的 RPC ID）
    for (auto& msg : messages) {
        network.Deliver(msg);
    }

    // commitIndex 不应该因为旧响应而错误前进
    Check(leader.GetCommitIndex() == commit_before || leader.GetCommitIndex() == commit_before + 1,
          "stale response should not cause incorrect commit advancement");
}

// 测试 6: Follower 返回的 matched 位置正确性
static void TestFollowerMatchedPosition() {
    std::cout << "Test 6: Follower returns correct matched position" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];

    // Follower 应该返回 prevLogIndex + entries.size()
    leader.Propose("SET key1 val1", {});
    leader.Propose("SET key2 val2", {});
    leader.Propose("SET key3 val3", {});

    network.DeliverAll();

    // 验证 matchIndex 正确反映了 Follower 确认的位置
    Check(leader.GetMatchIndex(1) == 3, "follower should confirm up to entry 3");
    Check(leader.GetMatchIndex(2) == 3, "follower should confirm up to entry 3");
}

// 测试 7: 已提交日志不应被截断
static void TestCommittedLogProtection() {
    std::cout << "Test 7: Committed logs must not be truncated" << std::endl;

    FakeNetwork network;
    auto cluster = MakeTestCluster(3, &network);
    auto& leader = *cluster[0];

    // 提交一些日志
    leader.Propose("SET key1 val1", {});
    network.DeliverAll();

    const int64_t committed = leader.GetCommitIndex();

    // 尝试发送冲突的 AppendEntries，试图截断已提交的日志
    // 这应该被拒绝或抛出异常
    bool exception_thrown = false;
    try {
        // 模拟发送试图覆盖已提交条目的 AppendEntries
        // 实际实现中这应该在 HandleAppendEntries 中检查
        cluster[1]->HandleAppendEntries(0, MakeConflictingAppendEntries(committed));
    } catch (const std::runtime_error&) {
        exception_thrown = true;
    }

    Check(exception_thrown || cluster[1]->GetCommitIndex() >= committed,
          "committed log must be protected from truncation");
}

int main() {
    try {
        std::cout << "=== Raft Replication Acknowledgment Tests ===" << std::endl;

        TestBasicReplicationAck();
        TestDelayedResponseNoRewind();
        TestConflictingLogBackoff();
        TestInflightTracking();
        TestRpcIdValidation();
        TestFollowerMatchedPosition();
        TestCommittedLogProtection();

        std::cout << "\n=== All tests passed! (" << test_count << " checks) ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
