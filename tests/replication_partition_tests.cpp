// 扩展的复制确认测试 - 网络分区和边界场景
// 补充 replication_logic_tests.cpp 未覆盖的场景

#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <cassert>
#include <cstdint>

static int test_count = 0;

static void Check(bool condition, const char* message) {
    ++test_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// 模拟网络分区场景的复制确认追踪
class PartitionedReplicationTracker {
public:
    PartitionedReplicationTracker(int node_id, const std::vector<int>& peer_ids)
        : node_id_(node_id), quorum_size_((peer_ids.size() + 1) / 2 + 1) {
        for (int peer : peer_ids) {
            match_index_[peer] = 0;
            next_index_[peer] = 1;
            inflight_[peer] = {};
            partitioned_[peer] = false;  // 默认所有节点可达
        }
        match_index_[node_id_] = 0;
    }

    // 模拟网络分区
    void PartitionPeer(int peer_id) {
        partitioned_[peer_id] = true;
    }

    // 恢复网络连接
    void RecoverPeer(int peer_id) {
        partitioned_[peer_id] = false;
    }

    // 发送 AppendEntries（分区的节点无法发送）
    bool SendAppendEntries(int peer_id, int64_t last_index) {
        if (partitioned_[peer_id]) {
            return false;  // 网络分区，发送失败
        }

        if (inflight_[peer_id].in_flight) {
            return false;  // 已有在途请求
        }

        Inflight flight;
        flight.in_flight = true;
        flight.rpc_id = ++rpc_id_;
        flight.last_index = last_index;
        flight.retry_count = 0;
        inflight_[peer_id] = flight;
        return true;
    }

    // 重试发送
    bool RetrySend(int peer_id) {
        if (partitioned_[peer_id]) {
            return false;
        }

        auto& flight = inflight_[peer_id];
        if (!flight.in_flight) {
            return false;
        }

        flight.retry_count++;
        return true;
    }

    // 处理成功响应
    bool HandleSuccessResponse(int peer_id, uint64_t rpc_id, int64_t matched) {
        auto& flight = inflight_[peer_id];

        // 验证 RPC ID
        if (!flight.in_flight || flight.rpc_id != rpc_id) {
            return false;  // 过期或无效的响应
        }

        // 验证匹配位置与请求对应
        if (matched != flight.last_index) {
            return false;  // 不匹配
        }

        // 更新 matchIndex（不允许倒退）
        match_index_[peer_id] = std::max(match_index_[peer_id], matched);
        next_index_[peer_id] = match_index_[peer_id] + 1;

        flight.in_flight = false;
        return true;
    }

    // 推进 commitIndex
    int64_t AdvanceCommitIndex(int64_t current_commit) {
        std::vector<int64_t> matches;
        for (auto& pair : match_index_) {
            matches.push_back(pair.second);
        }
        std::sort(matches.begin(), matches.end(), std::greater<int64_t>());

        int64_t candidate = matches[quorum_size_ - 1];
        return std::max(current_commit, candidate);
    }

    int64_t GetMatchIndex(int peer_id) const {
        auto it = match_index_.find(peer_id);
        return it != match_index_.end() ? it->second : 0;
    }

    int GetRetryCount(int peer_id) const {
        auto it = inflight_.find(peer_id);
        return it != inflight_.end() ? it->second.retry_count : 0;
    }

private:
    struct Inflight {
        bool in_flight = false;
        uint64_t rpc_id = 0;
        int64_t last_index = 0;
        int retry_count = 0;
    };

    int node_id_;
    int quorum_size_;
    uint64_t rpc_id_ = 0;
    std::map<int, int64_t> match_index_;
    std::map<int, int64_t> next_index_;
    std::map<int, Inflight> inflight_;
    std::map<int, bool> partitioned_;  // 网络分区状态
};

// 测试 1: 网络分区场景下的复制确认
static void TestReplicationWithPartition() {
    std::cout << "Test 1: Replication with network partition" << std::endl;

    PartitionedReplicationTracker tracker(0, {1, 2});

    // 分区 peer 2
    tracker.PartitionPeer(2);

    // 尝试向所有节点发送
    bool sent1 = tracker.SendAppendEntries(1, 10);
    bool sent2 = tracker.SendAppendEntries(2, 10);

    Check(sent1, "should send to non-partitioned peer");
    Check(!sent2, "should not send to partitioned peer");

    // 只有 peer 1 响应
    tracker.HandleSuccessResponse(1, 1, 10);

    // 此时还没有多数派（leader + peer1 = 2，需要3个）
    int64_t commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 0, "should not commit without majority");

    // 恢复 peer 2
    tracker.RecoverPeer(2);
    bool sent3 = tracker.SendAppendEntries(2, 10);
    Check(sent3, "should send after partition recovery");

    // peer 2 响应
    tracker.HandleSuccessResponse(2, 2, 10);

    // 现在有多数派了
    commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 10, "should commit with majority after recovery");
}

// 测试 2: 多次重试的边界情况
static void TestMultipleRetries() {
    std::cout << "Test 2: Multiple retry attempts" << std::endl;

    PartitionedReplicationTracker tracker(0, {1, 2});

    // 发送初始请求
    tracker.SendAppendEntries(1, 5);
    Check(tracker.GetRetryCount(1) == 0, "initial retry count should be 0");

    // 模拟多次重试
    for (int i = 0; i < 5; i++) {
        bool retried = tracker.RetrySend(1);
        Check(retried, "retry should succeed");
    }

    Check(tracker.GetRetryCount(1) == 5, "retry count should be 5");

    // 最终收到响应
    bool accepted = tracker.HandleSuccessResponse(1, 1, 5);
    Check(accepted, "should accept response after retries");
    Check(tracker.GetMatchIndex(1) == 5, "matchIndex should be updated to 5");
}

// 测试 3: 分区恢复后的追赶
static void TestCatchUpAfterPartition() {
    std::cout << "Test 3: Catch up after partition recovery" << std::endl;

    PartitionedReplicationTracker tracker(0, {1, 2});

    // peer 2 被分区
    tracker.PartitionPeer(2);

    // Leader 和 peer 1 继续前进
    tracker.SendAppendEntries(1, 5);
    tracker.HandleSuccessResponse(1, 1, 5);

    tracker.SendAppendEntries(1, 10);
    tracker.HandleSuccessResponse(1, 2, 10);

    tracker.SendAppendEntries(1, 15);
    tracker.HandleSuccessResponse(1, 3, 15);

    Check(tracker.GetMatchIndex(1) == 15, "peer 1 should be at 15");
    Check(tracker.GetMatchIndex(2) == 0, "peer 2 should be at 0");

    // 恢复 peer 2
    tracker.RecoverPeer(2);

    // peer 2 需要追赶
    tracker.SendAppendEntries(2, 15);
    tracker.HandleSuccessResponse(2, 4, 15);

    Check(tracker.GetMatchIndex(2) == 15, "peer 2 should catch up to 15");
}

// 测试 4: 交替分区
static void TestAlternatingPartitions() {
    std::cout << "Test 4: Alternating partitions" << std::endl;

    PartitionedReplicationTracker tracker(0, {1, 2});

    // 第一阶段：分区 peer 2
    tracker.PartitionPeer(2);
    tracker.SendAppendEntries(1, 10);
    tracker.HandleSuccessResponse(1, 1, 10);

    // 第二阶段：分区 peer 1，恢复 peer 2
    tracker.PartitionPeer(1);
    tracker.RecoverPeer(2);
    tracker.SendAppendEntries(2, 10);
    tracker.HandleSuccessResponse(2, 2, 10);

    // 两个节点都到了 10，但是是通过不同的分区阶段
    Check(tracker.GetMatchIndex(1) == 10, "peer 1 should be at 10");
    Check(tracker.GetMatchIndex(2) == 10, "peer 2 should be at 10");
}

// 测试 5: 所有节点被分区（失去多数派）
static void TestLoseQuorum() {
    std::cout << "Test 5: Lose quorum (all peers partitioned)" << std::endl;

    PartitionedReplicationTracker tracker(0, {1, 2});

    // 分区所有 peer
    tracker.PartitionPeer(1);
    tracker.PartitionPeer(2);

    // 无法发送到任何节点
    bool sent1 = tracker.SendAppendEntries(1, 10);
    bool sent2 = tracker.SendAppendEntries(2, 10);

    Check(!sent1, "should not send to partitioned peer 1");
    Check(!sent2, "should not send to partitioned peer 2");

    // 无法形成多数派
    int64_t commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 0, "should not commit without quorum");
}

int main() {
    try {
        std::cout << "=== Extended Replication Acknowledgment Tests ===" << std::endl;
        std::cout << "Testing network partition and boundary scenarios\n" << std::endl;

        TestReplicationWithPartition();
        TestMultipleRetries();
        TestCatchUpAfterPartition();
        TestAlternatingPartitions();
        TestLoseQuorum();

        std::cout << "\n=== All extended tests passed! (" << test_count << " checks) ===" << std::endl;
        std::cout << "\nThese tests verify partition-related replication scenarios:" << std::endl;
        std::cout << "  ✓ Replication during network partitions" << std::endl;
        std::cout << "  ✓ Multiple retry attempts handling" << std::endl;
        std::cout << "  ✓ Catch-up after partition recovery" << std::endl;
        std::cout << "  ✓ Alternating partition scenarios" << std::endl;
        std::cout << "  ✓ Quorum loss detection" << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
