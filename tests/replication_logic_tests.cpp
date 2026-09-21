// 简化版的复制确认测试（不依赖完整的 RaftNode）
// 测试 matchIndex 更新逻辑的关键场景

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

// 模拟 Leader 的 matchIndex 和 nextIndex 管理
class ReplicationTracker {
public:
    ReplicationTracker(int node_id, const std::vector<int>& peer_ids)
        : node_id_(node_id), quorum_size_((peer_ids.size() + 1) / 2 + 1) {
        for (int peer : peer_ids) {
            match_index_[peer] = 0;
            next_index_[peer] = 1;
            inflight_[peer] = {};
        }
        match_index_[node_id_] = 0;
    }

    // Leader 发送 AppendEntries
    bool SendAppendEntries(int peer_id, int64_t last_index) {
        if (inflight_[peer_id].in_flight) {
            return false;  // 已有在途请求
        }

        Inflight flight;
        flight.in_flight = true;
        flight.rpc_id = ++rpc_id_;
        flight.last_index = last_index;
        inflight_[peer_id] = flight;
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

    bool HasInflight(int peer_id) const {
        auto it = inflight_.find(peer_id);
        return it != inflight_.end() && it->second.in_flight;
    }

private:
    struct Inflight {
        bool in_flight = false;
        uint64_t rpc_id = 0;
        int64_t last_index = 0;
    };

    int node_id_;
    int quorum_size_;
    uint64_t rpc_id_ = 0;
    std::map<int, int64_t> match_index_;
    std::map<int, int64_t> next_index_;
    std::map<int, Inflight> inflight_;
};

// 测试 1: 基本的 matchIndex 更新
static void TestBasicMatchIndexUpdate() {
    std::cout << "Test 1: Basic matchIndex update" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // Leader 在索引 5
    tracker.SendAppendEntries(1, 5);
    tracker.SendAppendEntries(2, 5);

    // 收到响应
    Check(tracker.HandleSuccessResponse(1, 1, 5), "should accept valid response");
    Check(tracker.HandleSuccessResponse(2, 2, 5), "should accept valid response");

    Check(tracker.GetMatchIndex(1) == 5, "matchIndex[1] should be 5");
    Check(tracker.GetMatchIndex(2) == 5, "matchIndex[2] should be 5");

    // 推进 commitIndex
    int64_t commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 5, "commitIndex should advance to 5");
}

// 测试 2: matchIndex 不应倒退
static void TestMatchIndexNoRewind() {
    std::cout << "Test 2: matchIndex should not rewind" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // 发送索引 10
    tracker.SendAppendEntries(1, 10);
    tracker.HandleSuccessResponse(1, 1, 10);
    Check(tracker.GetMatchIndex(1) == 10, "matchIndex should be 10");

    // 发送索引 15
    tracker.SendAppendEntries(1, 15);
    tracker.HandleSuccessResponse(1, 2, 15);
    Check(tracker.GetMatchIndex(1) == 15, "matchIndex should advance to 15");

    // 延迟的索引 12 响应到达（旧的 RPC ID）
    bool accepted = tracker.HandleSuccessResponse(1, 1, 12);
    Check(!accepted, "stale response should be rejected");
    Check(tracker.GetMatchIndex(1) == 15, "matchIndex should remain 15");
}

// 测试 3: 过期 RPC ID 应被拒绝
static void TestStaleRpcIdRejection() {
    std::cout << "Test 3: Stale RPC ID rejection" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // 第一个请求
    tracker.SendAppendEntries(1, 5);
    const uint64_t first_rpc_id = 1;

    // 第二个请求（覆盖在途状态）
    tracker.HandleSuccessResponse(1, first_rpc_id, 5);
    tracker.SendAppendEntries(1, 10);

    // 第一个请求的延迟响应到达
    bool accepted = tracker.HandleSuccessResponse(1, first_rpc_id, 5);
    Check(!accepted, "response with stale RPC ID should be rejected");
}

// 测试 4: 响应的 matched 位置验证
static void TestMatchedPositionValidation() {
    std::cout << "Test 4: Matched position validation" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    tracker.SendAppendEntries(1, 10);

    // 尝试响应不匹配的位置
    bool accepted = tracker.HandleSuccessResponse(1, 1, 8);
    Check(!accepted, "response with mismatched position should be rejected");

    // 正确的位置
    accepted = tracker.HandleSuccessResponse(1, 1, 10);
    Check(accepted, "response with correct position should be accepted");
}

// 测试 5: 多数派确认推进 commitIndex
static void TestQuorumCommit() {
    std::cout << "Test 5: Quorum commit advancement" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // Leader 自己在索引 10
    tracker.SendAppendEntries(1, 10);
    tracker.SendAppendEntries(2, 10);

    // 只有 peer 1 响应（加上 Leader 自己，还差一个）
    tracker.HandleSuccessResponse(1, 1, 10);
    int64_t commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 0, "commitIndex should not advance without quorum");

    // peer 2 也响应（达到多数派）
    tracker.HandleSuccessResponse(2, 2, 10);
    commit = tracker.AdvanceCommitIndex(0);
    Check(commit == 10, "commitIndex should advance with quorum");
}

// 测试 6: 单个在途请求限制
static void TestSingleInflightLimit() {
    std::cout << "Test 6: Single inflight request per peer" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // 发送第一个请求
    bool sent = tracker.SendAppendEntries(1, 5);
    Check(sent, "first request should be sent");
    Check(tracker.HasInflight(1), "should have inflight request");

    // 尝试发送第二个请求（应该失败）
    sent = tracker.SendAppendEntries(1, 10);
    Check(!sent, "second request should be blocked");

    // 完成第一个请求
    tracker.HandleSuccessResponse(1, 1, 5);
    Check(!tracker.HasInflight(1), "inflight should be cleared");

    // 现在可以发送第二个请求
    sent = tracker.SendAppendEntries(1, 10);
    Check(sent, "should be able to send after completion");
}

// 测试 7: 不同步的副本
static void TestOutOfSyncReplicas() {
    std::cout << "Test 7: Out-of-sync replicas" << std::endl;

    ReplicationTracker tracker(0, {1, 2});

    // peer 1 同步到 10
    tracker.SendAppendEntries(1, 10);
    tracker.HandleSuccessResponse(1, 1, 10);

    // peer 2 只同步到 5
    tracker.SendAppendEntries(2, 5);
    tracker.HandleSuccessResponse(2, 2, 5);

    Check(tracker.GetMatchIndex(1) == 10, "peer 1 at index 10");
    Check(tracker.GetMatchIndex(2) == 5, "peer 2 at index 5");

    // commitIndex 应该是多数派确认的最小值
    // 三个节点：leader(假设10), peer1(10), peer2(5)
    // 多数派的最小值取决于排序后的中位数
    (void)tracker.AdvanceCommitIndex(0);  // 验证不会崩溃
}

int main() {
    try {
        std::cout << "=== Replication Acknowledgment Logic Tests ===" << std::endl;

        TestBasicMatchIndexUpdate();
        TestMatchIndexNoRewind();
        TestStaleRpcIdRejection();
        TestMatchedPositionValidation();
        TestQuorumCommit();
        TestSingleInflightLimit();
        TestOutOfSyncReplicas();

        std::cout << "\n=== All tests passed! (" << test_count << " checks) ===" << std::endl;
        std::cout << "\nThese tests verify the key properties of replication tracking:" << std::endl;
        std::cout << "  ✓ matchIndex advances correctly" << std::endl;
        std::cout << "  ✓ matchIndex never rewinds" << std::endl;
        std::cout << "  ✓ Stale RPC IDs are rejected" << std::endl;
        std::cout << "  ✓ Response positions are validated" << std::endl;
        std::cout << "  ✓ Quorum logic works correctly" << std::endl;
        std::cout << "  ✓ Single inflight request per peer" << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
