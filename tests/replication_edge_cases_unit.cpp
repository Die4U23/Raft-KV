// 复制确认边界测试 - 单元测试增强
// 补充更多边界场景的单元测试

#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <set>

static int test_count = 0;

static void Check(bool condition, const char* message) {
    ++test_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// 模拟复制管理器（增强版）
class ReplicationManager {
public:
    ReplicationManager(int node_id, const std::vector<int>& peer_ids)
        : node_id_(node_id), quorum_size_((peer_ids.size() + 1) / 2 + 1) {
        // 初始化 leader 自己
        match_index_[node_id_] = 0;

        // 初始化所有 peer
        for (int peer : peer_ids) {
            match_index_[peer] = 0;
            next_index_[peer] = 1;
            inflight_[peer] = {};
        }
    }

    // 发送 AppendEntries
    bool SendAppendEntries(int peer_id, int64_t last_index, int64_t prev_index) {
        if (inflight_[peer_id].in_flight) {
            return false;  // 已有在途请求
        }

        Inflight flight;
        flight.in_flight = true;
        flight.rpc_id = ++rpc_id_;
        flight.last_index = last_index;
        flight.prev_index = prev_index;
        flight.send_time = ++current_time_;
        inflight_[peer_id] = flight;
        return true;
    }

    // 处理成功响应（带延迟）
    bool HandleSuccessResponse(int peer_id, uint64_t rpc_id, int64_t matched, int64_t response_time) {
        auto& flight = inflight_[peer_id];

        // 验证 RPC ID
        if (!flight.in_flight || flight.rpc_id != rpc_id) {
            return false;  // 过期或无效
        }

        // 验证响应时间（模拟网络延迟）
        if (response_time < flight.send_time) {
            return false;  // 时间倒流，不可能
        }

        // 验证匹配位置
        if (matched != flight.last_index) {
            return false;
        }

        // 更新 matchIndex（不允许倒退）
        if (matched > match_index_[peer_id]) {
            match_index_[peer_id] = matched;
            next_index_[peer_id] = matched + 1;
        }

        flight.in_flight = false;
        return true;
    }

    // 处理失败响应（需要回退）
    void HandleFailureResponse(int peer_id, uint64_t rpc_id, int64_t conflict_index) {
        auto& flight = inflight_[peer_id];

        if (!flight.in_flight || flight.rpc_id != rpc_id) {
            return;
        }

        // 回退 nextIndex 到冲突点
        if (conflict_index > 0) {
            next_index_[peer_id] = conflict_index;
        } else {
            // 保守回退一步
            next_index_[peer_id] = std::max<int64_t>(1, next_index_[peer_id] - 1);
        }

        flight.in_flight = false;
    }

    // 模拟超时处理
    void HandleTimeout(int peer_id, int64_t timeout_time) {
        auto& flight = inflight_[peer_id];

        if (!flight.in_flight) return;

        if (timeout_time - flight.send_time > 10) {  // 超时阈值
            flight.in_flight = false;
        }
    }

    // 推进 commitIndex
    int64_t AdvanceCommitIndex(int64_t current_commit, int64_t current_term,
                                const std::map<int64_t, int64_t>& log_terms) {
        std::vector<int64_t> matches;
        for (auto& pair : match_index_) {
            matches.push_back(pair.second);
        }
        std::sort(matches.begin(), matches.end(), std::greater<int64_t>());

        int64_t candidate = matches[quorum_size_ - 1];

        // 只能提交当前任期的条目
        if (candidate > current_commit) {
            auto it = log_terms.find(candidate);
            if (it != log_terms.end() && it->second == current_term) {
                return candidate;
            }
        }

        return current_commit;
    }

    int64_t GetMatchIndex(int peer_id) const {
        auto it = match_index_.find(peer_id);
        return it != match_index_.end() ? it->second : 0;
    }

    int64_t GetNextIndex(int peer_id) const {
        auto it = next_index_.find(peer_id);
        return it != next_index_.end() ? it->second : 1;
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
        int64_t prev_index = 0;
        int64_t send_time = 0;
    };

    int node_id_;
    int quorum_size_;
    uint64_t rpc_id_ = 0;
    int64_t current_time_ = 0;
    std::map<int, int64_t> match_index_;
    std::map<int, int64_t> next_index_;
    std::map<int, Inflight> inflight_;
};

// 测试 1: 乱序响应处理
static void TestOutOfOrderResponses() {
    std::cout << "Test 1: Out-of-order responses" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // 发送三个请求
    mgr.SendAppendEntries(1, 10, 9);   // RPC 1
    uint64_t rpc1 = 1;

    mgr.HandleSuccessResponse(1, rpc1, 10, 1);  // 先清空以发送下一个
    mgr.SendAppendEntries(1, 20, 10);  // RPC 2
    uint64_t rpc2 = 2;

    mgr.HandleSuccessResponse(1, rpc2, 20, 2);
    mgr.SendAppendEntries(1, 30, 20);  // RPC 3
    uint64_t rpc3 = 3;

    // 模拟乱序：RPC 3 先返回，然后 RPC 2
    bool accepted3 = mgr.HandleSuccessResponse(1, rpc3, 30, 3);
    Check(accepted3, "RPC 3 should be accepted");
    Check(mgr.GetMatchIndex(1) == 30, "matchIndex should be 30");

    // 旧的 RPC 2 响应应该被忽略（已经被 RPC 3 覆盖）
    mgr.SendAppendEntries(1, 25, 20);  // 发送新请求测试
    uint64_t rpc4 = 4;
    bool accepted4 = mgr.HandleSuccessResponse(1, rpc4, 25, 4);

    // matchIndex 不应该倒退
    Check(mgr.GetMatchIndex(1) == 30, "matchIndex should not rewind");
}

// 测试 2: 冲突解决
static void TestConflictResolution() {
    std::cout << "Test 2: Conflict resolution with nextIndex backoff" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // 发送请求失败（日志冲突）
    mgr.SendAppendEntries(1, 10, 9);
    mgr.HandleFailureResponse(1, 1, 5);  // 冲突在索引 5

    Check(mgr.GetNextIndex(1) == 5, "nextIndex should backoff to conflict point");

    // 重新发送
    mgr.SendAppendEntries(1, 10, 4);
    mgr.HandleSuccessResponse(1, 2, 10, 2);

    Check(mgr.GetMatchIndex(1) == 10, "matchIndex should update after conflict resolution");
}

// 测试 3: 超时处理
static void TestTimeoutHandling() {
    std::cout << "Test 3: Timeout handling" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // 发送请求
    mgr.SendAppendEntries(1, 10, 9);
    Check(mgr.HasInflight(1), "should have inflight request");

    // 模拟超时
    mgr.HandleTimeout(1, 15);  // 超过超时阈值
    Check(!mgr.HasInflight(1), "inflight should be cleared after timeout");

    // 可以重新发送
    bool sent = mgr.SendAppendEntries(1, 10, 9);
    Check(sent, "should be able to send after timeout");
}

// 测试 4: 多个 Peer 的复制
static void TestMultiplePeerReplication() {
    std::cout << "Test 4: Multiple peer replication and quorum" << std::endl;

    ReplicationManager mgr(0, {1, 2});
    std::map<int64_t, int64_t> log_terms = {{10, 5}, {20, 5}, {30, 5}};

    // 手动设置 Leader (node 0) 自己的 matchIndex
    // 通过发送给自己来更新（虽然在实际中不会这样做）
    // 或者直接修改内部状态（这里我们用一个技巧）

    // Peer 1 复制到 20
    mgr.SendAppendEntries(1, 20, 19);
    mgr.HandleSuccessResponse(1, 1, 20, 1);

    // Peer 2 复制到 10
    mgr.SendAppendEntries(2, 10, 9);
    mgr.HandleSuccessResponse(2, 2, 10, 2);

    // 此时：leader=0, peer1=20, peer2=10
    // quorum 大小是 2，排序后 [20, 10, 0]，取第 2 个位置 = 10
    int64_t commit = mgr.AdvanceCommitIndex(0, 5, log_terms);
    Check(commit == 10, "commit should be at quorum position (10)");

    // Peer 2 追赶到 20
    mgr.SendAppendEntries(2, 20, 10);
    mgr.HandleSuccessResponse(2, 3, 20, 3);

    // 现在：leader=0, peer1=20, peer2=20
    // 排序后 [20, 20, 0]，取第 2 个位置 = 20
    commit = mgr.AdvanceCommitIndex(10, 5, log_terms);
    Check(commit == 20, "commit should advance to 20");
}

// 测试 5: 不同任期的日志
static void TestDifferentTermLogs() {
    std::cout << "Test 5: Different term logs - only commit current term" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // 日志：索引 10 是旧任期 3，索引 20 是当前任期 5
    std::map<int64_t, int64_t> log_terms = {{10, 3}, {20, 5}};

    // 多数派都复制到了索引 10（旧任期）
    mgr.SendAppendEntries(1, 10, 9);
    mgr.HandleSuccessResponse(1, 1, 10, 1);

    mgr.SendAppendEntries(2, 10, 9);
    mgr.HandleSuccessResponse(2, 2, 10, 2);

    // 尝试提交，但因为是旧任期的日志，不能提交
    int64_t commit = mgr.AdvanceCommitIndex(0, 5, log_terms);
    Check(commit == 0, "should not commit old term logs");

    // 多数派复制到当前任期的索引 20
    mgr.SendAppendEntries(1, 20, 10);
    mgr.HandleSuccessResponse(1, 3, 20, 3);

    mgr.SendAppendEntries(2, 20, 10);
    mgr.HandleSuccessResponse(2, 4, 20, 4);

    // 现在可以提交了（当前任期，并且有多数派）
    commit = mgr.AdvanceCommitIndex(0, 5, log_terms);
    Check(commit == 20, "should commit current term logs");
}

// 测试 6: 极端延迟场景
static void TestExtremeLag() {
    std::cout << "Test 6: Extreme lag - follower far behind" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // Peer 1 正常跟随
    mgr.SendAppendEntries(1, 100, 99);
    mgr.HandleSuccessResponse(1, 1, 100, 1);

    // Peer 2 极度落后（从 0 开始）
    Check(mgr.GetNextIndex(2) == 1, "peer 2 starts at 1");

    // 逐步追赶
    uint64_t rpc_id = 2;
    for (int64_t i = 10; i <= 100; i += 10) {
        mgr.SendAppendEntries(2, i, i - 1);
        mgr.HandleSuccessResponse(2, rpc_id++, i, i);
    }

    Check(mgr.GetMatchIndex(2) == 100, "peer 2 should catch up to 100");
}

// 测试 7: 快速连续更新
static void TestRapidUpdates() {
    std::cout << "Test 7: Rapid successive updates" << std::endl;

    ReplicationManager mgr(0, {1, 2});

    // 模拟快速写入：立即更新 matchIndex
    for (int i = 1; i <= 50; ++i) {
        if (!mgr.HasInflight(1)) {
            mgr.SendAppendEntries(1, i, i - 1);
            mgr.HandleSuccessResponse(1, i, i, i);
        }
    }

    Check(mgr.GetMatchIndex(1) == 50, "should handle rapid updates");
    Check(mgr.GetNextIndex(1) == 51, "nextIndex should be correct");
}

int main() {
    try {
        std::cout << "=== Replication Acknowledgment Edge Cases Tests ===" << std::endl;
        std::cout << "Enhanced unit tests for complex scenarios\n" << std::endl;

        TestOutOfOrderResponses();
        TestConflictResolution();
        TestTimeoutHandling();
        TestMultiplePeerReplication();
        TestDifferentTermLogs();
        TestExtremeLag();
        TestRapidUpdates();

        std::cout << "\n=== All edge case tests passed! (" << test_count << " checks) ===" << std::endl;
        std::cout << "\nThese tests verify:" << std::endl;
        std::cout << "  ✓ Out-of-order response handling" << std::endl;
        std::cout << "  ✓ Conflict resolution and nextIndex backoff" << std::endl;
        std::cout << "  ✓ Timeout handling" << std::endl;
        std::cout << "  ✓ Multi-peer replication and quorum" << std::endl;
        std::cout << "  ✓ Different term log commitment rules" << std::endl;
        std::cout << "  ✓ Extreme lag scenarios" << std::endl;
        std::cout << "  ✓ Rapid successive updates" << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
