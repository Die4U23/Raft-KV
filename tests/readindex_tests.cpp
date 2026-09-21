// ReadIndex 单元测试
// 测试 ReadIndex 机制的核心逻辑

#include <iostream>
#include <cassert>
#include <cstdint>
#include <string>
#include <functional>
#include <vector>
#include <set>
#include <deque>

static int test_count = 0;

static void Check(bool condition, const char* message) {
    ++test_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// 模拟 ReadIndex 状态管理
class ReadIndexManager {
public:
    ReadIndexManager(int node_id, int quorum_size)
        : node_id_(node_id), quorum_size_(quorum_size) {}

    struct ReadRequest {
        int64_t read_index;
        std::function<void(bool, int64_t, std::string)> callback;
        bool completed = false;
    };

    struct HeartbeatRound {
        uint64_t round_id;
        std::set<int> acks;
        std::vector<ReadRequest> requests;
    };

    // 请求 ReadIndex
    bool RequestReadIndex(int64_t current_commit, std::function<void(bool, int64_t, std::string)> cb) {
        if (!can_serve_read_) {
            cb(false, -1, "no-op not committed");
            return false;
        }

        ReadRequest req;
        req.read_index = current_commit;
        req.callback = cb;

        // 如果没有 in-flight 心跳，启动新一轮
        if (!heartbeat_in_flight_) {
            StartHeartbeatRound();
        }

        // 将请求加入当前轮
        heartbeat_rounds_.back().requests.push_back(req);
        return true;
    }

    // 启动心跳轮次
    void StartHeartbeatRound() {
        HeartbeatRound round;
        round.round_id = ++next_round_id_;
        round.acks.insert(node_id_);  // Leader 自己
        heartbeat_rounds_.push_back(round);
        heartbeat_in_flight_ = true;
    }

    // 处理心跳响应
    void HandleHeartbeatAck(int peer_id, uint64_t round_id) {
        if (heartbeat_rounds_.empty()) return;

        auto& current_round = heartbeat_rounds_.front();
        if (current_round.round_id != round_id) return;

        current_round.acks.insert(peer_id);

        // 检查是否达到多数派
        if (static_cast<int>(current_round.acks.size()) >= quorum_size_) {
            ProcessConfirmedRound(current_round);
            heartbeat_rounds_.pop_front();

            if (heartbeat_rounds_.empty()) {
                heartbeat_in_flight_ = false;
            }
        }
    }

    // 处理已确认的轮次
    void ProcessConfirmedRound(const HeartbeatRound& round) {
        for (auto& req : round.requests) {
            if (last_applied_ >= req.read_index) {
                req.callback(true, req.read_index, "");
            } else {
                pending_reads_.push_back(req);
            }
        }
    }

    // 更新 lastApplied
    void AdvanceLastApplied(int64_t new_last_applied) {
        last_applied_ = new_last_applied;
        ProcessPendingReads();
    }

    // 处理等待的读请求
    void ProcessPendingReads() {
        while (!pending_reads_.empty()) {
            auto& req = pending_reads_.front();
            if (last_applied_ >= req.read_index) {
                req.callback(true, req.read_index, "");
                pending_reads_.pop_front();
            } else {
                break;
            }
        }
    }

    // Step down 清空队列
    void StepDown() {
        for (auto& round : heartbeat_rounds_) {
            for (auto& req : round.requests) {
                req.callback(false, -1, "stepped down");
            }
        }
        heartbeat_rounds_.clear();
        heartbeat_in_flight_ = false;

        for (auto& req : pending_reads_) {
            req.callback(false, -1, "stepped down");
        }
        pending_reads_.clear();

        can_serve_read_ = false;
    }

    void SetCanServeRead(bool can) { can_serve_read_ = can; }
    bool CanServeRead() const { return can_serve_read_; }
    size_t PendingCount() const {
        size_t count = pending_reads_.size();
        for (const auto& round : heartbeat_rounds_) {
            count += round.requests.size();
        }
        return count;
    }

private:
    int node_id_;
    int quorum_size_;
    bool can_serve_read_ = false;
    uint64_t next_round_id_ = 0;
    std::deque<HeartbeatRound> heartbeat_rounds_;
    bool heartbeat_in_flight_ = false;
    std::deque<ReadRequest> pending_reads_;
    int64_t last_applied_ = 0;
};

// 测试 1: No-op 前置条件
static void TestNoOpPrerequisite() {
    std::cout << "Test 1: No-op prerequisite" << std::endl;

    ReadIndexManager mgr(0, 2);  // 3 节点，quorum=2

    bool callback_invoked = false;
    bool success = false;
    std::string error;

    // 未提交 no-op，应该拒绝
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        callback_invoked = true;
        success = s;
        error = err;
    });

    Check(callback_invoked, "callback should be invoked immediately");
    Check(!success, "should fail before no-op committed");
    Check(error == "no-op not committed", "error message should match");

    // 提交 no-op 后
    mgr.SetCanServeRead(true);
    mgr.AdvanceLastApplied(10);  // 设置 lastApplied >= read_index

    callback_invoked = false;
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        callback_invoked = true;
        success = s;
    });

    Check(!callback_invoked, "callback should wait for heartbeat");

    // 模拟心跳响应
    mgr.HandleHeartbeatAck(1, 1);  // 达到多数派

    Check(callback_invoked, "callback should be invoked after quorum");
    Check(success, "should succeed after no-op committed");
}

// 测试 2: 心跳 ack 统计
static void TestHeartbeatAcks() {
    std::cout << "Test 2: Heartbeat ack counting" << std::endl;

    ReadIndexManager mgr(0, 2);  // 3 节点，quorum=2
    mgr.SetCanServeRead(true);
    mgr.AdvanceLastApplied(10);  // 设置 lastApplied >= read_index

    bool callback_invoked = false;
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        callback_invoked = true;
    });

    // Leader 自己算一票，还需要一票
    Check(!callback_invoked, "should wait for more acks");

    // 收到一个 peer 的 ack
    mgr.HandleHeartbeatAck(1, 1);

    Check(callback_invoked, "should invoke after quorum (2/3)");
}

// 测试 3: 等待 lastApplied
static void TestWaitForApply() {
    std::cout << "Test 3: Wait for lastApplied" << std::endl;

    ReadIndexManager mgr(0, 2);
    mgr.SetCanServeRead(true);

    bool callback_invoked = false;
    int64_t result_index = -1;

    // 请求 read_index = 10，但 lastApplied = 0
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        callback_invoked = true;
        result_index = idx;
    });

    // 心跳达到多数派
    mgr.HandleHeartbeatAck(1, 1);

    // 此时还未应用到 10，应该等待
    Check(!callback_invoked, "should wait for apply");

    // 推进 lastApplied 到 5
    mgr.AdvanceLastApplied(5);
    Check(!callback_invoked, "should still wait (5 < 10)");

    // 推进到 10
    mgr.AdvanceLastApplied(10);
    Check(callback_invoked, "should invoke when lastApplied >= read_index");
    Check(result_index == 10, "should return correct read_index");
}

// 测试 4: Step down 清空队列
static void TestStepDownClearsQueues() {
    std::cout << "Test 4: Step down clears queues" << std::endl;

    ReadIndexManager mgr(0, 2);
    mgr.SetCanServeRead(true);

    int callback_count = 0;
    bool any_success = false;

    // 创建 3 个请求
    for (int i = 0; i < 3; ++i) {
        mgr.RequestReadIndex(10 + i, [&](bool s, int64_t idx, std::string err) {
            ++callback_count;
            if (s) any_success = true;
        });
    }

    Check(mgr.PendingCount() == 3, "should have 3 pending requests");

    // Step down
    mgr.StepDown();

    Check(callback_count == 3, "all callbacks should be invoked");
    Check(!any_success, "all should fail");
    Check(mgr.PendingCount() == 0, "queue should be empty");
    Check(!mgr.CanServeRead(), "can_serve_read should be reset");
}

// 测试 5: 心跳合并（批量请求）
static void TestHeartbeatBatching() {
    std::cout << "Test 5: Heartbeat batching" << std::endl;

    ReadIndexManager mgr(0, 2);
    mgr.SetCanServeRead(true);
    mgr.AdvanceLastApplied(10);  // 设置 lastApplied >= read_index

    int callback_count = 0;

    // 连续发送 5 个请求（应该共享同一轮心跳）
    for (int i = 0; i < 5; ++i) {
        mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
            ++callback_count;
        });
    }

    // 一次心跳应该处理所有 5 个请求
    mgr.HandleHeartbeatAck(1, 1);

    Check(callback_count == 5, "all requests should be processed in one round");
}

// 测试 6: 多轮心跳
static void TestMultipleRounds() {
    std::cout << "Test 6: Multiple heartbeat rounds" << std::endl;

    ReadIndexManager mgr(0, 2);
    mgr.SetCanServeRead(true);
    mgr.AdvanceLastApplied(15);  // 设置 lastApplied 足够大

    int round1_count = 0;
    int round2_count = 0;

    // Round 1: 2 个请求
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        ++round1_count;
    });
    mgr.RequestReadIndex(10, [&](bool s, int64_t idx, std::string err) {
        ++round1_count;
    });

    // 完成 Round 1
    mgr.HandleHeartbeatAck(1, 1);
    Check(round1_count == 2, "round 1 should complete");

    // Round 2: 3 个请求
    mgr.RequestReadIndex(15, [&](bool s, int64_t idx, std::string err) {
        ++round2_count;
    });
    mgr.RequestReadIndex(15, [&](bool s, int64_t idx, std::string err) {
        ++round2_count;
    });
    mgr.RequestReadIndex(15, [&](bool s, int64_t idx, std::string err) {
        ++round2_count;
    });

    // 完成 Round 2
    mgr.HandleHeartbeatAck(1, 2);
    Check(round2_count == 3, "round 2 should complete");
}

int main() {
    try {
        std::cout << "=== ReadIndex Unit Tests ===" << std::endl;
        std::cout << "Testing core ReadIndex logic\n" << std::endl;

        TestNoOpPrerequisite();
        TestHeartbeatAcks();
        TestWaitForApply();
        TestStepDownClearsQueues();
        TestHeartbeatBatching();
        TestMultipleRounds();

        std::cout << "\n=== All tests passed! (" << test_count << " checks) ===" << std::endl;
        std::cout << "\nThese tests verify:" << std::endl;
        std::cout << "  ✓ No-op prerequisite" << std::endl;
        std::cout << "  ✓ Heartbeat ack counting and quorum" << std::endl;
        std::cout << "  ✓ Wait for lastApplied mechanism" << std::endl;
        std::cout << "  ✓ Step down clears all queues" << std::endl;
        std::cout << "  ✓ Heartbeat batching (multiple requests share round)" << std::endl;
        std::cout << "  ✓ Multiple independent rounds" << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
