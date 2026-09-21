// Raft 测试支持工具
// 提供模拟网络、测试集群和辅助函数

#pragma once
#include "raft/raft_node.h"
#include <memory>
#include <vector>
#include <map>
#include <string>
#include <queue>

// 模拟网络消息
struct FakeMessage {
    int from;
    int to;
    RaftMsgType type;
    std::string payload;
    uint64_t delay_ms = 0;
};

// 模拟网络：可以控制消息传递、隔离节点等
class FakeNetwork {
public:
    void Send(int from, int to, RaftMsgType type, const std::string& payload) {
        if (isolated_.count(from) || isolated_.count(to)) return;
        pending_.push_back({from, to, type, payload, 0});
    }

    void Broadcast(int from, RaftMsgType type, const std::string& payload) {
        for (int to = 0; to < node_count_; ++to) {
            if (to != from) Send(from, to, type, payload);
        }
    }

    void DeliverAll() {
        auto messages = std::move(pending_);
        pending_.clear();
        for (auto& msg : messages) {
            if (handlers_.count(msg.to)) {
                handlers_[msg.to](msg.from, msg.type, msg.payload);
            }
        }
    }

    void Deliver(const FakeMessage& msg) {
        if (handlers_.count(msg.to)) {
            handlers_[msg.to](msg.from, msg.type, msg.payload);
        }
    }

    void Isolate(int node_id) { isolated_.insert(node_id); }
    void Reconnect(int node_id) { isolated_.erase(node_id); }
    void Clear() { pending_.clear(); }

    std::vector<FakeMessage> PendingMessages() const { return pending_; }

    void SetHandler(int node_id, std::function<void(int, RaftMsgType, const std::string&)> handler) {
        handlers_[node_id] = handler;
    }

    void SetNodeCount(int count) { node_count_ = count; }

private:
    std::vector<FakeMessage> pending_;
    std::map<int, std::function<void(int, RaftMsgType, const std::string&)>> handlers_;
    std::set<int> isolated_;
    int node_count_ = 0;
};

// 模拟 RaftNode（简化版，用于测试）
class FakeRaftNode {
public:
    FakeRaftNode(int id, FakeNetwork* network) : node_id_(id), network_(network) {
        network_->SetHandler(id, [this](int from, RaftMsgType type, const std::string& payload) {
            HandleMessage(from, type, payload);
        });
    }

    void Propose(const std::string& command, std::function<void(bool, const std::string&)> callback) {
        if (!is_leader_) {
            if (callback) callback(false, "-ERR not leader\r\n");
            return;
        }

        // 简化：直接添加到日志
        log_.push_back({++last_index_, current_term_, command});

        // 广播 AppendEntries
        network_->Broadcast(node_id_, RaftMsgType::kAppendEntries,
                           SerializeAppendEntries(last_index_, current_term_, command));

        // 记录待提交的提议
        pending_proposals_[last_index_] = callback;
    }

    void HandleMessage(int from, RaftMsgType type, const std::string& payload) {
        // 简化的消息处理
        if (type == RaftMsgType::kAppendEntries) {
            HandleAppendEntries(from, payload);
        } else if (type == RaftMsgType::kAppendEntriesResponse) {
            HandleAppendEntriesResponse(from, payload);
        }
    }

    void BecomeLeader() {
        is_leader_ = true;
        current_term_++;
        // 初始化 match_index 和 next_index
    }

    int64_t GetCommitIndex() const { return commit_index_; }
    int64_t GetMatchIndex(int peer_id) const {
        auto it = match_index_.find(peer_id);
        return it != match_index_.end() ? it->second : 0;
    }
    int32_t GetCurrentTerm() const { return current_term_; }
    bool IsLeader() const { return is_leader_; }

private:
    struct LogEntry {
        int64_t index;
        int32_t term;
        std::string command;
    };

    void HandleAppendEntries(int from, const std::string& payload) {
        // 简化：总是接受
        // 发送响应
        network_->Send(node_id_, from, RaftMsgType::kAppendEntriesResponse,
                      SerializeAppendEntriesResponse(true, last_index_));
    }

    void HandleAppendEntriesResponse(int from, const std::string& payload) {
        if (!is_leader_) return;

        // 更新 match_index
        bool success;
        int64_t matched;
        DeserializeAppendEntriesResponse(payload, success, matched);

        if (success) {
            match_index_[from] = std::max(match_index_[from], matched);

            // 尝试推进 commit_index
            AdvanceCommitIndex();
        }
    }

    void AdvanceCommitIndex() {
        // 找到多数派都确认的最大索引
        std::vector<int64_t> matches;
        matches.push_back(last_index_);  // leader 自己
        for (auto& pair : match_index_) {
            matches.push_back(pair.second);
        }
        std::sort(matches.begin(), matches.end(), std::greater<int64_t>());

        int quorum = (matches.size() + 1) / 2;
        int64_t new_commit = matches[quorum - 1];

        if (new_commit > commit_index_) {
            commit_index_ = new_commit;
            // 触发回调
            for (int64_t i = commit_index_ + 1; i <= new_commit; ++i) {
                auto it = pending_proposals_.find(i);
                if (it != pending_proposals_.end() && it->second) {
                    it->second(true, "+OK\r\n");
                    pending_proposals_.erase(it);
                }
            }
        }
    }

    std::string SerializeAppendEntries(int64_t index, int32_t term, const std::string& cmd) {
        // 简化的序列化
        return std::to_string(index) + ":" + std::to_string(term) + ":" + cmd;
    }

    std::string SerializeAppendEntriesResponse(bool success, int64_t matched) {
        return success ? ("OK:" + std::to_string(matched)) : "FAIL";
    }

    void DeserializeAppendEntriesResponse(const std::string& payload, bool& success, int64_t& matched) {
        if (payload.substr(0, 3) == "OK:") {
            success = true;
            matched = std::stoll(payload.substr(3));
        } else {
            success = false;
            matched = 0;
        }
    }

    int node_id_;
    FakeNetwork* network_;
    bool is_leader_ = false;
    int32_t current_term_ = 0;
    int64_t last_index_ = 0;
    int64_t commit_index_ = 0;
    std::vector<LogEntry> log_;
    std::map<int, int64_t> match_index_;
    std::map<int, int64_t> next_index_;
    std::map<int64_t, std::function<void(bool, const std::string&)>> pending_proposals_;
};

// 创建测试集群
inline std::vector<std::unique_ptr<FakeRaftNode>> MakeTestCluster(int size, FakeNetwork* network) {
    network->SetNodeCount(size);
    std::vector<std::unique_ptr<FakeRaftNode>> cluster;
    for (int i = 0; i < size; ++i) {
        cluster.push_back(std::make_unique<FakeRaftNode>(i, network));
    }
    // 默认 node 0 是 Leader
    cluster[0]->BecomeLeader();
    return cluster;
}

// 创建冲突的 AppendEntries（用于测试）
inline raftcore::AppendEntries MakeConflictingAppendEntries(int64_t conflicting_index) {
    raftcore::AppendEntries request;
    request.set_term(999);
    request.set_leader_id(0);
    request.set_prev_log_index(conflicting_index - 1);
    request.set_prev_log_term(1);
    request.set_leader_commit(0);
    request.set_rpc_id(1);

    auto* entry = request.add_entries();
    entry->set_index(conflicting_index);
    entry->set_term(999);
    entry->set_command("conflicting command");

    return request;
}
