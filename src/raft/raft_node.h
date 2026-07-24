#pragma once
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <random>

#include <muduo/net/EventLoop.h>

#include "raft/peer_manager.h"
#include "raft/kv_state_machine.h"
#include "raftcore/raft_log.h"
#include "raft_messages.pb.h"

// RaftNode: Raft 共识协议核心状态机
//
// 所有公共方法必须在 muduo EventLoop 线程中调用（该线程也处理所有 I/O）
// 因此无需互斥锁
class RaftNode {
public:
    // 提议提交后的回调: (success, result_string)
    // result_string 是 Apply 返回的 RESP 响应，如 "+OK\r\n"
    using ProposeCallback = std::function<void(bool success, const std::string& result)>;

    RaftNode(int node_id,
             const std::vector<PeerInfo>& all_peers,
             muduo::net::EventLoop* loop,
             const std::string& raft_log_path,
             KVStateMachine* sm,
             PeerManager* peer_mgr);

    ~RaftNode();

    // ---- 生命周期 ----
    void Start();
    void Stop();

    // ---- 客户端 API ----
    // 提议一条命令，仅 Leader 可调用
    // 成功返回 log_index (>0)，失败（非 Leader）返回 -1
    // 命令提交并应用后，回调 cb 被调用
    int64_t Propose(const std::string& command, ProposeCallback cb);

    // ---- 对端 RPC 消息处理 ----
    // 处理来自 PeerManager 的入站 RPC
    void HandleRequestVote(int from_peer, const raftcore::RequestVote& req);
    void HandleRequestVoteResponse(int from_peer, const raftcore::RequestVoteResponse& resp);
    void HandleAppendEntries(int from_peer, const raftcore::AppendEntries& req);
    void HandleAppendEntriesResponse(int from_peer, const raftcore::AppendEntriesResponse& resp);

    // ---- 定时器 ----
    // 每 10ms 调用一次
    void Tick();

    // ---- 状态查询 ----
    bool IsLeader() const { return _state == LEADER; }
    int GetLeaderId() const { return _leader_id; }
    int GetNodeId() const { return _node_id; }
    int GetCurrentTerm() const { return _current_term; }
    int64_t GetCommitIndex() const { return _commit_index; }

private:
    enum State { FOLLOWER, CANDIDATE, LEADER };

    // 状态转换
    void BecomeFollower(int32_t term);
    void BecomeCandidate();
    void BecomeLeader();

    // 选举
    void StartElection();
    void ResetElectionTimer();
    bool IsLogUpToDate(int64_t last_log_index, int64_t last_log_term) const;

    // 日志复制
    void SendAppendEntries(int peer_id);
    void BroadcastAppendEntries();  // 心跳或复制

    // 提交和应用
    void AdvanceCommitIndex();
    void ApplyCommitted();

    // 硬状态持久化
    void PersistHardState();
    void LoadHardState();

    int QuorumSize() const { return static_cast<int>(_all_peers.size()) / 2 + 1; }

    // ---- 配置 ----
    int _node_id;
    std::vector<PeerInfo> _all_peers;
    muduo::net::EventLoop* _loop;

    // ---- 依赖（不拥有所有权）----
    KVStateMachine* _sm;
    PeerManager*    _peer_mgr;

    // ---- 持久化组件 ----
    std::unique_ptr<RaftLog> _log;

    // ---- 硬状态（持久化）----
    int32_t _current_term;
    int32_t _voted_for;  // -1 表示未投票

    // ---- 易失性状态（所有服务器）----
    State   _state;
    int64_t _commit_index;
    int64_t _last_applied;
    int     _leader_id;       // 已知的 leader，-1 表示未知
    int     _votes_received;  // 仅 Candidate 有效

    // ---- Leader 易失性状态 ----
    std::vector<int64_t> _next_index;   // 对每个 peer，下一条要发送的日志索引
    std::vector<int64_t> _match_index;  // 对每个 peer，已知已复制的最高日志索引

    // ---- 定时器 ----
    int _election_timeout_ms;   // 当前选举超时剩余（ms）
    int _heartbeat_timer_ms;    // 心跳倒计时（ms）
    int _election_timeout_base; // 基础选举超时（ms）
    int _heartbeat_interval_ms; // 心跳间隔（ms）

    // ---- 待处理的客户端提议 ----
    struct PendingProposal {
        ProposeCallback callback;
    };
    std::map<int64_t, PendingProposal> _pending;

    // ---- 随机数 ----
    std::mt19937 _rng;

    static constexpr int kTickIntervalMs = 10;
    static constexpr int kMinElectionTimeoutMs = 150;
    static constexpr int kMaxElectionTimeoutMs = 300;
    static constexpr int kHeartbeatIntervalMs = 50;
};
