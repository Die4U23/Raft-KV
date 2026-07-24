#include "raft/raft_node.h"
#include <glog/logging.h>

// ============================================================
// RaftNode 实现
// ============================================================

RaftNode::RaftNode(int node_id,
                   const std::vector<PeerInfo>& all_peers,
                   muduo::net::EventLoop* loop,
                   const std::string& raft_log_path,
                   KVStateMachine* sm,
                   PeerManager* peer_mgr)
    : _node_id(node_id)
    , _all_peers(all_peers)
    , _loop(loop)
    , _sm(sm)
    , _peer_mgr(peer_mgr)
    , _current_term(0)
    , _voted_for(-1)
    , _state(FOLLOWER)
    , _commit_index(0)
    , _last_applied(0)
    , _leader_id(-1)
    , _votes_received(0)
    , _election_timeout_ms(0)
    , _heartbeat_timer_ms(kHeartbeatIntervalMs)
    , _election_timeout_base(0)
    , _heartbeat_interval_ms(kHeartbeatIntervalMs)
    , _rng(std::random_device{}())
{
    _log.reset(new RaftLog(raft_log_path));

    // 初始化 per-peer 状态
    int n = static_cast<int>(_all_peers.size());
    _next_index.resize(n, 1);
    _match_index.resize(n, 0);

    // 加载硬状态
    LoadHardState();
    LOG(INFO) << "RaftNode[" << _node_id << "] created, term=" << _current_term
              << ", voted_for=" << _voted_for
              << ", last_log_index=" << _log->LastIndex();
}

RaftNode::~RaftNode() = default;

// ---- 生命周期 ----

void RaftNode::Start() {
    BecomeFollower(_current_term);  // 总是以 Follower 启动
    LOG(INFO) << "RaftNode[" << _node_id << "] started as Follower, term="
              << _current_term;
}

void RaftNode::Stop() {
    _state = FOLLOWER;
}

// ---- 客户端 API ----

int64_t RaftNode::Propose(const std::string& command, ProposeCallback cb) {
    if (_state != LEADER) {
        return -1;
    }

    int64_t index = _log->LastIndex() + 1;

    raftcore::LogEntry entry;
    entry.set_index(index);
    entry.set_term(_current_term);
    entry.set_command(command);
    _log->Append(entry);

    _pending[index] = {std::move(cb)};
    _match_index[_node_id] = index;  // 自己的日志总是"已复制"

    BroadcastAppendEntries();
    return index;
}

// ---- 状态转换 ----

void RaftNode::BecomeFollower(int32_t term) {
    bool was_leader = (_state == LEADER);

    if (term > _current_term) {
        _current_term = term;
        _voted_for = -1;
        PersistHardState();
    }

    _state = FOLLOWER;
    _leader_id = -1;  // 将在收到有效心跳后更新
    ResetElectionTimer();

    // 如果是 Leader 降级，失败所有待处理的提议
    if (was_leader) {
        for (auto& kv : _pending) {
            if (kv.second.callback) {
                kv.second.callback(false, "-ERR leadership lost\r\n");
            }
        }
        _pending.clear();
    }

    LOG(INFO) << "RaftNode[" << _node_id << "] became Follower, term="
              << _current_term;
}

void RaftNode::BecomeCandidate() {
    _state = CANDIDATE;
    _current_term++;
    _voted_for = _node_id;
    _votes_received = 1;  // 投票给自己
    PersistHardState();
    _leader_id = -1;

    LOG(INFO) << "RaftNode[" << _node_id << "] became Candidate, term="
              << _current_term;

    StartElection();
}

void RaftNode::BecomeLeader() {
    _state = LEADER;
    _leader_id = _node_id;

    // 初始化 Leader 状态
    int64_t last_log = _log->LastIndex();
    for (size_t i = 0; i < _all_peers.size(); ++i) {
        _next_index[i] = last_log + 1;
        _match_index[i] = 0;
    }
    _match_index[_node_id] = last_log;

    _heartbeat_timer_ms = 0;  // 立即发送心跳

    LOG(INFO) << "RaftNode[" << _node_id << "] became Leader, term="
              << _current_term << ", last_log=" << last_log;
}

// ---- 选举 ----

void RaftNode::StartElection() {
    ResetElectionTimer();

    raftcore::RequestVote req;
    req.set_term(_current_term);
    req.set_candidate_id(_node_id);
    req.set_last_log_index(_log->LastIndex());
    req.set_last_log_term(_log->LastTerm());

    std::string payload;
    req.SerializeToString(&payload);

    _peer_mgr->Broadcast(RaftMsgType::kRequestVote, payload);
}

void RaftNode::ResetElectionTimer() {
    // 随机化选举超时: 150ms ~ 300ms
    std::uniform_int_distribution<int> dist(kMinElectionTimeoutMs,
                                            kMaxElectionTimeoutMs);
    _election_timeout_base = dist(_rng);
    _election_timeout_ms = _election_timeout_base;
}

bool RaftNode::IsLogUpToDate(int64_t last_log_index, int64_t last_log_term) const {
    // 候选人的日志至少和我们一样新：
    // term 更高 → 更新
    // term 相同但 index ≥ 我们的 → 更新
    int64_t my_last_term = _log->LastTerm();
    int64_t my_last_index = _log->LastIndex();

    if (last_log_term != my_last_term) {
        return last_log_term > my_last_term;
    }
    return last_log_index >= my_last_index;
}

// ---- 入站 RPC 处理 ----

void RaftNode::HandleRequestVote(int from_peer,
                                  const raftcore::RequestVote& req) {
    raftcore::RequestVoteResponse resp;

    // 如果请求者的 term 比我们小，拒绝
    if (req.term() < _current_term) {
        resp.set_term(_current_term);
        resp.set_vote_granted(false);
    } else {
        // 如果请求者的 term 更大，更新自己为 Follower
        if (req.term() > _current_term) {
            BecomeFollower(req.term());
        }

        // 检查是否可以投票
        bool can_vote = (_voted_for == -1 || _voted_for == req.candidate_id()) &&
                        IsLogUpToDate(req.last_log_index(), req.last_log_term());

        resp.set_term(_current_term);
        resp.set_vote_granted(can_vote);

        if (can_vote) {
            _voted_for = req.candidate_id();
            PersistHardState();
            ResetElectionTimer();  // 投票后重置选举超时
            LOG(INFO) << "RaftNode[" << _node_id << "] voted for "
                      << req.candidate_id() << " in term " << _current_term;
        }
    }

    // 发送响应
    std::string payload;
    resp.SerializeToString(&payload);
    _peer_mgr->Send(from_peer, RaftMsgType::kRequestVoteResponse, payload);
}

void RaftNode::HandleRequestVoteResponse(
    int from_peer, const raftcore::RequestVoteResponse& resp) {

    if (_state != CANDIDATE) {
        return;  // 已经不是候选人了，忽略
    }

    if (resp.term() > _current_term) {
        BecomeFollower(resp.term());
        return;
    }

    if (resp.term() < _current_term) {
        return;  // 过期的响应
    }

    if (resp.vote_granted()) {
        _votes_received++;
        LOG(INFO) << "RaftNode[" << _node_id << "] received vote from "
                  << from_peer << " (" << _votes_received << "/"
                  << QuorumSize() << " needed)";

        if (_votes_received >= QuorumSize()) {
            BecomeLeader();
        }
    }
}

void RaftNode::HandleAppendEntries(int from_peer,
                                    const raftcore::AppendEntries& req) {
    raftcore::AppendEntriesResponse resp;

    // 1. 如果 term 更小，拒绝
    if (req.term() < _current_term) {
        resp.set_term(_current_term);
        resp.set_success(false);
        resp.set_last_log_index(_log->LastIndex());
        goto send_response;
    }

    // 2. 如果 term 更大，可能是新的 Leader
    if (req.term() > _current_term) {
        BecomeFollower(req.term());
    }

    // 3. 收到有效的心跳，重置选举超时，记录 Leader
    ResetElectionTimer();
    _leader_id = req.leader_id();

    // 4. 一致性检查
    if (req.prev_log_index() > 0) {
        if (_log->LastIndex() < req.prev_log_index()) {
            // 我们的日志太短
            resp.set_term(_current_term);
            resp.set_success(false);
            resp.set_last_log_index(_log->LastIndex());
            goto send_response;
        }

        int64_t term_at_prev = _log->GetTerm(req.prev_log_index());
        if (term_at_prev != req.prev_log_term()) {
            // term 不匹配，删除冲突条目
            _log->TruncateSuffix(req.prev_log_index());
            resp.set_term(_current_term);
            resp.set_success(false);
            resp.set_last_log_index(_log->LastIndex());
            goto send_response;
        }
    }

    // 5. 追加新条目
    for (int i = 0; i < req.entries_size(); ++i) {
        const auto& entry = req.entries(i);
        int64_t idx = entry.index();

        int64_t existing_term = _log->GetTerm(idx);
        if (existing_term != -1 && existing_term != entry.term()) {
            // 冲突：删除此位置及之后的条目
            _log->TruncateSuffix(idx);
        }

        if (idx > _log->LastIndex()) {
            _log->Append(entry);
        }
    }

    // 6. 更新 commitIndex
    if (req.leader_commit() > _commit_index) {
        int64_t last = _log->LastIndex();
        _commit_index = std::min(req.leader_commit(), last);
        ApplyCommitted();
    }

    resp.set_term(_current_term);
    resp.set_success(true);
    resp.set_last_log_index(_log->LastIndex());

send_response:
    std::string payload;
    resp.SerializeToString(&payload);
    _peer_mgr->Send(from_peer, RaftMsgType::kAppendEntriesResponse, payload);
}

void RaftNode::HandleAppendEntriesResponse(
    int from_peer, const raftcore::AppendEntriesResponse& resp) {

    if (_state != LEADER) {
        return;
    }

    if (resp.term() > _current_term) {
        BecomeFollower(resp.term());
        return;
    }

    if (resp.term() < _current_term) {
        return;  // 过期响应
    }

    if (resp.success()) {
        // 成功：更新 matchIndex 和 nextIndex
        _match_index[from_peer] = resp.last_log_index();
        _next_index[from_peer] = resp.last_log_index() + 1;
        AdvanceCommitIndex();
    } else {
        // 失败：回退 nextIndex 并重试
        _next_index[from_peer] = std::max(int64_t(1), _next_index[from_peer] - 1);
        SendAppendEntries(from_peer);
    }
}

// ---- 日志复制 ----

void RaftNode::SendAppendEntries(int peer_id) {
    int64_t prev_log_index = _next_index[peer_id] - 1;
    int64_t prev_log_term = 0;
    if (prev_log_index > 0) {
        prev_log_term = _log->GetTerm(prev_log_index);
        if (prev_log_term < 0) prev_log_term = 0;
    }

    raftcore::AppendEntries req;
    req.set_term(_current_term);
    req.set_leader_id(_node_id);
    req.set_prev_log_index(prev_log_index);
    req.set_prev_log_term(prev_log_term);
    req.set_leader_commit(_commit_index);

    // 添加从 nextIndex 开始的所有新条目
    int64_t last_idx = _log->LastIndex();
    for (int64_t i = _next_index[peer_id]; i <= last_idx; ++i) {
        raftcore::LogEntry entry;
        if (_log->Get(i, &entry)) {
            *req.add_entries() = entry;
        }
    }

    std::string payload;
    req.SerializeToString(&payload);
    _peer_mgr->Send(peer_id, RaftMsgType::kAppendEntries, payload);
}

void RaftNode::BroadcastAppendEntries() {
    for (const auto& peer : _all_peers) {
        if (peer.id == _node_id) continue;
        SendAppendEntries(peer.id);
    }
}

// ---- 提交和应用 ----

void RaftNode::AdvanceCommitIndex() {
    int64_t last = _log->LastIndex();

    for (int64_t n = last; n > _commit_index; --n) {
        // Raft 只提交当前 term 的条目（由其提交间接提交以前的条目）
        if (_log->GetTerm(n) != _current_term) {
            continue;
        }

        int count = 0;
        for (size_t i = 0; i < _all_peers.size(); ++i) {
            if (_match_index[i] >= n) {
                count++;
            }
        }

        if (count >= QuorumSize()) {
            _commit_index = n;
            LOG(INFO) << "RaftNode[" << _node_id
                      << "] advanced commit_index to " << _commit_index;
            break;
        }
    }

    ApplyCommitted();
}

void RaftNode::ApplyCommitted() {
    while (_last_applied < _commit_index) {
        int64_t idx = _last_applied + 1;

        raftcore::LogEntry entry;
        if (!_log->Get(idx, &entry)) {
            LOG(ERROR) << "RaftNode[" << _node_id
                       << "] missing log entry at index " << idx
                       << ", stopping apply";
            break;
        }

        std::string result = _sm->Apply(entry.command());

        // 触发回调
        auto it = _pending.find(idx);
        if (it != _pending.end()) {
            if (it->second.callback) {
                it->second.callback(true, result);
            }
            _pending.erase(it);
        }

        _last_applied = idx;
    }
}

// ---- 硬状态持久化 ----

void RaftNode::PersistHardState() {
    _log->SaveHardState(_current_term, _voted_for);
}

void RaftNode::LoadHardState() {
    _log->LoadHardState(&_current_term, &_voted_for);
}

// ---- 定时器 ----

void RaftNode::Tick() {
    // 选举超时检查（Follower 和 Candidate）
    if (_state != LEADER) {
        _election_timeout_ms -= kTickIntervalMs;
        if (_election_timeout_ms <= 0) {
            // Follower 超时 or Candidate 超时 → 开始新选举（递增 term）
            BecomeCandidate();
        }
    }

    // Leader 心跳
    if (_state == LEADER) {
        _heartbeat_timer_ms -= kTickIntervalMs;
        if (_heartbeat_timer_ms <= 0) {
            BroadcastAppendEntries();  // 空的就是心跳
            _heartbeat_timer_ms = _heartbeat_interval_ms;
        }
    }
}
