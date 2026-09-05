#include "raft/raft_node.h"
#include "common/resp_parser.h"
#include "raft/peers.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

RaftNode::RaftNode(int id, const std::vector<PeerInfo>& peers,
                   muduo::net::EventLoop*, const std::string& log_path,
                   KVStateMachine* sm, PeerManager* peer_mgr, ApplyExecutor* apply_executor)
    : _node_id(id), _all_peers(peers), _sm(sm), _peer_mgr(peer_mgr),
      _apply_executor(apply_executor),
      _rng(std::random_device{}()) {
    ValidatePeers(id, peers);
    if (!sm || !peer_mgr) throw std::invalid_argument("missing Raft dependency");
    _log = std::make_unique<RaftLog>(log_path);
    _log->LoadHardState(&_current_term, &_voted_for);
    _last_applied = _sm->LastApplied();
    _commit_index = _last_applied;
    if (_last_applied > _log->LastIndex() || _current_term < _log->LastTerm())
        throw std::runtime_error("inconsistent KV/log recovery state");
    for (const auto& peer : peers) {
        _next_index[peer.id] = 1;
        _match_index[peer.id] = 0;
        _inflight[peer.id] = {};
    }
    if (_voted_for != -1 && !_next_index.count(_voted_for))
        throw std::runtime_error("persisted vote is outside the static membership");
}
RaftNode::~RaftNode() { _lifetime.reset(); }

std::string RaftNode::MetricsInfo() const {
    return _leader_log_write.ToInfo("leader_log_write") +
           _follower_log_write.ToInfo("follower_log_write") +
           _replication_data_ack.ToInfo("replication_data_ack") +
           _kv_apply.ToInfo("kv_apply") + _apply_dispatch.ToInfo("apply_dispatch") +
           "replication_retry_attempts:" + std::to_string(_replication_retry_attempts) + "\r\n";
}
const char* RaftNode::StateName() const {
    if (!_running) return "stopped";
    return _state == LEADER ? "leader" : (_state == CANDIDATE ? "candidate" : "follower");
}
void RaftNode::Start() {
    _running = true;
    BecomeFollower(_current_term);
    ApplyCommitted(); // resume committed backlog after Stop/Start
}
void RaftNode::Stop() {
    _running = false;
    _state = FOLLOWER;
    _leader_id = -1;
    FailPending("-ERR server stopped; outcome unknown\r\n");
}
bool RaftNode::IsRemotePeer(int id) const {
    return id != _node_id && _next_index.count(id) != 0;
}
void RaftNode::FailPending(const std::string& result) {
    auto pending = std::move(_pending);
    _pending.clear();
    _pending_bytes = 0;
    for (auto& item : pending)
        if (item.second.callback) item.second.callback(false, result);
}
int64_t RaftNode::Propose(const std::string& command, ProposeCallback callback) {
    std::vector<Proposal> proposals;
    proposals.push_back({command, std::move(callback)});
    return ProposeBatch(std::move(proposals));
}
int64_t RaftNode::ProposeBatch(std::vector<Proposal> proposals) {
    if (!IsLeader()) return -1;
    if (proposals.empty() || proposals.size() > kMaxBatchEntries ||
        proposals.size() > kMaxPending - _pending.size()) return -2;
    size_t bytes = 0;
    for (const auto& proposal : proposals) {
        if (proposal.command.size() > RespParser::kMaxCommandBytes ||
            proposal.command.size() > kMaxPendingBytes - bytes) return -2;
        bytes += proposal.command.size();
    }
    if (bytes > kMaxPendingBytes - _pending_bytes ||
        bytes > kMaxBatchBytes - 128 - proposals.size() * 64) return -2;
    if (_log->LastIndex() > INT64_MAX - static_cast<int64_t>(proposals.size()) - 1)
        throw std::runtime_error("Raft index exhausted");
    const int64_t first = _log->LastIndex() + 1;
    std::vector<raftcore::LogEntry> entries;
    entries.reserve(proposals.size());
    for (size_t i = 0; i < proposals.size(); ++i) {
        entries.emplace_back();
        entries.back().set_index(first + static_cast<int64_t>(i));
        entries.back().set_term(_current_term);
        entries.back().set_command(proposals[i].command);
    }
    const auto write_started = SteadyClock::now();
    _log->AppendBatch(entries); // one durable write before counting the local replica
    _leader_log_write.Observe(ElapsedMicros(write_started), entries.size(), bytes);
    ++_proposal_batches;
    for (size_t i = 0; i < proposals.size(); ++i)
        _pending.emplace(entries[i].index(), Pending{std::move(proposals[i].callback),
                                                   proposals[i].command.size()});
    _pending_bytes += bytes;
    _match_index[_node_id] = entries.back().index();
    BroadcastAppendEntries();
    AdvanceCommitIndex(); // also permits a single-member cluster
    return first;
}
void RaftNode::BecomeFollower(int32_t term) {
    if (term > _current_term) {
        _log->SaveHardState(term, -1);
        _current_term = term;
        _voted_for = -1;
    }
    _state = FOLLOWER;
    _leader_id = -1;
    _votes.clear();
    for (auto& item : _inflight) item.second = {};
    ResetElectionTimer();
    FailPending("-ERR leadership lost; outcome unknown\r\n");
}
void RaftNode::BecomeCandidate() {
    if (_current_term == INT32_MAX) throw std::runtime_error("Raft term exhausted");
    _log->SaveHardState(_current_term + 1, _node_id);
    ++_current_term;
    _voted_for = _node_id;
    _state = CANDIDATE;
    _leader_id = -1;
    _votes = {_node_id};
    ResetElectionTimer();
    if (QuorumSize() == 1) {
        BecomeLeader();
        return;
    }
    raftcore::RequestVote request;
    request.set_term(_current_term);
    request.set_candidate_id(_node_id);
    request.set_last_log_index(_log->LastIndex());
    request.set_last_log_term(_log->LastTerm());
    std::string payload;
    request.SerializeToString(&payload);
    _peer_mgr->Broadcast(RaftMsgType::kRequestVote, payload);
}
void RaftNode::BecomeLeader() {
    _state = LEADER;
    _leader_id = _node_id;
    for (const auto& peer : _all_peers) {
        _next_index[peer.id] = _log->LastIndex() + 1;
        _match_index[peer.id] = 0;
        _inflight[peer.id] = {};
    }
    _match_index[_node_id] = _log->LastIndex();
    _heartbeat_timer_ms = kHeartbeatIntervalMs;
    // Commit one entry in this term so recovered older entries can be applied.
    Propose("", {});
}
void RaftNode::ResetElectionTimer() {
    _election_timeout_ms = std::uniform_int_distribution<int>(150, 300)(_rng);
}
bool RaftNode::IsLogUpToDate(int64_t index, int64_t term) const {
    return term != _log->LastTerm() ? term > _log->LastTerm() : index >= _log->LastIndex();
}
void RaftNode::HandleRequestVote(int from, const raftcore::RequestVote& request) {
    if (!_running || !IsRemotePeer(from) || request.candidate_id() != from ||
        request.term() <= 0 || request.last_log_index() < 0 || request.last_log_term() < 0 ||
        request.last_log_term() > request.term()) return;
    if (request.term() > _current_term) BecomeFollower(request.term());
    const bool grant = request.term() == _current_term &&
        (_voted_for == -1 || _voted_for == from) &&
        IsLogUpToDate(request.last_log_index(), request.last_log_term());
    if (grant) {
        _log->SaveHardState(_current_term, from);
        _voted_for = from;
        ResetElectionTimer();
    }
    raftcore::RequestVoteResponse response;
    response.set_term(_current_term);
    response.set_vote_granted(grant);
    std::string payload;
    response.SerializeToString(&payload);
    _peer_mgr->Send(from, RaftMsgType::kRequestVoteResponse, payload);
}
void RaftNode::HandleRequestVoteResponse(int from, const raftcore::RequestVoteResponse& response) {
    if (!_running || !IsRemotePeer(from)) return;
    if (response.term() > _current_term) {
        BecomeFollower(response.term());
        return;
    }
    if (_state != CANDIDATE || response.term() != _current_term) return;
    if (response.vote_granted()) {
        _votes.insert(from);
        if (static_cast<int>(_votes.size()) >= QuorumSize()) BecomeLeader();
    }
}
void RaftNode::HandleAppendEntries(int from, const raftcore::AppendEntries& request) {
    if (!_running || !IsRemotePeer(from) || request.leader_id() != from ||
        request.term() <= 0 || request.rpc_id() == 0 || request.prev_log_index() < 0 ||
        request.prev_log_term() < 0 || request.prev_log_term() > request.term() ||
        request.leader_commit() < 0 ||
        request.prev_log_index() > INT64_MAX - request.entries_size()) return;
    if ((request.prev_log_index() == 0) != (request.prev_log_term() == 0)) return;
    int64_t previous_term = request.prev_log_term();
    for (int i = 0; i < request.entries_size(); ++i) {
        const auto& entry = request.entries(i);
        if (entry.index() != request.prev_log_index() + i + 1 ||
            entry.term() <= 0 || entry.term() < previous_term ||
            entry.term() > request.term() ||
            entry.command().size() > RespParser::kMaxCommandBytes) return;
        previous_term = entry.term();
    }
    raftcore::AppendEntriesResponse response;
    response.set_rpc_id(request.rpc_id());
    if (request.term() > _current_term ||
        (request.term() == _current_term && _state != FOLLOWER))
        BecomeFollower(request.term());
    response.set_term(_current_term);
    response.set_success(false);
    response.set_last_log_index(_log->LastIndex());
    if (request.term() == _current_term) {
        _leader_id = from;
        ResetElectionTimer();
        // A mismatched prefix rejects the request without deleting any entry.
        if (_log->GetTerm(request.prev_log_index()) == request.prev_log_term()) {
            std::vector<raftcore::LogEntry> appended;
            for (int i = 0; i < request.entries_size(); ++i) {
                const auto& entry = request.entries(i);
                const int64_t existing = _log->GetTerm(entry.index());
                if (existing != -1 && existing != entry.term()) {
                    if (entry.index() <= _commit_index)
                        throw std::runtime_error("attempt to replace committed Raft entry");
                    _log->TruncateSuffix(entry.index());
                }
                if (entry.index() > _log->LastIndex()) appended.push_back(entry);
            }
            if (!appended.empty()) {
                size_t bytes = 0;
                for (const auto& entry : appended) bytes += entry.command().size();
                const auto write_started = SteadyClock::now();
                _log->AppendBatch(appended);
                _follower_log_write.Observe(ElapsedMicros(write_started), appended.size(), bytes);
            }
            const int64_t matched = request.prev_log_index() + request.entries_size();
            _commit_index = std::max(_commit_index, std::min(request.leader_commit(), matched));
            ApplyCommitted();
            response.set_success(true);
            response.set_last_log_index(matched);
        }
    }
    std::string payload;
    response.SerializeToString(&payload);
    _peer_mgr->Send(from, RaftMsgType::kAppendEntriesResponse, payload);
}
void RaftNode::HandleAppendEntriesResponse(int from,
                                          const raftcore::AppendEntriesResponse& response) {
    if (!_running || !IsRemotePeer(from)) return;
    if (response.term() > _current_term) {
        BecomeFollower(response.term());
        return;
    }
    if (!IsLeader() || response.term() != _current_term) return;
    auto& flight = _inflight.at(from);
    if (!flight.id || response.rpc_id() != flight.id || response.last_log_index() < 0)
        return;
    if (response.success() && response.last_log_index() != flight.last_index) return;
    // One sample per correlated successful data RPC, including buffering and
    // retries since its first send attempt. This is not pure RTT/quorum latency.
    if (response.success() && flight.entries > 0)
        _replication_data_ack.Observe(ElapsedMicros(flight.first_send));
    const int64_t acknowledged = flight.last_index;
    flight = {};
    if (response.success()) {
        _match_index[from] = std::max(_match_index[from], acknowledged);
        _next_index[from] = _match_index[from] + 1;
        const int64_t before = _commit_index;
        AdvanceCommitIndex();
        if (_commit_index > before) BroadcastAppendEntries();
        if (_next_index[from] <= _log->LastIndex()) SendAppendEntries(from);
    } else {
        // Hints are bounded by known matches; a stale response cannot rewind progress.
        const int64_t hint = response.last_log_index() == INT64_MAX ?
                            INT64_MAX : response.last_log_index() + 1;
        _next_index[from] = std::max(_match_index[from] + 1,
                                    std::min(_next_index[from] - 1, hint));
        SendAppendEntries(from);
    }
}
void RaftNode::SendAppendEntries(int peer) {
    auto& flight = _inflight.at(peer);
    if (flight.id) {
        if (flight.elapsed_ms >= kRpcRetryMs) {
            flight.elapsed_ms = 0;
            ++_replication_retry_attempts;
            _peer_mgr->Send(peer, RaftMsgType::kAppendEntries, flight.payload);
        }
        return;
    }
    const int64_t previous = _next_index.at(peer) - 1;
    raftcore::AppendEntries request;
    request.set_term(_current_term);
    request.set_leader_id(_node_id);
    request.set_prev_log_index(previous);
    request.set_prev_log_term(_log->GetTerm(previous));
    request.set_leader_commit(_commit_index);
    if (_rpc_sequence == UINT64_MAX) throw std::runtime_error("Raft RPC sequence exhausted");
    request.set_rpc_id(++_rpc_sequence);
    size_t bytes = 128; // conservative protobuf envelope/varint overhead
    int64_t last = previous;
    for (int64_t index = _next_index.at(peer); index <= _log->LastIndex(); ++index) {
        raftcore::LogEntry entry;
        if (!_log->Get(index, &entry)) throw std::runtime_error("missing replication entry");
        const size_t size = entry.ByteSizeLong() + 16;
        if (size > kMaxBatchBytes - 128) throw std::runtime_error("oversized Raft entry");
        if (request.entries_size() >= kMaxBatchEntries || size > kMaxBatchBytes - bytes) break;
        *request.add_entries() = entry;
        bytes += size;
        last = index;
        if (index == INT64_MAX) break;
    }
    flight.id = request.rpc_id();
    flight.last_index = last;
    flight.elapsed_ms = 0;
    flight.entries = static_cast<size_t>(request.entries_size());
    request.SerializeToString(&flight.payload);
    flight.first_send = SteadyClock::now();
    _peer_mgr->Send(peer, RaftMsgType::kAppendEntries, flight.payload);
}
void RaftNode::BroadcastAppendEntries() {
    for (const auto& peer : _all_peers)
        if (peer.id != _node_id) SendAppendEntries(peer.id);
}
void RaftNode::AdvanceCommitIndex() {
    std::vector<int64_t> matched;
    for (const auto& peer : _all_peers) matched.push_back(_match_index.at(peer.id));
    std::sort(matched.begin(), matched.end(), std::greater<int64_t>());
    const int64_t candidate = matched[static_cast<size_t>(QuorumSize() - 1)];
    if (candidate > _commit_index && _log->GetTerm(candidate) == _current_term)
        _commit_index = candidate;
    ApplyCommitted();
}
void RaftNode::ApplyCommitted() {
    if (_apply_inflight) return;
    while (_running && _last_applied < _commit_index) {
        const int64_t first = _last_applied + 1;
        std::vector<std::string> commands;
        size_t bytes = 0;
        for (int64_t index = first; index <= _commit_index; ++index) {
            raftcore::LogEntry entry;
            if (!_log->Get(index, &entry)) throw std::runtime_error("missing committed entry");
            if (entry.command().size() > RespParser::kMaxCommandBytes)
                throw std::runtime_error("oversized committed entry");
            if (!commands.empty() && (commands.size() >= kMaxBatchEntries ||
                entry.command().size() > kMaxBatchBytes - bytes)) break;
            bytes += entry.command().size();
            commands.push_back(entry.command());
            if (index == _commit_index) break;
        }
        const size_t count = commands.size();
        if (_apply_executor) {
            _apply_inflight = true;
            const std::weak_ptr<int> lifetime = _lifetime;
            struct Timing { SteadyClock::time_point finished; uint64_t work_us = 0; };
            const auto timing = std::make_shared<Timing>();
            // Work may outlive this node; only the SM and owned input are used.
            const bool accepted = _apply_executor->Submit(
                [sm = _sm, first, commands = std::move(commands), timing] {
                    const auto started = SteadyClock::now();
                    auto results = sm->ApplyBatch(first, commands);
                    timing->finished = SteadyClock::now();
                    timing->work_us = ElapsedMicros(started, timing->finished);
                    return results;
                },
                [this, lifetime, first, count, bytes, timing](ApplyExecutor::Results results,
                                                             std::exception_ptr error) {
                    if (lifetime.expired()) return;
                    // Keep this set on error: the owner must fail-stop, not retry
                    // a batch whose durable outcome might be uncertain.
                    if (error) std::rethrow_exception(error);
                    FinishApply(first, count, results, timing->work_us, bytes,
                                ElapsedMicros(timing->finished));
                    _apply_inflight = false;
                    ApplyCommitted();
                });
            if (!accepted) throw std::runtime_error("state machine executor unavailable");
            return;
        }
        const auto started = SteadyClock::now();
        const auto results = _sm->ApplyBatch(first, commands);
        // Synchronous application has no worker-to-owner dispatch delay.
        FinishApply(first, count, results, ElapsedMicros(started), bytes, 0);
    }
}
void RaftNode::FinishApply(int64_t first, size_t count, const ApplyExecutor::Results& results,
                           uint64_t work_us, size_t bytes, uint64_t dispatch_us) {
    if (first != _last_applied + 1 || results.size() != count)
        throw std::runtime_error("out-of-order state machine completion");
    _last_applied += static_cast<int64_t>(count);
    ++_apply_batches;
    _kv_apply.Observe(work_us, count, bytes);
    _apply_dispatch.Observe(dispatch_us);
    // Publish the whole durable batch before callbacks, even if Stop was called.
    std::vector<std::pair<ProposeCallback, std::string>> callbacks;
    for (size_t i = 0; i < count; ++i) {
        const auto found = _pending.find(first + static_cast<int64_t>(i));
        if (found != _pending.end()) {
            _pending_bytes -= found->second.bytes;
            if (found->second.callback)
                callbacks.emplace_back(std::move(found->second.callback), results[i]);
            _pending.erase(found);
        }
    }
    for (auto& callback : callbacks) callback.first(true, callback.second);
}
void RaftNode::Tick() {
    if (!_running) return;
    if (_state != LEADER) {
        _election_timeout_ms -= kTickIntervalMs;
        if (_election_timeout_ms <= 0) BecomeCandidate();
    }
    if (_state == LEADER) {
        for (auto& item : _inflight)
            if (item.second.id) item.second.elapsed_ms += kTickIntervalMs;
        _heartbeat_timer_ms -= kTickIntervalMs;
        if (_heartbeat_timer_ms <= 0) {
            BroadcastAppendEntries();
            _heartbeat_timer_ms = kHeartbeatIntervalMs;
        }
    }
}
