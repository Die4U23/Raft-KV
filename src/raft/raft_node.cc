#include "raft/raft_node.h"
#include "common/log.h"
#include "common/resp_parser.h"
#include "raft/peers.h"
#include <algorithm>
#include <chrono>
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
    // The log snapshot is written before the KV install. A crash in between
    // leaves the image in the log and a KV that still ends at an older index.
    if (_log->SnapshotIndex() > _last_applied) {
        _sm->InstallSnapshot(_log->SnapshotIndex(), _log->SnapshotData());
        _last_applied = _sm->LastApplied();
    }
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
    std::string info = _leader_log_write.ToInfo("leader_log_write") +
           _follower_log_write.ToInfo("follower_log_write") +
           _replication_data_ack.ToInfo("replication_data_ack") +
           _kv_apply.ToInfo("kv_apply") + _apply_dispatch.ToInfo("apply_dispatch") +
           "replication_retry_attempts:" + std::to_string(_replication_retry_attempts) + "\r\n";

    // ReadIndex metrics
    info += "read_index_total:" + std::to_string(_read_index_total) + "\r\n";
    info += "read_index_succeeded:" + std::to_string(_read_index_succeeded) + "\r\n";
    info += "read_index_timeout:" + std::to_string(_read_index_timeout) + "\r\n";
    info += "read_index_not_leader:" + std::to_string(_read_index_not_leader) + "\r\n";
    info += "read_index_overload:" + std::to_string(_read_index_overload) + "\r\n";

    info += "read_index_pending:" + std::to_string(PendingReadIndexCount()) + "\r\n";
    info += "snapshot_index:" + std::to_string(_log->SnapshotIndex()) + "\r\n";

    return info;
}
const char* RaftNode::StateName() const {
    if (!_running) return "stopped";
    if (_state == LEADER) return "leader";
    if (_state == CANDIDATE) return "candidate";
    if (_state == PRE_CANDIDATE) return "pre-candidate";
    return "follower";
}
void RaftNode::Start() {
    _running = true;
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] start term=" << _current_term;
    BecomeFollower(_current_term);
    ApplyCommitted(); // resume committed backlog after Stop/Start
}
void RaftNode::Stop() {
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] stop term=" << _current_term;
    _running = false;
    _state = FOLLOWER;
    _leader_id = -1;
    FailPending("-ERR server stopped; outcome unknown\r\n");
    ClearReadIndexQueues("server stopped");
    _can_serve_read = false;
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
    if (!_storage_healthy) return -3;  // Reject proposals when storage is unhealthy
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
    try {
        _log->AppendBatch(entries); // one durable write before counting the local replica
        _leader_log_write.Observe(ElapsedMicros(write_started), entries.size(), bytes);
    } catch (const std::exception& e) {
        _storage_healthy = false;
        EventLog(LogLevel::Error) << "RaftNode[" << _node_id << "] log append failed: " << e.what();
        FailPending("-ERR storage failure; outcome unknown\r\n");
        throw;  // Let the exception propagate to process boundary (fail-stop)
    }
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
    const bool changed = _state != FOLLOWER || term > _current_term || _leader_id != -1;
    if (term > _current_term) {
        _log->SaveHardState(term, -1);
        _current_term = term;
        _voted_for = -1;
    }
    _state = FOLLOWER;
    _leader_id = -1;
    _votes.clear();
    _peer_active.clear();
    for (auto& item : _inflight) item.second = {};
    ResetElectionTimer();
    FailPending("-ERR leadership lost; outcome unknown\r\n");
    // Clear ReadIndex queues on step down
    ClearReadIndexQueues("leadership lost");
    _can_serve_read = false;
    if (changed) {
        EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] becomes follower term="
                                << _current_term;
    }
}
void RaftNode::BecomePreCandidate() {
    // A single node is already a quorum of one. Skip the extra round trip.
    if (QuorumSize() == 1) {
        BecomeCandidate();
        return;
    }
    if (_current_term == INT32_MAX) throw std::runtime_error("Raft term exhausted");
    _state = PRE_CANDIDATE;
    _leader_id = -1;
    _votes.clear();
    _votes.insert(_node_id);
    ResetElectionTimer();
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] becomes pre-candidate term="
                            << _current_term;
    raftcore::RequestVote request;
    request.set_term(_current_term + 1);
    request.set_candidate_id(_node_id);
    request.set_last_log_index(_log->LastIndex());
    request.set_last_log_term(_log->LastTerm());
    request.set_prevote(true);
    std::string payload;
    request.SerializeToString(&payload);
    _peer_mgr->Broadcast(RaftMsgType::kRequestVote, payload);
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
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] becomes candidate term="
                            << _current_term;
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
    _leader_since = Now();
    _peer_active.clear();
    NotePeerContact(_node_id);
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] becomes leader term="
                            << _current_term << " commit=" << _commit_index;
    for (const auto& peer : _all_peers) {
        _next_index[peer.id] = _log->LastIndex() + 1;
        _match_index[peer.id] = 0;
        _inflight[peer.id] = {};
    }
    _match_index[_node_id] = _log->LastIndex();
    _heartbeat_timer_ms = kHeartbeatIntervalMs;
    // Commit one entry in this term so recovered older entries can be applied.
    // This no-op also enables ReadIndex (once committed)
    _can_serve_read = false;
    Propose("", {});
}
void RaftNode::ResetElectionTimer() {
    const int timeout_ms = std::uniform_int_distribution<int>(
        kMinElectionTimeoutMs, kMaxElectionTimeoutMs)(_rng);
    _election_deadline = Now() + std::chrono::milliseconds(timeout_ms);
}
SteadyClock::time_point RaftNode::Now() const {
    return _clock ? _clock() : SteadyClock::now();
}
void RaftNode::NotePeerContact(int peer) {
    _peer_active[peer] = Now();
}
void RaftNode::CheckQuorum() {
    // The leader counts itself. Everyone else must have answered an
    // AppendEntries RPC in this term inside the minimum election timeout.
    NotePeerContact(_node_id);
    if (QuorumSize() <= 1) return;
    const auto now = Now();
    const auto window = std::chrono::milliseconds(kMinElectionTimeoutMs);
    if (now - _leader_since < window) return;
    int fresh = 0;
    for (const auto& peer : _all_peers) {
        const auto found = _peer_active.find(peer.id);
        if (found != _peer_active.end() && now - found->second < window)
            ++fresh;
    }
    if (fresh >= QuorumSize()) return;
    EventLog(LogLevel::Warning) << "RaftNode[" << _node_id << "] check quorum failed term="
                               << _current_term << " fresh=" << fresh
                               << " need=" << QuorumSize();
    BecomeFollower(_current_term);
}
bool RaftNode::IsLogUpToDate(int64_t index, int64_t term) const {
    return term != _log->LastTerm() ? term > _log->LastTerm() : index >= _log->LastIndex();
}
void RaftNode::HandleRequestVote(int from, const raftcore::RequestVote& request) {
    if (!_running || !IsRemotePeer(from) || request.candidate_id() != from ||
        request.term() <= 0 || request.last_log_index() < 0 || request.last_log_term() < 0 ||
        request.last_log_term() > request.term()) return;
    const bool prevote = request.prevote();
    auto reply = [&](bool granted) {
        raftcore::RequestVoteResponse response;
        response.set_term(_current_term);
        response.set_vote_granted(granted);
        response.set_prevote(prevote);
        std::string payload;
        response.SerializeToString(&payload);
        _peer_mgr->Send(from, RaftMsgType::kRequestVoteResponse, payload);
    };
    // The leader has heard from itself. Granting a pre-vote would let a peer
    // that can still reach this leader start a disruptive election.
    if (prevote && _state == LEADER) {
        EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                                << "] rejected pre-vote from " << from
                                << " term=" << request.term()
                                << " current=" << _current_term;
        reply(false);
        return;
    }
    // Raft thesis §4.2.3 / §9.6: a follower that has heard from a leader recently
    // must not bump its term or grant a vote or a pre-vote. Otherwise a delayed
    // ReadIndex probe ACK can confirm a read after a newer term has already won.
    if (_state == FOLLOWER && _leader_id != -1 && _leader_id != from &&
        Now() < _election_deadline) {
        EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                                << "] rejected disruptive " << (prevote ? "pre-vote" : "vote")
                                << " from " << from
                                << " term=" << request.term()
                                << " current=" << _current_term
                                << " leader=" << _leader_id;
        reply(false);
        return;
    }
    if (prevote) {
        // Do not adopt request.term or persist votedFor. A grant means this
        // node would vote in the next term; the caller raises the term only
        // after a majority of such grants.
        const bool grant = request.term() > _current_term &&
            IsLogUpToDate(request.last_log_index(), request.last_log_term());
        if (!grant) {
            EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                                    << "] rejected pre-vote from " << from
                                    << " term=" << request.term()
                                    << " current=" << _current_term;
        }
        reply(grant);
        return;
    }
    // Raft §5.1: If RPC contains term T > currentTerm, set currentTerm = T, convert to Follower
    if (request.term() > _current_term) BecomeFollower(request.term());
    // Raft §5.2, §5.4: Grant vote if candidate's log is at least as up-to-date as ours
    const bool grant = request.term() == _current_term &&
        (_voted_for == -1 || _voted_for == from) &&
        IsLogUpToDate(request.last_log_index(), request.last_log_term());
    if (grant) {
        _log->SaveHardState(_current_term, from);
        _voted_for = from;
        ResetElectionTimer();
    }
    reply(grant);
}
void RaftNode::HandleRequestVoteResponse(int from, const raftcore::RequestVoteResponse& response) {
    if (!_running || !IsRemotePeer(from)) return;
    if (response.term() > _current_term) {
        BecomeFollower(response.term());
        return;
    }
    if (_state == PRE_CANDIDATE) {
        // Grants carry the receiver's current term, which may be behind ours.
        // A peer that is already ahead took the branch above and stepped us down.
        if (response.prevote() && response.vote_granted() && response.term() <= _current_term) {
            _votes.insert(from);
            if (static_cast<int>(_votes.size()) >= QuorumSize()) BecomeCandidate();
        }
        return;
    }
    if (_state != CANDIDATE || response.prevote() || response.term() != _current_term) return;
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
    // Raft §5.1: If RPC request contains term T > currentTerm, set currentTerm = T.
    // A candidate or pre-candidate that hears the current leader reverts to follower.
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
                try {
                    _log->AppendBatch(appended);
                    _follower_log_write.Observe(ElapsedMicros(write_started), appended.size(), bytes);
                } catch (const std::exception& e) {
                    _storage_healthy = false;
                    EventLog(LogLevel::Error) << "RaftNode[" << _node_id
                                             << "] follower log append failed: " << e.what();
                    throw;  // Propagate to process boundary (fail-stop)
                }
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

    NotePeerContact(from);
    // Expire a probe round as soon as a late response arrives. Waiting for the
    // next Tick left the read hanging after the lease had already elapsed.
    CheckReadIndexTimeout();

    auto& flight = _inflight.at(from);

    // A matching in-flight RPC may also be a ReadIndex probe, but only if this
    // peer's rpc_id was recorded after the read round was created.
    if (flight.id && response.rpc_id() == flight.id)
        AckReadIndexProbe(from, flight.id);

    if (!flight.id || response.rpc_id() != flight.id || response.last_log_index() < 0) {
        FinishReadIndexRounds();
        return;
    }
    if (response.success() && response.last_log_index() != flight.last_index) {
        FinishReadIndexRounds();
        return;
    }
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
        // Start the next read round before sending so the new RPCs bind as probes.
        FinishReadIndexRounds();
        if (_commit_index > before) BroadcastAppendEntries();
        if (_next_index[from] <= _log->LastIndex()) SendAppendEntries(from);
    } else {
        // Hints are bounded by known matches; a stale response cannot rewind progress.
        const int64_t hint = response.last_log_index() == INT64_MAX ?
                            INT64_MAX : response.last_log_index() + 1;
        int64_t next = std::max(_match_index[from] + 1,
                               std::min(_next_index[from] - 1, hint));
        // The compacted prefix cannot be repaired one entry at a time.
        if (_log->SnapshotIndex() > 0 && next <= _log->SnapshotIndex())
            next = _log->SnapshotIndex();
        _next_index[from] = next;
        FinishReadIndexRounds();
        SendAppendEntries(from);
    }
}
void RaftNode::HandleInstallSnapshot(int from, const raftcore::InstallSnapshot& request) {
    if (!_running || !IsRemotePeer(from) || request.leader_id() != from ||
        request.term() <= 0 || request.rpc_id() == 0 ||
        request.last_included_index() <= 0 || request.last_included_term() <= 0 ||
        request.last_included_term() > request.term()) return;
    auto reply = [&](bool success) {
        raftcore::InstallSnapshotResponse response;
        response.set_term(_current_term);
        response.set_rpc_id(request.rpc_id());
        response.set_success(success);
        std::string payload;
        response.SerializeToString(&payload);
        _peer_mgr->Send(from, RaftMsgType::kInstallSnapshotResponse, payload);
    };
    if (request.term() > _current_term ||
        (request.term() == _current_term && _state != FOLLOWER))
        BecomeFollower(request.term());
    if (request.term() < _current_term) {
        reply(false);
        return;
    }
    _leader_id = from;
    ResetElectionTimer();
    const int64_t index = request.last_included_index();
    const int32_t snap_term = request.last_included_term();
    if (_log->SnapshotIndex() > index ||
        (_log->SnapshotIndex() == index && _log->SnapshotTerm() == snap_term &&
         _last_applied >= index)) {
        reply(true);
        return;
    }
    if (_log->SnapshotIndex() == index && _log->SnapshotTerm() != snap_term) {
        EventLog(LogLevel::Error) << "RaftNode[" << _node_id
                                 << "] rejected conflicting snapshot index=" << index;
        reply(false);
        return;
    }
    const bool matches = _log->GetTerm(index) == snap_term;
    if (_last_applied > index && !matches) {
        EventLog(LogLevel::Error) << "RaftNode[" << _node_id
                                 << "] snapshot conflicts with applied index " << _last_applied;
        throw std::runtime_error("snapshot conflicts with applied log");
    }
    if (request.offset() > _snapshot_recv.size() ||
        request.data().size() > std::numeric_limits<size_t>::max() - static_cast<size_t>(request.offset())) {
        _snapshot_recv.clear();
        reply(false);
        return;
    }
    const size_t offset = static_cast<size_t>(request.offset());
    if (offset == 0) {
        _snapshot_recv = request.data();
    } else if (offset == _snapshot_recv.size()) {
        _snapshot_recv.append(request.data());
    } else if (offset + request.data().size() <= _snapshot_recv.size() &&
               _snapshot_recv.compare(offset, request.data().size(), request.data()) == 0) {
        // Retransmit of a chunk that is already in the buffer.
    } else {
        _snapshot_recv.clear();
        reply(false);
        return;
    }
    if (!request.done()) {
        reply(true);
        return;
    }
    const std::string image = std::move(_snapshot_recv);
    _snapshot_recv.clear();
    if (!_sm->IsSnapshot(image)) {
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                   << "] rejected malformed snapshot from " << from;
        reply(false);
        return;
    }
    _log->SaveSnapshot(index, snap_term, image);
    if (_last_applied < index) {
        if (!_sm->TryInstallSnapshot(index, image))
            throw std::runtime_error("durable snapshot failed to install");
        _last_applied = _sm->LastApplied();
    }
    if (_commit_index < index) _commit_index = index;
    if (_commit_index > _log->LastIndex()) _commit_index = _log->LastIndex();
    ApplyCommitted();
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                            << "] installed snapshot index=" << index
                            << " term=" << snap_term;
    reply(true);
}
void RaftNode::HandleInstallSnapshotResponse(int from,
                                            const raftcore::InstallSnapshotResponse& response) {
    if (!_running || !IsRemotePeer(from)) return;
    if (response.term() > _current_term) {
        BecomeFollower(response.term());
        return;
    }
    if (!IsLeader() || response.term() != _current_term) return;
    NotePeerContact(from);
    auto& flight = _inflight.at(from);
    if (!flight.id || flight.type != RaftMsgType::kInstallSnapshot ||
        response.rpc_id() != flight.id) return;
    if (!response.success()) {
        const int64_t installed = flight.last_index;
        flight = {};
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                   << "] snapshot rejected by " << from
                                   << " index=" << installed;
        return;
    }
    if (!flight.snapshot_done) {
        flight.snapshot_offset += flight.snapshot_chunk;
        flight.id = 0;
        flight.payload.clear();
        SendSnapshotChunk(from);
        return;
    }
    const int64_t installed = flight.last_index;
    flight = {};
    _match_index[from] = std::max(_match_index[from], installed);
    _next_index[from] = _match_index[from] + 1;
    FinishReadIndexRounds();
    SendAppendEntries(from);
}
void RaftNode::SendInstallSnapshot(int peer) {
    auto& flight = _inflight.at(peer);
    if (flight.id) {
        if (flight.elapsed_ms >= kRpcRetryMs) {
            flight.elapsed_ms = 0;
            ++_replication_retry_attempts;
            _peer_mgr->Send(peer, flight.type, flight.payload);
        }
        return;
    }
    if (flight.snapshot_image.empty()) {
        if (_log->SnapshotIndex() <= 0 || _log->SnapshotData().empty()) return;
        flight.snapshot_image = _log->SnapshotData();
        flight.snapshot_offset = 0;
        flight.snapshot_term = _log->SnapshotTerm();
        flight.last_index = _log->SnapshotIndex();
    }
    SendSnapshotChunk(peer);
}
void RaftNode::SendSnapshotChunk(int peer) {
    auto& flight = _inflight.at(peer);
    if (flight.snapshot_image.empty() || flight.snapshot_offset > flight.snapshot_image.size()) {
        flight = {};
        return;
    }
    const size_t chunk_bytes = _snapshot_chunk_bytes == 0 ? kSnapshotChunkBytes
                                                          : _snapshot_chunk_bytes;
    const size_t n = std::min(chunk_bytes, flight.snapshot_image.size() - flight.snapshot_offset);
    raftcore::InstallSnapshot request;
    request.set_term(_current_term);
    request.set_leader_id(_node_id);
    request.set_last_included_index(flight.last_index);
    request.set_last_included_term(flight.snapshot_term);
    if (_rpc_sequence == UINT64_MAX) throw std::runtime_error("Raft RPC sequence exhausted");
    request.set_rpc_id(++_rpc_sequence);
    request.set_offset(flight.snapshot_offset);
    request.set_data(flight.snapshot_image.substr(flight.snapshot_offset, n));
    request.set_done(flight.snapshot_offset + n == flight.snapshot_image.size());
    flight.type = RaftMsgType::kInstallSnapshot;
    flight.id = request.rpc_id();
    flight.elapsed_ms = 0;
    flight.entries = 0;
    flight.snapshot_chunk = n;
    flight.snapshot_done = request.done();
    request.SerializeToString(&flight.payload);
    flight.first_send = SteadyClock::now();
    _peer_mgr->Send(peer, RaftMsgType::kInstallSnapshot, flight.payload);
}
void RaftNode::SendAppendEntries(int peer) {
    auto& flight = _inflight.at(peer);
    if (flight.id) {
        if (flight.elapsed_ms >= kRpcRetryMs) {
            flight.elapsed_ms = 0;
            ++_replication_retry_attempts;
            _peer_mgr->Send(peer, flight.type, flight.payload);
        }
        return;
    }
    if (_log->SnapshotIndex() > 0 && _next_index.at(peer) <= _log->SnapshotIndex()) {
        SendInstallSnapshot(peer);
        return;
    }
    const int64_t previous = _next_index.at(peer) - 1;
    if (previous > 0 && _log->GetTerm(previous) < 0) {
        _next_index[peer] = _log->SnapshotIndex();
        SendInstallSnapshot(peer);
        return;
    }
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
    flight.type = RaftMsgType::kAppendEntries;
    flight.id = request.rpc_id();
    flight.last_index = last;
    flight.elapsed_ms = 0;
    flight.entries = static_cast<size_t>(request.entries_size());
    request.SerializeToString(&flight.payload);
    flight.first_send = SteadyClock::now();
    BindReadIndexProbe(peer, flight.id);
    _peer_mgr->Send(peer, RaftMsgType::kAppendEntries, flight.payload);
}
void RaftNode::BroadcastAppendEntries() {
    for (const auto& peer : _all_peers)
        if (peer.id != _node_id) SendAppendEntries(peer.id);
}
void RaftNode::AdvanceCommitIndex() {
    // Raft §5.3, §5.4: Leader only commits entries from current term by counting replicas
    // Entry at index N is safe to commit if replicated on majority and term[N] == currentTerm
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
                    if (error) {
                        _storage_healthy = false;
                        std::rethrow_exception(error);
                    }
                    FinishApply(first, count, results, timing->work_us, bytes,
                                ElapsedMicros(timing->finished));
                    _apply_inflight = false;
                    ApplyCommitted();
                });
            if (!accepted) throw std::runtime_error("state machine executor unavailable");
            return;
        }
        const auto started = SteadyClock::now();
        try {
            const auto results = _sm->ApplyBatch(first, commands);
            // Synchronous application has no worker-to-owner dispatch delay.
            FinishApply(first, count, results, ElapsedMicros(started), bytes, 0);
        } catch (const std::exception& e) {
            _storage_healthy = false;
            throw;  // Propagate to process boundary (fail-stop)
        }
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

    // Check if no-op is committed (enables ReadIndex)
    if (_state == LEADER && !_can_serve_read) {
        // Check if we've committed an entry in current term
        if (_commit_index > 0 && _log->GetTerm(_commit_index) == _current_term) {
            _can_serve_read = true;
        }
    }

    // Process pending ReadIndex requests
    ProcessPendingReads();

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
    MaybeCompact();
}
void RaftNode::MaybeCompact() {
    if (!_running || !_storage_healthy || _snapshot_distance <= 0) return;
    if (_last_applied <= _log->SnapshotIndex()) return;
    if (_last_applied - _log->SnapshotIndex() < _snapshot_distance) return;
    const int64_t index = _last_applied;
    const int64_t term = _log->GetTerm(index);
    if (term <= 0 || term > INT32_MAX) return;
    std::string data;
    if (!_sm->TryExportSnapshot(&data)) {
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                   << "] snapshot export failed index=" << index;
        return;
    }
    if (data.size() > kMaxSnapshotBytes) {
        if (!_snapshot_skip_logged) {
            _snapshot_skip_logged = true;
            EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                       << "] snapshot exceeds " << kMaxSnapshotBytes
                                       << " bytes; log prefix kept";
        }
        return;
    }
    try {
        _log->SaveSnapshot(index, static_cast<int32_t>(term), data);
    } catch (const std::exception& e) {
        _storage_healthy = false;
        EventLog(LogLevel::Error) << "RaftNode[" << _node_id
                                 << "] snapshot failed: " << e.what();
        throw;
    }
    _snapshot_skip_logged = false;
    EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                            << "] compacted log through index=" << index
                            << " term=" << term;
}
void RaftNode::Tick() {
    if (!_running) return;
    if (_state != LEADER) {
        if (Now() >= _election_deadline) BecomePreCandidate();
    }
    if (_state == LEADER) {
        for (auto& item : _inflight)
            if (item.second.id) item.second.elapsed_ms += kTickIntervalMs;
        _heartbeat_timer_ms -= kTickIntervalMs;
        if (_heartbeat_timer_ms <= 0) {
            BroadcastAppendEntries();
            _heartbeat_timer_ms = kHeartbeatIntervalMs;
        }
        CheckReadIndexTimeout();
        CheckQuorum();
        if (!IsLeader()) return;
        FinishReadIndexRounds();
    }
}

// ReadIndex implementation
//
// A read is safe only after a majority replies to an AppendEntries RPC that
// was allocated after the request was recorded. Pre-existing in-flight RPCs
// and their retries are not probes. Later reads wait for a subsequent round
// instead of joining a round whose probes have already been sent.

size_t RaftNode::PendingReadIndexCount() const {
    size_t pending = _pending_reads.size() + _unsent_reads.size();
    for (const auto& round : _heartbeat_rounds)
        pending += round.requests.size();
    return pending;
}

bool RaftNode::RequestReadIndex(ReadIndexCallback callback) {
    ++_read_index_total;

    if (!IsLeader()) {
        ++_read_index_not_leader;
        const char* error = _leader_id == -1 ? "no leader" : "not leader";
        EventLog(LogLevel::Info) << "RaftNode[" << _node_id << "] read index rejected: " << error;
        callback(false, -1, error);
        return false;
    }

    if (!_can_serve_read) {
        ++_read_index_not_leader;
        EventLog(LogLevel::Info) << "RaftNode[" << _node_id
                                << "] read index rejected: waiting for no-op term="
                                << _current_term;
        callback(false, -1, "waiting for leader to commit no-op");
        return false;
    }

    if (PendingReadIndexCount() >= kMaxPendingReadIndex) {
        ++_read_index_overload;
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                   << "] read index queue full limit=" << kMaxPendingReadIndex;
        callback(false, -1, "read index queue full");
        return false;
    }

    ReadIndexRequest req;
    req.read_index = _commit_index;
    req.callback = std::move(callback);
    req.created_at = Now();
    _unsent_reads.push_back(std::move(req));
    if (!_completing_reads)
        FinishReadIndexRounds();
    return true;
}

void RaftNode::TryStartReadRound() {
    // A single-node confirmation invokes the callback, which may queue the
    // next read. Loop instead of recursing so a long pipeline does not grow
    // the stack with the queue depth.
    while (!_unsent_reads.empty() && _heartbeat_rounds.empty()) {
        HeartbeatRound round;
        round.round_id = ++_next_round_id;
        round.acks.insert(_node_id);
        round.requests.reserve(_unsent_reads.size());
        for (auto& req : _unsent_reads)
            round.requests.push_back(std::move(req));
        _unsent_reads.clear();
        round.sent_at = Now();
        round.confirmed = false;
        _heartbeat_rounds.push_back(std::move(round));
        _heartbeat_in_flight = true;

        if (QuorumSize() != 1)
            break;
        auto& started = _heartbeat_rounds.back();
        started.confirmed = true;
        ProcessConfirmedRound(started);
        _heartbeat_rounds.pop_front();
        _heartbeat_in_flight = false;
    }
}

void RaftNode::BindReadIndexProbe(int peer, uint64_t rpc_id) {
    for (auto& round : _heartbeat_rounds) {
        if (round.confirmed || round.probe_rpc_ids.count(peer) != 0)
            continue;
        round.probe_rpc_ids[peer] = rpc_id;
        return;
    }
}

void RaftNode::AckReadIndexProbe(int from, uint64_t rpc_id) {
    const auto now = Now();
    const auto lease = std::chrono::milliseconds(kMinElectionTimeoutMs);
    for (auto& round : _heartbeat_rounds) {
        const auto found = round.probe_rpc_ids.find(from);
        if (round.confirmed || found == round.probe_rpc_ids.end() || found->second != rpc_id)
            continue;
        // A delayed same-term ACK can arrive after a majority already elected a
        // new leader. Reject at the same age a follower is first allowed to
        // campaign (elapsed >= minimum election timeout), not one tick later.
        if (now - round.sent_at >= lease) {
            const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - round.sent_at).count();
            EventLog(LogLevel::Warning) << "RaftNode[" << _node_id
                                       << "] ignored expired probe ack from " << from
                                       << " rpc=" << rpc_id << " round=" << round.round_id
                                       << " age_ms=" << age_ms;
            return;
        }
        round.acks.insert(from);
        if (static_cast<int>(round.acks.size()) >= QuorumSize()) {
            round.confirmed = true;
            ProcessConfirmedRound(round);
        }
        return;
    }
}

void RaftNode::MaybeSendReadProbes() {
    if (_heartbeat_rounds.empty())
        return;
    auto& round = _heartbeat_rounds.front();
    if (round.confirmed)
        return;
    for (const auto& peer : _all_peers) {
        if (peer.id == _node_id || round.probe_rpc_ids.count(peer.id) != 0)
            continue;
        SendAppendEntries(peer.id);
    }
}

void RaftNode::FinishReadIndexRounds() {
    while (!_heartbeat_rounds.empty() && _heartbeat_rounds.front().confirmed)
        _heartbeat_rounds.pop_front();
    _heartbeat_in_flight = !_heartbeat_rounds.empty();
    TryStartReadRound();
    MaybeSendReadProbes();
}

void RaftNode::ProcessConfirmedRound(HeartbeatRound& round) {
    auto requests = std::move(round.requests);
    round.requests.clear();
    const bool nested = _completing_reads;
    _completing_reads = true;
    for (auto& req : requests) {
        if (_last_applied >= req.read_index) {
            req.callback(true, req.read_index, "");
            ++_read_index_succeeded;
        } else {
            _pending_reads.push_back(std::move(req));
        }
    }
    _completing_reads = nested;
}

void RaftNode::ProcessPendingReads() {
    const bool nested = _completing_reads;
    _completing_reads = true;
    while (!_pending_reads.empty()) {
        auto& req = _pending_reads.front();
        if (_last_applied >= req.read_index) {
            auto done = std::move(req);
            _pending_reads.pop_front();
            done.callback(true, done.read_index, "");
            ++_read_index_succeeded;
        } else {
            break;
        }
    }
    _completing_reads = nested;
    // A completing GET may have queued the next ReadIndex. Start its round now
    // instead of waiting for the next tick or AppendEntries response.
    if (!nested)
        FinishReadIndexRounds();
}

void RaftNode::CheckReadIndexTimeout() {
    const auto now = Now();
    const auto round_timeout = std::chrono::milliseconds(kMinElectionTimeoutMs);
    const auto timeout = std::chrono::milliseconds(kReadIndexTimeoutMs);
    const bool nested = _completing_reads;
    _completing_reads = true;

    while (!_heartbeat_rounds.empty()) {
        auto& round = _heartbeat_rounds.front();
        if (now - round.sent_at < round_timeout)
            break;
        const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - round.sent_at).count();
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id << "] read index round "
                                   << round.round_id << " expired age_ms=" << age_ms
                                   << " reads=" << round.requests.size();
        for (auto& req : round.requests) {
            req.callback(false, -1, "read index timeout");
            ++_read_index_timeout;
        }
        _heartbeat_rounds.pop_front();
    }
    _heartbeat_in_flight = !_heartbeat_rounds.empty();

    auto timeout_queue = [&](std::deque<ReadIndexRequest>& queue, const char* reason) {
        size_t expired = 0;
        while (!queue.empty()) {
            auto& req = queue.front();
            if (now - req.created_at <= timeout)
                break;
            req.callback(false, -1, reason);
            ++_read_index_timeout;
            ++expired;
            queue.pop_front();
        }
        if (expired != 0) {
            EventLog(LogLevel::Warning) << "RaftNode[" << _node_id << "] " << reason
                                       << " expired " << expired << " reads";
        }
    };
    timeout_queue(_unsent_reads, "read index timeout");
    timeout_queue(_pending_reads, "apply timeout");
    _completing_reads = nested;
}

void RaftNode::ClearReadIndexQueues(const std::string& reason) {
    const size_t pending = PendingReadIndexCount();
    if (pending != 0) {
        EventLog(LogLevel::Warning) << "RaftNode[" << _node_id << "] failing " << pending
                                   << " read index requests: " << reason;
    }
    const bool nested = _completing_reads;
    _completing_reads = true;
    auto fail = [&](std::deque<ReadIndexRequest>& queue) {
        for (auto& req : queue)
            req.callback(false, -1, reason);
        queue.clear();
    };
    for (auto& round : _heartbeat_rounds)
        for (auto& req : round.requests)
            req.callback(false, -1, reason);
    _heartbeat_rounds.clear();
    _heartbeat_in_flight = false;
    fail(_unsent_reads);
    fail(_pending_reads);
    _completing_reads = nested;
}

