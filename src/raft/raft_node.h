#pragma once
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>
#include "common/metrics.h"
#include "raft/apply_executor.h"
#include "raft/peer_manager.h"
#include "raft/kv_state_machine.h"
#include "raftcore/raft_log.h"
#include "raft_messages.pb.h"

// All calls and callbacks run on the owner EventLoop thread.
// Callbacks must enqueue subsequent Raft operations rather than re-enter them.
// Storage exceptions must escape to the process boundary (fail-stop).
class RaftNode {
public:
    using ProposeCallback = std::function<void(bool, const std::string&)>;
    struct Proposal { std::string command; ProposeCallback callback; };

    // ReadIndex callback: (success, read_index, error_message)
    using ReadIndexCallback = std::function<void(bool, int64_t, const std::string&)>;

    RaftNode(int node_id, const std::vector<PeerInfo>& peers,
             muduo::net::EventLoop* loop, const std::string& log_path,
             KVStateMachine* sm, PeerManager* peer_mgr,
             ApplyExecutor* apply_executor = nullptr);
    ~RaftNode();
    void Start();
    void Stop();
    // -1: not leader/stopped; -2: admission limit; -3: storage unhealthy.
    // No callback on rejection.
    // Once admitted, timeout/disconnection does NOT cancel the replicated entry.
    int64_t Propose(const std::string& command, ProposeCallback callback);
    // Returns the first index; a batch is admitted/persisted together.
    int64_t ProposeBatch(std::vector<Proposal> proposals);
    void HandleRequestVote(int from, const raftcore::RequestVote& request);
    void HandleRequestVoteResponse(int from, const raftcore::RequestVoteResponse& response);
    void HandleAppendEntries(int from, const raftcore::AppendEntries& request);
    void HandleAppendEntriesResponse(int from, const raftcore::AppendEntriesResponse& response);
    void Tick();
    bool IsLeader() const { return _running && _state == LEADER; }
    bool IsHealthy() const { return _storage_healthy; }
    const char* StateName() const;
    int GetLeaderId() const { return _leader_id; }
    int GetNodeId() const { return _node_id; }
    int GetCurrentTerm() const { return _current_term; }
    int64_t GetCommitIndex() const { return _commit_index; }
    int64_t GetLastApplied() const { return _last_applied; }
    size_t PendingProposals() const { return _pending.size(); }
    size_t PendingBytes() const { return _pending_bytes; }
    uint64_t ProposalBatches() const { return _proposal_batches; }
    uint64_t ApplyBatches() const { return _apply_batches; }
    bool ApplyInFlight() const { return _apply_inflight; }
    bool AsyncApplyEnabled() const { return _apply_executor != nullptr; }
    std::string MetricsInfo() const;

    // ReadIndex: request a safe read_index for linearizable reads
    // Callback will be invoked when read_index is safe to read
    // Returns false if not leader or cannot serve reads yet
    bool RequestReadIndex(ReadIndexCallback callback);
private:
    enum State { FOLLOWER, CANDIDATE, LEADER };
    struct Inflight {
        uint64_t id = 0;
        int64_t last_index = 0;
        int elapsed_ms = 0;
        size_t entries = 0;
        SteadyClock::time_point first_send;
        std::string payload;
    };

    // ReadIndex request
    struct ReadIndexRequest {
        int64_t read_index;                     // commitIndex when request arrived
        ReadIndexCallback callback;             // callback function
        SteadyClock::time_point created_at;     // creation time for timeout
    };

    // One batch of reads confirmed by AppendEntries RPCs allocated after those
    // reads were queued. rpc ids are per peer; a round is not a single rpc id.
    struct HeartbeatRound {
        uint64_t round_id = 0;
        std::map<int, uint64_t> probe_rpc_ids;
        std::set<int> acks;
        std::vector<ReadIndexRequest> requests;
        SteadyClock::time_point created_at{};
        SteadyClock::time_point sent_at{};
        bool closed = false;
        bool confirmed = false;
    };

    void BecomeFollower(int32_t term);
    void BecomeCandidate();
    void BecomeLeader();
    void ResetElectionTimer();
    bool IsLogUpToDate(int64_t index, int64_t term) const;
    bool IsRemotePeer(int id) const;
    void SendAppendEntries(int peer);
    void BroadcastAppendEntries();
    void AdvanceCommitIndex();
    void ApplyCommitted();
    void FinishApply(int64_t first, size_t count, const ApplyExecutor::Results& results,
                     uint64_t work_us, size_t bytes, uint64_t dispatch_us);
    void FailPending(const std::string& result);
    int QuorumSize() const { return static_cast<int>(_all_peers.size()) / 2 + 1; }

    // ReadIndex internal methods
    void TryDispatchReadBarrier();
    void BindReadProbe(int peer, uint64_t rpc_id);
    bool OldestRoundNeedsProbe(int peer) const;
    void ObserveReadBarrierAck(int from, uint64_t rpc_id);
    void FinishConfirmedReadRounds();
    void InvokeReadyReads(std::vector<ReadIndexRequest> ready);
    void ProcessPendingReads();
    void CheckReadIndexTimeout();
    void ClearReadIndexQueues(const std::string& reason);

    int _node_id;
    std::vector<PeerInfo> _all_peers;
    KVStateMachine* _sm;
    PeerManager* _peer_mgr;
    // Executor drains before SM destruction. Completions run on the owner thread.
    ApplyExecutor* _apply_executor;
    std::shared_ptr<int> _lifetime = std::make_shared<int>(0);
    bool _apply_inflight = false;
    bool _storage_healthy = true;  // Set to false on storage exceptions
    std::unique_ptr<RaftLog> _log;
    int32_t _current_term = 0;
    int32_t _voted_for = -1;
    State _state = FOLLOWER;
    bool _running = false;
    int64_t _commit_index = 0;
    int64_t _last_applied = 0;
    int _leader_id = -1;
    std::set<int> _votes;
    std::map<int, int64_t> _next_index;
    std::map<int, int64_t> _match_index;
    std::map<int, Inflight> _inflight;
    uint64_t _rpc_sequence = 0;
    int _election_timeout_ms = 0;
    int _heartbeat_timer_ms = 0;
    struct Pending { ProposeCallback callback; size_t bytes; };
    std::map<int64_t, Pending> _pending;
    size_t _pending_bytes = 0;
    uint64_t _proposal_batches = 0;
    uint64_t _apply_batches = 0;
    // Batch bytes are command payload bytes, including zero-byte internal no-ops.
    BatchStats _leader_log_write, _follower_log_write, _kv_apply;
    LatencyStats _replication_data_ack, _apply_dispatch;
    uint64_t _replication_retry_attempts = 0;
    std::mt19937 _rng;

    // ReadIndex state
    bool _can_serve_read = false;                       // Can serve read after committing no-op
    uint64_t _next_round_id = 0;                        // Next heartbeat round ID
    std::deque<HeartbeatRound> _heartbeat_rounds;       // Unconfirmed read-barrier rounds, oldest first
    std::deque<ReadIndexRequest> _pending_reads;        // Waiting for lastApplied >= readIndex

    // ReadIndex metrics
    uint64_t _read_index_total = 0;
    uint64_t _read_index_succeeded = 0;
    uint64_t _read_index_timeout = 0;
    uint64_t _read_index_not_leader = 0;
    uint64_t _read_index_overload = 0;

    static constexpr int kTickIntervalMs = 10;
    static constexpr int kHeartbeatIntervalMs = 50;
    // Retry an unacknowledged heartbeat before the minimum election timeout.
    static constexpr int kRpcRetryMs = kHeartbeatIntervalMs;
    static constexpr size_t kMaxPending = 1024;
    static constexpr size_t kMaxPendingBytes = 16 * 1024 * 1024;
    static constexpr size_t kMaxBatchBytes = 2 * 1024 * 1024;
    static constexpr int kMaxBatchEntries = 128;
    static constexpr size_t kMaxPendingReadIndex = 10000;  // Max pending ReadIndex requests
    static constexpr int kReadIndexTimeoutMs = 1000;        // ReadIndex timeout
};
