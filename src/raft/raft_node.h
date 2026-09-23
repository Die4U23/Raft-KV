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
    void HandleInstallSnapshot(int from, const raftcore::InstallSnapshot& request);
    void HandleInstallSnapshotResponse(int from, const raftcore::InstallSnapshotResponse& response);
    void Tick();
    bool IsLeader() const { return _running && _state == LEADER; }
    bool IsHealthy() const { return _storage_healthy; }
    const char* StateName() const;
    int GetLeaderId() const { return _leader_id; }
    int GetNodeId() const { return _node_id; }
    int GetCurrentTerm() const { return _current_term; }
    int64_t GetCommitIndex() const { return _commit_index; }
    int64_t GetLastApplied() const { return _last_applied; }
    int64_t GetSnapshotIndex() const { return _log->SnapshotIndex(); }
    // Tests compact after this many newly applied entries. Non-positive disables it.
    void SetSnapshotDistanceForTest(int64_t entries) { _snapshot_distance = entries; }
    // Tests split an InstallSnapshot into chunks of this many bytes.
    void SetSnapshotChunkBytesForTest(size_t bytes) {
        _snapshot_chunk_bytes = bytes == 0 ? kSnapshotChunkBytes : bytes;
    }
    int64_t MatchIndexOf(int peer_id) const {
        const auto found = _match_index.find(peer_id);
        return found == _match_index.end() ? -1 : found->second;
    }
    bool HasInflightRpc(int peer_id) const {
        const auto found = _inflight.find(peer_id);
        return found != _inflight.end() && found->second.id != 0;
    }
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
    // Empty in production: Now() is steady_clock. Tests install a manual clock
    // so elections and the ReadIndex lease advance together without sleeping.
    void SetClockForTest(std::function<SteadyClock::time_point()> clock) {
        _clock = std::move(clock);
    }

    static constexpr int kTickIntervalMs = 10;
    static constexpr int kHeartbeatIntervalMs = 50;
    static constexpr int kMinElectionTimeoutMs = 150;
    static constexpr int kMaxElectionTimeoutMs = 300;
    // Compact after this many applied entries past the previous snapshot.
    static constexpr int64_t kSnapshotDistance = 1024;
    // Whole KV image. Larger exports skip compaction and keep the log.
    static constexpr size_t kMaxSnapshotBytes = 8 * 1024 * 1024;
    // One InstallSnapshot RPC. The receiver installs only after done.
    static constexpr size_t kSnapshotChunkBytes = 1 * 1024 * 1024;
private:
    enum State { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER };
    struct Inflight {
        uint64_t id = 0;
        int64_t last_index = 0;
        int elapsed_ms = 0;
        size_t entries = 0;
        RaftMsgType type = RaftMsgType::kAppendEntries;
        SteadyClock::time_point first_send;
        std::string payload;
        // Copied when a snapshot transfer starts so a newer local snapshot
        // cannot change the bytes of a chunk sequence already in flight.
        std::string snapshot_image;
        size_t snapshot_offset = 0;
        size_t snapshot_chunk = 0;
        int32_t snapshot_term = 0;
        bool snapshot_done = false;
    };

    // ReadIndex request
    struct ReadIndexRequest {
        int64_t read_index;                     // commitIndex when request arrived
        ReadIndexCallback callback;             // callback function
        SteadyClock::time_point created_at;     // creation time for timeout
    };

    // Heartbeat round for ReadIndex.
    // A probe ACK counts only when its rpc_id was allocated after this round
    // was created. Per-peer inflight rpc_ids are independent; round_id is not
    // compared against them.
    struct HeartbeatRound {
        uint64_t round_id = 0;
        std::set<int> acks;                     // peers that acknowledged (includes self)
        std::vector<ReadIndexRequest> requests; // frozen when the round is created
        std::map<int, uint64_t> probe_rpc_ids;  // peer -> rpc_id sent after round creation
        SteadyClock::time_point sent_at{};
        bool confirmed = false;
    };

    void BecomeFollower(int32_t term);
    // Election timeout enters pre-vote. The term and votedFor stay unchanged
    // until a majority of pre-votes allows BecomeCandidate().
    void BecomePreCandidate();
    void BecomeCandidate();
    void BecomeLeader();
    void ResetElectionTimer();
    SteadyClock::time_point Now() const;
    void NotePeerContact(int peer);
    void CheckQuorum();
    bool IsLogUpToDate(int64_t index, int64_t term) const;
    bool IsRemotePeer(int id) const;
    void SendAppendEntries(int peer);
    void SendInstallSnapshot(int peer);
    void SendSnapshotChunk(int peer);
    void BroadcastAppendEntries();
    void MaybeCompact();
    void AdvanceCommitIndex();
    void ApplyCommitted();
    void FinishApply(int64_t first, size_t count, const ApplyExecutor::Results& results,
                     uint64_t work_us, size_t bytes, uint64_t dispatch_us);
    void FailPending(const std::string& result);
    int QuorumSize() const { return static_cast<int>(_all_peers.size()) / 2 + 1; }

    // ReadIndex internal methods
    size_t PendingReadIndexCount() const;
    void TryStartReadRound();
    void BindReadIndexProbe(int peer, uint64_t rpc_id);
    void AckReadIndexProbe(int from, uint64_t rpc_id);
    void MaybeSendReadProbes();
    void FinishReadIndexRounds();
    void ProcessConfirmedRound(HeartbeatRound& round);
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
    // Absolute steady-clock deadline. Tick observes it; it does not subtract a
    // fixed 10 ms, so an early timer wakeup cannot open a ReadIndex window.
    SteadyClock::time_point _election_deadline{};
    SteadyClock::time_point _leader_since{};
    std::map<int, SteadyClock::time_point> _peer_active;
    std::function<SteadyClock::time_point()> _clock;
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
    std::deque<HeartbeatRound> _heartbeat_rounds;       // At most one unconfirmed round
    bool _heartbeat_in_flight = false;                  // Has an unconfirmed read round
    bool _completing_reads = false;                     // True while invoking read callbacks
    std::deque<ReadIndexRequest> _unsent_reads;         // Waiting for a round that has not been sent
    std::deque<ReadIndexRequest> _pending_reads;        // Waiting for lastApplied >= readIndex

    // ReadIndex metrics
    uint64_t _read_index_total = 0;
    uint64_t _read_index_succeeded = 0;
    uint64_t _read_index_timeout = 0;
    uint64_t _read_index_not_leader = 0;
    uint64_t _read_index_overload = 0;

    // Retry an unacknowledged heartbeat before the minimum election timeout.
    static constexpr int kRpcRetryMs = kHeartbeatIntervalMs;
    static constexpr size_t kMaxPending = 1024;
    static constexpr size_t kMaxPendingBytes = 16 * 1024 * 1024;
    static constexpr size_t kMaxBatchBytes = 2 * 1024 * 1024;
    static constexpr int kMaxBatchEntries = 128;
    static constexpr size_t kMaxPendingReadIndex = 10000;  // Max pending ReadIndex requests
    // Apply-lag and unsent-queue bound. Probe rounds expire at
    // kMinElectionTimeoutMs on the same clock as the election deadline: a
    // follower cannot campaign until at least that long after a heartbeat,
    // and a leader rejects a probe ACK at the same age.
    static constexpr int kReadIndexTimeoutMs = 1000;
    int64_t _snapshot_distance = kSnapshotDistance;
    size_t _snapshot_chunk_bytes = kSnapshotChunkBytes;
    bool _snapshot_skip_logged = false;
    // Chunks of the snapshot currently being received. Offset 0 replaces it.
    std::string _snapshot_recv;
};
