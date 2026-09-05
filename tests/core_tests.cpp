// These tests compile the production consensus and storage sources against
// explicitly in-process test doubles. They do not exercise disk or TCP I/O.
#include "raft/raft_node.h"
#include "raft/apply_executor.h"
#include <deque>
#include <iostream>
#include <stdexcept>

static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
static uint64_t Metric(const RaftNode& node, const std::string& name) {
    const auto info = node.MetricsInfo();
    const auto start = info.find(name + ":");
    Check(start != std::string::npos && (start == 0 || info[start - 1] == '\n'),
          "missing metric field");
    return std::stoull(info.substr(start + name.size() + 1));
}
template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected storage/consistency failure");
}
static std::string Command(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}
static raftcore::LogEntry Entry(int64_t index, int term, std::string command = {}) {
    raftcore::LogEntry entry;
    entry.set_index(index); entry.set_term(term); entry.set_command(command);
    return entry;
}
struct Message { int from, to; RaftMsgType type; std::string payload; };
struct Member {
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<PeerManager> transport;
    std::unique_ptr<RaftNode> raft;
};

// Work and owner-thread completion are advanced independently. This exercises
// ordering without making the deliberately in-process DB double thread-safe.
class ManualApplyExecutor : public ApplyExecutor {
public:
    bool Submit(Work work, Completion completion) override {
        Check(!busy, "more than one apply batch submitted before completion");
        busy = true;
        worked = false;
        ++submissions;
        work_ = std::move(work);
        completion_ = std::move(completion);
        return true;
    }
    void RunWork() {
        Check(busy && !worked, "invalid manual apply work transition");
        worked = true;
        try { results_ = work_(); } catch (...) { error_ = std::current_exception(); }
        work_ = {};
    }
    void Complete() {
        Check(busy && worked, "completion arrived before apply work");
        auto completion = std::move(completion_);
        auto results = std::move(results_);
        auto error = std::move(error_);
        error_ = nullptr;
        busy = false;
        worked = false;
        completion(std::move(results), error);
    }
    void Finish() { RunWork(); Complete(); }
    bool busy = false;
    bool worked = false;
    int submissions = 0;
private:
    Work work_;
    Completion completion_;
    Results results_;
    std::exception_ptr error_;
};

class Cluster {
public:
    explicit Cluster(std::vector<int> ids = {10, 30, 50}, ApplyExecutor* executor = nullptr) {
        static int sequence = 0;
        prefix = "cluster-" + std::to_string(++sequence) + "/";
        if (executor) executors[ids.front()] = executor;
        for (int id : ids) peers.push_back({id, "127.0.0.1", 9000 + id});
        for (int id : ids) Restart(id);
    }
    std::string Path(int id, const char* suffix) const {
        return prefix + std::to_string(id) + suffix;
    }
    void Restart(int id) {
        members.erase(id);
        auto member = std::make_unique<Member>();
        member->sm = std::make_unique<KVStateMachine>(Path(id, "/kv"));
        member->transport = std::make_unique<PeerManager>(id, peers,
            [this](int from, int to, RaftMsgType type, const std::string& payload) {
                messages.push_back({from, to, type, payload});
            });
        member->raft = std::make_unique<RaftNode>(id, peers, nullptr, Path(id, "/log"),
            member->sm.get(), member->transport.get(),
            executors.count(id) ? executors.at(id) : nullptr);
        member->raft->Start();
        members.emplace(id, std::move(member));
    }
    RaftNode& Node(int id) { return *members.at(id)->raft; }
    KVStateMachine& State(int id) { return *members.at(id)->sm; }
    void Candidate(int id) {
        for (int ticks = 0; ticks < 31 && std::string(Node(id).StateName()) == "follower"; ++ticks)
            Node(id).Tick();
        Check(std::string(Node(id).StateName()) == "candidate" || Node(id).IsLeader(),
              "election did not start within bounded ticks");
    }
    void Deliver(const Message& message) {
        if (!members.count(message.to)) return;
        auto& node = Node(message.to);
        switch (message.type) {
        case RaftMsgType::kRequestVote: {
            raftcore::RequestVote rpc; Check(rpc.ParseFromString(message.payload), "vote decode");
            node.HandleRequestVote(message.from, rpc); break;
        }
        case RaftMsgType::kRequestVoteResponse: {
            raftcore::RequestVoteResponse rpc; Check(rpc.ParseFromString(message.payload), "vote reply decode");
            node.HandleRequestVoteResponse(message.from, rpc); break;
        }
        case RaftMsgType::kAppendEntries: {
            raftcore::AppendEntries rpc; Check(rpc.ParseFromString(message.payload), "append decode");
            node.HandleAppendEntries(message.from, rpc); break;
        }
        case RaftMsgType::kAppendEntriesResponse: {
            raftcore::AppendEntriesResponse rpc; Check(rpc.ParseFromString(message.payload), "append reply decode");
            node.HandleAppendEntriesResponse(message.from, rpc); break;
        }
        }
    }
    void Pump() {
        int remaining = 10000;
        while (!messages.empty()) {
            Check(--remaining > 0, "message processing did not settle");
            auto message = messages.front(); messages.pop_front(); Deliver(message);
        }
    }
    void Settle() {
        for (int tick = 0; tick < 10; ++tick) {
            for (auto& member : members) member.second->raft->Tick();
            Pump();
        }
    }
    void Elect(int id) { Candidate(id); Pump(); Settle(); Check(Node(id).IsLeader(), "leader election failed"); }
    std::vector<PeerInfo> peers;
    std::map<int, std::unique_ptr<Member>> members;
    std::map<int, ApplyExecutor*> executors;
    std::deque<Message> messages;
    std::string prefix;
};

static void QuorumAndDuplicateVotes() {
    Cluster cluster({10, 30, 50, 70, 90});
    cluster.Candidate(10);
    raftcore::RequestVoteResponse vote;
    vote.set_term(cluster.Node(10).GetCurrentTerm()); vote.set_vote_granted(true);
    cluster.Node(10).HandleRequestVoteResponse(30, vote);
    cluster.Node(10).HandleRequestVoteResponse(30, vote);
    Check(!cluster.Node(10).IsLeader(), "duplicate vote formed a majority");
    cluster.Node(10).HandleRequestVoteResponse(50, vote);
    Check(cluster.Node(10).IsLeader(), "three distinct votes did not elect leader");
    cluster.messages.clear();
    int success = 0, failure = 0;
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool ok, const std::string&) { ok ? ++success : ++failure; }) > 0, "proposal rejected");
    Check(success == 0 && cluster.Node(10).GetCommitIndex() == 0,
          "minority acknowledged or committed a write");
    vote.set_term(vote.term() + 1);
    cluster.Node(10).HandleRequestVoteResponse(90, vote);
    Check(!cluster.Node(10).IsLeader() && success == 0 && failure == 1,
          "leadership loss did not fail pending request exactly once");
}

static void ReplicationAndRecovery() {
    Cluster cluster;
    cluster.Elect(10);
    int callbacks = 0;
    const auto index = cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool ok, const std::string& value) { Check(ok && value == "+OK\r\n", "SET result"); ++callbacks; });
    cluster.Pump(); cluster.Settle();
    for (const auto& peer : cluster.peers) {
        std::string value;
        Check(cluster.State(peer.id).Get("default:k", &value) && value == "v", "replica state mismatch");
        Check(cluster.Node(peer.id).GetLastApplied() == index, "replica application lag");
    }
    Check(callbacks == 1, "proposal callback count");
    auto storage = rocksdb::testing::StateFor(cluster.Path(30, "/kv"));
    const auto writes = storage->successful_writes;
    cluster.Restart(30); cluster.Settle();
    Check(cluster.Node(30).GetLastApplied() == index && storage->successful_writes == writes,
          "recovery replayed already applied entries");
    std::string result;
    cluster.Node(10).Propose(Command({"DEL", "default:missing"}),
        [&](bool ok, const std::string& value) { Check(ok, "DEL failed"); result = value; });
    cluster.Pump(); cluster.Settle();
    Check(result == ":0\r\n", "missing DEL returned one");
    cluster.members.erase(10); cluster.messages.clear();
    cluster.Elect(50);
    bool acknowledged = false;
    cluster.Node(50).Propose(Command({"SET", "default:after", "restart"}),
        [&](bool ok, const std::string&) { acknowledged = ok; });
    cluster.Pump(); cluster.Settle();
    Check(acknowledged, "survivors could not commit");
    cluster.Restart(10); cluster.Settle();
    std::string value;
    Check(cluster.State(10).Get("default:after", &value) && value == "restart",
          "restarted former leader did not catch up");
}

static raftcore::AppendEntries Append(int term, uint64_t rpc, int64_t prev, int prev_term, int64_t commit) {
    raftcore::AppendEntries request;
    request.set_term(term); request.set_leader_id(30); request.set_rpc_id(rpc);
    request.set_prev_log_index(prev); request.set_prev_log_term(prev_term); request.set_leader_commit(commit);
    return request;
}
static raftcore::AppendEntriesResponse LastResponse(Cluster& cluster) {
    Check(!cluster.messages.empty(), "missing follower response");
    raftcore::AppendEntriesResponse response;
    Check(cluster.messages.back().type == RaftMsgType::kAppendEntriesResponse &&
          response.ParseFromString(cluster.messages.back().payload), "invalid follower response");
    cluster.messages.clear(); return response;
}
static void PrefixConflictAndCommitBound() {
    Cluster cluster;
    auto request = Append(1, 1, 0, 0, 0);
    *request.add_entries() = Entry(1, 1, Command({"SET", "default:a", "one"}));
    *request.add_entries() = Entry(2, 1, Command({"SET", "default:b", "old"}));
    cluster.Node(10).HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success(), "initial follower append failed");
    request = Append(2, 2, 1, 2, 0);
    cluster.Node(10).HandleAppendEntries(30, request);
    auto response = LastResponse(cluster);
    Check(!response.success() && response.last_log_index() == 2, "prefix rejection deleted log suffix");
    // A short heartbeat can confirm only index 1, despite a longer local tail.
    request = Append(2, 3, 1, 1, 2);
    cluster.Node(10).HandleAppendEntries(30, request);
    response = LastResponse(cluster);
    Check(response.success() && response.last_log_index() == 1 &&
          cluster.Node(10).GetCommitIndex() == 1, "unmatched tail was committed/acknowledged");
    request = Append(2, 4, 1, 1, 2);
    *request.add_entries() = Entry(2, 2, Command({"SET", "default:b", "new"}));
    cluster.Node(10).HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success(), "actual conflict replacement failed");
    std::string value;
    Check(cluster.State(10).Get("default:b", &value) && value == "new", "conflicting tail was applied");
    request = Append(3, 5, 0, 0, 0);
    *request.add_entries() = Entry(1, 3);
    Throws([&] { cluster.Node(10).HandleAppendEntries(30, request); });
    Check(cluster.messages.empty(), "committed replacement acknowledged");
}

static void LostResponseAndStaleRpc() {
    Cluster cluster;
    cluster.Candidate(10);
    raftcore::RequestVoteResponse vote;
    vote.set_term(cluster.Node(10).GetCurrentTerm()); vote.set_vote_granted(true);
    cluster.messages.clear(); cluster.Node(10).HandleRequestVoteResponse(30, vote);
    Message request{};
    for (const auto& message : cluster.messages) if (message.to == 30) request = message;
    cluster.messages.clear(); cluster.Deliver(request);
    auto reply = LastResponse(cluster); // drop this response
    const int term = cluster.Node(30).GetCurrentTerm();
    raftcore::AppendEntriesResponse stale = reply;
    stale.set_rpc_id(reply.rpc_id() + 100);
    cluster.Node(10).HandleAppendEntriesResponse(30, stale);
    Check(cluster.Node(10).GetCommitIndex() == 0, "unrelated RPC reply committed log");
    Check(Metric(cluster.Node(10), "replication_data_ack_count") == 0,
          "uncorrelated ACK counted as successful replication");
    for (int tick = 0; tick < 10; ++tick) { cluster.Node(10).Tick(); cluster.Node(30).Tick(); }
    bool retried = false;
    auto messages = std::move(cluster.messages); cluster.messages.clear();
    Check(Metric(cluster.Node(10), "replication_retry_attempts") == messages.size(),
          "retry send attempts were not counted once each");
    for (const auto& message : messages) if (message.to == 30 && message.type == RaftMsgType::kAppendEntries) {
        raftcore::AppendEntries decoded;
        Check(decoded.ParseFromString(message.payload), "retry decode");
        Check(decoded.rpc_id() == reply.rpc_id(), "retry replaced active RPC id");
        retried = true; cluster.Deliver(message);
    }
    Check(retried && cluster.Node(30).GetCurrentTerm() == term, "lost reply suppressed heartbeat until election");
    Check(Metric(cluster.Node(30), "follower_log_write_count") == 1 &&
          Metric(cluster.Node(30), "follower_log_write_entries") == 1 &&
          Metric(cluster.Node(10), "replication_data_ack_count") == 0,
          "retry counted as a new write or successful RPC");
    cluster.messages.clear(); cluster.Node(10).HandleAppendEntriesResponse(30, reply);
    Check(cluster.Node(10).GetCommitIndex() == 1, "valid correlated reply did not commit");
    cluster.Node(10).HandleAppendEntriesResponse(30, reply);
    Check(cluster.Node(10).GetCommitIndex() == 1, "duplicate reply changed committed prefix");
    Check(Metric(cluster.Node(10), "replication_data_ack_count") == 1,
          "duplicate ACK counted twice");
}

static void BatchedReplicationAndLimits() {
    Cluster cluster;
    cluster.Elect(10);
    auto leader_log = rocksdb::testing::StateFor(cluster.Path(10, "/log"));
    auto follower_log = rocksdb::testing::StateFor(cluster.Path(30, "/log"));
    const auto leader_before = leader_log->successful_writes;
    const auto follower_before = follower_log->successful_writes;
    const auto applies_before = cluster.Node(10).ApplyBatches();
    const auto acks_before = Metric(cluster.Node(10), "replication_data_ack_count");
    const auto log_bytes_before = Metric(cluster.Node(10), "leader_log_write_bytes");
    std::vector<RaftNode::Proposal> batch;
    int callbacks = 0;
    size_t bytes = 0;
    for (int i = 0; i < 64; ++i) {
        const auto command = Command({"SET", "default:batch-" + std::to_string(i), "v"});
        bytes += command.size();
        batch.push_back({command, [&, i](bool ok, const std::string& result) {
            Check(ok && result == "+OK\r\n" && callbacks == i, "batch callback order");
            ++callbacks;
        }});
    }
    Check(cluster.Node(10).ProposeBatch(std::move(batch)) == 2, "batch admission index");
    Check(leader_log->successful_writes == leader_before + 1 && callbacks == 0 &&
          cluster.Node(10).PendingProposals() == 64 && cluster.Node(10).PendingBytes() == bytes,
          "batch not persisted once or admitted before quorum");
    cluster.Pump(); cluster.Settle();
    Check(callbacks == 64 && follower_log->successful_writes == follower_before + 1 &&
          cluster.Node(10).ApplyBatches() == applies_before + 1 &&
          cluster.Node(10).PendingProposals() == 0 && cluster.Node(10).PendingBytes() == 0,
          "batch replication/application did not amortize writes or release accounting");
    Check(Metric(cluster.Node(10), "leader_log_write_count") == 2 &&
          Metric(cluster.Node(10), "leader_log_write_entries") == 65 &&
          Metric(cluster.Node(10), "leader_log_write_bytes") == log_bytes_before + bytes &&
          Metric(cluster.Node(30), "follower_log_write_count") == 2 &&
          Metric(cluster.Node(30), "follower_log_write_entries") == 65 &&
          Metric(cluster.Node(30), "follower_log_write_bytes") == bytes &&
          Metric(cluster.Node(10), "kv_apply_count") == applies_before + 1 &&
          Metric(cluster.Node(10), "kv_apply_entries") == 65 &&
          Metric(cluster.Node(10), "kv_apply_bytes") == bytes &&
          Metric(cluster.Node(10), "replication_data_ack_count") == acks_before + 2 &&
          Metric(cluster.Node(10), "apply_dispatch_count") == applies_before + 1 &&
          Metric(cluster.Node(10), "apply_dispatch_total_us") == 0,
          "stage metrics counted heartbeats, lost batch sizes, or timed synchronous dispatch");
    // Callback-free requests must also consume the entry budget during partition.
    for (int group = 0; group < 8; ++group) {
        std::vector<RaftNode::Proposal> pending(128, {Command({"SET", "default:p", "v"}), {}});
        Check(cluster.Node(10).ProposeBatch(std::move(pending)) > 0, "premature entry rejection");
    }
    const auto full_writes = leader_log->successful_writes;
    Check(cluster.Node(10).PendingProposals() == 1024 &&
          cluster.Node(10).Propose(Command({"SET", "default:p", "v"}), {}) == -2 &&
          leader_log->successful_writes == full_writes, "full queue persisted rejected proposal");
    cluster.Node(10).Stop();
    Check(cluster.Node(10).PendingBytes() == 0 && cluster.Node(10).PendingProposals() == 0,
          "stop did not release admission accounting");
    Cluster byte_limit;
    byte_limit.Elect(10);
    const auto large = Command({"SET", "default:large", std::string(1000000, 'x')});
    for (int i = 0; i < 16; ++i)
        Check(byte_limit.Node(10).Propose(large, {}) > 0, "premature byte rejection");
    Check(byte_limit.Node(10).Propose(large, {}) == -2 &&
          byte_limit.Node(10).PendingProposals() == 16,
          "pending byte budget did not reject a large proposal");
}

static void StorageFailures() {
    Cluster cluster;
    auto log = rocksdb::testing::StateFor(cluster.Path(10, "/log"));
    raftcore::RequestVote vote;
    vote.set_term(1); vote.set_candidate_id(30);
    log->fail_writes = 1;
    Throws([&] { cluster.Node(10).HandleRequestVote(30, vote); });
    Check(cluster.messages.empty() && cluster.Node(10).GetCurrentTerm() == 0,
          "hard-state failure granted vote or advanced volatile term");
    cluster.Restart(10);
    auto request = Append(1, 1, 0, 0, 0);
    cluster.Node(10).HandleAppendEntries(30, request); LastResponse(cluster);
    *request.add_entries() = Entry(1, 1, Command({"SET", "default:k", "v"}));
    log->fail_writes = 1;
    Throws([&] { cluster.Node(10).HandleAppendEntries(30, request); });
    Check(cluster.messages.empty(), "failed append acknowledged");
    Check(Metric(cluster.Node(10), "follower_log_write_count") == 0,
          "failed follower append counted as successful write");
    cluster.Restart(10);
    auto kv = rocksdb::testing::StateFor(cluster.Path(10, "/kv"));
    request.set_leader_commit(1); kv->fail_writes = 1;
    Throws([&] { cluster.Node(10).HandleAppendEntries(30, request); });
    Check(cluster.messages.empty() && cluster.State(10).LastApplied() == 0 && kv->data.empty(),
          "failed state application advanced marker or acknowledged");
    Check(Metric(cluster.Node(10), "kv_apply_count") == 0 &&
          Metric(cluster.Node(10), "apply_dispatch_count") == 0,
          "failed synchronous apply counted as success");
    cluster.Restart(10); cluster.Node(10).HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success() && cluster.State(10).LastApplied() == 1,
          "restart could not apply durable but previously unapplied log");
    kv->fail_reads = 1;
    std::string value;
    Throws([&] { cluster.State(10).Get("default:k", &value); });
    Check(log->non_sync_writes == 0 && kv->non_sync_writes == 0, "durable writes omitted sync option");
}

static void AsyncDelayedAndOrderedApply() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10);
    auto& leader = cluster.Node(10);
    Check(leader.AsyncApplyEnabled() && leader.ApplyInFlight() && executor.busy &&
          leader.GetCommitIndex() == 1 && leader.GetLastApplied() == 0,
          "committed leader noop was not deferred to executor");
    Check(Metric(leader, "kv_apply_count") == 0 && Metric(leader, "apply_dispatch_count") == 0,
          "queued apply published completion metrics");
    executor.Finish();
    const int initial_submissions = executor.submissions;
    int callbacks = 0;
    const auto propose_pair = [&](int first) {
        std::vector<RaftNode::Proposal> proposals;
        for (int i = first; i < first + 2; ++i) {
            proposals.push_back({Command({"SET", "default:async", std::to_string(i)}),
                [&, i](bool ok, const std::string& result) {
                    Check(ok && result == "+OK\r\n" && callbacks == i,
                          "async callback order or result");
                    ++callbacks;
                }});
        }
        Check(leader.ProposeBatch(std::move(proposals)) > 0, "async batch rejected");
        cluster.Pump(); cluster.Settle();
    };
    propose_pair(0);
    Check(callbacks == 0 && leader.GetCommitIndex() == 3 && leader.GetLastApplied() == 1 &&
          leader.PendingProposals() == 2 && leader.PendingBytes() > 0,
          "async client succeeded or released admission before apply completion");
    executor.RunWork();
    Check(cluster.State(10).LastApplied() == 3 && leader.GetLastApplied() == 1 && callbacks == 0,
          "worker persistence ran owner-thread completion inline");
    Check(Metric(leader, "kv_apply_count") == 1 && Metric(leader, "kv_apply_entries") == 1 &&
          Metric(leader, "kv_apply_bytes") == 0 && Metric(leader, "apply_dispatch_count") == 1,
          "worker published apply metrics before owner completion");
    propose_pair(2);
    Check(leader.GetCommitIndex() == 5 && leader.GetLastApplied() == 1 &&
          leader.PendingProposals() == 4 && executor.submissions == initial_submissions + 1,
          "commit advance scheduled overlapping apply work");
    // Settle keeps delivering heartbeats while completion remains withheld.
    Check(leader.IsLeader() && cluster.Node(30).GetCurrentTerm() == leader.GetCurrentTerm(),
          "delayed application interrupted leader heartbeats");
    executor.Complete();
    Check(callbacks == 2 && leader.GetLastApplied() == 3 && leader.PendingProposals() == 2 &&
          executor.busy && executor.submissions == initial_submissions + 2,
          "first completion did not release exactly its prefix and schedule backlog");
    Check(Metric(leader, "kv_apply_count") == 2 && Metric(leader, "kv_apply_entries") == 3 &&
          Metric(leader, "apply_dispatch_count") == 2,
          "owner completion did not publish its successful apply metrics");
    executor.Finish();
    std::string value;
    Check(callbacks == 4 && leader.GetLastApplied() == 5 && !leader.ApplyInFlight() &&
          leader.PendingProposals() == 0 && leader.PendingBytes() == 0 &&
          cluster.State(10).Get("default:async", &value) && value == "3",
          "ordered async backlog did not fully apply and release admission");
    Check(Metric(leader, "kv_apply_count") == 3 && Metric(leader, "kv_apply_entries") == 5 &&
          Metric(leader, "apply_dispatch_count") == 3,
          "async backlog metrics did not count completed batches");
}

static void AsyncStopAndResume() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10); executor.Finish();
    auto& leader = cluster.Node(10);
    int successes = 0, failures = 0;
    const auto callback = [&](bool ok, const std::string&) { ok ? ++successes : ++failures; };
    leader.Propose(Command({"SET", "default:stopped", "first"}), callback);
    cluster.Pump(); cluster.Settle();
    leader.Propose(Command({"SET", "default:stopped", "second"}), callback);
    cluster.Pump(); cluster.Settle();
    Check(leader.GetCommitIndex() == 3 && executor.busy, "stop fixture has no committed backlog");
    const auto submitted = executor.submissions;
    leader.Stop(); leader.Stop();
    Check(failures == 2 && successes == 0 && leader.ApplyInFlight() &&
          leader.PendingProposals() == 0 && leader.PendingBytes() == 0,
          "stop duplicated replies, lost in-flight state, or retained admission");
    executor.Finish();
    Check(leader.GetLastApplied() == 2 && cluster.State(10).LastApplied() == 2 &&
          leader.GetCommitIndex() == 3 && !executor.busy &&
          executor.submissions == submitted && successes == 0 && failures == 2,
          "stopped completion lost durable progress or scheduled more backlog");
    leader.Start();
    Check(executor.busy && leader.ApplyInFlight() && executor.submissions == submitted + 1,
          "restart did not resume committed unapplied backlog");
    leader.Stop(); leader.Start();
    Check(executor.submissions == submitted + 1 && leader.ApplyInFlight(),
          "stop/start duplicated an outstanding apply batch");
    executor.Finish();
    std::string value;
    Check(leader.GetLastApplied() == 3 && !leader.ApplyInFlight() &&
          successes == 0 && failures == 2 &&
          cluster.State(10).Get("default:stopped", &value) && value == "second",
          "resumed apply reordered state or replied again to failed clients");
}

static void AsyncTermChangeAndDestruction() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10); executor.Finish();
    auto& leader = cluster.Node(10);
    int successes = 0, failures = 0;
    const int old_term = leader.GetCurrentTerm();
    leader.Propose(Command({"SET", "default:term", "committed"}),
        [&](bool ok, const std::string&) { ok ? ++successes : ++failures; });
    cluster.Pump(); cluster.Settle();
    raftcore::RequestVoteResponse higher_term;
    higher_term.set_term(old_term + 1);
    leader.HandleRequestVoteResponse(30, higher_term);
    Check(!leader.IsLeader() && failures == 1 && successes == 0 && leader.ApplyInFlight(),
          "term change did not fail client while preserving committed apply");
    executor.Finish();
    Check(leader.GetLastApplied() == 2 && successes == 0 && failures == 1,
          "late old-term apply duplicated a client reply");
    cluster.messages.clear();
    auto request = Append(old_term + 1, 999, 2, old_term, 3);
    *request.add_entries() = Entry(3, old_term + 1, Command({"SET", "default:term", "after"}));
    leader.HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success() && executor.busy,
          "follower did not accept next committed async batch");
    // Keep KVStateMachine alive while deleting only RaftNode. Pending executor
    // closures must not dereference the destroyed node on late completion.
    cluster.members.at(10)->raft.reset();
    executor.Finish();
    std::string value;
    Check(cluster.State(10).LastApplied() == 3 &&
          cluster.State(10).Get("default:term", &value) && value == "after" &&
          cluster.messages.empty() && successes == 0 && failures == 1,
          "completion after node destruction touched transport or client state");
}

static void AsyncStorageFailure() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10); executor.Finish();
    auto& leader = cluster.Node(10);
    int successes = 0;
    leader.Propose(Command({"SET", "default:failed", "v"}),
        [&](bool ok, const std::string&) { if (ok) ++successes; });
    cluster.Pump(); cluster.Settle();
    auto kv = rocksdb::testing::StateFor(cluster.Path(10, "/kv"));
    const auto before = kv->data;
    const auto applies_before = Metric(leader, "kv_apply_count");
    kv->fail_writes = 1;
    executor.RunWork();
    Check(successes == 0 && leader.GetLastApplied() == 1 && cluster.State(10).LastApplied() == 1,
          "failed worker advanced apply state or succeeded client");
    Throws([&] { executor.Complete(); });
    Check(successes == 0 && leader.GetLastApplied() == 1 && cluster.State(10).LastApplied() == 1 &&
          kv->data == before,
          "owner-thread failure completion advanced marker, mutated KV, or acknowledged client");
    Check(Metric(leader, "kv_apply_count") == applies_before &&
          Metric(leader, "apply_dispatch_count") == applies_before,
          "failed async apply counted as successful completion");
}

static void MetricArithmeticAndLeaderFailure() {
    BatchStats batch;
    Check(batch.ToInfo("batch") == "batch_count:0\r\nbatch_total_us:0\r\nbatch_max_us:0\r\n"
          "batch_avg_us:0\r\nbatch_entries:0\r\nbatch_bytes:0\r\nbatch_avg_entries:0\r\n",
          "empty metrics formatting or division failed");
    batch.Observe(3, 2, 10); batch.Observe(8, 5, 21);
    Check(batch.ToInfo("batch") == "batch_count:2\r\nbatch_total_us:11\r\nbatch_max_us:8\r\n"
          "batch_avg_us:5\r\nbatch_entries:7\r\nbatch_bytes:31\r\nbatch_avg_entries:3\r\n",
          "metric counts, maxima or integer averages are incorrect");
    const SteadyClock::time_point start{};
    const auto end = start + std::chrono::microseconds(25);
    Check(ElapsedMicros(start, end) == 25 && ElapsedMicros(end, start) == 0 &&
          ElapsedMicros(start, start) == 0, "elapsed duration is not nonnegative microseconds");
    Cluster cluster;
    cluster.Elect(10);
    const auto before = cluster.Node(10).MetricsInfo();
    rocksdb::testing::StateFor(cluster.Path(10, "/log"))->fail_writes = 1;
    Throws([&] { cluster.Node(10).Propose(Command({"SET", "default:failure", "v"}), {}); });
    Check(cluster.Node(10).MetricsInfo() == before, "failed leader append changed success metrics");
}

static void AsyncFollowerCommitGuard() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    auto& follower = cluster.Node(10);
    auto request = Append(1, 1, 0, 0, 1);
    *request.add_entries() = Entry(1, 1, Command({"SET", "default:guard", "original"}));
    follower.HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success() && follower.GetCommitIndex() == 1 &&
          follower.GetLastApplied() == 0 && executor.busy,
          "async follower did not acknowledge durable log independently of KV apply");
    request = Append(2, 2, 0, 0, 1);
    *request.add_entries() = Entry(1, 2, Command({"SET", "default:guard", "replacement"}));
    Throws([&] { follower.HandleAppendEntries(30, request); });
    Check(cluster.messages.empty() && follower.GetCommitIndex() == 1,
          "unapplied committed prefix was replaced or acknowledged");
    executor.Finish();
    std::string value;
    Check(follower.GetLastApplied() == 1 && cluster.State(10).Get("default:guard", &value) &&
          value == "original", "committed guard did not preserve pending apply content");
}

int main() {
    try {
        const std::pair<const char*, void(*)()> tests[] = {
            {"quorum, duplicate votes and pending failure", QuorumAndDuplicateVotes},
            {"replication, restart, failover and catch-up", ReplicationAndRecovery},
            {"prefix conflicts and commit bounds", PrefixConflictAndCommitBound},
            {"lost responses and stale RPC correlation", LostResponseAndStaleRpc},
            {"storage failures and application recovery", StorageFailures},
            {"batched replication and admission limits", BatchedReplicationAndLimits},
            {"async delayed replies, heartbeats and ordered batches", AsyncDelayedAndOrderedApply},
            {"async stop/start and late completion", AsyncStopAndResume},
            {"async term change and destroyed node completion", AsyncTermChangeAndDestruction},
            {"async owner-thread storage failure", AsyncStorageFailure},
            {"async follower durable ack and committed conflict guard", AsyncFollowerCommitGuard},
            {"metric arithmetic and failed leader write", MetricArithmeticAndLeaderFailure},
        };
        for (const auto& test : tests) { test.second(); std::cout << "PASS: " << test.first << '\n'; }
        std::cout << "PASS: " << sizeof(tests) / sizeof(tests[0])
                  << " core regression scenarios (in-process doubles)\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << "FAIL: " << error.what() << '\n'; return 1; }
}
