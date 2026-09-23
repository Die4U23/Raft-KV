// Production RaftNode admission, voting, and health paths not covered by the
// existing scenario files. Homemade election/quota models are not CTest.
#include "in_process_cluster.h"
#include "common/resp_parser.h"
#include <iostream>
#include <vector>

static raftcore::RequestVote Vote(int term, int candidate, int64_t last_index, int64_t last_term) {
    raftcore::RequestVote request;
    request.set_term(term);
    request.set_candidate_id(candidate);
    request.set_last_log_index(last_index);
    request.set_last_log_term(last_term);
    return request;
}

static void ProposeRejectsFollowerStoppedUnhealthyAndOversize() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    int follower_cb = 0;
    Check(cluster.Node(30).Propose(Command({"SET", "default:k", "v"}),
        [&](bool, const std::string&) { ++follower_cb; }) == -1,
          "follower Propose did not return -1");
    Check(follower_cb == 0, "rejected follower Propose invoked a callback");

    cluster.Node(10).Stop();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) == -1,
          "stopped leader Propose did not return -1");
    cluster.Node(10).Start();
    cluster.Elect(10);
    cluster.Settle();

    Check(cluster.Node(10).ProposeBatch({}) == -2, "empty batch was admitted");
    std::vector<RaftNode::Proposal> oversize(129, {Command({"SET", "default:k", "v"}), {}});
    Check(cluster.Node(10).ProposeBatch(std::move(oversize)) == -2,
          "129-entry batch exceeded kMaxBatchEntries but was admitted");
    std::vector<RaftNode::Proposal> allowed(128, {Command({"SET", "default:k", "v"}), {}});
    Check(cluster.Node(10).ProposeBatch(std::move(allowed)) > 0, "128-entry batch rejected");
    cluster.Pump();
    cluster.Settle();

    const std::string huge(RespParser::kMaxCommandBytes + 1, 'x');
    Check(cluster.Node(10).Propose(huge, {}) == -2, "oversized command was admitted");

    const auto before = cluster.Node(10).PendingProposals();
    rocksdb::testing::StateFor(cluster.Path(10, "/log"))->fail_writes = 1;
    Throws([&] { cluster.Node(10).Propose(Command({"SET", "default:fail", "v"}), {}); });
    Check(!cluster.Node(10).IsHealthy() &&
          cluster.Node(10).Propose(Command({"SET", "default:fail", "v"}), {}) == -3 &&
          cluster.Node(10).PendingProposals() == 0 && before == 0,
          "unhealthy leader did not return -3 or retained pending entries");
}

static void ReadIndexBeforeNoopAndOnCandidate() {
    Cluster cluster;
    cluster.Candidate(10);
    bool candidate_called = false;
    Check(!cluster.Node(10).IsLeader(), "candidate became leader before a quorum");
    Check(!cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) {
            candidate_called = true;
            Check(!success, "candidate ReadIndex reported success");
        }), "candidate ReadIndex was accepted");
    Check(candidate_called, "candidate ReadIndex skipped the failure callback");
    Check(Metric(cluster.Node(10), "read_index_not_leader") >= 1,
          "candidate reject was not counted");

    raftcore::RequestVoteResponse vote;
    vote.set_term(cluster.Node(10).GetCurrentTerm());
    vote.set_vote_granted(true);
    cluster.messages.clear();
    cluster.Node(10).HandleRequestVoteResponse(30, vote);
    Check(cluster.Node(10).IsLeader(), "one additional vote did not elect the leader");
    bool waiting_called = false;
    std::string error;
    Check(!cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string& err) {
            waiting_called = true;
            Check(!success, "pre-noop ReadIndex reported success");
            error = err;
        }), "ReadIndex accepted before this term's no-op committed");
    Check(waiting_called && error.find("no-op") != std::string::npos,
          "pre-noop ReadIndex used an unexpected error");
}

static void VotesFollowLogUpToDateAndOneVotePerTerm() {
    Cluster cluster({10, 30, 50, 70, 90});
    cluster.Elect(10);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "seed write");
    cluster.Pump();
    cluster.Settle();
    const int term = cluster.Node(30).GetCurrentTerm();
    const int64_t last = cluster.Node(30).GetCommitIndex();
    Check(last >= 2, "follower did not commit the seed write");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(50, Vote(term + 1, 50, last, term));
    auto live = LastVoteResponse(cluster);
    Check(!live.vote_granted() && live.term() == term,
          "follower with a live heartbeat granted a vote or bumped term");

    cluster.Restart(30);
    Check(cluster.Node(30).GetLeaderId() == -1, "restarted follower still has a leader");
    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(50, Vote(term + 1, 50, 0, 0));
    auto reply = LastVoteResponse(cluster);
    Check(!reply.vote_granted() && reply.term() == term + 1,
          "stale last_log_term won a vote");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(50, Vote(term + 1, 50, last - 1, term));
    reply = LastVoteResponse(cluster);
    Check(!reply.vote_granted(), "shorter same-term log won a vote");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(50, Vote(term + 1, 50, last, term));
    reply = LastVoteResponse(cluster);
    Check(reply.vote_granted() && reply.term() == term + 1,
          "up-to-date candidate was denied");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(70, Vote(term + 1, 70, last, term));
    reply = LastVoteResponse(cluster);
    Check(!reply.vote_granted(), "second candidate in the same term received a vote");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(70, Vote(term + 2, 70, 1, term + 1));
    reply = LastVoteResponse(cluster);
    Check(reply.vote_granted(), "higher last_log_term was denied");
}

static void InvalidVoteRequestsAreIgnored() {
    Cluster cluster;
    cluster.messages.clear();
    cluster.Node(10).HandleRequestVote(30, Vote(0, 30, 0, 0));
    cluster.Node(10).HandleRequestVote(30, Vote(1, 99, 0, 0));
    cluster.Node(10).HandleRequestVote(30, Vote(1, 30, 0, 2));
    cluster.Node(10).HandleRequestVote(30, Vote(1, 30, -1, 0));
    Check(cluster.messages.empty() && cluster.Node(10).GetCurrentTerm() == 0,
          "invalid RequestVote produced a reply or advanced term");
}

static void PreVoteDoesNotRaiseTermWithoutAQuorum() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(50);
    const int term = cluster.Node(50).GetCurrentTerm();
    cluster.messages.clear();
    for (int i = 0; i < 8; ++i) {
        cluster.Advance(50, RaftNode::kMaxElectionTimeoutMs);
        cluster.Node(50).Tick();
    }
    Check(cluster.Node(50).GetCurrentTerm() == term,
          "partitioned pre-vote raised the term");
    Check(std::string(cluster.Node(50).StateName()) == "pre-candidate",
          "partitioned node left pre-vote");
    int prevotes = 0;
    for (const auto& message : cluster.messages) {
        if (message.type != RaftMsgType::kRequestVote || message.from != 50) continue;
        raftcore::RequestVote rpc;
        Check(rpc.ParseFromString(message.payload), "pre-vote decode");
        Check(rpc.prevote() && rpc.term() == term + 1,
              "partitioned campaign sent a real vote or the wrong term");
        ++prevotes;
    }
    Check(prevotes > 0, "partitioned node sent no pre-votes");
    Check(cluster.Node(10).IsLeader() && cluster.Node(10).GetCurrentTerm() == term,
          "leader term changed while a peer was only pre-voting");
}

static void PartitionedPreVoteRejoinsWithoutDisturbingLeader() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    const int term = cluster.Node(10).GetCurrentTerm();
    cluster.Partition(50);
    cluster.messages.clear();
    for (int i = 0; i < 8; ++i) {
        cluster.Advance(50, RaftNode::kMaxElectionTimeoutMs);
        cluster.Node(50).Tick();
    }
    Check(cluster.Node(50).GetCurrentTerm() == term, "isolated pre-vote raised the term");
    cluster.Heal(50);
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).IsLeader() && cluster.Node(10).GetCurrentTerm() == term,
          "rejoining pre-votes disturbed the leader");
    Check(std::string(cluster.Node(50).StateName()) == "follower" &&
          cluster.Node(50).GetLeaderId() == 10 &&
          cluster.Node(50).GetCurrentTerm() == term,
          "rejoining node did not return to the same leader");
    bool ok = false;
    Check(cluster.Node(10).Propose(Command({"SET", "default:rejoin", "v"}),
        [&](bool success, const std::string&) { ok = success; }) > 0,
          "write after rejoin rejected");
    cluster.Pump();
    cluster.Settle();
    Check(ok, "write after rejoin was not committed");
    for (int id : {10, 30, 50}) {
        std::string value;
        Check(cluster.State(id).Get("default:rejoin", &value) && value == "v",
              "replica missed the write after pre-vote rejoin");
    }
}

// A node that missed a term must still be able to complete pre-vote once the
// leader is gone. Replying with the receiver's own term lets it learn the
// higher term from a rejection instead of ignoring the round.
static void LaggingNodeCompletesPreVoteElection() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    const int old_term = cluster.Node(50).GetCurrentTerm();
    cluster.Partition(50);
    cluster.messages.clear();
    cluster.Advance(30, RaftNode::kMaxElectionTimeoutMs);
    cluster.Advance(10, RaftNode::kMinElectionTimeoutMs);
    cluster.Node(10).Tick();
    Check(!cluster.Node(10).IsLeader(), "leader with a stale quorum did not step down");
    cluster.Advance(10, RaftNode::kMaxElectionTimeoutMs);
    cluster.Node(10).Tick();
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).IsLeader() && cluster.Node(10).GetCurrentTerm() == old_term + 1,
          "majority did not elect exactly one new term");
    Check(cluster.Node(50).GetCurrentTerm() == old_term,
          "partitioned node adopted the new term");

    cluster.members.erase(10);
    cluster.messages.clear();
    cluster.Heal(50);
    const int elected = cluster.ElectAmong({30, 50});
    Check(cluster.Node(30).GetCurrentTerm() == cluster.Node(50).GetCurrentTerm() &&
          cluster.Node(elected).IsLeader(),
          "lagging node could not finish a pre-vote election");
    bool ok = false;
    Check(cluster.Node(elected).Propose(Command({"SET", "default:lag", "v"}),
        [&](bool success, const std::string&) { ok = success; }) > 0,
          "write after lagging election rejected");
    cluster.Pump();
    cluster.Settle();
    Check(ok, "lagging majority could not commit");
    for (int id : {30, 50}) {
        std::string value;
        Check(cluster.State(id).Get("default:lag", &value) && value == "v",
              "replica missing the value after a lagging pre-vote election");
    }
}

static void PreVoteGrantDoesNotPersistVote() {
    Cluster cluster;
    cluster.messages.clear();
    auto first_request = Vote(1, 50, 0, 0);
    first_request.set_prevote(true);
    cluster.Node(30).HandleRequestVote(50, first_request);
    auto first = LastVoteResponse(cluster);
    Check(first.prevote() && first.vote_granted() && first.term() == 0 &&
          cluster.Node(30).GetCurrentTerm() == 0,
          "fresh follower refused a pre-vote or bumped its term");

    cluster.messages.clear();
    auto second_request = Vote(1, 10, 0, 0);
    second_request.set_prevote(true);
    cluster.Node(30).HandleRequestVote(10, second_request);
    auto second = LastVoteResponse(cluster);
    Check(second.prevote() && second.vote_granted() && cluster.Node(30).GetCurrentTerm() == 0,
          "second pre-vote was refused as if votedFor had been persisted");

    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(50, Vote(1, 50, 0, 0));
    auto real = LastVoteResponse(cluster);
    Check(!real.prevote() && real.vote_granted() && real.term() == 1,
          "real vote after pre-votes was refused");
    cluster.messages.clear();
    cluster.Node(30).HandleRequestVote(10, Vote(1, 10, 0, 0));
    auto again = LastVoteResponse(cluster);
    Check(!again.vote_granted() && cluster.Node(30).GetCurrentTerm() == 1,
          "second real vote in that term was granted");
}

static void LeaderRejectsPreVote() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    const int term = cluster.Node(10).GetCurrentTerm();
    cluster.messages.clear();
    auto request = Vote(term + 1, 30, cluster.Node(10).GetCommitIndex(), term);
    request.set_prevote(true);
    cluster.Node(10).HandleRequestVote(30, request);
    auto reply = LastVoteResponse(cluster);
    Check(reply.prevote() && !reply.vote_granted() && reply.term() == term,
          "leader granted a pre-vote or answered with another term");
    Check(cluster.Node(10).IsLeader() && cluster.Node(10).GetCurrentTerm() == term,
          "leader stepped down or raised its term for a pre-vote");
}

static void CandidateTimeoutReturnsToPreVote() {
    Cluster cluster;
    cluster.Candidate(10);
    const int term = cluster.Node(10).GetCurrentTerm();
    Check(std::string(cluster.Node(10).StateName()) == "candidate" && term > 0,
          "pre-vote quorum did not start one real term");
    cluster.messages.clear();
    cluster.Advance(10, RaftNode::kMaxElectionTimeoutMs);
    cluster.Node(10).Tick();
    Check(cluster.Node(10).GetCurrentTerm() == term,
          "candidate timeout raised the term again");
    Check(std::string(cluster.Node(10).StateName()) == "pre-candidate",
          "candidate timeout did not return to pre-vote");
    int prevotes = 0;
    for (const auto& message : cluster.messages) {
        if (message.type != RaftMsgType::kRequestVote || message.from != 10) continue;
        raftcore::RequestVote rpc;
        Check(rpc.ParseFromString(message.payload) && rpc.prevote() && rpc.term() == term + 1,
              "retry after a lost election was not a pre-vote for the next term");
        ++prevotes;
    }
    Check(prevotes > 0, "candidate timeout sent no pre-votes");
}

static void HigherTermPreVoteResponseStepsDown() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(50);
    const int term = cluster.Node(50).GetCurrentTerm();
    cluster.Campaign(50);
    Check(std::string(cluster.Node(50).StateName()) == "pre-candidate" &&
          cluster.Node(50).GetCurrentTerm() == term,
          "isolated campaign raised the term");
    raftcore::RequestVoteResponse response;
    response.set_term(term + 3);
    response.set_vote_granted(true);
    response.set_prevote(true);
    cluster.Node(50).HandleRequestVoteResponse(30, response);
    Check(std::string(cluster.Node(50).StateName()) == "follower" &&
          cluster.Node(50).GetCurrentTerm() == term + 3,
          "higher-term pre-vote response did not step down");
}

static void SameTermAppendStepsDownPreCandidate() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(50);
    const int term = cluster.Node(50).GetCurrentTerm();
    cluster.Advance(50, RaftNode::kMaxElectionTimeoutMs);
    cluster.Node(50).Tick();
    Check(std::string(cluster.Node(50).StateName()) == "pre-candidate" &&
          cluster.Node(50).GetCurrentTerm() == term,
          "timed-out follower did not enter pre-vote");
    cluster.messages.clear();
    auto heartbeat = Append(term, 1, 0, 0, 0);
    heartbeat.set_leader_id(10);
    cluster.Node(50).HandleAppendEntries(10, heartbeat);
    Check(std::string(cluster.Node(50).StateName()) == "follower" &&
          cluster.Node(50).GetCurrentTerm() == term &&
          cluster.Node(50).GetLeaderId() == 10,
          "current leader heartbeat did not end pre-vote");
}

static std::string IndexKey(int64_t index) {
    std::string key(8, '\0');
    auto value = static_cast<uint64_t>(index);
    for (int i = 7; i >= 0; --i) {
        key[static_cast<size_t>(i)] = static_cast<char>(value & 0xff);
        value >>= 8;
    }
    return key;
}
static std::string EmptySnapshot() { return std::string("\x01\x00\x00\x00\x00", 5); }
static raftcore::InstallSnapshotResponse LastSnapshotResponse(Cluster& cluster) {
    Check(!cluster.messages.empty(), "missing snapshot response");
    raftcore::InstallSnapshotResponse response;
    Check(cluster.messages.back().type == RaftMsgType::kInstallSnapshotResponse &&
          response.ParseFromString(cluster.messages.back().payload),
          "invalid snapshot response");
    cluster.messages.clear();
    return response;
}

static void SnapshotCatchesUpLaggingFollower() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(50);
    cluster.Node(10).SetSnapshotDistanceForTest(1);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0,
          "leader rejected the compacted write");
    cluster.Pump();
    cluster.Settle();
    const int64_t compacted = cluster.Node(10).GetSnapshotIndex();
    Check(compacted >= 2 && compacted == cluster.Node(10).GetCommitIndex(),
          "leader did not compact through the applied index");
    const auto& log = rocksdb::testing::StateFor(cluster.Path(10, "/log"))->data;
    for (int64_t index = 1; index <= compacted; ++index)
        Check(log.count(IndexKey(index)) == 0, "compacted log index is still stored");
    Check(log.count(IndexKey(0)) == 1 && log.count(std::string("\x01snapmeta", 9)) == 1 &&
          log.count(std::string("\x01snapdata", 9)) == 1,
          "compaction removed hard state or skipped the snapshot keys");
    std::string value;
    Check(cluster.State(30).Get("default:k", &value) && value == "v" &&
          cluster.Node(30).GetSnapshotIndex() == 0,
          "in-sync follower did not apply the write through AppendEntries");
    Check(!cluster.State(50).Get("default:k", &value) && cluster.Node(50).GetSnapshotIndex() == 0,
          "partitioned follower already had the compacted write");

    // Restart drops the AppendEntries that still carried the now-deleted prefix.
    // The next catch-up has to install the snapshot, then replicate the suffix.
    cluster.Restart(10);
    cluster.Elect(10);
    Check(cluster.Node(10).GetSnapshotIndex() == compacted &&
          cluster.Node(10).GetCommitIndex() > compacted,
          "restarted leader lost its snapshot or did not commit a suffix entry");
    cluster.Heal(50);
    cluster.Settle();
    Check(cluster.Node(50).GetSnapshotIndex() == compacted &&
          cluster.State(50).Get("default:k", &value) && value == "v",
          "lagging follower did not install the leader snapshot");
    Check(cluster.Node(10).Propose(Command({"SET", "default:more", "x"}), {}) > 0,
          "leader rejected a write after compaction");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).GetSnapshotIndex() == compacted &&
          cluster.Node(10).GetCommitIndex() > compacted, "suffix write compacted the log again");
    for (int id : {10, 30, 50})
        Check(cluster.State(id).Get("default:more", &value) && value == "x",
              "suffix entry after the snapshot did not reach every replica");

    rocksdb::testing::StateFor(cluster.Path(10, "/kv"))->data.clear();
    cluster.Restart(10);
    Check(cluster.Node(10).GetSnapshotIndex() == compacted &&
          cluster.State(10).LastApplied() == compacted &&
          cluster.State(10).Get("default:k", &value) && value == "v",
          "restart did not rebuild KV from the log snapshot");
    Check(cluster.Node(10).MetricsInfo().find("snapshot_index:" + std::to_string(compacted)) !=
          std::string::npos, "INFO omitted snapshot_index");
}

static void MatchingSnapshotDoesNotRewindAppliedState() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "seed write");
    cluster.Pump();
    cluster.Settle();
    const int term = cluster.Node(30).GetCurrentTerm();
    raftcore::InstallSnapshot snapshot;
    snapshot.set_term(term);
    snapshot.set_leader_id(10);
    snapshot.set_last_included_index(1);
    snapshot.set_last_included_term(term);
    snapshot.set_rpc_id(7);
    snapshot.set_data(EmptySnapshot());
    cluster.messages.clear();
    cluster.Node(30).HandleInstallSnapshot(10, snapshot);
    auto applied = LastSnapshotResponse(cluster);
    std::string value;
    Check(applied.success() && applied.term() == term &&
          cluster.Node(30).GetSnapshotIndex() == 1 &&
          cluster.Node(30).GetCurrentTerm() == term &&
          cluster.State(30).Get("default:k", &value) && value == "v" &&
          cluster.State(30).LastApplied() >= 2,
          "a term-matched older snapshot rewound applied keys or raised the term");

    snapshot.set_rpc_id(8);
    snapshot.set_data("not-a-snapshot");
    snapshot.set_last_included_index(cluster.Node(30).GetCommitIndex());
    snapshot.set_last_included_term(term);
    cluster.Node(30).HandleInstallSnapshot(10, snapshot);
    auto rejected = LastSnapshotResponse(cluster);
    Check(!rejected.success() && rejected.term() == term &&
          cluster.Node(30).GetCurrentTerm() == term &&
          cluster.Node(30).GetSnapshotIndex() == 1 &&
          cluster.State(30).Get("default:k", &value) && value == "v",
          "malformed snapshot was stored or changed the term");
}

static void SnapshotConflictWithAppliedLogStops() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "seed write");
    cluster.Pump();
    cluster.Settle();
    cluster.Partition(10);
    const int leader = cluster.ElectAmong({30, 50});
    // ElectAmong may choose either survivor. A snapshot from a node to itself
    // is ignored, so the applied-log conflict has to be delivered to the other one.
    const int follower = leader == 50 ? 30 : 50;
    const int term = cluster.Node(follower).GetCurrentTerm();
    Check(term > 1 && cluster.State(follower).LastApplied() > 1,
          "follower has no applied prefix");
    raftcore::InstallSnapshot snapshot;
    snapshot.set_term(term);
    snapshot.set_leader_id(leader);
    snapshot.set_last_included_index(1);
    snapshot.set_last_included_term(term);
    snapshot.set_rpc_id(9);
    snapshot.set_data(EmptySnapshot());
    Throws([&] { cluster.Node(follower).HandleInstallSnapshot(leader, snapshot); });
    std::string value;
    Check(cluster.Node(follower).GetSnapshotIndex() == 0 &&
          cluster.State(follower).Get("default:k", &value) && value == "v",
          "conflicting snapshot changed the applied prefix");
}

static void HigherTermAppendStepsDownCandidate() {
    Cluster cluster;
    cluster.Candidate(10);
    const int old_term = cluster.Node(10).GetCurrentTerm();
    Check(std::string(cluster.Node(10).StateName()) == "candidate", "did not start as candidate");
    auto heartbeat = Append(old_term + 1, 1, 0, 0, 0);
    heartbeat.set_leader_id(30);
    cluster.messages.clear();
    cluster.Node(10).HandleAppendEntries(30, heartbeat);
    Check(!cluster.Node(10).IsLeader() &&
          std::string(cluster.Node(10).StateName()) == "follower" &&
          cluster.Node(10).GetCurrentTerm() == old_term + 1 &&
          cluster.Node(10).GetLeaderId() == 30,
          "higher-term AppendEntries did not convert the candidate");
}

int main() {
    try {
        const std::pair<const char*, void(*)()> tests[] = {
            {"propose rejects follower, stop, empty/oversize batch, unhealthy",
             ProposeRejectsFollowerStoppedUnhealthyAndOversize},
            {"ReadIndex rejected on candidate and before no-op commit",
             ReadIndexBeforeNoopAndOnCandidate},
            {"votes require up-to-date logs and one grant per term",
             VotesFollowLogUpToDateAndOneVotePerTerm},
            {"invalid RequestVote is ignored", InvalidVoteRequestsAreIgnored},
            {"partitioned pre-vote does not raise the term",
             PreVoteDoesNotRaiseTermWithoutAQuorum},
            {"rejoining pre-vote leaves the leader in place",
             PartitionedPreVoteRejoinsWithoutDisturbingLeader},
            {"lagging node can finish a pre-vote election",
             LaggingNodeCompletesPreVoteElection},
            {"pre-vote grants are not persisted", PreVoteGrantDoesNotPersistVote},
            {"leader rejects pre-vote", LeaderRejectsPreVote},
            {"lost election retries with pre-vote", CandidateTimeoutReturnsToPreVote},
            {"higher-term pre-vote response steps down", HigherTermPreVoteResponseStepsDown},
            {"current leader heartbeat ends pre-vote", SameTermAppendStepsDownPreCandidate},
            {"snapshot installs on a lagging follower and restores KV",
             SnapshotCatchesUpLaggingFollower},
            {"matching snapshot does not rewind applied keys",
             MatchingSnapshotDoesNotRewindAppliedState},
            {"snapshot term conflict with applied log stops the node",
             SnapshotConflictWithAppliedLogStops},
            {"higher-term AppendEntries steps down a candidate",
             HigherTermAppendStepsDownCandidate},
        };
        for (const auto& test : tests) {
            test.second();
            std::cout << "PASS: " << test.first << '\n';
        }
        std::cout << "PASS: " << sizeof(tests) / sizeof(tests[0])
                  << " RaftNode coverage scenarios\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
