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
