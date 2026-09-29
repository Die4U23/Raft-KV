// Production RaftNode replication ACK rules. Homemade trackers are not CTest.
#include "in_process_cluster.h"
#include <iostream>

static void StaleRpcDoesNotCommit() {
    Cluster cluster;
    cluster.Candidate(10);
    raftcore::RequestVoteResponse vote;
    vote.set_term(cluster.Node(10).GetCurrentTerm());
    vote.set_vote_granted(true);
    cluster.messages.clear();
    cluster.Node(10).HandleRequestVoteResponse(30, vote);
    Message request{};
    for (const auto& message : cluster.messages)
        if (message.to == 30) request = message;
    cluster.messages.clear();
    cluster.Deliver(request);
    auto reply = LastResponse(cluster);
    raftcore::AppendEntriesResponse stale = reply;
    stale.set_rpc_id(reply.rpc_id() + 100);
    cluster.Node(10).HandleAppendEntriesResponse(30, stale);
    Check(cluster.Node(10).GetCommitIndex() == 0, "uncorrelated RPC committed");
    cluster.Node(10).HandleAppendEntriesResponse(30, reply);
    Check(cluster.Node(10).GetCommitIndex() == 1, "matching ACK did not commit no-op");
}

static void OneInflightPerPeer() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:a", "1"}), {}) > 0, "first write");
    Check(cluster.Node(10).HasInflightRpc(30) && cluster.Node(10).HasInflightRpc(50),
          "write did not create per-peer inflight");
    const auto first = TakeAppendsFrom(cluster, 10);
    Check(first.size() >= 2, "missing first-round appends");
    Check(cluster.Node(10).Propose(Command({"SET", "default:b", "2"}), {}) > 0, "second write");
    auto extra = TakeAppendsFrom(cluster, 10);
    Check(extra.empty(), "second write replaced or resent a new rpc_id while inflight");
    Check(cluster.Node(10).HasInflightRpc(30), "inflight cleared before ACK");
}

static void FiveNodeQuorumNeedsTwoFollowers() {
    Cluster cluster({10, 30, 50, 70, 90});
    cluster.Elect(10);
    cluster.Settle();
    const int64_t before = cluster.Node(10).GetCommitIndex();
    cluster.messages.clear();
    bool ok = false;
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool success, const std::string&) { ok = success; }) > 0, "propose");
    auto appends = TakeAppendsFrom(cluster, 10);
    Check(appends.size() >= 4, "leader did not replicate to all followers");
    cluster.Deliver(appends.front());
    auto ack = LastResponse(cluster);
    cluster.Node(10).HandleAppendEntriesResponse(appends.front().to, ack);
    Check(!ok && cluster.Node(10).GetCommitIndex() == before,
          "one follower ACK formed a 5-node majority");
    cluster.Deliver(appends[1]);
    ack = LastResponse(cluster);
    cluster.Node(10).HandleAppendEntriesResponse(appends[1].to, ack);
    Check(ok && cluster.Node(10).GetCommitIndex() > before,
          "two follower ACKs did not commit");
}

static void RefreshQuorum(Cluster& cluster, int leader) {
    raftcore::AppendEntriesResponse ping;
    ping.set_term(cluster.Node(leader).GetCurrentTerm());
    ping.set_success(false);
    ping.set_last_log_index(0);
    cluster.Node(leader).HandleAppendEntriesResponse(30, ping);
}

static void AdvanceLeader(Cluster& cluster, int leader, int ms) {
    for (int left = ms; left > 0; left -= RaftNode::kTickIntervalMs) {
        cluster.Advance(leader, RaftNode::kTickIntervalMs);
        RefreshQuorum(cluster, leader);
        cluster.Node(leader).Tick();
    }
}

static void UncommittedProposalExpiresButStaysInTheLog() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    int callbacks = 0;
    bool ok = true;
    std::string result;
    const int64_t index = cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool success, const std::string& reply) {
            ++callbacks;
            ok = success;
            result = reply;
        });
    Check(index > 0, "propose");
    const int64_t commit = cluster.Node(10).GetCommitIndex();
    AdvanceLeader(cluster, 10, RaftNode::kProposalTimeoutMs - RaftNode::kTickIntervalMs);
    Check(callbacks == 0 && cluster.Node(10).PendingProposals() == 1,
          "proposal expired before the deadline");
    AdvanceLeader(cluster, 10, RaftNode::kTickIntervalMs);
    Check(callbacks == 1 && !ok && cluster.Node(10).IsLeader() &&
          cluster.Node(10).GetCommitIndex() == commit &&
          cluster.Node(10).PendingProposals() == 0 &&
          Metric(cluster.Node(10), "proposal_timeouts") == 1,
          "uncommitted proposal did not fail closed");
    Check(result == "-ERR proposal timeout; outcome unknown\r\n",
          "timeout reply was not an unknown outcome");
    cluster.Pump();
    std::string value;
    Check(cluster.Node(10).GetCommitIndex() >= index &&
          cluster.State(10).Get("default:k", &value) && value == "v" && callbacks == 1,
          "timed-out entry was dropped or the callback ran again");
}

static void CommittedWriteDoesNotExpireWhileApplyLags() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10);
    executor.Finish();
    cluster.Settle();
    int callbacks = 0;
    bool ok = false;
    const int64_t index = cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool success, const std::string&) { ++callbacks; ok = success; });
    Check(index > 0, "propose");
    cluster.Pump();
    Check(cluster.Node(10).GetCommitIndex() >= index &&
          cluster.Node(10).GetLastApplied() < index && cluster.Node(10).ApplyInFlight(),
          "committed write was not waiting on apply");
    AdvanceLeader(cluster, 10, RaftNode::kProposalTimeoutMs);
    Check(callbacks == 0 && cluster.Node(10).IsLeader() &&
          Metric(cluster.Node(10), "proposal_timeouts") == 0,
          "committed write expired before its result was applied");
    executor.Finish();
    Check(callbacks == 1 && ok, "apply did not deliver the committed write");
}

static void UnsentAppendDoesNotOccupyInflight() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    cluster.SetTransmit(10, false);
    Check(cluster.Node(10).Propose(Command({"SET", "default:held", "v"}), {}) > 0,
          "propose while disconnected");
    Check(cluster.messages.empty() && !cluster.Node(10).HasInflightRpc(30) &&
          !cluster.Node(10).HasInflightRpc(50),
          "dropped append was recorded as in flight");
    cluster.Node(10).Tick();
    Check(cluster.messages.empty() && !cluster.Node(10).HasInflightRpc(30),
          "tick queued an append while transmit was disabled");
    cluster.SetTransmit(10, true);
    cluster.Node(10).Tick();
    Check(cluster.Node(10).HasInflightRpc(30) && cluster.Node(10).HasInflightRpc(50) &&
          !cluster.messages.empty(),
          "tick did not send once transmit was enabled");
}

static void MatchIndexDoesNotRewind() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:a", "1"}), {}) > 0, "first");
    auto first = TakeAppendsFrom(cluster, 10);
    std::vector<Message> first_acks;
    for (const auto& append : first)
        first_acks.push_back(DeliverAppendAndTakeAck(cluster, append));
    for (const auto& ack : first_acks) cluster.Deliver(ack);
    const int64_t match = cluster.Node(10).MatchIndexOf(30);
    Check(match > 0, "matchIndex was not advanced");
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:b", "2"}), {}) > 0, "second");
    auto second = TakeAppendsFrom(cluster, 10);
    for (const auto& append : second) {
        auto ack = DeliverAppendAndTakeAck(cluster, append);
        cluster.Deliver(ack);
    }
    const int64_t after = cluster.Node(10).MatchIndexOf(30);
    Check(after >= match, "matchIndex rewound after the second write");
    raftcore::AppendEntriesResponse stale;
    Check(stale.ParseFromString(first_acks.front().payload), "stale ack decode");
    cluster.Node(10).HandleAppendEntriesResponse(first_acks.front().from, stale);
    Check(cluster.Node(10).MatchIndexOf(30) >= after, "delayed old ACK rewound matchIndex");
}

int main() {
    try {
        StaleRpcDoesNotCommit();
        OneInflightPerPeer();
        FiveNodeQuorumNeedsTwoFollowers();
        MatchIndexDoesNotRewind();
        UnsentAppendDoesNotOccupyInflight();
        UncommittedProposalExpiresButStaysInTheLog();
        CommittedWriteDoesNotExpireWhileApplyLags();
        std::cout << "PASS: production replication ACK correlation\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
