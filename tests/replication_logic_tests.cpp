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
        std::cout << "PASS: production replication ACK correlation\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
