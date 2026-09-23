// Production RaftNode edge cases. The previous homemade ReplicationManager
// could pass while RaftNode was wrong; these tests link the real node.
#include "in_process_cluster.h"
#include <iostream>

static void PrefixConflictDoesNotDeleteOrAck() {
    Cluster cluster;
    auto request = Append(1, 1, 0, 0, 0);
    *request.add_entries() = Entry(1, 1, Command({"SET", "default:a", "one"}));
    *request.add_entries() = Entry(2, 1, Command({"SET", "default:b", "old"}));
    cluster.Node(10).HandleAppendEntries(30, request);
    Check(LastResponse(cluster).success(), "initial append failed");
    request = Append(2, 2, 1, 2, 0);
    cluster.Node(10).HandleAppendEntries(30, request);
    auto response = LastResponse(cluster);
    Check(!response.success() && response.last_log_index() == 2,
          "prefix mismatch deleted the log suffix");
    request = Append(2, 3, 1, 1, 2);
    cluster.Node(10).HandleAppendEntries(30, request);
    response = LastResponse(cluster);
    Check(response.success() && response.last_log_index() == 1 &&
          cluster.Node(10).GetCommitIndex() == 1,
          "unmatched tail was committed");
}

static void DuplicateAckDoesNotAdvanceTwice() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "propose");
    auto appends = TakeAppendsFrom(cluster, 10);
    Check(!appends.empty(), "missing append");
    auto ack = DeliverAppendAndTakeAck(cluster, appends.front());
    const int64_t before = cluster.Node(10).GetCommitIndex();
    cluster.Deliver(ack);
    const int64_t committed = cluster.Node(10).GetCommitIndex();
    const auto acks = Metric(cluster.Node(10), "replication_data_ack_count");
    Check(committed >= before, "first ACK did not move commit");
    cluster.Deliver(ack);
    Check(cluster.Node(10).GetCommitIndex() == committed, "duplicate ACK advanced commit again");
    Check(Metric(cluster.Node(10), "replication_data_ack_count") == acks,
          "duplicate ACK was counted twice");
}

static void RetryKeepsRpcId() {
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
    for (int tick = 0; tick < 10; ++tick) {
        cluster.Node(10).Tick();
        cluster.Node(30).Tick();
    }
    bool retried = false;
    auto messages = std::move(cluster.messages);
    cluster.messages.clear();
    for (const auto& message : messages) {
        if (message.to != 30 || message.type != RaftMsgType::kAppendEntries)
            continue;
        raftcore::AppendEntries decoded;
        Check(decoded.ParseFromString(message.payload), "retry decode");
        Check(decoded.rpc_id() == reply.rpc_id(), "retry replaced the in-flight rpc_id");
        retried = true;
    }
    Check(retried, "lost reply was not retried with the same rpc_id");
}

static void CurrentTermRequiredToCommit() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    Check(cluster.Node(10).GetCommitIndex() >= 1, "leader no-op was not committed");
    Check(cluster.Node(10).GetCurrentTerm() >= 1, "leader term stayed 0");
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:now", "v"}), {}) > 0, "propose");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(10).GetCommitIndex() >= 2, "current-term entry did not commit");
    std::string value;
    Check(cluster.State(30).Get("default:now", &value) && value == "v",
          "follower missing current-term entry");
}

int main() {
    try {
        PrefixConflictDoesNotDeleteOrAck();
        DuplicateAckDoesNotAdvanceTwice();
        RetryKeepsRpcId();
        CurrentTermRequiredToCommit();
        std::cout << "PASS: production replication edge cases\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
