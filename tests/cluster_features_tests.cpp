#include "test_support/in_process_cluster.h"
#include <iostream>
#include <string>

static void ConfigCommandReplicates() {
    Cluster cluster;
    cluster.Elect(10);
    Check(cluster.Node(10).Propose(Command({"CFGSET", "policy", "on", "app", "1"}), {}) > 0,
          "CFGSET rejected");
    cluster.Pump();
    uint64_t version = 0;
    std::string value;
    Check(cluster.State(30).GetConfig("policy", &version, &value) && version == 1 && value == "on",
          "follower did not apply CFGSET");
    Check(cluster.Node(10).Propose(Command({"CFGSET", "policy", "off", "app", "1"}), {}) > 0,
          "CFGSET retry rejected");
    cluster.Pump();
    Check(cluster.State(50).GetConfig("policy", &version, &value) && version == 1 && value == "on",
          "CFGSET retry published a second version");
    Check(cluster.Node(10).Propose(Command({"CFGROLLBACK", "policy", "1", "app", "2"}), {}) > 0,
          "rollback rejected");
    cluster.Pump();
    Check(cluster.State(30).GetConfig("policy", &version, &value) && version == 2 && value == "on",
          "rollback did not copy version 1 forward");
}

static void LeaseReadServesWithoutProbeAndWaitsForApply() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10);
    while (executor.busy) executor.Finish();
    cluster.Settle();
    cluster.Node(10).SetLeaseReads(true);
    cluster.messages.clear();
    bool done = false;
    Check(cluster.Node(10).RequestReadIndex([&](bool success, int64_t, const std::string&) {
        done = success;
    }), "lease read was rejected");
    Check(done, "fresh lease did not serve the read");
    Check(cluster.messages.empty(), "lease read sent a quorum probe");
    Check(Metric(cluster.Node(10), "lease_reads") == 1, "lease counter did not move");

    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "lagging write rejected");
    cluster.Pump();
    Check(executor.busy, "write was applied before the lease read");
    bool lagged = false;
    Check(cluster.Node(10).RequestReadIndex([&](bool success, int64_t index, const std::string&) {
        lagged = success && index <= cluster.Node(10).GetLastApplied();
    }), "lease read during apply lag was rejected");
    Check(!lagged, "lease read returned before lastApplied");
    executor.Finish();
    Check(lagged, "lease read did not finish after apply");

    const auto served = Metric(cluster.Node(10), "lease_reads");
    cluster.Advance(10, RaftNode::kMinElectionTimeoutMs - RaftNode::kLeaseClockDriftMs);
    bool late = false;
    Check(cluster.Node(10).RequestReadIndex([&](bool, int64_t, const std::string&) { late = true; }),
          "expired lease rejected the ReadIndex fallback");
    Check(!late, "expired lease still served the read locally");
    Check(Metric(cluster.Node(10), "lease_reads") == served, "expired lease was counted as a lease read");
}

static void RemovedVoterCannotCampaignAndSnapshotKeepsTheSet() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Partition(50);
    int applied = 0;
    Check(cluster.Node(10).ProposeMemberChange(false, 50, [&](bool ok, const std::string& reply) {
        if (ok && reply == "+OK\r\n") ++applied;
    }) > 0, "leave was rejected");
    cluster.Pump();
    cluster.Settle();
    Check(applied == 1, "leave did not apply");
    Check(!cluster.Node(10).MembershipJoint(), "joint config stayed after commit");
    Check(!cluster.Node(10).IsClusterVoter(50) && cluster.Node(30).IsClusterVoter(10) &&
          !cluster.Node(30).IsClusterVoter(50), "voter set did not drop 50");
    Check(cluster.Node(50).IsClusterVoter(50), "partitioned node applied the removal");
    cluster.Node(10).SetSnapshotDistanceForTest(1);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "post-leave write rejected");
    cluster.Pump();
    Check(cluster.Node(10).GetSnapshotIndex() > 0, "leader did not compact after the voter change");
    cluster.Heal(50);
    cluster.Pump();
    cluster.Settle();
    Check(!cluster.Node(50).IsClusterVoter(50), "snapshot install kept the removed voter");
    for (int i = 0; i < 40; ++i) {
        cluster.Advance(50, RaftNode::kTickIntervalMs);
        cluster.Node(50).Tick();
    }
    Check(std::string(cluster.Node(50).StateName()) == "follower", "removed node campaigned");
    Check(cluster.Node(10).IsLeader(), "leader stepped down after the removal");
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v2"}), {}) > 0, "remaining quorum rejected a write");
    cluster.Pump();
    std::string value;
    Check(cluster.State(30).Get("default:k", &value) && value == "v2", "remaining quorum did not commit");
    cluster.bootstrap_voters.clear();
    cluster.Restart(30);
    Check(!cluster.Node(30).IsClusterVoter(50) && cluster.Node(30).IsClusterVoter(10),
          "restart lost the applied voter set");
}

static void JoinedNonVoterEntersTheQuorum() {
    Cluster cluster({10, 30, 50, 70});
    cluster.Rebootstrap({10, 30, 50});
    cluster.Elect(10);
    for (int i = 0; i < 40; ++i) {
        cluster.Advance(70, RaftNode::kTickIntervalMs);
        cluster.Node(70).Tick();
    }
    Check(std::string(cluster.Node(70).StateName()) == "follower", "non-voter campaigned");
    Check(cluster.Node(10).ProposeMemberChange(true, 70, {}) > 0, "join was rejected");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(70).IsClusterVoter(70) && !cluster.Node(10).MembershipJoint(),
          "join did not finish");
    Check(cluster.Node(10).ProposeMemberChange(true, 70, {}) == -4, "overlapping join was accepted");
    cluster.Partition(50);
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "joined"}), {}) > 0,
          "quorum of the new set rejected a write");
    cluster.Pump();
    std::string value;
    Check(cluster.State(70).Get("default:k", &value) && value == "joined",
          "new voter did not apply the committed write");
}

static void NewPeerJoinsByAddress() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.AddNode(70, "127.0.0.1", 9070);
    Check(!cluster.Node(10).IsClusterVoter(70), "new peer started as a voter");
    Check(cluster.Node(10).ProposeMemberChange(true, 70, {}, "127.0.0.1", 9070) > 0,
          "join by address was rejected");
    cluster.Pump();
    cluster.Settle();
    Check(cluster.Node(70).IsClusterVoter(70) && cluster.Node(30).IsClusterVoter(70),
          "address join did not enter the voter set");
    Check(cluster.Node(10).ProposeMemberChange(true, 70, {}, "127.0.0.1", 9070) == -4,
          "second address join was accepted");
}

static void LeaderCanRemoveItself() {
    Cluster cluster;
    cluster.Elect(10);
    Check(cluster.Node(10).ProposeMemberChange(false, 10, {}) > 0, "self leave was rejected");
    cluster.Pump();
    cluster.Settle();
    Check(!cluster.Node(10).IsLeader() && !cluster.Node(10).IsClusterVoter(10),
          "removed leader stayed in the voter set");
    Check(cluster.Node(30).IsClusterVoter(30) && !cluster.Node(30).IsClusterVoter(10),
          "follower kept the removed leader");
    const int leader = cluster.ElectAmong({30, 50});
    Check(cluster.Node(leader).Propose(Command({"SET", "default:k", "left"}), {}) > 0,
          "remaining pair rejected a write");
    cluster.Pump();
    const int other = leader == 30 ? 50 : 30;
    std::string value;
    Check(cluster.State(other).Get("default:k", &value) && value == "left",
          "remaining pair did not commit");
}

static void FollowerForwardsMembershipToTheLeader() {
    Cluster cluster;
    cluster.Elect(10);
    bool applied = false;
    Check(cluster.Node(30).ForwardMemberChange(10, false, 50, {}, 0, [&](bool ok, const std::string& reply) {
        applied = ok && reply == "+OK\r\n";
    }), "forward was not sent");
    cluster.Pump();
    cluster.Settle();
    Check(applied, "forwarded leave did not apply");
    Check(!cluster.Node(10).IsClusterVoter(50) && !cluster.Node(30).IsClusterVoter(50),
          "forwarded leave did not drop 50");
}

int main() {
    try {
        ConfigCommandReplicates();
        LeaseReadServesWithoutProbeAndWaitsForApply();
        RemovedVoterCannotCampaignAndSnapshotKeepsTheSet();
        JoinedNonVoterEntersTheQuorum();
        NewPeerJoinsByAddress();
        LeaderCanRemoveItself();
        FollowerForwardsMembershipToTheLeader();
        std::cout << "PASS: config, lease, and membership\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
