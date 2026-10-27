// Production RaftNode partition behavior. Homemade partition trackers are not CTest.
#include "in_process_cluster.h"
#include <iostream>

static void MajoritySurvivesOnePartition() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(50);
    cluster.messages.clear();
    bool ok = false;
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "live"}),
        [&](bool success, const std::string&) { ok = success; }) > 0, "propose");
    cluster.Pump();
    Check(ok, "majority write failed with one follower partitioned");
    std::string value;
    Check(cluster.State(10).Get("default:k", &value) && value == "live", "leader missing write");
    Check(cluster.Node(10).MatchIndexOf(50) < cluster.Node(10).GetCommitIndex(),
          "partitioned follower still matched the new commit");
}

static void TwoPartitionsBlockCommit() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    const int64_t before = cluster.Node(10).GetCommitIndex();
    cluster.Partition(30);
    cluster.Partition(50);
    cluster.messages.clear();
    bool ok = false;
    Check(cluster.Node(10).Propose(Command({"SET", "default:blocked", "x"}),
        [&](bool success, const std::string&) { ok = success; }) > 0, "propose");
    cluster.Pump();
    Check(!ok && cluster.Node(10).GetCommitIndex() == before,
          "write committed without any follower");
    cluster.Heal(30);
    cluster.Heal(50);
    cluster.Settle();
    Check(ok && cluster.Node(10).GetCommitIndex() > before,
          "healed majority did not commit the blocked write");
    for (int id : {10, 30, 50}) {
        std::string value;
        Check(cluster.State(id).Get("default:blocked", &value) && value == "x",
              "replica did not catch up after heal");
    }
}

static void IsolatedFollowerCatchesUp() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.Partition(30);
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:ahead", "v"}), {}) > 0, "propose");
    cluster.Pump();
    const int64_t leader_applied = cluster.Node(10).GetLastApplied();
    Check(cluster.Node(30).GetLastApplied() < leader_applied,
          "isolated follower applied the partitioned write");
    cluster.Heal(30);
    cluster.Settle();
    Check(cluster.Node(30).GetLastApplied() == leader_applied,
          "follower did not catch up after heal");
    std::string value;
    Check(cluster.State(30).Get("default:ahead", &value) && value == "v",
          "follower KV missing catch-up value");
}

int main() {
    try {
        MajoritySurvivesOnePartition();
        TwoPartitionsBlockCommit();
        IsolatedFollowerCatchesUp();
        std::cout << "PASS: production partition replication\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
