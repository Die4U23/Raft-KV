// Production RaftNode ReadIndex tests. A homemade manager is not enough:
// these fail if probe rpc_ids, stale ACKs, apply lag, or timeout are wrong.
#include "in_process_cluster.h"
#include <chrono>
#include <iostream>
#include <thread>

static void DistinctRpcIdsReachQuorum() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    bool done = false, ok = false;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) { done = true; ok = success; }),
          "leader rejected ReadIndex");
    Check(!done, "ReadIndex completed before probe ACKs");
    auto probes = TakeAppendsFrom(cluster, 10);
    Check(probes.size() >= 2, "did not probe both followers");
    Check(AppendRpcId(probes[0]) != AppendRpcId(probes[1]),
          "followers shared one rpc_id");
    for (const auto& probe : probes) cluster.Deliver(probe);
    cluster.Pump();
    Check(done && ok, "ReadIndex failed after distinct post-request ACKs");
}

static void StaleAckDoesNotConfirm() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    int write_ok = 0;
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}),
        [&](bool success, const std::string&) { if (success) ++write_ok; }) > 0,
          "write rejected");
    auto stale = TakeAppendsFrom(cluster, 10);
    Check(stale.size() >= 2, "write was not replicated");
    std::vector<Message> acks;
    std::set<uint64_t> stale_ids;
    for (const auto& append : stale) {
        stale_ids.insert(AppendRpcId(append));
        acks.push_back(DeliverAppendAndTakeAck(cluster, append));
    }
    bool read_ok = false;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) { read_ok = success; }),
          "ReadIndex rejected");
    for (const auto& ack : acks) cluster.Deliver(ack);
    Check(write_ok == 1, "stale ACKs should still commit the write");
    Check(!read_ok, "ReadIndex succeeded on pre-request ACKs");
    auto probes = TakeAppendsFrom(cluster, 10);
    Check(!probes.empty(), "no post-request probe");
    for (const auto& probe : probes)
        Check(stale_ids.count(AppendRpcId(probe)) == 0, "probe reused pre-request rpc_id");
    for (const auto& probe : probes) cluster.Deliver(probe);
    cluster.Pump();
    Check(read_ok, "ReadIndex failed after live probes");
}

static void ApplyLagWaitsForLastApplied() {
    ManualApplyExecutor executor;
    Cluster cluster({10, 30, 50}, &executor);
    cluster.Elect(10);
    while (executor.busy) executor.Finish();
    cluster.Settle();
    cluster.messages.clear();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "v"}), {}) > 0, "write rejected");
    cluster.Pump();
    Check(executor.busy, "committed write was applied too early");
    const int64_t commit = cluster.Node(10).GetCommitIndex();
    Check(cluster.Node(10).GetLastApplied() < commit, "lastApplied already caught commit");
    bool done = false, ok = false;
    int64_t read_index = -1;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t index, const std::string&) {
            done = true; ok = success; read_index = index;
        }), "ReadIndex rejected during apply lag");
    auto probes = TakeAppendsFrom(cluster, 10);
    for (const auto& probe : probes) cluster.Deliver(probe);
    cluster.Pump();
    Check(!done, "ReadIndex completed before lastApplied caught the barrier");
    executor.Finish();
    Check(done && ok && read_index <= cluster.Node(10).GetLastApplied(),
          "ReadIndex did not wait for apply");
}

static void TimeoutWithoutMajority() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    bool done = false, ok = true;
    std::string error;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string& err) {
            done = true; ok = success; error = err;
        }), "ReadIndex rejected");
    cluster.messages.clear();
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    cluster.Node(10).Tick();
    Check(done && !ok, "ReadIndex did not time out without majority");
    Check(error.find("timeout") != std::string::npos, "timeout used unexpected error");
}

int main() {
    try {
        DistinctRpcIdsReachQuorum();
        StaleAckDoesNotConfirm();
        ApplyLagWaitsForLastApplied();
        TimeoutWithoutMajority();
        std::cout << "PASS: production RaftNode ReadIndex\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
