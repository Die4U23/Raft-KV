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
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    cluster.Node(10).Tick();
    Check(done && !ok, "ReadIndex did not time out without majority");
    Check(error.find("timeout") != std::string::npos, "timeout used unexpected error");
}

static void OverloadRejectsAfterTenThousandPending() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    int accepted = 0;
    int overloaded = 0;
    int completed = 0;
    for (int i = 0; i < 10001; ++i) {
        const bool ok = cluster.Node(10).RequestReadIndex(
            [&](bool, int64_t, const std::string&) { ++completed; });
        if (ok) ++accepted;
        else ++overloaded;
    }
    Check(accepted == 10000 && overloaded == 1,
          "ReadIndex overload did not reject the 10001st request");
    Check(Metric(cluster.Node(10), "read_index_overload") == 1,
          "overload counter was not incremented");
    Check(Metric(cluster.Node(10), "read_index_pending") == 10000,
          "pending ReadIndex count drifted from the admitted queue");
    Check(completed == 1, "overload path must invoke the failure callback once");
    cluster.Node(10).Stop();
    Check(completed == 10001, "Stop did not fail the queued ReadIndex requests");
}

static void IsolatedOldLeaderCannotConfirmRead() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    Check(cluster.Node(10).Propose(Command({"SET", "default:k", "old"}), {}) > 0, "seed");
    cluster.Pump();
    cluster.Settle();
    cluster.Partition(10);
    cluster.messages.clear();

    bool done = false, ok = true;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) { done = true; ok = success; }),
          "isolated leader rejected ReadIndex");
    cluster.Pump();
    Check(!done && cluster.Node(10).IsLeader(),
          "isolated leader confirmed a read with only its self-ack");

    cluster.Elect(50);
    Check(cluster.Node(50).Propose(Command({"SET", "default:k", "new"}), {}) > 0,
          "majority could not write while the old leader was isolated");
    cluster.Pump();
    cluster.Settle();
    std::string majority, isolated;
    Check(cluster.State(50).Get("default:k", &majority) && majority == "new",
          "new leader missing the post-partition write");
    Check(cluster.State(10).Get("default:k", &isolated) && isolated == "old",
          "isolated leader applied a majority write");

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    cluster.Node(10).Tick();
    Check(done && !ok, "isolated leader ReadIndex succeeded or hung past the timeout");
}

static void InflightRetryDoesNotConfirm() {
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
    std::set<uint64_t> stale_ids;
    for (const auto& append : stale) stale_ids.insert(AppendRpcId(append));

    bool read_ok = false;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) { read_ok = success; }),
          "ReadIndex rejected");
    Check(TakeAppendsFrom(cluster, 10).empty(),
          "blocked inflight still allocated a new probe rpc_id");

    for (int tick = 0; tick < 5; ++tick)
        cluster.Node(10).Tick();
    auto retries = TakeAppendsFrom(cluster, 10);
    Check(!retries.empty(), "inflight was not retried");
    for (const auto& retry : retries)
        Check(stale_ids.count(AppendRpcId(retry)) != 0,
              "retry allocated a new rpc_id and bound it as a ReadIndex probe");
    std::vector<Message> retry_acks;
    for (const auto& retry : retries)
        retry_acks.push_back(DeliverAppendAndTakeAck(cluster, retry));
    for (const auto& ack : retry_acks)
        cluster.Deliver(ack);
    Check(write_ok == 1, "retried pre-request RPC should still commit the write");
    Check(!read_ok, "ReadIndex succeeded on a retried pre-request rpc_id");

    auto probes = TakeAppendsFrom(cluster, 10);
    Check(!probes.empty(), "no post-retry probe");
    for (const auto& probe : probes)
        Check(stale_ids.count(AppendRpcId(probe)) == 0, "probe reused pre-request rpc_id");
    for (const auto& probe : probes) cluster.Deliver(probe);
    cluster.Pump();
    Check(read_ok, "ReadIndex failed after live probes replaced the inflight");
}

static void LateProbeAckPastElectionTimeoutDoesNotConfirm() {
    Cluster cluster;
    cluster.Elect(10);
    cluster.Settle();
    cluster.messages.clear();
    bool done = false, ok = true;
    Check(cluster.Node(10).RequestReadIndex(
        [&](bool success, int64_t, const std::string&) { done = true; ok = success; }),
          "ReadIndex rejected");
    auto probes = TakeAppendsFrom(cluster, 10);
    Check(probes.size() >= 2, "did not probe both followers");
    std::vector<Message> acks;
    for (const auto& probe : probes)
        acks.push_back(DeliverAppendAndTakeAck(cluster, probe));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    for (const auto& ack : acks) cluster.Deliver(ack);
    Check(!done, "ReadIndex completed on probe ACKs older than min election timeout");
    cluster.Node(10).Tick();
    Check(done && !ok, "expired probe round was left hanging");
}

int main() {
    try {
        DistinctRpcIdsReachQuorum();
        StaleAckDoesNotConfirm();
        ApplyLagWaitsForLastApplied();
        TimeoutWithoutMajority();
        OverloadRejectsAfterTenThousandPending();
        IsolatedOldLeaderCannotConfirmRead();
        InflightRetryDoesNotConfirm();
        LateProbeAckPastElectionTimeoutDoesNotConfirm();
        std::cout << "PASS: production RaftNode ReadIndex\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
