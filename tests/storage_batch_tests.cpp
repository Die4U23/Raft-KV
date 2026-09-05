// Production storage with explicit in-process doubles: no real WAL/fsync coverage.
#include "raft/kv_state_machine.h"
#include "raftcore/raft_log.h"
#include <iostream>
#include <limits>
#include <stdexcept>

static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected batch failure");
}
static std::string Command(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
}
static raftcore::LogEntry Entry(int64_t index, int term = 1) {
    raftcore::LogEntry entry;
    entry.set_index(index); entry.set_term(term); entry.set_command("value");
    return entry;
}
static void LogBatches() {
    const std::string path = "storage-batch/log";
    auto state = rocksdb::testing::StateFor(path);
    {
        RaftLog log(path);
        std::vector<raftcore::LogEntry> entries;
        for (int i = 1; i <= 64; ++i) entries.push_back(Entry(i));
        log.AppendBatch(entries);
        Check(log.LastIndex() == 64 && state->successful_writes == 1 &&
              state->non_sync_writes == 0, "64 appends did not use one sync write");
        const auto durable = state->data;
        Throws([&] { log.AppendBatch({Entry(65), Entry(67)}); });
        Throws([&] { log.AppendBatch({Entry(65), Entry(66, 0)}); });
        state->fail_writes = 1;
        Throws([&] { log.AppendBatch({Entry(65), Entry(66)}); });
        log.AppendBatch({});
        Check(log.LastIndex() == 64 && state->data == durable &&
              state->successful_writes == 1, "failed append advanced durable/volatile state");
    }
    RaftLog reopened(path);
    raftcore::LogEntry last;
    Check(reopened.LastIndex() == 64 && reopened.Get(64, &last) && last.command() == "value",
          "reopened log lost batch tail");
}
static void ApplicationBatches() {
    const std::string path = "storage-batch/kv";
    auto state = rocksdb::testing::StateFor(path);
    {
        KVStateMachine machine(path);
        std::vector<std::string> commands;
        for (int i = 0; i < 64; ++i)
            commands.push_back(Command({"SET", "default:" + std::to_string(i), "value"}));
        const auto replies = machine.ApplyBatch(1, commands);
        Check(replies == std::vector<std::string>(64, "+OK\r\n") &&
              machine.LastApplied() == 64 && state->successful_writes == 1 &&
              state->non_sync_writes == 0, "64 applies did not use one sync write");
        const auto mixed = machine.ApplyBatch(65, {
            Command({"SET", "default:k", "first"}), Command({"DEL", "default:k"}),
            Command({"DEL", "default:k"}), Command({"SET", "default:k", "second"}),
            Command({"DEL", "default:k"}), "", Command({"DEL", "default:0"}),
            Command({"DEL", "default:0"})});
        Check(mixed == std::vector<std::string>({"+OK\r\n", ":1\r\n", ":0\r\n", "+OK\r\n",
                                               ":1\r\n", "+OK\r\n", ":1\r\n", ":0\r\n"}),
              "batch DEL did not observe preceding mutations");
        Check(state->successful_writes == 2 && machine.LastApplied() == 72,
              "mixed batch wrote more than once or lost its final marker");
        const auto durable = state->data;
        Throws([&] { machine.ApplyBatch(73, {Command({"SET", "default:partial", "x"}), "bad"}); });
        Throws([&] { machine.ApplyBatch(73, {"", Command({"GET", "default:k"})}); });
        Throws([&] { machine.ApplyBatch(74, {Command({"SET", "default:partial", "x"})}); });
        Throws([&] { machine.ApplyBatch(std::numeric_limits<int64_t>::max(), {"", ""}); });
        Check(machine.ApplyBatch(73, {}).empty() && state->data == durable &&
              machine.LastApplied() == 72 && state->successful_writes == 2,
              "invalid command/index partially applied a batch");
    }
    KVStateMachine reopened(path);
    std::string value;
    Check(reopened.LastApplied() == 72 && reopened.Get("default:63", &value) && value == "value" &&
          !reopened.Get("default:k", &value) && !reopened.Get("default:0", &value),
          "reopened state/marker does not match committed batch");
}
static void StoreFailures() {
    using Mutation = RocksDBStore::Mutation;
    using Kind = Mutation::Kind;
    const std::string path = "storage-batch/failure";
    auto state = rocksdb::testing::StateFor(path);
    RocksDBStore store(path);
    store.ApplyPut(1, "default:existing", "old");
    const auto durable = state->data;
    Throws([&] { store.ApplyBatch({{2, Kind::Put, "default:new", "x"},
                                  {4, Kind::Noop, {}, {}}}); });
    state->fail_reads = 1;
    Throws([&] { store.ApplyBatch({{2, Kind::Put, "default:new", "x"},
                                  {3, Kind::Delete, "default:existing", {}}}); });
    state->fail_writes = 1;
    Throws([&] { store.ApplyBatch({{2, Kind::Put, "default:new", "x"},
                                  {3, Kind::Delete, "default:existing", {}}}); });
    Check(store.LastApplied() == 1 && state->data == durable && state->successful_writes == 1,
          "failed store batch changed data/marker/volatile index");
    Check(store.ApplyDelete(2, "default:existing"), "single delete wrapper changed semantics");
    store.ApplyNoop(3);
    Check(store.LastApplied() == 3 && state->non_sync_writes == 0, "wrapper omitted durable marker");
}
int main() {
    try {
        LogBatches(); ApplicationBatches(); StoreFailures();
        std::cout << "PASS: storage batching, atomic validation, failures and reopen (in-process doubles)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n'; return 1;
    }
}
