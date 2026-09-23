// Production RaftLog: append, truncate, hard state, scan, and GetTerm.
// storage_batch_tests covers write amortization; these fail if log layout changes.
#include "raftcore/raft_log.h"
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>

static int checks = 0;
static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) throw std::runtime_error(message);
}
template<class F> static void Throws(F call) {
    bool threw = false;
    try { call(); } catch (const std::runtime_error&) { threw = true; }
    Check(threw, "expected Raft log failure");
}
static raftcore::LogEntry Entry(int64_t index, int term, std::string command = "cmd") {
    raftcore::LogEntry entry;
    entry.set_index(index);
    entry.set_term(term);
    entry.set_command(std::move(command));
    return entry;
}

static void EmptyLogAndHardStateDefaults() {
    RaftLog log("raft-log/empty");
    Check(log.LastIndex() == 0 && log.LastTerm() == 0, "empty log retained a tail");
    Check(log.GetTerm(0) == 0, "index 0 term is not 0");
    Check(log.GetTerm(1) == -1, "missing index returned a term");
    raftcore::LogEntry entry;
    Check(!log.Get(0, &entry) && !log.Get(1, &entry) && !log.Get(-1, &entry),
          "Get accepted a non-log index");
    int32_t term = 99, voted = 99;
    Check(!log.LoadHardState(&term, &voted) && term == 0 && voted == -1,
          "missing hard state did not report the default vote");
}

static void AppendGetTruncateAndReopen() {
    const std::string path = "raft-log/cycle";
    {
        RaftLog log(path);
        log.Append(Entry(1, 1, "a"));
        log.AppendBatch({Entry(2, 1, "b"), Entry(3, 2, "c")});
        Check(log.LastIndex() == 3 && log.LastTerm() == 2, "append tail");
        raftcore::LogEntry first, last;
        Check(log.Get(1, &first) && first.command() == "a" && log.GetTerm(1) == 1, "index 1");
        Check(log.Get(3, &last) && last.term() == 2 && last.command() == "c", "index 3");
        log.TruncateSuffix(4);
        Check(log.LastIndex() == 3 && log.LastTerm() == 2, "truncate past the tail mutated state");
        log.TruncateSuffix(2);
        Check(log.LastIndex() == 1 && log.LastTerm() == 1 && log.GetTerm(2) == -1,
              "truncate did not drop the suffix or restore the previous term");
        Throws([&] { log.TruncateSuffix(0); });
        Throws([&] { log.TruncateSuffix(-1); });
        Check(log.LastIndex() == 1, "hard-state truncate deleted log entries");
        log.TruncateSuffix(1);
        Check(log.LastIndex() == 0 && log.LastTerm() == 0, "truncate to empty retained a term");
        log.Append(Entry(1, 4, "d"));
        Check(log.LastIndex() == 1 && log.LastTerm() == 4, "append after empty truncate");
    }
    RaftLog reopened(path);
    raftcore::LogEntry entry;
    Check(reopened.LastIndex() == 1 && reopened.LastTerm() == 4 &&
          reopened.Get(1, &entry) && entry.command() == "d",
          "reopened log lost the truncated-then-appended tail");
}

static void RejectInvalidAppends() {
    RaftLog log("raft-log/invalid");
    log.Append(Entry(1, 1));
    const auto durable = rocksdb::testing::StateFor("raft-log/invalid")->data;
    Throws([&] { log.Append(Entry(3, 1)); });
    Throws([&] { log.Append(Entry(2, 0)); });
    Throws([&] { log.Append(Entry(1, 1)); });
    Throws([&] { log.AppendBatch({Entry(2, 1), Entry(4, 1)}); });
    Check(log.LastIndex() == 1 && log.LastTerm() == 1 &&
          rocksdb::testing::StateFor("raft-log/invalid")->data == durable,
          "invalid append advanced volatile or durable state");
}

static void HardStateRoundTrip() {
    const std::string path = "raft-log/hard";
    {
        RaftLog log(path);
        log.SaveHardState(7, 30);
        int32_t term = 0, voted = 0;
        Check(log.LoadHardState(&term, &voted) && term == 7 && voted == 30,
              "hard state round-trip");
        log.SaveHardState(8, -1);
        Check(log.LoadHardState(&term, &voted) && term == 8 && voted == -1,
              "voted_for -1 did not round-trip");
        Throws([&] { log.SaveHardState(-1, 0); });
        Throws([&] { log.SaveHardState(1, -2); });
        Check(log.LoadHardState(&term, &voted) && term == 8 && voted == -1,
              "invalid hard state overwrote the previous record");
    }
    RaftLog reopened(path);
    int32_t term = 0, voted = 0;
    Check(reopened.LoadHardState(&term, &voted) && term == 8 && voted == -1,
          "reopened log lost hard state");
}

static void ScanRejectsNonContiguousLog() {
    const std::string path = "raft-log/gap";
    {
        RaftLog log(path);
        log.Append(Entry(1, 1));
    }
    auto state = rocksdb::testing::StateFor(path);
    std::string key(8, '\0');
    key[7] = 3;
    state->data[key] = "junk";
    Throws([&] { RaftLog log(path); });
}

int main() {
    try {
        EmptyLogAndHardStateDefaults();
        AppendGetTruncateAndReopen();
        RejectInvalidAppends();
        HardStateRoundTrip();
        ScanRejectsNonContiguousLog();
        Check(checks >= 20, "too few RaftLog assertions");
        std::cout << "PASS: production RaftLog (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
