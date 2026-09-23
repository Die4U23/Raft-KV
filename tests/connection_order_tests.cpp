// Production command classifier + per-connection FIFO used by src/server/main.cpp.
// These tests fail if ClassifyCommand, ERROR-in-queue, or F3 queue limits change.
#include "common/command_type.h"
#include "common/session_queue.h"
#include <deque>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int checks = 0;

static void Check(bool ok, const char* message) {
    ++checks;
    if (!ok) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

static void TestClassifier() {
    auto set = ClassifyCommand({"SET", "k", "v"});
    Check(set.type == CommandClass::WRITE && set.error.empty(), "SET 3 args is WRITE");
    auto del = ClassifyCommand({"DEL", "k"});
    Check(del.type == CommandClass::WRITE && del.error.empty(), "DEL 2 args is WRITE");
    auto get = ClassifyCommand({"GET", "k"});
    Check(get.type == CommandClass::READ && get.error.empty(), "GET 2 args is READ");
    auto ping = ClassifyCommand({"PING"});
    Check(ping.type == CommandClass::READ, "PING is READ");
    auto info = ClassifyCommand({"INFO"});
    Check(info.type == CommandClass::READ, "INFO is READ");
    auto select = ClassifyCommand({"SELECT", "ns"});
    Check(select.type == CommandClass::READ, "SELECT is READ");

    auto bogus = ClassifyCommand({"BOGUS"});
    Check(bogus.type == CommandClass::ERROR, "unknown command is ERROR");
    Check(bogus.error == "ERR unknown command 'BOGUS'", "unknown command text");
    auto short_set = ClassifyCommand({"SET", "k"});
    Check(short_set.type == CommandClass::ERROR, "SET missing value is ERROR");
    Check(short_set.error.find("wrong number of arguments") != std::string::npos,
          "SET arity error text");
    auto bad_get = ClassifyCommand({"GET"});
    Check(bad_get.type == CommandClass::ERROR, "GET missing key is ERROR");
    auto empty = ClassifyCommand({});
    Check(empty.type == CommandClass::ERROR && empty.error == "ERR empty command",
          "empty command text");

    auto ping_args = ClassifyCommand({"PING", "x"});
    Check(ping_args.type == CommandClass::ERROR &&
          ping_args.error == "ERR wrong number of arguments for 'PING' command",
          "PING arity");
    auto info_args = ClassifyCommand({"INFO", "all"});
    Check(info_args.type == CommandClass::ERROR, "INFO extra arg is ERROR");
    auto select_short = ClassifyCommand({"SELECT"});
    Check(select_short.type == CommandClass::ERROR, "SELECT missing ns is ERROR");
    auto select_extra = ClassifyCommand({"SELECT", "a", "b"});
    Check(select_extra.type == CommandClass::ERROR, "SELECT extra arg is ERROR");
    auto get_extra = ClassifyCommand({"GET", "k", "extra"});
    Check(get_extra.type == CommandClass::ERROR, "GET extra arg is ERROR");
    auto set_extra = ClassifyCommand({"SET", "k", "v", "nx"});
    Check(set_extra.type == CommandClass::ERROR &&
          set_extra.error.find("wrong number of arguments") != std::string::npos,
          "SET extra arg is ERROR");
    auto del_extra = ClassifyCommand({"DEL", "k", "k2"});
    Check(del_extra.type == CommandClass::ERROR, "DEL extra arg is ERROR");
    auto del_short = ClassifyCommand({"DEL"});
    Check(del_short.type == CommandClass::ERROR, "DEL missing key is ERROR");
    auto lower = ClassifyCommand({"set", "k", "v"});
    Check(lower.type == CommandClass::ERROR &&
          lower.error == "ERR unknown command 'set'",
          "classifier is case-sensitive; DrainClient must uppercase first");
}

static void TestSessionQueueLimits() {
    Check(SessionQueueLimits::kMaxCommands == 1000, "F3 command limit drifted");
    Check(SessionQueueLimits::kMaxBytes == 4 * 1024 * 1024, "F3 byte limit drifted");
    Check(!SessionQueueLimits::IsFull(0, 0), "empty queue reported full");
    Check(!SessionQueueLimits::IsFull(999, SessionQueueLimits::kMaxBytes - 1),
          "999 commands just under 4MiB reported full");
    Check(SessionQueueLimits::IsFull(1000, 0), "1000 queued commands were still admitted");
    Check(SessionQueueLimits::IsFull(0, SessionQueueLimits::kMaxBytes),
          "exactly 4MiB queued was still admitted");
    const std::vector<std::string> args{"SET", "k", "value"};
    Check(SessionQueueLimits::AccountedBytes(20, args) == 20 + 3 + 1 + 5,
          "accounted bytes dropped RESP framing or argument bytes");
}

struct Queued {
    CommandClass classified;
    std::string name;
    size_t bytes = 0;
};

class SessionQueue {
public:
    std::deque<Queued> queue;
    std::vector<std::string> replies;
    bool executing = false;
    std::function<void()> inflight_write;
    size_t queued_bytes = 0;

    bool CanAdmit() const {
        return !SessionQueueLimits::IsFull(queue.size(), queued_bytes);
    }

    void Enqueue(const std::vector<std::string>& args, size_t consumed = 0) {
        Check(CanAdmit(), "admitted a command after the production queue was full");
        auto classified = ClassifyCommand(args);
        const auto bytes = SessionQueueLimits::AccountedBytes(consumed, args);
        queue.push_back({classified, args.empty() ? "" : args[0], bytes});
        queued_bytes += bytes;
        Process();
    }

    void Process() {
        if (executing || queue.empty())
            return;
        executing = true;
        auto cmd = std::move(queue.front());
        queue.pop_front();
        queued_bytes -= cmd.bytes;
        if (cmd.classified.type == CommandClass::ERROR) {
            replies.push_back("-ERR " + cmd.classified.error);
            Complete();
        } else if (cmd.classified.type == CommandClass::READ) {
            replies.push_back("OK: " + cmd.name);
            Complete();
        } else {
            inflight_write = [this, name = cmd.name] {
                replies.push_back("OK: " + name);
                Complete();
            };
        }
    }

    void FinishWrite() {
        Check(static_cast<bool>(inflight_write), "no in-flight write");
        auto done = std::move(inflight_write);
        inflight_write = nullptr;
        done();
    }

private:
    void Complete() {
        executing = false;
        Process();
    }
};

static void TestErrorDoesNotOvertakeWrite() {
    SessionQueue q;
    q.Enqueue({"SET", "k", "v"});
    q.Enqueue({"BOGUS"});
    q.Enqueue({"GET", "k"});
    Check(q.replies.empty(), "WRITE must not reply before commit");
    Check(q.executing, "WRITE holds executing");
    Check(q.queue.size() == 2, "ERROR and GET remain queued");
    q.FinishWrite();
    Check(q.replies.size() == 3, "three replies");
    Check(q.replies[0] == "OK: SET", "SET first");
    Check(q.replies[1] == "-ERR ERR unknown command 'BOGUS'", "ERROR does not overtake SET");
    Check(q.replies[2] == "OK: GET", "GET last");
    Check(!q.executing && q.queue.empty(), "idle after drain");
}

static void TestMixedPipeline() {
    SessionQueue q;
    q.Enqueue({"SET", "k", "v"});
    q.Enqueue({"PING"});
    q.Enqueue({"GET", "k"});
    q.Enqueue({"SET", "k2", "v2"});
    q.Enqueue({"GET", "k2"});
    q.FinishWrite();
    q.FinishWrite();
    Check(q.replies.size() == 5, "pipeline size");
    Check(q.replies[0] == "OK: SET" && q.replies[1] == "OK: PING" &&
          q.replies[2] == "OK: GET" && q.replies[3] == "OK: SET" &&
          q.replies[4] == "OK: GET", "pipeline RESP order");
}

static void TestQueueStopsAtCommandLimit() {
    SessionQueue q;
    q.Enqueue({"SET", "k", "v"});
    Check(q.executing && q.queue.empty(), "write should be executing, not queued");
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        q.Enqueue({"PING"});
    Check(q.queue.size() == SessionQueueLimits::kMaxCommands, "lost queued PINGs");
    Check(!q.CanAdmit(), "queue still admitted after 1000 waiting commands");
    q.FinishWrite();
    Check(q.replies.size() == 1 + SessionQueueLimits::kMaxCommands, "limit drain replies");
    Check(q.CanAdmit() && q.queue.empty() && q.queued_bytes == 0,
          "queue did not resume after draining the command limit");
}

static void TestQueueStopsAtByteLimit() {
    SessionQueue q;
    q.Enqueue({"SET", "k", "v"}, 8);
    const std::string payload(64 * 1024, 'x');
    size_t admitted = 0;
    while (q.CanAdmit()) {
        q.Enqueue({"SET", "k", payload}, 20);
        ++admitted;
        Check(admitted <= SessionQueueLimits::kMaxCommands,
              "byte-limit fill exceeded the command cap; loop is not making progress");
    }
    Check(q.queued_bytes >= SessionQueueLimits::kMaxBytes ||
          q.queue.size() >= SessionQueueLimits::kMaxCommands,
          "stopped admitting without hitting either production limit");
    Check(admitted > 1, "byte-limit test admitted only the in-flight write");
}

int main() {
    try {
        TestClassifier();
        TestSessionQueueLimits();
        TestErrorDoesNotOvertakeWrite();
        TestMixedPipeline();
        TestQueueStopsAtCommandLimit();
        TestQueueStopsAtByteLimit();
        Check(checks >= 40, "too few assertions");
        std::cout << "PASS: production ClassifyCommand and FIFO ERROR ordering ("
                  << checks << " checks)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << " after " << checks << " checks\n";
        return 1;
    }
}
