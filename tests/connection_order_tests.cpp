// Production DrainClient/ExecuteNextCommand queue. Homemade FIFO is not CTest.
#include "common/command_type.h"
#include "common/session_queue.h"
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

static std::string Resp(std::initializer_list<std::string> args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args)
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    return out;
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

static void TestReadIndexRedirectErrors() {
    Check(IsReadIndexRedirectError("not leader"), "not leader is a redirect");
    Check(IsReadIndexRedirectError("no leader"), "no leader is a redirect");
    Check(IsReadIndexRedirectError("leadership lost"), "step-down is a redirect");
    Check(IsReadIndexRedirectError("server stopped"), "stop is a redirect");
    Check(!IsReadIndexRedirectError("read index timeout"), "timeout stays a local error");
    Check(!IsReadIndexRedirectError("read index queue full"), "overload stays a local error");
    Check(!IsReadIndexRedirectError("waiting for leader to commit no-op"),
          "pre-noop wait is not MOVED");
}

static void TestSessionQueueLimits() {
    Check(SessionQueueLimits::kMaxCommands == 1000, "F3 command limit drifted");
    Check(SessionQueueLimits::kMaxBytes == 4 * 1024 * 1024, "F3 byte limit drifted");
    Check(SessionQueueLimits::kMaxCommandsPerTurn == 128, "drain turn limit drifted");
    Check(!SessionQueueLimits::IsFull(0, 0), "empty queue reported full");
    Check(!SessionQueueLimits::IsFull(999, SessionQueueLimits::kMaxBytes - 1),
          "999 commands just under 4MiB reported full");
    Check(SessionQueueLimits::IsFull(1000, 0), "1000 queued commands were still admitted");
    Check(SessionQueueLimits::IsFull(0, SessionQueueLimits::kMaxBytes),
          "exactly 4MiB queued was still admitted");
}

static DrainOutcome Feed(SessionCommandQueue& queue, const std::string& wire,
                         size_t max_per_turn = SessionQueueLimits::kMaxCommandsPerTurn) {
    CommandBuffer buffer;
    Check(buffer.Append(wire), "RESP append rejected");
    return DrainCommands(buffer, queue, max_per_turn, true);
}

struct SessionDriver {
    SessionCommandQueue queue;
    std::vector<std::string> replies;
    bool executing = false;
    QueuedCommand inflight;

    void Drain(const std::string& wire) {
        const auto drained = Feed(queue, wire);
        Check(!drained.invalid, drained.invalid_error);
        Process();
    }

    void Process() {
        if (executing || queue.Empty())
            return;
        executing = true;
        inflight = queue.PopFront();
        if (inflight.type == CommandClass::ERROR) {
            replies.push_back("-ERR " + inflight.error_message);
            Complete();
        } else if (inflight.type == CommandClass::READ) {
            replies.push_back("OK: " + inflight.args[0]);
            Complete();
        }
    }

    void FinishWrite() {
        Check(executing && inflight.type == CommandClass::WRITE, "no in-flight write");
        replies.push_back("OK: " + inflight.args[0]);
        Complete();
    }

    void Complete() {
        executing = false;
        inflight = {};
        Process();
    }
};

static void TestErrorDoesNotOvertakeWrite() {
    SessionDriver q;
    q.Drain(Resp({"SET", "k", "v"}) + Resp({"BOGUS"}) + Resp({"GET", "k"}));
    Check(q.replies.empty(), "WRITE must not reply before commit");
    Check(q.executing, "WRITE holds executing");
    Check(q.queue.size() == 2, "ERROR and GET remain queued");
    q.FinishWrite();
    Check(q.replies.size() == 3, "three replies");
    Check(q.replies[0] == "OK: SET", "SET first");
    Check(q.replies[1] == "-ERR ERR unknown command 'BOGUS'", "ERROR does not overtake SET");
    Check(q.replies[2] == "OK: GET", "GET last");
    Check(!q.executing && q.queue.Empty() && q.queue.queued_bytes == 0,
          "idle after drain leaked accounting");
}

static void TestMixedPipelineAndLowercase() {
    SessionDriver q;
    q.Drain(Resp({"set", "k", "v"}) + Resp({"PING"}) + Resp({"GET", "k"}) +
            Resp({"SET", "k2", "v2"}) + Resp({"GET", "k2"}));
    Check(q.inflight.args[0] == "SET", "DrainClient did not uppercase SET");
    q.FinishWrite();
    q.FinishWrite();
    Check(q.replies.size() == 5, "pipeline size");
    Check(q.replies[0] == "OK: SET" && q.replies[1] == "OK: PING" &&
          q.replies[2] == "OK: GET" && q.replies[3] == "OK: SET" &&
          q.replies[4] == "OK: GET", "pipeline RESP order");
}

static void TestQueueStopsAtCommandLimit() {
    SessionCommandQueue queue;
    Check(queue.Enqueue({"SET", "k", "v"}, 14), "first write rejected");
    Check(!queue.Empty() && queue.PopFront().type == CommandClass::WRITE, "write pop");
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        Check(queue.Enqueue({"PING"}, 14), "PING rejected before the production cap");
    Check(queue.IsFull() && !queue.Enqueue({"PING"}, 14),
          "queue still admitted after 1000 waiting commands");
    while (!queue.Empty())
        queue.PopFront();
    Check(!queue.IsFull() && queue.queued_bytes == 0, "pop did not restore admission");
}

static void TestQueueStopsAtByteLimitAndDoesNotLeak() {
    SessionDriver q;
    const auto ping = Resp({"PING"});
    std::string many;
    for (int i = 0; i < 32; ++i)
        many += ping;
    q.Drain(many);
    Check(q.replies.size() == 32 && q.queue.Empty() && q.queue.queued_bytes == 0,
          "completed reads leaked queued_bytes");

    SessionCommandQueue queue;
    const std::string payload(64 * 1024, 'x');
    size_t admitted = 0;
    while (!queue.IsFull()) {
        Check(queue.Enqueue({"SET", "k", payload}, 20), "byte-limit fill rejected a command");
        ++admitted;
        Check(admitted <= SessionQueueLimits::kMaxCommands, "byte fill made no progress");
    }
    Check(queue.queued_bytes >= SessionQueueLimits::kMaxBytes, "byte limit was not reached");
    Check(admitted > 1, "byte-limit test admitted only one command");
    const auto held = queue.queued_bytes;
    auto first = queue.PopFront();
    Check(queue.queued_bytes == held - first.bytes, "pop subtracted the wrong accounted size");
}

static void TestDrainTurnLimitAndStopRead() {
    std::string wire;
    for (int i = 0; i < 129; ++i)
        wire += Resp({"PING"});
    CommandBuffer buffer;
    Check(buffer.Append(wire), "129 PINGs rejected");
    SessionCommandQueue queue;
    const auto first = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(first.enqueued == 128 && !first.stop_read && !first.invalid,
          "first drain turn did not stop at 128");
    Check(buffer.UnreadBytes() == Resp({"PING"}).size(), "turn limit consumed the 129th command");
    const auto second = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(second.enqueued == 1 && queue.size() == 129, "leftover command was dropped");

    SessionCommandQueue full;
    for (size_t i = 0; i < SessionQueueLimits::kMaxCommands; ++i)
        Check(full.Enqueue({"PING"}, 14), "fill");
    CommandBuffer extra;
    extra.Append(Resp({"PING"}));
    const auto blocked = DrainCommands(extra, full, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(blocked.stop_read && blocked.enqueued == 0 && extra.UnreadBytes() == Resp({"PING"}).size(),
          "full queue consumed more input instead of stopping reads");
}

static void TestInvalidStopsWithoutEnqueue() {
    SessionCommandQueue queue;
    CommandBuffer buffer;
    buffer.Append("*0\r\n" + Resp({"PING"}));
    const auto drained = DrainCommands(buffer, queue, SessionQueueLimits::kMaxCommandsPerTurn, true);
    Check(drained.invalid && drained.stop_read && drained.enqueued == 0 && queue.Empty(),
          "invalid RESP was enqueued or skipped");
}

int main() {
    try {
        TestClassifier();
        TestReadIndexRedirectErrors();
        TestSessionQueueLimits();
        TestErrorDoesNotOvertakeWrite();
        TestMixedPipelineAndLowercase();
        TestQueueStopsAtCommandLimit();
        TestQueueStopsAtByteLimitAndDoesNotLeak();
        TestDrainTurnLimitAndStopRead();
        TestInvalidStopsWithoutEnqueue();
        Check(checks >= 50, "too few assertions");
        std::cout << "PASS: production DrainClient queue (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << " after " << checks << " checks\n";
        return 1;
    }
}
