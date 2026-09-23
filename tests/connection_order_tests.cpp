// Production command classifier + per-connection FIFO used by src/server/main.cpp.
// These tests fail if ClassifyCommand or the ERROR-in-queue contract changes.
#include "common/command_type.h"
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
    Check(empty.type == CommandClass::ERROR, "empty command is ERROR");
}

struct Queued {
    CommandClass classified;
    std::string name;
};

class SessionQueue {
public:
    std::deque<Queued> queue;
    std::vector<std::string> replies;
    bool executing = false;
    std::function<void()> inflight_write;

    void Enqueue(const std::vector<std::string>& args) {
        auto classified = ClassifyCommand(args);
        queue.push_back({classified, args.empty() ? "" : args[0]});
        Process();
    }

    void Process() {
        if (executing || queue.empty())
            return;
        executing = true;
        auto cmd = std::move(queue.front());
        queue.pop_front();
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

int main() {
    try {
        TestClassifier();
        TestErrorDoesNotOvertakeWrite();
        TestMixedPipeline();
        Check(checks >= 20, "too few assertions");
        std::cout << "PASS: production ClassifyCommand and FIFO ERROR ordering ("
                  << checks << " checks)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << " after " << checks << " checks\n";
        return 1;
    }
}
