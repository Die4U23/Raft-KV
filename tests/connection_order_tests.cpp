// Models the per-connection FIFO used by src/server/main.cpp.
// This is not the Muduo ClientSession; it checks queue/order semantics that
// production relies on: ERROR items share the sequence, WRITE is async, and
// executing prevents overlapping work. Production ReadIndex coverage is in
// core_tests.cpp against the real RaftNode.
#include <deque>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static int check_count = 0;

static void Check(bool condition, const char* message) {
    ++check_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

struct QueuedCommand {
    enum Type { READ, WRITE, ERROR };
    Type type;
    std::string name;
    std::string error;
};

class CommandQueue {
public:
    std::deque<QueuedCommand> queue;
    std::vector<std::string> responses;
    bool executing = false;
    std::function<void()> pending_write;

    void Enqueue(QueuedCommand cmd) { queue.push_back(std::move(cmd)); }

    void ProcessNext() {
        if (executing || queue.empty())
            return;
        executing = true;
        auto cmd = std::move(queue.front());
        queue.pop_front();
        if (cmd.type == QueuedCommand::ERROR) {
            responses.push_back("-ERR " + cmd.error);
            Complete();
        } else if (cmd.type == QueuedCommand::READ) {
            responses.push_back("OK: " + cmd.name);
            Complete();
        } else {
            pending_write = [this, name = cmd.name] {
                responses.push_back("OK: " + name);
                Complete();
            };
        }
    }

    void CompleteWrite() {
        Check(static_cast<bool>(pending_write), "no in-flight write to complete");
        auto done = std::move(pending_write);
        pending_write = nullptr;
        done();
    }

    void Drain() {
        while (!queue.empty() || pending_write) {
            ProcessNext();
            if (pending_write)
                CompleteWrite();
        }
    }

private:
    void Complete() {
        executing = false;
        ProcessNext();
    }
};

static QueuedCommand Write(const std::string& name) {
    return {QueuedCommand::WRITE, name, ""};
}
static QueuedCommand Read(const std::string& name) {
    return {QueuedCommand::READ, name, ""};
}
static QueuedCommand Error(const std::string& name) {
    return {QueuedCommand::ERROR, name, "unknown command '" + name + "'"};
}

static void TestErrorDoesNotOvertakeWrite() {
    std::cout << "Test 1: ERROR stays behind an in-flight WRITE" << std::endl;
    CommandQueue q;
    q.Enqueue(Write("SET"));
    q.Enqueue(Error("BOGUS"));
    q.Enqueue(Read("GET"));
    q.ProcessNext();
    Check(q.responses.empty(), "WRITE should not reply before commit");
    Check(q.executing, "WRITE should hold the executing flag");
    Check(q.queue.size() == 2, "ERROR and GET should remain queued");
    q.CompleteWrite();
    Check(q.responses.size() == 3, "all three commands should reply once");
    Check(q.responses[0] == "OK: SET", "SET should be first");
    Check(q.responses[1] == "-ERR unknown command 'BOGUS'", "ERROR should not overtake SET");
    Check(q.responses[2] == "OK: GET", "GET should be last");
    Check(!q.executing && q.queue.empty(), "queue should be idle after drain");
    std::cout << "  PASS" << std::endl;
}

static void TestMixedPipelineOrder() {
    std::cout << "Test 2: mixed READ/WRITE pipeline keeps RESP order" << std::endl;
    CommandQueue q;
    q.Enqueue(Write("SET"));
    q.Enqueue(Read("PING"));
    q.Enqueue(Read("GET"));
    q.Enqueue(Write("SET"));
    q.Enqueue(Read("GET"));
    q.Drain();
    Check(q.responses.size() == 5, "pipeline should produce 5 replies");
    Check(q.responses[0] == "OK: SET", "response 1");
    Check(q.responses[1] == "OK: PING", "response 2");
    Check(q.responses[2] == "OK: GET", "response 3");
    Check(q.responses[3] == "OK: SET", "response 4");
    Check(q.responses[4] == "OK: GET", "response 5");
    std::cout << "  PASS" << std::endl;
}

static void TestExecutingBlocksOverlap() {
    std::cout << "Test 3: executing flag blocks overlapping work" << std::endl;
    CommandQueue q;
    q.executing = true;
    q.Enqueue(Read("GET"));
    q.ProcessNext();
    Check(q.responses.empty(), "must not run while executing");
    Check(!q.queue.empty(), "command should remain queued");
    q.executing = false;
    q.ProcessNext();
    Check(q.responses.size() == 1, "should run after executing clears");
    std::cout << "  PASS" << std::endl;
}

static void TestEmptyQueueAndUnknownCommand() {
    std::cout << "Test 4: empty queue and unknown command complete" << std::endl;
    CommandQueue q;
    q.ProcessNext();
    Check(q.responses.empty() && !q.executing, "empty queue must be a no-op");
    q.Enqueue(Error("BOGUS"));
    q.ProcessNext();
    Check(q.responses.size() == 1, "unknown command must reply");
    Check(q.responses[0] == "-ERR unknown command 'BOGUS'", "unknown command text");
    Check(!q.executing, "unknown command must release executing");
    std::cout << "  PASS" << std::endl;
}

int main() {
    try {
        std::cout << "=== Connection Command Order Tests ===" << std::endl;
        TestErrorDoesNotOvertakeWrite();
        TestMixedPipelineOrder();
        TestExecutingBlocksOverlap();
        TestEmptyQueueAndUnknownCommand();
        std::cout << "\n=== All tests passed (" << check_count << " checks) ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        std::cerr << "Completed " << check_count << " checks before failure" << std::endl;
        return 1;
    }
}
