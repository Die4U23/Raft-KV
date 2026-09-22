// Connection command order tests
// Verify that commands on the same connection execute and respond in order

#include <iostream>
#include <queue>
#include <string>
#include <vector>
#include <cassert>

static int check_count = 0;

static void Check(bool condition, const char* message) {
    ++check_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// Simulate per-connection command queue
struct CommandQueue {
    enum Type { READ, WRITE, ERROR };

    struct Command {
        Type type;
        std::string op;
        std::vector<std::string> args;
        std::string error_msg;
    };

    std::queue<Command> queue;
    bool executing = false;
    std::vector<std::string> responses;

    void Enqueue(Command cmd) {
        queue.push(cmd);
    }

    void ProcessNext() {
        if (queue.empty() || executing) return;

        executing = true;
        Command cmd = queue.front();
        queue.pop();

        // Simulate command execution
        if (cmd.type == ERROR) {
            responses.push_back("ERR: " + cmd.error_msg);
        } else if (cmd.type == READ) {
            responses.push_back("OK: " + cmd.op);
        } else {
            responses.push_back("OK: " + cmd.op);
        }

        executing = false;
        ProcessNext();
    }
};

static void TestErrorCommandOrder() {
    std::cout << "Test 1: Error commands preserve order" << std::endl;

    CommandQueue q;

    // SET key1 value1
    q.Enqueue({CommandQueue::WRITE, "SET", {"SET", "key1", "value1"}, ""});

    // Invalid GET (wrong arg count)
    q.Enqueue({CommandQueue::ERROR, "GET", {"GET"}, "wrong number of arguments"});

    // GET key1
    q.Enqueue({CommandQueue::READ, "GET", {"GET", "key1"}, ""});

    // Process all commands
    while (!q.queue.empty()) {
        q.ProcessNext();
    }

    // Verify responses are in order
    Check(q.responses.size() == 3, "Should have 3 responses");
    Check(q.responses[0] == "OK: SET", "First response should be SET");
    Check(q.responses[1] == "ERR: wrong number of arguments", "Second response should be error");
    Check(q.responses[2] == "OK: GET", "Third response should be GET");

    std::cout << "  PASS: Error responses maintain order" << std::endl;
}

static void TestMixedCommandOrder() {
    std::cout << "Test 2: Mixed READ/WRITE commands preserve order" << std::endl;

    CommandQueue q;

    // Pipeline: SET -> PING -> GET -> SET -> GET
    q.Enqueue({CommandQueue::WRITE, "SET", {"SET", "k", "v"}, ""});
    q.Enqueue({CommandQueue::READ, "PING", {"PING"}, ""});
    q.Enqueue({CommandQueue::READ, "GET", {"GET", "k"}, ""});
    q.Enqueue({CommandQueue::WRITE, "SET", {"SET", "k2", "v2"}, ""});
    q.Enqueue({CommandQueue::READ, "GET", {"GET", "k2"}, ""});

    while (!q.queue.empty()) {
        q.ProcessNext();
    }

    Check(q.responses.size() == 5, "Should have 5 responses");
    Check(q.responses[0] == "OK: SET", "Response 1: SET");
    Check(q.responses[1] == "OK: PING", "Response 2: PING");
    Check(q.responses[2] == "OK: GET", "Response 3: GET");
    Check(q.responses[3] == "OK: SET", "Response 4: SET");
    Check(q.responses[4] == "OK: GET", "Response 5: GET");

    std::cout << "  PASS: Mixed commands maintain order" << std::endl;
}

static void TestQueueNotExecutingDuringExecution() {
    std::cout << "Test 3: Queue respects executing flag" << std::endl;

    CommandQueue q;
    q.executing = true;

    q.Enqueue({CommandQueue::READ, "GET", {"GET", "k"}, ""});
    q.ProcessNext();

    Check(q.responses.empty(), "Should not process when already executing");
    Check(!q.queue.empty(), "Command should remain in queue");

    q.executing = false;
    q.ProcessNext();

    Check(q.responses.size() == 1, "Should process after executing=false");

    std::cout << "  PASS: Execution flag prevents concurrent processing" << std::endl;
}

static void TestEmptyQueueHandling() {
    std::cout << "Test 4: Empty queue handling" << std::endl;

    CommandQueue q;
    q.ProcessNext();

    Check(q.responses.empty(), "Should not crash on empty queue");
    Check(!q.executing, "Should not be executing");

    std::cout << "  PASS: Empty queue handled gracefully" << std::endl;
}

int main() {
    try {
        std::cout << "=== Connection Command Order Tests ===" << std::endl;

        TestErrorCommandOrder();
        TestMixedCommandOrder();
        TestQueueNotExecutingDuringExecution();
        TestEmptyQueueHandling();

        std::cout << "\n=== All tests passed (" << check_count << " checks) ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        std::cerr << "Completed " << check_count << " checks before failure" << std::endl;
        return 1;
    }
}
