// 测试同连接命令顺序
// 验证 SET -> GET 和 SET -> PING 在同一连接上的执行顺序

#include <iostream>
#include <string>
#include <vector>
#include <cassert>

static int test_count = 0;

static void Check(bool condition, const char* message) {
    ++test_count;
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        throw std::runtime_error(message);
    }
}

// 模拟连接状态
struct Connection {
    int id;
    std::string buffer;
    std::vector<std::string> responses;
    bool pending_write = false;
};

// 测试 1: 同连接 SET 后立即 GET
static void TestSetThenGetOnSameConnection() {
    std::cout << "Test 1: SET followed by GET on same connection" << std::endl;

    Connection conn{1, "", {}, false};

    // 模拟客户端在同一连接上发送 SET 和 GET
    // 当前问题：SET 是异步的，GET 可能先返回

    // 期望行为：GET 应该等待 SET 完成
    // 实际行为：GET 可能读到旧值或空值

    std::cout << "  WARNING: This test documents the current issue" << std::endl;
    std::cout << "  Current behavior: GET may execute before SET commits" << std::endl;
    std::cout << "  Expected behavior: GET should wait for SET" << std::endl;
}

// 测试 2: 同连接 SET 后 PING
static void TestSetThenPingOnSameConnection() {
    std::cout << "Test 2: SET followed by PING on same connection" << std::endl;

    Connection conn{1, "", {}, false};

    // 当前问题：PING 可能在 SET 响应之前返回
    // 期望：响应顺序应该与请求顺序一致

    std::cout << "  WARNING: Response order may not match request order" << std::endl;
}

// 测试 3: 多个 SET 在同一连接上
static void TestMultipleSetsOnSameConnection() {
    std::cout << "Test 3: Multiple SETs on same connection" << std::endl;

    Connection conn{1, "", {}, false};

    // 期望：SET 的执行顺序和响应顺序都应该保持
    // 当前：由于都是异步等待，顺序应该正确，但响应可能乱序

    std::cout << "  Execution order likely correct, response order may vary" << std::endl;
}

// 测试 4: Pipeline 模式下的命令顺序
static void TestPipelineCommandOrder() {
    std::cout << "Test 4: Command order in pipeline mode" << std::endl;

    // redis-cli --pipe 模式会发送多个命令而不等待响应
    // 这种情况下更容易暴露顺序问题

    std::cout << "  Pipeline mode increases likelihood of order issues" << std::endl;
}

// 测试 5: 失去多数派后的命令积压
static void TestCommandBacklogAfterLostQuorum() {
    std::cout << "Test 5: Command backlog after losing quorum" << std::endl;

    // 当失去多数派时，写请求会积压
    // 应该设置上限并拒绝新请求

    std::cout << "  Should enforce pending proposal limits" << std::endl;
    std::cout << "  Check: kMaxPending and kMaxPendingBytes" << std::endl;
}

// 测试 6: 断开连接时的待处理请求
static void TestPendingRequestsOnDisconnect() {
    std::cout << "Test 6: Pending requests when connection closes" << std::endl;

    // 客户端断开连接时，正在等待的写请求应该如何处理？
    // 当前：FailPending 会调用回调，但连接已关闭
    // 改进：使用弱引用或可取消的上下文

    std::cout << "  Should handle disconnected clients gracefully" << std::endl;
}

// 建议的改进方案示例
static void ExamplePerConnectionQueue() {
    std::cout << "\nExample: Per-connection command queue (not implemented)" << std::endl;

    std::cout << R"(
struct ConnectionState {
    std::queue<Command> pending_commands;
    bool executing = false;

    void EnqueueCommand(Command cmd) {
        pending_commands.push(cmd);
        if (!executing) ExecuteNext();
    }

    void ExecuteNext() {
        if (pending_commands.empty()) {
            executing = false;
            return;
        }
        executing = true;
        auto cmd = pending_commands.front();
        pending_commands.pop();

        if (cmd.type == READ) {
            // 立即执行并返回
            ExecuteRead(cmd);
            ExecuteNext();  // 继续下一个
        } else {
            // 异步等待提交
            ProposeWrite(cmd, [this](bool success, const std::string& result) {
                SendResponse(result);
                ExecuteNext();  // 完成后执行下一个
            });
        }
    }
};
)" << std::endl;
}

int main() {
    try {
        std::cout << "=== Connection Command Order Tests ===" << std::endl;
        std::cout << "NOTE: These tests document the current limitation" << std::endl;
        std::cout << "Same-connection command order is NOT guaranteed currently\n" << std::endl;

        TestSetThenGetOnSameConnection();
        TestSetThenPingOnSameConnection();
        TestMultipleSetsOnSameConnection();
        TestPipelineCommandOrder();
        TestCommandBacklogAfterLostQuorum();
        TestPendingRequestsOnDisconnect();

        ExamplePerConnectionQueue();

        std::cout << "\n=== Documentation tests completed (" << test_count << " checks) ===" << std::endl;
        std::cout << "\nRECOMMENDATION:" << std::endl;
        std::cout << "1. Document current limitation in README" << std::endl;
        std::cout << "2. Add per-connection queue as medium-term improvement" << std::endl;
        std::cout << "3. Implement connection-level backpressure" << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\n=== Test failed: " << e.what() << " ===" << std::endl;
        return 1;
    }
}
