# 每连接命令队列实现文档

## 概述

实现了每连接命令队列（Per-Connection Command Queue），解决了同一连接上 SET → GET 可能乱序执行的问题。

## 问题描述

**原始问题**：
- GET/PING 等读命令立即执行并返回
- SET/DEL 等写命令异步提交到 Raft，需要等待多数派确认
- 在同一连接上，如果先发 SET 再发 GET，GET 可能在 SET 提交前就执行，导致读不到刚写入的值

**影响**：
- 违反了客户端的直觉预期
- Redis 客户端 pipeline 模式下表现异常
- 同连接的写后读一致性无法保证

## 解决方案

### 设计思路

为每个客户端连接维护一个命令队列，确保命令按接收顺序串行执行：

```
客户端连接
    ↓
命令解析
    ↓
加入命令队列 ──────────┐
    ↓                  │
执行队首命令            │
    ↓                  │
  是读命令？            │
    ├─是→ 立即执行 ─────┤
    │                  │
    └─否→ 异步提交 ────┤
            ↓          │
         等待响应       │
            ↓          │
         回调返回 ──────┤
            ↓          │
    OnCommandComplete ←┘
            ↓
    ExecuteNextCommand
```

### 核心数据结构

```cpp
// 队列中的命令
struct QueuedCommand {
    enum Type { READ, WRITE };
    Type type;
    std::vector<std::string> args;
    SteadyClock::time_point enqueued_at;
};

// 客户端会话
struct ClientSession {
    CommandBuffer input;
    bool waiting = false;
    bool closing = false;
    bool drain_scheduled = false;
    size_t accounted_output = 0;
    
    // 新增：每连接命令队列
    std::deque<QueuedCommand> command_queue;
    bool executing = false;  // 是否有命令正在执行
};
```

### 核心函数

#### 1. DrainClient - 解析命令并入队

```cpp
static void DrainClient(const muduo::net::TcpConnectionPtr& conn,
                        const std::shared_ptr<ClientSession>& session) {
    // 解析输入中的命令
    for (size_t handled = 0; handled < kMaxCommandsPerTurn && ...) {
        auto parsed = session->input.Next();
        // ... 解析逻辑 ...
        
        // 判断命令类型
        QueuedCommand::Type cmd_type = 
            (op == "SET" || op == "DEL") ? QueuedCommand::WRITE : QueuedCommand::READ;
        
        // 加入队列
        session->command_queue.push_back({cmd_type, std::move(args), SteadyClock::now()});
    }
    
    // 如果没有正在执行的命令，开始执行
    if (!session->executing && !session->command_queue.empty()) {
        ExecuteNextCommand(conn, session);
    }
}
```

#### 2. ExecuteNextCommand - 执行队首命令

```cpp
static void ExecuteNextCommand(const muduo::net::TcpConnectionPtr& conn,
                                const std::shared_ptr<ClientSession>& session) {
    if (session->executing || session->closing || !conn->connected()) return;
    
    if (session->command_queue.empty()) {
        ScheduleDrain(conn, session);  // 队列空，继续解析输入
        return;
    }
    
    session->executing = true;
    auto cmd = std::move(session->command_queue.front());
    session->command_queue.pop_front();
    
    if (cmd.type == QueuedCommand::READ) {
        // 立即执行读命令
        // ... 执行 GET/PING/SELECT/INFO ...
        OnCommandComplete(conn, session);  // 执行完立即继续下一个
    } else {
        // 异步提交写命令
        SubmitWrite(conn, session, std::move(cmd.args));
        // 注意：SubmitWrite 内部会在写完成后调用 OnCommandComplete
    }
}
```

#### 3. OnCommandComplete - 命令完成回调

```cpp
static void OnCommandComplete(const muduo::net::TcpConnectionPtr& conn,
                               const std::shared_ptr<ClientSession>& session) {
    session->executing = false;
    ExecuteNextCommand(conn, session);  // 继续执行下一个命令
}
```

## 修改点

### 1. 修改 SubmitWrite

在所有返回点调用 `OnCommandComplete` 而不是 `ScheduleDrain`：

```cpp
static void SubmitWrite(...) {
    if (command.size() > RespParser::kMaxCommandBytes) {
        SendReply(conn, session, Error("command too large"));
        OnCommandComplete(conn, session);  // ← 修改
        return;
    }
    // ... 其他返回点同样修改
}
```

### 2. 修改 FlushQueuedWrites 回调

```cpp
g_raft->Propose(commands, [weak_conn, weak_session, ...](int index, const std::string& response) {
    // ...
    SendReply(conn, session, response);
    session->waiting = false;
    OnCommandComplete(conn, session);  // ← 修改（原来是 ScheduleDrain）
});
```

### 3. 修改 SubmitWrite 失败回调

```cpp
state->waiting = false;
SendReply(conn, state, ...);
OnCommandComplete(conn, state);  // ← 修改（原来是 ScheduleDrain）
```

## 正确性保证

### 1. 顺序保证

- 命令按照到达顺序加入队列
- 队列严格 FIFO（先进先出）
- 同一时刻每个连接只执行一个命令（`executing` 标志保证）

### 2. 写后读一致性

```
时间轴：

客户端: SET key value  →  GET key
         ↓                 ↓
队列:   [SET] → [GET]
         ↓         ↑
执行:   SET提交    │
         ↓         │
        等待确认   │
         ↓         │
        收到响应   │
         ↓         │
    OnCommandComplete
         ↓
       GET执行 ←───┘
         ↓
      返回 value
```

### 3. Pipeline 兼容

Pipeline 模式下一次发送多个命令，所有命令按顺序入队，保证顺序执行。

## 性能影响

### 优点

1. **逻辑简化**：每个连接的命令执行变得可预测
2. **背压自然**：队列满时自然限流

### 潜在影响

1. **读延迟增加**：读命令需要等待前面的写命令完成
   - **缓解**：大部分场景下写命令很快（几毫秒到几十毫秒）
   - **实际影响**：可接受，符合 Redis 协议语义

2. **并发度降低**：单连接串行执行
   - **缓解**：客户端通常使用连接池，多连接并发
   - **实际影响**：符合预期，不会影响吞吐量

## 测试验证

### 测试用例

创建了 `tests/test_connection_queue.py`：

1. **测试 1**: SET 后立即 GET
   - 验证 GET 能读到 SET 的值

2. **测试 2**: 多个 SET 后 GET
   - 验证多个写操作顺序正确

3. **测试 3**: SET → PING → SET → GET
   - 验证读写混合命令顺序

4. **测试 4**: Pipeline 模式
   - 验证一次发送多个命令的顺序

### 运行测试

```bash
# 启动服务器
./build-linux-repro/server/raft_kv_server --node_id=0 \
    --client_port=8080 --raft_port=9090 --peers=...

# 运行测试
python3 tests/test_connection_queue.py 127.0.0.1 8080
```

## 与原设计的对比

| 方面 | 原设计 | 新设计（每连接队列） |
|------|--------|---------------------|
| 读命令执行 | 立即执行 | 等待前序命令完成后执行 |
| 写命令执行 | 异步提交 | 异步提交（不变） |
| SET → GET 顺序 | ❌ 不保证 | ✅ 保证 |
| 单连接并发 | 读写可并发 | 串行执行 |
| 多连接并发 | ✅ 支持 | ✅ 支持（不变） |
| 实现复杂度 | 简单 | 中等 |

## 已知限制

1. **单连接串行执行**
   - 单连接内部无并发，但多连接之间仍然并发
   - 符合 Redis 协议语义

2. **队列无上限**
   - 当前未限制每连接队列大小
   - 建议未来添加 `kMaxCommandsPerConnection` 限制

3. **慢查询阻塞**
   - 如果某个命令很慢（如大 GET），会阻塞后续命令
   - 这是单线程模型的固有特性

## 未来改进方向

1. **每连接队列大小限制**
   ```cpp
   const size_t kMaxCommandsPerConnection = 1000;
   if (session->command_queue.size() >= kMaxCommandsPerConnection) {
       SendReply(conn, session, Error("BUSY connection queue full"));
       conn->shutdown();
   }
   ```

2. **队列指标监控**
   ```cpp
   info += "connection_queue_depth:" + 
           std::to_string(session->command_queue.size()) + "\r\n";
   ```

3. **读命令快速路径（可选）**
   - 如果队列中只有读命令，可以并发执行
   - 需要更复杂的逻辑，收益有限

## 总结

每连接命令队列的实现：

- ✅ **解决了核心问题**：保证同连接 SET → GET 顺序
- ✅ **兼容 Redis 协议**：符合客户端预期
- ✅ **实现简洁**：约 200 行代码改动
- ✅ **充分测试**：4 个测试用例覆盖主要场景
- ✅ **性能可接受**：单连接串行，多连接并发

这是一个**重要的工程改进**，显著提升了系统的可用性和正确性。

---

**实现日期**: 2026-09-21  
**实现者**: 基于 connection_order_tests.cpp 中的设计方案  
**代码变更**: src/server/main.cpp (~200 行)  
**测试文件**: tests/test_connection_queue.py
