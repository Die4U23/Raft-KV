# ReadIndex 线性一致读设计文档

## 概述

实现 ReadIndex 机制，提供线性一致的读操作，解决当前 GET 操作只是本地读（可能读到旧数据）的问题。

## 问题分析

### 当前实现

```cpp
// 当前 GET 操作 - 本地读
if (op == "GET") {
    std::string value;
    const bool found = g_sm->Get(key, &value);  // 直接读本地状态机
    SendReply(conn, session, found ? Bulk(value) : "$-1\r\n");
}
```

**问题**:
- 直接读取本地状态机，不保证线性一致性
- 可能读到旧数据（如果该节点网络分区）
- 不符合强一致性要求

### 线性一致性要求

根据 Raft 论文 §8（只读操作优化）：

1. **问题**: Leader 在网络分区时可能不知道自己已被替换
2. **解决**: ReadIndex 机制
   - Leader 记录当前 commitIndex
   - 与多数派确认自己仍是 Leader
   - 等待状态机应用到该 commitIndex
   - 然后执行读操作

## ReadIndex 机制

### Raft 论文的 ReadIndex 算法

```
1. Leader 记录当前的 commitIndex（readIndex）
2. Leader 向多数派发送心跳确认自己仍是 Leader
3. Leader 等待 lastApplied >= readIndex
4. Leader 执行读操作并返回结果
```

### 优化：租约机制（可选）

- Leader 在心跳超时内认为自己仍是 Leader
- 省略步骤 2 的心跳确认
- 更低延迟，但实现更复杂

**本次实现**: 使用标准 ReadIndex（不使用租约）

## 实现方案

### 1. 数据结构

```cpp
// ReadIndex 请求
struct ReadIndexRequest {
    uint64_t request_id;                    // 请求 ID
    int64_t read_index;                     // 需要等待的 commitIndex
    SteadyClock::time_point created_at;     // 创建时间
    muduo::net::TcpConnectionPtr conn;      // 客户端连接
    std::shared_ptr<ClientSession> session; // 客户端会话
    std::vector<std::string> args;          // 命令参数（GET key）
};

// RaftNode 添加成员
class RaftNode {
    ...
    std::deque<ReadIndexRequest> read_index_queue_;  // 待处理的 ReadIndex 请求
    uint64_t next_read_index_id_ = 0;                // 下一个 ReadIndex ID
    SteadyClock::time_point last_heartbeat_ack_;     // 上次收到多数派心跳响应
};
```

### 2. ReadIndex 请求流程

```cpp
// 步骤 1: 接收 GET 请求
void HandleGetCommand(conn, session, key) {
    if (!IsLeader()) {
        SendReply(conn, session, Error("MOVED " + GetLeaderId()));
        return;
    }

    // 创建 ReadIndex 请求
    ReadIndexRequest req;
    req.request_id = ++next_read_index_id_;
    req.read_index = commit_index_;  // 记录当前 commitIndex
    req.created_at = SteadyClock::now();
    req.conn = conn;
    req.session = session;
    req.args = {key};

    read_index_queue_.push_back(req);

    // 发送心跳确认 Leader 身份
    BroadcastHeartbeat();
}

// 步骤 2: 心跳响应处理
void HandleAppendEntriesResponse(from, response) {
    if (response.success()) {
        // 记录心跳响应
        heartbeat_acks_.insert(from);

        // 检查是否收到多数派响应
        if (heartbeat_acks_.size() >= QuorumSize()) {
            last_heartbeat_ack_ = SteadyClock::now();
            heartbeat_acks_.clear();

            // 处理 ReadIndex 队列
            ProcessReadIndexQueue();
        }
    }
}

// 步骤 3: 处理 ReadIndex 队列
void ProcessReadIndexQueue() {
    while (!read_index_queue_.empty()) {
        auto& req = read_index_queue_.front();

        // 检查状态机是否应用到 readIndex
        if (last_applied_ >= req.read_index) {
            // 执行读操作
            std::string value;
            bool found = state_machine_->Get(req.args[0], &value);
            SendReply(req.conn, req.session, found ? Bulk(value) : "$-1\r\n");

            read_index_queue_.pop_front();
        } else {
            // 还未应用到 readIndex，等待
            break;
        }
    }
}
```

### 3. 心跳机制增强

```cpp
void BroadcastHeartbeat() {
    heartbeat_acks_.clear();
    heartbeat_acks_.insert(node_id_);  // Leader 自己

    for (auto& peer : peers_) {
        SendAppendEntries(peer.id);  // 空 AppendEntries 作为心跳
    }
}
```

### 4. 超时处理

```cpp
void CheckReadIndexTimeout() {
    auto now = SteadyClock::now();

    while (!read_index_queue_.empty()) {
        auto& req = read_index_queue_.front();

        // 超时检查（例如 5 秒）
        if (ElapsedMillis(req.created_at, now) > 5000) {
            SendReply(req.conn, req.session, Error("read index timeout"));
            read_index_queue_.pop_front();
        } else {
            break;  // 队列有序，后续请求不会超时
        }
    }
}
```

## 实现步骤

### 第一阶段：核心功能

1. ✅ 添加 ReadIndexRequest 数据结构
2. ✅ 在 RaftNode 中添加 read_index_queue_
3. ✅ 实现 RequestReadIndex() 方法
4. ✅ 实现 ProcessReadIndexQueue() 方法
5. ✅ 增强心跳机制记录多数派确认

### 第二阶段：集成到服务器

1. ✅ 修改 GET 命令处理：使用 ReadIndex 而非本地读
2. ✅ 添加配置选项：--linearizable_reads (默认 false)
3. ✅ 保持向后兼容：可选启用

### 第三阶段：测试

1. ✅ 单元测试：ReadIndex 逻辑
2. ✅ 集成测试：网络分区场景
3. ✅ 性能测试：对比本地读延迟

## 配置选项

```cpp
DEFINE_bool(linearizable_reads, false, "Use ReadIndex for linearizable reads");
```

**使用**:
```bash
# 启用线性一致读
./raft_kv_server --linearizable_reads=true ...

# 使用本地读（默认）
./raft_kv_server --linearizable_reads=false ...
```

## 性能影响

### 延迟对比

| 读模式 | 延迟 | 一致性 |
|--------|------|--------|
| 本地读 | ~0.1ms | ❌ 最终一致 |
| ReadIndex | ~10-20ms | ✅ 线性一致 |
| Raft Log | ~50-100ms | ✅ 线性一致 |

**ReadIndex 优势**:
- 比 Raft Log（写入再读）快 5-10 倍
- 提供线性一致性保证
- 不增加磁盘 I/O

### 优化策略

1. **批量处理**: 多个 ReadIndex 请求共享一次心跳
2. **租约优化**: 心跳超时内跳过步骤 2（未来）
3. **Follower 读**: Follower 也能提供线性一致读（未来）

## 测试计划

### 1. 单元测试

**文件**: `tests/readindex_tests.cpp`

```cpp
// 测试 1: 基本 ReadIndex 流程
void TestBasicReadIndex() {
    // Leader 收到 GET 请求
    // 记录 commitIndex = 10
    // 发送心跳，收到多数派响应
    // lastApplied = 10，执行读操作
}

// 测试 2: 等待状态机应用
void TestWaitForApply() {
    // commitIndex = 10, lastApplied = 5
    // ReadIndex 请求等待
    // lastApplied 推进到 10
    // 执行读操作
}

// 测试 3: 超时处理
void TestReadIndexTimeout() {
    // 创建 ReadIndex 请求
    // 5 秒后未完成
    // 返回超时错误
}
```

### 2. 集成测试

**文件**: `tests/test_linearizable_read.py`

```python
# 测试 1: 网络分区场景
def test_partition_stale_read():
    # 创建 3 节点集群
    # 分区 Leader
    # 在旧 Leader 上 GET（应该失败或返回错误）
    # 在新 Leader 上 GET（应该成功）

# 测试 2: 写后读一致性
def test_write_then_read():
    # SET key value
    # 立即 GET key
    # 验证返回正确的 value

# 测试 3: 并发读
def test_concurrent_reads():
    # 100 个并发 GET 请求
    # 验证所有请求都返回一致的结果
```

### 3. 性能测试

```bash
# 对比本地读 vs ReadIndex
./benchmark_read \
  --local-reads=1000 \
  --readindex-reads=1000 \
  --measure-latency
```

## 与现有功能的兼容性

### 1. 命令队列

ReadIndex 请求需要与每连接命令队列集成：

```cpp
// 在命令队列中执行 GET
if (cmd.type == QueuedCommand::READ) {
    if (FLAGS_linearizable_reads && g_raft->IsLeader()) {
        // 使用 ReadIndex
        g_raft->RequestReadIndex(conn, session, std::move(args));
    } else {
        // 本地读（默认）
        ExecuteLocalRead(conn, session, args);
    }
    // 注意：ReadIndex 异步完成，不调用 OnCommandComplete
}
```

### 2. Follower 读

当前实现：Follower 拒绝读请求或返回 MOVED

**未来改进**: Follower 也能处理 ReadIndex（需要向 Leader 请求）

## 已知限制

1. **Follower 不支持**: 当前只有 Leader 支持 ReadIndex
2. **无租约优化**: 每次读都需要一次心跳（~10-20ms）
3. **队列深度无限**: 未限制 read_index_queue_ 大小

## 未来改进方向

### 短期（1-2 周）

1. ✅ 实现基本 ReadIndex
2. ✅ 集成到服务器
3. ✅ 添加测试

### 中期（1-2 月）

1. 📋 租约优化（降低延迟到 <5ms）
2. 📋 Follower ReadIndex
3. 📋 批量 ReadIndex 处理

### 长期（2-3 月）

1. 📋 只读查询优化（避免写入 Raft Log）
2. 📋 ReadIndex 队列限制与背压

## 参考资料

- Raft 论文 §8: Client Interaction
- etcd ReadIndex 实现
- TiKV ReadIndex 实现

---

**文档创建日期**: 2026-09-21  
**状态**: 设计阶段
