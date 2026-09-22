# ReadIndex 线性一致读设计文档（修订版）

## 概述

实现 ReadIndex 机制，提供线性一致的读操作，解决当前 GET 操作只是本地读（可能读到旧数据）的问题。

## 问题分析

### 当前实现的问题

```cpp
// 当前 GET 操作 - 本地读
if (op == "GET") {
    std::string value;
    const bool found = g_sm->Get(key, &value);  // 直接读本地状态机
    SendReply(conn, session, found ? Bulk(value) : "$-1\r\n");
}
```

**问题**:
- ❌ 直接读取本地状态机，不保证线性一致性
- ❌ 可能读到旧数据（网络分区时）
- ❌ 不符合强一致性要求

### 线性一致性要求

根据 Raft 论文 §8（只读操作优化）：

**问题**: Leader 在网络分区时可能不知道自己已被替换

**解决**: ReadIndex 机制
1. Leader 必须先提交至少一条当前 term 的日志（no-op）
2. Leader 记录当前 commitIndex 作为 readIndex
3. Leader 向多数派发送心跳确认自己仍是 Leader（**心跳必须在请求到达后发出**）
4. Leader 等待 lastApplied >= readIndex
5. Leader 执行读操作并返回结果

## 正确性关键点

### 1. No-op 日志前置条件 ⚠️

**Raft §8 要求**: 新 Leader 上任后必须先提交一条本 term 的 no-op。

**原因**: 新 Leader 只知道自己 log 里有哪些条目，但不知道前任 term 的哪些条目已被提交。此时它的 commitIndex 可能偏小，导致读到旧值。

**实现**:
```cpp
// Leader 上任时
void OnBecomeLeader() {
    // 追加 no-op 日志
    AppendNoOpEntry();
    can_serve_read_ = false;  // 标记不能处理读请求
}

// 当 no-op 提交后
void OnCommitNoOp() {
    can_serve_read_ = true;  // 现在可以处理读请求
}

// 处理 ReadIndex 请求时
Status RequestReadIndex(ReadIndexCallback cb) {
    if (!can_serve_read_) {
        return Status::NotReady("waiting for leader to commit no-op");
    }
    // ... 继续处理
}
```

### 2. 心跳 ack 与请求绑定 ⚠️

**问题**: 全局 heartbeat_acks_ 无法保证时序正确性

**错误示例**:
```
t0: 发出心跳 H1
t1: 收到 GET 请求，入队
t2: H1 的多数派响应到齐 → 错误地处理 t1 的请求
```

H1 的响应是在 t1 之前发出的，不能证明 t1 时刻 Leader 身份。

**正确做法** (参考 etcd `raft/read_only.go`):

```cpp
struct HeartbeatRound {
    uint64_t round_id;                      // 心跳轮次 ID
    std::set<int> acks;                     // 收到的 ack
    std::vector<ReadIndexRequest> requests; // 该轮绑定的请求
    SteadyClock::time_point sent_at;       // 发送时间
};

class RaftNode {
    uint64_t next_round_id_ = 0;
    std::deque<HeartbeatRound> heartbeat_rounds_;  // 按 round_id 排序
    bool heartbeat_in_flight_ = false;
};

// 请求到达时
void RequestReadIndex(ReadIndexCallback cb) {
    if (!can_serve_read_) {
        cb(Status::NotReady(...), -1);
        return;
    }

    ReadIndexRequest req;
    req.read_index = commit_index_;
    req.callback = cb;
    req.created_at = SteadyClock::now();

    // 如果没有 in-flight 的心跳，发起新一轮
    if (!heartbeat_in_flight_) {
        StartHeartbeatRound();
    }

    // 将请求绑定到当前轮（最后一个）
    heartbeat_rounds_.back().requests.push_back(req);
}

// 发起新一轮心跳
void StartHeartbeatRound() {
    HeartbeatRound round;
    round.round_id = ++next_round_id_;
    round.acks.insert(node_id_);  // Leader 自己
    round.sent_at = SteadyClock::now();

    heartbeat_rounds_.push_back(round);
    heartbeat_in_flight_ = true;

    // 发送心跳（带 round_id 上下文）
    for (auto& peer : peers_) {
        SendAppendEntries(peer.id, round.round_id);
    }
}

// 处理心跳响应
void HandleAppendEntriesResponse(int from, uint64_t round_id, bool success, int64_t term) {
    // 检查 term
    if (term != current_term_) {
        return;  // 旧 term 的响应，丢弃
    }

    // 找到对应的 round
    auto it = std::find_if(heartbeat_rounds_.begin(), heartbeat_rounds_.end(),
        [round_id](const HeartbeatRound& r) { return r.round_id == round_id; });

    if (it == heartbeat_rounds_.end()) {
        return;  // 过期的 round
    }

    // success=false 也计数（follower 承认 Leader 身份）
    it->acks.insert(from);

    // 检查是否达到多数派
    if (it->acks.size() >= QuorumSize()) {
        ProcessConfirmedRound(*it);
        heartbeat_rounds_.erase(heartbeat_rounds_.begin(), it + 1);  // 删除已确认的 round

        if (heartbeat_rounds_.empty()) {
            heartbeat_in_flight_ = false;
        }
    }
}

// 处理已确认的 round
void ProcessConfirmedRound(const HeartbeatRound& round) {
    for (auto& req : round.requests) {
        // 检查是否已应用
        if (last_applied_ >= req.read_index) {
            // 立即执行
            req.callback(Status::OK(), req.read_index);
        } else {
            // 加入等待队列
            pending_reads_.push_back(req);
        }
    }
}
```

### 3. LastApplied 推进时触发队列处理 ⚠️

**问题**: 只在心跳多数派时调用 ProcessReadIndexQueue，可能导致请求卡住

**解决**: 在 apply 路径也调用

```cpp
void OnApplied(int64_t index) {
    last_applied_ = index;

    // 处理等待的读请求
    ProcessPendingReads();
}

void ProcessPendingReads() {
    while (!pending_reads_.empty()) {
        auto& req = pending_reads_.front();

        if (last_applied_ >= req.read_index) {
            req.callback(Status::OK(), req.read_index);
            pending_reads_.pop_front();
        } else {
            break;  // 队列有序，后续请求也未就绪
        }
    }
}
```

### 4. Leader 卸任时清空队列 ⚠️

**问题**: step down 时必须拒绝所有 pending 请求

```cpp
void StepDown(int64_t new_term) {
    state_ = State::Follower;
    current_term_ = new_term;
    can_serve_read_ = false;

    // 清空心跳轮次
    for (auto& round : heartbeat_rounds_) {
        for (auto& req : round.requests) {
            req.callback(Status::NotLeader("stepped down"), -1);
        }
    }
    heartbeat_rounds_.clear();
    heartbeat_in_flight_ = false;

    // 清空等待队列
    for (auto& req : pending_reads_) {
        req.callback(Status::NotLeader("stepped down"), -1);
    }
    pending_reads_.clear();
}
```

## 架构设计

### 分层原则

**Raft 层**: 只负责返回安全的 read_index

**Server 层**: 负责等待 apply 并执行具体命令

```cpp
// Raft 层接口
using ReadIndexCallback = std::function<void(Status status, int64_t read_index)>;

class RaftNode {
public:
    // 请求一个安全的 read_index
    void RequestReadIndex(ReadIndexCallback cb);

    // 获取当前已应用的索引
    int64_t GetLastApplied() const { return last_applied_; }
};

// Server 层使用
void HandleGetCommand(conn, session, key) {
    if (FLAGS_linearizable_reads) {
        g_raft->RequestReadIndex([conn, session, key](Status status, int64_t read_index) {
            if (!status.ok()) {
                SendReply(conn, session, Error(status.message()));
                OnCommandComplete(conn, session);
                return;
            }

            // 等待状态机应用到 read_index
            WaitForApply(read_index, [conn, session, key]() {
                // 执行读操作
                std::string value;
                bool found = g_sm->Get(key, &value);
                SendReply(conn, session, found ? Bulk(value) : "$-1\r\n");
                OnCommandComplete(conn, session);
            });
        });
    } else {
        // 本地读（默认）
        ExecuteLocalRead(conn, session, key);
        OnCommandComplete(conn, session);
    }
}
```

### 心跳合并策略

**关键**: 避免每个 GET 都发心跳，造成心跳风暴

**实现**:
```cpp
// 请求到达时
void RequestReadIndex(ReadIndexCallback cb) {
    // 1. 创建请求
    ReadIndexRequest req;
    req.read_index = commit_index_;
    req.callback = cb;

    // 2. 如果没有 in-flight 的心跳，立即发起新一轮
    if (!heartbeat_in_flight_) {
        StartHeartbeatRound();
    }

    // 3. 将请求挂到当前轮（多个请求共享一轮心跳）
    heartbeat_rounds_.back().requests.push_back(req);
}
```

**效果**: 同一 tick 到达的多个 GET 请求共享一轮心跳

## 数据结构

```cpp
// ReadIndex 请求
struct ReadIndexRequest {
    int64_t read_index;                     // 需要等待的 commitIndex
    ReadIndexCallback callback;             // 回调函数
    SteadyClock::time_point created_at;     // 创建时间
};

// 心跳轮次
struct HeartbeatRound {
    uint64_t round_id;                      // 轮次 ID
    std::set<int> acks;                     // 收到的 ack（包含 Leader 自己）
    std::vector<ReadIndexRequest> requests; // 该轮绑定的请求
    SteadyClock::time_point sent_at;       // 发送时间
};

// RaftNode 添加成员
class RaftNode {
private:
    // ReadIndex 相关
    bool can_serve_read_ = false;                   // 是否可以处理读请求
    uint64_t next_round_id_ = 0;                    // 下一个 round ID
    std::deque<HeartbeatRound> heartbeat_rounds_;   // 心跳轮次队列
    bool heartbeat_in_flight_ = false;              // 是否有 in-flight 心跳
    std::deque<ReadIndexRequest> pending_reads_;    // 等待 apply 的请求
    static constexpr size_t kMaxPendingReads = 10000;  // 队列上限
};
```

## 超时处理

```cpp
// 定时检查（每 100ms 调用一次）
void CheckReadIndexTimeout() {
    auto now = SteadyClock::now();
    const auto timeout = std::chrono::milliseconds(FLAGS_read_index_timeout_ms);  // 默认 1000ms

    // 1. 检查心跳轮次超时
    while (!heartbeat_rounds_.empty()) {
        auto& round = heartbeat_rounds_.front();

        if (now - round.sent_at > timeout) {
            // 超时，拒绝该轮所有请求
            for (auto& req : round.requests) {
                req.callback(Status::Timeout("read index timeout"), -1);
            }
            heartbeat_rounds_.pop_front();
        } else {
            break;  // 队列有序
        }
    }

    // 2. 检查等待 apply 的请求超时
    while (!pending_reads_.empty()) {
        auto& req = pending_reads_.front();

        if (now - req.created_at > timeout) {
            req.callback(Status::Timeout("apply timeout"), -1);
            pending_reads_.pop_front();
        } else {
            break;
        }
    }
}
```

**超时值**: 1 个选举超时（~1000ms），超过这个时间未拿到多数派确认通常意味着网络分区。

## 队列深度限制

```cpp
void RequestReadIndex(ReadIndexCallback cb) {
    // 1. 检查队列深度
    size_t total_pending = 0;
    for (auto& round : heartbeat_rounds_) {
        total_pending += round.requests.size();
    }
    total_pending += pending_reads_.size();

    if (total_pending >= kMaxPendingReads) {
        cb(Status::Overload("read index queue full"), -1);
        return;
    }

    // 2. 继续处理...
}
```

## 连接生命周期管理

**问题**: 客户端断连后，队列里的请求持有连接引用

**解决**: Server 层使用 weak_ptr

```cpp
// Server 层
void HandleGetCommand(conn, session, key) {
    std::weak_ptr<muduo::net::TcpConnection> weak_conn = conn;
    std::weak_ptr<ClientSession> weak_session = session;

    g_raft->RequestReadIndex([weak_conn, weak_session, key](Status status, int64_t read_index) {
        auto conn = weak_conn.lock();
        auto session = weak_session.lock();

        if (!conn || !session || !conn->connected()) {
            return;  // 连接已断开，直接返回
        }

        // 继续处理...
    });
}
```

## 只读命令白名单

```cpp
// Server 层
static const std::set<std::string> kReadOnlyCommands = {
    "GET", "EXISTS", "STRLEN", "MGET", "INFO", "PING"
};

void ExecuteReadOnlyCommand(const std::vector<std::string>& args) {
    const auto& op = args[0];

    if (op == "GET") {
        // ... 执行 GET
    } else if (op == "EXISTS") {
        // ... 执行 EXISTS
    }
    // ... 其他只读命令
}
```

## 与每连接命令队列集成

```cpp
// ExecuteNextCommand 中
if (cmd.type == QueuedCommand::READ) {
    if (FLAGS_linearizable_reads && g_raft->IsLeader()) {
        // 使用 ReadIndex
        g_raft->RequestReadIndex([conn, session, cmd](Status status, int64_t read_index) {
            if (!status.ok()) {
                SendReply(conn, session, Error(status.message()));
                OnCommandComplete(conn, session);  // 继续下一个命令
                return;
            }

            // 等待 apply
            WaitForApply(read_index, [conn, session, cmd]() {
                ExecuteReadOnlyCommand(conn, session, cmd.args);
                OnCommandComplete(conn, session);  // 继续下一个命令
            });
        });
    } else {
        // 本地读
        ExecuteLocalRead(conn, session, cmd.args);
        OnCommandComplete(conn, session);
    }
}
```

**关键**: 在 ReadIndex 回调里调用 OnCommandComplete，保证命令队列继续执行。

## 非 Leader 处理

```cpp
void RequestReadIndex(ReadIndexCallback cb) {
    if (!IsLeader()) {
        if (leader_id_ == -1) {
            // 选举期间，Leader 未知
            cb(Status::NoLeader("election in progress"), -1);
        } else {
            // 重定向到 Leader
            cb(Status::NotLeader("redirect to " + std::to_string(leader_id_)), -1);
        }
        return;
    }

    // ... 继续处理
}
```

**Server 层**:
```cpp
if (status.code() == StatusCode::NotLeader) {
    SendReply(conn, session, Error("MOVED " + ExtractLeaderId(status.message())));
} else if (status.code() == StatusCode::NoLeader) {
    SendReply(conn, session, Error("CLUSTERDOWN election in progress"));
}
```

## 租约读说明

**为什么不实现租约读**:

租约读依赖于时钟漂移上界（clock drift bound）。如果时钟漂移超过上界，可能破坏线性一致性（旧 Leader 的租约未过期，但新 Leader 已当选）。

这是**正确性风险**，不仅仅是实现复杂度问题。

## 可观测性

需要暴露以下指标：

```cpp
// RaftNode 添加
struct ReadIndexMetrics {
    uint64_t total_requests = 0;         // 总请求数
    uint64_t succeeded = 0;               // 成功数
    uint64_t timeout = 0;                 // 超时数
    uint64_t not_leader = 0;              // 非 Leader 拒绝数
    uint64_t overload = 0;                // 过载拒绝数
    uint64_t pending_count = 0;           // 当前 pending 数
    Histogram wait_duration_us;           // 等待时长分布
    uint64_t heartbeat_rounds_per_sec = 0;  // 每秒心跳轮次
};
```

**INFO 命令输出**:
```
read_index_requests:1000
read_index_succeeded:980
read_index_timeout:10
read_index_not_leader:10
read_index_pending:5
read_index_p50_us:5000
read_index_p99_us:15000
heartbeat_rounds_per_sec:10
```

## 与快照的交互

**场景**: InstallSnapshot 后 last_applied_ 跳变

**处理**: pending_reads_ 中的请求如果 read_index < snapshot_last_included_index，则已被快照覆盖，可以安全执行。

```cpp
void OnInstallSnapshot(int64_t last_included_index) {
    last_applied_ = last_included_index;

    // 处理等待的读请求
    ProcessPendingReads();
}
```

## 实现步骤

### 第一阶段：Raft 层核心功能（2-3 天）

1. [ ] 添加 ReadIndexRequest, HeartbeatRound 数据结构
2. [ ] 实现 RequestReadIndex() 方法
3. [ ] 实现 StartHeartbeatRound() 心跳轮次管理
4. [ ] 实现 HandleAppendEntriesResponse() 中的 ack 统计
5. [ ] 实现 ProcessPendingReads() 方法
6. [ ] 实现 StepDown() 清空队列
7. [ ] 实现 CheckReadIndexTimeout() 超时处理
8. [ ] 实现 no-op 日志与 can_serve_read_ 标志

### 第二阶段：Server 层集成（1-2 天）

1. [ ] 添加 --linearizable_reads 配置选项
2. [ ] 实现 WaitForApply() 辅助函数
3. [ ] 修改 GET 命令处理使用 ReadIndex
4. [ ] 实现只读命令白名单
5. [ ] 集成到每连接命令队列
6. [ ] 实现 weak_ptr 连接管理

### 第三阶段：测试（2-3 天）

1. [ ] 单元测试：no-op 前置条件
2. [ ] 单元测试：ack 与请求绑定（round_id）
3. [ ] 单元测试：step down 清队列
4. [ ] 单元测试：apply 路径唤醒
5. [ ] 单元测试：客户端断连
6. [ ] 集成测试：网络分区场景
7. [ ] 集成测试：高并发读心跳合并
8. [ ] 性能测试：延迟对比

### 第四阶段：可观测性（1 天）

1. [ ] 添加 ReadIndexMetrics 结构
2. [ ] 暴露 INFO 命令输出
3. [ ] 添加日志记录

**预计总工作量**: 6-9 天

## 测试计划

### 1. 单元测试 (tests/readindex_tests.cpp)

| 测试 | 场景 | 验证点 |
|------|------|--------|
| Test 1 | No-op 前置条件 | 未提交 no-op 时拒绝读请求 |
| Test 2 | 心跳 round_id 绑定 | 旧 round 的响应不计数 |
| Test 3 | 旧 term 响应 | term != current_term 的响应丢弃 |
| Test 4 | success=false 也计数 | follower 日志不匹配时仍计数 |
| Test 5 | Step down 清队列 | 卸任时所有 pending 请求被拒绝 |
| Test 6 | Apply 路径唤醒 | last_applied 推进触发请求完成 |
| Test 7 | 客户端断连 | 不 crash、不泄漏 |
| Test 8 | 心跳合并 | 多个请求共享一轮心跳 |
| Test 9 | 队列深度限制 | 超过上限返回 Overload |
| Test 10 | 超时处理 | 超时请求被拒绝 |

### 2. 集成测试 (tests/test_linearizable_read.py)

| 测试 | 场景 | 验证点 |
|------|------|--------|
| Test 1 | 网络分区旧 Leader | 旧 Leader 超时报错，不返回旧值 |
| Test 2 | 写后读一致性 | SET 后立即 GET 返回正确值 |
| Test 3 | 并发读 | 100 个并发 GET 都返回一致结果 |
| Test 4 | 高频读心跳合并 | 1000 个 GET/s，心跳 << 1000/s |
| Test 5 | Leader 切换 | 新 Leader 上的读不能看到未提交的写 |

### 3. 线性一致性验证（可选）

使用 Porcupine 或类似工具验证：
- 记录所有操作的 invoke/response 时间
- 验证是否存在合法的线性化顺序

## 性能预期

| 指标 | 本地读 | ReadIndex | Raft Log |
|------|--------|-----------|----------|
| 延迟 | ~0.1ms | ~1-2ms (局域网) | ~50-100ms |
| 吞吐 | 100k ops/s | 50k ops/s | 1k ops/s |
| 一致性 | ❌ 最终一致 | ✅ 线性一致 | ✅ 线性一致 |

**注意**: 延迟取决于网络 RTT。局域网 <1ms，跨机房可能 10-20ms。

## 配置选项

```cpp
DEFINE_bool(linearizable_reads, false, "Use ReadIndex for linearizable reads");
DEFINE_int32(read_index_timeout_ms, 1000, "ReadIndex timeout in milliseconds");
DEFINE_int32(max_pending_read_index, 10000, "Max pending ReadIndex requests");
```

## 已知限制

1. **Follower 不支持**: 当前只有 Leader 支持 ReadIndex
2. **无租约优化**: 每次读都需要一次心跳确认
3. **需要 no-op**: Leader 上任后需要先提交 no-op

## 未来改进方向

### 短期（1-2 周）
- ✅ 基本 ReadIndex 实现

### 中期（1-2 月）
- 📋 Follower ReadIndex（向 Leader 请求）
- 📋 批量优化（定期心跳，多个请求共享）

### 长期（2-3 月）
- 📋 租约读（需要时钟同步保证）
- 📋 ReadIndex 与 Raft Log 读混合优化

## 参考资料

- Raft 论文 §8: Client Interaction
- etcd `raft/read_only.go` 实现
- TiKV ReadIndex 实现
- [线性一致性验证工具 Porcupine](https://github.com/anishathalye/porcupine)

---

**文档创建日期**: 2026-09-21  
**最后更新**: 2026-09-21（修订版）  
**状态**: 设计完成，待实现
