# 读一致性保证

## GET 命令行为

Raft-KV 提供多种读一致性级别，以平衡性能和正确性。本文档详细说明了当前实现的读取语义及其限制。

### 默认模式（Follower 读启用）

**行为：**
- GET 命令**立即**在本地状态机上执行
- 不经过 Raft 共识协议
- Follower 和 Leader 都可以处理读请求

**一致性保证：**
- ❌ **线性一致性**：可能返回过时数据
- ✅ **读己之写**（同连接）：GET 能看到同一连接上之前所有 SET 的结果
- ⚠️ **有界过期**：读取可能落后于最新提交

**可能返回过时数据的场景：**
1. **Leader 选举期间**：旧 Leader 可能已失去领导权但尚未意识到
2. **分区恢复后**：被隔离的节点重新加入集群时可能有过期数据
3. **应用延迟**：`commit_index > last_applied` 时，已提交但未应用的数据不可见
4. **Follower 落后**：Follower 节点可能落后于 Leader 多个操作

**适用场景：**
- 低延迟读取要求
- 可接受最终一致性的应用
- 读密集型工作负载
- 非关键数据查询

**示例：**
```bash
# 客户端 A 写入
redis-cli -p 8080 SET user:1 alice
# OK

# 客户端 B 从 Follower 读取（可能读到旧值或 nil）
redis-cli -p 8081 GET user:1
# 可能返回旧值、nil 或 "alice"
```

---

### Leader-Only 模式（--leader_only_reads=true）

**行为：**
- Follower 拒绝 GET 请求，返回 `-ERR MOVED <leader_id>`
- Leader 直接从其本地状态机读取
- 客户端需要重定向到 Leader

**一致性保证：**
- ❌ **线性一致性**：Leader 可能已失去领导权但尚未感知
- ✅ **读己之写**（同连接）：同一连接上的 GET 能看到之前的 SET
- ⚠️ **有界过期**：在一个心跳间隔内（50ms）

**不保证线性一致性的原因：**
- Leader 可能已被隔离但尚未超时
- Leader 可能在网络分区中失去多数派支持
- Leader 在处理读请求前不验证其领导权

**适用场景：**
- 需要更严格一致性保证的应用
- 可容忍重定向的客户端
- 写入和读取都集中在 Leader 的场景

**示例：**
```bash
# 在 Follower 上尝试读取
redis-cli -p 8081 GET user:1
# -ERR MOVED 0

# 客户端重定向到 Leader
redis-cli -p 8080 GET user:1
# "alice"
```

---

### 线性一致读（`--linearizable_reads=true`）

默认关闭。开启后，Leader 使用 ReadIndex：记录当时的 `commit_index`，向 peer 发送**请求之后新分配**的 AppendEntries，多数派按 `rpc_id` 确认，且 `last_applied >= read_index` 后再读本地 KV。Follower 返回 `-ERR MOVED <leader_id>`，不会静默降级为本地读。

**保证：**
- 读屏障只接纳该轮创建之后发出的探针 ACK；请求之前已在途的复制/心跳及其重试不能确认这次读。
- 探针 ACK 必须在最短选举超时（150 ms）内到达。年龄达到 150 ms 的同任期 ACK 不能确认读，并且到达时就失败该轮，不等下一次 Tick。Follower 的选举截止时间用同一把 `steady_clock`：刚收到心跳之后，至少要过 150 ms 才能给其他候选投票。`Tick` 提前触发不会把这段等待缩短。
- Leader 失去多数派应答超过 150 ms 后卸任（CheckQuorum）。未完成的读失败，而不是继续返回本地值。
- 多数派确认之后、读本地 KV 之前再检查一次仍是 Leader。已经卸任则 `MOVED`。
- 同一连接上 GET 仍排在前面的 SET/DEL 之后，保留读己之写。非法 RESP 帧若撞上尚未完成的命令，只关闭连接，不把帧错误插进那条回复。
- 卸任或 `Stop()` 会拒绝未完成的读；这些失败在 `--linearizable_reads=true` 时映射为 `MOVED`。

**不保证 / 已知边界：**
- 默认 `--linearizable_reads=false` 时 GET 仍是本地读。
- 应用落后与未发送队列的超时是 1000 ms；探针轮次本身在 150 ms 失败。队列深度上限 10000。
- 选举超时先走 Pre-Vote。得不到多数派预投票就不会抬任期，隔离节点不能靠连续竞选打断仍有多数派的 Leader。CheckQuorum 仍会让隔离旧 Leader 卸任；在此之前线性一致 GET 超时或 `MOVED`，不会成功返回过期值。刚听过心跳的 Follower 在选举截止时间之前不给预投票，也不给正式投票。进程内回归在 `readindex_tests` 和 `raft_node_coverage_tests`；Linux 三节点在 `tests/cluster_linearizable.py`。
- 共识检查日志写到 stderr（`INFO` / `WARNING` / `ERROR`），不进入 Raft 日志。
- 这不是租约读：时钟不同步或 RTT 接近选举超时会使 ReadIndex 失败，而不是放宽确认窗口。

**参考资料：**
- [Raft 论文第 8 节](https://raft.github.io/raft.pdf)
- [etcd API 保证](https://etcd.io/docs/v3.6/learning/api_guarantees/)

---

## 当前实现细节

### 读路径分析

1. **接收请求**：客户端通过 RESP 协议发送 GET 命令
2. **命令解析**：`main.cpp` 中的 `DrainClient` 解析命令并入每连接队列
3. **一致性分流**：
   - `--linearizable_reads=true`：非 Leader 返回 `MOVED`；Leader 调用 `RequestReadIndex`，多数派确认后再读
   - `--leader_only_reads=true`：非 Leader 返回 `MOVED`；Leader 直接读本地
   - 默认：任意角色直接读本地
4. **返回结果**：将结果通过 RESP 协议返回客户端

**关键代码位置：**
- 读取处理：`src/server/main.cpp`（`ExecuteNextCommand`）
- ReadIndex：`src/raft/raft_node.cc`（`RequestReadIndex` / `probe_rpc_ids`）
- 状态机读取：`src/raft/kv_state_machine.cc`

### 读己之写保证

**实现方式：**
- 每个 TCP 连接维护一个命令队列
- GET 命令在同连接的所有 pending SET 之后执行
- 保证因果一致性：客户端总能读到自己写入的数据

**代码位置：**
- 命令队列：`src/server/main.cpp`（连接级别的顺序保证）

---

## 使用建议

### 场景选择指南

| 场景 | 推荐模式 | 原因 |
|------|---------|------|
| 会话状态读取 | 默认模式 | 读己之写足够，低延迟 |
| 缓存查询 | 默认模式 | 可容忍短暂过期 |
| 关键业务决策 | Leader-Only | 更严格保证，重定向可接受 |
| 金融交易查询 | `--linearizable_reads=true` | Leader 上走 ReadIndex；Follower 返回 MOVED |
| 监控指标 | 默认模式 | 最终一致性即可 |

### 客户端最佳实践

1. **使用连接池时注意连接重用**
   - 同一连接上的操作保证有序
   - 不同连接的操作不保证顺序

2. **处理 MOVED 错误**
   ```python
   def read_with_redirect(key):
       response = redis_client.get(key)
       if response.startswith("-ERR MOVED"):
           leader_id = int(response.split()[-1])
           # 重定向到 Leader
           return read_from_leader(leader_id, key)
       return response
   ```

3. **需要强一致性时的变通方案**
   ```bash
   # 通过写入一个空操作强制同步
   SET _sync_token_{timestamp} ""
   # 现在读取是在这个同步点之后
   GET actual_key
   ```

---

## 配置选项

### --leader_only_reads

**默认值：** `false`

**启用方式：**
```bash
./raft_kv_server --leader_only_reads=true
```

**效果：**
- Follower 拒绝所有 GET 请求
- 返回 `-ERR MOVED <leader_id>`
- 客户端需要处理重定向逻辑

---

## 与其他系统对比

| 系统 | 默认读一致性 | 强一致性选项 |
|------|-------------|-------------|
| Raft-KV（当前） | 本地读（无保证） | Leader-Only（非线性） |
| etcd | 序列化读 | 线性一致读 |
| Redis Cluster | 本地读（最终一致） | 无（需要 WAIT 命令） |
| Consul | Stale 模式 | Consistent 模式 |

---

## 未来改进

### 短期（下个版本）

ReadIndex 与 Pre-Vote 已经在当前代码里。接下来是快照 / InstallSnapshot / 日志压缩，以及 `client_id + request_id` 去重。Lease read 仍未做：它需要时钟同步假设，用来省掉每次读取的多数派往返。

### 长期

1. **Follower Reads with Timestamps**
   - 允许 Follower 提供带时间戳的读取
   - 客户端指定可容忍的过期程度

2. **Read Quorum**
   - 从多数派读取以保证一致性
   - 用于读密集型工作负载

---

## 故障场景分析

### 场景 1：网络分区

**设置：**
- 3 节点集群：A、B、C
- Leader 是 A
- 网络分区：A 与 B、C 隔离

**默认模式下的行为：**
1. A 仍然认为自己是 Leader（心跳超时前）
2. 客户端从 A 读取可能得到过时数据
3. B、C 选举出新 Leader（比如 B）
4. 客户端从 B 读取得到最新数据
5. **不一致窗口**：在新 Leader 选出到旧 Leader 超时之间

**Leader-Only 模式下的行为：**
- 行为与默认模式相同
- Leader-Only **不能**防止分区场景的过期读

**缓解措施：**
- 减小选举超时时间（当前 150-300ms）
- 客户端侧读取验证（比如版本号）
- 等待线性一致读实现

### 场景 2：Leader 崩溃

**设置：**
- Leader A 崩溃
- B、C 选举新 Leader

**读取行为：**
1. 客户端连接到 A 失败
2. 客户端重连到 B 或 C
3. B、C 中的新 Leader 能提供最新数据
4. **恢复时间**：一个选举超时（150-300ms）

**数据可见性：**
- 所有已提交的写入在新 Leader 上可见
- 未提交的写入会丢失（符合 Raft 保证）

---

## 监控和调试

### INFO 命令

使用 `INFO` 命令检查节点状态：

```bash
redis-cli -p 8080 INFO
```

**关键字段：**
```
state:leader                # 节点角色
commit_index:42             # 已提交索引
last_applied:42             # 已应用索引
apply_lag:0                 # 应用延迟（commit - applied）
```

**诊断读一致性问题：**
- `apply_lag > 0`：本地读取可能缺少最近的提交
- `state:follower`：在 Leader-Only 模式下应该返回 MOVED
- `leader_id:-1`：正在选举，读取可能失败或返回过时数据

---

## 常见问题

### Q: 为什么 Leader-Only 不保证线性一致性？

A: 因为 Leader 在处理读请求时不验证自己是否仍然被多数派承认。一个被隔离的旧 Leader 可能在超时前继续提供过时数据。

### Q: 如何实现强一致性读取？

A: 启动时加上 `--linearizable_reads=true`。Leader 会走 ReadIndex（请求之后的探针 ACK 才计入多数派），Follower 返回 `MOVED`。默认模式和 `--leader_only_reads` 都不是线性一致读。

### Q: 同一连接的 SET 和 GET 顺序保证吗？

A: 是的。同一 TCP 连接上的命令按顺序执行。SET 完成后，同连接的 GET 一定能看到这个写入。

### Q: 不同连接的操作顺序如何？

A: 不保证。不同连接的操作并发执行，可能以任意顺序完成。

### Q: 读取的数据可能有多旧？

A: 在最坏情况下：
- 默认模式：无上界（取决于网络分区持续时间）
- Leader-Only：约 50ms（一个心跳间隔）
- `--linearizable_reads=true`：多数派确认后的已提交前缀（仍受应用延迟约束，要等 `last_applied >= read_index`）

---

## 参考文档

- [Raft 论文](https://raft.github.io/raft.pdf) - 第 8 节讨论了读取优化
- [并发处理说明](./concurrency.md) - 详细的请求处理流程
- [压测方法](./benchmark.md) - 性能测试和读写比例
- [etcd 一致性保证](https://etcd.io/docs/v3.6/learning/api_guarantees/) - 工业实现参考

---

## 更新日志

- 2026-09-21: 初始文档，描述当前读一致性行为和限制
