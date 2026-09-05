# 并发写入与过载保护

当前实现合并单个 Raft 组的写入，并限制请求积压。`--async_apply=true` 默认把已提交的 KV 批次交给一个串行工作线程；`--async_apply=false` 保留在 EventLoop 中同步应用的基线。共识状态始终由 EventLoop 线程管理，下文称它为 owner 线程。真实 Linux 构建、Muduo 调度、TCP、fsync 与性能测量均为 **UNRUN（未运行）**，目前没有实际 QPS 或尾延迟结论。

## 写入路径

```mermaid
flowchart LR
    C[多个客户端连接] --> Q[有界写队列]
    Q --> B[最多 128 条或 1 MiB 命令]
    B --> L[Leader 日志批量同步写入]
    L --> F[Follower 日志批量同步写入]
    F --> M[多数派确认]
    M --> Q2[owner 按日志顺序提交一个应用批次]
    Q2 --> A[串行 worker 同步写入 KV 与 lastApplied]
    A --> O[owner 接收完成通知并更新应用进度]
    O --> S[按日志顺序完成客户端请求]
```

`--group_commit_ms=1` 默认给未满批队列一个收集期限。排队条数达到 128，或排队命令字节达到 1 MiB 时，立即排入 EventLoop 的待执行队列，不再等待期限；`0` 也走该路径，仍可能合并同轮到达的请求。参数范围为 0–10 ms，实际执行还受调度和磁盘延迟影响，“立即排队”不表示内联执行或已经落盘。

未满批的期限按当前队列最老条目的入队时刻计算，新增请求不重置窗口。处理一批后，剩余部分若已到期就立即排队，否则仅等待剩余时间。每次 flush 回调最多检查 128 个队列条目，失效或断连的条目也消耗该轮预算，避免大量清理占住 owner。升格为立即执行时取消旧定时器，并用代际 token 拒绝已过期列表中的旧回调及重复回调；即使由定时器到期触发，实际 flush 也先经 `queueInLoop` 排队，各批之间交回事件循环。单连接低负载时，收集时间可能增加延迟，应与吞吐一起测量。

Leader 的新日志批次使用一次同步 WriteBatch；Follower 合并同一 AppendEntries 中需要追加的日志。状态机将已提交日志按最多 128 条、约 2 MiB 命令数据分批应用，KV 变更和最终 lastApplied 标记同批写入。日志冲突截断可能另有一次同步写入。两种应用模式均保留 `sync=true`，异步指执行线程变化，不是关闭同步持久化。

状态机先解析全部命令再提交；批内 `SET k v / DEL k / DEL k` 依次返回 `OK / 1 / 0`。同步持久化失败不会返回成功。存储替身测试检查批量同步写接口的调用与批内语义，不能据此推算物理 fsync 次数或性能提升倍数。

异步路径只把已提交批次的 `KVStateMachine::ApplyBatch` 移至 worker。Raft 日志与硬状态的读写、准备应用批次时读取日志，以及客户端本地 `GET` 仍在 owner 上执行，仍可能阻塞心跳和网络处理。KV 应用内部为处理删除而做的读取随该批次在 worker 执行。

每个 RaftNode 同时最多有一个应用批次在途，包含工作线程运行期间和完成通知等待 owner 处理的期间。完成通知返回前不会提交下一个批次，正常运行时也不会提前释放对应的 pending 条数或字节配额。只有 KV 与 lastApplied 同步持久化成功，且 owner 处理完成通知后，客户端才可能收到成功回复。应用异常返回 owner 后触发停止服务，不能对落盘结果不确定的批次返回成功。

Follower 的成功复制回复仍要求对应 Raft 日志已同步持久化；异步模式下该回复不再等待本地 KV 应用完成。因此 `commit_index` 可以领先于 `last_applied`，读取 Follower 时也可能看到较旧的已应用状态。

每连接仍最多有一个等待结果的写请求，保证随后的 GET/SELECT 不越过前面的写。跨连接请求可合并落盘。尚未在单连接内展开多个未确认写请求，压测应分别改变连接数与 pipeline。

`RocksDBStore::LastApplied()` 使用原子读写发布存储端进度；Raft 自身的应用进度仍由 owner 在完成通知中更新。随项目提供的 RocksDB `db.h` 允许同一 DB 实例被多线程并发访问，因此 owner 的 GET 可以与串行 worker 的写入并行；这项线程安全约定不提供 ReadIndex 或线性一致读。

`RaftNode::Stop()` 会停止后续应用调度，并结束未决客户端请求。已经在执行的批次若成功，其完成通知仍记录该批全部已应用进度，避免再次应用；剩余已提交日志等待 `Start()` 后继续。RaftNode 析构会让晚到的完成回调失效；工作线程会完成已接收任务并被 join，然后才销毁状态机及其依赖。执行器的 `Stop()` 不会替 owner 执行排队中的完成回调。

## 容量与反馈

| 位置 | 限额 | 超限行为 |
|---|---|---|
| 客户端连接 | 默认 1024，`--max_clients` 可设 | 关闭新增连接 |
| 单条 RESP 命令 | 1 MiB，含协议与命名空间展开 | 拒绝超大命令 |
| 未消费输入 | 单连接 4 MiB，合计 64 MiB | 关闭继续超量输入的连接 |
| 客户端待发送输出 | 单连接 4 MiB，合计 64 MiB | 关闭慢客户端 |
| 等待批量提交的写队列 | 1024 条、16 MiB 命令 | 返回 `ERR BUSY` |
| Leader 未完成提案 | 1024 条、16 MiB 命令 | 整批拒绝，返回 `ERR BUSY` |
| 每个 Raft 对端待发送数据 | 4 MiB | 暂缓整个帧，等待重发机制再尝试 |

这些是应用层待处理字节上限，**不是进程 RSS 上限**。输出统计可能暂时保守高估尚未收到写完成通知的字节。较大的空输入/输出缓冲会回收；小缓冲、系统 socket 缓冲、对象开销和 RocksDB 缓存仍占内存。

每轮最多解析 128 条客户端命令，再交回事件循环。断连不撤销已复制的日志，客户端未收到回复时结果可能未知；目前仍没有请求去重。

## INFO 指标

- `connected_clients`：当前连接数。
- `queued_writes / queued_write_bytes`：收集队列中的请求。
- `pending_proposals / pending_proposal_bytes`：已追加日志但未完成应用的提案。
- `proposal_batches / apply_batches`：本进程的 Leader 提案批次与应用批次，含内部 no-op，重启清零。
- `async_apply`：是否启用串行 worker 应用，`1` 为启用、`0` 为同步基线。
- `apply_inflight`：是否有一个应用批次尚未完成 owner 通知处理，包含 worker 已结束但通知还在排队的阶段。
- `apply_lag`：`commit_index - last_applied`，表示已提交但尚未由 owner 确认应用完成的日志条数。
- `client_input_bytes / client_output_reserved_bytes`：输入积压和保守记录的输出积压。
- `overload_rejections`：容量不足导致的拒绝或关闭事件数。
- `flush_immediate_scheduled / flush_delayed_scheduled / flush_promotions`：立即排队、延时调度，以及已有延时被升格的次数。它们统计调度动作，不是请求数、成功写数或真实落盘批次数。
- `replication_retry_attempts`：未确认 AppendEntries 的重发尝试次数，可能包括空心跳；不代表重发成功或新增写入。

队列持续上升时，应检查服务速度是否跟得上输入；pending 高且提交不推进时，先检查多数派和网络；提交推进但 `apply_lag` 持续增大时，检查 KV 应用及完成通知调度；输出积压高时，检查对端是否及时读取。

阶段耗时使用以下前缀，每个前缀提供 `_count / _total_us / _max_us / _avg_us`：

| 指标前缀 | 一个样本及其边界 |
|---|---|
| `write_queue_wait` | 有效客户端条目从入队到被取出组装提案批次的等待时间；跳过失效客户端，但组批后仍可能因 Leader 或容量检查被拒绝，不等于成功写样本。 |
| `write_completed` | 从入队到 Raft 成功完成回调；提交后客户端即使已断连仍计入。它不含 socket 送达或客户端网络耗时，不是客户端端到端延迟。 |
| `local_read` | 一次本地 `Get` 的执行时间，包含 key 不存在的正常返回；不含抛错、重定向或回复发送。 |
| `leader_log_write` | 一次成功 Leader 日志 `AppendBatch` 的执行时间。 |
| `follower_log_write` | 一次成功 Follower 追加非空日志批次的执行时间；重复请求未追加新日志时不计样本。 |
| `kv_apply` | 一次成功 KV `ApplyBatch` 的执行时间；异步模式只测 worker 中的应用工作，并在 owner 收到完成通知后记入统计。 |
| `replication_data_ack` | 一次携带日志条目的 RPC 从首次发送尝试到匹配的成功确认，包含缓冲和重试；不计空心跳，不是纯网络 RTT，也不是多数派提交时间。 |
| `apply_dispatch` | 成功应用工作结束到 owner 收到完成通知的等待时间；同步模式也计应用批次样本，但时长为 0。 |

日志写与 KV 应用三个批次前缀还提供 `_entries / _bytes / _avg_entries`。字节数是批内命令字节，不是磁盘写入量或网络帧大小；内部 no-op 计一条日志，命令字节为 0。存储、复制及应用耗时仅计成功完成的样本，失败不会作为成功样本计入；不能仅凭这些指标判断错误率。

所有阶段指标自本进程启动累计，重启归零。耗时以整数微秒记录，两个平均值字段都用整数除法向下取整；无样本时平均值为 0。统计使用固定数量的计数器，不保留逐请求样本、直方图或分位数。阶段分别按请求、批次或 RPC 取样，样本集合不同，时间区间也可能重叠，不能把各阶段平均值相加当作请求总耗时。

同一进程未重启时，两次快照间的阶段均值为 `Δtotal_us / Δcount`，平均批大小为 `Δentries / Δcount`；分母为 0 时该区间没有样本。累计 `_max_us` 不能通过相减得到区间最大值。客户端 p50/p95/p99 仍需从压测工具获取。

## 验证边界

可移植测试分为五个目标。协议测试使用实际协议与缓冲代码；核心和存储批量测试通过进程内存储/网络替身检查批量顺序、配额、失败及恢复，并检查阶段指标的成功计数、批大小、整数均值及应用通知时机。核心异步场景使用手动执行器，控制应用与 owner 完成通知的先后。`async_executor_tests` 执行真实 `std::thread`，检查串行执行、owner 通知、异常传递及停止时 join；`batch_flush_tests` 使用实际调度策略和显式假事件队列，检查满批升格、旧 token 失效、不重置窗口、分轮处理及尾批期限，不调用 Muduo 定时器。

这些测试不验证真实 RocksDB fsync、TCP 缓冲、Muduo EventLoop 调度或掉电恢复。Linux 集成脚本包含并发连接的 SET/GET 检查，命令见 [测试说明](../tests/README.md)；同步与异步模式的公平比较见 [压测说明](benchmark.md)。真实 Linux 构建、Muduo 调度、TCP、fsync 和压测均尚未运行。

KV 应用移出 owner 后，Raft 日志同步写和本地 GET 仍可能占用 EventLoop；是否减少心跳延迟、任期抖动或请求尾延迟，需要在 Linux 实测。GET 仍是本地读，没有 ReadIndex；线程安全和吞吐结果都不能证明线性一致性。
