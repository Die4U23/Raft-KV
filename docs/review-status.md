# 项目状态

对应仓库 `main`。历史实验的数字以各自报告的日期和提交为准，不能把旧套件的通过数写成当前测试规模。

## 现在有什么

固定成员的单 Raft 组 KV。写路径是 RESP → 每连接队列 → Leader 提案 → 多数派复制 → 顺序应用到 RocksDB。KV 与 `lastApplied` 在同一个 WriteBatch 里提交。

- 选举含 Pre-Vote：没有多数派预投票就不抬任期、不写 `votedFor`。
- Leader 在最短选举超时（150 ms）内没有多数派 AppendEntries 应答时卸任（CheckQuorum）。
- `--linearizable_reads=true` 时，Leader 的 GET 走 ReadIndex：只接受该读请求之后发出的探针 ACK，探针年龄达到 150 ms 即失败，并等到 `last_applied` 追上后再读。Follower 返回 `MOVED`。默认关闭，默认 GET 是本地读。
- `--leader_only_reads=true` 只检查本机角色，不是线性一致读。
- 连接数、输入输出、写队列和提案有上限，过载返回 `BUSY` 或直接关连接。
- 已提交 KV 批次默认交给串行工作线程；Raft 日志和硬状态仍在所有者线程上同步落盘。
- 快照：`--snapshot_threshold`（默认 1024，0 表示关闭）之后，已应用的日志前缀换成一份 KV 快照。落后副本的 `nextIndex` 落在快照里时，Leader 分块发送 `InstallSnapshot`，而不是逐条重放已丢掉的前缀。

## 现在没有什么

- 动态成员变更、多分片。
- 快照只覆盖已经应用的前缀。快照之后的日志仍会增长，重启时仍扫描这段后缀。没有增量传输 RocksDB 文件。
- `client_id + request_id` 去重。超时或丢回复后的重试可能执行两次。
- 租约读。线性一致读每次都要多数派往返。
- 整机掉电、介质损坏、静默丢包、非对称分区和长时间压测的已核验证据。进程内测试使用存储和网络替身，不能代替这些场景。

## 测试怎么分层

可移植 CTest（`RAFTKV_BUILD_SERVER=OFF`）当前有 16 个目标，包括 `protocol_tests`、`core_tests`、`kv_state_machine_tests`、`raft_log_tests`、`raft_node_coverage_tests`、`readindex_tests`、`snapshot_tests`、`replication_logic_tests`、`replication_partition_tests`、`replication_edge_cases_unit`、`connection_order_tests`、`storage_batch_tests`、`storage_failure_tests`、`async_executor_tests`、`batch_flush_tests`、`peer_retry_tests`。复制和 ReadIndex 目标链接生产 `RaftNode`。`replication_ack_tests.cpp` 仍是独立替身，不在 CTest 里。

带 Muduo/RocksDB 的构建另有 `peer_manager_transport_tests`。GitHub Actions `linux-cluster.yml` 会构建真实服务、跑冒烟，并跑 `tests/cluster_linearizable.py`（Follower `MOVED`、Leader 写后读、隔离旧 Leader 的 GET 必须失败或重定向）。

2026-09-08 到 09-13 的分区、写入重启、过载和性能包是更早提交上的归档。那些报告里的 CTest 5/5、7/7 是当时的目标数。ReadIndex 和 Pre-Vote 合入之后，这些故障包没有按新二进制重跑。

## 阅读顺序

行为变更看 [CHANGELOG](../CHANGELOG.md)。读语义看 [读一致性](read-consistency.md)。怎么测看 [tests/README.md](../tests/README.md)。某一次实测的数字只看 `docs/benchmarks/` 里标了日期的那一份。
