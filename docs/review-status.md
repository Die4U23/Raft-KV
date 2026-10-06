# 项目状态

对应仓库 `main`。历史实验的数字以各自报告的日期和提交为准，不能把旧套件的通过数写成当前测试规模。

## 现在有什么

固定成员的单 Raft 组 KV。写路径是 RESP → 每连接队列 → Leader 提案 → 多数派复制 → 顺序应用到 RocksDB。KV 与 `lastApplied` 在同一个 WriteBatch 里提交。

- 选举含 Pre-Vote：没有多数派预投票就不抬任期、不写 `votedFor`。
- Leader 在最短选举超时（150 ms）内没有多数派 AppendEntries 应答时卸任（CheckQuorum）。
- `--linearizable_reads=true` 时，Leader 的 GET 走 ReadIndex：只接受该读请求之后发出的探针 ACK，探针年龄达到 150 ms 即失败，并等到 `last_applied` 追上后再读。Follower 返回 `MOVED`。再加上 `--lease_reads=true` 时，本任期已满 300 ms 且多数派在 150 ms 内应答过，GET 直接用本地提交位置，不再发探针；租约不成立时仍走 ReadIndex。两个开关默认都关闭，默认 GET 是本地读。
- `--leader_only_reads=true` 只检查本机角色，不是线性一致读。
- 连接数、输入输出、写队列和提案有上限，过载返回 `BUSY` 或直接关连接。
- 已提交 KV 批次默认交给串行工作线程；Raft 日志和硬状态仍在所有者线程上同步落盘。
- 快照：`--snapshot_threshold`（默认 1024，0 表示关闭）之后，已应用的日志前缀换成一份 KV 快照。一次截断之后，已应用但还留在日志里的条数小于这个阈值；新条目再积累到阈值时再次截断。落后副本的 `nextIndex` 落在快照里时，Leader 分块发送 `InstallSnapshot`，而不是逐条重放已丢掉的前缀。副本在收到半段快照后重启，内存里的半段丢掉，Leader 从偏移 0 再传一遍；装完之后的新写入仍会复制到这个副本。副本拒绝 AppendEntries 时带回冲突任期的起点，Leader 的 `nextIndex` 一次跳到那里；该位置已在快照里时，下一轮就是 `InstallSnapshot`。重启时尾记录给出终点，再读出快照之后的每一条；中间条目损坏或断开会在打开时失败。没有尾记录的旧库仍扫描一次并补上尾记录。
- 幂等写：`IDEMP <client-id> <request-id> SET <key> <value>` 和 `IDEMP <client-id> <request-id> DEL <key>`。`request-id` 从 1 起，十进制，无前导 0。`client-id` 为 1 到 128 字节。同一个 id 再提交时返回第一次的回复，不改数据；更小的 id 返回 `-ERR stale request`。每个 client id 只保留最近一次，记录在 KV 里并随快照保留，不会过期。
- 已受理但仍未提交的写，超过 1000 ms 回调 `-ERR proposal timeout; outcome unknown`。日志条目不删除，之后仍可能提交并应用。已经提交、只是还没应用完的写不会因这个期限失败。失去多数派时仍由 CheckQuorum 更快卸任，回调是 leadership lost。
- `--metrics_port` 默认 0。打开后 `GET /metrics` 返回与 `INFO` 相同的数字，格式是 Prometheus 文本。角色、任期、提交和应用位置、应用积压、过载拒绝、阶段累计耗时都在里面。`namespace` 不在抓取结果里。没有直方图。同一端口的 `GET /health` 在进程仍在跑且存储没有失败时返回 200；Follower 也可以是 200。存储失败或进程已停止时返回 503。
- `docker/runtime.Dockerfile` 和 `docker/compose.yaml` 可以在一台机器上起三个容器，每个容器有独立数据卷。镜像构建不在 CI 里，也不是多机部署。Linux 冒烟会给每个节点打开 `--metrics_port`，并检查 `GET /health` 在选举后和旧 Leader 重启后都返回 200，且恰好一个节点是 leader。

## 现在没有什么

- 动态成员变更、多分片。
- 不按 RocksDB 文件做增量拷贝。落后副本收到的是整份 KV 快照，分块传输。
- 普通 `SET` / `DEL` 不去重。超时或丢回复后直接重试这两条命令仍可能执行两次。
- 租约读默认关闭。打开后也要等本任期满 300 ms，并且多数派联系变旧时退回 ReadIndex。它不让 Follower 读。
- 整机掉电、介质损坏和长时间压测的已核验证据。进程内测试使用存储和网络替身，不能代替这些场景。Linux 分区脚本会在 TCP 不断开时丢掉字节，包括两个 Follower 的回复都丢掉、只丢掉一个 Follower 的回复、只丢掉 Leader 发往一个或两个 Follower 的方向，以及在 Leader 链路都丢掉时再丢掉两个 Follower 之间的一个方向；也会把每一块按原顺序多等 40 ms，把同一次读里已经完整的相邻 Raft 帧对调，或只把旧 Leader 的链路按住 400 ms 直到多数派改选。单独一帧会马上转发。其余有向组合没有逐项验证。

## 测试怎么分层

可移植 CTest（`RAFTKV_BUILD_SERVER=OFF`）当前有 17 个目标，包括 `protocol_tests`、`core_tests`、`kv_state_machine_tests`、`raft_log_tests`、`raft_node_coverage_tests`、`readindex_tests`、`snapshot_tests`、`replication_logic_tests`、`replication_partition_tests`、`replication_edge_cases_unit`、`connection_order_tests`、`storage_batch_tests`、`storage_failure_tests`、`async_executor_tests`、`batch_flush_tests`、`peer_retry_tests`、`prometheus_text_tests`。复制和 ReadIndex 目标链接生产 `RaftNode`。`replication_ack_tests.cpp` 仍是独立替身，不在 CTest 里。

带 Muduo/RocksDB 的构建另有 `peer_manager_transport_tests`。GitHub Actions `linux-cluster.yml` 会构建真实服务并跑冒烟、`tests/cluster_linearizable.py`（Follower `MOVED`、Leader 写后读、隔离旧 Leader 的 GET 必须失败或重定向）、`tests/cluster_partition.py`、`tests/cluster_write_restart.py` 和 `tests/cluster_overload.py`。

2026-09-08 到 09-13 的分区、写入重启、过载和性能包是更早提交上的归档。那些报告里的 CTest 5/5、7/7 是当时的目标数。分区、写入重启和过载脚本会在当前 Linux CI 里用本次构建的服务再跑。2026-09-30 用当前构建在同一台 2 CPU 机器上对照了 `--async_apply`，数据目录在 ext4。12 个单元里有 1 个因 CheckQuorum 卸任出现 `MOVED`，报告状态是 FAIL。零错误单元的吞吐区间重叠，不能写成某一种模式更快。数字在 `docs/benchmarks/async-apply-compare-2026-09-30.json`。这组对照不在 CI 里。

## 阅读顺序

行为变更看 [CHANGELOG](../CHANGELOG.md)。读语义看 [读一致性](read-consistency.md)。怎么测看 [tests/README.md](../tests/README.md)。某一次实测的数字只看 `docs/benchmarks/` 里标了日期的那一份。
