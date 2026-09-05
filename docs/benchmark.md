# 并发压测客户端

`tests/load_benchmark.py` 使用 Python 3.8+ 标准库 asyncio。它连接明确指定的服务端；不会启动、停止或发现其他进程，也不会把 `leader_id` 猜成端口。请在专用测试集群运行，先通过 `INFO` 确认实际 Leader 的客户端端口。

```sh
python3 tests/load_benchmark.py --self-test
python3 tests/load_benchmark.py --host 127.0.0.1 --port 8080 --connections 32 --requests 10000 --pipeline 1 --value-size 128 --write-ratio 0.5 --timeout 5 --namespace bench --output result.json
```

`--self-test` 校验 RESP 解析、编码、分位数、采样及模拟多连接请求计数，不连接真实服务端。**当前真实 Linux 构建、Muduo 调度、TCP、fsync 和 Raft 性能测量均为 UNRUN（未运行），没有可报告的 Raft 吞吐或尾延迟数字。**

## 测量范围

`--requests` 是所有连接合计的测量操作数，不是每连接请求数。有效连接数为 `min(connections, requests)`。客户端先检查目标的 `INFO state:leader`；即使 `--write-ratio 0`，预填充阶段仍需要写入，所以目标也必须是 Leader。目标不是 Leader 时会报告其 `leader_id` 并退出，须使用真实地址重新运行。

每个持久 TCP 连接都会先执行 `SELECT`，再预填充自己的 key；key 包含本次运行 UUID 和 worker 编号。连接建立、命名空间选择和初始 `SET` 均在计时之外，任何预填充错误都会取消测量。测量中各 worker 只访问自己的 key，所有 `SET` 使用同一已知值，所有 `GET` 都检查返回值。测试结束保留这些专用 key，不执行可能影响其他数据的清理。该工作负载是小工作集热点测试，不能代表大数据集随机访问或读写同一个热点 key 的竞争。

`--write-ratio` 在固定顺序中分散写入，例如 `0.5` 交替安排读写。每连接一次发出最多 `--pipeline` 条命令，按 RESP 回复边界读取全部回复后再发下一批。这是有界、闭环负载；服务变慢时客户端自然减速，不能替代恒定到达率的过载测试。

输出指标：

- `attempted`：进入测量批次的逻辑操作数，包括因连接恢复失败而没有实际送达的操作。`success + errors = attempted`；正常完成时等于 `requested`。
- `success`：`SET` 收到 `OK`，或 `GET` 返回预期值的操作数。`goodput_ops_per_second = success / elapsed_seconds`，错误不计入有效吞吐。
- `errors_by_category`：`moved`、`overload`、`other`、`timeout`、`connection`。已收到的部分回复会保留计数；批次剩余未确认操作按超时或连接失败计错，即使它们可能已在服务端执行。
- `batch_latency_ms`：从提交整批发送到收到最后一条回复的 p50 / p95 / p99，包含事件循环调度、网络及服务端处理。`pipeline > 1` 时它是**整批延迟，不是每操作延迟**；不能除以 pipeline 当作真实单请求延迟。
- 延迟只采集收到全部回复的批次，包括完整返回错误的批次；超时和断连批次没有“最后回复”而不进入分位数。须同时阅读错误率，不能只看延迟。
- 最多保存 100,000 个延迟样本，超过时使用固定种子的蓄水池抽样；`completed_batches` 与 `latency_sample_count` 显示总体和样本数。

超时或协议失步后会关闭该连接，下一批重新连接并 `SELECT`，不自动跟随 `MOVED`。重连时间计入总体时间，预填充不重复执行。`--timeout` 限制每次连接准备或请求批次，连接关闭最多等待 1 秒；总请求数、连接数、pipeline 和 value 大小均有上限，同时按 `min(requests, connections × pipeline) × (value_size + 256)` 限制单轮并发载荷估算不超过 128 MiB，此值不是进程总内存上限。它没有固定整轮时间上限，长期故障下多批次超时会延长运行。退出码 `0` 表示测量完成且无错误；有测量错误或准备失败时为 `1`。

## 建议记录的实验矩阵

在相同数据和持久化配置下，逐项改变参数；每组运行多次并保留 JSON，先做一次不计入正式结果的预热运行。

| 维度 | 可比较取值 |
| --- | --- |
| 持久连接数 | 1、16、64、128 |
| 每连接 pipeline | 1、16 |
| 写入占比 | 0（纯读）、0.5（混合）、1（纯写） |
| value 字节数 | 128、1024、4096 |
| 总操作数 | 固定同一数量，例如 100,000；慢速配置应适当缩小 |

报告至少包含服务器与客户端 CPU、核数、内存、操作系统、编译器及优化模式、磁盘型号/文件系统、节点部署方式、网络 RTT、连接/pipeline/操作数/value 大小、写入比例、全部错误分类、有效吞吐和批次延迟。客户端与三个节点在同一机器时会竞争 CPU；增加客户端连接不保证服务端吞吐持续增加。还应记录应用提交版本以及 RocksDB WAL / sync 等实际持久化配置，避免把不同落盘策略的数字直接比较。

当前 `GET` 是节点本地读取；命中 Leader 也不等于实现了线性一致读。写入确认、复制和落盘语义必须按当前实现说明，不能仅凭吞吐或此脚本声称“强一致”“无丢失”。故障转移正确性由 `tests/cluster_smoke.py` 的独立场景检查；本客户端自身通过检查不构成真实 Raft 正确性或性能证据。

批量提交与容量参数见 [并发处理说明](concurrency.md)。对照 `--group_commit_ms=0、1、5` 时，保持 async_apply、`sync=true`、负载及容量配置相同，每次只改变收集期限。三组都在排队达到 128 条或 1 MiB 时立即排入事件队列；未满批按最老条目的入队时间到期，处理前批后也不重新获得完整等待窗口。`0` 仍可能合并同轮请求，所有实际 flush 都排队执行，因此它不是“逐条落盘”或“内联执行”的基线。分别观察低并发延迟和高并发批大小，不能仅按参数值推算收益。

## Linux 同步与异步应用对照

服务端默认 `--async_apply=true`，只把已提交 KV 批次交给串行 worker；`--async_apply=false` 将该批次留在 owner EventLoop 中执行。两组均使用当前实现的 `sync=true`，不能用关闭同步持久化的一组代表异步优化效果。Raft 日志/硬状态读写及本地 GET 在两组中都可能阻塞 owner。

使用相同构建版本、硬件、三节点部署、磁盘、数据规模、预热过程及容量参数，分别启动全体节点都带 `--async_apply=false` 和都带 `--async_apply=true` 的专用集群。只改变这一参数，使用两组隔离但等价的成对 KV / 日志目录；不要同时运行两组造成资源竞争。`--async_apply` 是服务端参数，不是压测客户端参数。

每组启动并选出稳定 Leader 后，核对所有节点 `INFO` 返回的 `async_apply` 与预期一致，再对实际 Leader 地址运行同一请求配置。例如在两组中分别把输出名设为 `sync.json`、`async.json`：

```sh
python3 tests/load_benchmark.py --host 127.0.0.1 --port 8080 --connections 64 --requests 100000 --pipeline 1 --value-size 128 --write-ratio 0.5 --output sync.json
```

这里的 `8080` 必须替换为该组实际 Leader 的端口。先用 `pipeline=1` 对照请求延迟，再单独以相同的更高 pipeline 配置对照吞吐；两组都报告成功吞吐、错误分类及 p50/p95/p99，不把不同 pipeline 的批次延迟直接比较。

在测量前、中、后，以相同频率向所有节点发送 `INFO`，采集其中的 `async_apply / apply_inflight / apply_lag / commit_index / last_applied / term / state / leader_id`，并记录采样开销。`apply_lag` 是已提交但 owner 尚未确认完成应用的日志条数，`apply_inflight=1` 包含完成通知仍排队的阶段。结合日志或单独的诊断采集记录心跳处理延迟、选举和 Leader 切换，比较任期变化是否增多；这些 INFO 字段本身不是心跳延迟测量工具，现有压测客户端也不会自动持续采样它们。

Follower 在异步模式下可以在 Raft 日志同步持久化后、KV 应用结束前回复复制成功；客户端写成功仍需等待 KV 同步持久化和 owner 完成通知。应同时观察有效吞吐、`apply_lag`、心跳及任期变化，确认积压与服务行为，不能仅用复制确认速度代表客户端写入完成速度。上述实验尚未执行，文档不提供性能估算或提升倍数。

同时保存测量区间起止的阶段计数器。各前缀的准确边界见 [阶段指标说明](concurrency.md)：`write_queue_wait` 到组批即结束，后续可能被拒绝；`write_completed` 到服务端成功回调结束，即使客户端已断连仍计数；`local_read` 包括未命中的正常读取。日志写和 KV 应用以批为样本，`replication_data_ack` 以成功数据 RPC 为样本，包含缓冲和重试且排除空心跳；`apply_dispatch` 是 worker 完成到 owner 收到通知的等待，同步模式为 0。它们都不能替代客户端端到端延迟，也不能把各阶段均值相加。

这些服务端计数器使用固定内存，自进程启动累计，重启归零，不保留分位数；`_avg_us` 和 `_avg_entries` 是向下取整的累计均值。在确认进程未重启且 `Δcount > 0` 时，用 `Δtotal_us / Δcount` 得到测量区间均值，用 `Δentries / Δcount` 得到区间平均批大小。`_bytes` 只累计命令字节，no-op 计一条且占 0 命令字节；累计最大值不能差分成区间最大值。结合客户端错误率解读成功阶段样本，另记录 `flush_immediate_scheduled / flush_delayed_scheduled / flush_promotions` 的增量；这些是调度动作次数，不是成功写数或落盘次数。
