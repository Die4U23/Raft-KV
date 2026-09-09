# 优化准备 002：定位正常读写的性能限制

状态：诊断工具和本地辅助检查已完成，**真实 Linux 诊断 UNRUN**。本轮不改服务端算法或持久化设置，先采集正常负载的客户端、节点和整机 CPU，以及写入阶段耗时。

## 为什么先测

[旧版双核 VM 基线](../benchmarks/ubuntu-2cpu-abba.md)中，三个服务进程和 Python 客户端同机运行，整轮 CPU 空闲约 5%，客户端中段快照占单核 55%～78%。这些数据说明 CPU 余量很小，尚不能区分客户端、服务线程、网络内核路径各自的限制。

新版本的[重连退避优化](../benchmarks/reconnect-validation.md)已验证故障期间的重连和日志减少，不能据此推断正常 QPS 提升。本次建立当前二进制的正常读写诊断记录；单轮结果不是性能优化前后对比。

## 本轮做什么

`tests/profile_benchmark.py` 验证既有构建报告与二进制/源码指纹后，启动自己管理的三个节点和一个独立 Python 负载进程；直接通过本机 TCP 通信，不使用分区转发器。

| 项目 | 固定设置 |
| --- | --- |
| 服务 | async_apply=true，group_commit_ms=1；原有同步持久化与多数派确认不变 |
| 负载 | 32 连接，pipeline=1，128 字节 value，读写各半 |
| 预热 / 正式 | 10,000 / 100,000 次请求；每段 32 个私有键，正式测量前重新预填充 |
| CPU 采样 | 客户端与三个节点的正式窗口起止累计 CPU；另每 0.5 秒保存进程 RSS、CPU、线程、FD 和整机 CPU 计数 |
| INFO | 正式预填充完成后、计时工作开始前采一次；计时工作结束后、关闭客户端连接前采一次 |
| 正确性 | 正式客户端零错误，稳定 Leader/任期，服务端成功写计数等于正式写请求数；结束后三副本检查正式负载的 32 个键 |
| 清理 | 只清理本轮进程和临时数据库，保留报告、客户端日志、三个节点日志与构建身份 |

客户端沿用 `load_benchmark.py`，只新增可选的测量边界观察回调；普通 CLI 不启用该回调，负载生成与响应校验逻辑不变。回调在吞吐/延迟计时区间外采集 INFO 和进程计数。CPU 窗口包围实际负载，含少量边界 `/proc` 采样开销；不宣称纳秒级同步。

子进程内的起止 CPU 快照用于正式窗口汇总。外部采样记录包含启动、预热和结束阶段，分析时必须按 `client.windows.start/end.at` 过滤，不能把整份采样的平均值当作正式负载平均值。外部采样还记录采样器自身进程，便于观察监控开销；监控与负载仍共用同一 VM。

## 如何读结果

- `client.measurement`：成功吞吐、错误分类、P50/P95/P99；pipeline=1 时每批只有一个操作，不把 32 连接当作 32 个独立客户端进程。
- `client.cpu.processes`：每个进程消耗的 CPU 秒和单核百分比，100% 等于一个逻辑核。它不能直接与整机 CPU 百分比相加。
- `client.cpu.system_percent`：整机 user/nice/system/idle/iowait/irq/softirq/steal，guest 时间不重复累计。若内核计数异常下降，保留原始采样并将该汇总标为不可用，不虚构百分比。
- `client.stages`：使用 `Δtotal_us / Δcount` 得到阶段均值，含排队、Leader 日志写、复制确认、KV 应用、完成通知和本地读；部分阶段还有平均批大小。它们按请求、批次、RPC 取样，区间可能重叠，不能直接相加。
- `verified_keys_per_replica`：结束后逐副本校验的键数；资源数据不替代正确性检查。

如果客户端 CPU 很高，先考虑降低客户端开销或迁到另一台机器；如果整机 CPU 饱和，先区分客户端、服务与内核开销；如果节点阶段耗时偏高，再选择更细的函数级采样。CPU 数据本身不能证明具体某个函数是热点，也不能提前决定日志缓存一定有效。

本轮不改变部署拓扑、不隔离 CPU，所以不是“远程客户端已消除竞争”的证据；后续是否需要远程客户端或更轻的负载工具，由结果决定。不添加无依据的 QPS 门槛。

## Ubuntu 执行

服务端 C++ 未改，无需重编译。使用已核验的重连优化构建：

```bash
cd ~/projects/raft-kv &&
git pull --ff-only origin main &&
python3 tests/profile_benchmark.py \
  --build-report build-linux-reconnect/reports/run-c4arz1uk/build-report.json
```

默认整轮期限 240 秒，正常耗时随机器性能而变，通常几十秒；结束清理有额外短暂等待。最终预期 `PASS: aligned performance diagnostic; report: .../report.json`，随后输出吞吐、延迟、CPU 和阶段汇总。把结果与该 report.json 回传，失败时先查看同目录的 client-report.json、client.log 和节点日志，不重复跑来挑选更好的数字。

本地辅助检查：`python tests/profile_benchmark_tests.py` 与 `python tests/load_benchmark.py --self-test`。它们验证计算口径与回调边界，不启动真实服务，也不证明 Linux 诊断已通过。
