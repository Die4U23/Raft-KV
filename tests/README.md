# 测试

## 可移植协议与核心测试

不构建 Linux 服务端时，可运行五个独立的 C++ 测试目标：

```sh
cmake -S . -B build-protocol -DRAFTKV_BUILD_SERVER=OFF -DBUILD_TESTING=ON
cmake --build build-protocol
ctest --test-dir build-protocol --output-on-failure
```

- `protocol_tests` 使用实际解析器、输入缓冲区、帧编解码器和命名空间实现。
- `core_tests` 编译实际 RaftNode、RaftLog、KVStateMachine 与 RocksDBStore 源码，通过可控消息队列测试多数派、重复投票、复制、重启、日志冲突、丢失回复与存储失败。
- `storage_batch_tests` 检查批量同步写调用次数、批内删除语义、整批校验、故障和恢复；核心测试还检查批量复制顺序与条数/字节配额。
- `async_executor_tests` 使用实际 `SerialApplyExecutor` 与真实 `std::thread`，检查串行任务、owner 线程完成通知、异常传递和停止时 join；没有数据库或真实网络。
- `batch_flush_tests` 使用实际 `BatchFlushPolicy` 和显式假事件队列，检查满条数/字节数或已到期时升格、保留初始窗口、取消竞态下的旧 token 与重复回调、多批分轮处理、尾批剩余期限及零延迟；不调用 Muduo 定时器。

核心测试中的异步应用场景使用手动执行器，控制工作执行及完成通知的时机。它检查 KV 完成前不回复客户端成功、不提前释放正常在途提案配额；完成通知顺序、Stop/Start 恢复、任期变化与已销毁节点的晚到回调；以及 Follower 日志持久化确认不等待 KV 应用。真实线程调度由 `async_executor_tests` 单独覆盖，不能把手动执行器结果当作真实数据库并发压力测试。

核心测试还检查阶段计数器的成功样本、批次条数与命令字节、整数均值、数据 RPC 确认与重试口径，以及异步应用指标只在 owner 完成通知后发布。失败追加或应用不增加对应成功阶段计数。替身环境只能验证这些采样边界和计算规则，不能验证真实耗时或性能。

`tests/test_support/` 仅在核心和批量存储测试中使用。其 RocksDB 替身是支持故障注入的内存映射，Protobuf 替身使用进程内对象快照，PeerManager 替身使用消息队列。因此核心测试能检查状态转换、调用顺序及错误传播，不能验证 Protobuf 真实编码、磁盘 fsync、掉电恢复、文件锁、实际连接重连或网络调度。服务端目标不包含这些替身头文件。执行器测试虽使用真实线程，也不验证 RocksDB 并发 GET/Write 或 Muduo 调度。

Python 测试客户端仅使用标准库，兼容 Python 3.8+。以下命令检查 RESP 编解码、碎片回复、粘连回复、数组、二进制 bulk、错误、整数和 EOF；不会启动集群：

```sh
python3 tests/cluster_smoke.py --self-test
```

## Linux 真实三节点 smoke test

另有独立的[网络分区与失去多数派测试](../docs/partition-test.md)，通过测试专用 TCP 转发器切断 Raft 通信，不修改系统防火墙。本地辅助测试已通过，真实 Linux 分区验收仍待执行，不能由下述冒烟结果替代。

需要 Linux、Python 3.8+，以及能运行的 `raft_kv_server` 和相应动态库。先按项目说明安装 Muduo、RocksDB、Protobuf 等依赖，再从项目根目录运行：

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
python3 tests/cluster_smoke.py --binary build/raft_kv_server --timeout 90
```

服务端默认开启 `--async_apply=true`，因此未额外传参的集成脚本走串行 worker 应用路径。每个 RaftNode 最多一个不超过 128 条、约 2 MiB 的应用批次在途，包含等待 owner 完成通知的阶段；客户端成功回复仍在 KV 同步持久化与 owner 通知处理之后。`--async_apply=false` 是同步应用基线，两种模式都保留 `sync=true`。仅 KV 已提交批次移到 worker，Raft 日志/硬状态和本地 GET 仍在 owner；本地 GET 仍不具备线性一致读保证。运行时可查看 `INFO` 返回的 `async_apply / apply_inflight / apply_lag` 检查模式和应用积压，完整语义见 [并发处理说明](../docs/concurrency.md)。

收集队列达到 128 条或 1 MiB 时立即排队执行 flush；未满批沿用当前最老条目的收集期限。每次回调最多检查 128 个条目，包含失效客户端。升格会取消旧定时器并用 token 拒绝过期回调；定时器到期也先排入事件队列，不内联刷新。策略测试不验证该路径与真实 Muduo 的集成，需在 Linux 检查；`INFO` 中的调度次数和阶段指标如何解读，见 [压测说明](../docs/benchmark.md)。

脚本启动自己管理的三个进程，通过 `INFO` 发现实际 Leader，依次验证：

1. 三节点对 Leader 和任期达成一致。
2. 分片发送 `PING`；同一 TCP 连接流水发送 `SET`、`GET`，检查回复顺序和包含 NUL、CRLF 的二进制值；连续发送 257 条 PING 后再 GET，覆盖多轮命令处理。
3. 同一连接内切换 `SELECT` 命名空间，验证相同 key 的隔离及切回后的值。
4. `DEL` 不存在的 key 返回 `0`，删除已有 key 返回 `1`，再次删除返回 `0`。
5. Follower 拒绝写入并返回 RESP `MOVED` 错误。
6. 32 个并发连接完成 SET/GET，并检查回复与应用顺序。
7. 用 `SIGKILL` 杀死当前 Leader，剩余两节点选出新 Leader，保留已确认数据并成功确认新写入。
8. 旧 Leader 使用原来的成对 KV / Raft 日志目录重启；三节点最终都读到故障前后数据和命名空间数据，且应用进度达到故障后写入的提交位置。

每次运行都新建临时数据目录，使用六个自动分配的本地空闲端口。脚本只终止自己启动的进程；退出时清理自己的临时数据，不访问已有集群目录。端口在启动对应节点前释放，仍存在很小的竞争窗口；绑定失败会使测试失败并保留日志。

默认整轮期限为 90 秒，单次网络请求也有期限。进程清理在期限之外，每个进程最多等待两次、每次 3 秒。日志和 `report.json` 写入 `build/cluster-smoke/run-*`，成功与失败均保留；可通过 `--artifacts /path/to/reports` 修改父目录。失败时还会打印各节点日志末尾，退出码为 `1`；完整通过为 `0`。`--self-test` 通过只代表客户端辅助代码通过，不能代表真实集群通过。

也可使用现有 Docker 开发镜像，所有节点运行在同一个容器内，无需 host 网络或映射服务端口：

```sh
docker build -t raft-kv-dev -f docker/dev.Dockerfile .
docker run --rm -v "$PWD":/workspace -w /workspace raft-kv-dev sh -c 'cmake -S . -B build-linux -DCMAKE_BUILD_TYPE=Release && cmake --build build-linux --parallel 2 && LD_LIBRARY_PATH=/usr/local/lib python3 tests/cluster_smoke.py --binary build-linux/raft_kv_server'
```

Docker 镜像构建依赖 `third_party/` 中的压缩包及网络；其耗时不计入 smoke test 期限。上述命令使用 Linux shell 语法。该脚本是有限场景的集成验收，不等于线性一致性、网络分区、磁盘故障或完整 Raft 正确性证明。

**当前证据状态：Ubuntu 三节点冒烟原始报告确认 10 项检查 PASS，四轮同步/异步负载共 40 万次正式请求、0 错误，两批原始材料与事后构建快照均已归档核验。** 详见 [性能基线与证据边界](../docs/benchmarks/ubuntu-2cpu-abba.md)及 [构建与冒烟核验](../docs/benchmarks/ubuntu-build-and-smoke.md)。测试时二进制身份不能由事后快照独立证明；该结果不代表掉电、磁盘故障、网络分区或完整并发行为已验证。Windows 可移植测试、本地客户端自测与用户提供的真实 Linux 结果分别记录；对照流程见 [压测说明](../docs/benchmark.md)，尚无稳定性能提升结论。

新增自动流程见 [Ubuntu 构建与验证](../docs/linux-build.md)。`python tests/build_workflow_tests.py` 在本地检查固定 Muduo 归档的自动修补、重复运行、编辑保护、身份记录与失败状态；不需要安装 Linux 依赖。自动流程默认构建后执行 CTest，带 `--smoke` 时再执行真实三节点测试。[2026-09-08 原始证据](../docs/benchmarks/linux-workflow-validation.md)确认新流程在用户 VM 的已有构建目录上通过，包含 CTest 5/5、真实三节点冒烟 10 项及测试时身份记录；后续[空目录全量构建](../docs/benchmarks/linux-fresh-validation.md)也已核验通过，含重新编译、链接及上述两类测试。干净系统依赖安装复现仍待验证。
