# Raft-KV

一个用 C++17 实现的三节点 Raft KV 存储学习项目，把 Muduo 网络接入、Raft 共识、RocksDB 持久化、有界并发与真实故障验证放进同一条请求链路。

项目实践长文：[从三节点存储到故障验证与性能优化](docs/raft-kv-project-practice.md)。

## 核心能力

- **Raft 写路径**：Leader 选举、日志复制、多数派提交、顺序状态机应用。
- **持久化语义**：Raft 日志和硬状态同步落盘；KV 与 `lastApplied` 在同一 RocksDB WriteBatch 中提交。
- **有界服务**：限制连接数、输入/输出缓冲、写队列、提案数及字节数，过载时拒绝而不是无限积压。
- **批量与异步应用**：单个 EventLoop 轮次内组批，已提交 KV 批次交给串行工作线程，完成回调回到所有者线程。
- **RESP 协议接入**：处理 TCP 分片/粘包、连续命令、二进制 value 和恶意长度输入。
- **可追溯验证**：分区、Leader 崩溃/重启、过载和对照实验都保留报告、节点日志、源码/二进制指纹及独立核验脚本。

## 架构

```text
客户端（RESP）
      │
      ▼
Muduo Client Server ── GET ──► KVStateMachine ──► RocksDB
      │                         ▲
      └─ SET / DEL ─► RaftNode ───────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        RaftLog / RocksDB      PeerManager / Muduo
                                    │
                              其他 Raft 节点
```

写请求在 Leader 上入日志，通过 PeerManager 复制到多数节点，推进 `commit_index`，再按序应用到 KV 状态机。详细时序、线程归属与回调生命周期见[并发处理说明](docs/concurrency.md)。

## 已验证结果

| 场景 | 结果 | 证据 |
| --- | --- | --- |
| Ubuntu 26.04 三进程冒烟 | 构建、CTest 5/5、冒烟 10/10 | [全量构建记录](docs/benchmarks/linux-fresh-validation.md) |
| 短时 TCP 分区 | 隔离旧 Leader、多数派继续写入、恢复后副本收敛 | [分区验证](docs/benchmarks/partition-validation.md) |
| 持续写入中崩溃/重启 | 1,704 个已确认键在两次恢复后的三副本上保留 | [恢复验证](docs/benchmarks/write-restart-validation.md) |
| 过载与 60 秒观察 | 连接准入、`BUSY`、恢复清零和资源阈值均通过 | [过载验证](docs/benchmarks/overload-validation.md) |

已归档的双核 VM 基线在三节点和客户端同机、32 连接、`pipeline=1`、128 字节 value、50% GET / 50% SET 下测得：

| 模式 | 合并吞吐 | 两轮 p50 | 两轮 p99 |
| --- | ---: | ---: | ---: |
| 同步应用 | 3,656.53 ops/s | 7.88 / 8.85 ms | 20.61 / 25.67 ms |
| 串行异步应用 | 3,824.56 ops/s | 8.35 / 8.05 ms | 18.39 / 26.89 ms |

这些是指定 VM 和负载下的测量值，用于回归与机制分析，不是通用容量承诺。完整参数、CPU 成本和原始证据见[性能基线](docs/benchmarks/ubuntu-2cpu-abba.md)。

## 快速开始

### 1. 构建

Ubuntu 上推荐使用会记录源码、依赖与二进制身份的自动流程：

```bash
python3 scripts/build_linux.py --jobs 2 --smoke
```

成功后服务位于 `build-linux-repro/server/raft_kv_server`。系统依赖、Muduo 准备方式和报告结构见 [Ubuntu 构建说明](docs/linux-build.md)。

只运行不依赖 Muduo/RocksDB 实库的可移植回归：

```bash
cmake -S . -B build-portable -G Ninja \
  -DRAFTKV_BUILD_SERVER=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build build-portable --parallel 2
ctest --test-dir build-portable --output-on-failure
python3 -m unittest discover -s tests -p '*_tests.py'
```

### 2. 启动三节点

为每个节点使用配对且独立的 KV / Raft 目录：

```bash
BIN=./build-linux-repro/server/raft_kv_server

$BIN --node_id=0 --client_port=8080 --raft_port=9080 \
  --db_path=/tmp/kv_db_0 --raft_log_path=/tmp/raft_log_0
$BIN --node_id=1 --client_port=8081 --raft_port=9081 \
  --db_path=/tmp/kv_db_1 --raft_log_path=/tmp/raft_log_1
$BIN --node_id=2 --client_port=8082 --raft_port=9082 \
  --db_path=/tmp/kv_db_2 --raft_log_path=/tmp/raft_log_2
```

三条命令需在三个终端分别运行。启动后用 `redis-cli -p 8080 INFO` 查看角色和 Leader。

### 3. 读写

```bash
redis-cli -p 8080 SET user:1 alice
redis-cli -p 8080 GET user:1
redis-cli -p 8080 DEL user:1
```

向 Follower 写入会返回项目自定义的 `-ERR MOVED <leader_id>`；它不是 Redis Cluster 的完整重定向协议。

## 客户端命令

| 命令 | 语义 |
| --- | --- |
| `PING` | 返回 `PONG` |
| `SET key value` | 通过 Raft 提交和状态机应用后返回 `OK` |
| `GET key` | 读当前节点的本地状态机 |
| `DEL key` | 通过 Raft 删除，返回 `0` 或 `1` |
| `SELECT namespace` | 为当前 TCP 连接选择逻辑命名空间 |
| `INFO` | 查看角色、任期、Leader、提交/应用位置和过载指标 |

`SELECT` 只在当前连接上生效，分别执行的 `redis-cli` 进程不会共享命名空间状态。

## 验证层级

- **可移植 C++ 回归**：协议、Raft 边界、存储批量、异步执行器、组批和重连策略。
- **Python 辅助测试**：构建流程、客户端模式、性能对照与故障编排的可测部分。
- **Linux 真实服务**：Muduo TCP、Protobuf、RocksDB、三进程冒烟与故障注入。
- **证据核验**：`docs/benchmarks/verify_*.py` 对归档、哈希、报告与节点日志做交叉检查。

完整命令和每层能够/不能证明的内容见 [tests/README.md](tests/README.md)。

## 当前边界

- `GET` 是本地读；`--leader_only_reads=true` 只检查本机角色，**不是 ReadIndex，不保证线性一致读**。
- 集群是固定成员的单 Raft 组，没有动态成员变更和多分片。
- 没有快照与日志压缩，日志和启动扫描成本会随历史增长。
- 没有 `client_id + request_id` 去重；超时或回复丢失后重试可能重复执行。
- 已有验证不覆盖整机掉电、存储介质损坏、长时间压测或完整 Raft 正确性证明。

详细实现范围与验证边界见 [项目状态](docs/review-status.md)。

## 项目导航

```text
src/server/        RESP 接入、会话、组批与过载保护
src/raft/          RaftNode、PeerManager、KV 状态机与串行应用器
src/raftcore/      Raft 日志及硬状态持久化
src/storage/       RocksDB 封装和存储错误边界
src/common/        RESP、组批、指标和重连策略
src/namespace/     按连接管理的逻辑命名空间
proto/             Raft RPC 消息
tests/             可移植回归、真实集群故障测试与压测工具
docs/benchmarks/   原始证据、核验脚本和结果边界
scripts/           Linux 构建与固定 Muduo 准备流程
```

进一步阅读：[测试说明](tests/README.md) · [构建说明](docs/linux-build.md) · [压测方法](docs/benchmark.md) · [更新日志](CHANGELOG.md)

## 路线图

1. Pre-Vote / CheckQuorum：减少隔离节点恢复后的无效任期抬升和重新选举。
2. ReadIndex 线性一致读：多数派确认读屏障，等待本地应用位置追上后再读取。
3. 快照 / InstallSnapshot / 日志压缩：限制日志增长和启动恢复时间。
4. 客户端请求去重：为超时重试提供明确语义。

## 依赖与来源

主要依赖为 Muduo、RocksDB、Protobuf、gflags、glog 与 Boost。项目早期参考过现已无法确认的教程/代码，因此不把全部基础实现声明为个人原创；可追溯的后续改造范围在[项目实践长文](docs/raft-kv-project-practice.md#项目来源许可证与改造范围)中单独列出。在源码归属核对完成前，仓库不宣告项目级开源许可证。
