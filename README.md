# Raft-KV

[![Portable checks](https://github.com/Die4U23/Raft-KV/actions/workflows/portable.yml/badge.svg)](https://github.com/Die4U23/Raft-KV/actions/workflows/portable.yml)
[![Linux cluster](https://github.com/Die4U23/Raft-KV/actions/workflows/linux-cluster.yml/badge.svg)](https://github.com/Die4U23/Raft-KV/actions/workflows/linux-cluster.yml)

一个用 C++17 实现的三节点 Raft KV 存储学习项目，把 Muduo 网络接入、Raft 共识、RocksDB 持久化、有界并发与真实故障验证放进同一条请求链路。

项目实践长文：[从三节点存储到故障验证与性能优化](docs/raft-kv-project-practice.md)。

## 核心能力

- **Raft 写路径**：Leader 选举、日志复制、多数派提交、顺序状态机应用。
- **ReadIndex 线性一致读**：通过心跳多数派确认读屏障，等待本地应用位置追上后读取，无需日志复制即可保证线性一致性。
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
$BIN --node_id=0 --client_port=8080 --raft_port=9080 \
  --db_path=/tmp/kv_db_0 --raft_log_path=/tmp/raft_log_0 \
  --linearizable_reads=true
$BIN --node_id=1 --client_port=8081 --raft_port=9081 \
  --db_path=/tmp/kv_db_1 --raft_log_path=/tmp/raft_log_1 \
  --linearizable_reads=true
$BIN --node_id=2 --client_port=8082 --raft_port=9082 \
  --db_path=/tmp/kv_db_2 --raft_log_path=/tmp/raft_log_2 \
  --linearizable_reads=true
```

三条命令需在三个终端分别运行。启动后用 `redis-cli -p 8080 INFO` 查看角色和 Leader。

**可选配置**（默认都保持原来的单组、无令牌、无租约行为）：
- `--linearizable_reads=true`：启用 ReadIndex 线性一致读（默认 false）
- `--leader_only_reads=true`：仅在 Leader 节点响应读请求（默认 false）
- `--lease_reads=true`：在漂移上界内用 Leader 租约服务强一致读，窗口外退回 ReadIndex（默认 false）
- `--shards=N`：同一组静态 peer 上的 Raft 组数量，1–64。1 时路径和帧字节不变（默认 1）
- `--cluster_token=`：非空时在 Raft 帧外加 HMAC-SHA256；空则帧字节不变
- `--client_token=`：非空时除 `AUTH` 外的命令要先认证；空则 `AUTH` 在执行期是未知命令

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
| `SET key value client_id request_id` | 同上，但同一 `client_id` 的同一 `request_id` 只执行一次，重试返回上一次的回复 |
| `GET key` | 默认读当前节点本地状态机；`--linearizable_reads=true` 时 Leader 走 ReadIndex，Follower 返回 `MOVED` |
| `DEL key` | 通过 Raft 删除，返回 `0` 或 `1` |
| `DEL key client_id request_id` | 同上，重复序号返回上一次的 `0` 或 `1`，不再次删除 |
| `SELECT namespace` | 为当前 TCP 连接选择逻辑命名空间 |
| `CFGSET name value` | 通过 Raft 发布配置，成功返回新版本号 `:<version>` |
| `CFGSET name value client_id request_id` | 同上。同一序号重试不升版本，返回上一次的版本号 |
| `CFGROLLBACK name version` | 把该版本的值复制成一个新版本。没有这个版本时返回 `-ERR no such config version`，不消耗序号 |
| `CFGROLLBACK name version client_id request_id` | 同上，序号规则与 `SET` 相同 |
| `CFGGET name` | 读当前版本，回复 `*2`（版本号和值）。没有该名称时返回 `*-1`。`--linearizable_reads` 或 `--lease_reads` 时与 `GET` 走同一条强一致路径 |
| `CFGCACHE name max_age_ms` | 只读本机缓存。年龄不超过 `max_age_ms` 才返回；否则 `-ERR config not fresh`，不把过期值当成功 |
| `MEMBER JOIN id` / `MEMBER LEAVE id` | 一次加减一个已经在静态 peer 列表里的投票者。Leader 不能移除自己，也不能把投票者减空 |
| `AUTH password` | `--client_token` 为空时执行期返回未知命令。非空且密码正确返回 `OK`；未认证时其他命令返回 `-ERR NOAUTH Authentication required` |
| `INFO` | 查看角色、任期、Leader、提交/应用位置、过载指标、`shards` 和 `lease_reads` |

`SELECT` 只在当前连接上生效，分别执行的 `redis-cli` 进程不会共享命名空间状态。以 NUL 开头的键是保留键，`SET`/`GET`/`DEL` 会拒绝。空键仍是普通用户键。

## 验证层级

- **可移植 C++ 回归**：协议、Raft 边界、存储批量、异步执行器、组批和重连策略。
- **Python 辅助测试**：构建流程、客户端模式、性能对照与故障编排的可测部分。
- **Linux 真实服务**：Muduo TCP、Protobuf、RocksDB、三进程冒烟与故障注入。
- **证据核验**：`docs/benchmarks/verify_*.py` 对归档、哈希、报告与节点日志做交叉检查。

完整命令和每层能够/不能证明的内容见 [tests/README.md](tests/README.md)。

## 当前边界

- 默认仍是一个 Raft 组，启动时静态 peer 全部是投票者。`MEMBER JOIN` / `MEMBER LEAVE` 一次一个：进入 joint 配置后，提交要旧集合和新集合都过半数；应用内部的 `MEMBER COMMIT` 后才切到新投票者。Leader 不能移除自己，不能把集合减空，也不能加入静态列表之外的主机。`--shards` 大于 1 时同一组 peer 上有多个 Raft 组，键按 FNV-1a 分片，各分片各自选主；`MEMBER` 要求本节点在每个分片上都是 Leader。默认 `--shards=1`，数据路径和帧字节不变。
- `--cluster_token` 为空时 Raft 帧不变。非空时帧外是 `MAC1`、内层长度和 HMAC-SHA256，校验失败就关闭连接。`--client_token` 为空时客户端命令不认证。`--lease_reads` 默认关闭。打开后，投票者联系时间的多数派加上（150 ms − 10 ms）之前，Leader 把读排到当前提交位置，仍要等应用到那一位；恰好到达该窗口或窗口之外退回 ReadIndex。Follower 时钟快过这 10 ms 时，仍可能在 Leader 认为租约有效时开始竞选。
- 已应用条目超过快照距离（默认 1024）后压缩日志。落后副本用 InstallSnapshot 追平，镜像按 1 MiB 分片，收齐后再安装。镜像再大也压缩；压缩和安装时整份镜像留在内存里。
- `client_id` 为 1–128 字节且不能含 NUL，`request_id` 从 1 起按十进制连续递增、不补零。每个客户端只记住最新序号和那次回复。序号对不上时返回 `-ERR stale request id`，不改键。不带序号的 `SET`/`DEL` 仍会在重试时再执行一次。去重记录写进同一次状态机批次，并放进快照。
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

进一步阅读：[测试说明](tests/README.md) · [构建说明](docs/linux-build.md) · [压测方法](docs/benchmark.md) · [读一致性保证](docs/read-consistency.md) · [更新日志](CHANGELOG.md)

## 路线图

1. ✅ ~~ReadIndex 线性一致读~~：多数派确认读屏障，等待本地应用位置追上后再读取。默认关闭。选举截止时间和探针租约用同一把 `steady_clock`。隔离旧 Leader 在 CheckQuorum 到期后卸任，线性一致 GET 失败而不是返回过期值。
2. ✅ ~~Pre-Vote~~：选举超时先进入 pre-candidate，不抬任期、不记 `votedFor`。只有多数派预投票才开始真正的选举。竞选失败后回到预投票，而不是再次抬任期。
3. ✅ ~~快照 / InstallSnapshot / 日志压缩~~：应用后按距离导出 KV 镜像并截断日志前缀。落后副本按 1 MiB 分片接收 InstallSnapshot，收齐后安装。日志快照先于 KV 落盘，重启时用日志里的镜像补上尚未安装的状态。镜像变大不再跳过压缩。
4. ✅ ~~客户端请求去重~~：`SET`/`DEL` 带上 `client_id` 和 `request_id` 后，超时重试返回上一次的回复，不把同一条写再执行一次。序号必须从 1 连续递增。不带序号的写入保持原来的语义。`CFGSET` / `CFGROLLBACK` 使用同一张会话表。
5. ✅ ~~版本化配置~~：`CFGSET` 发布并返回新版本，`CFGROLLBACK` 把旧版本复制成新版本，`CFGGET` 读取当前版本，`CFGCACHE` 只在本地缓存还新鲜时返回。没有配置记录时快照仍是版本 2；有记录时快照带上配置节。
6. ✅ ~~成员变更~~：joint consensus，一次加减一个已在静态 peer 列表里的投票者。投票者集合写入 Raft 日志，并随 InstallSnapshot 带走。
7. ✅ ~~多分片~~：`--shards` 默认 1。大于 1 时每个分片有自己的日志、KV 和 Leader，帧内多一个分片号。
8. ✅ ~~认证~~：`--cluster_token` 给 Raft 帧加 HMAC-SHA256；`--client_token` 要求客户端先 `AUTH`。两者默认都为空。
9. ✅ ~~租约读~~：`--lease_reads` 默认关闭。窗口内不等待新的 ReadIndex 探针，仍等待 `lastApplied`；窗口外退回 ReadIndex。

## 依赖与来源

主要依赖为 Muduo、RocksDB、Protobuf、gflags、glog 与 Boost。

项目的实现与设计参考了以下论文、资料和开源项目：

| 参考项目 | 地址 | 用途 |
|---|---|---|
| **Raft 论文** | https://raft.github.io/raft.pdf | Raft 算法权威文档 |
| **Raft 可视化** | https://raft.github.io/ | 帮助理解选举与日志复制 |
| **muduo** | https://github.com/chenshuo/muduo | 网络层 |
| **RocksDB** | https://github.com/facebook/rocksdb | 存储引擎 |
| **etcd** | https://github.com/etcd-io/etcd | 成熟 Raft 应用，思路参考 |
| **TiKV** | https://github.com/tikv/tikv | Raft + RocksDB 工业实践 |

感谢这些开源项目及其维护者提供的学习资源。可追溯的后续改造范围在[项目实践长文](docs/raft-kv-project-practice.md#项目来源许可证与改造范围)中单独列出。

## 许可证

本项目采用 [MIT License](LICENSE) 开源许可证。
